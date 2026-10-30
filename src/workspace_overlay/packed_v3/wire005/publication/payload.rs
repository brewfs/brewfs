//! Full GC05/LD05 authentication with bounded sequential ranges.
//! The returned facts still require group/FD/namespace and codec joins.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::cadapter::read_observer::{Origin, ReadClass, ReadWork};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};
use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec};

use super::super::{
    V3_FOOTER_LEN, V3_HEADER_LEN, V3_MAX_BODY_BYTES, V3BudgetPool, V3GroupRef, V3MountBudget,
    V3ObjectKind, V3ObjectRef, V3OwnedPermit, decode_v3_envelope_header, observer_backend_error,
    observer_validation_error, validate_v3_envelope_footer,
};

const MAX_RANGE_BYTES: usize = 1024 * 1024;
const GC_MAX_GROUPS: usize = 1024;
const GC_MAX_FRAMES: u32 = 65536;
const GC_MAX_NAME_BYTES: usize = 1024;
const GC_ENTRY_FIXED_BYTES: usize = 108;
const LD_BODY_LIMIT: usize = 24 * 1024 * 1024;
const LD_RAW_LIMIT: u64 = 16 * 1024 * 1024;
const LD_FRAME_LIMIT: u32 = 1024;

#[derive(Clone, Copy, Debug)]
pub struct PayloadLimits {
    pub chunk_bytes: usize,
    pub max_body_bytes: usize,
}

impl Default for PayloadLimits {
    fn default() -> Self {
        Self {
            chunk_bytes: MAX_RANGE_BYTES,
            max_body_bytes: V3_MAX_BODY_BYTES,
        }
    }
}

#[derive(Debug)]
pub enum PayloadSummary {
    Group {
        container_id: u64,
        profile: AccessProfile,
        frame_count: u32,
        groups: Vec<V3GroupRef>,
        directory_end: u64,
        metadata_end: u64,
    },
    Large {
        inode: u64,
        chunk_id: u64,
        frame_count: u32,
        raw_total: u64,
    },
}

/// Summary allocations cannot escape their reservation by moving out fields.
#[derive(Debug)]
pub struct AuthenticatedPayload {
    summary: PayloadSummary,
    _metadata: V3OwnedPermit,
}

impl AuthenticatedPayload {
    pub fn summary(&self) -> &PayloadSummary {
        &self.summary
    }
}

fn cancelled() -> PackedWireError {
    PackedWireError::Backend("packed publication payload verification cancelled".into())
}

fn closed() -> PackedWireError {
    PackedWireError::LimitExceeded("packed publication budget closed".into())
}

fn invalid(what: &str) -> PackedWireError {
    PackedWireError::Invalid(what.into())
}

fn live(budget: &V3MountBudget, cancel: &CancellationToken) -> PackedResult<()> {
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    if budget.state().closed {
        return Err(closed());
    }
    Ok(())
}

/// HEAD proves physical length; hashes prove all bytes in the authenticated ref.
/// Neither proves frame decodability or the logical external-file digest.
pub async fn authenticate_payload<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    reference: &V3ObjectRef,
    container_ordinal: u32,
    budget: &Arc<V3MountBudget>,
    limits: PayloadLimits,
    cancel: &CancellationToken,
) -> PackedResult<AuthenticatedPayload> {
    let class = match reference.kind {
        V3ObjectKind::GroupContainer => ReadClass::PackedPayload,
        V3ObjectKind::LargeData => ReadClass::ExternalPayload,
        _ => {
            return Err(invalid(
                "publication payload reference has a non-payload kind",
            ));
        }
    };
    let origin = client
        .read_context(class)
        .map_or(Origin::Demand, |c| c.origin);
    let validation = client.begin_validation_with_origin(class, origin);
    let result = authenticate_inner(
        client,
        reference,
        container_ordinal,
        budget,
        limits,
        cancel,
        class,
    )
    .await;
    if let Some(validation) = validation {
        match &result {
            Ok(_) => validation.succeed(),
            Err(_) if cancel.is_cancelled() => drop(validation),
            Err(error) => validation.fail(observer_validation_error(error.clone()).0),
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn authenticate_inner<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    reference: &V3ObjectRef,
    container_ordinal: u32,
    budget: &Arc<V3MountBudget>,
    limits: PayloadLimits,
    cancel: &CancellationToken,
    class: ReadClass,
) -> PackedResult<AuthenticatedPayload> {
    live(budget, cancel)?;
    if limits.chunk_bytes == 0
        || limits.chunk_bytes > MAX_RANGE_BYTES
        || limits.max_body_bytes > V3_MAX_BODY_BYTES
        || limits.max_body_bytes == 0
    {
        return Err(PackedWireError::LimitExceeded(
            "publication payload range/body limits are invalid".into(),
        ));
    }
    let body_limit = if reference.kind == V3ObjectKind::LargeData {
        limits.max_body_bytes.min(LD_BODY_LIMIT)
    } else {
        limits.max_body_bytes
    };
    // Include the stream response and exact-range accumulation, header/footer,
    // one directory entry and its temporary validation encoding before I/O.
    let stored = budget.admit(&[
        (
            V3BudgetPool::Stored,
            (2 * limits.chunk_bytes + V3_HEADER_LEN + V3_FOOTER_LEN) as u64,
        ),
        (V3BudgetPool::Control, 16384),
    ])?;
    reference.encode_value()?;
    if reference.object_len > (V3_HEADER_LEN + body_limit + V3_FOOTER_LEN) as u64 {
        return Err(PackedWireError::LimitExceeded(
            "publication payload exceeds body limit".into(),
        ));
    }
    verify_physical_length(client, reference, budget, cancel, class).await?;
    let mut stream = PayloadStream {
        client,
        reference,
        budget,
        cancel,
        class,
        chunk_bytes: limits.chunk_bytes,
        fetched: 0,
        consumed: 0,
        cursor: 0,
        chunk: Vec::new(),
        full_hash: Sha256::new(),
        body_hash: Sha256::new(),
        _stored: stored,
    };
    let mut header = [0; V3_HEADER_LEN];
    stream.read_into(&mut header).await?;
    let body_len =
        decode_v3_envelope_header(&header, reference.kind, reference.object_len, body_limit)?;
    let (summary, metadata) = match reference.kind {
        V3ObjectKind::GroupContainer => read_group_summary(&mut stream, container_ordinal).await?,
        V3ObjectKind::LargeData => read_large_summary(&mut stream).await?,
        _ => unreachable!("payload kind checked before I/O"),
    };
    let body_end = (V3_HEADER_LEN + body_len) as u64;
    if stream.consumed > body_end {
        return Err(invalid(
            "publication payload prefix exceeds authenticated body",
        ));
    }
    stream.skip_to(body_end).await?;
    let mut footer = [0; V3_FOOTER_LEN];
    stream.read_into(&mut footer).await?;
    if stream.consumed != reference.object_len || stream.fetched != reference.object_len {
        return Err(invalid(
            "publication payload did not consume its complete envelope",
        ));
    }
    let body_digest: [u8; 32] = stream.body_hash.clone().finalize().into();
    let computed: [u8; 32] = stream.full_hash.clone().finalize().into();
    if computed != reference.digest {
        return Err(PackedWireError::HashMismatch {
            what: "publication complete payload",
            expected: hex::encode(reference.digest),
            computed: hex::encode(computed),
        });
    }
    validate_v3_envelope_footer(&header, &footer, reference.object_len, &body_digest)?;
    // Catch a suffix appended during bounded range reads. Remote immutable
    // object semantics remain necessary: two HEADs cannot create an atomic
    // view of a backend that permits arbitrary replacement after validation.
    verify_physical_length(client, reference, budget, cancel, class).await?;
    live(budget, cancel)?;
    Ok(AuthenticatedPayload {
        summary,
        _metadata: metadata,
    })
}

async fn verify_physical_length<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    reference: &V3ObjectRef,
    budget: &V3MountBudget,
    cancel: &CancellationToken,
    class: ReadClass,
) -> PackedResult<()> {
    live(budget, cancel)?;
    let physical = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(cancelled()),
        _ = budget.wait_closed() => return Err(closed()),
        size = client.typed_object_size(class, &reference.key) => {
            size.map_err(observer_backend_error)?
        }
    };
    if physical != Some(reference.object_len) {
        return Err(invalid(
            "publication payload physical length differs from authenticated ref",
        ));
    }
    Ok(())
}

struct PayloadStream<'a, B: ObjectBackend> {
    client: &'a ObjectClient<B>,
    reference: &'a V3ObjectRef,
    budget: &'a Arc<V3MountBudget>,
    cancel: &'a CancellationToken,
    class: ReadClass,
    chunk_bytes: usize,
    fetched: u64,
    consumed: u64,
    cursor: usize,
    chunk: Vec<u8>,
    full_hash: Sha256,
    body_hash: Sha256,
    _stored: V3OwnedPermit,
}

impl<B: ObjectBackend + Clone> PayloadStream<'_, B> {
    async fn fetch(&mut self) -> PackedResult<()> {
        live(self.budget, self.cancel)?;
        if self.fetched == self.reference.object_len {
            return Err(PackedWireError::Truncated {
                what: "publication payload prefix",
                need: 1,
                have: 0,
            });
        }
        // Release the consumed vector before allocating the next response.
        self.chunk = Vec::new();
        self.cursor = 0;
        let length = (self.reference.object_len - self.fetched).min(self.chunk_bytes as u64);
        self.chunk = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(cancelled()),
            _ = self.budget.wait_closed() => return Err(closed()),
            chunk = self.client.typed_exact(
                self.class,
                &self.reference.key,
                self.fetched,
                length,
                self.chunk_bytes as u64,
                Ok,
            ) => chunk.map_err(observer_backend_error)?,
        };
        live(self.budget, self.cancel)?;
        let timer = self
            .client
            .measure_read_work(self.class, ReadWork::Authentication);
        self.full_hash.update(&self.chunk);
        let start = self.fetched.max(V3_HEADER_LEN as u64);
        let end = (self.fetched + length).min(self.reference.object_len - V3_FOOTER_LEN as u64);
        if start < end {
            self.body_hash.update(
                &self.chunk[(start - self.fetched) as usize..(end - self.fetched) as usize],
            );
        }
        drop(timer);
        self.fetched += length;
        Ok(())
    }

    async fn read_into(&mut self, mut destination: &mut [u8]) -> PackedResult<()> {
        while !destination.is_empty() {
            if self.cursor == self.chunk.len() {
                self.fetch().await?;
            }
            let count = destination.len().min(self.chunk.len() - self.cursor);
            destination[..count].copy_from_slice(&self.chunk[self.cursor..self.cursor + count]);
            self.cursor += count;
            self.consumed += count as u64;
            destination = &mut destination[count..];
        }
        Ok(())
    }

    async fn skip_to(&mut self, end: u64) -> PackedResult<()> {
        while self.consumed < end {
            if self.cursor == self.chunk.len() {
                self.fetch().await?;
            }
            let count = (end - self.consumed).min((self.chunk.len() - self.cursor) as u64);
            self.cursor += count as usize;
            self.consumed += count;
        }
        Ok(())
    }
}

async fn read_group_summary<B: ObjectBackend + Clone>(
    stream: &mut PayloadStream<'_, B>,
    container_ordinal: u32,
) -> PackedResult<(PayloadSummary, V3OwnedPermit)> {
    let mut prefix = [0; 24];
    stream.read_into(&mut prefix).await?;
    if &prefix[..4] != b"GC05" || prefix[13..16] != [0; 3] {
        return Err(invalid(
            "publication GC05 prefix identity/reserved fields mismatch",
        ));
    }
    let container_id = u64::from_le_bytes(prefix[4..12].try_into().unwrap());
    let profile = AccessProfile::from_u8(prefix[12])
        .map_err(|_| invalid("publication GC05 profile is invalid"))?;
    let count = u32::from_le_bytes(prefix[16..20].try_into().unwrap()) as usize;
    let frame_count = u32::from_le_bytes(prefix[20..24].try_into().unwrap());
    if count == 0 || count > GC_MAX_GROUPS || frame_count > GC_MAX_FRAMES {
        return Err(PackedWireError::LimitExceeded(
            "publication GC05 group/frame count exceeds limits".into(),
        ));
    }
    let summary_bytes = count * (std::mem::size_of::<V3GroupRef>() + 2 * GC_MAX_NAME_BYTES)
        + std::mem::size_of::<PayloadSummary>();
    let metadata = stream
        .budget
        .admit(&[(V3BudgetPool::Metadata, summary_bytes as u64)])?;
    let mut groups: Vec<V3GroupRef> = Vec::with_capacity(count);
    let body_end = stream.reference.object_len - V3_FOOTER_LEN as u64;
    let mut used_frames = 0_u32;
    for _ in 0..count {
        let mut entry = [0; GC_ENTRY_FIXED_BYTES];
        stream.read_into(&mut entry).await?;
        let first_len = u16::from_le_bytes(entry[104..106].try_into().unwrap()) as usize;
        let last_len = u16::from_le_bytes(entry[106..108].try_into().unwrap()) as usize;
        if entry[25..28] != [0; 3]
            || first_len == 0
            || last_len == 0
            || first_len > GC_MAX_NAME_BYTES
            || last_len > GC_MAX_NAME_BYTES
        {
            return Err(invalid(
                "publication GC05 directory name/reserved fields mismatch",
            ));
        }
        let mut first_name = vec![0; first_len];
        let mut last_name = vec![0; last_len];
        stream.read_into(&mut first_name).await?;
        stream.read_into(&mut last_name).await?;
        let group = V3GroupRef {
            group_id: u64::from_le_bytes(entry[..8].try_into().unwrap()),
            container_ordinal,
            parent_dir_key: entry[72..104].try_into().unwrap(),
            first_name,
            last_name,
            meta_offset: u64::from_le_bytes(entry[8..16].try_into().unwrap()),
            meta_stored_len: u32::from_le_bytes(entry[16..20].try_into().unwrap()),
            meta_raw_len: u32::from_le_bytes(entry[20..24].try_into().unwrap()),
            meta_codec: PackedCodec::from_u8(entry[24])?,
            meta_digest: entry[40..72].try_into().unwrap(),
            entry_count: u32::from_le_bytes(entry[28..32].try_into().unwrap()),
            first_frame: u32::from_le_bytes(entry[32..36].try_into().unwrap()),
            frame_count: u32::from_le_bytes(entry[36..40].try_into().unwrap()),
        };
        // Reuse the production GR05 rules, with the temporary bounded encoding
        // covered by the directory-entry scratch admission.
        group.encode_value()?;
        let frame_end = group
            .first_frame
            .checked_add(group.frame_count)
            .ok_or_else(|| invalid("publication GC05 frame range overflows"))?;
        if groups
            .iter()
            .any(|previous| previous.group_id == group.group_id)
            || frame_end > frame_count
            || (group.frame_count == 0 && group.first_frame != 0)
            || group
                .meta_offset
                .checked_add(u64::from(group.meta_stored_len))
                .is_none_or(|end| end > body_end)
            || groups.iter().any(|previous| {
                group.frame_count > 0
                    && previous.frame_count > 0
                    && group.first_frame < previous.first_frame + previous.frame_count
                    && previous.first_frame < frame_end
            })
        {
            return Err(invalid(
                "publication GC05 directory identity/range conflict",
            ));
        }
        used_frames = used_frames
            .checked_add(group.frame_count)
            .ok_or_else(|| invalid("publication GC05 frame count overflows"))?;
        groups.push(group);
    }
    let directory_end = stream.consumed;
    let mut metadata_end = directory_end;
    for group in &groups {
        if group.meta_offset != metadata_end {
            return Err(invalid(
                "publication GC05 metadata ranges are not contiguous after directory",
            ));
        }
        metadata_end += u64::from(group.meta_stored_len);
    }
    if used_frames != frame_count
        || metadata_end > body_end
        || (frame_count == 0 && metadata_end != body_end)
        || (frame_count > 0 && metadata_end == body_end)
    {
        return Err(invalid(
            "publication GC05 metadata/frame coverage disagrees with body",
        ));
    }
    Ok((
        PayloadSummary::Group {
            container_id,
            profile,
            frame_count,
            groups,
            directory_end,
            metadata_end,
        },
        metadata,
    ))
}

async fn read_large_summary<B: ObjectBackend + Clone>(
    stream: &mut PayloadStream<'_, B>,
) -> PackedResult<(PayloadSummary, V3OwnedPermit)> {
    let metadata = stream.budget.admit(&[(
        V3BudgetPool::Metadata,
        std::mem::size_of::<PayloadSummary>() as u64,
    )])?;
    let mut prefix = [0; 32];
    stream.read_into(&mut prefix).await?;
    if &prefix[..4] != b"LD05" {
        return Err(invalid("publication LD05 prefix identity mismatch"));
    }
    let inode = u64::from_le_bytes(prefix[4..12].try_into().unwrap());
    let chunk_id = u64::from_le_bytes(prefix[12..20].try_into().unwrap());
    let frame_count = u32::from_le_bytes(prefix[20..24].try_into().unwrap());
    let raw_total = u64::from_le_bytes(prefix[24..32].try_into().unwrap());
    if inode == 0
        || inode > i64::MAX as u64
        || frame_count == 0
        || frame_count > LD_FRAME_LIMIT
        || raw_total < u64::from(frame_count)
        || raw_total > LD_RAW_LIMIT
        || stream.consumed >= stream.reference.object_len - V3_FOOTER_LEN as u64
    {
        return Err(invalid(
            "publication LD05 inode/count/raw total exceeds limits",
        ));
    }
    Ok((
        PayloadSummary::Large {
            inode,
            chunk_id,
            frame_count,
            raw_total,
        },
        metadata,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    use anyhow::Result;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Notify;

    use super::*;
    use crate::cadapter::client::ObjectByteStream;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::wire005::{
        V3BudgetLimits, build_v3_container, decode_v3_object, encode_v3_object,
    };
    use crate::workspace_overlay::packed_v3::{
        GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedFrameInput, PackedGroupInput, SizeClass,
        SizeClassTable,
    };

    #[derive(Clone)]
    struct RecordingBackend {
        local: LocalFsBackend,
        root: std::path::PathBuf,
        ranges: Arc<Mutex<Vec<(u64, u64)>>>,
        head_count: Arc<AtomicUsize>,
        full_get_count: Arc<AtomicUsize>,
        // 0: real local stream, 1: short, 2: excess, 3: pending body.
        stream_mode: Arc<AtomicU8>,
        pending_head: Arc<AtomicBool>,
        live_streams: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        size_override: Arc<AtomicU64>,
        append_on_first_range: Arc<AtomicBool>,
    }

    impl RecordingBackend {
        fn new(path: &std::path::Path) -> Self {
            Self {
                local: LocalFsBackend::new(path),
                root: path.to_path_buf(),
                ranges: Arc::new(Mutex::new(Vec::new())),
                head_count: Arc::new(AtomicUsize::new(0)),
                full_get_count: Arc::new(AtomicUsize::new(0)),
                stream_mode: Arc::new(AtomicU8::new(0)),
                pending_head: Arc::new(AtomicBool::new(false)),
                live_streams: Arc::new(AtomicUsize::new(0)),
                entered: Arc::new(Notify::new()),
                size_override: Arc::new(AtomicU64::new(u64::MAX)),
                append_on_first_range: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    struct ActiveStream(Arc<AtomicUsize>);

    impl Drop for ActiveStream {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ObjectBackend for RecordingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
            self.local.put_object(key, data).await
        }

        async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.full_get_count.fetch_add(1, Ordering::SeqCst);
            self.local.get_object(key).await
        }

        async fn get_object_range(&self, key: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
            self.local.get_object_range(key, offset, buf).await
        }

        async fn get_object_range_stream(
            &self,
            key: &str,
            offset: u64,
            length: u64,
        ) -> Result<ObjectByteStream> {
            self.ranges.lock().unwrap().push((offset, length));
            if self.append_on_first_range.swap(false, Ordering::SeqCst) {
                let mut object = tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(self.root.join(key))
                    .await?;
                object.write_all(b"suffix").await?;
                object.flush().await?;
            }
            match self.stream_mode.load(Ordering::SeqCst) {
                1 => {
                    self.local
                        .get_object_range_stream(key, offset, length - 1)
                        .await
                }
                2 => {
                    let body = self
                        .local
                        .get_object_range_stream(key, offset, length)
                        .await?;
                    Ok(Box::pin(body.chain(futures_util::stream::once(async {
                        Ok(Bytes::from_static(b"extra"))
                    }))))
                }
                3 => {
                    self.live_streams.fetch_add(1, Ordering::SeqCst);
                    let owner = ActiveStream(self.live_streams.clone());
                    self.entered.notify_one();
                    Ok(Box::pin(futures_util::stream::once(async move {
                        let _owner = owner;
                        std::future::pending::<Result<Bytes>>().await
                    })))
                }
                _ => {
                    self.local
                        .get_object_range_stream(key, offset, length)
                        .await
                }
            }
        }

        async fn get_object_size_bounded(&self, key: &str) -> Result<Option<u64>> {
            self.head_count.fetch_add(1, Ordering::SeqCst);
            if self.pending_head.load(Ordering::SeqCst) {
                self.live_streams.fetch_add(1, Ordering::SeqCst);
                let _owner = ActiveStream(self.live_streams.clone());
                self.entered.notify_one();
                return std::future::pending().await;
            }
            let size = self.size_override.load(Ordering::SeqCst);
            if size != u64::MAX {
                return Ok(Some(size));
            }
            self.local.get_object_size_bounded(key).await
        }

        async fn get_etag(&self, key: &str) -> Result<String> {
            self.local.get_etag(key).await
        }

        async fn delete_object(&self, key: &str) -> Result<()> {
            self.local.delete_object(key).await
        }
    }

    fn frame() -> PackedFrameInput {
        PackedFrameInput {
            raw: vec![37; MAX_RANGE_BYTES + 123],
            size_class: SizeClass::Large,
            codec: 0,
            first_file_slot: 0,
            last_file_slot: 0,
        }
    }

    fn group_bytes(codec: PackedCodec) -> (Vec<u8>, Vec<V3GroupRef>) {
        let frame = frame();
        let group = PackedGroupInput {
            group_id: 7,
            parent_dir_key: [4; 32],
            metadata: GroupMeta::new(vec![GroupMetaEntry {
                name: b"file".to_vec(),
                inode: 400,
                kind: 1,
                mode: 0o100644,
                uid: 1,
                gid: 2,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: frame.raw.len() as u64,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: frame.raw.len() as u32,
                    frame_ordinal: 0,
                    raw_offset: 0,
                    raw_len: frame.raw.len() as u32,
                }],
            }])
            .unwrap()
            .encode()
            .unwrap(),
            frame_ordinals: vec![0],
            entry_count: 1,
            file_count: 1,
            layout_profile: AccessProfile::RandomSmallFile,
        };
        let built = build_v3_container(
            17,
            91,
            AccessProfile::RandomSmallFile,
            SizeClassTable {
                max_random_frame_raw_bytes: 4 * 1024 * 1024,
                ..SizeClassTable::default()
            },
            (codec, codec),
            &[group],
            &[frame],
        )
        .unwrap();
        (built.bytes, built.groups)
    }

    fn large_bytes(codec: PackedCodec) -> Vec<u8> {
        super::super::super::large_chunk::build_large_chunk(
            400,
            999,
            AccessProfile::RandomSmallFile,
            SizeClassTable {
                max_random_frame_raw_bytes: 4 * 1024 * 1024,
                ..SizeClassTable::default()
            },
            codec,
            &[frame()],
        )
        .unwrap()
        .bytes
    }

    async fn fixture(
        kind: V3ObjectKind,
        bytes: &[u8],
    ) -> (
        tempfile::TempDir,
        RecordingBackend,
        ObjectClient<RecordingBackend>,
        V3ObjectRef,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let backend = RecordingBackend::new(directory.path());
        let client = ObjectClient::new(backend.clone());
        let reference = V3ObjectRef::from_bytes("payload/object".into(), kind, bytes).unwrap();
        client.put_object(&reference.key, bytes).await.unwrap();
        (directory, backend, client, reference)
    }

    async fn verify(
        client: &ObjectClient<RecordingBackend>,
        reference: &V3ObjectRef,
        budget: &Arc<V3MountBudget>,
    ) -> PackedResult<AuthenticatedPayload> {
        authenticate_payload(
            client,
            reference,
            17,
            budget,
            PayloadLimits::default(),
            &CancellationToken::new(),
        )
        .await
    }

    fn released(budget: &V3MountBudget) {
        assert_eq!(
            budget.state().used,
            [0; 8],
            "verification leaked budget ownership"
        );
    }

    #[tokio::test]
    async fn publication_payload_streams_real_gc_and_ld_raw_zstd_and_retains_summary_owner() {
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let (bytes, expected_groups) = group_bytes(codec);
            let (_directory, backend, client, reference) =
                fixture(V3ObjectKind::GroupContainer, &bytes).await;
            let budget = V3MountBudget::defaults();
            let verified = verify(&client, &reference, &budget).await.unwrap();
            let PayloadSummary::Group {
                container_id,
                profile,
                frame_count,
                groups,
                directory_end,
                metadata_end,
            } = verified.summary()
            else {
                panic!("GC facts expected")
            };
            assert_eq!(*container_id, 91);
            assert_eq!(*profile, AccessProfile::RandomSmallFile);
            assert_eq!(*frame_count, 1);
            assert_eq!(groups, &expected_groups);
            assert_eq!(*directory_end, expected_groups[0].meta_offset);
            assert_eq!(
                *metadata_end,
                expected_groups[0].meta_offset + u64::from(expected_groups[0].meta_stored_len)
            );
            assert!(budget.state().used[V3BudgetPool::Metadata as usize] > 0);
            assert_eq!(budget.state().used[V3BudgetPool::Stored as usize], 0);
            assert_eq!(backend.head_count.load(Ordering::SeqCst), 2);
            assert_eq!(backend.full_get_count.load(Ordering::SeqCst), 0);
            {
                let ranges = backend.ranges.lock().unwrap();
                assert!(
                    ranges
                        .iter()
                        .all(|(_, length)| *length <= MAX_RANGE_BYTES as u64)
                );
                assert_eq!(
                    ranges.iter().map(|(_, length)| length).sum::<u64>(),
                    reference.object_len
                );
                if codec == PackedCodec::Raw {
                    assert!(ranges.len() > 1, "real payload must cross a range boundary");
                }
            }
            assert!(
                budget.state().peak[V3BudgetPool::Stored as usize]
                    <= (2 * MAX_RANGE_BYTES + V3_HEADER_LEN + V3_FOOTER_LEN) as u64
            );
            drop(verified);
            released(&budget);

            let bytes = large_bytes(codec);
            let (_directory, backend, client, reference) =
                fixture(V3ObjectKind::LargeData, &bytes).await;
            let verified = verify(&client, &reference, &budget).await.unwrap();
            assert!(matches!(verified.summary(), PayloadSummary::Large {
                inode: 400, chunk_id: 999, frame_count: 1, raw_total,
            } if *raw_total == (MAX_RANGE_BYTES + 123) as u64));
            assert_eq!(backend.full_get_count.load(Ordering::SeqCst), 0);
            drop(verified);
            released(&budget);
        }
    }

    #[tokio::test]
    async fn publication_payload_accepts_real_inline_only_gc_without_fd_frames() {
        let group = PackedGroupInput {
            group_id: 8,
            parent_dir_key: [4; 32],
            metadata: GroupMeta::new(vec![GroupMetaEntry {
                name: b"inline".to_vec(),
                inode: 401,
                kind: 1,
                mode: 0o100644,
                uid: 1,
                gid: 2,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 4,
                flags: crate::workspace_overlay::packed_v3::INLINE_DATA_FLAG,
                inline_data: Arc::from(&b"data"[..]),
                extents: Vec::new(),
            }])
            .unwrap()
            .encode()
            .unwrap(),
            frame_ordinals: Vec::new(),
            entry_count: 1,
            file_count: 1,
            layout_profile: AccessProfile::RandomSmallFile,
        };
        let built = build_v3_container(
            17,
            92,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            (PackedCodec::Raw, PackedCodec::Raw),
            &[group],
            &[],
        )
        .unwrap();
        let (_directory, _backend, client, reference) =
            fixture(V3ObjectKind::GroupContainer, &built.bytes).await;
        let budget = V3MountBudget::defaults();
        let verified = verify(&client, &reference, &budget).await.unwrap();
        assert!(matches!(verified.summary(), PayloadSummary::Group {
            frame_count: 0, groups, metadata_end, ..
        } if groups == &built.groups && *metadata_end == reference.object_len - V3_FOOTER_LEN as u64));
        drop(verified);
        released(&budget);
    }

    #[tokio::test]
    async fn publication_payload_rejects_actual_suffix_and_short_length_before_range() {
        let (bytes, _) = group_bytes(PackedCodec::Raw);
        let (_directory, backend, client, reference) =
            fixture(V3ObjectKind::GroupContainer, &bytes).await;
        let budget = V3MountBudget::defaults();
        let mut appended = bytes.clone();
        appended.push(9);
        for changed in [&appended[..], &bytes[..bytes.len() - 1]] {
            client.put_object(&reference.key, changed).await.unwrap();
            assert!(matches!(
                verify(&client, &reference, &budget).await,
                Err(PackedWireError::Invalid(_))
            ));
            assert!(backend.ranges.lock().unwrap().is_empty());
            released(&budget);
        }
        client.delete_object(&reference.key).await.unwrap();
        assert!(verify(&client, &reference, &budget).await.is_err());
        assert!(backend.ranges.lock().unwrap().is_empty());
        released(&budget);
    }

    #[tokio::test]
    async fn publication_payload_rejects_suffix_appended_after_initial_head() {
        let bytes = large_bytes(PackedCodec::Raw);
        let (_directory, backend, client, reference) =
            fixture(V3ObjectKind::LargeData, &bytes).await;
        backend.append_on_first_range.store(true, Ordering::SeqCst);
        let budget = V3MountBudget::defaults();
        assert!(matches!(
            verify(&client, &reference, &budget).await,
            Err(PackedWireError::Invalid(_))
        ));
        assert_eq!(backend.head_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            backend
                .ranges
                .lock()
                .unwrap()
                .iter()
                .map(|(_, length)| length)
                .sum::<u64>(),
            reference.object_len
        );
        assert_eq!(backend.full_get_count.load(Ordering::SeqCst), 0);
        released(&budget);
    }

    #[tokio::test]
    async fn publication_payload_rejects_self_consistent_replacement_under_original_reference() {
        let (bytes, _) = group_bytes(PackedCodec::Raw);
        let (_directory, _backend, client, reference) =
            fixture(V3ObjectKind::GroupContainer, &bytes).await;
        let mut body = decode_v3_object(&bytes, reference.kind, V3_MAX_BODY_BYTES)
            .unwrap()
            .to_vec();
        *body.last_mut().unwrap() ^= 1;
        let replacement = encode_v3_object(reference.kind, &body, V3_MAX_BODY_BYTES).unwrap();
        assert!(decode_v3_object(&replacement, reference.kind, V3_MAX_BODY_BYTES).is_ok());
        client
            .put_object(&reference.key, &replacement)
            .await
            .unwrap();
        let budget = V3MountBudget::defaults();
        assert!(matches!(
            verify(&client, &reference, &budget).await,
            Err(PackedWireError::HashMismatch { .. })
        ));
        released(&budget);
    }

    #[tokio::test]
    async fn publication_payload_streaming_envelope_rejection_matches_buffered_decoder() {
        let bytes = large_bytes(PackedCodec::Raw);
        // Give each corrupt object an authentic new whole-object reference so
        // these tests exercise envelope validation, not an outer SHA shortcut.
        let mutations = [
            0,
            8,
            10,
            12,
            16,
            24,
            32,
            40,
            44,
            48,
            49,
            50,
            60,
            64,
            bytes.len() - 64,
            bytes.len() - 56,
            bytes.len() - 48,
            bytes.len() - 16,
        ];
        for offset in mutations {
            let mut malformed = bytes.clone();
            malformed[offset] ^= 1;
            if offset < 60 {
                // Keep CRC valid so semantic header mutations cannot be
                // rejected solely because an unrelated checksum became stale.
                let crc = crc32c::crc32c(&malformed[..60]);
                malformed[60..64].copy_from_slice(&crc.to_le_bytes());
            }
            let (_directory, _backend, client, reference) =
                fixture(V3ObjectKind::LargeData, &malformed).await;
            assert!(
                decode_v3_object(&malformed, reference.kind, LD_BODY_LIMIT).is_err(),
                "buffered accepted mutation at {offset}"
            );
            let budget = V3MountBudget::defaults();
            assert!(
                verify(&client, &reference, &budget).await.is_err(),
                "streaming accepted mutation at {offset}"
            );
            released(&budget);
        }
    }

    #[tokio::test]
    async fn publication_payload_rejects_authenticated_gc_and_ld_prefix_inconsistency() {
        let (bytes, _) = group_bytes(PackedCodec::Raw);
        let body =
            decode_v3_object(&bytes, V3ObjectKind::GroupContainer, V3_MAX_BODY_BYTES).unwrap();
        // Invalid profile; nonzero prefix padding; zero group count; changed
        // embedded metadata offset; nonzero entry padding; frame overlap/count.
        for offset in [12, 13, 16, 24 + 8, 24 + 25, 24 + 36] {
            let mut changed = body.to_vec();
            changed[offset] = if offset == 12 {
                255
            } else {
                changed[offset] ^ 1
            };
            let malformed =
                encode_v3_object(V3ObjectKind::GroupContainer, &changed, V3_MAX_BODY_BYTES)
                    .unwrap();
            let (_directory, _backend, client, reference) =
                fixture(V3ObjectKind::GroupContainer, &malformed).await;
            let budget = V3MountBudget::defaults();
            assert!(
                verify(&client, &reference, &budget).await.is_err(),
                "GC accepted changed prefix at {offset}"
            );
            released(&budget);
        }
        let bytes = large_bytes(PackedCodec::Raw);
        let body = decode_v3_object(&bytes, V3ObjectKind::LargeData, LD_BODY_LIMIT).unwrap();
        for range in [0..4, 4..12, 20..24, 24..32] {
            let mut changed = body.to_vec();
            changed[range].fill(0);
            let malformed =
                encode_v3_object(V3ObjectKind::LargeData, &changed, LD_BODY_LIMIT).unwrap();
            let (_directory, _backend, client, reference) =
                fixture(V3ObjectKind::LargeData, &malformed).await;
            let budget = V3MountBudget::defaults();
            assert!(verify(&client, &reference, &budget).await.is_err());
            released(&budget);
        }
    }

    #[tokio::test]
    async fn publication_payload_rejects_short_and_excess_streams_and_releases_permits() {
        let bytes = large_bytes(PackedCodec::Raw);
        let (_directory, backend, client, reference) =
            fixture(V3ObjectKind::LargeData, &bytes).await;
        let budget = V3MountBudget::defaults();
        backend.stream_mode.store(1, Ordering::SeqCst);
        assert!(matches!(
            verify(&client, &reference, &budget).await,
            Err(PackedWireError::Truncated { .. })
        ));
        released(&budget);
        backend.stream_mode.store(2, Ordering::SeqCst);
        assert!(matches!(
            verify(&client, &reference, &budget).await,
            Err(PackedWireError::Invalid(_))
        ));
        released(&budget);
        assert_eq!(backend.full_get_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn publication_payload_cancel_and_closed_budget_retire_pending_head_and_body() {
        for pending_head in [false, true] {
            for close_budget in [false, true] {
                let bytes = large_bytes(PackedCodec::Raw);
                let (_directory, backend, client, reference) =
                    fixture(V3ObjectKind::LargeData, &bytes).await;
                backend.pending_head.store(pending_head, Ordering::SeqCst);
                backend.stream_mode.store(3, Ordering::SeqCst);
                let budget = V3MountBudget::defaults();
                let cancel = CancellationToken::new();
                let task_budget = budget.clone();
                let task_cancel = cancel.clone();
                let task = tokio::spawn(async move {
                    authenticate_payload(
                        &client,
                        &reference,
                        17,
                        &task_budget,
                        PayloadLimits::default(),
                        &task_cancel,
                    )
                    .await
                });
                tokio::time::timeout(Duration::from_secs(3), backend.entered.notified())
                    .await
                    .unwrap();
                assert_eq!(backend.live_streams.load(Ordering::SeqCst), 1);
                if close_budget {
                    budget.close();
                } else {
                    cancel.cancel();
                }
                let result = tokio::time::timeout(Duration::from_secs(3), task)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    result.is_err(),
                    "pending operation returned facts after termination"
                );
                assert_eq!(backend.live_streams.load(Ordering::SeqCst), 0);
                assert_eq!(
                    budget.state().closed,
                    close_budget,
                    "cancellation closed caller budget"
                );
                released(&budget);
            }
        }
    }

    #[tokio::test]
    async fn publication_payload_preflight_rejection_issues_no_remote_request() {
        let bytes = large_bytes(PackedCodec::Raw);
        let (_directory, backend, client, reference) =
            fixture(V3ObjectKind::LargeData, &bytes).await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let budget = V3MountBudget::defaults();
        assert!(
            authenticate_payload(
                &client,
                &reference,
                17,
                &budget,
                PayloadLimits::default(),
                &cancel
            )
            .await
            .is_err()
        );
        released(&budget);
        let cancel = CancellationToken::new();
        budget.close();
        assert!(
            authenticate_payload(
                &client,
                &reference,
                17,
                &budget,
                PayloadLimits::default(),
                &cancel
            )
            .await
            .is_err()
        );
        released(&budget);
        let mut limits = V3BudgetLimits::default();
        limits.bytes[V3BudgetPool::Stored as usize] = 1;
        let budget = V3MountBudget::new(limits).unwrap();
        assert!(
            authenticate_payload(
                &client,
                &reference,
                17,
                &budget,
                PayloadLimits::default(),
                &cancel
            )
            .await
            .is_err()
        );
        released(&budget);
        let budget = V3MountBudget::defaults();
        for limits in [
            PayloadLimits {
                chunk_bytes: 0,
                ..PayloadLimits::default()
            },
            PayloadLimits {
                chunk_bytes: MAX_RANGE_BYTES + 1,
                ..PayloadLimits::default()
            },
            PayloadLimits {
                max_body_bytes: 1,
                ..PayloadLimits::default()
            },
        ] {
            assert!(
                authenticate_payload(&client, &reference, 17, &budget, limits, &cancel)
                    .await
                    .is_err()
            );
            released(&budget);
        }
        assert_eq!(backend.head_count.load(Ordering::SeqCst), 0);
        assert!(backend.ranges.lock().unwrap().is_empty());
        assert_eq!(backend.full_get_count.load(Ordering::SeqCst), 0);
    }
}
