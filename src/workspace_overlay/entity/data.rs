//! Write-through file I/O for the experimental current-entity namespace.
//!
//! A file operation retains the namespace's inode lock through transport and
//! publication. Other inodes remain independent. Uploads precede visibility;
//! each bounded group publishes its inode/maps/memberships and authenticated
//! object descriptors atomically. Mount admission remains a separate qualification.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[cfg(test)]
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::cadapter::client::ObjectBackend;

use super::backend::{
    CommitOutcome, EntityBackend, EntityPlan, RowCheck, RowWrite, TransactionAuthority,
};
use super::codec;
use super::lifecycle::MaintenanceToken;
use super::model::{ChunkMap, ChunkSource, EntityInode, EntityKind, UpperInode};
use super::namespace::{EntityHandle, EntityNamespace, Prepared};
use super::ownership::{
    DurableUpload, Ownership, SliceOwnership, plan_chunk_memberships_from_rows,
};
use super::recovery::{DiscardProgress, Recovery, absent_intent_check, prepare_resize};
use super::{EntityError, Result};

pub use super::resolver::ImmutableLowerData;

pub const MAX_IO_BYTES: usize = 16 * 1024 * 1024;
const MAX_IO_CHUNKS: u64 = 64;
const MAX_READ_PIECES: usize = 4096;
const MAX_RETRIES: usize = 16;
// Even 64 chunks of hints fit the metadata row bound. Fragmented maps use
// the existing dependent-read path instead of growing the advisory cache.
const MAX_HINTED_SLICES_PER_CHUNK: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteOutcome {
    /// The selected starting offset, including the atomic append decision.
    pub offset: u64,
    /// Only the prefix whose metadata publications were acknowledged.
    pub written: usize,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DataConfig {
    chunk_size: u64,
}

/// Immutable metadata committed with the first visible reference. The object
/// key is derived from the volume/namespace/slice identity, never arbitrary input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectDescriptor {
    pub namespace_id: Uuid,
    pub slice_id: u64,
    pub size: u64,
    pub sha256: [u8; 32],
}

pub struct EntityData<B, L, O> {
    namespace: Arc<EntityNamespace<B, L>>,
    objects: Arc<O>,
    chunk_size: u64,
    config_key: Vec<u8>,
    config_bytes: Vec<u8>,
    // Only identities that nominate keys, never cached metadata or authority.
    slice_hints: moka::sync::Cache<(i64, u64), Arc<[u64]>>,
}

struct UploadedRange {
    offset: u64,
    length: usize,
    descriptor: ObjectDescriptor,
}

impl<B, L, O> EntityData<B, L, O>
where
    B: EntityBackend + 'static,
    L: ImmutableLowerData + 'static,
    O: ObjectBackend + 'static,
{
    /// Establish immutable namespace geometry before exposing file I/O. This
    /// row is checked, never incremented, by ordinary file transactions.
    pub async fn new(
        namespace: Arc<EntityNamespace<B, L>>,
        objects: Arc<O>,
        chunk_size: u64,
    ) -> Result<Arc<Self>> {
        if chunk_size == 0 || chunk_size > MAX_IO_BYTES as u64 {
            return Err(EntityError::Invalid("invalid entity chunk size".into()));
        }
        let _admission = namespace.session.admit()?;
        let config_key = [namespace.keys.prefix(), b"data-config"].concat();
        let config_bytes = codec::encode(&DataConfig { chunk_size })?;
        for _ in 0..MAX_RETRIES {
            let authority = namespace.authority().await?;
            let snapshot = namespace
                .backend
                .read(std::slice::from_ref(&config_key))
                .await?;
            let current = snapshot
                .rows
                .into_iter()
                .next()
                .ok_or_else(|| EntityError::Corrupt("missing data geometry response".into()))?;
            if current.as_ref().is_some_and(|bytes| bytes != &config_bytes) {
                return Err(EntityError::Invalid(
                    "namespace chunk geometry differs".into(),
                ));
            }
            let writes = if current.is_none() {
                vec![RowWrite::Put {
                    key: config_key.clone(),
                    value: config_bytes.clone(),
                }]
            } else {
                Vec::new()
            };
            if namespace
                .backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![RowCheck {
                            key: config_key.clone(),
                            expected: current,
                        }],
                        writes,
                    },
                )
                .await?
                == CommitOutcome::Committed
            {
                return Ok(Arc::new(Self {
                    namespace,
                    objects,
                    chunk_size,
                    config_key,
                    config_bytes,
                    slice_hints: moka::sync::Cache::new(4096),
                }));
            }
        }
        Err(EntityError::Busy)
    }

    async fn snapshot(
        &self,
        ino: i64,
        extra: Vec<Vec<u8>>,
    ) -> Result<(TransactionAuthority, Prepared<'_, B, L>)> {
        let mut keys = vec![
            self.namespace.keys.inode(ino),
            self.namespace.keys.truncate(ino),
            self.config_key.clone(),
        ];
        keys.extend(extra);
        let (authority, rows) = self.namespace.snapshot(keys).await?;
        if rows.observed(&self.config_key).and_then(Option::as_deref)
            != Some(self.config_bytes.as_slice())
        {
            return Err(EntityError::Fenced);
        }
        Ok((authority, rows))
    }

    /// Ordinary I/O observes the inode, intent and known chunk rows together.
    /// Only an actual outstanding shrink enters the existing bounded cleanup.
    async fn settled_snapshot(
        &self,
        ino: i64,
        extra: Vec<Vec<u8>>,
    ) -> Result<(TransactionAuthority, Prepared<'_, B, L>)> {
        for _ in 0..MAX_RETRIES {
            let (authority, rows) = self.snapshot(ino, extra.clone()).await?;
            self.check_failure(ino)?;
            if rows
                .observed(&self.namespace.keys.truncate(ino))
                .is_some_and(Option::is_none)
            {
                return Ok((authority, rows));
            }
            self.recover_locked(ino).await?;
        }
        Err(EntityError::Busy)
    }

    fn remember_failure(&self, ino: i64, error: &EntityError) {
        // Known rejections (including contention) publish no uncertain bytes.
        // Expiry remains typed fencing; it cannot become a clean live session.
        // Keep uncertain transport/commit outcomes and corruption sticky.
        if matches!(error, EntityError::Backend(_) | EntityError::Corrupt(_)) {
            self.namespace
                .session
                .remember_io_failure(ino, error.to_string());
        }
    }

    fn check_failure(&self, ino: i64) -> Result<()> {
        if let Some(error) = self.namespace.session.io_failure(ino) {
            return Err(EntityError::Backend(format!(
                "earlier file I/O failed: {error}"
            )));
        }
        Ok(())
    }

    fn check_range(&self, offset: u64, length: usize) -> Result<u64> {
        let end = offset
            .checked_add(length as u64)
            .ok_or(EntityError::Overflow)?;
        if length > MAX_IO_BYTES
            || end > i64::MAX as u64
            || (length != 0
                && (offset % self.chunk_size + length as u64).div_ceil(self.chunk_size)
                    > MAX_IO_CHUNKS)
        {
            return Err(EntityError::Invalid(
                "entity I/O range exceeds bound".into(),
            ));
        }
        Ok(end)
    }

    fn chunk_keys(&self, ino: i64, offset: u64, length: usize) -> Vec<Vec<u8>> {
        if length == 0 {
            return Vec::new();
        }
        (offset / self.chunk_size..=(offset + length as u64 - 1) / self.chunk_size)
            .map(|index| self.namespace.keys.chunk(ino, index))
            .collect()
    }

    fn slice_hint(ids: impl IntoIterator<Item = u64>) -> Option<Arc<[u64]>> {
        let mut slices = BTreeSet::new();
        for id in ids {
            slices.insert(id);
            if slices.len() > MAX_HINTED_SLICES_PER_CHUNK {
                return None;
            }
        }
        if slices.is_empty() {
            None
        } else {
            Some(slices.into_iter().collect::<Vec<_>>().into())
        }
    }

    fn remember_slice_hint(&self, ino: i64, index: u64, hint: Option<Arc<[u64]>>) {
        match hint {
            Some(ids) => self.slice_hints.insert((ino, index), ids),
            None => self.slice_hints.invalidate(&(ino, index)),
        }
    }

    fn read_hint_keys(&self, ino: i64, offset: u64, length: usize) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        if length != 0 {
            for index in offset / self.chunk_size..=(offset + length as u64 - 1) / self.chunk_size {
                if let Some(ids) = self.slice_hints.get(&(ino, index)) {
                    for &slice_id in ids.iter() {
                        keys.extend([
                            self.namespace.keys.slice(slice_id),
                            self.namespace.keys.owner(slice_id, ino, index),
                            self.namespace.keys.object_descriptor(slice_id),
                        ]);
                    }
                }
            }
        }
        keys
    }

    async fn inode(&self, ino: i64, rows: &mut Prepared<'_, B, L>) -> Result<EntityInode> {
        let inode = rows.inode(ino).await?;
        if inode.kind != EntityKind::File {
            return Err(EntityError::Unsupported(
                "entity file I/O requires regular inode",
            ));
        }
        // nlink == 0 is intentionally valid for an admitted, retained open handle.
        Ok(inode)
    }

    async fn chunk(
        &self,
        inode: &EntityInode,
        index: u64,
        rows: &mut Prepared<'_, B, L>,
    ) -> Result<ChunkMap> {
        rows.chunk(inode, index, self.chunk_size).await
    }

    async fn recover_locked(&self, ino: i64) -> Result<()> {
        let recovery = Recovery::new(self.namespace.backend.clone(), self.namespace.keys.clone());
        // The reference scanner admits at most 4096 chunk rows. One additional
        // step clears the intent after the final boundary/tail row.
        for _ in 0..4098 {
            let mut completed = None;
            for _ in 0..MAX_RETRIES {
                let authority = self.namespace.authority().await?;
                match recovery.truncate_step(&authority, ino).await {
                    Err(EntityError::Conflict) => continue,
                    result => {
                        completed = Some(result?);
                        break;
                    }
                }
            }
            match completed {
                Some(true) => return Ok(()),
                Some(false) => {}
                None => return Err(EntityError::Busy),
            }
        }
        Err(EntityError::Busy)
    }

    pub async fn attr(self: &Arc<Self>, handle: Arc<EntityHandle>) -> Result<EntityInode> {
        let ino = self.namespace.validate_handle(&handle, 0)?;
        let _admission = self.namespace.session.admit()?;
        let _guard = self.namespace.read_lock_inode(ino).await;
        for _ in 0..MAX_RETRIES {
            let result = async {
                let (_, mut rows) = self.snapshot(ino, Vec::new()).await?;
                let inode = self.inode(ino, &mut rows).await?;
                rows.validate_read(false).await?;
                Ok(inode)
            }
            .await;
            match result {
                Err(EntityError::Conflict) => continue,
                result => return result,
            }
        }
        Err(EntityError::Busy)
    }

    pub async fn write(
        self: &Arc<Self>,
        handle: Arc<EntityHandle>,
        offset: u64,
        data: Bytes,
        append: bool,
    ) -> Result<WriteOutcome> {
        let ino = self.namespace.validate_handle(&handle, 2)?;
        if data.len() > MAX_IO_BYTES {
            return Err(EntityError::Invalid("entity write exceeds bound".into()));
        }
        let admission = self.namespace.session.admit()?;
        // Enqueue on the shared FIFO lock before spawning; a following fsync
        // cannot overtake an already admitted write waiting on this inode.
        let guard = self.namespace.lock_inode(ino).await;
        handle.mark_mutated();
        let this = self.clone();
        tokio::spawn(async move {
            let (_handle, _admission, _guard) = (handle, admission, guard);
            let result = this.write_locked(ino, offset, data, append).await;
            if let Err(error) = &result {
                this.remember_failure(ino, error);
            }
            result
        })
        .await
        .map_err(|e| EntityError::Backend(format!("owned file write failed: {e}")))?
    }

    // These are aggregate read/plan bounds, never semantic Invalid errors or
    // uncertain backend results. Redis renders extension errors as code: detail.
    fn aggregate_metadata_bound(error: &EntityError) -> bool {
        matches!(error, EntityError::Invalid(message) if matches!(message.as_str(),
            "entity transaction exceeds row bound"
                | "entity transaction exceeds byte bound"
                | "operation read set exceeds row bound"
                | "membership batch exceeds row bound"
                | "ENTITY_INVALID: read exceeds byte bound"))
    }

    async fn write_snapshot(
        &self,
        ino: i64,
        mut keys: Vec<Vec<u8>>,
    ) -> Result<(TransactionAuthority, Prepared<'_, B, L>)> {
        loop {
            match self.settled_snapshot(ino, keys.clone()).await {
                Err(error) if keys.len() > 1 && Self::aggregate_metadata_bound(&error) => {
                    keys.truncate(keys.len() / 2);
                }
                result => return result,
            }
        }
    }

    async fn write_locked(
        &self,
        ino: i64,
        offset: u64,
        data: Bytes,
        append: bool,
    ) -> Result<WriteOutcome> {
        if !append {
            self.check_range(offset, data.len())?;
        }
        let keys = if append {
            Vec::new()
        } else {
            self.chunk_keys(ino, offset, data.len())
        };
        let (mut authority, mut initial) = self.write_snapshot(ino, keys).await?;
        let inode = self.inode(ino, &mut initial).await?;
        let offset = if append { inode.size } else { offset };
        self.check_range(offset, data.len())?;
        let chunk_keys = self.chunk_keys(ino, offset, data.len());
        if append && let Err(error) = initial.prefetch(chunk_keys.clone()).await {
            if !Self::aggregate_metadata_bound(&error) {
                return Err(error);
            }
            (authority, initial) = self.write_snapshot(ino, chunk_keys.clone()).await?;
        }
        // Empty writes still validate live authority and the observed inode.
        if data.is_empty() {
            if self
                .namespace
                .backend
                .commit(&authority, &initial.read_plan())
                .await?
                != CommitOutcome::Committed
            {
                return Err(EntityError::Conflict);
            }
            return Ok(WriteOutcome {
                offset,
                written: 0,
                size: inode.size,
            });
        }
        let mut lengths = Vec::new();
        let mut selected = 0;
        while selected < data.len() {
            let position = offset + selected as u64;
            let length = (self.chunk_size - position % self.chunk_size)
                .min((data.len() - selected) as u64) as usize;
            lengths.push(length as u64);
            selected += length;
        }
        let ownership = Ownership::new(self.namespace.backend.clone(), self.namespace.keys.clone());
        let mut reserved = None;
        for _ in 0..MAX_RETRIES {
            match ownership
                .reserve_uploads(&self.namespace.session, &authority, &lengths)
                .await
            {
                Ok(tokens) => {
                    reserved = Some(tokens);
                    break;
                }
                Err(EntityError::Conflict) => {
                    (authority, initial) = self.write_snapshot(ino, chunk_keys.clone()).await?;
                }
                Err(error) => return Err(error),
            }
        }
        let tokens = reserved.ok_or(EntityError::Busy)?;
        let mut uploaded = Vec::new();
        let mut receipts = Vec::new();
        let mut written = 0;
        let mut upload_error = None;
        for (token, length) in tokens.into_iter().zip(lengths) {
            let length = length as usize;
            let bytes = &data[written..written + length];
            let descriptor = ObjectDescriptor {
                namespace_id: self.namespace.keys.namespace_id(),
                slice_id: token.slice_id(),
                size: length as u64,
                sha256: Sha256::digest(bytes).into(),
            };
            if let Err(error) = self
                .objects
                .put_object_create_only_durable(
                    &self.namespace.keys.object(token.slice_id()),
                    bytes,
                )
                .await
            {
                upload_error = Some(EntityError::Backend(format!(
                    "entity object PUT failed: {error}"
                )));
                break;
            }
            receipts.push(token.confirm_durable_put());
            uploaded.push(UploadedRange {
                offset: offset + written as u64,
                length,
                descriptor,
            });
            written += length;
        }
        if written == 0 {
            let error = upload_error.expect("nonempty write failed before publication");
            return Err(error);
        }
        // Ordinary writes publish once. Split only a known preparation bound;
        // every smaller group starts with fresh, narrowly selected observations.
        // A failed PUT's unselected tail must not inflate a prefix read set.
        let mut first = upload_error.is_none().then_some((authority, initial));
        let mut result = WriteOutcome {
            offset,
            written: 0,
            size: inode.size,
        };
        let mut completed = 0;
        let mut group = uploaded.len();
        let mut publication_error = None;
        while completed < uploaded.len() {
            group = group.min(uploaded.len() - completed);
            let end = completed + group;
            match self
                .publish_write(
                    ino,
                    &uploaded[completed..end],
                    &receipts[completed..end],
                    first.take(),
                    group > 1,
                )
                .await
            {
                Ok(Some(size)) => {
                    result.written += uploaded[completed..end]
                        .iter()
                        .map(|range| range.length)
                        .sum::<usize>();
                    result.size = size;
                    completed = end;
                }
                Ok(None) => group /= 2,
                Err(error) => {
                    publication_error = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = upload_error.as_ref() {
            self.remember_failure(ino, error);
        }
        if let Some(error) = publication_error {
            self.remember_failure(ino, &error);
            if result.written == 0 {
                return Err(error);
            }
        }
        Ok(result)
    }

    async fn publish_write<'a>(
        &'a self,
        ino: i64,
        uploaded: &[UploadedRange],
        receipts: &[DurableUpload],
        mut first: Option<(TransactionAuthority, Prepared<'a, B, L>)>,
        can_split: bool,
    ) -> Result<Option<u64>> {
        let keys = uploaded
            .iter()
            .map(|range| {
                self.namespace
                    .keys
                    .chunk(ino, range.offset / self.chunk_size)
            })
            .collect::<Vec<_>>();
        for _ in 0..MAX_RETRIES {
            let prepared = async {
                let (authority, mut rows) = match first.take() {
                    Some(snapshot) => snapshot,
                    None => self.settled_snapshot(ino, keys.clone()).await?,
                };
                let mut inode = self.inode(ino, &mut rows).await?;
                let mut replacements = Vec::new();
                let mut hints = Vec::new();
                let mut owner_keys = Vec::new();
                let mut writes = Vec::new();
                for (range, receipt) in uploaded.iter().zip(receipts) {
                    let index = range.offset / self.chunk_size;
                    let old = self.chunk(&inode, index, &mut rows).await?;
                    for slice_id in old.upper_slices()? {
                        owner_keys.extend([
                            self.namespace.keys.slice(slice_id),
                            self.namespace.keys.owner(slice_id, ino, index),
                        ]);
                    }
                    let new = old.replace(
                        range.offset % self.chunk_size,
                        range.length as u64,
                        ChunkSource::Data {
                            slice_id: receipt.slice_id(),
                            slice_offset: 0,
                        },
                    )?;
                    hints.push((
                        index,
                        Self::slice_hint(new.spans.iter().filter_map(|span| match span.source {
                            ChunkSource::Data { slice_id, .. } => Some(slice_id),
                            _ => None,
                        })),
                    ));
                    rows.add(RowCheck {
                        key: self.namespace.keys.object_descriptor(receipt.slice_id()),
                        expected: None,
                    })?;
                    writes.extend([
                        RowWrite::Put {
                            key: self.namespace.keys.chunk(ino, index),
                            value: codec::encode(&new)?,
                        },
                        RowWrite::Put {
                            key: self.namespace.keys.object_descriptor(receipt.slice_id()),
                            value: codec::encode(&range.descriptor)?,
                        },
                    ]);
                    replacements.push((index, old, new));
                    inode.size = inode.size.max(range.offset + range.length as u64);
                }
                rows.prefetch(owner_keys).await?;
                let membership = plan_chunk_memberships_from_rows(
                    &self.namespace.keys,
                    ino,
                    &replacements,
                    rows.observations(),
                    receipts,
                )?;
                for check in membership.checks {
                    rows.add(check)?;
                }
                inode.data_version = inode
                    .data_version
                    .checked_add(1)
                    .ok_or(EntityError::Overflow)?;
                inode.mtime_ns = rows.now_ns();
                inode.ctime_ns = rows.now_ns();
                inode.mode &= !0o6000;
                inode.validate()?;
                let size = inode.size;
                writes.extend(membership.writes);
                writes.push(RowWrite::Put {
                    key: self.namespace.keys.inode(ino),
                    value: codec::encode(&UpperInode::Present(inode))?,
                });
                let plan = rows.plan_with_writes(writes);
                super::backend::validate_plan(&authority, &plan)?;
                // Redis also bounds existing values at every check/write position.
                // All publication writes have exact observed predicates, including
                // the locally known Pending reservation and new-key absence.
                let mut stored_bytes = 0;
                for check in authority.checks.iter().chain(&plan.checks) {
                    super::backend::add_bytes(
                        &mut stored_bytes,
                        &check.key,
                        check.expected.as_deref(),
                    )?;
                }
                for write in &plan.writes {
                    let old = rows.observed(write.key()).ok_or_else(|| {
                        EntityError::Corrupt("publication write lacks observation".into())
                    })?;
                    super::backend::add_bytes(&mut stored_bytes, write.key(), old.as_deref())?;
                }
                Ok::<_, EntityError>((authority, plan, size, hints))
            }
            .await;
            let (authority, plan, size, hints) = match prepared {
                Ok(prepared) => prepared,
                Err(error) if can_split && Self::aggregate_metadata_bound(&error) => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            match self.namespace.backend.commit(&authority, &plan).await {
                Ok(CommitOutcome::Committed) => {
                    for (index, hint) in hints {
                        self.remember_slice_hint(ino, index, hint);
                    }
                    return Ok(Some(size));
                }
                // A durable PUT may outlive this snapshot's deadline while the
                // same writer successfully renews. Refresh exact authority after
                // a known rejection; a truly expired/fenced session fails the
                // next snapshot. Unknown commit results are never replayed.
                Ok(CommitOutcome::Conflict) | Err(EntityError::Fenced) => {}
                Err(error) => return Err(error),
            }
        }
        Err(EntityError::Busy)
    }

    pub async fn read(
        self: &Arc<Self>,
        handle: Arc<EntityHandle>,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let ino = self.namespace.validate_handle(&handle, 4)?;
        self.check_range(offset, length)?;
        let admission = self.namespace.session.admit()?;
        let guard = self.namespace.read_lock_inode(ino).await;
        let this = self.clone();
        tokio::spawn(async move {
            let (_handle, _admission, _guard) = (handle, admission, guard);
            this.read_locked(ino, offset, length).await
        })
        .await
        .map_err(|e| EntityError::Backend(format!("owned file read failed: {e}")))?
    }

    async fn read_locked(&self, ino: i64, offset: u64, length: usize) -> Result<Vec<u8>> {
        for _ in 0..MAX_RETRIES {
            let result = match self.read_attempt(ino, offset, length).await {
                Err(error) if Self::aggregate_metadata_bound(&error) => {
                    self.read_grouped(ino, offset, length).await
                }
                result => result,
            };
            match result {
                Err(EntityError::Conflict) => continue,
                result => return result,
            }
        }
        Err(EntityError::Busy)
    }

    async fn read_grouped(&self, ino: i64, offset: u64, length: usize) -> Result<Vec<u8>> {
        // The caller retains the handle, session admission and inode lock for
        // the whole request. Keep a small common view across bounded groups:
        // every data/map mutation advances the inode's monotonic data_version,
        // and truncate/config changes must also invalidate the assembled read.
        let (_, mut anchor) = self.snapshot(ino, Vec::new()).await?;
        let inode = self.inode(ino, &mut anchor).await?;
        let length = inode.size.saturating_sub(offset).min(length as u64) as usize;
        let mut output = Vec::with_capacity(length);
        let mut group_length = length.div_ceil(2).max(1);
        while output.len() < length {
            let count = group_length.min(length - output.len());
            match self
                .read_attempt(ino, offset + output.len() as u64, count)
                .await
            {
                Err(error) if count > 1 && Self::aggregate_metadata_bound(&error) => {
                    group_length = count / 2;
                }
                result => {
                    let bytes = result?;
                    anchor.validate_read(true).await?;
                    if bytes.len() != count {
                        return Err(EntityError::Conflict);
                    }
                    output.extend_from_slice(&bytes);
                }
            }
        }
        // No partial prefix escapes on conflict or fencing, including expiry
        // after the last group. The outer retry starts with a new common view.
        anchor.validate_read(true).await?;
        Ok(output)
    }

    async fn read_attempt(&self, ino: i64, offset: u64, length: usize) -> Result<Vec<u8>> {
        let hint_keys = self.read_hint_keys(ino, offset, length);
        let mut chunk_keys = self.chunk_keys(ino, offset, length);
        chunk_keys.extend(hint_keys.iter().cloned());
        let snapshot = match self.snapshot(ino, chunk_keys).await {
            // Unused stale hints may name oversized or malformed backend rows.
            // A speculative read must not make an otherwise valid read fail.
            Err(_) if !hint_keys.is_empty() => {
                self.snapshot(ino, self.chunk_keys(ino, offset, length))
                    .await
            }
            result => result,
        };
        let mut fetch_visible_chunks = false;
        let (_, mut rows) = match snapshot {
            Err(error) if Self::aggregate_metadata_bound(&error) => {
                // A visible truncate cut can leave large tail maps awaiting
                // cleanup. Read the inode first when eager prefetch exceeds
                // its bound; invisible tail rows must not make EOF fail.
                fetch_visible_chunks = true;
                self.snapshot(ino, Vec::new()).await?
            }
            result => result?,
        };
        let inode = self.inode(ino, &mut rows).await?;
        let length = inode.size.saturating_sub(offset).min(length as u64) as usize;
        if fetch_visible_chunks {
            rows.prefetch(self.chunk_keys(ino, offset, length)).await?;
        }
        let mut output = vec![0; length];
        let mut position = offset;
        let mut pieces = Vec::new();
        let mut descriptors = BTreeMap::new();
        let mut maps = Vec::new();
        let mut ownership_keys = BTreeSet::new();
        while position < offset + length as u64 {
            let index = position / self.chunk_size;
            let count = (self.chunk_size - position % self.chunk_size)
                .min(offset + length as u64 - position);
            let map = self.chunk(&inode, index, &mut rows).await?;
            let slices = map.upper_slices()?;
            self.remember_slice_hint(ino, index, Self::slice_hint(slices.iter().copied()));
            for slice_id in slices {
                ownership_keys.extend([
                    self.namespace.keys.slice(slice_id),
                    self.namespace.keys.owner(slice_id, ino, index),
                ]);
            }
            for span in map.clip(position % self.chunk_size, count)? {
                if let ChunkSource::Data { slice_id, .. } = span.source {
                    ownership_keys.insert(self.namespace.keys.object_descriptor(slice_id));
                }
            }
            maps.push((index, count, map));
            position += count;
        }
        // Fresh maps determine the required rows. Discard unused hints so they
        // cannot consume the dependent-read budget or create false conflicts.
        rows.discard_unused_hints(&hint_keys, &ownership_keys);
        // Cold/stale hints still discover all dependencies in one atomic batch.
        rows.prefetch(ownership_keys.into_iter().collect()).await?;
        position = offset;
        for (index, count, map) in maps {
            let membership = super::ownership::plan_chunk_membership_from_rows(
                &self.namespace.keys,
                ino,
                index,
                &map,
                &map,
                rows.observations(),
                None,
            )?;
            for check in membership.checks {
                rows.add(check)?;
            }
            for span in map.clip(position % self.chunk_size, count)? {
                if pieces.len() >= MAX_READ_PIECES {
                    return Err(EntityError::Invalid(
                        "entity read fragmentation exceeds bound".into(),
                    ));
                }
                if let ChunkSource::Data { slice_id, .. } = span.source
                    && !descriptors.contains_key(&slice_id)
                {
                    let bytes = rows
                        .raw(self.namespace.keys.object_descriptor(slice_id))
                        .await?
                        .ok_or_else(|| {
                            EntityError::Corrupt("visible slice lacks object authentication".into())
                        })?;
                    let descriptor: ObjectDescriptor = codec::decode(&bytes)?;
                    let ownership: SliceOwnership = codec::decode(
                        rows.observed(&self.namespace.keys.slice(slice_id))
                            .and_then(Option::as_deref)
                            .ok_or_else(|| {
                                EntityError::Corrupt("missing validated slice owner".into())
                            })?,
                    )?;
                    if descriptor.namespace_id != self.namespace.keys.namespace_id()
                        || descriptor.slice_id != slice_id
                        || descriptor.size != ownership.size
                        || descriptor.size == 0
                        || descriptor.size > MAX_IO_BYTES as u64
                    {
                        return Err(EntityError::Corrupt(
                            "invalid entity object descriptor".into(),
                        ));
                    }
                    descriptors.insert(slice_id, descriptor);
                }
                pieces.push((index * self.chunk_size + span.offset - offset, span));
            }
            position += count;
        }
        rows.validate_read(false).await?;
        // EOF and all-hole reads are complete at this validation point.
        // There is no transport interval requiring a second validation.
        if pieces
            .iter()
            .all(|(_, span)| matches!(span.source, ChunkSource::Hole))
        {
            return Ok(output);
        }
        let mut object_pieces = BTreeMap::<_, Vec<_>>::new();
        for (destination, span) in pieces {
            let destination = usize::try_from(destination).map_err(|_| EntityError::Overflow)?;
            let count = usize::try_from(span.length).map_err(|_| EntityError::Overflow)?;
            match span.source {
                ChunkSource::Hole => {}
                ChunkSource::LowerRange {
                    binding_digest,
                    ino,
                    file_offset,
                } => {
                    if binding_digest != self.namespace.lower.binding().manifest_digest {
                        return Err(EntityError::Fenced);
                    }
                    let bytes = self
                        .namespace
                        .lower
                        .read_range(ino, file_offset, count)
                        .await?;
                    if bytes.len() != count {
                        return Err(EntityError::Corrupt(
                            "short authenticated lower read".into(),
                        ));
                    }
                    output[destination..destination + count].copy_from_slice(&bytes);
                }
                ChunkSource::Data {
                    slice_id,
                    slice_offset,
                } => {
                    object_pieces.entry(slice_id).or_default().push((
                        destination,
                        count,
                        slice_offset,
                    ));
                }
            }
        }
        // An overwrite can leave several visible fragments of one immutable
        // object. Authenticate it once and scatter its fragments, retaining
        // only one full object buffer at a time. The piece bound above also
        // bounds these request-local recipes.
        for (slice_id, pieces) in object_pieces {
            let descriptor = descriptors
                .get(&slice_id)
                .expect("validated object descriptor");
            let bytes = self.read_object(descriptor).await?;
            for (destination, count, slice_offset) in pieces {
                let start = usize::try_from(slice_offset).map_err(|_| EntityError::Overflow)?;
                let source = bytes
                    .get(start..start.checked_add(count).ok_or(EntityError::Overflow)?)
                    .ok_or_else(|| {
                        EntityError::Corrupt("slice read exceeds authenticated object".into())
                    })?;
                output[destination..destination + count].copy_from_slice(source);
            }
        }
        // Revalidate after transport too. Old objects remain protected by
        // the retained handle/admission even if authority expired meanwhile.
        rows.validate_read(true).await?;
        Ok(output)
    }

    async fn read_object(&self, descriptor: &ObjectDescriptor) -> Result<Vec<u8>> {
        let key = self.namespace.keys.object(descriptor.slice_id);
        // Probe one byte past the authenticated length: an exact-size range
        // would hide an appended suffix. Short EOF is checked below as well.
        let probe_size = descriptor
            .size
            .checked_add(1)
            .ok_or(EntityError::Overflow)?;
        let mut stream = self
            .objects
            .get_object_range_stream(&key, 0, probe_size)
            .await
            .map_err(|e| EntityError::Backend(format!("entity object GET failed: {e}")))?;
        let mut bytes = Vec::with_capacity(descriptor.size as usize);
        while let Some(part) = stream.next().await {
            let part =
                part.map_err(|e| EntityError::Backend(format!("entity object body failed: {e}")))?;
            if bytes
                .len()
                .checked_add(part.len())
                .is_none_or(|n| n > descriptor.size as usize)
            {
                return Err(EntityError::Corrupt(
                    "oversized entity object response".into(),
                ));
            }
            bytes.extend_from_slice(&part);
        }
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if bytes.len() != descriptor.size as usize || digest != descriptor.sha256 {
            return Err(EntityError::Corrupt(
                "entity object authentication failed".into(),
            ));
        }
        Ok(bytes)
    }

    pub async fn truncate(
        self: &Arc<Self>,
        handle: Arc<EntityHandle>,
        size: u64,
    ) -> Result<EntityInode> {
        let ino = self.namespace.validate_handle(&handle, 2)?;
        if size > i64::MAX as u64 {
            return Err(EntityError::Invalid(
                "entity size exceeds signed offset range".into(),
            ));
        }
        let admission = self.namespace.session.admit()?;
        let guard = self.namespace.lock_inode(ino).await;
        handle.mark_mutated();
        let this = self.clone();
        tokio::spawn(async move {
            let (_handle, _admission, _guard) = (handle, admission, guard);
            this.truncate_locked(ino, size).await
        })
        .await
        .map_err(|e| EntityError::Backend(format!("owned file truncate failed: {e}")))?
    }

    async fn truncate_locked(&self, ino: i64, size: u64) -> Result<EntityInode> {
        for _ in 0..MAX_RETRIES {
            let (authority, mut rows) = self.settled_snapshot(ino, Vec::new()).await?;
            let inode = self.inode(ino, &mut rows).await?;
            let old = rows
                .observed(&self.namespace.keys.inode(ino))
                .cloned()
                .flatten();
            let mut plan = prepare_resize(
                &self.namespace.keys,
                ino,
                old,
                &inode,
                size,
                self.chunk_size,
                rows.now_ns(),
            )?;
            for check in plan.checks.drain(..) {
                rows.add(check)?;
            }
            let updated = plan
                .writes
                .iter()
                .find_map(|write| match write {
                    RowWrite::Put { key, value } if key == &self.namespace.keys.inode(ino) => {
                        Some(value)
                    }
                    _ => None,
                })
                .ok_or_else(|| EntityError::Corrupt("resize lacks updated inode".into()))?;
            let UpperInode::Present(updated) = codec::decode(updated)? else {
                return Err(EntityError::Corrupt("resize retired its inode".into()));
            };
            match self
                .namespace
                .backend
                .commit(&authority, &rows.plan_with_writes(plan.writes))
                .await
            {
                Ok(CommitOutcome::Committed) => {
                    if size < inode.size {
                        self.recover_locked(ino).await?;
                    }
                    return Ok(updated);
                }
                Ok(CommitOutcome::Conflict) => {}
                Err(error) => {
                    if matches!(error, EntityError::Backend(_)) {
                        self.remember_failure(ino, &error);
                    }
                    return Err(error);
                }
            }
        }
        Err(EntityError::Busy)
    }

    /// Every acknowledged write already completed upload and publication.
    /// This FIFO file barrier waits for preceding operations, finishes a visible
    /// shrink, reports retained failures, and validates the configured metadata
    /// durability boundary. Redis persistence policy remains deployment policy.
    pub async fn fsync(self: &Arc<Self>, handle: Arc<EntityHandle>) -> Result<()> {
        self.sync_handle(handle, false).await
    }

    /// A clean close still waits for preceding inode operations and reports
    /// retained I/O failures. Only explicit fsync or a mutated handle needs
    /// the metadata durability barrier. The mutation flag is never cleared.
    pub async fn flush_release(self: &Arc<Self>, handle: Arc<EntityHandle>) -> Result<()> {
        self.sync_handle(handle, true).await
    }

    async fn sync_handle(self: &Arc<Self>, handle: Arc<EntityHandle>, release: bool) -> Result<()> {
        let ino = self.namespace.validate_handle(&handle, 0)?;
        let admission = self.namespace.session.admit()?;
        let guard = self.namespace.lock_inode(ino).await;
        let this = self.clone();
        tokio::spawn(async move {
            if release && !handle.needs_release_barrier() {
                let (_handle, _admission, _guard) = (handle, admission, guard);
                return this.check_failure(ino);
            }
            let (_handle, _admission, _guard) = (handle, admission, guard);
            for _ in 0..MAX_RETRIES {
                let (authority, mut rows) = this.settled_snapshot(ino, Vec::new()).await?;
                this.inode(ino, &mut rows).await?;
                rows.add(absent_intent_check(&this.namespace.keys, ino))?;
                if this
                    .namespace
                    .backend
                    .commit(&authority, &rows.read_plan())
                    .await?
                    == CommitOutcome::Committed
                {
                    return Ok(());
                }
            }
            Err(EntityError::Busy)
        })
        .await
        .map_err(|e| EntityError::Backend(format!("owned file fsync failed: {e}")))?
    }

    /// One bounded administrative cleanup step. The recovery transaction stores
    /// deletion exclusion before this owned task submits transport DELETE. A
    /// lost reply is returned without replay; a later explicit step reconciles
    /// the same permanently reserved object identity.
    pub async fn discard_step(self: &Arc<Self>, maintenance: &MaintenanceToken) -> Result<bool> {
        let this = self.clone();
        let maintenance = maintenance.clone();
        tokio::spawn(async move {
            let recovery =
                Recovery::new(this.namespace.backend.clone(), this.namespace.keys.clone());
            match recovery.discard_step(&maintenance).await? {
                DiscardProgress::Pending => Ok(false),
                DiscardProgress::Complete => Ok(true),
                DiscardProgress::ObjectDeletion(token) => {
                    if !this.objects.forbids_mutation_replay() {
                        return Err(EntityError::Unsupported(
                            "entity deletion requires nonreplaying transport",
                        ));
                    }
                    let ownership =
                        Ownership::new(this.namespace.backend.clone(), this.namespace.keys.clone());
                    ownership.validate_delete(&maintenance, &token).await?;
                    this.objects
                        .delete_object(&this.namespace.keys.object(token.slice_id()))
                        .await
                        .map_err(|e| {
                            EntityError::Backend(format!("entity object DELETE failed: {e}"))
                        })?;
                    ownership.complete_delete(&maintenance, &token).await?;
                    Ok(false)
                }
            }
        })
        .await
        .map_err(|e| EntityError::Backend(format!("owned entity discard failed: {e}")))?
    }
}

#[cfg(test)]
#[async_trait]
impl ImmutableLowerData for super::resolver::test_support::MemoryLower {
    async fn read_range(&self, ino: i64, offset: u64, length: usize) -> Result<Vec<u8>> {
        let inode = self.inodes.get(&ino).ok_or(EntityError::NotFound)?;
        let end = offset
            .checked_add(length as u64)
            .ok_or(EntityError::Overflow)?;
        if inode.kind != EntityKind::File || length > MAX_IO_BYTES || end > inode.size {
            return Err(EntityError::Invalid("test lower read outside file".into()));
        }
        Ok(test_lower_bytes(ino, offset, length))
    }
}

#[cfg(test)]
pub(crate) fn test_lower_bytes(ino: i64, offset: u64, length: usize) -> Vec<u8> {
    (offset..offset + length as u64)
        .map(|position| ((ino as u64 * 17 + position) % 251) as u8)
        .collect()
}

#[cfg(test)]
#[path = "data_tests.rs"]
mod tests;
