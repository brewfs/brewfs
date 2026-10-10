//! Independently authenticated, bounded wire-005 frame descriptor pages.

use super::{V3_FOOTER_LEN, V3_HEADER_LEN, V3ObjectKind, V3ObjectRef, encode_v3_object};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError, Reader, Writer};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, PackedCodec, PackedFrameDescriptor, SizeClass, SizeClassTable,
};

const MAX_PAGE_FRAMES: usize = 4096;
const MAX_PAGE_BODY: usize = 256 * 1024;
const RECORD_LEN: usize = 48;
const PREFIX_LEN: usize = 80;

#[derive(Debug)]
pub(super) struct BudgetedFrame {
    pub raw: Vec<u8>,
    pub coverage: Option<crate::cadapter::read_observer::RawLease>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3FrameDirectoryPage {
    pub container_digest: [u8; 32],
    pub container_len: u64,
    pub profile: AccessProfile,
    pub size_classes: SizeClassTable,
    pub frame_policy: super::V3FramePolicy,
    pub first_ordinal: u32,
    pub frames: Vec<PackedFrameDescriptor>,
}

impl V3FrameDirectoryPage {
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        self.validate()?;
        let mut w = Writer::default();
        w.bytes(b"FD06");
        w.bytes(&self.container_digest);
        w.u64(self.container_len);
        w.u8(self.profile as u8);
        w.u8(self.frame_policy as u8);
        w.bytes(&[0; 2]);
        w.u64(self.size_classes.min_frame_raw_bytes);
        w.u64(self.size_classes.max_random_frame_raw_bytes);
        w.u64(self.size_classes.max_sequential_frame_raw_bytes);
        w.u32(self.first_ordinal);
        w.u32(self.frames.len() as u32);
        for frame in &self.frames {
            w.u32(frame.frame_ordinal);
            w.u8(frame.size_class as u8);
            w.u8(frame.codec);
            w.bytes(&[0; 2]);
            w.u64(frame.object_offset);
            w.u32(frame.stored_len);
            w.u32(frame.raw_len);
            w.u32(frame.first_file_slot);
            w.u32(frame.last_file_slot);
            w.bytes(&frame.frame_digest);
        }
        encode_v3_object(V3ObjectKind::FrameDirectory, &w.finish(), MAX_PAGE_BODY)
    }

    /// `reference` must come from a previously authenticated manifest/page.
    /// Recomputing the descriptor's own envelope/footer is not sufficient.
    pub fn decode(reference: &V3ObjectRef, bytes: &[u8]) -> PackedResult<Self> {
        if reference.kind != V3ObjectKind::FrameDirectory {
            return Err(PackedWireError::Invalid(
                "wire 005 descriptor ref has wrong kind".into(),
            ));
        }
        let body = reference.verify(bytes, MAX_PAGE_BODY)?;
        let mut r = Reader::new(body);
        if r.take(4)? != b"FD06" {
            return Err(PackedWireError::UnsupportedFormat(
                "wire 005 frame directory payload mismatch".into(),
            ));
        }
        let container_digest = r.array::<32>()?;
        let container_len = r.u64()?;
        let profile = AccessProfile::from_u8(r.u8()?)
            .map_err(|_| PackedWireError::Invalid("invalid wire 005 frame profile".into()))?;
        let frame_policy = super::V3FramePolicy::from_u8(r.u8()?)?;
        r.skip_zeroes(2)?;
        let size_classes = SizeClassTable {
            min_frame_raw_bytes: r.u64()?,
            max_random_frame_raw_bytes: r.u64()?,
            max_sequential_frame_raw_bytes: r.u64()?,
        };
        let first_ordinal = r.u32()?;
        let count = r.u32()? as usize;
        if count == 0 || count > MAX_PAGE_FRAMES || body.len() != PREFIX_LEN + count * RECORD_LEN {
            return Err(PackedWireError::Invalid(
                "wire 005 frame count exceeds exact page budget".into(),
            ));
        }
        let mut frames = Vec::with_capacity(count);
        for _ in 0..count {
            let frame_ordinal = r.u32()?;
            let size_class = SizeClass::from_u8(r.u8()?).map_err(|_| {
                PackedWireError::Invalid("invalid wire 005 frame size class".into())
            })?;
            let codec = r.u8()?;
            r.skip_zeroes(2)?;
            frames.push(PackedFrameDescriptor {
                frame_ordinal,
                size_class,
                codec,
                object_offset: r.u64()?,
                stored_len: r.u32()?,
                raw_len: r.u32()?,
                first_file_slot: r.u32()?,
                last_file_slot: r.u32()?,
                frame_digest: r.array::<16>()?,
            });
        }
        let page = Self {
            container_digest,
            container_len,
            profile,
            size_classes,
            frame_policy,
            first_ordinal,
            frames,
        };
        page.validate()?;
        Ok(page)
    }

    pub async fn read<B: ObjectBackend + Clone>(
        client: &ObjectClient<B>,
        reference: &V3ObjectRef,
        container_digest: [u8; 32],
    ) -> PackedResult<Self> {
        super::read_v3_page_validated(client, reference, MAX_PAGE_BODY, |bytes| {
            let page = Self::decode(reference, &bytes)?;
            if page.container_digest != container_digest {
                return Err(PackedWireError::Invalid(
                    "wire 005 descriptor page belongs to another container".into(),
                ));
            }
            Ok(page)
        })
        .await
    }

    /// Strict demand fetch: the page/ref must already be authenticated by the
    /// manifest chain. No full-container materialization or adjacent-frame GET.
    pub async fn read_frame<B: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<B>,
        container: &V3ObjectRef,
        ordinal: u32,
        allocation_limit: usize,
    ) -> PackedResult<Vec<u8>> {
        use sha2::{Digest, Sha256};
        self.validate()?;
        super::validate_key(&container.key)?;
        if !matches!(
            container.kind,
            V3ObjectKind::GroupContainer | V3ObjectKind::LargeData
        ) || container.digest != self.container_digest
            || container.object_len != self.container_len
        {
            return Err(PackedWireError::Invalid(
                "wire 005 frame source disagrees with authenticated container".into(),
            ));
        }
        let index = ordinal
            .checked_sub(self.first_ordinal)
            .ok_or_else(|| PackedWireError::Invalid("wire 005 frame precedes page".into()))?
            as usize;
        let frame = self
            .frames
            .get(index)
            .ok_or_else(|| PackedWireError::Invalid("wire 005 frame lies after page".into()))?;
        let allocation = (frame.stored_len as usize)
            .checked_add(frame.raw_len as usize)
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("wire 005 frame allocation overflows".into())
            })?;
        if allocation > allocation_limit {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 frame stored/raw allocation exceeds budget".into(),
            ));
        }
        let class = if container.kind == V3ObjectKind::LargeData {
            crate::cadapter::read_observer::ReadClass::ExternalPayload
        } else {
            crate::cadapter::read_observer::ReadClass::PackedPayload
        };
        client
            .typed_exact(
                class,
                &container.key,
                frame.object_offset,
                u64::from(frame.stored_len),
                allocation_limit as u64,
                |stored| {
                    let hash_timer = client.measure_read_work(
                        class,
                        crate::cadapter::read_observer::ReadWork::Authentication,
                    );
                    let digest: [u8; 16] = Sha256::digest(&stored)[..16].try_into().unwrap();
                    drop(hash_timer);
                    if digest != frame.frame_digest {
                        return Err(super::observer_validation_error(
                            PackedWireError::HashMismatch {
                                what: "wire 005 authenticated frame",
                                expected: hex::encode(frame.frame_digest),
                                computed: hex::encode(digest),
                            },
                        ));
                    }
                    let _decode_timer = client
                        .measure_read_work(class, crate::cadapter::read_observer::ReadWork::Decode);
                    super::super::codec::decode_block(
                        PackedCodec::from_u8(frame.codec)
                            .map_err(super::observer_validation_error)?,
                        &stored,
                        frame.raw_len as usize,
                        super::super::remote::MAX_PACKED_STREAM_RANGE_BYTES as usize,
                        frame.raw_len as usize,
                    )
                    .map_err(|error| {
                        (
                            crate::cadapter::read_observer::FailureClass::Decode,
                            error.into(),
                        )
                    })
                },
            )
            .await
            .map_err(super::observer_backend_error)
    }

    pub(super) async fn read_frame_owned<B: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<B>,
        container: &V3ObjectRef,
        ordinal: u32,
        budget: &std::sync::Arc<super::V3MountBudget>,
    ) -> PackedResult<super::budget::V3Owned<BudgetedFrame>> {
        self.read_frame_owned_observed(client, container, ordinal, budget, None)
            .await
    }

    pub(super) async fn read_frame_owned_observed<B: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<B>,
        container: &V3ObjectRef,
        ordinal: u32,
        budget: &std::sync::Arc<super::V3MountBudget>,
        delivery: Option<std::sync::Arc<crate::cadapter::read_observer::OperationDelivery>>,
    ) -> PackedResult<super::budget::V3Owned<BudgetedFrame>> {
        use super::V3BudgetPool;
        let index = ordinal
            .checked_sub(self.first_ordinal)
            .ok_or_else(|| PackedWireError::Invalid("frame precedes descriptor page".into()))?
            as usize;
        let frame = self
            .frames
            .get(index)
            .ok_or_else(|| PackedWireError::Invalid("frame exceeds descriptor page".into()))?;
        let workspace =
            super::super::codec::decode_workspace_bytes(PackedCodec::from_u8(frame.codec)?)?;
        // Response stream chunk and accumulation can coexist, including test
        // backends whose streaming default materializes the entire range.
        // G06's two raw-coverage bitmaps share this frame's lifecycle/budget.
        let tracking = if delivery.is_some() {
            crate::cadapter::read_observer::RawCoverage::required_tracking_bytes(u64::from(
                frame.raw_len,
            ))
            .map_err(|error| PackedWireError::LimitExceeded(error.to_string()))?
        } else {
            0
        };
        let mut permit = budget.admit(&[
            (V3BudgetPool::Stored, u64::from(frame.stored_len) * 2),
            (V3BudgetPool::Raw, u64::from(frame.raw_len)),
            (V3BudgetPool::Workspace, workspace as u64 + tracking),
            (V3BudgetPool::Control, 2048),
        ])?;
        let raw = self
            .read_frame(
                client,
                container,
                ordinal,
                frame.stored_len as usize + frame.raw_len as usize,
            )
            .await?;
        permit.shrink(V3BudgetPool::Stored, 0)?;
        permit.shrink(V3BudgetPool::Workspace, tracking)?;
        permit.shrink(V3BudgetPool::Raw, raw.capacity() as u64)?;
        let coverage = delivery
            .map(|delivery| {
                use crate::cadapter::read_observer::{RawLease, ReadClass};
                let class = if container.kind == V3ObjectKind::LargeData {
                    ReadClass::ExternalPayload
                } else {
                    ReadClass::PackedPayload
                };
                let context = client.read_context(class).ok_or_else(|| {
                    anyhow::anyhow!("raw delivery token requires a classified source client")
                })?;
                RawLease::new(u64::from(frame.raw_len), tracking, delivery, context)
            })
            .transpose()
            .map_err(|error| PackedWireError::LimitExceeded(error.to_string()))?;
        Ok(super::budget::V3Owned::new(
            BudgetedFrame { raw, coverage },
            permit,
        ))
    }

    fn validate(&self) -> PackedResult<()> {
        self.size_classes
            .validate()
            .map_err(|_| PackedWireError::Invalid("invalid wire 005 size table".into()))?;
        if self.container_len > (V3_HEADER_LEN + super::V3_MAX_BODY_BYTES + V3_FOOTER_LEN) as u64
            || self.container_digest == [0; 32]
            || self.container_len < (V3_HEADER_LEN + V3_FOOTER_LEN) as u64
            || self.frames.is_empty()
            || self.frames.len() > MAX_PAGE_FRAMES
            || self.size_classes.min_frame_raw_bytes > 256 * 1024
            || self.size_classes.max_random_frame_raw_bytes > 4 * 1024 * 1024
            || self.size_classes.max_sequential_frame_raw_bytes > 8 * 1024 * 1024
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 frame directory exceeds identity/profile/count limits".into(),
            ));
        }
        super::V3BuildPolicy {
            frames: self.frame_policy,
            inline_data: false,
            p90: None,
        }
        .select(1, self.profile, self.size_classes)?;
        let mut previous_end = V3_HEADER_LEN as u64;
        for (index, frame) in self.frames.iter().enumerate() {
            let ordinal = self
                .first_ordinal
                .checked_add(index as u32)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("wire 005 frame ordinal overflows".into())
                })?;
            let end = frame
                .object_offset
                .checked_add(u64::from(frame.stored_len))
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("wire 005 frame range overflows".into())
                })?;
            let maximum = self
                .frame_policy
                .target()
                .unwrap_or(match frame.size_class {
                    SizeClass::Tiny => 256 * 1024,
                    SizeClass::Small => 1024 * 1024,
                    SizeClass::Medium | SizeClass::Large => match self.profile {
                        AccessProfile::RandomSmallFile | AccessProfile::Mixed => {
                            self.size_classes.max_random_frame_raw_bytes
                        }
                        AccessProfile::SequentialSmallFile => {
                            self.size_classes.max_sequential_frame_raw_bytes
                        }
                    },
                });
            let codec = PackedCodec::from_u8(frame.codec)?;
            if frame.frame_ordinal != ordinal
                || frame.object_offset < previous_end
                || end > self.container_len - V3_FOOTER_LEN as u64
                || frame.raw_len == 0
                || frame.stored_len == 0
                || u64::from(frame.raw_len) > maximum
                || u64::from(frame.stored_len) > super::super::remote::MAX_PACKED_STREAM_RANGE_BYTES
                || frame.first_file_slot > frame.last_file_slot
                || (codec == PackedCodec::Raw && frame.stored_len != frame.raw_len)
            {
                return Err(PackedWireError::Invalid(
                    "wire 005 descriptor ordinal/range/class/codec mismatch".into(),
                ));
            }
            previous_end = end;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn raw_coverage_uses_the_source_engine_phase_and_origin() {
        use crate::cadapter::localfs::LocalFsBackend;
        use crate::cadapter::read_observer::{
            Engine, Ledger, Origin, Phase, ReadClass, ReadContext, ReadObserver,
        };
        use sha2::{Digest, Sha256};
        use std::sync::Arc;
        let root = tempfile::tempdir().unwrap();
        for (engine, phase, origin, kind) in [
            (
                Engine::Native,
                Phase::Startup,
                Origin::Prefetch,
                V3ObjectKind::GroupContainer,
            ),
            (
                Engine::PackedV3,
                Phase::Runtime,
                Origin::Demand,
                V3ObjectKind::LargeData,
            ),
        ] {
            let observer = Arc::new(ReadObserver::default());
            let client = ObjectClient::new(LocalFsBackend::new(root.path())).with_read_observer(
                observer.clone(),
                engine,
                phase,
                origin,
            );
            let raw = b"scoped frame";
            let object = encode_v3_object(kind, raw, 1024).unwrap();
            let container = V3ObjectRef::from_bytes("raw-context".into(), kind, &object).unwrap();
            client.put_object(&container.key, &object).await.unwrap();
            let page = V3FrameDirectoryPage {
                container_digest: container.digest,
                container_len: container.object_len,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                frame_policy: Default::default(),
                first_ordinal: 0,
                frames: vec![PackedFrameDescriptor {
                    frame_ordinal: 0,
                    object_offset: V3_HEADER_LEN as u64,
                    stored_len: raw.len() as u32,
                    raw_len: raw.len() as u32,
                    first_file_slot: 0,
                    last_file_slot: 0,
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    frame_digest: Sha256::digest(raw)[..16].try_into().unwrap(),
                }],
            };
            let class = if kind == V3ObjectKind::LargeData {
                ReadClass::ExternalPayload
            } else {
                ReadClass::PackedPayload
            };
            let tag = ReadContext {
                engine,
                phase,
                origin,
                class,
            };
            let guard = observer.start(
                Ledger::LogicalOperation,
                ReadContext {
                    class: ReadClass::LogicalRead,
                    ..tag
                },
                raw.len() as u64,
            );
            let budget = super::super::V3MountBudget::defaults();
            let mut frame = page
                .read_frame_owned_observed(&client, &container, 0, &budget, guard.delivery_token())
                .await
                .unwrap();
            let coverage = frame.value_mut().coverage.as_mut().unwrap();
            coverage.request(0, raw.len() as u64).unwrap();
            coverage.copied(0, raw.len() as u64).unwrap();
            guard.deliver(raw.len() as u64);
            drop(frame);
            let observed = observer.snapshot();
            assert_eq!(observed.raw.len(), 1);
            assert_eq!(observed.raw[&tag].decoded_raw, raw.len() as u64);
            assert_eq!(observed.raw[&tag].delivered_union, raw.len() as u64);
            assert_eq!(budget.state().used, [0; 8]);
        }
    }
    #[tokio::test]
    async fn budget_owned_frame_backend_failure_and_cancellation_release_full_bundle() {
        use crate::cadapter::client::{ObjectBackend, ObjectByteStream};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        #[derive(Clone)]
        struct Blocked {
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
            fail: Arc<AtomicBool>,
        }
        #[async_trait::async_trait]
        impl ObjectBackend for Blocked {
            async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
                unreachable!()
            }
            async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
                unreachable!()
            }
            async fn get_object_range(
                &self,
                _: &str,
                _: u64,
                _: &mut [u8],
            ) -> anyhow::Result<usize> {
                unreachable!()
            }
            async fn get_object_range_stream(
                &self,
                _: &str,
                _: u64,
                _: u64,
            ) -> anyhow::Result<ObjectByteStream> {
                self.entered.notify_one();
                if !self.fail.load(Ordering::SeqCst) {
                    self.release.notified().await;
                }
                anyhow::bail!("injected transport failure")
            }
            async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
                unreachable!()
            }
            async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
                unreachable!()
            }
        }
        let backend = Blocked {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            fail: Arc::new(AtomicBool::new(false)),
        };
        let budget = super::super::V3MountBudget::defaults();
        let page = page();
        let container = V3ObjectRef {
            key: "blocked".into(),
            kind: V3ObjectKind::GroupContainer,
            object_len: page.container_len,
            digest: page.container_digest,
        };
        let task_budget = budget.clone();
        let task_page = page.clone();
        let task_container = container.clone();
        let task_client = ObjectClient::new(backend.clone());
        let task = tokio::spawn(async move {
            task_page
                .read_frame_owned(&task_client, &task_container, 9, &task_budget)
                .await
        });
        backend.entered.notified().await;
        assert_eq!(
            budget.state().used[super::super::V3BudgetPool::Stored as usize],
            512
        );
        assert_eq!(
            budget.state().used[super::super::V3BudgetPool::Raw as usize],
            256
        );
        task.abort();
        let _ = task.await;
        assert_eq!(budget.state().used, [0; 8]);
        backend.fail.store(true, Ordering::SeqCst);
        assert!(
            page.read_frame_owned(&ObjectClient::new(backend), &container, 9, &budget)
                .await
                .is_err()
        );
        assert_eq!(budget.state().used, [0; 8]);
    }

    fn page() -> V3FrameDirectoryPage {
        V3FrameDirectoryPage {
            container_digest: [7; 32],
            container_len: V3_HEADER_LEN as u64 + 512 + V3_FOOTER_LEN as u64,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            frame_policy: Default::default(),
            first_ordinal: 9,
            frames: (0..2)
                .map(|i| PackedFrameDescriptor {
                    frame_ordinal: 9 + i,
                    object_offset: V3_HEADER_LEN as u64 + u64::from(i) * 256,
                    stored_len: 256,
                    raw_len: 256,
                    first_file_slot: i,
                    last_file_slot: i,
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    frame_digest: [i as u8; 16],
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn authenticated_frame_read_validates_stored_hash_before_decoding() {
        use crate::cadapter::localfs::LocalFsBackend;
        use sha2::{Digest, Sha256};
        let root = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(root.path()));
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let raw = b"independent payload";
            let stored = super::super::super::codec::encode_block(codec, raw, 1024).unwrap();
            let bytes = encode_v3_object(V3ObjectKind::GroupContainer, &stored, 1024).unwrap();
            let container =
                V3ObjectRef::from_bytes("container".into(), V3ObjectKind::GroupContainer, &bytes)
                    .unwrap();
            client.put_object(&container.key, &bytes).await.unwrap();
            let page = V3FrameDirectoryPage {
                container_digest: container.digest,
                container_len: container.object_len,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                frame_policy: Default::default(),
                first_ordinal: 0,
                frames: vec![PackedFrameDescriptor {
                    frame_ordinal: 0,
                    object_offset: V3_HEADER_LEN as u64,
                    stored_len: stored.len() as u32,
                    raw_len: raw.len() as u32,
                    first_file_slot: 0,
                    last_file_slot: 0,
                    size_class: SizeClass::Tiny,
                    codec: codec as u8,
                    frame_digest: Sha256::digest(&stored)[..16].try_into().unwrap(),
                }],
            };
            let page_bytes = page.encode().unwrap();
            let page_ref = V3ObjectRef::from_bytes(
                "directory".into(),
                V3ObjectKind::FrameDirectory,
                &page_bytes,
            )
            .unwrap();
            let authenticated = V3FrameDirectoryPage::decode(&page_ref, &page_bytes).unwrap();
            assert_eq!(
                authenticated
                    .read_frame(&client, &container, 0, 1024)
                    .await
                    .unwrap(),
                raw
            );
            assert!(matches!(
                authenticated.read_frame(&client, &container, 0, 1).await,
                Err(PackedWireError::LimitExceeded(_))
            ));
            let mut corrupt = bytes;
            corrupt[V3_HEADER_LEN] ^= 1;
            client.put_object(&container.key, &corrupt).await.unwrap();
            assert!(matches!(
                authenticated.read_frame(&client, &container, 0, 1024).await,
                Err(PackedWireError::HashMismatch { .. })
            ));
            assert!(
                authenticated
                    .read_frame(&client, &container, 1, 1024)
                    .await
                    .is_err()
            );
        }
    }

    #[test]
    fn authenticated_descriptor_page_roundtrip_and_substitution_rejection() {
        let page = page();
        let bytes = page.encode().unwrap();
        let reference =
            V3ObjectRef::from_bytes("directory/one".into(), V3ObjectKind::FrameDirectory, &bytes)
                .unwrap();
        assert_eq!(
            V3FrameDirectoryPage::decode(&reference, &bytes).unwrap(),
            page
        );
        let mut replacement = page.clone();
        replacement.frames[0].frame_digest = [8; 16];
        assert!(matches!(
            V3FrameDirectoryPage::decode(&reference, &replacement.encode().unwrap()),
            Err(PackedWireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn descriptor_builder_rejects_noncanonical_or_out_of_profile_claims() {
        let original = page();
        for field in 0..8 {
            let mut page = original.clone();
            match field {
                0 => page.frames[1].frame_ordinal = 9,
                1 => page.frames[1].object_offset -= 1,
                2 => page.frames[0].raw_len = 0,
                3 => page.frames[0].raw_len = 1024 * 1024,
                4 => page.frames[0].codec = 2,
                5 => page.frames[0].stored_len = 0,
                6 => page.first_ordinal = u32::MAX,
                _ => page.frames[0].first_file_slot = 3,
            }
            assert!(page.encode().is_err(), "field {field}");
        }
    }
}
