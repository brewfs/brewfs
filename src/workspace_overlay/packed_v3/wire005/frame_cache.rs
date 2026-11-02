//! Optional mount-owned retention of fully authenticated decoded frames.
//!
//! The entity mount supplies the storage and shares its budget with upper
//! objects. Other readers have no retained frame cache. No request, permission,
//! lease, delivery receipt or in-flight pipeline permit is retained here.

use std::sync::Arc;

use super::{V3FramePolicy, V3ObjectRef};
use crate::chunk::read_plan::ReadGeneration;
use crate::workspace_overlay::packed_v3::{AccessProfile, PackedFrameDescriptor, SizeClassTable};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct FrameCacheKey(pub(crate) [u8; 32]);

impl FrameCacheKey {
    /// Hash the entire authenticated route and decoding contract, excluding the
    /// logical file/range so different files sharing a frame can reuse it.
    pub(super) fn new(
        generation: ReadGeneration,
        container: &V3ObjectRef,
        frame: &PackedFrameDescriptor,
        profile: AccessProfile,
        classes: SizeClassTable,
        policy: V3FramePolicy,
    ) -> Self {
        let mut identity = blake3::Hasher::new();
        identity.update(b"brewfs-entity-decoded-frame-v1");
        identity.update(&generation.lower_snapshot);
        identity.update(&generation.workspace_head_epoch.to_le_bytes());
        identity.update(&generation.workspace_mutation_sequence.to_le_bytes());
        identity.update(&(container.key.len() as u64).to_le_bytes());
        identity.update(container.key.as_bytes());
        identity.update(&container.digest);
        identity.update(&container.object_len.to_le_bytes());
        identity.update(&[container.kind as u8, profile as u8, policy as u8]);
        identity.update(&classes.min_frame_raw_bytes.to_le_bytes());
        identity.update(&classes.max_random_frame_raw_bytes.to_le_bytes());
        identity.update(&classes.max_sequential_frame_raw_bytes.to_le_bytes());
        identity.update(&frame.frame_ordinal.to_le_bytes());
        identity.update(&frame.object_offset.to_le_bytes());
        identity.update(&frame.stored_len.to_le_bytes());
        identity.update(&frame.raw_len.to_le_bytes());
        identity.update(&frame.first_file_slot.to_le_bytes());
        identity.update(&frame.last_file_slot.to_le_bytes());
        identity.update(&[frame.size_class as u8, frame.codec]);
        identity.update(&frame.frame_digest);
        Self(identity.finalize().into())
    }
}

pub(crate) trait DecodedFrameCache: Send + Sync {
    fn get(&self, key: &FrameCacheKey) -> Option<Arc<[u8]>>;
    /// Only a successful exact-body authentication and decode may insert.
    fn insert(&self, key: FrameCacheKey, bytes: &[u8]);
    /// Clear residency and permanently reject subsequent fills.
    fn close(&self);
}

pub(super) enum ReadFrame {
    Demand(Arc<super::V3Owned<super::pipeline::V3SharedFrame>>),
    Cached {
        bytes: Arc<[u8]>,
        // Bound each active Arc pin through Control admission. The shared
        // bytes belong to the retained cache budget, not a new Raw decode
        // allocation. Pins may outlive eviction, just like upper cache reads.
        _permit: super::V3OwnedPermit,
    },
}

impl ReadFrame {
    pub(super) fn bytes(&self) -> &[u8] {
        match self {
            Self::Demand(frame) => &frame.raw,
            Self::Cached { bytes, .. } => bytes,
        }
    }

    pub(super) fn coverage(
        &self,
    ) -> Option<&Arc<crate::cadapter::read_observer::SharedRawCoverage>> {
        match self {
            Self::Demand(frame) => frame.coverage.as_ref(),
            // A hit performs no new fetch/decode. Never reattribute a prior
            // operation's decoded bytes or delivery receipts to this read.
            Self::Cached { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::packed_v3::{SizeClass, wire005::V3ObjectKind};

    #[test]
    fn frame_cache_identity_separates_route_generation_and_decode_contract() {
        let generation = ReadGeneration {
            lower_snapshot: [1; 32],
            workspace_head_epoch: 2,
            workspace_mutation_sequence: 3,
        };
        let container = V3ObjectRef {
            key: "container".into(),
            kind: V3ObjectKind::GroupContainer,
            object_len: 8192,
            digest: [2; 32],
        };
        let frame = PackedFrameDescriptor {
            frame_ordinal: 0,
            object_offset: 4096,
            stored_len: 64,
            raw_len: 128,
            first_file_slot: 0,
            last_file_slot: 1,
            size_class: SizeClass::Tiny,
            codec: 1,
            frame_digest: [3; 16],
        };
        let key = |g, c: &V3ObjectRef, f: &PackedFrameDescriptor| {
            FrameCacheKey::new(
                g,
                c,
                f,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                V3FramePolicy::default(),
            )
        };
        let base = key(generation, &container, &frame);
        for change in 0..15 {
            let mut g = generation;
            let mut c = container.clone();
            let mut f = frame.clone();
            match change {
                0 => g.lower_snapshot[0] ^= 1,
                1 => g.workspace_head_epoch += 1,
                2 => g.workspace_mutation_sequence += 1,
                3 => c.key.push('x'),
                4 => c.digest[0] ^= 1,
                5 => c.object_len += 1,
                6 => c.kind = V3ObjectKind::LargeData,
                7 => f.frame_ordinal += 1,
                8 => f.object_offset += 1,
                9 => f.stored_len += 1,
                10 => f.raw_len += 1,
                11 => f.codec ^= 1,
                12 => f.frame_digest[0] ^= 1,
                13 => f.first_file_slot += 1,
                _ => f.last_file_slot += 1,
            }
            assert_ne!(base, key(g, &c, &f), "identity field {change}");
        }
    }
}
