//! Control-plane key layout for the native write pipeline.
//!
//! Every key lives under `nb2/{volume}/` so independent volumes (and the
//! legacy `nb1`/meta namespaces) never collide. Within a volume:
//!
//! | prefix | value |
//! |---|---|
//! | `dom/{domain_id}` | BNCT kind 1 `OwnershipDomain` envelope |
//! | `reg/{namespace_id}/{object_key}` | bound `object_id` (16 bytes) |
//! | `obj/{domain_id}/{object_id}` | BNCT kind 2 `ObjectRegistration` envelope |
//! | `inv/{domain_id}/{seq:BE64}` | appended `object_id` (16 bytes) |
//! | `head/{workspace_id}` | [`HeadState`](super::records::HeadState) |
//! | `mut/{operation_id}` | BNCT kind 19 `NativeMutationResult` envelope |
//! | `ino/{workspace_id}/{inode}` | [`InodeData`](super::records::InodeData) |
//! | `ext/{workspace_id}/{inode}/{offset:BE64}` | [`NativeExtent`](super::records::NativeExtent) |
//! | `bnd/{slice_id}/{block:BE64}` | [`BlockBinding`](crate::native_base::seal::BlockBinding) (36 bytes) |
//! | `plc/{slice_id}/{block:BE64}` | [`HeadPlacement`](super::records::HeadPlacement) |
//! | `view/{workspace_id}` | BNCT kind 16 `NativeWorkspaceHead` envelope |
//! | `pubj/{operation_id}` | BNCT kind 17 `NativePublicationJournal` envelope |
//! | `drn/{operation_id}/{batch:BE64}` | BNCT kind 18 `NativeDrainBatch` envelope |
//! | `ret/{domain_id}/{seq:BE64}` | BNCT kind 3 `RetentionReceipt` envelope |
//! | `pub/{storage_view_id}` | BNCT kind 4 `PublishedRevision` envelope |
//! | `kvr/{layer_id}/{sealed_version:BE64}` | BNCT kind 7 `KvBaseRetention` envelope |
//!
//! Big-endian suffixes keep the unsigned byte-string key order (spec 02 §10)
//! equal to the numeric order, so `scan` yields extents in logical offset
//! order and inventory entries in append order.
//!
//! The registry is the GC identity (spec 01 §6): an `(namespace, object
//! key)` pair can only ever be bound to one `ObjectId`.

use crate::native_base::wire::refs::ObjectId;

/// Key builder for one volume's control plane.
#[derive(Debug, Clone)]
pub struct Keys {
    prefix: Vec<u8>,
    volume_id: [u8; 16],
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl Keys {
    pub fn new(volume_id: &[u8; 16]) -> Self {
        Keys {
            prefix: format!("nb2/{}/", hex(volume_id)).into_bytes(),
            volume_id: *volume_id,
        }
    }

    /// The volume every key built by this instance belongs to.
    pub fn volume_id(&self) -> [u8; 16] {
        self.volume_id
    }

    /// Locator for the native volume header. This key is intentionally
    /// outside the per-volume prefix because the header is what supplies the
    /// volume ID used to construct every other key.
    pub fn volume_header(namespace: &str) -> Vec<u8> {
        let mut key = b"nb2/header/".to_vec();
        key.extend_from_slice(namespace.as_bytes());
        key
    }

    fn join(&self, parts: &[&[u8]]) -> Vec<u8> {
        let mut key = self.prefix.clone();
        for part in parts {
            key.extend_from_slice(part);
        }
        key
    }

    /// Ownership domain record (BNCT kind 1).
    pub fn domain(&self, domain_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"dom/", domain_id.as_slice()])
    }

    /// Registry binding: `(namespace, object key) -> object_id`.
    pub fn registry(&self, namespace_id: &[u8; 16], object_key: &[u8]) -> Vec<u8> {
        let mut key = self.join(&[b"reg/", namespace_id.as_slice(), b"/"]);
        key.extend_from_slice(object_key);
        key
    }

    /// Object registration (BNCT kind 2), scoped to the owning domain.
    pub fn object(&self, domain_id: &[u8; 16], object_id: &ObjectId) -> Vec<u8> {
        self.join(&[b"obj/", domain_id.as_slice(), b"/", object_id.as_slice()])
    }

    /// Inventory append log entry `seq -> object_id`.
    pub fn inventory(&self, domain_id: &[u8; 16], seq: u64) -> Vec<u8> {
        let mut key = self.join(&[b"inv/", domain_id.as_slice(), b"/"]);
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Prefix for the append-only object inventory of one ownership domain.
    pub fn inventory_prefix(&self, domain_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"inv/", domain_id.as_slice(), b"/"])
    }

    /// Workspace head token.
    pub fn head(&self, workspace_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"head/", workspace_id.as_slice()])
    }

    /// Mutation result (BNCT kind 19), keyed by OperationId.
    pub fn mutation(&self, operation_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"mut/", operation_id.as_slice()])
    }

    /// Inode data record.
    pub fn inode(&self, workspace_id: &[u8; 16], inode: u64) -> Vec<u8> {
        let mut key = self.join(&[b"ino/", workspace_id.as_slice(), b"/"]);
        key.extend_from_slice(&inode.to_be_bytes());
        key
    }

    /// One extent row, keyed by logical offset.
    pub fn extent(&self, workspace_id: &[u8; 16], inode: u64, offset: u64) -> Vec<u8> {
        let mut key = self.join(&[b"ext/", workspace_id.as_slice(), b"/"]);
        key.extend_from_slice(&inode.to_be_bytes());
        key.push(b'/');
        key.extend_from_slice(&offset.to_be_bytes());
        key
    }

    /// Scan prefix for every extent row of one inode.
    pub fn extents_prefix(&self, workspace_id: &[u8; 16], inode: u64) -> Vec<u8> {
        let mut key = self.join(&[b"ext/", workspace_id.as_slice(), b"/"]);
        key.extend_from_slice(&inode.to_be_bytes());
        key.push(b'/');
        key
    }

    /// Ownership/permission attribute row of one inode.  Kept separate from
    /// the inode data row so a metadata-only mutation never rides along with a
    /// data commit (WRITE-010).
    pub fn inode_attributes(&self, workspace_id: &[u8; 16], inode: u64) -> Vec<u8> {
        let mut key = self.join(&[b"attr/", workspace_id.as_slice(), b"/"]);
        key.extend_from_slice(&inode.to_be_bytes());
        key
    }

    /// One block binding row.
    pub fn binding(&self, slice_id: &[u8; 16], block: u64) -> Vec<u8> {
        let mut key = self.join(&[b"bnd/", slice_id.as_slice(), b"/"]);
        key.extend_from_slice(&block.to_be_bytes());
        key
    }

    /// One block placement row.
    pub fn placement(&self, slice_id: &[u8; 16], block: u64) -> Vec<u8> {
        let mut key = self.join(&[b"plc/", slice_id.as_slice(), b"/"]);
        key.extend_from_slice(&block.to_be_bytes());
        key
    }

    /// Rich workspace view record (BNCT kind 16). The compact `head/` row
    /// remains byte-for-byte unchanged for the PR04 write path; lifecycle
    /// transactions update both rows atomically.
    pub fn workspace_view(&self, workspace_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"view/", workspace_id.as_slice()])
    }

    /// Publication journal (BNCT kind 17), keyed by OperationId.
    pub fn publication_journal(&self, operation_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"pubj/", operation_id.as_slice()])
    }

    /// Stored canonical request digest for publication idempotency.
    pub fn publication_operation(&self, operation_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"pop/", operation_id.as_slice()])
    }

    /// One persisted drain batch (BNCT kind 18).
    pub fn drain_batch(&self, operation_id: &[u8; 16], batch_id: u64) -> Vec<u8> {
        let mut key = self.join(&[b"drn/", operation_id.as_slice(), b"/"]);
        key.extend_from_slice(&batch_id.to_be_bytes());
        key
    }

    /// Prefix for every drain batch belonging to a publication operation.
    pub fn drain_batches_prefix(&self, operation_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"drn/", operation_id.as_slice(), b"/"])
    }

    /// Append-only retention receipt for one origin domain.
    pub fn retention_receipt(&self, domain_id: &[u8; 16], seq: u64) -> Vec<u8> {
        let mut key = self.join(&[b"ret/", domain_id.as_slice(), b"/"]);
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Prefix for every committed retention receipt of a domain.
    pub fn retention_receipts_prefix(&self, domain_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"ret/", domain_id.as_slice(), b"/"])
    }

    /// Permanent published-revision record, keyed by StorageViewId.
    pub fn published_revision(&self, storage_view_id: &[u8; 32]) -> Vec<u8> {
        self.join(&[b"pub/", storage_view_id.as_slice()])
    }

    /// Permanent P1 KV-base retention record.
    pub fn kv_base_retention(&self, layer_id: &[u8; 16], sealed_version: u64) -> Vec<u8> {
        let mut key = self.join(&[b"kvr/", layer_id.as_slice(), b"/"]);
        key.extend_from_slice(&sealed_version.to_be_bytes());
        key
    }

    /// Fork base fixed at workspace creation; fast-forward must match it.
    pub fn fork_base(&self, workspace_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"forkbase/", workspace_id.as_slice()])
    }

    /// Fork-operation idempotency row.
    pub fn fork_operation(&self, operation_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"forkop/", operation_id.as_slice()])
    }

    /// Orphan-carry operation record (ORD-010), keyed by OperationId.
    pub fn orphan_carry(&self, operation_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"oc/", operation_id.as_slice()])
    }

    /// Presence means the target workspace currently has a valid writer.
    pub fn writer_lease(&self, workspace_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"writer/", workspace_id.as_slice()])
    }

    /// Protected upload receipt of one uncommitted operation (WRITE-006).
    pub fn orphan_receipt(&self, operation_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"ufo/", operation_id.as_slice()])
    }

    /// Prefix for every protected upload receipt of the volume.
    pub fn orphan_receipts_prefix(&self) -> Vec<u8> {
        self.join(&[b"ufo/"])
    }

    /// Prefix for all object registrations belonging to one ownership domain.
    pub fn objects_prefix(&self, domain_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"obj/", domain_id.as_slice(), b"/"])
    }

    /// Domain close certificate (BNCT kind 5), written once per domain.
    pub fn close_certificate(&self, domain_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"close/", domain_id.as_slice()])
    }

    /// Cleanup batch journal (BNCT kind 6), keyed by close generation and
    /// batch number. The cleanup id is part of the value and binds retries.
    pub fn cleanup_batch(
        &self,
        domain_id: &[u8; 16],
        close_generation: u64,
        batch_number: u64,
    ) -> Vec<u8> {
        let mut key = self.join(&[b"clean/", domain_id.as_slice(), b"/"]);
        key.extend_from_slice(&close_generation.to_be_bytes());
        key.push(b'/');
        key.extend_from_slice(&batch_number.to_be_bytes());
        key
    }

    /// Prefix for all cleanup batches of a closed domain generation.
    pub fn cleanup_batches_prefix(&self, domain_id: &[u8; 16], close_generation: u64) -> Vec<u8> {
        let mut key = self.join(&[b"clean/", domain_id.as_slice(), b"/"]);
        key.extend_from_slice(&close_generation.to_be_bytes());
        key.push(b'/');
        key
    }

    /// Immutable cleanup-plan digest and batch-count binding for one cleanup
    /// operation. This prevents retrying a cleanup id with a different plan.
    pub fn cleanup_operation(&self, cleanup_id: &[u8; 16]) -> Vec<u8> {
        self.join(&[b"cleanop/", cleanup_id.as_slice()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::wire::refs::MAX_KEY_LEN;

    #[test]
    fn keys_are_volume_scoped_and_lexicographic() {
        let a = Keys::new(&[1u8; 16]);
        let b = Keys::new(&[2u8; 16]);
        assert_ne!(a.domain(&[9u8; 16]), b.domain(&[9u8; 16]));
        assert_eq!(Keys::volume_header("trial"), b"nb2/header/trial");
        assert!(a.domain(&[1u8; 16]).starts_with(b"nb2/0101"));
        // BE suffix: numeric order == byte order.
        assert!(a.inventory(&[0u8; 16], 1) < a.inventory(&[0u8; 16], 2));
        assert!(a.inventory(&[0u8; 16], 255) < a.inventory(&[0u8; 16], 256));
        assert!(a.extent(&[0u8; 16], 7, 10) < a.extent(&[0u8; 16], 7, 11));
        assert!(a.extent(&[0u8; 16], 7, u64::MAX) < a.extent(&[0u8; 16], 8, 0));
        assert!(a.retention_receipt(&[4u8; 16], 9) < a.retention_receipt(&[4u8; 16], 10));
        assert!(a.drain_batch(&[5u8; 16], 9) < a.drain_batch(&[5u8; 16], 10));
        assert!(
            a.retention_receipt(&[4u8; 16], 9)
                .starts_with(&a.retention_receipts_prefix(&[4u8; 16]))
        );
        assert!(
            a.drain_batch(&[5u8; 16], 9)
                .starts_with(&a.drain_batches_prefix(&[5u8; 16]))
        );
        assert!(
            a.cleanup_batch(&[6u8; 16], 1, 2)
                .starts_with(&a.cleanup_batches_prefix(&[6u8; 16], 1))
        );
        // Registry keys embed the full object key.
        let reg = a.registry(&[3u8; 16], b"native-base/v3/v/k/o/h.brfcl");
        assert!(reg.starts_with(&a.prefix));
        assert!(reg.ends_with(b"h.brfcl"));
        assert!(reg.len() < MAX_KEY_LEN + 64);
    }
}
