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
        assert!(a.domain(&[1u8; 16]).starts_with(b"nb2/0101"));
        // BE suffix: numeric order == byte order.
        assert!(a.inventory(&[0u8; 16], 1) < a.inventory(&[0u8; 16], 2));
        assert!(a.inventory(&[0u8; 16], 255) < a.inventory(&[0u8; 16], 256));
        assert!(a.extent(&[0u8; 16], 7, 10) < a.extent(&[0u8; 16], 7, 11));
        assert!(a.extent(&[0u8; 16], 7, u64::MAX) < a.extent(&[0u8; 16], 8, 0));
        // Registry keys embed the full object key.
        let reg = a.registry(&[3u8; 16], b"native-base/v3/v/k/o/h.brfcl");
        assert!(reg.starts_with(&a.prefix));
        assert!(reg.ends_with(b"h.brfcl"));
        assert!(reg.len() < MAX_KEY_LEN + 64);
    }
}
