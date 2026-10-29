//! One Redis hash slot per volume; raw names are injectively hex encoded.

use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct EntityKeys {
    volume_id: Uuid,
    namespace_id: Uuid,
    volume: Vec<u8>,
    namespace: Vec<u8>,
}

impl EntityKeys {
    pub fn new(volume_id: Uuid, namespace_id: Uuid) -> Self {
        let volume = format!("{{brewfs-entity:{volume_id}}}/").into_bytes();
        let namespace = [
            volume.as_slice(),
            format!("workspace/{namespace_id}/").as_bytes(),
        ]
        .concat();
        Self {
            volume_id,
            namespace_id,
            volume,
            namespace,
        }
    }

    pub fn namespace_id(&self) -> Uuid {
        self.namespace_id
    }
    pub fn volume_id(&self) -> Uuid {
        self.volume_id
    }
    pub fn volume_prefix(&self) -> &[u8] {
        &self.volume
    }
    pub fn prefix(&self) -> &[u8] {
        &self.namespace
    }
    fn key(&self, suffix: &str) -> Vec<u8> {
        [self.prefix(), suffix.as_bytes()].concat()
    }
    fn volume_key(&self, suffix: &str) -> Vec<u8> {
        [self.volume_prefix(), suffix.as_bytes()].concat()
    }
    pub(super) fn key_index(&self) -> Vec<u8> {
        self.volume_key("key-index")
    }
    pub fn descriptor(&self) -> Vec<u8> {
        self.key("descriptor")
    }
    pub fn lease(&self) -> Vec<u8> {
        self.key("lease")
    }
    pub fn lower_pin(&self) -> Vec<u8> {
        self.key("lower-pin")
    }
    pub fn maintenance(&self) -> Vec<u8> {
        self.key("maintenance")
    }
    /// Present while immutable upper objects still depend on one local spool.
    pub fn remote_pending(&self) -> Vec<u8> {
        self.key("remote-pending")
    }
    /// Changed atomically whenever maintenance introduces a Deleting slice.
    pub fn slice_delete_fence(&self) -> Vec<u8> {
        self.key("slice-delete-fence")
    }
    pub fn inode(&self, ino: i64) -> Vec<u8> {
        self.key(&format!("inode/{ino}"))
    }
    pub fn dentry_prefix(&self, parent: i64) -> Vec<u8> {
        self.key(&format!("dentry/{parent}/"))
    }
    pub fn dentry(&self, parent: i64, name: &[u8]) -> Vec<u8> {
        [
            self.dentry_prefix(parent).as_slice(),
            hex::encode(name).as_bytes(),
        ]
        .concat()
    }
    pub fn xattr(&self, ino: i64, name: &[u8]) -> Vec<u8> {
        self.key(&format!("xattr/{ino}/{}", hex::encode(name)))
    }
    pub fn chunk(&self, ino: i64, index: u64) -> Vec<u8> {
        self.key(&format!("chunk/{ino}/{index}"))
    }
    pub fn chunk_prefix(&self, ino: i64) -> Vec<u8> {
        self.key(&format!("chunk/{ino}/"))
    }
    pub fn truncate(&self, ino: i64) -> Vec<u8> {
        self.key(&format!("truncate/{ino}"))
    }
    /// Physical immutable objects never reuse the legacy slice object domain.
    pub fn object(&self, slice: u64) -> String {
        format!(
            "workspace-entity-v1/{}/{}/{slice}",
            self.volume_id, self.namespace_id
        )
    }
    pub fn object_descriptor(&self, slice: u64) -> Vec<u8> {
        self.volume_key(&format!("object/{slice}"))
    }
    pub fn slice(&self, id: u64) -> Vec<u8> {
        self.volume_key(&format!("slice/{id}"))
    }
    pub fn owner(&self, id: u64, ino: i64, index: u64) -> Vec<u8> {
        self.key(&format!("owner/{id}/{ino}/{index}"))
    }
    pub fn orphan(&self, ino: i64) -> Vec<u8> {
        self.key(&format!("orphan/{ino}"))
    }
    pub fn counter(&self, name: &str) -> Vec<u8> {
        if name == "slice" {
            self.volume_key("counter/slice")
        } else {
            self.key(&format!("counter/{name}"))
        }
    }
    pub fn lower_registry(&self, digest: &[u8; 32]) -> Vec<u8> {
        self.volume_key(&format!("lower/{}", hex::encode(digest)))
    }
    pub fn lower_owner(&self, digest: &[u8; 32], namespace: Uuid) -> Vec<u8> {
        self.volume_key(&format!("lower-owner/{}/{namespace}", hex::encode(digest)))
    }
    pub fn publication_owner(&self, slice: u64, digest: &[u8; 32]) -> Vec<u8> {
        self.volume_key(&format!(
            "publication-owner/{slice}/{}",
            hex::encode(digest)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_names_are_injective_and_namespaces_share_only_volume_objects() {
        let volume = Uuid::new_v4();
        let a = EntityKeys::new(volume, Uuid::new_v4());
        let b = EntityKeys::new(volume, Uuid::new_v4());
        assert_ne!(a.dentry(1, b"/a"), a.dentry(1, b"2f61"));
        assert_ne!(a.inode(1), b.inode(1));
        assert_eq!(a.slice(1), b.slice(1));
        assert_eq!(a.counter("slice"), b.counter("slice"));
        assert_ne!(a.counter("inode"), b.counter("inode"));
        assert_eq!(a.volume_prefix(), b.volume_prefix());
    }
}
