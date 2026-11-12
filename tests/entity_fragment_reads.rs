#![cfg(feature = "workspace")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use brewfs::ObjectBackend;
use brewfs::workspace::entity::data::EntityData;
use brewfs::workspace::entity::keys::EntityKeys;
use brewfs::workspace::entity::lifecycle::Lifecycle;
use brewfs::workspace::entity::memory::MemoryEntityBackend;
use brewfs::workspace::entity::model::{
    ChunkMap, EntityDentry, EntityInode, EntityKind, LowerBinding,
};
use brewfs::workspace::entity::namespace::{
    CreateRequest, Credentials, EntityHandle, EntityNamespace,
};
use brewfs::workspace::entity::resolver::{ImmutableLower, ImmutableLowerData};
use brewfs::workspace::entity::{EntityError, Result};
use bytes::Bytes;
use uuid::Uuid;

struct EmptyLower(LowerBinding);

#[async_trait]
impl ImmutableLower for EmptyLower {
    fn binding(&self) -> &LowerBinding {
        &self.0
    }

    async fn inode(&self, ino: i64) -> Result<Option<EntityInode>> {
        Ok((ino == 1).then_some(EntityInode {
            ino,
            kind: EntityKind::Directory,
            size: 0,
            mode: 0o755,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 2,
            atime_ns: 1,
            mtime_ns: 1,
            ctime_ns: 1,
            symlink_target: None,
            parent_hint: Some(1),
            lower_visible_size: 0,
            data_version: 0,
        }))
    }

    async fn dentry(&self, _: i64, _: &[u8]) -> Result<Option<EntityDentry>> {
        Ok(None)
    }

    async fn xattr(&self, _: i64, _: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn chunk(&self, _: i64, _: u64, _: u64) -> Result<Option<ChunkMap>> {
        Ok(None)
    }

    async fn readdir(&self, _: i64) -> Result<Vec<(Vec<u8>, EntityDentry)>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl ImmutableLowerData for EmptyLower {
    async fn read_range(&self, _: i64, _: u64, _: usize) -> Result<Vec<u8>> {
        Err(EntityError::Corrupt("empty lower has no data".into()))
    }
}

#[derive(Default)]
struct Objects {
    bytes: Mutex<BTreeMap<String, Vec<u8>>>,
    reads: AtomicUsize,
    read_bytes: AtomicUsize,
}

#[async_trait]
impl ObjectBackend for Objects {
    fn forbids_mutation_replay(&self) -> bool {
        true
    }

    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("entity writes must use durable create-only PUT")
    }

    async fn put_object_create_only_durable(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let mut objects = self.bytes.lock().unwrap();
        anyhow::ensure!(!objects.contains_key(key), "object already exists");
        objects.insert(key.into(), data.into());
        Ok(())
    }

    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        anyhow::bail!("entity reads must use a bounded GET")
    }

    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        let objects = self.bytes.lock().unwrap();
        let bytes = objects.get(key).expect("published object exists");
        let start = usize::try_from(offset)?.min(bytes.len());
        let count = buf.len().min(bytes.len() - start);
        buf[..count].copy_from_slice(&bytes[start..start + count]);
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.read_bytes.fetch_add(count, Ordering::SeqCst);
        Ok(count)
    }

    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        Ok(self.bytes.lock().unwrap().get(key).map(|v| v.len() as u64))
    }

    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        anyhow::bail!("entity reads authenticate content rather than ETags")
    }

    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.bytes.lock().unwrap().remove(key);
        Ok(())
    }
}

type Data = EntityData<MemoryEntityBackend, EmptyLower, Objects>;

async fn fixture() -> (Arc<Data>, Arc<EntityHandle>, Arc<Objects>) {
    let lower = Arc::new(EmptyLower(LowerBinding {
        manifest_key: b"empty-lower".to_vec(),
        manifest_digest: [1; 32],
        root_ino: 1,
        max_ino: 1,
    }));
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle
        .acquire(Uuid::new_v4(), 60_000_000_000)
        .await
        .unwrap();
    let namespace = EntityNamespace::new(backend, lower, keys, session);
    let objects = Arc::new(Objects::default());
    let data = EntityData::new(namespace.clone(), objects.clone(), 1024 * 1024)
        .await
        .unwrap();
    let root = Credentials {
        uid: 0,
        gid: 0,
        groups: vec![0],
        umask: 0o022,
    };
    namespace
        .create(
            CreateRequest {
                parent: 1,
                name: b"fragments".to_vec(),
                kind: EntityKind::File,
                mode: 0o666,
                rdev: 0,
                symlink_target: None,
            },
            root.clone(),
        )
        .await
        .unwrap();
    let handle = Arc::new(namespace.open(1, b"fragments", 6, &root).await.unwrap());
    (data, handle, objects)
}

#[tokio::test]
async fn fragmented_entity_read_fetches_each_immutable_object_once() {
    let (data, handle, objects) = fixture().await;
    let mut expected = vec![b'a'; 1024 * 1024];
    data.write(handle.clone(), 0, Bytes::from(expected.clone()), false)
        .await
        .unwrap();
    data.write(
        handle.clone(),
        512 * 1024,
        Bytes::from(vec![b'b'; 4096]),
        false,
    )
    .await
    .unwrap();
    expected[512 * 1024..512 * 1024 + 4096].fill(b'b');

    assert_eq!(
        data.read(handle, 0, expected.len()).await.unwrap(),
        expected
    );
    let reads = objects.reads.load(Ordering::SeqCst);
    let read_bytes = objects.read_bytes.load(Ordering::SeqCst);
    eprintln!("fragmented read: {reads} GETs, {read_bytes} bytes");
    assert_eq!(
        reads, 2,
        "the surviving prefix and suffix refetched the same object"
    );
    assert_eq!(read_bytes, 1024 * 1024 + 4096);
}
