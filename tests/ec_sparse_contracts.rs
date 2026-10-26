#![cfg(feature = "workspace-overlay")]

use async_trait::async_trait;
use brewfs::cadapter::{
    client::{ObjectBackend, ObjectClient},
    localfs::LocalFsBackend,
};
use brewfs::chunk::{
    cache::ChunksCacheConfig,
    compress::{Compression, encode_persisted_block},
    store::{BlockStore, BlockStoreConfig, ObjectBlockStore, ReadFetchPolicy},
};
use brewfs::native_base::{
    runtime::BackendObjectRepository, wire::refs::ObjectRef, write::receipts::ObjectSink,
};
use brewfs::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, PackedFrameInput, PackedGroupContainer, PackedGroupInput,
    PackedObjectKind, RemotePackedObject, SizeClass,
};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct HintBackend {
    inner: LocalFsBackend,
    ranges: Arc<Mutex<Vec<(u64, usize)>>>,
}

#[async_trait]
impl ObjectBackend for HintBackend {
    fn prefers_full_object_reads(&self) -> bool {
        true
    }
    async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, data).await
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object_create_only(key, data).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.ranges.lock().unwrap().push((offset, buf.len()));
        self.inner.get_object_range(key, offset, buf).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

#[tokio::test]
async fn sparse_store_and_packed_authenticated_ranges_keep_distinct_contracts() -> anyhow::Result<()>
{
    let root = tempfile::tempdir()?;
    let backend = HintBackend {
        inner: LocalFsBackend::new(root.path()),
        ranges: Arc::default(),
    };
    let client = ObjectClient::new(backend.clone());
    client
        .put_object(
            "chunks-v2/1/0",
            &encode_persisted_block(&vec![37; 4 << 20], Compression::None),
        )
        .await?;
    let store = ObjectBlockStore::new_with_configs_async(
        client.clone(),
        ChunksCacheConfig::with_budgets(8 << 20, 0, root.path().join("cache")),
        BlockStoreConfig::default(),
    )
    .await?
    .with_read_fetch_policy(ReadFetchPolicy::Sparse);
    let mut small = [0; 4096];
    store.read_range((1, 0), 0, &mut small).await?;
    assert_eq!(small, [37; 4096]);
    assert_eq!(*backend.ranges.lock().unwrap(), [(0, 4), (4, 65536)]);

    let object = PackedGroupContainer::build(
        8,
        AccessProfile::RandomSmallFile,
        vec![PackedGroupInput {
            group_id: 1,
            parent_dir_key: [0; 32],
            metadata: GroupMeta::new(Vec::new())?.encode()?,
            frame_ordinals: vec![0],
            entry_count: 0,
            file_count: 0,
            layout_profile: AccessProfile::RandomSmallFile,
        }],
        vec![PackedFrameInput {
            raw: vec![7; 1024],
            size_class: SizeClass::Tiny,
            codec: 0,
            first_file_slot: 0,
            last_file_slot: 0,
        }],
    )?;
    client.put_object("packed", &object).await?;
    let opened = PackedGroupContainer::open(object.clone())?;
    let frame = &opened.frames()[0];
    let remote = RemotePackedObject::open(
        &client,
        "packed",
        opened.object_len(),
        PackedObjectKind::GroupContainer,
    )
    .await?;
    backend.ranges.lock().unwrap().clear();
    assert_eq!(remote.read_frame(frame).await?.as_ref(), &[7; 1024]);
    assert_eq!(
        *backend.ranges.lock().unwrap(),
        [(frame.object_offset, frame.stored_len as usize)]
    );
    let mut corrupted = object;
    corrupted[frame.object_offset as usize] ^= 1;
    client.put_object("packed", &corrupted).await?;
    assert!(
        remote.read_frame(frame).await.is_err(),
        "authenticated packed frames must reject tampering"
    );
    Ok(())
}

#[tokio::test]
async fn native_p1_publication_readback_still_verifies_length_and_hash() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let backend = LocalFsBackend::new(root.path());
    let repository = BackendObjectRepository::new(ObjectClient::new(backend.clone()));
    let bytes = b"verified native payload";
    let object = ObjectRef {
        object_id: [1; 16],
        kind: 6,
        object_len: bytes.len() as u64,
        full_hash: Sha256::digest(bytes).into(),
        key: b"native-p1".to_vec(),
    };
    repository.put(&object, bytes).await?;
    assert_eq!(repository.get(&object).await?, bytes);
    backend
        .put_object("native-p1", b"tampered native payload")
        .await?;
    assert!(repository.get(&object).await.is_err());
    backend.put_object("native-p1", b"short").await?;
    assert!(repository.get(&object).await.is_err());
    Ok(())
}
