use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3BuildPolicy, V3FramePolicy, V3IndexReader, V3ProducerOptions,
    V3SnapshotProducer,
};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, PackedCodec, PackedFileInput, SizeClassTable, pack_group_files_with_policy,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
struct CountingBackend {
    inner: LocalFsBackend,
    gets: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ObjectBackend for CountingBackend {
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, bytes).await
    }
    async fn put_object_create_only(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object_create_only(key, bytes).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_object_range(key, offset, bytes).await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

#[tokio::test]
async fn repeated_authenticated_inode_read_fetches_only_payload() {
    let objects = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let client = ObjectClient::new(CountingBackend {
        inner: LocalFsBackend::new(objects.path()),
        gets: gets.clone(),
    });
    let policy = V3BuildPolicy {
        frames: V3FramePolicy::Static256Kib,
        inline_data: false,
        p90: None,
    };
    let profile = AccessProfile::RandomSmallFile;
    let classes = SizeClassTable::default();
    let (group, frames) = pack_group_files_with_policy(
        7,
        [2; 32],
        vec![PackedFileInput {
            name: b"file".to_vec(),
            inode: 2,
            kind: 1,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            flags: 0,
            data: vec![0x5a; 256 << 10],
        }],
        profile,
        classes,
        policy,
    )
    .unwrap();
    let mut producer = V3SnapshotProducer::new(
        client.clone(),
        scratch.path(),
        "metadata-contract".into(),
        V3ProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 1,
            profile,
            size_classes: classes,
            build_policy: policy,
            metadata_codec: PackedCodec::Raw,
            data_codec: PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    producer
        .add_container(1, &[group], &frames, &[1])
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
        .await
        .unwrap();
    let reader = V3IndexReader::new(client.clone(), 8 << 20);
    let mut output = [0; 4096];
    snapshot
        .read_inode_range(&client, &reader, 2, 0, &mut output, 32 << 20)
        .await
        .unwrap();
    assert_eq!(output, [0x5a; 4096]);
    let before = gets.load(Ordering::SeqCst);
    snapshot
        .read_inode_range(&client, &reader, 2, 4096, &mut output, 32 << 20)
        .await
        .unwrap();
    assert_eq!(output, [0x5a; 4096]);
    assert_eq!(
        gets.load(Ordering::SeqCst) - before,
        1,
        "only the payload is fetched again"
    );
    let before = gets.load(Ordering::SeqCst);
    reader.budget().invalidate_read_caches();
    snapshot
        .read_inode_range(&client, &reader, 2, 0, &mut output, 32 << 20)
        .await
        .unwrap();
    assert_eq!(output, [0x5a; 4096]);
    assert_eq!(
        gets.load(Ordering::SeqCst) - before,
        3,
        "lease fence retires both metadata caches"
    );
}
