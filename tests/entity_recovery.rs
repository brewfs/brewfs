#![cfg(feature = "workspace-overlay")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use brewfs::ObjectBackend;
use brewfs::workspace_overlay::entity::backend::{
    CommitOutcome, CreateAllocation, EntityBackend, EntityPlan, RowCheck, RowSnapshot, RowWrite,
    SliceAllocation, TransactionAuthority,
};
use brewfs::workspace_overlay::entity::codec::decode;
use brewfs::workspace_overlay::entity::data::EntityData;
use brewfs::workspace_overlay::entity::keys::EntityKeys;
use brewfs::workspace_overlay::entity::lifecycle::{Lifecycle, WriterSession};
use brewfs::workspace_overlay::entity::memory::MemoryEntityBackend;
use brewfs::workspace_overlay::entity::model::{
    ChunkMap, EntityDentry, EntityInode, EntityKind, LowerBinding, UpperInode,
};
use brewfs::workspace_overlay::entity::namespace::{
    CreateRequest, Credentials, EntityHandle, EntityNamespace,
};
use brewfs::workspace_overlay::entity::ownership::{OrphanRecord, SliceOwnership};
use brewfs::workspace_overlay::entity::recovery::{Recovery, TruncateIntent, prepare_resize};
use brewfs::workspace_overlay::entity::redis::RedisEntityBackend;
use brewfs::workspace_overlay::entity::resolver::{ImmutableLower, ImmutableLowerData};
use brewfs::workspace_overlay::entity::{EntityError, Result};
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

const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// A recovery attempt must save progress before inspecting more than 4096
/// chunk keys, even if every scanned key belongs to the retained prefix.
struct BoundedDiscovery<B> {
    inner: Arc<B>,
    scanned: AtomicUsize,
}

#[async_trait]
impl<B: EntityBackend> EntityBackend for BoundedDiscovery<B> {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.inner.read(keys).await
    }

    async fn read_checked(
        &self,
        keys: &[Vec<u8>],
        checks: &[RowCheck],
    ) -> Result<(RowSnapshot, bool)> {
        self.inner.read_checked(keys, checks).await
    }

    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        let result = self.inner.commit(authority, plan).await?;
        assert_eq!(result, CommitOutcome::Committed, "uncontended recovery");
        self.scanned.store(0, Ordering::SeqCst);
        Ok(result)
    }

    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        self.inner
            .commit_with_inode_allocation(authority, plan, create)
            .await
    }

    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        self.inner.reserve_slices(authority, allocation).await
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix, limit).await
    }

    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_batch(prefix, limit).await
    }

    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let page = self.inner.scan_keys_page(prefix, after, limit).await?;
        let scanned = self.scanned.fetch_add(page.len(), Ordering::SeqCst) + page.len();
        assert!(
            scanned <= 4096,
            "cleanup scanned {scanned} keys without saving progress"
        );
        Ok(page)
    }
}

fn root() -> Credentials {
    Credentials {
        uid: 0,
        gid: 0,
        groups: vec![0],
        umask: 0o022,
    }
}

struct Fixture<B> {
    backend: Arc<B>,
    keys: EntityKeys,
    lifecycle: Lifecycle<B>,
    session: WriterSession,
    namespace: Arc<EntityNamespace<B, EmptyLower>>,
    data: Arc<EntityData<B, EmptyLower, Objects>>,
    handle: Option<Arc<EntityHandle>>,
    ino: i64,
    slices: Vec<(u64, u64)>,
}

impl<B: EntityBackend + 'static> Fixture<B> {
    async fn new(backend: Arc<B>, keys: EntityKeys, chunks: u64) -> Self {
        let lower = Arc::new(EmptyLower(LowerBinding {
            manifest_key: b"empty-lower".to_vec(),
            manifest_digest: [1; 32],
            root_ino: 1,
            max_ino: 1,
        }));
        let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
        lifecycle.provision(lower.binding()).await.unwrap();
        let session = lifecycle
            .acquire(Uuid::new_v4(), 3_600_000_000_000)
            .await
            .unwrap();
        let namespace = EntityNamespace::new(backend.clone(), lower, keys.clone(), session.clone());
        let data = EntityData::new(namespace.clone(), Arc::new(Objects::default()), CHUNK_SIZE)
            .await
            .unwrap();
        namespace
            .create(
                CreateRequest {
                    parent: 1,
                    name: b"large".to_vec(),
                    kind: EntityKind::File,
                    mode: 0o666,
                    rdev: 0,
                    symlink_target: None,
                },
                root(),
            )
            .await
            .unwrap();
        let handle = Arc::new(namespace.open(1, b"large", 6, &root()).await.unwrap());
        let ino = handle.ino();
        let mut slices = Vec::new();
        // One byte per 4 MiB chunk creates a >16 GiB logical file cheaply.
        for index in 0..chunks {
            data.write(
                handle.clone(),
                index * CHUNK_SIZE,
                Bytes::from_static(b"x"),
                false,
            )
            .await
            .unwrap();
            let snapshot = backend.read(&[keys.chunk(ino, index)]).await.unwrap();
            let map: ChunkMap = decode(snapshot.rows[0].as_deref().unwrap()).unwrap();
            let ids = map.upper_slices().unwrap();
            assert_eq!(ids.len(), 1);
            slices.push((index, *ids.first().unwrap()));
        }
        Self {
            backend,
            keys,
            lifecycle,
            session,
            namespace,
            data,
            handle: Some(handle),
            ino,
            slices,
        }
    }

    async fn assert_drained(&self) {
        assert!(
            self.backend
                .scan_prefix(&self.keys.chunk_prefix(self.ino), 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            self.backend
                .read(&[self.keys.truncate(self.ino)])
                .await
                .unwrap()
                .rows[0]
                .is_none()
        );
        for &(index, id) in &self.slices {
            let rows = self
                .backend
                .read(&[self.keys.slice(id), self.keys.owner(id, self.ino, index)])
                .await
                .unwrap()
                .rows;
            let owner: SliceOwnership = decode(rows[0].as_deref().unwrap()).unwrap();
            assert_eq!(owner.current_refs, 0);
            assert!(owner.retirement_hold);
            assert!(rows[1].is_none(), "chunk membership must be released");
        }
    }

    async fn unlink_and_drain(&mut self, max_steps: u64) {
        self.namespace
            .unlink(1, b"large".to_vec(), false, root())
            .await
            .unwrap();
        drop(self.handle.take());
        let orphan = self
            .backend
            .read(&[self.keys.orphan(self.ino)])
            .await
            .unwrap();
        let bytes = orphan.rows[0].as_deref().unwrap();
        let mut legacy: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        legacy.as_object_mut().unwrap().remove("discovery");
        assert_eq!(
            decode::<OrphanRecord>(&serde_json::to_vec(&legacy).unwrap()).unwrap(),
            decode::<OrphanRecord>(bytes).unwrap(),
            "old orphan records start with empty discovery"
        );
        let maintenance = self
            .lifecycle
            .enter_maintenance(&self.session)
            .await
            .unwrap();
        let mut complete = false;
        for _ in 0..max_steps {
            complete = Recovery::new(self.backend.clone(), self.keys.clone())
                .recover_orphan_step(&maintenance, self.ino)
                .await
                .expect("orphan cleanup must drain every chunk row");
            if complete {
                break;
            }
        }
        assert!(complete, "orphan recovery must reach EOF");
        assert!(
            Recovery::new(self.backend.clone(), self.keys.clone())
                .recover_orphan_step(&maintenance, self.ino)
                .await
                .unwrap()
        );
        let rows = self
            .backend
            .read(&[self.keys.orphan(self.ino), self.keys.inode(self.ino)])
            .await
            .unwrap()
            .rows;
        assert!(rows[0].is_none());
        assert_eq!(
            decode::<UpperInode>(rows[1].as_deref().unwrap()).unwrap(),
            UpperInode::Deleted
        );
    }
}

async fn large_file_contract<B: EntityBackend + 'static>(
    backend: Arc<B>,
    keys: EntityKeys,
    orphan: bool,
    chunks: u64,
) {
    eprintln!(
        "backend={}, chunks={chunks}, orphan={orphan}",
        std::any::type_name::<B>()
    );
    let backend = Arc::new(BoundedDiscovery {
        inner: backend,
        scanned: AtomicUsize::new(0),
    });
    let mut fixture = Fixture::new(backend, keys, chunks).await;
    let prefix = fixture.keys.chunk_prefix(fixture.ino);
    // Full observations must still reject overflow: directory emptiness and
    // other callers rely on scan_prefix never returning a partial answer.
    assert!(matches!(
        fixture.backend.scan_prefix(&prefix, 4096).await,
        Err(EntityError::Invalid(_))
    ));
    let page = fixture
        .backend
        .scan_keys_page(&prefix, None, 2)
        .await
        .unwrap();
    assert_eq!(
        page,
        vec![
            fixture.keys.chunk(fixture.ino, 0),
            fixture.keys.chunk(fixture.ino, 1)
        ]
    );
    let batch = fixture.backend.scan_prefix_batch(&prefix, 2).await.unwrap();
    let rows = fixture.backend.read(&page).await.unwrap().rows;
    assert_eq!(
        batch,
        page.iter()
            .cloned()
            .zip(rows.into_iter().map(Option::unwrap))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        fixture
            .backend
            .scan_keys_page(&prefix, Some(&page[1]), 2)
            .await
            .unwrap(),
        vec![
            fixture.keys.chunk(fixture.ino, 10),
            fixture.keys.chunk(fixture.ino, 100)
        ]
    );
    assert!(matches!(
        fixture
            .backend
            .scan_keys_page(&prefix, Some(&fixture.keys.inode(fixture.ino)), 2)
            .await,
        Err(EntityError::Invalid(_))
    ));
    if orphan {
        // Discovery-only steps also have to survive reconstructing Recovery.
        fixture
            .unlink_and_drain(chunks * (chunks / 1024 + 1) + 1)
            .await;
        fixture.assert_drained().await;
    } else {
        let handle = fixture.handle.as_ref().unwrap().clone();
        let result = fixture.data.truncate(handle.clone(), 0).await;
        let rows = fixture
            .backend
            .read(&[fixture.keys.inode(fixture.ino)])
            .await
            .unwrap()
            .rows;
        let UpperInode::Present(inode) = decode(rows[0].as_deref().unwrap()).unwrap() else {
            panic!("truncate must retain the inode");
        };
        assert_eq!(inode.size, 0, "size cut is already committed");
        eprintln!(
            "truncate {chunks} chunks: committed size={}, result={:?}",
            inode.size,
            result.as_ref().map(|_| ())
        );
        if result.is_err() {
            eprintln!(
                "subsequent read: {:?}",
                fixture.data.read(handle.clone(), 0, 1).await
            );
        }
        result.expect("truncate must drain every chunk row");
        fixture.assert_drained().await;
        fixture
            .data
            .write(handle.clone(), 0, Bytes::from_static(b"new"), false)
            .await
            .unwrap();
        assert_eq!(
            fixture.data.read(handle.clone(), 0, 3).await.unwrap(),
            b"new"[..]
        );
        fixture.data.fsync(handle).await.unwrap();
        fixture.unlink_and_drain(2).await;
    }
}

#[tokio::test]
async fn issue_188_truncate_drains_4097_chunk_rows() {
    large_file_contract(
        Arc::new(MemoryEntityBackend::new(1000)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        false,
        4097,
    )
    .await;
}

#[tokio::test]
async fn issue_188_orphan_drains_4097_chunk_rows() {
    large_file_contract(
        Arc::new(MemoryEntityBackend::new(1000)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        true,
        4097,
    )
    .await;
}

#[tokio::test]
async fn issue_188_runtime_truncate_drains_more_than_4098_steps() {
    large_file_contract(
        Arc::new(MemoryEntityBackend::new(1000)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        false,
        4100,
    )
    .await;
}

#[tokio::test]
async fn issue_188_partial_cut_preserves_large_prefix_and_drains_sparse_tail() {
    let fixture = Fixture::new(
        Arc::new(BoundedDiscovery {
            inner: Arc::new(MemoryEntityBackend::new(1000)),
            scanned: AtomicUsize::new(0),
        }),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        4097,
    )
    .await;
    let handle = fixture.handle.as_ref().unwrap().clone();
    // The tail sorts into the first lexical page, but the smaller numeric
    // boundary (4096) appears several pages later. Both must survive selection.
    let far_index = 10_000_000_000;
    fixture
        .data
        .write(
            handle.clone(),
            far_index * CHUNK_SIZE,
            Bytes::from_static(b"tail"),
            false,
        )
        .await
        .unwrap();
    let tail = fixture
        .backend
        .read(&[fixture.keys.chunk(fixture.ino, far_index)])
        .await
        .unwrap();
    let tail: ChunkMap = decode(tail.rows[0].as_deref().unwrap()).unwrap();
    let tail_id = *tail.upper_slices().unwrap().first().unwrap();
    let cut = 4096 * CHUNK_SIZE + 1;
    // Commit a legacy intent without discovery fields, then interrupt recovery
    // after two pages. Each new Recovery instance must resume its durable scan.
    let raw = fixture
        .backend
        .read(&[fixture.keys.inode(fixture.ino)])
        .await
        .unwrap()
        .rows
        .remove(0);
    let UpperInode::Present(inode) = decode(raw.as_deref().unwrap()).unwrap() else {
        panic!("file inode must be present");
    };
    let mut plan =
        prepare_resize(&fixture.keys, fixture.ino, raw, &inode, cut, CHUNK_SIZE, 2).unwrap();
    for write in &mut plan.writes {
        if let RowWrite::Put { key, value } = write
            && *key == fixture.keys.truncate(fixture.ino)
        {
            let mut legacy: serde_json::Value = serde_json::from_slice(value).unwrap();
            legacy.as_object_mut().unwrap().remove("discovery");
            *value = serde_json::to_vec(&legacy).unwrap();
        }
    }
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    fixture.backend.commit(&authority, &plan).await.unwrap();
    let observed = [
        fixture.keys.chunk(fixture.ino, 4096),
        fixture.keys.chunk(fixture.ino, far_index),
    ];
    let before = fixture.backend.read(&observed).await.unwrap().rows;
    let mut previous_after = None;
    for _ in 0..2 {
        assert!(
            !Recovery::new(fixture.backend.clone(), fixture.keys.clone())
                .truncate_step(&authority, fixture.ino)
                .await
                .unwrap()
        );
        let row = fixture
            .backend
            .read(&[fixture.keys.truncate(fixture.ino)])
            .await
            .unwrap();
        let bytes = row.rows[0].as_deref().unwrap();
        assert_eq!(decode::<TruncateIntent>(bytes).unwrap().next_chunk, 4096);
        let state: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        let after = fixture
            .keys
            .chunk(fixture.ino, state["discovery"]["after"].as_u64().unwrap());
        assert!(
            previous_after
                .as_ref()
                .is_none_or(|previous| *previous < after)
        );
        previous_after = Some(after);
        assert_eq!(
            state["discovery"]["smallest"],
            serde_json::json!([far_index])
        );
        assert_eq!(fixture.backend.read(&observed).await.unwrap().rows, before);
    }
    fixture
        .data
        .truncate(handle.clone(), cut)
        .await
        .expect("partial cut must page past retained chunks");
    fixture
        .data
        .truncate(handle.clone(), cut + 2)
        .await
        .unwrap();
    assert_eq!(
        fixture.data.read(handle.clone(), cut - 1, 3).await.unwrap(),
        Bytes::from_static(b"x\0\0")
    );
    fixture.data.fsync(handle).await.unwrap();
    let rows = fixture
        .backend
        .read(&[
            fixture.keys.truncate(fixture.ino),
            fixture.keys.chunk(fixture.ino, far_index),
            fixture.keys.slice(tail_id),
            fixture.keys.owner(tail_id, fixture.ino, far_index),
        ])
        .await
        .unwrap()
        .rows;
    assert!(rows[0].is_none());
    assert!(rows[1].is_none());
    let owner: SliceOwnership = decode(rows[2].as_deref().unwrap()).unwrap();
    assert_eq!(owner.current_refs, 0);
    assert!(owner.retirement_hold);
    assert!(rows[3].is_none());
    for &(index, id) in &fixture.slices {
        let rows = fixture
            .backend
            .read(&[
                fixture.keys.chunk(fixture.ino, index),
                fixture.keys.slice(id),
            ])
            .await
            .unwrap()
            .rows;
        assert!(rows[0].is_some());
        assert_eq!(
            decode::<SliceOwnership>(rows[1].as_deref().unwrap())
                .unwrap()
                .current_refs,
            1
        );
    }
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn issue_188_redis_drains_4097_chunk_rows() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    for orphan in [false, true] {
        let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
        let backend = Arc::new(
            RedisEntityBackend::connect(&url, keys.volume_id())
                .await
                .unwrap(),
        );
        large_file_contract(backend, keys, orphan, 4097).await;
    }
}
