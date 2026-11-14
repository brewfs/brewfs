use super::*;
use crate::chunk::read_plan::{ResolvedReadPlan, WorkspaceReadPlanProvider};
use crate::chunk::store::InMemoryBlockStore;
use crate::meta::factory::create_meta_store_from_url;
use crate::vfs::config::VFSConfig;
use tokio::time::timeout;

#[tokio::test]
async fn ready_driver_finishes_in_caller_poll_with_scoped_authority() {
    use crate::meta::layer::{NamespaceActor, scope_open_actor};
    use futures_util::FutureExt;
    let gate = PackedMutationGate::new(true, None);
    let inside = gate.clone();
    let driver = gate.admit(None).unwrap();
    let actor = NamespaceActor {
        uid: 123,
        gid: 456,
        groups: vec![456, 789],
    };
    let result = scope_open_actor(
        Some(actor),
        4,
        driver.run_ready(async move {
            assert!(
                !inside.requires_driver(),
                "nested operation must retain its owner"
            );
            assert_eq!(crate::meta::layer::namespace_actor().unwrap().uid, 123);
            assert_eq!(crate::meta::layer::open_access_mask(), Some(4));
            42
        }),
    )
    .now_or_never()
    .expect("a ready open must not schedule a detached task");
    assert_eq!(result.unwrap(), 42);
    assert!(
        gate.requires_driver(),
        "task-local authority must not escape"
    );
    assert!(crate::meta::layer::namespace_actor().is_none());
    assert!(gate.start_close().unwrap());
    gate.wait_drivers().await.unwrap();
}

#[tokio::test]
async fn pending_ready_driver_survives_cancellation_and_holds_drain() {
    use crate::meta::layer::{NamespaceActor, scope_open_actor};
    let gate = PackedMutationGate::new(true, None);
    let inside = gate.clone();
    let driver = gate.admit(None).unwrap();
    let (entered, reached) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let (done, completed) = oneshot::channel();
    let actor = NamespaceActor {
        uid: 123,
        gid: 456,
        groups: vec![456],
    };
    let caller = tokio::spawn(scope_open_actor(
        Some(actor),
        4,
        driver.run_ready(async move {
            entered.send(()).unwrap();
            wait.await.unwrap();
            assert!(!inside.requires_driver());
            assert_eq!(crate::meta::layer::namespace_actor().unwrap().uid, 123);
            assert_eq!(crate::meta::layer::open_access_mask(), Some(4));
            done.send(()).unwrap();
        }),
    ));
    reached.await.unwrap();
    caller.abort();
    let _ = caller.await;
    gate.start_close().unwrap();
    assert!(
        timeout(Duration::from_millis(20), gate.wait_drivers())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    completed.await.unwrap();
    timeout(Duration::from_secs(2), gate.wait_drivers())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn ready_driver_panic_poison_matches_detached_driver() {
    let gate = PackedMutationGate::new(true, None);
    assert!(
        gate.admit(None)
            .unwrap()
            .run_ready::<_, ()>(async {
                panic!("injected ready driver panic");
            })
            .await
            .is_err()
    );
    assert!(gate.start_close().is_err());
}

struct LocalProvider;
#[async_trait::async_trait]
impl WorkspaceReadPlanProvider for LocalProvider {
    fn requires_unified_read_request_fence(&self) -> bool {
        true
    }
    async fn read_plan(
        &self,
        _: i64,
        _: u64,
        _: u64,
        _: u64,
    ) -> Result<ResolvedReadPlan, MetaError> {
        Err(MetaError::NotSupported(
            "local gate fixture has no lower objects".into(),
        ))
    }
    async fn range_has_data(&self, _: i64, _: u64, _: u64) -> Result<bool, MetaError> {
        Ok(false)
    }
}

async fn fixture() -> VFS<InMemoryBlockStore, impl MetaLayer> {
    let meta = create_meta_store_from_url("sqlite::memory:")
        .await
        .unwrap()
        .layer();
    let layout = ChunkLayout {
        chunk_size: 8192,
        block_size: 4096,
    };
    VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        Arc::new(InMemoryBlockStore::new()),
        meta,
        Arc::new(LocalProvider),
    )
    .unwrap()
}

#[tokio::test]
async fn packed_local_gate_blocks_real_namespace_attrs_data_and_xattr_after_drain() {
    let vfs = fixture().await;
    let ino = vfs.create_file("/kept").await.unwrap();
    let fence = timeout(Duration::from_secs(2), vfs.quiesce_packed_vfs())
        .await
        .unwrap()
        .unwrap();
    assert!(fence.is_same_vfs(&vfs));
    fence.validate_local().await.unwrap();
    assert!(vfs.mkdir_p("/later").await.is_err());
    assert!(vfs.create_file("/later").await.is_err());
    assert!(vfs.rename("/kept", "/moved").await.is_err());
    assert!(vfs.unlink("/kept").await.is_err());
    assert!(vfs.truncate_inode(ino, 1).await.is_err());
    assert!(vfs.write_ino(ino, 0, b"later").await.is_err());
    assert!(vfs.write_cached_ino(ino, 0, b"later", 1).await.is_err());
    assert!(
        vfs.set_attr(
            ino,
            &SetAttrRequest {
                mode: Some(0o600),
                ..Default::default()
            },
            SetAttrFlags::empty()
        )
        .await
        .is_err()
    );
    assert!(
        vfs.set_xattr_bytes_ino(ino, b"user.proof", b"later", 0)
            .await
            .is_err()
    );
    assert!(vfs.stat("/kept").await.is_ok());
    assert!(!vfs.exists("/later").await);
    drop(fence);
    assert!(vfs.create_file("/after-drop").await.is_err());
}

#[tokio::test]
async fn packed_cancelled_reply_keeps_real_committed_mutation_owned_until_terminal() {
    let vfs = fixture().await;
    let gate = vfs.state.packed_mutation_gate.clone();
    let driver = vfs.packed_mutation_admit(&[]).unwrap();
    let owned_vfs = vfs.clone();
    let (committed, observed) = oneshot::channel();
    let (release, held_reply) = oneshot::channel();
    let caller = tokio::spawn(driver.run(async move {
        let ino = owned_vfs.create_file("/committed").await.unwrap();
        let _ = committed.send(ino);
        let _ = held_reply.await;
    }));
    let ino = observed.await.unwrap();
    assert!(vfs.meta_stat(ino).await.unwrap().is_some());
    caller.abort();
    let _ = caller.await;
    assert!(gate.start_close().unwrap());
    assert!(
        timeout(Duration::from_millis(20), gate.wait_drivers())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    timeout(Duration::from_secs(2), gate.wait_drivers())
        .await
        .unwrap()
        .unwrap();
    assert!(vfs.create_file("/after").await.is_err());
}

#[tokio::test]
async fn packed_nested_mutation_keeps_existing_admission_after_freeze_without_deadlock() {
    let vfs = fixture().await;
    let driver = vfs.packed_mutation_admit(&[]).unwrap();
    let owned_vfs = vfs.clone();
    let (entered, observed) = oneshot::channel();
    let (release, proceed) = oneshot::channel();
    let caller = tokio::spawn(driver.run(async move {
        let _ = entered.send(());
        proceed.await.unwrap();
        owned_vfs.mkdir_p("/nested/a/b").await
    }));
    observed.await.unwrap();
    let mut drain = Box::pin(vfs.quiesce_packed_vfs());
    assert!(
        timeout(Duration::from_millis(20), drain.as_mut())
            .await
            .is_err()
    );
    assert!(vfs.mkdir_p("/outside").await.is_err());
    release.send(()).unwrap();
    timeout(Duration::from_secs(2), caller)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), drain)
        .await
        .unwrap()
        .unwrap();
    assert!(vfs.exists("/nested/a/b").await);
    assert!(!vfs.exists("/outside").await);
}

#[tokio::test]
async fn packed_cancelled_drain_waiter_does_not_reopen_or_detach_drain() {
    let vfs = fixture().await;
    let driver = vfs.packed_mutation_admit(&[]).unwrap();
    let (entered, observed) = oneshot::channel();
    let (release, proceed) = oneshot::channel();
    let caller = tokio::spawn(driver.run(async move {
        let _ = entered.send(());
        let _ = proceed.await;
    }));
    observed.await.unwrap();
    let owned_vfs = vfs.clone();
    let waiter = tokio::spawn(async move { owned_vfs.quiesce_packed_vfs().await });
    timeout(Duration::from_secs(2), async {
        loop {
            if vfs.state.packed_mutation_gate.state.lock().unwrap().closed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    let _ = waiter.await;
    assert!(vfs.create_file("/reopened").await.is_err());
    release.send(()).unwrap();
    caller.await.unwrap().unwrap();
    timeout(Duration::from_secs(2), vfs.quiesce_packed_vfs())
        .await
        .unwrap()
        .unwrap();
    assert!(vfs.create_file("/after").await.is_err());
}

#[tokio::test]
async fn packed_panicked_mutation_poison_prevents_any_drain_receipt() {
    let gate = PackedMutationGate::new(true, None);
    let driver = gate.admit(None).unwrap();
    assert!(
        driver
            .run::<_, ()>(async {
                panic!("simulated backend driver panic");
            })
            .await
            .is_err()
    );
    assert!(gate.start_close().is_err());
}

#[test]
fn packed_mutation_admission_is_bounded_before_driver_allocation() {
    let gate = PackedMutationGate::new(true, None);
    let owners: Vec<_> = (0..MAX_MUTATION_DRIVERS)
        .map(|_| gate.admit(None).unwrap())
        .collect();
    assert!(matches!(gate.admit(None), Err(VfsError::ResourceBusy)));
    drop(owners);
    assert!(gate.start_close().is_err());
}
