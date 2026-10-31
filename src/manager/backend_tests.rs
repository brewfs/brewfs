//! Real Manager admission, durable recovery and ownership against disposable
//! shared services. These are correctness fixtures, never C1/C4 measurements.
use super::*;
use crate::workspace_overlay::{
    catalog::{AcquireLease, ReleaseLease},
    ids::LeaseId,
    model::LayerState,
    stores::{
        kv_backend::{KvCheck, KvWrite, WorkspaceKvBackend},
        kv_store::KvWorkspaceStore,
        manager_guard::MANAGER_CONTROLLER_KEY,
        redis::RedisWorkspaceBackend,
        tikv::TiKvWorkspaceBackend,
    },
};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

#[derive(Clone, Copy)]
pub(super) enum Remote {
    Redis,
    TiKv,
}

impl Remote {
    pub(super) fn config(self, namespace: &str) -> serde_json::Value {
        match self {
            Self::Redis => {
                serde_json::json!({"backend":"redis", "url_env":"BREWFS_TEST_REDIS_URL", "namespace":namespace, "stable_cluster_id":"isolated-manager-redis"})
            }
            Self::TiKv => {
                serde_json::json!({"backend":"tikv", "pd_endpoints":std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS").unwrap().split(',').collect::<Vec<_>>(), "namespace":namespace, "stable_cluster_id":"isolated-manager-tikv"})
            }
        }
    }

    pub(super) async fn raw(self, namespace: &str) -> Arc<dyn WorkspaceKvBackend> {
        match self {
            Self::Redis => Arc::new(
                RedisWorkspaceBackend::connect_read_only(
                    &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
                    namespace,
                )
                .await
                .unwrap(),
            ),
            Self::TiKv => Arc::new(
                TiKvWorkspaceBackend::connect(
                    std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
                        .unwrap()
                        .split(',')
                        .map(str::to_owned)
                        .collect(),
                    namespace,
                )
                .await
                .unwrap(),
            ),
        }
    }

    async fn store(self, namespace: &str) -> Arc<dyn WorkspaceStore> {
        match self {
            Self::Redis => Arc::new(KvWorkspaceStore::new(
                RedisWorkspaceBackend::connect_read_only(
                    &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
                    namespace,
                )
                .await
                .unwrap(),
            )),
            Self::TiKv => Arc::new(KvWorkspaceStore::new(
                TiKvWorkspaceBackend::connect(
                    std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
                        .unwrap()
                        .split(',')
                        .map(str::to_owned)
                        .collect(),
                    namespace,
                )
                .await
                .unwrap(),
            )),
        }
    }

    pub(super) async fn cleanup(self, namespace: &str, volume: Option<Uuid>) {
        #[cfg(feature = "native-packed-base")]
        if let Some(volume) = volume {
            use crate::native_base::write::{
                keys::Keys,
                store::{ControlStore, Txn},
            };
            let control: Arc<dyn ControlStore> = match self {
                Self::Redis => Arc::new(
                    crate::native_base::write::redis::RedisControlStore::connect(
                        &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
                    )
                    .await
                    .unwrap(),
                ),
                Self::TiKv => Arc::new(
                    crate::native_base::write::tikv::TiKvControlStore::connect(
                        &std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
                            .unwrap()
                            .split(',')
                            .map(str::to_owned)
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .unwrap(),
                ),
            };
            let mut txn = Txn::new();
            for (key, value) in control
                .scan(&Keys::volume_prefix(volume.as_bytes()))
                .await
                .unwrap()
            {
                txn = txn.check_bytes(key.clone(), value).delete(key);
            }
            let key = Keys::volume_header(namespace);
            if let Some(value) = control.get(&key).await.unwrap() {
                txn = txn.check_bytes(key.clone(), value).delete(key);
            }
            control.run(txn).await.unwrap();
        }
        #[cfg(not(feature = "native-packed-base"))]
        let _ = volume;
        let raw = self.raw(namespace).await;
        let deletes = raw
            .scan_prefix(b"")
            .await
            .unwrap()
            .into_iter()
            .map(|row| KvWrite::Delete { key: row.key })
            .collect::<Vec<_>>();
        assert!(raw.compare_and_swap(&[], &deletes).await.unwrap());
        raw.shutdown_metadata_backend().await.unwrap();
        if matches!(self, Self::Redis) {
            let mut connection =
                redis::Client::open(std::env::var("BREWFS_TEST_REDIS_URL").unwrap())
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await
                    .unwrap();
            redis::cmd("DEL")
                .arg(format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/__index/keys"))
                .arg(format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/__index/ready"))
                .query_async::<()>(&mut connection)
                .await
                .unwrap();
        }
    }
}

pub(super) fn configure(dir: &Path, catalog: serde_json::Value) {
    std::fs::write(
        dir.join("manager-catalogs.json"),
        serde_json::to_vec(&serde_json::json!({"catalogs":{"shared":catalog}})).unwrap(),
    )
    .unwrap();
}

fn initialize(format: &str) -> VolumeRequest {
    VolumeRequest {
        mode: VolumeMode::Initialize,
        catalog_ref: "shared".into(),
        owner: Some("real-manager-fixture".into()),
        expected_volume_id: None,
        confirm_empty: true,
        volume_format: format.into(),
    }
}

pub(super) fn register(volume: Uuid, format: &str) -> VolumeRequest {
    VolumeRequest {
        mode: VolumeMode::Register,
        catalog_ref: "shared".into(),
        owner: None,
        expected_volume_id: Some(volume),
        confirm_empty: false,
        volume_format: format.into(),
    }
}

pub(super) async fn settled(manager: &Manager, id: Uuid) -> Operation {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            let operation = manager.operation(id).await.unwrap();
            if !matches!(
                operation.state,
                OperationState::Accepted | OperationState::Running
            ) {
                // Join the serialized worker before closing its durable journal.
                let _worker = manager.inner.gate.lock().await;
                return manager.operation(id).await.unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("Manager lifecycle operation did not reach a durable terminal state")
}

pub(super) async fn close(manager: Manager) {
    manager.shutdown().await.unwrap();
    manager.inner.pool.close().await;
    drop(manager);
}

async fn initialized(manager: &Manager, format: &str) -> (Operation, VolumeView) {
    let accepted = manager
        .create_volume("initialize", initialize(format))
        .await
        .unwrap();
    let completed = settled(manager, accepted.id).await;
    assert_eq!(completed.state, OperationState::Succeeded, "{completed:?}");
    let volume = manager.inspect_volume(completed.volume_id).await.unwrap();
    assert!(volume.metadata_ready);
    assert_eq!(volume.remote_ready, None);
    assert_eq!(volume.volume_format, format);
    assert_eq!(
        manager
            .create_volume("initialize", initialize(format))
            .await
            .unwrap()
            .id,
        completed.id
    );
    (completed, volume)
}

async fn recovery_contract(remote: Remote, format: &str) {
    let namespace = format!("manager-recover-{}", Uuid::new_v4().simple());
    let dir = tempfile::tempdir().unwrap();
    configure(dir.path(), remote.config(&namespace));
    let manager = Manager::open(dir.path()).await.unwrap();
    let (initialization, volume) = initialized(&manager, format).await;
    let revision = volume.root_revision.clone().unwrap();
    assert_eq!(
        matches!(revision, Revision::Native(_)),
        format == "workspace-native-v2"
    );
    let request = ForkRequest {
        revision: revision.clone(),
        count: 3,
        owner: Some("fork-owner".into()),
    };
    manager.inner.fail_after_effect.store(1, Ordering::SeqCst);
    let accepted = manager
        .submit_fork(volume.id, "lost-reply", request.clone(), false)
        .await
        .unwrap();
    manager.execute(accepted.id).await.unwrap();
    let partial = manager.operation(accepted.id).await.unwrap();
    assert_eq!(
        partial.state,
        OperationState::RecoveryRequired,
        "{partial:?}"
    );
    assert_eq!(partial.items[0].state, ItemState::Succeeded);
    // The catalog committed, but no item outcome checkpoint reached the
    // journal. RecoveryRequired belongs to the enclosing operation here.
    assert_eq!(partial.items[1].state, ItemState::Pending);
    assert_eq!(manager.list_workspaces(volume.id).await.unwrap().len(), 3);
    let identities = partial
        .items
        .iter()
        .map(|item| (item.workspace_id, item.head_layer_id))
        .collect::<Vec<_>>();
    close(manager).await;
    let reopened = Manager::open(dir.path()).await.unwrap();
    reopened.execute(accepted.id).await.unwrap();
    let recovered = reopened.operation(accepted.id).await.unwrap();
    assert_eq!(recovered.state, OperationState::Succeeded, "{recovered:?}");
    assert_eq!(
        recovered
            .items
            .iter()
            .map(|item| (item.workspace_id, item.head_layer_id))
            .collect::<Vec<_>>(),
        identities
    );
    assert_eq!(
        reopened
            .fork(volume.id, "lost-reply", request.clone())
            .await
            .unwrap()
            .id,
        accepted.id
    );
    let all = reopened.list_workspaces(volume.id).await.unwrap();
    assert_eq!(all.len(), 4);
    assert_eq!(
        all.iter()
            .map(|row| row.workspace_id)
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
    for (id, head) in &identities {
        let view = reopened
            .inspect_workspace(volume.id, id.unwrap())
            .await
            .unwrap();
        assert_eq!(view.record.head_layer_id, head.unwrap());
        assert_eq!(view.base_revision.layer_id, revision.layer_id);
        assert_eq!(view.base_revision.root_hash, revision.root_hash);
        assert_eq!(view.record.owner_id.as_deref(), Some("fork-owner"));
    }

    // A known pre-effect interruption has a different durable checkpoint from
    // an acknowledged catalog effect whose Manager reply was lost.
    reopened.inner.fail_before_effect.store(1, Ordering::SeqCst);
    let pre = reopened
        .submit_fork(volume.id, "pre-effect", request.clone(), false)
        .await
        .unwrap();
    reopened.execute(pre.id).await.unwrap();
    let checkpoint = reopened.operation(pre.id).await.unwrap();
    assert_eq!(
        checkpoint
            .items
            .iter()
            .map(|item| item.state)
            .collect::<Vec<_>>(),
        vec![
            ItemState::Succeeded,
            ItemState::RecoveryRequired,
            ItemState::Pending
        ]
    );
    let retried = reopened
        .submit_fork(volume.id, "pre-effect", request.clone(), false)
        .await
        .unwrap();
    assert_eq!(retried.id, pre.id);
    reopened.execute(retried.id).await.unwrap();
    assert_eq!(
        reopened.operation(pre.id).await.unwrap().state,
        OperationState::Succeeded
    );
    assert_eq!(reopened.list_workspaces(volume.id).await.unwrap().len(), 7);

    let mut wrong = request.clone();
    wrong.revision.root_hash[0] ^= 1;
    let rejected = reopened
        .submit_fork(volume.id, "wrong-revision", wrong, false)
        .await;
    match rejected {
        Err(error) => assert_eq!(error.code, "conflict"),
        Ok(operation) => {
            reopened.execute(operation.id).await.unwrap();
            assert_ne!(
                reopened.operation(operation.id).await.unwrap().state,
                OperationState::Succeeded
            );
        }
    }
    assert_eq!(reopened.list_workspaces(volume.id).await.unwrap().len(), 7);

    let store = remote.store(&namespace).await;
    let id = identities[0].0.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: id,
            lease_id: LeaseId::new(),
            holder_generation: 7,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let deleting = reopened
        .delete_workspace(volume.id, id, "delete-leased")
        .await
        .unwrap();
    let refused = settled(&reopened, deleting.id).await;
    assert_eq!(refused.state, OperationState::Failed, "{refused:?}");
    assert_eq!(
        store.load_workspace(id).await.unwrap().state,
        WorkspaceState::Active
    );
    store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: 7,
        })
        .await
        .unwrap();
    let deleting_again = reopened
        .delete_workspace(volume.id, id, "delete-leased")
        .await
        .unwrap();
    assert_eq!(deleting_again.id, deleting.id);
    let deleted = settled(&reopened, deleting_again.id).await;
    assert_eq!(deleted.state, OperationState::Succeeded, "{deleted:?}");
    assert!(deleted.items[0].deletion_marked);
    assert!(!deleted.items[0].physical_reclaimed);
    assert_eq!(
        store.load_layer(revision.layer_id).await.unwrap().state,
        LayerState::Sealed
    );
    assert_eq!(
        reopened
            .inspect_workspace(volume.id, id)
            .await
            .unwrap()
            .record
            .state,
        WorkspaceState::Deleting
    );
    assert_eq!(
        reopened.operation(initialization.id).await.unwrap().state,
        OperationState::Succeeded
    );
    store.shutdown_metadata_backend().await.unwrap();
    #[cfg(feature = "native-packed-base")]
    let reopened = if format == "workspace-native-v2" {
        native_progressed_reply_recovery(reopened, dir.path(), volume.id, revision).await
    } else {
        reopened
    };
    close(reopened).await;
    remote.cleanup(&namespace, Some(volume.id)).await;
}

#[cfg(feature = "native-packed-base")]
async fn native_progressed_reply_recovery(
    manager: Manager,
    dir: &Path,
    volume: Uuid,
    revision: Revision,
) -> Manager {
    use crate::native_base::write::{
        keys::Keys,
        overlay::{OverlayParams, ensure_workspace_head},
        records::HeadState,
    };

    let before_count = manager.list_workspaces(volume).await.unwrap().len();
    let request = ForkRequest {
        revision,
        count: 1,
        owner: Some("progressed-native-recovery".into()),
    };
    manager.inner.fail_after_effect.store(0, Ordering::SeqCst);
    let accepted = manager
        .submit_fork(
            volume,
            "native-progressed-lost-reply",
            request.clone(),
            false,
        )
        .await
        .unwrap();
    manager.execute(accepted.id).await.unwrap();
    let partial = manager.operation(accepted.id).await.unwrap();
    assert_eq!(
        partial.state,
        OperationState::RecoveryRequired,
        "{partial:?}"
    );
    assert_eq!(partial.items[0].state, ItemState::Pending);
    let item = &partial.items[0];
    let workspace = item.workspace_id.unwrap();
    let creation = item.native_creation.as_ref().unwrap();
    let identity = (
        workspace,
        item.head_layer_id,
        creation.domain_id,
        creation.owner_id,
    );

    let record = manager.record(accepted.id).await.unwrap();
    let connection = manager.operation_catalog(&record).await.unwrap();
    let control = connection.native.as_ref().unwrap();
    let keys = Keys::new(volume.as_bytes());
    let head_key = keys.head(workspace.as_bytes());
    let pristine = control.get(&head_key).await.unwrap().unwrap();
    assert_eq!(HeadState::decode(&pristine).unwrap().writer_generation, 1);
    // The actual mount acquires a catalog lease and advances this native
    // generation before using the child. Run that same publication primitive
    // after the lost Manager reply, then release the lease before recovery.
    let generation = 73;
    let lease = connection
        .store
        .acquire_lease(AcquireLease {
            workspace_id: workspace,
            lease_id: LeaseId::new(),
            holder_generation: generation,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let progressed = ensure_workspace_head(
        control.as_ref(),
        &keys,
        &OverlayParams {
            volume_id: *volume.as_bytes(),
            workspace_id: *workspace.as_bytes(),
            domain_id: *creation.domain_id.as_bytes(),
            writer_generation: generation,
            block_size: 64,
        },
        0,
    )
    .await
    .unwrap()
    .encode();
    assert_ne!(progressed, pristine);
    assert_eq!(
        control.get(&head_key).await.unwrap(),
        Some(progressed.clone())
    );
    connection
        .store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: generation,
        })
        .await
        .unwrap();
    drop(connection);
    close(manager).await;

    let reopened = Manager::open(dir).await.unwrap();
    reopened.execute(accepted.id).await.unwrap();
    let recovered = reopened.operation(accepted.id).await.unwrap();
    assert_eq!(
        recovered.state,
        OperationState::RecoveryRequired,
        "{recovered:?}"
    );
    assert_eq!(recovered.items[0].state, ItemState::RecoveryRequired);
    assert_eq!(recovered.error.as_ref().unwrap().code, "recovery_required");
    let item = &recovered.items[0];
    let creation = item.native_creation.as_ref().unwrap();
    assert_eq!(
        (
            item.workspace_id.unwrap(),
            item.head_layer_id,
            creation.domain_id,
            creation.owner_id
        ),
        identity
    );
    assert!(!item.metadata_ready);
    let retry = reopened
        .submit_fork(volume, "native-progressed-lost-reply", request, false)
        .await
        .unwrap();
    assert_eq!(retry.id, accepted.id);
    reopened.execute(retry.id).await.unwrap();
    assert_eq!(
        reopened.operation(retry.id).await.unwrap().state,
        OperationState::RecoveryRequired
    );
    let connection = reopened.connection("shared", false, true).await.unwrap();
    assert_eq!(
        connection
            .native
            .as_ref()
            .unwrap()
            .get(&head_key)
            .await
            .unwrap(),
        Some(progressed)
    );
    assert_eq!(
        connection
            .store
            .load_workspace(workspace)
            .await
            .unwrap()
            .state,
        WorkspaceState::Active
    );
    assert_eq!(
        reopened.list_workspaces(volume).await.unwrap().len(),
        before_count + 1
    );
    drop(connection);
    reopened
}

async fn ownership_contract(remote: Remote) {
    let namespace = format!("manager-owners-{}", Uuid::new_v4().simple());
    let dir = tempfile::tempdir().unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    let copied_dir = tempfile::tempdir().unwrap();
    for target in [&dir, &other_dir, &copied_dir] {
        configure(target.path(), remote.config(&namespace));
    }
    let manager = Manager::open(dir.path()).await.unwrap();
    let (operation, volume) = initialized(&manager, "workspace-v1").await;
    let other = Manager::open(other_dir.path()).await.unwrap();
    let registration = other
        .create_volume("foreign-journal", register(volume.id, "workspace-v1"))
        .await
        .unwrap();
    assert_ne!(
        settled(&other, registration.id).await.state,
        OperationState::Succeeded
    );
    assert!(other.list_volumes().await.unwrap().is_empty());
    close(other).await;

    // SQLite's online backup statement captures the live journal/WAL at one
    // snapshot, unlike copying an open SQLite file alone.
    sqlx::query("VACUUM INTO ?")
        .bind(copied_dir.path().join("manager.sqlite").to_str().unwrap())
        .execute(&manager.inner.pool)
        .await
        .unwrap();
    let copied = Manager::open(copied_dir.path()).await.unwrap();
    assert_eq!(
        copied.operation(operation.id).await.unwrap().state,
        OperationState::Succeeded
    );
    let request = ForkRequest {
        revision: volume.root_revision.clone().unwrap(),
        count: 1,
        owner: None,
    };
    let competing = copied
        .fork(volume.id, "copied-active", request.clone())
        .await
        .unwrap();
    let waiting = settled(&copied, competing.id).await;
    assert_eq!(waiting.state, OperationState::RecoveryRequired);
    assert_eq!(waiting.error.as_ref().unwrap().code, "session_busy");
    assert_eq!(manager.list_workspaces(volume.id).await.unwrap().len(), 1);
    close(manager).await;
    let after_release = copied
        .fork(volume.id, "copied-active", request)
        .await
        .unwrap();
    assert_eq!(after_release.id, competing.id);
    copied.execute(after_release.id).await.unwrap();
    assert_eq!(
        settled(&copied, after_release.id).await.state,
        OperationState::Succeeded
    );
    assert_eq!(copied.list_workspaces(volume.id).await.unwrap().len(), 2);
    close(copied).await;
    remote.cleanup(&namespace, Some(volume.id)).await;
}

async fn foreign_namespace_contract(remote: Remote, format: &str) {
    let namespace = format!("manager-foreign-{}", Uuid::new_v4().simple());
    let dir = tempfile::tempdir().unwrap();
    configure(dir.path(), remote.config(&namespace));
    let raw = remote.raw(&namespace).await;
    assert!(
        raw.compare_and_swap(
            &[],
            &[KvWrite::Put {
                key: b"foreign".to_vec(),
                value: b"keep-original".to_vec()
            }]
        )
        .await
        .unwrap()
    );
    let manager = Manager::open(dir.path()).await.unwrap();
    let accepted = manager
        .create_volume("empty-only", initialize(format))
        .await
        .unwrap();
    let rejected = settled(&manager, accepted.id).await;
    assert_eq!(rejected.state, OperationState::Failed);
    assert_eq!(rejected.error.unwrap().code, "conflict");
    let reservations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM manager_reservations")
        .fetch_one(&manager.inner.pool)
        .await
        .unwrap();
    assert_eq!(reservations, 0);
    assert_eq!(
        raw.get(b"foreign").await.unwrap(),
        Some(b"keep-original".to_vec())
    );
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    assert!(raw.get(b"control").await.unwrap().is_none());
    close(manager).await;
    raw.shutdown_metadata_backend().await.unwrap();
    remote.cleanup(&namespace, None).await;
}

async fn reopen_configuration_contract(remote: Remote) {
    let namespace = format!("manager-config-{}", Uuid::new_v4().simple());
    let rebound_namespace = format!("manager-rebound-{}", Uuid::new_v4().simple());
    let dir = tempfile::tempdir().unwrap();
    configure(dir.path(), remote.config(&namespace));
    let manager = Manager::open(dir.path()).await.unwrap();
    let (operation, volume) = initialized(&manager, "workspace-v1").await;
    let pending = manager
        .submit_fork(
            volume.id,
            "queued-before-offline",
            ForkRequest {
                revision: volume.root_revision.clone().unwrap(),
                count: 1,
                owner: None,
            },
            false,
        )
        .await
        .unwrap();
    close(manager).await;
    let mut unavailable = remote.config(&namespace);
    match remote {
        Remote::Redis => {
            unavailable["url_env"] =
                format!("BREWFS_MANAGER_MISSING_{}", Uuid::new_v4().simple()).into()
        }
        Remote::TiKv => {
            unavailable["tls"] = serde_json::json!({"ca_path":dir.path().join("missing-ca.pem"), "cert_path":dir.path().join("missing-cert.pem"), "key_path":dir.path().join("missing-key.pem")})
        }
    }
    configure(dir.path(), unavailable);
    let unavailable = Manager::open(dir.path()).await.unwrap();
    assert_eq!(
        unavailable.operation(operation.id).await.unwrap().state,
        OperationState::Succeeded
    );
    // Startup schedules remote recovery instead of awaiting backend access.
    // The durable operation remains queryable even before that attempt ends.
    let offline_pending = unavailable.operation(pending.id).await.unwrap();
    assert_eq!(offline_pending.id, pending.id);
    assert_eq!(
        offline_pending.items[0].workspace_id,
        pending.items[0].workspace_id
    );
    assert_ne!(offline_pending.state, OperationState::Succeeded);
    assert!(unavailable.inspect_volume(volume.id).await.is_err());
    close(unavailable).await;
    configure(dir.path(), remote.config(&rebound_namespace));
    let rebound = Manager::open(dir.path()).await.unwrap();
    assert_eq!(
        rebound.operation(operation.id).await.unwrap().state,
        OperationState::Succeeded
    );
    let attempted = rebound
        .fork(
            volume.id,
            "do-not-rebind",
            ForkRequest {
                revision: volume.root_revision.unwrap(),
                count: 1,
                owner: None,
            },
        )
        .await;
    if let Ok(operation) = attempted {
        assert_ne!(
            settled(&rebound, operation.id).await.state,
            OperationState::Succeeded
        );
    }
    let raw = remote.raw(&rebound_namespace).await;
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    assert!(raw.get(b"control").await.unwrap().is_none());
    raw.shutdown_metadata_backend().await.unwrap();
    close(rebound).await;
    configure(dir.path(), remote.config(&namespace));
    let restored = Manager::open(dir.path()).await.unwrap();
    restored.execute(pending.id).await.unwrap();
    assert_eq!(
        restored.operation(pending.id).await.unwrap().state,
        OperationState::Succeeded
    );
    assert_eq!(restored.list_workspaces(volume.id).await.unwrap().len(), 2);
    close(restored).await;
    remote.cleanup(&namespace, Some(volume.id)).await;
    remote.cleanup(&rebound_namespace, None).await;
}

async fn manager_contract(remote: Remote) {
    foreign_namespace_contract(remote, "workspace-v1").await;
    recovery_contract(remote, "workspace-v1").await;
    ownership_contract(remote).await;
    reopen_configuration_contract(remote).await;
    expired_crash_session_contract(remote).await;
    #[cfg(feature = "native-packed-base")]
    {
        foreign_namespace_contract(remote, "workspace-native-v2").await;
        recovery_contract(remote, "workspace-native-v2").await;
    }
}

async fn expired_crash_session_contract(remote: Remote) {
    let namespace = format!("manager-crash-session-{}", Uuid::new_v4().simple());
    let dir = tempfile::tempdir().unwrap();
    configure(dir.path(), remote.config(&namespace));
    let manager = Manager::open(dir.path()).await.unwrap();
    let (_, volume) = initialized(&manager, "workspace-v1").await;
    let pending = manager
        .submit_fork(
            volume.id,
            "queued-before-crash",
            ForkRequest {
                revision: volume.root_revision.clone().unwrap(),
                count: 1,
                owner: None,
            },
            false,
        )
        .await
        .unwrap();
    close(manager).await;

    // Recreate precisely the durable remote residue of a crashed process:
    // permanent journal owner intact, a different dead session still leased,
    // and the accepted operation awaiting its original resource IDs.
    let raw = remote.raw(&namespace).await;
    let previous = raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().unwrap();
    let mut orphan: serde_json::Value = serde_json::from_slice(&previous).unwrap();
    let dead_session = Uuid::new_v4();
    let deadline = raw.server_time_ns().await.unwrap() + 2_000_000_000;
    orphan["session_id"] = dead_session.to_string().into();
    orphan["expires_at_ns"] = deadline.into();
    let orphan = serde_json::to_vec(&orphan).unwrap();
    assert!(
        raw.compare_and_swap(
            &[KvCheck {
                key: MANAGER_CONTROLLER_KEY.to_vec(),
                expected: Some(previous),
            }],
            &[KvWrite::Put {
                key: MANAGER_CONTROLLER_KEY.to_vec(),
                value: orphan,
            }]
        )
        .await
        .unwrap()
    );

    let reopened = Manager::open(dir.path()).await.unwrap();
    assert_eq!(
        reopened.operation(pending.id).await.unwrap().items[0].workspace_id,
        pending.items[0].workspace_id
    );
    let completed = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let operation = reopened.operation(pending.id).await.unwrap();
            if operation.state == OperationState::Succeeded {
                break operation;
            }
            assert_ne!(operation.state, OperationState::Failed, "{operation:?}");
            if operation.state == OperationState::RecoveryRequired {
                assert_eq!(operation.error.as_ref().unwrap().code, "session_busy");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("startup did not resume the original operation after the dead lease expired");
    assert!(raw.server_time_ns().await.unwrap() >= deadline);
    assert_eq!(completed.id, pending.id);
    assert_eq!(
        completed.items[0].workspace_id,
        pending.items[0].workspace_id
    );
    assert_eq!(
        completed.items[0].head_layer_id,
        pending.items[0].head_layer_id
    );
    assert_eq!(reopened.list_workspaces(volume.id).await.unwrap().len(), 2);
    let successor: serde_json::Value =
        serde_json::from_slice(&raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().unwrap()).unwrap();
    assert_ne!(successor["session_id"], dead_session.to_string());
    close(reopened).await;
    raw.shutdown_metadata_backend().await.unwrap();
    remote.cleanup(&namespace, Some(volume.id)).await;
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL; actual Manager APIs and durable journals"]
async fn real_redis_manager_lifecycle_recovery_and_ownership() {
    manager_contract(Remote::Redis).await;
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; actual Manager APIs and durable journals"]
async fn real_tikv_manager_lifecycle_recovery_and_ownership() {
    manager_contract(Remote::TiKv).await;
}
