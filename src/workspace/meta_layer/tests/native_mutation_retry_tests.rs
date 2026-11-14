//! Exercise real independent SQLite connections and the actual retry timer.
//! A one-shot gate pauses before BEGIN/write_gate, never while holding a DB lock.

use super::*;
use crate::meta::layer::scope_namespace_actor;
use crate::meta::store::OpenFlags;
use crate::workspace::meta_layer::native_mutation_retry::NativeRetryEvent;
use crate::workspace::stores::database::StoreFailpoint;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use tokio::sync::mpsc::UnboundedReceiver;

struct Fixture {
    _directory: tempfile::TempDir,
    writer: WorkspaceMetaLayer<SqliteWorkspaceStore>,
    observer: WorkspaceMetaLayer<SqliteWorkspaceStore>,
    ino: i64,
}

async fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("native-retry.sqlite").display()
    );
    let first = Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap());
    first.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::new();
    let workspace = first
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace".into(),
            schema_version: crate::workspace::model::WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id,
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: Some("native-mutation-retry".into()),
        })
        .await
        .unwrap();
    let lease = first
        .acquire_lease(AcquireLease {
            workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let view = ViewContext {
        workspace_id,
        head_layer_id: workspace.head_layer_id,
        head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };
    let writer = WorkspaceMetaLayer::new(first, view.clone());
    let observer = WorkspaceMetaLayer::new(
        Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap()),
        view,
    );
    let ino = writer.create_file(1, "shared-policy".into()).await.unwrap();
    writer
        .set_xattr(ino, "system.posix_acl_access", &named_acl(0o640), 0)
        .await
        .unwrap();
    Fixture {
        _directory: directory,
        writer,
        observer,
        ino,
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Open,
    SetAttr,
}

#[derive(Clone, Copy, Debug)]
enum RevokedPolicy {
    Posix,
    ControlOnly,
}

fn assert_effective_write_permission(
    snapshot: &crate::workspace::catalog::PermissionSnapshot,
    ino: i64,
    operation: Operation,
    policy: RevokedPolicy,
    expected: bool,
) {
    let (inode, access, _, control) =
        WorkspaceMetaLayer::<SqliteWorkspaceStore>::decode_permission_inode(snapshot, ino).unwrap();
    match policy {
        RevokedPolicy::Posix => {
            let mode = if expected { 0o666 } else { 0o600 };
            assert_eq!(inode.mode & 0o777, mode, "{operation:?}/{policy:?}");
            assert_eq!(access, Some(named_acl(mode)), "{operation:?}/{policy:?}");
            // A present POSIX ACL is authoritative even if the fallback
            // control ACL would allow the named user to write.
            assert_eq!(control, Some(control_acl_json("access", Some("rwx"))));
        }
        RevokedPolicy::ControlOnly => {
            assert!(
                access.is_none(),
                "{operation:?}/{policy:?}: control must be effective"
            );
            assert_eq!(
                control,
                Some(control_acl_json(
                    "access",
                    Some(if expected { "rwx" } else { "---" })
                )),
                "{operation:?}/{policy:?}"
            );
        }
    }
    let actor = policy_actor();
    assert_eq!(
        WorkspaceMetaLayer::<SqliteWorkspaceStore>::inode_allows_actor(
            &inode,
            access.as_deref(),
            control.as_deref(),
            actor.uid,
            &actor.groups,
            2,
        )
        .unwrap(),
        expected,
        "{operation:?}/{policy:?}: actual snapshot authorization"
    );
}

async fn perform(
    f: &Fixture,
    operation: Operation,
) -> Result<crate::meta::store::FileAttr, MetaError> {
    match operation {
        Operation::Open => f.observer.open(f.ino, OpenFlags::RDONLY).await,
        Operation::SetAttr => {
            f.observer
                .set_attr(
                    f.ino,
                    &SetAttrRequest {
                        mode: Some(0o604),
                        ..Default::default()
                    },
                    SetAttrFlags::empty(),
                )
                .await
        }
    }
}

async fn perform_requiring_write(
    f: &Fixture,
    operation: Operation,
) -> Result<crate::meta::store::FileAttr, MetaError> {
    match operation {
        Operation::Open => {
            scope_namespace_actor(
                Some(policy_actor()),
                f.observer.open(f.ino, OpenFlags::WRONLY | OpenFlags::TRUNC),
            )
            .await
        }
        Operation::SetAttr => {
            let actor = policy_actor();
            f.observer
                .set_attr_as(
                    f.ino,
                    &SetAttrRequest {
                        size: Some(0),
                        ..Default::default()
                    },
                    SetAttrFlags::empty(),
                    actor.uid,
                    &actor.groups,
                )
                .await
        }
    }
}

/// Leave the operation unpolled immediately after its real Sleep returns Pending.
/// No duration-based sleep is used to guess whether the operation is waiting.
async fn poll_until_retry_pending<F: Future>(
    mut operation: Pin<&mut F>,
    events: &mut UnboundedReceiver<NativeRetryEvent>,
) -> usize {
    std::future::poll_fn(|cx| {
        assert!(
            operation.as_mut().poll(cx).is_pending(),
            "operation completed before retry wait"
        );
        while let Ok(event) = events.try_recv() {
            if let NativeRetryEvent::Pending { attempt } = event {
                return Poll::Ready(attempt);
            }
        }
        Poll::Pending
    })
    .await
}

fn scheduled(events: &mut UnboundedReceiver<NativeRetryEvent>) -> Vec<(usize, u64)> {
    std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            NativeRetryEvent::Scheduled { attempt, millis } => Some((attempt, millis)),
            NativeRetryEvent::Pending { .. } => None,
        })
        .collect()
}

#[tokio::test]
async fn native_open_and_setattr_persistent_busy_keep_64_attempts_and_63_waits() {
    for operation in [Operation::Open, Operation::SetAttr] {
        let f = fixture().await;
        let before = f
            .observer
            .permission_snapshot(vec![f.ino], None)
            .await
            .unwrap();
        let mut events = f.observer.observe_native_retry_waits();
        f.observer
            .store()
            .set_failpoint(StoreFailpoint::VersionedMutationAlwaysBusy);
        let error = perform(&f, operation).await.unwrap_err();
        assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EBUSY)));
        assert_eq!(f.observer.store().versioned_mutation_counts(), (64, 0));
        let waits = scheduled(&mut events);
        assert_eq!(
            waits
                .iter()
                .map(|(attempt, _)| *attempt)
                .collect::<Vec<_>>(),
            (0..63).collect::<Vec<_>>()
        );
        assert_eq!(waits.iter().map(|(_, millis)| millis).sum::<u64>(), 5727);
        f.observer.store().set_failpoint(StoreFailpoint::Disabled);
        assert_eq!(
            f.observer
                .permission_snapshot(vec![f.ino], None)
                .await
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn native_open_and_setattr_nonbusy_errors_do_not_wait_or_write() {
    for operation in [Operation::Open, Operation::SetAttr] {
        let f = fixture().await;
        let before = f
            .observer
            .permission_snapshot(vec![f.ino], None)
            .await
            .unwrap();
        let mut events = f.observer.observe_native_retry_waits();
        f.observer
            .store()
            .set_failpoint(StoreFailpoint::VersionedMutationBackendError);
        let error = perform(&f, operation).await.unwrap_err();
        assert!(
            matches!(error, MetaError::Internal(message) if message == "injected versioned mutation backend error")
        );
        assert_eq!(f.observer.store().versioned_mutation_counts(), (1, 0));
        assert!(events.try_recv().is_err());
        f.observer.store().set_failpoint(StoreFailpoint::Disabled);
        assert_eq!(
            f.observer
                .permission_snapshot(vec![f.ino], None)
                .await
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn native_open_and_setattr_cancel_pending_retry_releases_gate_without_write() {
    for operation in [Operation::Open, Operation::SetAttr] {
        let f = fixture().await;
        let before = f
            .observer
            .permission_snapshot(vec![f.ino], None)
            .await
            .unwrap();
        let mut events = f.observer.observe_native_retry_waits();
        f.observer
            .store()
            .set_failpoint(StoreFailpoint::VersionedMutationAlwaysBusy);
        let mut pending = Box::pin(perform(&f, operation));
        let attempt = poll_until_retry_pending(pending.as_mut(), &mut events).await;
        assert!(attempt < 63);
        assert!(f.observer.mutation_gate.try_lock().is_err());
        let counts = f.observer.store().versioned_mutation_counts();
        assert_eq!(counts, (attempt + 1, 0));
        drop(pending);
        let gate = f
            .observer
            .mutation_gate
            .try_lock()
            .expect("cancelled timer must release gate");
        drop(gate);
        assert_eq!(f.observer.store().versioned_mutation_counts(), counts);
        f.observer.store().set_failpoint(StoreFailpoint::Disabled);
        assert_eq!(
            f.observer
                .permission_snapshot(vec![f.ino], None)
                .await
                .unwrap(),
            before
        );
        perform(&f, operation).await.unwrap();
        assert_eq!(
            f.observer.store().versioned_mutation_counts(),
            (counts.0 + 1, 1)
        );
    }
}

#[tokio::test]
async fn native_open_and_setattr_recheck_revocation_after_real_sqlite_conflict() {
    for (policy, operation) in [
        (RevokedPolicy::Posix, Operation::Open),
        (RevokedPolicy::Posix, Operation::SetAttr),
        (RevokedPolicy::ControlOnly, Operation::Open),
        (RevokedPolicy::ControlOnly, Operation::SetAttr),
    ] {
        let f = fixture().await;
        f.writer
            .truncate(f.ino, 31, crate::chunk::layout::DEFAULT_CHUNK_SIZE)
            .await
            .unwrap();
        f.writer
            .set_xattr(f.ino, "system.posix_acl_access", &named_acl(0o666), 0)
            .await
            .unwrap();
        f.writer
            .set_xattr(
                f.ino,
                "system.brewfs.acl",
                &control_acl_json("access", Some("rwx")),
                0,
            )
            .await
            .unwrap();
        if matches!(policy, RevokedPolicy::ControlOnly) {
            f.writer
                .remove_xattr(f.ino, "system.posix_acl_access")
                .await
                .unwrap();
        }
        let allowed = f
            .writer
            .permission_snapshot(vec![f.ino], None)
            .await
            .unwrap();
        assert_effective_write_permission(&allowed, f.ino, operation, policy, true);
        let mut events = f.observer.observe_native_retry_waits();
        let (entered, resume) = f.observer.store().hold_next_versioned_mutation();
        let mut pending = Box::pin(perform_requiring_write(&f, operation));
        tokio::select! {
            result = &mut pending => panic!("{operation:?}/{policy:?}: returned before real CAS gate: {result:?}"),
            _ = entered.notified() => {}
        }
        // Advance the real layer version after policy capture. No failpoint
        // supplies Busy: the original SQLite expected-layer comparison must.
        f.writer
            .set_xattr(f.ino, "user.retry-marker", b"committed", 0)
            .await
            .unwrap();
        assert_eq!(f.observer.store().versioned_mutation_counts(), (1, 0));
        // Revoke after the old permission capture, while still held outside
        // the write transaction. The rejected CAS must be followed by fresh
        // authorization, regardless of whether the 1 ms timer yields Pending.
        match policy {
            RevokedPolicy::Posix => {
                f.writer
                    .set_xattr(f.ino, "system.posix_acl_access", &named_acl(0o600), 0)
                    .await
                    .unwrap();
            }
            RevokedPolicy::ControlOnly => {
                f.writer
                    .set_xattr(
                        f.ino,
                        "system.brewfs.acl",
                        &control_acl_json("access", Some("---")),
                        0,
                    )
                    .await
                    .unwrap();
            }
        }
        let before = f
            .writer
            .permission_snapshot(vec![f.ino], None)
            .await
            .unwrap();
        assert_effective_write_permission(&before, f.ino, operation, policy, false);
        resume.notify_one();
        let error = match pending.await {
            Err(error) => error,
            Ok(attr) => {
                panic!("{operation:?}/{policy:?}: revoked write unexpectedly succeeded: {attr:?}")
            }
        };
        assert!(
            matches!(&error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EACCES)),
            "{operation:?}/{policy:?}: expected EACCES, got {error:?}"
        );
        assert_eq!(
            f.observer.store().versioned_mutation_counts(),
            (1, 0),
            "{operation:?}/{policy:?}"
        );
        assert_eq!(
            scheduled(&mut events),
            vec![(0, 1)],
            "{operation:?}/{policy:?}"
        );
        assert_eq!(
            f.writer
                .permission_snapshot(vec![f.ino], None)
                .await
                .unwrap(),
            before,
            "{operation:?}/{policy:?}"
        );
        assert_eq!(
            f.writer.stat(f.ino).await.unwrap().unwrap().size,
            31,
            "{operation:?}/{policy:?}"
        );
    }
}

#[tokio::test]
async fn native_setattr_refreshes_acl_and_commits_after_real_sqlite_conflict() {
    let f = fixture().await;
    let before = f.writer.inode_permissions(f.ino).await.unwrap().unwrap();
    let requested_atime = before.attr.atime.checked_add(1_000_000_000).unwrap();
    let request = SetAttrRequest {
        atime: Some(requested_atime),
        ..Default::default()
    };
    let mut events = f.observer.observe_native_retry_waits();
    let (entered, resume) = f.observer.store().hold_next_versioned_mutation();
    let mut pending = Box::pin(f.observer.set_attr(f.ino, &request, SetAttrFlags::empty()));
    tokio::select! {
        result = &mut pending => panic!("operation returned before real CAS gate: {result:?}"),
        _ = entered.notified() => {}
    }
    f.writer
        .set_attr(
            f.ino,
            &SetAttrRequest {
                mode: Some(0o604),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    resume.notify_one();
    pending.await.unwrap();
    let after = f.writer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(after.attr.mode & 0o777, 0o604);
    assert_eq!(after.access_acl, Some(named_acl(0o604)));
    assert_eq!(after.attr.atime, requested_atime);
    assert_eq!(f.observer.store().versioned_mutation_counts(), (2, 1));
    assert_eq!(scheduled(&mut events), vec![(0, 1)]);
}
