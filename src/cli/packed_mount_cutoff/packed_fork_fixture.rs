//! Test-only real kernel mount and original clean shutdown for the fork fixture.
//! No cutoff/proof constructor is exposed outside the owning CLI module.

use super::*;
use crate::chunk::BlockStore;
use crate::fuse::mount::{FuseConcurrencyConfig, mount_vfs_privileged, mount_vfs_unprivileged};
use crate::vfs::fs::VFS;
use crate::workspace::catalog::WorkspaceStore;
use crate::workspace::lifecycle::WorkspaceMountSession;
use crate::workspace::meta_layer::WorkspaceMetaLayer;
use crate::workspace::packed_shutdown::VerifiedCleanPackedShutdown;
use crate::workspace::stores::kv_store::packed_admin::PackedReleasedMountReference;

pub(crate) async fn write_and_release<S, W>(
    vfs: VFS<S, WorkspaceMetaLayer<W>>,
    store: Arc<W>,
    mut session: WorkspaceMountSession<W>,
    budget: Arc<SnapshotMountBudget>,
) -> PackedReleasedMountReference
where
    S: BlockStore + Send + Sync + 'static,
    W: WorkspaceStore + 'static,
{
    let reference = session
        .packed_mount_reference()
        .expect("actual packed joint mount");
    let directory = tempfile::tempdir().unwrap();
    let path = prepare(directory.path(), &budget).unwrap();
    let concurrency = FuseConcurrencyConfig {
        worker_count: 2,
        max_background: 8,
    };
    session.mark_packed_attachment_attempted();
    let handle = if std::env::var("BREWFS_TEST_PRIVILEGED_FUSE").ok().as_deref() == Some("1") {
        mount_vfs_privileged(vfs.clone(), &path, concurrency)
            .await
            .unwrap()
    } else {
        mount_vfs_unprivileged(vfs.clone(), &path, concurrency)
            .await
            .unwrap()
    };
    let identity = match OwnedPackedMountIdentity::capture(&path, vfs.clone(), budget.clone()) {
        Ok(identity) => identity,
        Err(error) => {
            handle
                .unmount()
                .await
                .expect("join actual child mount after identity error");
            panic!("actual child mount identity failed: {error}");
        }
    };
    let operations = tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let target = path.join("child-repacked");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .unwrap();
        file.write_all(b"actual child repack").unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert_eq!(std::fs::read(target).unwrap(), b"actual child repack");
    })
    .await;
    // Always join the actual FUSE workers before propagating a kernel-operation
    // panic. A status flag cannot substitute for this owned mount's cutoff.
    handle.unmount().await.unwrap();
    if let Err(error) = operations {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("actual child kernel operations cancelled: {error}");
    }
    let cutoff = identity.after_worker_join().unwrap();
    cutoff.validate().unwrap();
    session.close_packed_renewals_for_shutdown().await.unwrap();
    let drain = vfs.quiesce_packed_vfs().await.unwrap();
    let proof = VerifiedCleanPackedShutdown::from_original_shutdown(
        reference.clone(),
        drain,
        cutoff,
        &store,
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(&proof.budget(), &budget));
    assert!(!budget.state().closed);
    session.release_clean(proof).await.unwrap();
    assert!(budget.state().closed);
    reference
}
