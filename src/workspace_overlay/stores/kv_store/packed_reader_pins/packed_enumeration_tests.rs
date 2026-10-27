//! Real fixture regressions for merged raw enumeration and retained ownership.

use super::*;
use crate::workspace_overlay::packed_v3::wire005::V3BudgetPool;
use futures::{FutureExt, StreamExt};

#[tokio::test]
async fn packed_directory_lease_fence_ignores_attribute_commits_but_not_namespace_changes() {
    use crate::workspace_overlay::lifecycle::LeaseValidity;
    let lease = Arc::new(LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(60),
    ));
    let f = fixture_with_lease(false, packed().await, Some(lease.clone())).await;
    for index in 0..73 {
        f.meta
            .create_file(1, format!("native-{index:03}"))
            .await
            .unwrap();
    }
    let directory = f.meta.opendir(1).await.unwrap();
    // A first metadata-freshness check may discard cached rows without
    // changing any directory entry or the meaning of a published cookie.
    f.meta.invalidate_upper_rows();
    assert_eq!(
        directory
            .get_entries_page_raw_owned(0, 1)
            .await
            .unwrap()
            .entries[0]
            .name,
        b"native-000"
    );
    for offset in 0..74 {
        // Each chmod advances the writable head, but cannot change a cookie's
        // meaning. Include more than one merge page and checkpoint eviction.
        f.meta
            .chmod(1, if offset % 2 == 0 { 0o755 } else { 0o777 })
            .await
            .unwrap();
        f.meta.invalidate_upper_rows();
        let page = directory
            .get_entries_page_raw_owned(offset, 1)
            .await
            .expect("an attribute-only commit must not invalidate directory cookies");
        assert_eq!(page.entries.len(), 1);
        let expected = if offset == 73 {
            b"nonzero".to_vec()
        } else {
            format!("native-{offset:03}").into_bytes()
        };
        assert_eq!(page.entries[0].name, expected);
    }
    assert!(
        directory
            .get_entries_page_raw_owned(74, 1)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(
        directory
            .get_entries_page_raw_owned(2, 1)
            .await
            .unwrap()
            .entries[0]
            .name,
        b"native-002"
    );
    f.meta.create_file(1, "later".into()).await.unwrap();
    assert!(matches!(directory.get_entries_page_raw_owned(3, 1).await,
        Err(crate::meta::store::MetaError::Io(error)) if error.raw_os_error() == Some(libc::EBUSY)));
    let reopened = f.meta.opendir(1).await.unwrap();
    assert_eq!(
        reopened
            .get_entries_page_raw_owned(0, 1)
            .await
            .unwrap()
            .entries[0]
            .name,
        b"later"
    );
    lease.revoke();
    assert!(
        reopened.get_entries_page_raw_owned(1, 1).await.is_err(),
        "a locally unchanged namespace is insufficient after losing the lease"
    );
    drop(reopened);
    drop(directory);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_directory_lease_fence_survives_lower_file_copy_up_and_data_commit() {
    use crate::workspace_overlay::lifecycle::LeaseValidity;
    let lease = Arc::new(LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(60),
    ));
    let f = fixture_with_lease(false, packed().await, Some(lease)).await;
    let ino = f.meta.lookup(1, "nonzero").await.unwrap().unwrap();
    let directory = f.meta.opendir(1).await.unwrap();
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
    f.meta
        .write(
            ino,
            chunk_id,
            crate::chunk::SliceDesc {
                slice_id: 50_000,
                chunk_id,
                offset: 0,
                length: 64,
            },
            64,
        )
        .await
        .unwrap();
    // A freshness refresh after this head-sequence change still must not
    // invalidate directory cookies: no dentry changed.
    f.meta.invalidate_upper_rows();
    // Opening saw no native dentries. Copying up the file's data/inode must
    // preserve both the first page and EOF of the captured directory view.
    assert_eq!(
        directory
            .get_entries_page_raw_owned(0, 1)
            .await
            .unwrap()
            .entries[0]
            .name,
        b"nonzero"
    );
    assert!(
        directory
            .get_entries_page_raw_owned(1, 1)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    let view = f.meta.view_context().await;
    f.meta.replace_view_context(view).await;
    assert!(
        directory.get_entries_page_raw_owned(0, 1).await.is_err(),
        "an explicit view replacement must invalidate captured local authority"
    );
    drop(directory);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_directory_lease_fence_survives_last_close_of_an_unlinked_inode() {
    use crate::workspace_overlay::lifecycle::LeaseValidity;
    let lease = Arc::new(LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(60),
    ));
    let f = fixture_with_lease(false, packed().await, Some(lease)).await;
    let ino = f.meta.create_file(1, "orphan".into()).await.unwrap();
    let attr = f.meta.stat(ino).await.unwrap().unwrap();
    f.meta
        .record_open(ino, attr, true, false, false)
        .await
        .unwrap();
    f.meta.unlink(1, "orphan").await.unwrap();
    assert_eq!(f.meta.stat(ino).await.unwrap().unwrap().nlink, 0);
    // The name has already disappeared before this handle captures its
    // namespace. Reclaiming the remaining open inode changes no cookie.
    let directory = f.meta.opendir(1).await.unwrap();
    assert_eq!(
        directory
            .get_entries_page_raw_owned(0, 1)
            .await
            .unwrap()
            .entries[0]
            .name,
        b"nonzero"
    );
    f.meta.record_close(ino).await.unwrap();
    assert!(f.meta.stat(ino).await.unwrap().is_none());
    let page = directory
        .get_entries_page_raw_owned(0, 1)
        .await
        .expect("orphan reclamation must not invalidate unchanged directory cookies");
    assert_eq!(page.entries[0].name, b"nonzero");
    assert!(
        directory
            .get_entries_page_raw_owned(1, 1)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    drop(page);
    drop(directory);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_workspace_opendir_retries_a_concurrent_head_change() {
    let f = fixture(false).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *f.backend.page_barrier.lock().await = Some((entered.clone(), resume.clone()));
    let meta = f.meta.clone();
    let opening = tokio::spawn(async move { meta.opendir(1).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .expect("opendir reached the native directory page");
    // A metadata commit can race a multi-step open even when the captured
    // application's requests are serialized (for example, a delayed flush).
    f.meta.create_file(1, "during-open".into()).await.unwrap();
    resume.notify_one();
    let directory = tokio::time::timeout(std::time::Duration::from_secs(2), opening)
        .await
        .unwrap()
        .unwrap()
        .expect("opendir must rebuild a changed view before publishing a handle");
    let page = directory.get_entries_page_raw_owned(0, 256).await.unwrap();
    assert!(
        page.entries
            .iter()
            .any(|entry| entry.name == b"during-open")
    );
    drop(page);
    drop(directory);
    tokio::time::timeout(std::time::Duration::from_secs(2), f.reader.shutdown())
        .await
        .expect("discarded attempts must release their reader owners")
        .unwrap();
}

#[tokio::test]
async fn packed_workspace_fuse_readdir_stream_retains_page_and_reply_until_final_drop() {
    use asyncfuse::raw::{Filesystem, Request};
    let f = fixture(false).await;
    let output_pool = V3BudgetPool::Output as usize;
    let baseline = f.budget.state().used[output_pool];
    let fh = f.vfs.opendir(1).await.unwrap();
    let mut reply = Filesystem::readdir(&f.vfs, Request::default(), 1, fh, 0)
        .await
        .unwrap();
    assert_eq!(reply.entries.next().await.unwrap().unwrap().offset, 1);
    f.vfs.closedir(fh).unwrap();
    let retained =
        baseline + (128 << 10) + (256 << 10) + V3MountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES;
    assert_eq!(f.budget.state().used[output_pool], retained);
    let reader = f.reader.clone();
    let mut shutdown = tokio::spawn(async move { reader.shutdown().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.budget.state().used[output_pool], retained);
    // Simulate a kernel reply consumer abandoning a partially read stream.
    drop(reply);
    assert_eq!(f.budget.state().used[output_pool], baseline);
    tokio::time::timeout(std::time::Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn packed_workspace_fuse_readdirplus_stream_retains_page_and_reply_until_final_drop() {
    use asyncfuse::raw::{Filesystem, Request};
    let f = fixture(false).await;
    let output_pool = V3BudgetPool::Output as usize;
    let baseline = f.budget.state().used[output_pool];
    let fh = f.vfs.opendir(1).await.unwrap();
    let mut reply = Filesystem::readdirplus(&f.vfs, Request::default(), 1, fh, 0, 0)
        .await
        .unwrap();
    assert_eq!(reply.entries.next().await.unwrap().unwrap().offset, 1);
    f.vfs.closedir(fh).unwrap();
    let retained =
        baseline + (128 << 10) + (256 << 10) + V3MountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES;
    assert_eq!(f.budget.state().used[output_pool], retained);
    let reader = f.reader.clone();
    let mut shutdown = tokio::spawn(async move { reader.shutdown().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.budget.state().used[output_pool], retained);
    drop(reply);
    assert_eq!(f.budget.state().used[output_pool], baseline);
    tokio::time::timeout(std::time::Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn packed_workspace_common_fuse_memory_reservations_delegate_and_fail_when_closed() {
    use crate::meta::layer::MetadataMemoryKind;
    use asyncfuse::raw::Filesystem;
    let f = fixture(false).await;
    let baseline = f.budget.state().used;
    let kinds = [
        MetadataMemoryKind::Roots,
        MetadataMemoryKind::Request,
        MetadataMemoryKind::Reply,
        MetadataMemoryKind::Handle,
        MetadataMemoryKind::Control,
    ];
    for kind in kinds {
        let mut expected = baseline;
        match kind {
            MetadataMemoryKind::Roots => expected[V3BudgetPool::Roots as usize] += 513,
            MetadataMemoryKind::Request => {
                expected[V3BudgetPool::Control as usize] += 8192;
                expected[V3BudgetPool::Metadata as usize] += (2 << 20) + 513;
            }
            MetadataMemoryKind::Reply => {
                expected[V3BudgetPool::Output as usize] +=
                    513 + V3MountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES;
                expected[V3BudgetPool::Control as usize] += 4096;
            }
            MetadataMemoryKind::Handle => expected[V3BudgetPool::Metadata as usize] += 8192,
            MetadataMemoryKind::Control => expected[V3BudgetPool::Control as usize] += 513,
        }
        let guard = f.meta.reserve_memory(kind, 513).unwrap().unwrap();
        assert_eq!(f.budget.state().used, expected, "{kind:?}");
        drop(guard);
        assert_eq!(f.budget.state().used, baseline);
    }
    let request = Filesystem::reserve_request_memory(&f.vfs, 513)
        .unwrap()
        .unwrap();
    assert_eq!(
        f.budget.state().used[V3BudgetPool::Metadata as usize],
        baseline[V3BudgetPool::Metadata as usize] + (2 << 20) + 513
    );
    drop(request);
    let reply = Filesystem::reserve_reply_memory(&f.vfs, 513)
        .unwrap()
        .unwrap();
    assert_eq!(
        f.budget.state().used[V3BudgetPool::Output as usize],
        baseline[V3BudgetPool::Output as usize]
            + 513
            + V3MountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES
    );
    drop(reply);
    assert_eq!(f.budget.state().used, baseline);
    // The same KV store without a packed lower preserves native's opt-out.
    let native =
        WorkspaceMetaLayer::with_chunk_size(f.store.clone(), f.meta.view_context().await, CHUNK);
    for kind in kinds {
        assert!(native.reserve_memory(kind, 513).unwrap().is_none());
    }
    f.reader.shutdown().await.unwrap();
    f.budget.close();
    for kind in kinds {
        assert!(f.meta.reserve_memory(kind, 513).is_err(), "{kind:?}");
        assert!(native.reserve_memory(kind, 513).unwrap().is_none());
    }
    assert!(Filesystem::reserve_request_memory(&f.vfs, 513).is_err());
    assert!(Filesystem::reserve_reply_memory(&f.vfs, 513).is_err());
}

#[tokio::test]
async fn packed_workspace_directory_page_retains_reader_after_directory_is_released() {
    let f = fixture(false).await;
    let directory = f.meta.opendir(1).await.unwrap();
    let page = directory.get_entries_page_raw_owned(0, 1).await.unwrap();
    drop(directory);
    let reader = f.reader.clone();
    let mut shutdown = tokio::spawn(async move { reader.shutdown().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(page.entries[0].name, b"nonzero");
    drop(page);
    tokio::time::timeout(std::time::Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn packed_workspace_raw_directory_merges_whiteout_and_upper_without_duplicates() {
    let f = fixture(false).await;
    f.vfs.link("/nonzero", "/replacement").await.unwrap();
    add_raw_alias(&f).await;
    f.vfs.unlink("/nonzero").await.unwrap();
    f.vfs.create_file("/upper-only").await.unwrap();
    let directory = f.meta.opendir(1).await.unwrap();
    assert!(directory.is_paged());
    let first = directory.get_entries_page_raw_owned(0, 2).await.unwrap();
    let second = directory.get_entries_page_raw_owned(2, 2).await.unwrap();
    let names: Vec<_> = first
        .entries
        .iter()
        .chain(&second.entries)
        .map(|entry| entry.name.clone())
        .collect();
    assert_eq!(
        names,
        vec![
            RAW_ALIAS.to_vec(),
            b"replacement".to_vec(),
            b"upper-only".to_vec()
        ]
    );
    assert!(
        directory
            .get_entries_page_raw_owned(3, 1)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    drop(first);
    drop(second);
    drop(directory);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_workspace_directory_restores_evicted_checkpoint_and_fences_same_epoch_mutation() {
    let f = fixture(false).await;
    for index in 0..73 {
        f.vfs
            .create_file(&format!("/native-{index:03}"))
            .await
            .unwrap();
    }
    let directory = f.meta.opendir(1).await.unwrap();
    for offset in 0..74 {
        let page = directory
            .get_entries_page_raw_owned(offset, 1)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 1);
    }
    // Offset 2 is no longer one of the last 16 cached positions.
    let restored = directory.get_entries_page_raw_owned(2, 1).await.unwrap();
    assert_eq!(restored.entries[0].name, b"native-002");
    let epoch = f.meta.view_context().await.head_epoch;
    f.vfs.create_file("/later").await.unwrap();
    assert_eq!(f.meta.view_context().await.head_epoch, epoch);
    assert!(matches!(directory.get_entries_page_raw_owned(3, 1).await,
        Err(crate::meta::store::MetaError::Io(error)) if error.raw_os_error() == Some(libc::EBUSY)));
    let reopened = f.meta.opendir(1).await.unwrap();
    assert_eq!(
        reopened
            .get_entries_page_raw_owned(0, 1)
            .await
            .unwrap()
            .entries[0]
            .name,
        b"later"
    );
    drop(restored);
    drop(reopened);
    drop(directory);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_workspace_directory_output_owner_and_cancel_release_exact_charges() {
    let f = fixture(false).await;
    let directory = f.meta.opendir(1).await.unwrap();
    let output_pool = V3BudgetPool::Output as usize;
    let before = f.budget.state().used[output_pool];
    let page = directory.get_entries_page_raw_owned(0, 256).await.unwrap();
    assert_eq!(page.entries[0].name, b"nonzero");
    assert_eq!(f.budget.state().used[output_pool], before + (128 << 10));
    drop(page);
    assert_eq!(f.budget.state().used[output_pool], before);
    // Block the actual backend read at the already-admitted validation await.
    let rows = f.backend.rows.lock().await;
    let baseline = f.budget.state().used;
    let mut pending = Box::pin(directory.get_entries_page_raw_owned(0, 256));
    assert!(pending.as_mut().now_or_never().is_none());
    assert!(f.budget.state().used[output_pool] > baseline[output_pool]);
    drop(pending);
    assert_eq!(f.budget.state().used, baseline);
    drop(rows);
    drop(directory);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_workspace_rmdir_uses_effective_first_page_for_upper_only_directory() {
    let f = fixture(false).await;
    f.vfs.mkdir_err("/empty").await.unwrap();
    f.vfs.rmdir("/empty").await.unwrap();
    assert!(f.vfs.stat("/empty").await.is_err());
    f.vfs.mkdir_err("/occupied").await.unwrap();
    f.vfs.create_file("/occupied/child").await.unwrap();
    assert!(matches!(
        f.vfs.rmdir("/occupied").await,
        Err(crate::vfs::error::VfsError::DirectoryNotEmpty { .. })
    ));
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_workspace_rename_directory_replaces_empty_and_rejects_nonempty_destination() {
    use crate::vfs::error::VfsError;
    let f = fixture(false).await;
    let source = f.vfs.mkdir_err("/source").await.unwrap();
    let payload = f.vfs.create_file("/source/payload").await.unwrap();
    let empty = f.vfs.mkdir_err("/empty").await.unwrap();
    assert_ne!(source, empty);
    let output_pool = V3BudgetPool::Output as usize;
    let baseline = f.budget.state().used[output_pool];
    f.vfs.can_rename("/source", "/empty").await.unwrap();
    assert_eq!(f.budget.state().used[output_pool], baseline);
    f.vfs.rename("/source", "/empty").await.unwrap();
    assert!(f.vfs.stat("/source").await.is_err());
    assert_eq!(f.vfs.stat("/empty").await.unwrap().ino, source);
    assert_eq!(f.vfs.stat("/empty/payload").await.unwrap().ino, payload);

    let occupied = f.vfs.mkdir_err("/occupied").await.unwrap();
    let child = f.vfs.create_file("/occupied/child").await.unwrap();
    let baseline = f.budget.state().used[output_pool];
    assert!(matches!(
        f.vfs.can_rename("/empty", "/occupied").await,
        Err(VfsError::DirectoryNotEmpty { .. })
    ));
    assert_eq!(f.budget.state().used[output_pool], baseline);
    assert!(matches!(
        f.vfs.rename("/empty", "/occupied").await,
        Err(VfsError::DirectoryNotEmpty { .. })
    ));
    assert_eq!(f.vfs.stat("/empty").await.unwrap().ino, source);
    assert_eq!(f.vfs.stat("/empty/payload").await.unwrap().ino, payload);
    assert_eq!(f.vfs.stat("/occupied").await.unwrap().ino, occupied);
    assert_eq!(f.vfs.stat("/occupied/child").await.unwrap().ino, child);
    f.reader.shutdown().await.unwrap();
}
