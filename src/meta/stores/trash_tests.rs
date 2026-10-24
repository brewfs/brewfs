//! Exercise the same control-plane trash contract against each xattr backend.
use crate::control::protocol::{ControlRequest, ControlResponse};
use crate::control::server::ControlHandler;
use crate::meta::client::MetaClient;
use crate::meta::config::{CacheCapacity, CacheTtl};
use crate::meta::{MetaLayer, MetaStore};
use std::sync::Arc;

const TRASH_XATTR: &str = "system.brewfs.trash";

async fn trash_path<T: MetaStore>(store: &T, ino: i64) -> String {
    let raw = store.get_xattr(ino, TRASH_XATTR).await.unwrap().unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    metadata["original_path"].as_str().unwrap().to_string()
}

pub(super) async fn rename_overwrite_trash_is_restorable<T: MetaStore + 'static>(store: T) {
    let store = Arc::new(store);
    let client = MetaClient::new(store.clone(), CacheCapacity::default(), CacheTtl::default());
    let docs = client.mkdir(1, "docs".into()).await.unwrap();
    let dst = client
        .create_file(docs, "destination".into())
        .await
        .unwrap();
    let src = client.create_file(1, "source".into()).await.unwrap();
    let chunk_id = crate::vfs::chunk_id_for(dst, 0).unwrap();
    let slice = crate::chunk::SliceDesc {
        slice_id: 42,
        chunk_id,
        offset: 0,
        length: 12,
    };
    store.append_slice(chunk_id, slice).await.unwrap();
    store.extend_file_size(dst, 12).await.unwrap();
    store
        .set_xattr(dst, "user.keep", b"original contents", 0)
        .await
        .unwrap();
    client
        .rename(1, "source", docs, "destination".into())
        .await
        .unwrap();
    assert_eq!(store.lookup(docs, "destination").await.unwrap(), Some(src));

    // Read through a fresh client, as after losing the renaming process.
    let recovered = MetaClient::new(store.clone(), CacheCapacity::default(), CacheTtl::default());
    let ControlResponse::Trash { entries } = recovered.handle(ControlRequest::ListTrash).await
    else {
        panic!("expected trash listing");
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, dst.to_string());
    eprintln!("displaced inode trash entry: {:?}", entries[0]);
    // Free the original name so a name conflict cannot mask missing metadata.
    recovered
        .rename(docs, "destination", docs, "moved-source".into())
        .await
        .unwrap();
    let restored = recovered
        .handle(ControlRequest::RestoreTrashEntry {
            entry_id: entries[0].id.clone(),
        })
        .await;
    assert!(
        matches!(restored, ControlResponse::TrashRestored { .. }),
        "{restored:?}"
    );
    assert_eq!(entries[0].original_path, "/docs/destination");
    assert_eq!(entries[0].size, Some(12));
    assert!(entries[0].deleted_at.is_some());
    assert_eq!(store.lookup(docs, "destination").await.unwrap(), Some(dst));
    assert_eq!(store.get_slices(chunk_id).await.unwrap(), vec![slice]);
    assert_eq!(store.stat(dst).await.unwrap().unwrap().size, 12);
    assert_eq!(
        store.get_xattr(dst, "user.keep").await.unwrap(),
        Some(b"original contents".to_vec())
    );
    assert!(store.get_deleted_files().await.unwrap().is_empty());
}

pub(super) async fn rename_trash_last_link_and_retry<T: MetaStore + 'static>(store: T) {
    let store = Arc::new(store);
    let client = MetaClient::new(store.clone(), CacheCapacity::default(), CacheTtl::default());
    let dir = client.mkdir(1, "before".into()).await.unwrap();
    let dst = client.create_file(dir, "dst".into()).await.unwrap();
    let src = client.create_file(1, "src".into()).await.unwrap();
    client.link(dst, dir, "last:link").await.unwrap();
    let previous = br#"{"original_path":"/obsolete","deleted_at":"old"}"#;
    store
        .set_xattr(dst, TRASH_XATTR, previous, 0)
        .await
        .unwrap();
    client.rename(1, "src", dir, "dst".into()).await.unwrap();
    assert_eq!(store.stat(dst).await.unwrap().unwrap().nlink, 1);
    assert!(store.get_deleted_files().await.unwrap().is_empty());
    assert_eq!(
        store.get_xattr(dst, TRASH_XATTR).await.unwrap().unwrap(),
        previous
    );

    // A live inode's old record is inert. A subsequent last-link replacement
    // must record the current path, even if a client's ancestor cache is stale.
    let denied = client
        .handle(ControlRequest::RestoreTrashEntry {
            entry_id: dst.to_string(),
        })
        .await;
    assert!(matches!(denied, ControlResponse::Error { code, .. } if code == "not_found"));
    store.rename(1, "before", 1, "after".into()).await.unwrap();
    store
        .rename(dir, "dst", dir, "last:link".into())
        .await
        .unwrap();
    assert_eq!(trash_path(store.as_ref(), dst).await, "/after/last:link");
    let committed = store.get_xattr(dst, TRASH_XATTR).await.unwrap();
    assert!(
        store
            .rename(dir, "dst", dir, "last:link".into())
            .await
            .is_err()
    );
    assert_eq!(store.get_xattr(dst, TRASH_XATTR).await.unwrap(), committed);
    assert_eq!(store.lookup(dir, "last:link").await.unwrap(), Some(src));

    // Unlink/restore/re-delete the same inode: each namespace transaction owns
    // its record, with no delayed client cleanup able to erase the next one.
    store
        .restore_deleted_file(dst, dir, "restored".into())
        .await
        .unwrap();
    assert!(store.get_xattr(dst, TRASH_XATTR).await.unwrap().is_none());
    store.unlink(dir, "restored").await.unwrap();
    assert_eq!(trash_path(store.as_ref(), dst).await, "/after/restored");
    store
        .restore_deleted_file(dst, dir, "again".into())
        .await
        .unwrap();
    store
        .rename(dir, "last:link", dir, "again".into())
        .await
        .unwrap();
    assert_eq!(trash_path(store.as_ref(), dst).await, "/after/again");
}

pub(super) async fn rename_trash_noop_and_rejections<T: MetaStore>(store: T) {
    let src = store.create_file(1, "src".into()).await.unwrap();
    let dst = store.create_file(1, "dst".into()).await.unwrap();
    let dir = store.mkdir(1, "dir".into()).await.unwrap();
    assert!(
        store
            .rename_noreplace(1, "src", 1, "dst".into())
            .await
            .is_err()
    );
    assert!(store.rename(1, "missing", 1, "dst".into()).await.is_err());
    assert!(store.rename(1, "dir", 1, "dst".into()).await.is_err());
    assert!(store.rename(1, "src", 1, "dir".into()).await.is_err());
    store.rename(1, "src", dir, "new".into()).await.unwrap();
    store.link(src, dir, "alias").await.unwrap();
    store.rename(dir, "new", dir, "alias".into()).await.unwrap();
    assert_eq!(store.lookup(dir, "new").await.unwrap(), Some(src));
    assert_eq!(store.stat(src).await.unwrap().unwrap().nlink, 2);
    store.rename(1, "dir", 1, "moved".into()).await.unwrap();
    let empty = store.mkdir(1, "empty".into()).await.unwrap();
    store.rename(1, "moved", 1, "empty".into()).await.unwrap();
    assert!(store.stat(empty).await.unwrap().is_none());
    for ino in [src, dst, dir] {
        assert!(store.get_xattr(ino, TRASH_XATTR).await.unwrap().is_none());
    }
    assert!(store.get_deleted_files().await.unwrap().is_empty());
}

pub(super) async fn unlink_trash_is_restorable<T: MetaStore + 'static>(store: T) {
    let store = Arc::new(store);
    let client = MetaClient::new(store.clone(), CacheCapacity::default(), CacheTtl::default());
    let ino = client.create_file(1, "unlinked".into()).await.unwrap();
    client.unlink(1, "unlinked").await.unwrap();
    assert_eq!(trash_path(store.as_ref(), ino).await, "/unlinked");
    let restored = client
        .handle(ControlRequest::RestoreTrashEntry {
            entry_id: ino.to_string(),
        })
        .await;
    assert!(
        matches!(restored, ControlResponse::TrashRestored { .. }),
        "{restored:?}"
    );
    assert_eq!(store.lookup(1, "unlinked").await.unwrap(), Some(ino));
    assert!(store.get_xattr(ino, TRASH_XATTR).await.unwrap().is_none());
}
