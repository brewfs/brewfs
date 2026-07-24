use super::*;
use crate::chunk::SliceDesc;
use crate::meta::config::{
    CacheConfig, ClientOptions, CompactConfig, DatabaseConfig, DatabaseType,
};
use crate::meta::factory::MetaStoreFactory;
use crate::meta::store::{FileType, MetaStore};

#[test]
fn special_node_round_trip_preserves_kind_mode_and_rdev() {
    let node = StoredNode {
        ino: 42,
        parent: ROOT_INODE,
        name: "null".to_string(),
        kind: StoredNodeKind::CharDevice,
        size: 0,
        blocks: 0,
        mode: FileType::CharDevice.mode_type_bits() | 0o600,
        rdev: libc::makedev(1, 3) as u32,
        uid: 1000,
        gid: 1000,
        atime: 1,
        mtime: 2,
        ctime: 3,
        nlink: 1,
        symlink_target: None,
        deleted: false,
    };

    let encoded = TiKvMetaStore::encode(&node).unwrap();
    let decoded = TiKvMetaStore::decode_node(&encoded).unwrap();
    let attr = decoded.to_attr();

    assert_eq!(attr.kind, FileType::CharDevice);
    assert_eq!(attr.mode, node.mode);
    assert_eq!(attr.rdev, node.rdev);
}

fn test_config(namespace: &str) -> Config {
    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::TiKv {
                pd_endpoints: vec!["127.0.0.1:2379".to_string()],
                namespace: namespace.to_string(),
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
        compact: CompactConfig::default(),
    }
}

fn integration_config(test_name: &str) -> Config {
    let pd_endpoints = std::env::var("BREWFS_TIKV_PD_ENDPOINTS")
        .or_else(|_| std::env::var("BREWFS_META_TIKV_PD_ENDPOINTS"))
        .unwrap_or_else(|_| "127.0.0.1:2379".to_string())
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    let namespace = format!(
        "brewfs-test-{test_name}-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );

    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::TiKv {
                pd_endpoints,
                namespace,
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
        compact: CompactConfig::default(),
    }
}

#[test]
fn parses_tikv_database_config_from_yaml() {
    let config: Config = serde_yaml::from_str(
        r#"
database:
  type: tikv
  pd_endpoints:
    - 127.0.0.1:2379
  namespace: tenant-a
"#,
    )
    .expect("tikv YAML config should parse");

    match config.database.db_config {
        DatabaseType::TiKv {
            pd_endpoints,
            namespace,
        } => {
            assert_eq!(pd_endpoints, vec!["127.0.0.1:2379"]);
            assert_eq!(namespace, "tenant-a");
        }
        other => panic!("expected TiKv config, got {other:?}"),
    }
}

#[test]
fn key_schema_uses_namespace_prefixes() {
    assert_eq!(normalize_namespace("/tenant-a/"), "tenant-a");
    assert_eq!(normalize_namespace(""), default_tikv_namespace());
    assert_eq!(
        TiKvMetaStore::scoped_key("tenant-a", "inode/1"),
        b"tenant-a/inode/1".to_vec()
    );
    assert_eq!(
        TiKvMetaStore::scoped_key("tenant-a", "dentry/1/name"),
        b"tenant-a/dentry/1/name".to_vec()
    );
    assert_eq!(
        TiKvMetaStore::scoped_key("tenant-a", "chunk/42"),
        b"tenant-a/chunk/42".to_vec()
    );
    assert_eq!(
        TiKvMetaStore::scoped_key("tenant-a", "counter/brewfs:next_inode_id"),
        b"tenant-a/counter/brewfs:next_inode_id".to_vec()
    );
}

#[test]
fn prefix_range_end_bounds_namespace_scans() {
    assert_eq!(
        prefix_range_end(b"tenant-a/chunk/"),
        Some(b"tenant-a/chunk0".to_vec())
    );
    assert_eq!(prefix_range_end(&[0xff, 0xff]), None);
}

#[test]
fn stored_values_round_trip_through_json() {
    let node = TiKvMetaStore::root_node();
    let encoded = TiKvMetaStore::encode(&node).unwrap();
    let decoded = TiKvMetaStore::decode_node(&encoded).unwrap();

    assert_eq!(decoded.ino, ROOT_INODE);
    assert_eq!(decoded.kind, StoredNodeKind::Dir);
    assert_eq!(decoded.nlink, 2);
    assert!(!decoded.deleted);
    assert_eq!(decoded.symlink_target, None);
}

#[test]
fn stored_node_decodes_legacy_json_defaults() {
    let json = br#"{
        "ino": 2,
        "parent": 1,
        "name": "legacy",
        "kind": "File",
        "size": 0,
        "blocks": 0,
        "mode": 33188,
        "uid": 0,
        "gid": 0,
        "atime": 1,
        "mtime": 1,
        "ctime": 1,
        "nlink": 1
    }"#;
    let decoded = TiKvMetaStore::decode_node(json).unwrap();
    assert!(!decoded.deleted);
    assert_eq!(decoded.symlink_target, None);
}

#[test]
fn relatime_updates_after_metadata_change_or_one_day() {
    let mut node = TiKvMetaStore::root_node();
    node.atime = 30;
    node.mtime = 20;
    node.ctime = 20;

    assert!(!TiKvMetaStore::atime_needs_update(&node, 40));

    node.mtime = 30;
    assert!(TiKvMetaStore::atime_needs_update(&node, 40));

    node.mtime = 20;
    assert!(TiKvMetaStore::atime_needs_update(
        &node,
        node.atime + RELATIME_MAX_AGE_NANOS
    ));
}

#[test]
fn link_parent_values_round_trip() {
    let parents = vec![
        StoredLinkParent {
            parent: 1,
            name: "a".to_string(),
        },
        StoredLinkParent {
            parent: 2,
            name: "b:c".to_string(),
        },
    ];
    let encoded = TiKvMetaStore::encode(&parents).unwrap();
    let decoded = TiKvMetaStore::decode_link_parents(&encoded).unwrap();
    assert_eq!(decoded, parents);
}

#[test]
fn plock_values_round_trip() {
    let sid = Uuid::new_v4();
    let plocks = vec![StoredPlock {
        sid,
        owner: 42,
        records: vec![PlockRecord::new(FileLockType::Write, 1234, 10, 20)],
    }];

    let encoded = TiKvMetaStore::encode(&plocks).unwrap();
    let decoded = TiKvMetaStore::decode_plocks(&encoded).unwrap();

    assert_eq!(decoded, plocks);
}

#[test]
fn plock_update_detects_conflicts_and_unlocks_ranges() {
    let inode = 99;
    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();
    let range = FileLockRange { start: 0, end: 100 };
    let mut plocks = Vec::new();

    TiKvMetaStore::apply_plock_update(
        &mut plocks,
        sid_a,
        1,
        PlockRecord::new(FileLockType::Write, 111, range.start, range.end),
        FileLockType::Write,
        range,
        inode,
    )
    .unwrap();

    let conflict = TiKvMetaStore::apply_plock_update(
        &mut plocks,
        sid_b,
        2,
        PlockRecord::new(FileLockType::Read, 222, 50, 60),
        FileLockType::Read,
        FileLockRange { start: 50, end: 60 },
        inode,
    )
    .expect_err("overlapping read should conflict with another owner's write lock");
    assert!(matches!(
        conflict,
        MetaError::LockConflict {
            inode: 99,
            owner: 2,
            ..
        }
    ));

    TiKvMetaStore::apply_plock_update(
        &mut plocks,
        sid_a,
        1,
        PlockRecord::new(FileLockType::UnLock, 0, 25, 75),
        FileLockType::UnLock,
        FileLockRange { start: 25, end: 75 },
        inode,
    )
    .unwrap();

    assert_eq!(plocks.len(), 1);
    assert_eq!(
        plocks[0].records,
        vec![
            PlockRecord::new(FileLockType::Write, 111, 0, 25),
            PlockRecord::new(FileLockType::Write, 111, 75, 100),
        ]
    );
}

#[test]
fn gc_record_values_round_trip() {
    let delayed = StoredDelayedSliceRecord {
        id: 1,
        slice_id: 101,
        chunk_id: 11,
        offset: 64,
        size: 4096,
        created_at: 1000,
        reason: "compact".to_string(),
        status: "pending".to_string(),
    };
    let encoded = TiKvMetaStore::encode(&delayed).unwrap();
    assert_eq!(
        TiKvMetaStore::decode_delayed_slice(&encoded).unwrap(),
        delayed
    );

    let uncommitted = StoredUncommittedSliceRecord {
        id: 2,
        slice_id: 202,
        chunk_id: 22,
        size: 2048,
        created_at: 1001,
        operation: "write".to_string(),
        status: "pending".to_string(),
    };
    let encoded = TiKvMetaStore::encode(&uncommitted).unwrap();
    assert_eq!(
        TiKvMetaStore::decode_uncommitted_slice(&encoded).unwrap(),
        uncommitted
    );
}

#[test]
fn nlink_delta_saturates() {
    assert_eq!(apply_nlink_delta(2, -1), 1);
    assert_eq!(apply_nlink_delta(0, -1), 0);
    assert_eq!(apply_nlink_delta(u32::MAX, 1), u32::MAX);
}

#[tokio::test]
async fn rejects_empty_pd_endpoints() {
    let mut config = test_config("tenant-a");
    config.database.db_config = DatabaseType::TiKv {
        pd_endpoints: Vec::new(),
        namespace: "tenant-a".to_string(),
    };

    let err = TiKvMetaStore::from_config(config)
        .await
        .expect_err("empty PD endpoint list should be rejected before connecting to TiKV");
    assert!(matches!(err, MetaError::Config(message) if message.contains("PD endpoint")));
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_factory_initializes_root() {
    let handle = MetaStoreFactory::<TiKvMetaStore>::create_from_config(integration_config("root"))
        .await
        .expect("factory should create and initialize tikv store");
    let store = handle.store();

    assert_eq!(store.name(), "tikv");
    assert_eq!(store.root_ino(), 1);
    let root = store.stat(1).await.unwrap().unwrap();
    assert_eq!(root.kind, FileType::Dir);
    assert_eq!(
        store.lookup_path("/").await.unwrap(),
        Some((1, FileType::Dir))
    );
    assert!(store.readdir(1).await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_transactional_namespace_schema() {
    let store = TiKvMetaStore::from_config(integration_config("namespace"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();
    let root = store.root_ino();

    let dir = store.mkdir(root, "dir".to_string()).await.unwrap();
    let file = store.create_file(dir, "file".to_string()).await.unwrap();

    assert_eq!(store.lookup(root, "dir").await.unwrap(), Some(dir));
    assert_eq!(store.lookup(dir, "file").await.unwrap(), Some(file));
    assert_eq!(
        store.lookup_path("/dir/file").await.unwrap(),
        Some((file, FileType::File))
    );

    let file_attr = store.stat(file).await.unwrap().unwrap();
    assert_eq!(file_attr.kind, FileType::File);
    assert_eq!(file_attr.size, 0);

    let entries = store.readdir(dir).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "file");
    assert_eq!(
        store.get_names(file).await.unwrap(),
        vec![(Some(dir), "file".to_string())]
    );
    assert_eq!(
        store.get_paths(file).await.unwrap(),
        vec!["/dir/file".to_string()]
    );

    store
        .rename(dir, "file", root, "moved".to_string())
        .await
        .unwrap();
    assert_eq!(store.lookup(dir, "file").await.unwrap(), None);
    assert_eq!(store.lookup(root, "moved").await.unwrap(), Some(file));
    assert_eq!(
        store.get_paths(file).await.unwrap(),
        vec!["/moved".to_string()]
    );

    store.unlink(root, "moved").await.unwrap();
    let tombstone = store.stat(file).await.unwrap().unwrap();
    assert_eq!(tombstone.nlink, 0);
    assert!(store.get_deleted_files().await.unwrap().contains(&file));
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_rejects_non_empty_rmdir() {
    let store = TiKvMetaStore::from_config(integration_config("rmdir"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();
    let root = store.root_ino();

    let dir = store.mkdir(root, "dir".to_string()).await.unwrap();
    store.create_file(dir, "file".to_string()).await.unwrap();

    let err = store
        .rmdir(root, "dir")
        .await
        .expect_err("non-empty directory should not be removed");
    assert!(matches!(err, MetaError::DirectoryNotEmpty(ino) if ino == dir));

    store.unlink(dir, "file").await.unwrap();
    store.rmdir(root, "dir").await.unwrap();
    assert_eq!(store.lookup(root, "dir").await.unwrap(), None);
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_transactional_file_data_schema() {
    let store = TiKvMetaStore::from_config(integration_config("file-data"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();
    let root = store.root_ino();
    let file = store.create_file(root, "file".to_string()).await.unwrap();
    let first = SliceDesc {
        slice_id: 7,
        chunk_id: 42,
        offset: 0,
        length: 128,
    };
    let second = SliceDesc {
        slice_id: 8,
        chunk_id: 42,
        offset: 128,
        length: 64,
    };

    store.write(file, 42, first, 128).await.unwrap();
    store.append_slice(42, second).await.unwrap();

    assert_eq!(store.get_slices(42).await.unwrap(), vec![first, second]);
    assert_eq!(store.list_chunk_ids(10).await.unwrap(), vec![42]);
    assert_eq!(store.stat(file).await.unwrap().unwrap().size, 128);

    store
        .write(
            file,
            43,
            SliceDesc {
                slice_id: 9,
                chunk_id: 43,
                offset: 0,
                length: 32,
            },
            32,
        )
        .await
        .unwrap();
    assert_eq!(
        store.stat(file).await.unwrap().unwrap().size,
        128,
        "write should not shrink file size when commits arrive out of order"
    );

    store.extend_file_size(file, 256).await.unwrap();
    assert_eq!(store.stat(file).await.unwrap().unwrap().size, 256);
    store.extend_file_size(file, 64).await.unwrap();
    assert_eq!(
        store.stat(file).await.unwrap().unwrap().size,
        256,
        "extend_file_size should be monotonic"
    );

    store.set_file_size(file, 64).await.unwrap();
    assert_eq!(store.stat(file).await.unwrap().unwrap().size, 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_serializes_hot_inode_and_parent_transactions() {
    let store = Arc::new(
        TiKvMetaStore::from_config(integration_config("hot-inode"))
            .await
            .expect("tikv store should connect"),
    );
    store.initialize().await.unwrap();
    let root = store.root_ino();
    let file = store
        .create_file(root, "hot-file".to_string())
        .await
        .unwrap();

    let mut writes = Vec::new();
    for index in 0..64_u64 {
        let store = Arc::clone(&store);
        writes.push(tokio::spawn(async move {
            let chunk_id = 10_000 + index;
            let size = (index + 1) * 4096;
            store
                .write(
                    file,
                    chunk_id,
                    SliceDesc {
                        slice_id: 20_000 + index,
                        chunk_id,
                        offset: 0,
                        length: 4096,
                    },
                    size,
                )
                .await
        }));
    }
    for write in writes {
        write.await.expect("write task should not panic").unwrap();
    }
    assert_eq!(store.stat(file).await.unwrap().unwrap().size, 64 * 4096);

    let mut creates = Vec::new();
    for index in 0..64_u64 {
        let store = Arc::clone(&store);
        creates.push(tokio::spawn(async move {
            store.create_file(root, format!("entry-{index}")).await
        }));
    }
    for create in creates {
        create.await.expect("create task should not panic").unwrap();
    }
    assert_eq!(store.readdir(root).await.unwrap().len(), 65);
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_compaction_gc_workflow() {
    let store = TiKvMetaStore::from_config(integration_config("gc"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();

    let initial = SliceDesc {
        slice_id: 101,
        chunk_id: 11,
        offset: 0,
        length: 4096,
    };
    let replacement = SliceDesc {
        slice_id: 102,
        chunk_id: 11,
        offset: 0,
        length: 2048,
    };
    store.append_slice(11, initial).await.unwrap();

    let delayed = SliceDesc::encode_delayed_data(&[initial], &[initial.slice_id]);
    store
        .replace_slices_for_compact(11, &[replacement], &delayed)
        .await
        .unwrap();
    assert_eq!(store.get_slices(11).await.unwrap(), vec![replacement]);

    let pending = store.process_delayed_slices(10, -1).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0, initial.slice_id);
    assert_eq!(pending[0].1, initial.offset);
    assert_eq!(pending[0].2, initial.length);
    store
        .confirm_delayed_deleted(&[pending[0].3])
        .await
        .unwrap();
    assert!(
        store
            .process_delayed_slices(10, -1)
            .await
            .unwrap()
            .is_empty()
    );

    let uncommitted_id = store
        .record_uncommitted_slice(9001, 11, 8192, "compact_heavy")
        .await
        .unwrap();
    assert!(uncommitted_id > 0);
    assert_eq!(
        store
            .cleanup_orphan_uncommitted_slices(-1, 10)
            .await
            .unwrap(),
        vec![(9001, 8192)]
    );
    store.delete_uncommitted_slices(&[9001]).await.unwrap();
    assert!(
        store
            .cleanup_orphan_uncommitted_slices(-1, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_allocates_generic_counters_transactionally() {
    let store = TiKvMetaStore::from_config(integration_config("counters"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();

    assert_eq!(store.next_id("custom").await.unwrap(), 1);
    assert_eq!(store.next_id("custom").await.unwrap(), 2);
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_rename_overwrites_file_and_tombstones_destination() {
    let store = TiKvMetaStore::from_config(integration_config("rename-overwrite"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();
    let root = store.root_ino();
    let src = store.create_file(root, "src".to_string()).await.unwrap();
    let dst = store.create_file(root, "dst".to_string()).await.unwrap();

    store
        .rename(root, "src", root, "dst".to_string())
        .await
        .unwrap();

    assert_eq!(store.lookup(root, "src").await.unwrap(), None);
    assert_eq!(store.lookup(root, "dst").await.unwrap(), Some(src));
    let dst_attr = store.stat(dst).await.unwrap().unwrap();
    assert_eq!(dst_attr.nlink, 0);
    assert!(store.get_deleted_files().await.unwrap().contains(&dst));
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_rename_exchange_swaps_entries() {
    let store = TiKvMetaStore::from_config(integration_config("rename-exchange"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();
    let root = store.root_ino();
    let a = store.create_file(root, "a".to_string()).await.unwrap();
    let b = store.create_file(root, "b".to_string()).await.unwrap();

    store.rename_exchange(root, "a", root, "b").await.unwrap();

    assert_eq!(store.lookup(root, "a").await.unwrap(), Some(b));
    assert_eq!(store.lookup(root, "b").await.unwrap(), Some(a));
    assert_eq!(store.get_paths(a).await.unwrap(), vec!["/b".to_string()]);
    assert_eq!(store.get_paths(b).await.unwrap(), vec!["/a".to_string()]);
}

#[tokio::test]
#[ignore = "requires a running TiKV/PD cluster; set BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_link_and_symlink_extensions() {
    let store = TiKvMetaStore::from_config(integration_config("links"))
        .await
        .expect("tikv store should connect");
    store.initialize().await.unwrap();
    let root = store.root_ino();
    let file = store.create_file(root, "origin".to_string()).await.unwrap();

    let linked_attr = store.link(file, root, "hard").await.unwrap();
    assert_eq!(linked_attr.nlink, 2);
    assert_eq!(store.lookup(root, "hard").await.unwrap(), Some(file));
    assert_eq!(
        store.get_paths(file).await.unwrap(),
        vec!["/hard".to_string(), "/origin".to_string()]
    );

    let (link_ino, link_attr) = store.symlink(root, "sym", "/origin").await.unwrap();
    assert_eq!(link_attr.kind, FileType::Symlink);
    assert_eq!(store.read_symlink(link_ino).await.unwrap(), "/origin");

    store.unlink(root, "origin").await.unwrap();
    let remaining = store.stat(file).await.unwrap().unwrap();
    assert_eq!(remaining.nlink, 1);
    assert_eq!(
        store.get_paths(file).await.unwrap(),
        vec!["/hard".to_string()]
    );
}
