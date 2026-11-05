use brewfs::meta::client::session::SessionInfo;
use brewfs::{
    Config, DatabaseConfig, DatabaseMetaStore, DatabaseType, EtcdMetaStore, FileLockQuery,
    FileLockRange, FileLockType, MetaStore, RedisMetaStore, TiKvMetaStore,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn config(database: DatabaseType) -> Config {
    Config {
        database: DatabaseConfig {
            db_config: database,
        },
        cache: Default::default(),
        client: Default::default(),
        compact: Default::default(),
    }
}

async fn start_session(store: &(impl MetaStore + ?Sized), token: CancellationToken) {
    store
        .start_session(
            SessionInfo {
                version: "getlk-regression".into(),
                host_name: "localhost".into(),
                ip_addrs: vec![],
                mount_point: None,
                mount_time: chrono::Utc::now(),
                process_id: std::process::id(),
                created_at: chrono::Utc::now(),
            },
            token,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_getlk_ignores_own_lock() {
    let temp = tempfile::tempdir().unwrap();
    verify_getlk(DatabaseType::Sqlite {
        url: format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("meta.db").display()
        ),
    })
    .await;
}

async fn new_store(database: DatabaseType) -> Arc<dyn MetaStore> {
    let config = config(database);
    match &config.database.db_config {
        DatabaseType::Sqlite { .. } | DatabaseType::Postgres { .. } => {
            Arc::new(DatabaseMetaStore::from_config(config).await.unwrap())
        }
        DatabaseType::Redis { .. } => Arc::new(RedisMetaStore::from_config(config).await.unwrap()),
        DatabaseType::Etcd { .. } => Arc::new(EtcdMetaStore::from_config(config).await.unwrap()),
        DatabaseType::TiKv { .. } => Arc::new(TiKvMetaStore::from_config(config).await.unwrap()),
    }
}

async fn verify_getlk(database: DatabaseType) {
    // TiKV assigns a lock session to each store at construction.
    let explicit_sessions = !matches!(database, DatabaseType::TiKv { .. });
    let store = new_store(database.clone()).await;
    store.initialize().await.unwrap();
    let other = new_store(database).await;
    other.initialize().await.unwrap();
    let token = CancellationToken::new();
    let _cancel = token.clone().drop_guard();
    if explicit_sessions {
        start_session(store.as_ref(), token.clone()).await;
    }
    let inode = store
        .create_file(store.root_ino(), format!("getlk-{}", uuid::Uuid::new_v4()))
        .await
        .unwrap();
    let range = FileLockRange { start: 0, end: 100 };
    store
        .set_plock(inode, 1001, false, FileLockType::Write, range, 1234)
        .await
        .unwrap();
    let read_range = FileLockRange {
        start: 200,
        end: 300,
    };
    for (owner, pid) in [(1001, 1234), (1002, 5678)] {
        store
            .set_plock(inode, owner, false, FileLockType::Read, read_range, pid)
            .await
            .unwrap();
    }
    // Connect the querying session after all lock writes. Its initial
    // heartbeat can then run alongside reads, without racing SQLite writes.
    if explicit_sessions {
        start_session(other.as_ref(), token.clone()).await;
    }
    for lock_type in [FileLockType::Read, FileLockType::Write] {
        let query = FileLockQuery {
            owner: 1001,
            lock_type,
            range,
        };
        let info = store.get_plock(inode, &query).await.unwrap();
        assert_eq!(
            info.lock_type,
            FileLockType::UnLock,
            "GETLK must ignore the querying owner's own write lock"
        );
        let conflict = store
            .get_plock(
                inode,
                &FileLockQuery {
                    owner: 1002,
                    ..query
                },
            )
            .await
            .unwrap();
        assert_eq!(conflict.lock_type, FileLockType::Write);
        assert_eq!(conflict.range, range);
        assert_eq!(conflict.pid, 1234);
        // Equal numeric owners in different sessions are distinct owners.
        let conflict = other.get_plock(inode, &query).await.unwrap();
        assert_eq!(conflict.lock_type, FileLockType::Write);
        assert_eq!(conflict.range, range);
        assert_eq!(conflict.pid, 0);
    }
    let query = FileLockQuery {
        owner: 1001,
        lock_type: FileLockType::Read,
        range: read_range,
    };
    assert_eq!(
        store.get_plock(inode, &query).await.unwrap().lock_type,
        FileLockType::UnLock
    );
    assert_eq!(
        other.get_plock(inode, &query).await.unwrap().lock_type,
        FileLockType::UnLock
    );
    // Ignoring A must still find B's read lock when A wants a write lock.
    let conflict = store
        .get_plock(
            inode,
            &FileLockQuery {
                lock_type: FileLockType::Write,
                ..query
            },
        )
        .await
        .unwrap();
    assert_eq!(conflict.lock_type, FileLockType::Read);
    assert_eq!(conflict.pid, 5678);
    if explicit_sessions {
        token.cancel();
        other.shutdown_session().await.unwrap();
        store.shutdown_session().await.unwrap();
    } else {
        for owner in [1001, 1002] {
            store
                .set_plock(
                    inode,
                    owner,
                    false,
                    FileLockType::UnLock,
                    FileLockRange { start: 0, end: 300 },
                    0,
                )
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "requires Redis via BREWFS_REDIS_TEST_URL"]
async fn redis_getlk_ignores_own_lock() {
    verify_getlk(DatabaseType::Redis {
        url: std::env::var("BREWFS_REDIS_TEST_URL").unwrap(),
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL via BREWFS_POSTGRES_TEST_URL"]
async fn postgres_getlk_ignores_own_lock() {
    verify_getlk(DatabaseType::Postgres {
        url: std::env::var("BREWFS_POSTGRES_TEST_URL").unwrap(),
    })
    .await;
}

#[tokio::test]
#[ignore = "requires etcd via BREWFS_ETCD_TEST_URL"]
async fn etcd_getlk_ignores_own_lock() {
    verify_getlk(DatabaseType::Etcd {
        urls: vec![std::env::var("BREWFS_ETCD_TEST_URL").unwrap()],
    })
    .await;
}

#[tokio::test]
#[ignore = "requires TiKV via BREWFS_TIKV_TEST_PD_ENDPOINT"]
async fn tikv_getlk_ignores_own_lock() {
    let Ok(endpoint) = std::env::var("BREWFS_TIKV_TEST_PD_ENDPOINT") else {
        eprintln!("skip: BREWFS_TIKV_TEST_PD_ENDPOINT is unset");
        return;
    };
    verify_getlk(DatabaseType::TiKv {
        pd_endpoints: vec![endpoint],
        namespace: format!("getlk-{}", uuid::Uuid::new_v4()),
    })
    .await;
}

#[tokio::test]
async fn fuse_getlk_ignores_own_lock() {
    use asyncfuse::raw::{Filesystem, Request};
    use brewfs::{ChunkLayout, InMemoryBlockStore, VFS};

    let temp = tempfile::tempdir().unwrap();
    let store = new_store(DatabaseType::Sqlite {
        url: format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("fuse.db").display()
        ),
    })
    .await;
    let fs = VFS::new(ChunkLayout::default(), InMemoryBlockStore::new(), store)
        .await
        .unwrap();
    let inode = fs.create_file("/getlk").await.unwrap();
    let fh = fs
        .open(inode, fs.stat("/getlk").await.unwrap(), true, true, false)
        .await
        .unwrap();
    let request = || Request {
        unique: 1,
        uid: 0,
        gid: 0,
        pid: 1234,
    };
    Filesystem::setlk(
        &fs,
        request(),
        inode as u64,
        fh,
        1001,
        0,
        99,
        libc::F_WRLCK as u32,
        1234,
        false,
    )
    .await
    .unwrap();
    let own = Filesystem::getlk(
        &fs,
        request(),
        inode as u64,
        fh,
        1001,
        0,
        99,
        libc::F_WRLCK as u32,
        1234,
    )
    .await
    .unwrap();
    assert_eq!(
        own.r#type,
        libc::F_UNLCK as u32,
        "FUSE GETLK must ignore the querying owner's lock"
    );
    let other = Filesystem::getlk(
        &fs,
        request(),
        inode as u64,
        fh,
        1002,
        0,
        99,
        libc::F_WRLCK as u32,
        5678,
    )
    .await
    .unwrap();
    assert_eq!(other.r#type, libc::F_WRLCK as u32);
    assert_eq!((other.start, other.end, other.pid), (0, 99, 1234));
    Filesystem::setlk(
        &fs,
        request(),
        inode as u64,
        fh,
        1001,
        0,
        99,
        libc::F_UNLCK as u32,
        1234,
        false,
    )
    .await
    .unwrap();
    fs.close(fh).await.unwrap();
    Filesystem::destroy(&fs, request()).await;
}
