use crate::meta::client::MetaClient;
use crate::meta::config::Config;
use crate::meta::config::{
    CacheCapacity, CacheConfig, CacheTtl, ClientOptions, CompactConfig, DatabaseConfig,
    DatabaseType,
};
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{FileType, LockName, MetaError, SetAttrFlags, SetAttrRequest};
use crate::meta::stores::RedisMetaStore;
use crate::meta::{MetaLayer, MetaStore};
use crate::vfs::fs::VFS;
use crate::{chunk::layout::ChunkLayout, chunk::store::InMemoryBlockStore};
use redis::AsyncCommands;
use serial_test::serial;
use std::sync::Arc;
use tokio::time::{self, Duration};
use uuid::Uuid;

#[test]
fn local_txlock_slot_is_stable_for_same_key() {
    assert_eq!(
        super::RedisMetaStore::local_lock_slot_for_key("c42_0"),
        super::RedisMetaStore::local_lock_slot_for_key("c42_0")
    );
    assert!(
        super::RedisMetaStore::local_lock_slot_for_key("c42_0") < super::REDIS_TXN_LOCK_STRIPES
    );
}

#[tokio::test]
async fn local_txlock_serializes_same_primary_key() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let first_active = active.clone();
    let first_max = max_active.clone();
    let first_started = started.clone();
    let first_release = release.clone();
    let first = tokio::spawn(async move {
        super::RedisMetaStore::with_local_lock_for_key("cserialized_0", async move {
            let now = first_active.fetch_add(1, Ordering::SeqCst) + 1;
            first_max.fetch_max(now, Ordering::SeqCst);
            first_started.notify_one();
            first_release.notified().await;
            first_active.fetch_sub(1, Ordering::SeqCst);
        })
        .await;
    });

    started.notified().await;

    let second_active = active.clone();
    let second_max = max_active.clone();
    let second = tokio::spawn(async move {
        super::RedisMetaStore::with_local_lock_for_key("cserialized_0", async move {
            let now = second_active.fetch_add(1, Ordering::SeqCst) + 1;
            second_max.fetch_max(now, Ordering::SeqCst);
            second_active.fetch_sub(1, Ordering::SeqCst);
        })
        .await;
    });

    tokio::task::yield_now().await;
    assert_eq!(max_active.load(Ordering::SeqCst), 1);

    release.notify_one();
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
}

async fn cleanup_test_data() -> Result<(), MetaError> {
    let url = "redis://127.0.0.1:6379/0";
    let client = redis::Client::open(url)
        .map_err(|e| MetaError::Config(format!("Failed to create Redis client: {}", e)))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| MetaError::Config(format!("Failed to connect to Redis: {}", e)))?;

    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut conn)
        .await
        .map_err(|e| MetaError::Internal(format!("Failed to flush Redis DB: {}", e)))?;

    let config = test_config();
    let _store = RedisMetaStore::from_config(config.clone())
        .await
        .map_err(|e| MetaError::Internal(format!("Failed to reinitialize root: {}", e)))?;

    Ok(())
}

fn test_config() -> Config {
    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Redis {
                url: "redis://127.0.0.1:6379/0".to_string(),
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
        compact: CompactConfig::default(),
    }
}

/// Configuration for shared database testing (multi-session)
fn shared_db_config() -> Config {
    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Redis {
                url: "redis://127.0.0.1:6379/0".to_string(),
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
        compact: CompactConfig::default(),
    }
}

async fn new_test_store() -> RedisMetaStore {
    if let Err(e) = cleanup_test_data().await {
        eprintln!("Failed to cleanup Redis test data: {}", e);
    }

    RedisMetaStore::from_config(test_config())
        .await
        .expect("Failed to create test database store")
}

async fn reset_redis_commandstats(store: &RedisMetaStore) {
    let _: () = redis::cmd("CONFIG")
        .arg("RESETSTAT")
        .query_async(&mut store.conn.clone())
        .await
        .expect("failed to reset Redis command stats");
}

async fn redis_command_calls(store: &RedisMetaStore, command: &str) -> u64 {
    let info: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut store.conn.clone())
        .await
        .expect("failed to read Redis command stats");
    let needle = format!("cmdstat_{}:", command.to_ascii_lowercase());
    info.lines()
        .find_map(|line| {
            let rest = line.strip_prefix(&needle)?;
            let calls = rest.strip_prefix("calls=")?.split(',').next()?;
            calls.parse::<u64>().ok()
        })
        .unwrap_or(0)
}

async fn redis_script_calls(store: &RedisMetaStore) -> u64 {
    redis_command_calls(store, "eval").await + redis_command_calls(store, "evalsha").await
}

/// Create a new test store with pre-configured session ID
async fn new_test_store_with_session(session_id: Uuid) -> RedisMetaStore {
    let store = new_test_store().await;
    store.set_sid(session_id);
    store
}

/// Create a fully-initialized test store with both session ID and plating epoch.
/// This is required for any lock-related tests.
async fn new_test_store_with_epoch() -> RedisMetaStore {
    let store = new_test_store().await;
    // Initialize the fencing-token epoch via INCR (same as start_session does)
    let mut conn = store.conn.clone();
    let epoch: i64 = conn
        .incr("plock_epoch", 1)
        .await
        .expect("Failed to incr plock epoch");
    store.set_epoch(epoch);
    let session_id = Uuid::now_v7();
    store.set_sid(session_id);
    store
}

/// Helper struct to manage multiple test sessions
struct TestSessionManager {
    stores: Vec<RedisMetaStore>,
}

use std::sync::LazyLock;
use tokio::sync::Mutex;

// Static init, executed once.
static SHARED_DB_INIT: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

impl TestSessionManager {
    async fn new(session_count: usize) -> Self {
        // Lock to ensure serial initialization.
        let _guard = SHARED_DB_INIT.lock().await;

        use std::env;
        // Clean up existing shared test database
        let temp_dir = env::temp_dir();
        let db_path = temp_dir.join("brewfs_shared_test.db");

        static FIRST_INIT: std::sync::Once = std::sync::Once::new();
        FIRST_INIT.call_once(|| {
            let _ = std::fs::remove_file(&db_path);
        });

        let mut stores = Vec::with_capacity(session_count);
        let mut session_ids = Vec::with_capacity(session_count);

        let config = shared_db_config();
        let first_store = RedisMetaStore::from_config(config.clone())
            .await
            .expect("Failed to create shared test database store");

        let first_session_id = Uuid::now_v7();
        first_store.set_sid(first_session_id);

        stores.push(first_store);
        session_ids.push(first_session_id);

        for _ in 1..session_count {
            let store = RedisMetaStore::from_config(config.clone())
                .await
                .expect("Failed to create shared test database store");

            let session_id = Uuid::now_v7();
            store.set_sid(session_id);

            stores.push(store);
            session_ids.push(session_id);

            time::sleep(time::Duration::from_millis(5)).await;
        }

        Self { stores }
    }

    fn get_store(&self, index: usize) -> &RedisMetaStore {
        &self.stores[index]
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_symlink_roundtrip_and_unlink() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir = store.mkdir(root, "links".to_string()).await.unwrap();
    let (ino, attr) = store
        .symlink(dir, "link.txt", "/target/path")
        .await
        .unwrap();

    assert_eq!(attr.kind, crate::meta::store::FileType::Symlink);
    assert_eq!(attr.size, "/target/path".len() as u64);
    assert_eq!(store.lookup(dir, "link.txt").await.unwrap(), Some(ino));
    assert_eq!(store.read_symlink(ino).await.unwrap(), "/target/path");

    store.unlink(dir, "link.txt").await.unwrap();
    assert_eq!(store.lookup(dir, "link.txt").await.unwrap(), None);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_hardlink_dentry_binding_cross_dir_rename_unlink() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_a = store.mkdir(root, "a".to_string()).await.unwrap();
    let dir_b = store.mkdir(root, "b".to_string()).await.unwrap();

    let ino = store.create_file(dir_a, "x".to_string()).await.unwrap();
    store.link(ino, dir_b, "y").await.unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert!(names.contains(&(Some(dir_a), "x".to_string())));
    assert!(names.contains(&(Some(dir_b), "y".to_string())));

    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), Some(ino));

    store
        .rename(dir_b, "y", dir_b, "z".to_string())
        .await
        .unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert!(names.contains(&(Some(dir_a), "x".to_string())));
    assert!(names.contains(&(Some(dir_b), "z".to_string())));
    assert!(!names.contains(&(Some(dir_b), "y".to_string())));

    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), None);
    assert_eq!(store.lookup(dir_b, "z").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));

    store.unlink(dir_a, "x").await.unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert_eq!(names, vec![(Some(dir_b), "z".to_string())]);
    assert_eq!(store.lookup(dir_b, "z").await.unwrap(), Some(ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_hardlink_dentry_binding_cross_dir_move_rename() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_a = store.mkdir(root, "a".to_string()).await.unwrap();
    let dir_b = store.mkdir(root, "b".to_string()).await.unwrap();
    let dir_c = store.mkdir(root, "c".to_string()).await.unwrap();

    let ino = store.create_file(dir_a, "x".to_string()).await.unwrap();
    store.link(ino, dir_b, "y").await.unwrap();

    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), Some(ino));

    store
        .rename(dir_b, "y", dir_c, "z".to_string())
        .await
        .unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert!(names.contains(&(Some(dir_a), "x".to_string())));
    assert!(names.contains(&(Some(dir_c), "z".to_string())));
    assert!(!names.contains(&(Some(dir_b), "y".to_string())));

    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), None);
    assert_eq!(store.lookup(dir_c, "z").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_basic_read_lock() {
    let store = new_test_store().await;
    let session_id = Uuid::now_v7();
    let owner: i64 = 1001;

    // Set session
    store.set_sid(session_id);

    // Create a file first
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "test_basic_read_lock_file.txt".to_string())
        .await
        .unwrap();

    // Acquire read lock
    store
        .set_plock(
            file_ino,
            owner,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Verify lock exists
    let query = FileLockQuery {
        owner,
        lock_type: FileLockType::Read,
        range: FileLockRange { start: 0, end: 100 },
    };

    let lock_info = store.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::UnLock);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_multiple_read_locks() {
    // Create session manager with 2 sessions
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: i64 = 1001;
    let owner2: i64 = 1002;

    // Create a file first using the first session
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(
            parent,
            format!("test_multiple_read_locks_{}.txt", Uuid::now_v7()),
        )
        .await
        .unwrap();

    // First session acquires read lock
    store1
        .set_plock(
            file_ino,
            owner1,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Second session should be able to acquire read lock on same range
    let store2 = session_mgr.get_store(1);
    store2
        .set_plock(
            file_ino,
            owner2,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            5678,
        )
        .await
        .unwrap();

    // Verify both locks exist by querying each session
    let query1 = FileLockQuery {
        owner: owner1,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let query2 = FileLockQuery {
        owner: owner2,
        lock_type: FileLockType::Read,
        range: FileLockRange { start: 0, end: 100 },
    };

    let lock_info1 = store1.get_plock(file_ino, &query1).await.unwrap();
    assert_eq!(lock_info1.lock_type, FileLockType::Read);
    assert_eq!(lock_info1.range.start, 0);
    assert_eq!(lock_info1.range.end, 100);
    assert_eq!(lock_info1.pid, 1234);

    let lock_info2 = store2.get_plock(file_ino, &query2).await.unwrap();
    assert_eq!(lock_info2.lock_type, FileLockType::UnLock);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_write_lock_conflict() {
    // Create session manager with 2 sessions
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: u64 = 1001;
    let owner2: u64 = 1002;

    // Create a file first using the first session
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(parent, "test_write_lock_conflict_file.txt".to_string())
        .await
        .unwrap();

    // First session acquires read lock
    store1
        .set_plock(
            file_ino,
            owner1 as i64,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Second session should not be able to acquire write lock on overlapping range
    let store2 = session_mgr.get_store(1);
    let result = store2
        .set_plock(
            file_ino,
            owner2 as i64,
            false, // non-blocking
            FileLockType::Write,
            FileLockRange {
                start: 50,
                end: 150,
            }, // Overlapping range
            5678,
        )
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::LockConflict {
            inode: err_inode,
            owner: err_owner,
            range: err_range,
        } => {
            assert_eq!(err_inode, file_ino);
            assert_eq!(err_owner, owner2 as i64);
            assert_eq!(err_range.start, 50);
            assert_eq!(err_range.end, 150);
        }
        _ => panic!("Expected LockConflict error"),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_lock_release() {
    let session_id = Uuid::now_v7();
    let owner = 1001;

    // Create a store with pre-configured session
    let store = new_test_store_with_session(session_id).await;

    // Create a file first
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "test_lock_release_file.txt".to_string())
        .await
        .unwrap();

    // Acquire lock
    store
        .set_plock(
            file_ino,
            owner,
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Verify lock exists
    let query = FileLockQuery {
        owner,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let lock_info = store.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::Write);

    // Release lock
    store
        .set_plock(
            file_ino,
            owner,
            false,
            FileLockType::UnLock,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Verify lock is released
    let lock_info = store.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::UnLock);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_non_overlapping_locks() {
    // Create session manager with 2 sessions
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: i64 = 1001;
    let owner2: i64 = 1002;

    // Create a file first using the first session
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(parent, "test_none_overlapping_locks_file.txt".to_string())
        .await
        .unwrap();

    // First session acquires lock on range 0-100
    store1
        .set_plock(
            file_ino,
            owner1,
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Second session should be able to acquire lock on non-overlapping range 200-300
    let store2 = session_mgr.get_store(1);
    store2
        .set_plock(
            file_ino,
            owner2,
            false,
            FileLockType::Write,
            FileLockRange {
                start: 200,
                end: 300,
            },
            5678,
        )
        .await
        .unwrap();

    // Verify both locks exist
    let query1 = FileLockQuery {
        owner: owner1,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let query2 = FileLockQuery {
        owner: owner2,
        lock_type: FileLockType::Write,
        range: FileLockRange {
            start: 200,
            end: 300,
        },
    };

    let lock_info1 = store1.get_plock(file_ino, &query1).await.unwrap();
    assert_eq!(lock_info1.lock_type, FileLockType::Write);
    assert_eq!(lock_info1.range.start, 0);
    assert_eq!(lock_info1.range.end, 100);
    assert_eq!(lock_info1.pid, 1234);

    let lock_info2 = store2.get_plock(file_ino, &query2).await.unwrap();
    assert_eq!(lock_info2.lock_type, FileLockType::Write);
    assert_eq!(lock_info2.range.start, 200);
    assert_eq!(lock_info2.range.end, 300);
    assert_eq!(lock_info2.pid, 5678);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_concurrent_read_write_locks() {
    // Test multiple sessions acquiring different types of locks
    let session_mgr = TestSessionManager::new(3).await;

    // Create a file
    let store0 = session_mgr.get_store(0);
    let parent = store0.root_ino();
    let file_ino = store0
        .create_file(parent, "test_concurrent_read_write_locks.txt".to_string())
        .await
        .unwrap();

    let owner1: i64 = 1001;
    let owner2: i64 = 1002;
    let owner3: i64 = 1003;

    // Session 1: Acquire write lock on range 0-100
    {
        let store1 = session_mgr.get_store(0);
        store1
            .set_plock(
                file_ino,
                owner1,
                false,
                FileLockType::Write,
                FileLockRange { start: 0, end: 100 },
                1111,
            )
            .await
            .expect("Failed to acquire write lock");
    }

    // Session 2: Acquire read lock on range 200-300 (should succeed)
    {
        let store2 = session_mgr.get_store(1);
        store2
            .set_plock(
                file_ino,
                owner2,
                false,
                FileLockType::Read,
                FileLockRange {
                    start: 200,
                    end: 300,
                },
                2222,
            )
            .await
            .expect("Failed to acquire read lock");
    }

    // Session 3: Try to acquire write lock on overlapping range 50-150 (should fail)
    {
        let store3 = session_mgr.get_store(2);
        let result = store3
            .set_plock(
                file_ino,
                owner3,
                false,
                FileLockType::Write,
                FileLockRange {
                    start: 50,
                    end: 150,
                },
                3333,
            )
            .await;

        // Verify it fails with LockConflict
        assert!(result.is_err());
        match result.unwrap_err() {
            MetaError::LockConflict { .. } => {}
            _ => panic!("Expected LockConflict error"),
        }
    }

    // Verify successful locks exist
    let query1 = FileLockQuery {
        owner: owner1,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let query2 = FileLockQuery {
        owner: owner2,
        lock_type: FileLockType::Read,
        range: FileLockRange {
            start: 200,
            end: 300,
        },
    };

    // Check locks from different sessions
    {
        let store1 = session_mgr.get_store(0);
        let lock_info1 = store1.get_plock(file_ino, &query1).await.unwrap();
        assert_eq!(lock_info1.lock_type, FileLockType::Write);
    }

    {
        let store2 = session_mgr.get_store(1);
        let lock_info2 = store2.get_plock(file_ino, &query2).await.unwrap();
        assert_eq!(lock_info2.lock_type, FileLockType::UnLock);
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_cross_session_lock_visibility() {
    // Test that locks set by one session are visible to another session
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: u64 = 1001;

    // Create a file
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(parent, "test_cross_session_lock_visibility.txt".to_string())
        .await
        .unwrap();

    // Session 1 acquires a write lock
    store1
        .set_plock(
            file_ino,
            owner1 as i64,
            false,
            FileLockType::Write,
            FileLockRange {
                start: 0,
                end: 1000,
            },
            4444,
        )
        .await
        .unwrap();

    // Session 2 should be able to see the lock (and respect it)
    let store2 = session_mgr.get_store(1);
    let conflict_result = store2
        .set_plock(
            file_ino,
            2002, // different owner
            false,
            FileLockType::Write,
            FileLockRange {
                start: 500,
                end: 600,
            }, // overlapping range
            5555,
        )
        .await;

    // Should fail due to lock conflict
    assert!(conflict_result.is_err());
    match conflict_result.unwrap_err() {
        MetaError::LockConflict { .. } => {}
        _ => panic!("Expected LockConflict error"),
    }

    // Session 1 releases the lock
    store1
        .set_plock(
            file_ino,
            owner1 as i64,
            false,
            FileLockType::UnLock,
            FileLockRange {
                start: 0,
                end: 1000,
            },
            4444,
        )
        .await
        .unwrap();

    // Now Session 2 should be able to acquire the lock
    store2
        .set_plock(
            file_ino,
            2002,
            false,
            FileLockType::Write,
            FileLockRange {
                start: 500,
                end: 600,
            },
            5555,
        )
        .await
        .unwrap();

    // Verify the lock exists
    let query = FileLockQuery {
        owner: 2002,
        lock_type: FileLockType::Write,
        range: FileLockRange {
            start: 500,
            end: 600,
        },
    };

    let lock_info = store2.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::Write);
    assert_eq!(lock_info.pid, 5555);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_extend_file_size_lua_concurrent() {
    use crate::meta::MetaStore;

    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "test.txt".to_string())
        .await
        .unwrap();

    let store1 = std::sync::Arc::new(store);
    let store2 = store1.clone();
    let store3 = store1.clone();
    let store4 = store1.clone();

    let h1 = tokio::spawn(async move { store2.extend_file_size(ino, 1000).await });
    let h2 = tokio::spawn(async move { store3.extend_file_size(ino, 2000).await });
    let h3 = tokio::spawn(async move { store4.extend_file_size(ino, 1500).await });

    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();
    h3.await.unwrap().unwrap();

    let attr = store1.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr.size, 2000);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_extend_file_size_lua_idempotent() {
    use crate::meta::MetaStore;

    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "test.txt".to_string())
        .await
        .unwrap();

    store.extend_file_size(ino, 1000).await.unwrap();
    let attr1 = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr1.size, 1000);

    store.extend_file_size(ino, 500).await.unwrap();
    let attr2 = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr2.size, 1000);

    store.extend_file_size(ino, 1000).await.unwrap();
    let attr3 = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr3.size, 1000);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_extend_file_size_lua_missing_node() {
    use crate::meta::MetaStore;

    let store = new_test_store().await;
    let result = store.extend_file_size(99999, 1000).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotFound(ino) => assert_eq!(ino, 99999),
        other => panic!("expected NotFound error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_link_unlink_lua_atomicity() {
    use crate::meta::MetaStore;

    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_a = store.mkdir(root, "a".to_string()).await.unwrap();
    let dir_b = store.mkdir(root, "b".to_string()).await.unwrap();

    let ino = store.create_file(dir_a, "x".to_string()).await.unwrap();

    let attr1 = store.link(ino, dir_b, "y").await.unwrap();
    assert_eq!(attr1.nlink, 2);

    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), Some(ino));

    let result = store.link(ino, dir_b, "y").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::AlreadyExists { parent, name } => {
            assert_eq!(parent, dir_b);
            assert_eq!(name, "y");
        }
        other => panic!("expected AlreadyExists error, got {:?}", other),
    }

    store.unlink(dir_a, "x").await.unwrap();
    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), None);
    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), Some(ino));

    let attr2 = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr2.nlink, 1);

    store.unlink(dir_b, "y").await.unwrap();
    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), None);

    let deleted = store.get_deleted_files().await.unwrap();
    assert!(deleted.contains(&ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rmdir_lua_concurrent() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();

    let _test_dir = store.mkdir(root, "testdir".to_string()).await.unwrap();

    let store1 = store.clone();
    let store2 = store.clone();
    let store3 = store.clone();
    let store4 = store.clone();

    let h1 = tokio::spawn(async move { store1.rmdir(root, "testdir").await });
    let h2 = tokio::spawn(async move { store2.rmdir(root, "testdir").await });
    let h3 = tokio::spawn(async move { store3.rmdir(root, "testdir").await });
    let h4 = tokio::spawn(async move { store4.rmdir(root, "testdir").await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();
    let r4 = h4.await.unwrap();

    let results = [r1, r2, r3, r4];
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        success_count, 1,
        "Exactly one rmdir should succeed, got {} successes",
        success_count
    );

    let not_found_count = results
        .iter()
        .filter(|r| matches!(r, Err(MetaError::NotFound(ino)) if ino == &root))
        .count();
    assert_eq!(
        not_found_count, 3,
        "Three rmdir should return NotFound(parent), got {}",
        not_found_count
    );

    assert_eq!(store.lookup(root, "testdir").await.unwrap(), None);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rmdir_lua_not_empty() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let parent_dir = store.mkdir(root, "parent".to_string()).await.unwrap();
    let _child_dir = store.mkdir(parent_dir, "child".to_string()).await.unwrap();

    let result = store.rmdir(root, "parent").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::DirectoryNotEmpty(ino) => assert_eq!(ino, parent_dir),
        other => panic!("expected DirectoryNotEmpty error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rmdir_lua_not_found() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let result = store.rmdir(root, "nonexistent").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotFound(ino) => assert_eq!(ino, root),
        other => panic!("expected NotFound(parent) error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rmdir_lua_not_directory() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file.txt".to_string())
        .await
        .unwrap();

    let result = store.rmdir(root, "file.txt").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotDirectory(ino) => assert_eq!(ino, file_ino),
        other => panic!("expected NotDirectory error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_create_entry_lua_concurrent() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();

    let store1 = store.clone();
    let store2 = store.clone();
    let store3 = store.clone();
    let store4 = store.clone();

    let h1 = tokio::spawn(async move { store1.mkdir(root, "newdir".to_string()).await });
    let h2 = tokio::spawn(async move { store2.mkdir(root, "newdir".to_string()).await });
    let h3 = tokio::spawn(async move { store3.mkdir(root, "newdir".to_string()).await });
    let h4 = tokio::spawn(async move { store4.mkdir(root, "newdir".to_string()).await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();
    let r4 = h4.await.unwrap();

    let results = [r1, r2, r3, r4];
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        success_count, 1,
        "Exactly one mkdir should succeed, got {} successes",
        success_count
    );

    let already_exists_count = results
            .iter()
            .filter(|r| matches!(r, Err(MetaError::AlreadyExists { parent, name }) if parent == &root && name == "newdir"))
            .count();
    assert_eq!(
        already_exists_count, 3,
        "Three mkdir should return AlreadyExists, got {}",
        already_exists_count
    );

    let ino = store.lookup(root, "newdir").await.unwrap();
    assert!(ino.is_some());
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_create_entry_lua_already_exists() {
    let store = new_test_store().await;
    let root = store.root_ino();

    store.mkdir(root, "existing".to_string()).await.unwrap();

    let result = store.mkdir(root, "existing".to_string()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::AlreadyExists { parent, name } => {
            assert_eq!(parent, root);
            assert_eq!(name, "existing");
        }
        other => panic!("expected AlreadyExists error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_unlink_last_reference_updates_parent_and_deleted_child_atomically() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "atomic_unlink.txt".to_string())
        .await
        .unwrap();

    store.unlink(root, "atomic_unlink.txt").await.unwrap();

    let parent = store.get_node(root).await.unwrap().unwrap();
    let deleted = store.get_node(ino).await.unwrap().unwrap();
    assert!(deleted.deleted);
    assert_eq!(deleted.attr.nlink, 0);
    assert_eq!(
        parent.attr.mtime, deleted.attr.ctime,
        "last unlink should update parent and deleted child with one atomic timestamp"
    );
    assert_eq!(parent.attr.ctime, deleted.attr.ctime);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_create_entry_lua_parent_not_found() {
    let store = new_test_store().await;

    let result = store.mkdir(999999, "newdir".to_string()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::ParentNotFound(ino) => assert_eq!(ino, 999999),
        other => panic!("expected ParentNotFound error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_create_entry_lua_parent_not_directory() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file.txt".to_string())
        .await
        .unwrap();

    let result = store.mkdir(file_ino, "newdir".to_string()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotDirectory(ino) => assert_eq!(ino, file_ino),
        other => panic!("expected NotDirectory error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_concurrent() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file.txt".to_string())
        .await
        .unwrap();
    store.mkdir(root, "dir1".to_string()).await.unwrap();
    store.mkdir(root, "dir2".to_string()).await.unwrap();
    store.mkdir(root, "dir3".to_string()).await.unwrap();
    store.mkdir(root, "dir4".to_string()).await.unwrap();

    let store1 = store.clone();
    let store2 = store.clone();
    let store3 = store.clone();
    let store4 = store.clone();

    let h1 = tokio::spawn(async move {
        store1
            .rename(root, "file.txt", root, "moved1.txt".to_string())
            .await
    });
    let h2 = tokio::spawn(async move {
        store2
            .rename(root, "file.txt", root, "moved2.txt".to_string())
            .await
    });
    let h3 = tokio::spawn(async move {
        store3
            .rename(root, "file.txt", root, "moved3.txt".to_string())
            .await
    });
    let h4 = tokio::spawn(async move {
        store4
            .rename(root, "file.txt", root, "moved4.txt".to_string())
            .await
    });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();
    let r4 = h4.await.unwrap();

    let success_count = [&r1, &r2, &r3, &r4].iter().filter(|r| r.is_ok()).count();
    assert_eq!(success_count, 1, "exactly one rename should succeed");

    let not_found_count = [&r1, &r2, &r3, &r4]
        .iter()
        .filter(|r| matches!(r, Err(MetaError::NotFound(ino)) if *ino == root))
        .count();
    assert_eq!(
        not_found_count, 3,
        "three renames should return NotFound(parent)"
    );

    let final_node = store.get_node(file_ino).await.unwrap().unwrap();
    assert!(
        final_node.name.starts_with("moved") && final_node.name.ends_with(".txt"),
        "file should be renamed to one of the target names"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_source_not_found() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let result = store
        .rename(root, "nonexistent.txt", root, "moved.txt".to_string())
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotFound(ino) => assert_eq!(ino, root),
        other => panic!("expected NotFound(parent) error, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_existing_file_target_is_replaced() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store
        .create_file(root, "file1.txt".to_string())
        .await
        .unwrap();
    let dst_ino = store
        .create_file(root, "file2.txt".to_string())
        .await
        .unwrap();

    store
        .rename(root, "file1.txt", root, "file2.txt".to_string())
        .await
        .unwrap();

    assert_eq!(store.lookup(root, "file1.txt").await.unwrap(), None);
    assert_eq!(
        store.lookup(root, "file2.txt").await.unwrap(),
        Some(src_ino)
    );
    let replaced = store.get_node(dst_ino).await.unwrap().unwrap();
    assert!(replaced.deleted);
    assert_eq!(replaced.attr.nlink, 0);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_overwrite_file() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store
        .create_file(root, "src.txt".to_string())
        .await
        .unwrap();
    let dst_ino = store
        .create_file(root, "dst.txt".to_string())
        .await
        .unwrap();

    store
        .rename(root, "src.txt", root, "dst.txt".to_string())
        .await
        .unwrap();

    assert_eq!(store.lookup(root, "src.txt").await.unwrap(), None);
    assert_eq!(store.lookup(root, "dst.txt").await.unwrap(), Some(src_ino));

    let overwritten = store.get_node(dst_ino).await.unwrap().unwrap();
    assert!(
        overwritten.deleted,
        "overwritten inode should be tombstoned"
    );
    assert_eq!(
        overwritten.attr.nlink, 0,
        "overwritten inode should have nlink=0"
    );
    assert!(
        store.stat(dst_ino).await.unwrap().is_some(),
        "tombstoned inode should remain until GC"
    );

    let deleted = store.get_deleted_files().await.unwrap();
    assert!(
        deleted.contains(&dst_ino),
        "overwritten inode should be queued for cleanup"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_directory_replaces_empty_directory() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store.mkdir(root, "src".to_string()).await.unwrap();
    let dst_ino = store.mkdir(root, "dst".to_string()).await.unwrap();

    store
        .rename(root, "src", root, "dst".to_string())
        .await
        .unwrap();

    assert_eq!(store.lookup(root, "src").await.unwrap(), None);
    assert_eq!(store.lookup(root, "dst").await.unwrap(), Some(src_ino));
    assert!(store.get_node(dst_ino).await.unwrap().is_none());
    assert!(store.get_node(src_ino).await.unwrap().is_some());
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_directory_to_missing_target_updates_parent_name() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store.mkdir(root, "src".to_string()).await.unwrap();

    store
        .rename(root, "src", root, "dst".to_string())
        .await
        .unwrap();

    assert_eq!(store.lookup(root, "src").await.unwrap(), None);
    assert_eq!(store.lookup(root, "dst").await.unwrap(), Some(src_ino));

    let renamed = store.get_node(src_ino).await.unwrap().unwrap();
    assert_eq!(renamed.parent, root);
    assert_eq!(renamed.name, "dst");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_cross_dir_directory_preserves_parent_nlink_after_cleanup() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_x = store.mkdir(root, "x".to_string()).await.unwrap();
    let dir_y = store.mkdir(root, "y".to_string()).await.unwrap();
    store.mkdir(dir_x, "src".to_string()).await.unwrap();
    store.mkdir(dir_y, "dst".to_string()).await.unwrap();

    // Mirror the VFS overwrite flow: remove the empty destination first, then rename.
    store.rmdir(dir_y, "dst").await.unwrap();
    store
        .rename(dir_x, "src", dir_y, "dst".to_string())
        .await
        .unwrap();

    let y_attr = store.stat(dir_y).await.unwrap().unwrap();
    assert_eq!(
        y_attr.nlink, 3,
        "moved subdir should increment new parent nlink"
    );

    store.rmdir(dir_y, "dst").await.unwrap();

    let y_attr = store.stat(dir_y).await.unwrap().unwrap();
    assert_eq!(y_attr.nlink, 2, "cleanup should restore parent nlink");
    assert_eq!(store.lookup(root, "y").await.unwrap(), Some(dir_y));
    assert_eq!(
        store.get_paths(dir_y).await.unwrap(),
        vec!["/y".to_string()]
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_directory_rejects_non_empty_directory_target() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store.mkdir(root, "src".to_string()).await.unwrap();
    let dst_ino = store.mkdir(root, "dst".to_string()).await.unwrap();
    store
        .create_file(dst_ino, "child.txt".to_string())
        .await
        .unwrap();

    let result = store.rename(root, "src", root, "dst".to_string()).await;
    match result.unwrap_err() {
        MetaError::DirectoryNotEmpty(ino) => assert_eq!(ino, dst_ino),
        other => panic!("expected DirectoryNotEmpty error, got {:?}", other),
    }

    assert_eq!(store.lookup(root, "src").await.unwrap(), Some(src_ino));
    assert_eq!(store.lookup(root, "dst").await.unwrap(), Some(dst_ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_directory_rejects_file_target() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store.mkdir(root, "src".to_string()).await.unwrap();
    let dst_ino = store
        .create_file(root, "dst.txt".to_string())
        .await
        .unwrap();

    let result = store.rename(root, "src", root, "dst.txt".to_string()).await;
    match result.unwrap_err() {
        MetaError::Io(err) => assert_eq!(err.kind(), std::io::ErrorKind::NotADirectory),
        other => panic!("expected NotADirectory IO error, got {:?}", other),
    }

    assert_eq!(store.lookup(root, "src").await.unwrap(), Some(src_ino));
    assert_eq!(store.lookup(root, "dst.txt").await.unwrap(), Some(dst_ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_file_rejects_directory_target() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store
        .create_file(root, "src.txt".to_string())
        .await
        .unwrap();
    let dst_ino = store.mkdir(root, "dst".to_string()).await.unwrap();

    let result = store.rename(root, "src.txt", root, "dst".to_string()).await;
    match result.unwrap_err() {
        MetaError::Io(err) => assert_eq!(err.kind(), std::io::ErrorKind::IsADirectory),
        other => panic!("expected IsADirectory IO error, got {:?}", other),
    }

    assert_eq!(store.lookup(root, "src.txt").await.unwrap(), Some(src_ino));
    assert_eq!(store.lookup(root, "dst").await.unwrap(), Some(dst_ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_same_name() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file.txt".to_string())
        .await
        .unwrap();
    let node_before = store.get_node(file_ino).await.unwrap().unwrap();

    let result = store
        .rename(root, "file.txt", root, "file.txt".to_string())
        .await;
    assert!(result.is_ok(), "self-rename should be no-op");

    let node_after = store.get_node(file_ino).await.unwrap().unwrap();
    assert_eq!(
        node_before.attr.mtime, node_after.attr.mtime,
        "mtime should not change"
    );
    assert_eq!(
        node_before.attr.ctime, node_after.attr.ctime,
        "ctime should not change"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_uses_lua_dentry_lookup_without_rust_prelookups() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let src_ino = store
        .create_file(root, "src.txt".to_string())
        .await
        .unwrap();

    reset_redis_commandstats(&store).await;
    store
        .rename(root, "src.txt", root, "dst.txt".to_string())
        .await
        .unwrap();

    assert_eq!(store.lookup(root, "dst.txt").await.unwrap(), Some(src_ino));
    let hget_calls = redis_command_calls(&store, "hget").await;
    assert!(
        hget_calls <= 3,
        "rename should avoid Rust-side dentry prelookups; observed {hget_calls} Redis HGET calls"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_same_dir_skips_redundant_parent_get_set() {
    let store = new_test_store().await;
    let root = store.root_ino();

    store
        .create_file(root, "src.txt".to_string())
        .await
        .unwrap();

    reset_redis_commandstats(&store).await;
    store
        .rename(root, "src.txt", root, "dst.txt".to_string())
        .await
        .unwrap();

    let get_calls = redis_command_calls(&store, "get").await;
    let set_calls = redis_command_calls(&store, "set").await;
    assert!(
        get_calls <= 2,
        "same-dir rename should fetch parent once and child once; observed {get_calls} Redis GET calls"
    );
    assert!(
        set_calls <= 2,
        "same-dir rename should save child and parent once; observed {set_calls} Redis SET calls"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_hardlink() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file.txt".to_string())
        .await
        .unwrap();
    store.link(file_ino, root, "link.txt").await.unwrap();

    let node_before = store.get_node(file_ino).await.unwrap().unwrap();
    assert_eq!(node_before.attr.nlink, 2, "file should have nlink=2");
    assert_eq!(
        node_before.parent, 0,
        "hardlinked file should have parent=0"
    );
    assert_eq!(node_before.name, "", "hardlinked file should have name=''");

    let link_parents_before = store.load_link_parents(file_ino).await.unwrap();
    assert_eq!(link_parents_before.len(), 2);
    assert!(link_parents_before.contains(&(root, "file.txt".to_string())));
    assert!(link_parents_before.contains(&(root, "link.txt".to_string())));

    let result = store
        .rename(root, "file.txt", root, "renamed.txt".to_string())
        .await;
    assert!(result.is_ok());

    let node_after = store.get_node(file_ino).await.unwrap().unwrap();
    assert_eq!(node_after.attr.nlink, 2, "nlink should remain 2");
    assert_eq!(node_after.parent, 0, "parent should remain 0");
    assert_eq!(node_after.name, "", "name should remain ''");

    let link_parents_after = store.load_link_parents(file_ino).await.unwrap();
    assert_eq!(link_parents_after.len(), 2);
    assert!(link_parents_after.contains(&(root, "renamed.txt".to_string())));
    assert!(link_parents_after.contains(&(root, "link.txt".to_string())));
    assert!(!link_parents_after.contains(&(root, "file.txt".to_string())));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_lua_hardlink_same_inode_target_is_noop() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file.txt".to_string())
        .await
        .unwrap();
    store.link(file_ino, root, "link.txt").await.unwrap();

    store
        .rename(root, "file.txt", root, "link.txt".to_string())
        .await
        .unwrap();

    assert_eq!(
        store.lookup(root, "file.txt").await.unwrap(),
        Some(file_ino)
    );
    assert_eq!(
        store.lookup(root, "link.txt").await.unwrap(),
        Some(file_ino)
    );

    let node_after = store.get_node(file_ino).await.unwrap().unwrap();
    assert_eq!(node_after.attr.nlink, 2);
    let link_parents_after = store.load_link_parents(file_ino).await.unwrap();
    assert_eq!(link_parents_after.len(), 2);
    assert!(link_parents_after.contains(&(root, "file.txt".to_string())));
    assert!(link_parents_after.contains(&(root, "link.txt".to_string())));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_exchange_lua_concurrent() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();

    let file1 = store
        .create_file(root, "file1.txt".to_string())
        .await
        .unwrap();
    let file2 = store
        .create_file(root, "file2.txt".to_string())
        .await
        .unwrap();

    let mut handles = vec![];
    for _ in 0..4 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            store_clone
                .rename_exchange(root, "file1.txt", root, "file2.txt")
                .await
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let successes = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(successes, 4, "all exchanges should succeed (idempotent)");

    let lookup1 = store.lookup(root, "file1.txt").await.unwrap();
    let lookup2 = store.lookup(root, "file2.txt").await.unwrap();
    assert!(
        (lookup1 == Some(file1) && lookup2 == Some(file2))
            || (lookup1 == Some(file2) && lookup2 == Some(file1)),
        "entries should be exchanged or restored to original"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_exchange_lua_old_not_found() {
    let store = new_test_store().await;
    let root = store.root_ino();

    store
        .create_file(root, "file2.txt".to_string())
        .await
        .unwrap();

    let result = store
        .rename_exchange(root, "nonexistent.txt", root, "file2.txt")
        .await;

    assert!(result.is_err());
    if let Err(MetaError::Internal(msg)) = result {
        assert!(
            msg.contains("Entry 'nonexistent.txt' not found in parent")
                && msg.contains("for exchange"),
            "error message should match format: got '{}'",
            msg
        );
    } else {
        panic!("expected Internal error");
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_exchange_lua_new_not_found() {
    let store = new_test_store().await;
    let root = store.root_ino();

    store
        .create_file(root, "file1.txt".to_string())
        .await
        .unwrap();

    let result = store
        .rename_exchange(root, "file1.txt", root, "nonexistent.txt")
        .await;

    assert!(result.is_err());
    if let Err(MetaError::Internal(msg)) = result {
        assert!(
            msg.contains("Entry 'nonexistent.txt' not found in parent")
                && msg.contains("for exchange"),
            "error message should match format: got '{}'",
            msg
        );
    } else {
        panic!("expected Internal error");
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_exchange_lua_same_entry() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file.txt".to_string())
        .await
        .unwrap();
    let node_before = store.get_node(file_ino).await.unwrap().unwrap();

    let result = store
        .rename_exchange(root, "file.txt", root, "file.txt")
        .await;
    assert!(result.is_ok(), "self-exchange should be no-op");

    let node_after = store.get_node(file_ino).await.unwrap().unwrap();
    assert_eq!(
        node_before.attr.mtime, node_after.attr.mtime,
        "mtime should not change"
    );
    assert_eq!(
        node_before.attr.ctime, node_after.attr.ctime,
        "ctime should not change"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_exchange_lua_hardlinks() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file1 = store
        .create_file(root, "file1.txt".to_string())
        .await
        .unwrap();
    store.link(file1, root, "link1.txt").await.unwrap();

    let file2 = store
        .create_file(root, "file2.txt".to_string())
        .await
        .unwrap();
    store.link(file2, root, "link2.txt").await.unwrap();

    let node1_before = store.get_node(file1).await.unwrap().unwrap();
    assert_eq!(node1_before.attr.nlink, 2);
    assert_eq!(node1_before.parent, 0);
    assert_eq!(node1_before.name, "");

    let node2_before = store.get_node(file2).await.unwrap().unwrap();
    assert_eq!(node2_before.attr.nlink, 2);
    assert_eq!(node2_before.parent, 0);
    assert_eq!(node2_before.name, "");

    let result = store
        .rename_exchange(root, "file1.txt", root, "file2.txt")
        .await;
    assert!(result.is_ok());

    let link_parents1 = store.load_link_parents(file1).await.unwrap();
    assert_eq!(link_parents1.len(), 2);
    assert!(link_parents1.contains(&(root, "file2.txt".to_string())));
    assert!(link_parents1.contains(&(root, "link1.txt".to_string())));
    assert!(!link_parents1.contains(&(root, "file1.txt".to_string())));

    let link_parents2 = store.load_link_parents(file2).await.unwrap();
    assert_eq!(link_parents2.len(), 2);
    assert!(link_parents2.contains(&(root, "file1.txt".to_string())));
    assert!(link_parents2.contains(&(root, "link2.txt".to_string())));
    assert!(!link_parents2.contains(&(root, "file2.txt".to_string())));

    let node1_after = store.get_node(file1).await.unwrap().unwrap();
    assert_eq!(node1_after.attr.nlink, 2);
    assert_eq!(node1_after.parent, 0);
    assert_eq!(node1_after.name, "");

    let node2_after = store.get_node(file2).await.unwrap().unwrap();
    assert_eq!(node2_after.attr.nlink, 2);
    assert_eq!(node2_after.parent, 0);
    assert_eq!(node2_after.name, "");
}

#[test]
fn test_deserialize_i64_from_number() {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct TestStruct {
        #[serde(deserialize_with = "super::deserialize_i64_from_number")]
        value: i64,
    }

    // Integer input (normal case)
    let json = r#"{"value": 1234567890}"#;
    let result: TestStruct = serde_json::from_str(json).unwrap();
    assert_eq!(result.value, 1234567890);

    // Float input (the bug case - scientific notation)
    let json = r#"{"value": 1.7698324007242e+18}"#;
    let result: TestStruct = serde_json::from_str(json).unwrap();
    assert!(result.value > 1_700_000_000_000_000_000); // ~1.77e18

    // Negative value
    let json = r#"{"value": -1000}"#;
    let result: TestStruct = serde_json::from_str(json).unwrap();
    assert_eq!(result.value, -1000);

    // Zero
    let json = r#"{"value": 0}"#;
    let result: TestStruct = serde_json::from_str(json).unwrap();
    assert_eq!(result.value, 0);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_truncate_rewrite_is_atomic_for_partial_chunk() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "truncate_race.bin".to_string())
        .await
        .unwrap();
    let chunk_size = 8 * 1024u64;
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();

    let base = [
        crate::chunk::SliceDesc {
            slice_id: 11,
            chunk_id,
            offset: 0,
            length: chunk_size,
        },
        crate::chunk::SliceDesc {
            slice_id: 12,
            chunk_id,
            offset: 0,
            length: chunk_size,
        },
    ];
    for desc in base {
        store.append_slice(chunk_id, desc).await.unwrap();
    }
    store.extend_file_size(ino, chunk_size).await.unwrap();

    store.truncate(ino, 1024, chunk_size).await.unwrap();
    let after = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(after.len(), 2);
    assert!(after.iter().all(|s| s.length == 1024));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_truncate_rewrite_never_exposes_empty_chunk_list() {
    use tokio::task::yield_now;

    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();
    let ino = store
        .create_file(root, "truncate_visibility.bin".to_string())
        .await
        .unwrap();
    let chunk_size = 8 * 1024u64;
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();

    store
        .append_slice(
            chunk_id,
            crate::chunk::SliceDesc {
                slice_id: 21,
                chunk_id,
                offset: 0,
                length: chunk_size,
            },
        )
        .await
        .unwrap();
    store.extend_file_size(ino, chunk_size).await.unwrap();

    let writer = store.clone();
    let trunc = tokio::spawn(async move {
        writer.truncate(ino, 1024, chunk_size).await.unwrap();
    });

    let reader = store.clone();
    let observer = tokio::spawn(async move {
        for _ in 0..512 {
            let slices = reader.get_slices(chunk_id).await.unwrap();
            if slices.is_empty() {
                return true;
            }
            yield_now().await;
        }
        false
    });

    trunc.await.unwrap();
    let saw_empty = observer.await.unwrap();
    assert!(!saw_empty, "truncate rewrite exposed an empty slice list");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_file_default_mode() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "perm_file.txt".to_string())
        .await
        .unwrap();

    let attr = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr.mode & 0o777, 0o644);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_directory_default_mode() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store.mkdir(parent, "perm_dir".to_string()).await.unwrap();

    let attr = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr.mode & 0o7777, 0o755);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_chmod_updates_mode() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "chmod_test.txt".to_string())
        .await
        .unwrap();

    let attr = store.chmod(ino, 0o755).await.unwrap();
    assert_eq!(attr.mode & 0o777, 0o755);

    let stat = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(stat.mode & 0o777, 0o755);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_set_attr_mode_strips_special_bits() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "special_bits.txt".to_string())
        .await
        .unwrap();

    let req = SetAttrRequest {
        mode: Some(0o4755),
        ..Default::default()
    };
    let attr = store
        .set_attr(ino, &req, SetAttrFlags::empty())
        .await
        .unwrap();
    assert_eq!(attr.mode & 0o7777, 0o755);

    let stat = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(stat.mode & 0o7777, 0o755);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_chmod_nonexistent_inode() {
    let store = new_test_store().await;
    let result = store.chmod(999999, 0o644).await;
    assert!(result.is_err(), "chmod on nonexistent inode should fail");
}

// --- Flow correctness tests ---

#[serial]
#[tokio::test]
#[ignore]
async fn test_file_full_lifecycle_flow() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let ino = store
        .create_file(root, "lifecycle.txt".to_string())
        .await
        .unwrap();
    let attr = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr.kind, crate::meta::store::FileType::File);
    assert_eq!(attr.nlink, 1);

    let req = SetAttrRequest {
        mode: Some(0o600),
        ..Default::default()
    };
    store
        .set_attr(ino, &req, SetAttrFlags::empty())
        .await
        .unwrap();

    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
    let slice = crate::chunk::SliceDesc {
        slice_id: 101,
        chunk_id,
        offset: 0,
        length: 4096,
    };
    store.write(ino, chunk_id, slice, 4096).await.unwrap();

    let stat_after_write = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(stat_after_write.size, 4096);

    let slices = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(slices.len(), 1);
    assert_eq!(slices[0].slice_id, 101);

    store.truncate(ino, 2048, 4096).await.unwrap();
    let stat_after_truncate = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(stat_after_truncate.size, 2048);

    store.unlink(root, "lifecycle.txt").await.unwrap();
    assert_eq!(store.lookup(root, "lifecycle.txt").await.unwrap(), None);

    let deleted = store.get_deleted_files().await.unwrap();
    assert!(
        deleted.contains(&ino),
        "file should be in deleted set after unlink"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_directory_full_lifecycle_flow() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let root_attr_before = store.stat(root).await.unwrap().unwrap();

    let dir = store
        .mkdir(root, "dir_lifecycle".to_string())
        .await
        .unwrap();
    let root_attr_after = store.stat(root).await.unwrap().unwrap();
    assert_eq!(
        root_attr_after.nlink,
        root_attr_before.nlink + 1,
        "mkdir should increase parent nlink"
    );

    let child = store
        .create_file(dir, "child.txt".to_string())
        .await
        .unwrap();
    let entries = store.readdir(dir).await.unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.ino == child && e.name == "child.txt")
    );

    store.unlink(dir, "child.txt").await.unwrap();
    let entries_after = store.readdir(dir).await.unwrap();
    assert!(!entries_after.iter().any(|e| e.name == "child.txt"));

    store.rmdir(root, "dir_lifecycle").await.unwrap();
    let root_attr_final = store.stat(root).await.unwrap().unwrap();
    assert_eq!(
        root_attr_final.nlink, root_attr_before.nlink,
        "parent nlink should be restored after rmdir"
    );
    assert_eq!(store.lookup(root, "dir_lifecycle").await.unwrap(), None);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_lookup_path_resolution_flow() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir = store.mkdir(root, "a".to_string()).await.unwrap();
    let sub = store.mkdir(dir, "b".to_string()).await.unwrap();
    let _file = store.create_file(sub, "c.txt".to_string()).await.unwrap();

    assert_eq!(
        store.lookup_path("/").await.unwrap(),
        Some((root, crate::meta::store::FileType::Dir))
    );
    assert_eq!(
        store.lookup_path("/a").await.unwrap().map(|(ino, _)| ino),
        Some(dir)
    );
    assert_eq!(
        store.lookup_path("/a/b").await.unwrap().map(|(ino, _)| ino),
        Some(sub)
    );
    assert!(store.lookup_path("/a/b/c.txt").await.unwrap().is_some());
    assert_eq!(store.lookup_path("/nonexistent").await.unwrap(), None);
    assert_eq!(
        store.lookup_path("/a/b/nonexistent.txt").await.unwrap(),
        None
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_batch_stat_mixed_flow() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let ino1 = store.create_file(root, "f1.txt".to_string()).await.unwrap();
    let ino2 = store.create_file(root, "f2.txt".to_string()).await.unwrap();

    let results = store.batch_stat(&[root, ino1, ino2, 999999]).await.unwrap();
    assert_eq!(results.len(), 4);
    assert!(results[0].is_some(), "root should exist");
    assert!(results[1].is_some(), "f1 should exist");
    assert!(results[2].is_some(), "f2 should exist");
    assert!(results[3].is_none(), "999999 should not exist");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_get_node_cache_expires_after_ttl() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let original = store.get_node(root).await.unwrap().unwrap();
    let mut mutated = original.clone();
    mutated.attr.mode = 0o040700;

    let data = serde_json::to_vec(&mutated).unwrap();
    let mut conn = store.conn.clone();
    let _: () = conn.set(store.node_key(root), data).await.unwrap();

    let cached = store.get_node(root).await.unwrap().unwrap();
    assert_eq!(cached.attr.mode, original.attr.mode);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let refreshed = store.get_node(root).await.unwrap().unwrap();
    assert_eq!(refreshed.attr.mode, mutated.attr.mode);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_create_entry_updates_parent_node_cache() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let root_before = store.get_node(root).await.unwrap().unwrap();
    assert!(store.node_cache.get(&root).await.is_some());

    store.mkdir(root, "cache_dir".to_string()).await.unwrap();

    let root_after = store
        .node_cache
        .get(&root)
        .await
        .expect("parent cache should stay warm after create")
        .expect("parent node should be cached");
    assert_eq!(root_after.attr.nlink, root_before.attr.nlink + 1);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_create_entry_uses_lua_parent_lookup_without_rust_prelookup() {
    let store = new_test_store().await;
    let root = store.root_ino();

    store.node_cache.invalidate(&root).await;
    reset_redis_commandstats(&store).await;
    let ino = store
        .create_file(root, "hot.txt".to_string())
        .await
        .unwrap();

    let get_calls = redis_command_calls(&store, "get").await;
    assert!(
        get_calls <= 1,
        "create_file should let Lua fetch the parent once; observed {get_calls} Redis GET calls"
    );
    assert_eq!(store.lookup(root, "hot.txt").await.unwrap(), Some(ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_meta_client_create_file_avoids_parent_stat_after_lua_create() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );

    store.node_cache.invalidate(&root).await;
    reset_redis_commandstats(&store).await;

    let ino = client
        .create_file(root, "client_hot.txt".to_string())
        .await
        .unwrap();

    let get_calls = redis_command_calls(&store, "get").await;
    assert!(
        get_calls <= 1,
        "MetaClient create_file should not stat the parent after Lua already loaded it; observed {get_calls} Redis GET calls"
    );
    assert_eq!(
        client.lookup(root, "client_hot.txt").await.unwrap(),
        Some(ino)
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_meta_client_mkdir_avoids_parent_stat_after_lua_create() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );

    store.node_cache.invalidate(&root).await;
    reset_redis_commandstats(&store).await;

    let ino = client.mkdir(root, "client_dir".to_string()).await.unwrap();

    let get_calls = redis_command_calls(&store, "get").await;
    assert!(
        get_calls <= 1,
        "MetaClient mkdir should not stat the parent after Lua already loaded it; observed {get_calls} Redis GET calls"
    );
    assert_eq!(client.lookup(root, "client_dir").await.unwrap(), Some(ino));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_meta_client_new_directory_negative_lookup_stays_local() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );

    let dir = client.mkdir(root, "empty_dir".to_string()).await.unwrap();

    reset_redis_commandstats(&store).await;
    assert_eq!(client.lookup(dir, "missing.txt").await.unwrap(), None);

    let hget_calls = redis_command_calls(&store, "hget").await;
    assert_eq!(
        hget_calls, 0,
        "negative lookup in a freshly-created empty directory should stay local; observed {hget_calls} Redis HGET calls"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_lookup_with_attr_returns_inode_attr_and_warms_node_cache() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "lookup_attr.txt".to_string())
        .await
        .unwrap();

    store.node_cache.invalidate(&ino).await;
    reset_redis_commandstats(&store).await;

    let (found, attr) = store
        .lookup_with_attr(root, "lookup_attr.txt")
        .await
        .unwrap()
        .expect("lookup_with_attr should find created file");

    assert_eq!(found, ino);
    assert_eq!(attr.ino, ino);
    assert_eq!(attr.kind, FileType::File);
    assert!(
        matches!(store.node_cache.get(&ino).await, Some(Some(_))),
        "lookup_with_attr should warm the Redis store node cache"
    );
    let script_calls = redis_script_calls(&store).await;
    assert!(
        (1..=2).contains(&script_calls),
        "Redis lookup_with_attr should use one business Lua script execution; observed {script_calls} script calls including possible EVALSHA warm-up"
    );
    assert_eq!(
        redis_command_calls(&store, "hget").await,
        1,
        "Redis lookup_with_attr should perform one directory HGET inside Lua"
    );
    assert_eq!(
        redis_command_calls(&store, "get").await,
        1,
        "Redis lookup_with_attr should perform one node GET inside Lua"
    );

    let missing = store.lookup_with_attr(root, "missing.txt").await.unwrap();
    assert!(missing.is_none());
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_meta_client_stat_fresh_uses_warm_store_node_cache() {
    let store = Arc::new(new_test_store().await);
    let root = store.root_ino();
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );

    let ino = client
        .create_file(root, "fresh_hot.txt".to_string())
        .await
        .unwrap();
    client.stat_fresh(ino).await.unwrap().unwrap();

    reset_redis_commandstats(&store).await;
    for _ in 0..10 {
        client.stat_fresh(ino).await.unwrap().unwrap();
    }

    let get_calls = redis_command_calls(&store, "get").await;
    assert_eq!(
        get_calls, 0,
        "hot stat_fresh should reuse RedisMetaStore node_cache instead of issuing Redis GET calls"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_vfs_deleted_inode_timestamp_setattr_stays_local() {
    let store = Arc::new(new_test_store().await);
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );
    client.initialize().await.unwrap();

    let fs = VFS::with_meta_layer_with_default_background(
        ChunkLayout::default(),
        Arc::new(InMemoryBlockStore::new()),
        client,
    )
    .unwrap();
    let root = fs.root_ino();
    let ino = fs
        .create_file_at(root, "deleted_setattr.txt", true)
        .await
        .unwrap();
    fs.unlink_at(root, "deleted_setattr.txt").await.unwrap();

    reset_redis_commandstats(&store).await;
    let attr = fs
        .set_attr(
            ino,
            &SetAttrRequest {
                mtime: Some(123),
                ctime: Some(456),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();

    assert_eq!(attr.nlink, 0);
    assert_eq!(attr.mtime, 123);
    assert_eq!(attr.ctime, 456);
    assert_eq!(
        redis_command_calls(&store, "get").await,
        0,
        "timestamp-only setattr for a just-deleted inode should avoid Redis GET"
    );
    assert_eq!(
        redis_command_calls(&store, "set").await,
        0,
        "timestamp-only setattr for a just-deleted inode should avoid Redis SET"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_vfs_close_without_write_skips_timestamp_metadata_update() {
    let store = Arc::new(new_test_store().await);
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );
    client.initialize().await.unwrap();

    let fs = VFS::with_meta_layer_with_default_background(
        ChunkLayout::default(),
        Arc::new(InMemoryBlockStore::new()),
        client,
    )
    .unwrap();
    let root = fs.root_ino();
    let ino = fs
        .create_file_at(root, "empty_close.txt", true)
        .await
        .unwrap();
    let attr = fs.stat_ino(ino).await.unwrap();
    let fh = fs
        .open_with_cached_attr(ino, attr, false, true, false)
        .await
        .unwrap();

    reset_redis_commandstats(&store).await;
    fs.close(fh).await.unwrap();

    assert_eq!(
        redis_command_calls(&store, "set").await,
        0,
        "closing a write-opened handle with no writes should avoid timestamp metadata SET"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_vfs_flush_without_write_skips_timestamp_metadata_update() {
    let store = Arc::new(new_test_store().await);
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );
    client.initialize().await.unwrap();

    let fs = VFS::with_meta_layer_with_default_background(
        ChunkLayout::default(),
        Arc::new(InMemoryBlockStore::new()),
        client,
    )
    .unwrap();
    let root = fs.root_ino();
    let ino = fs
        .create_file_at(root, "empty_flush.txt", true)
        .await
        .unwrap();
    let attr = fs.stat_ino(ino).await.unwrap();
    let fh = fs
        .open_with_cached_attr(ino, attr, false, true, false)
        .await
        .unwrap();

    reset_redis_commandstats(&store).await;
    fs.flush(fh).await.unwrap();

    assert_eq!(
        redis_command_calls(&store, "set").await,
        0,
        "flushing a write-opened handle with no writes should avoid timestamp metadata SET"
    );
    fs.close(fh).await.unwrap();
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_fuse_flush_without_posix_locks_skips_unlock_metadata() {
    let store = Arc::new(new_test_store().await);
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );
    client.initialize().await.unwrap();

    let fs = VFS::with_meta_layer_with_default_background(
        ChunkLayout::default(),
        Arc::new(InMemoryBlockStore::new()),
        client,
    )
    .unwrap();
    let root = fs.root_ino();
    let ino = fs
        .create_file_at(root, "fuse_flush_no_lock.txt", true)
        .await
        .unwrap();
    let fh = fs.open_fresh_ino(ino, false, true, false).await.unwrap();

    reset_redis_commandstats(&store).await;
    <VFS<InMemoryBlockStore, MetaClient<RedisMetaStore>> as rfuse3::raw::Filesystem>::flush(
        &fs,
        rfuse3::raw::Request {
            unique: 1,
            uid: 0,
            gid: 0,
            pid: 0,
        },
        ino as u64,
        fh,
        0xabc,
    )
    .await
    .unwrap();

    let script_calls = redis_script_calls(&store).await;
    assert!(
        script_calls <= 1,
        "FUSE flush without any known POSIX locks should skip redundant Redis lock-cleanup scripts; observed {script_calls}"
    );
    fs.close(fh).await.unwrap();
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_fuse_flush_releases_known_posix_lock_owner() {
    let store = Arc::new(new_test_store().await);
    let client = MetaClient::new(
        store.clone(),
        CacheCapacity {
            inode: 100,
            path: 100,
        },
        CacheTtl::for_redis(),
    );
    client.initialize().await.unwrap();

    let fs = VFS::with_meta_layer_with_default_background(
        ChunkLayout::default(),
        Arc::new(InMemoryBlockStore::new()),
        client,
    )
    .unwrap();
    let root = fs.root_ino();
    let ino = fs
        .create_file_at(root, "fuse_flush_lock.txt", true)
        .await
        .unwrap();
    let fh = fs.open_fresh_ino(ino, true, true, false).await.unwrap();
    let owner = 0_u64;
    let range = FileLockRange { start: 0, end: 1 };
    store
        .set_plock(ino, owner as i64, false, FileLockType::Write, range, 1234)
        .await
        .unwrap();
    fs.remember_posix_lock_owner(ino, owner as i64, FileLockType::Write);

    let conflict = fs
        .get_plock_ino(
            ino,
            &FileLockQuery {
                owner: owner as i64,
                lock_type: FileLockType::Write,
                range,
            },
        )
        .await
        .unwrap();
    assert_eq!(conflict.lock_type, FileLockType::Write);

    <VFS<InMemoryBlockStore, MetaClient<RedisMetaStore>> as rfuse3::raw::Filesystem>::flush(
        &fs,
        rfuse3::raw::Request {
            unique: 2,
            uid: 0,
            gid: 0,
            pid: 0,
        },
        ino as u64,
        fh,
        owner,
    )
    .await
    .unwrap();

    let after_flush = fs
        .get_plock_ino(
            ino,
            &FileLockQuery {
                owner: owner as i64,
                lock_type: FileLockType::Write,
                range,
            },
        )
        .await
        .unwrap();
    assert_eq!(after_flush.lock_type, FileLockType::UnLock);
    fs.close(fh).await.unwrap();
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_symlink_lookup_path_flow() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let (ino, attr) = store
        .symlink(root, "link.txt", "/target/path")
        .await
        .unwrap();
    assert_eq!(attr.kind, crate::meta::store::FileType::Symlink);

    let resolved = store.lookup_path("/link.txt").await.unwrap();
    assert_eq!(resolved, Some((ino, crate::meta::store::FileType::Symlink)));

    let target = store.read_symlink(ino).await.unwrap();
    assert_eq!(target, "/target/path");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_readdir_basic_flow() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir = store.mkdir(root, "readdir_test".to_string()).await.unwrap();
    let f1 = store
        .create_file(dir, "file.txt".to_string())
        .await
        .unwrap();
    let d1 = store.mkdir(dir, "subdir".to_string()).await.unwrap();
    let (s1, _) = store.symlink(dir, "link.txt", "/dest").await.unwrap();

    let entries = store.readdir(dir).await.unwrap();
    assert_eq!(entries.len(), 3);

    let mut found = std::collections::HashSet::new();
    for e in &entries {
        found.insert((e.ino, e.name.clone(), e.kind));
    }

    assert!(found.contains(&(
        f1,
        "file.txt".to_string(),
        crate::meta::store::FileType::File
    )));
    assert!(found.contains(&(d1, "subdir".to_string(), crate::meta::store::FileType::Dir)));
    assert!(found.contains(&(
        s1,
        "link.txt".to_string(),
        crate::meta::store::FileType::Symlink
    )));
}

// --- State machine tests ---

#[serial]
#[tokio::test]
#[ignore]
async fn test_hardlink_state_machine_full_transition() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let ino = store
        .create_file(root, "origin.txt".to_string())
        .await
        .unwrap();
    let node1 = store.get_node(ino).await.unwrap().unwrap();
    assert_eq!(node1.attr.nlink, 1);
    assert_eq!(node1.parent, root);
    assert_eq!(node1.name, "origin.txt");

    store.link(ino, root, "link.txt").await.unwrap();
    let node2 = store.get_node(ino).await.unwrap().unwrap();
    assert_eq!(node2.attr.nlink, 2);
    assert_eq!(node2.parent, 0, "hardlink state parent should be 0");
    assert_eq!(node2.name, "", "hardlink state name should be empty");

    let link_parents = store.load_link_parents(ino).await.unwrap();
    assert_eq!(link_parents.len(), 2);
    assert!(link_parents.contains(&(root, "origin.txt".to_string())));
    assert!(link_parents.contains(&(root, "link.txt".to_string())));

    store.unlink(root, "origin.txt").await.unwrap();
    let node3 = store.get_node(ino).await.unwrap().unwrap();
    assert_eq!(node3.attr.nlink, 1);
    assert_eq!(node3.parent, root, "restored to single parent");
    assert_eq!(node3.name, "link.txt", "restored to single name");

    let link_parents_after = store.load_link_parents(ino).await.unwrap();
    assert!(
        link_parents_after.is_empty(),
        "link_parents should be cleared when nlink=1"
    );

    store.unlink(root, "link.txt").await.unwrap();
    let deleted = store.get_deleted_files().await.unwrap();
    assert!(
        deleted.contains(&ino),
        "should enter deleted after last link removed"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_directory_nlink_state_machine() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let base_nlink = store.stat(root).await.unwrap().unwrap().nlink;

    let d1 = store.mkdir(root, "d1".to_string()).await.unwrap();
    assert_eq!(
        store.stat(root).await.unwrap().unwrap().nlink,
        base_nlink + 1
    );

    let _d2 = store.mkdir(d1, "d2".to_string()).await.unwrap();
    assert_eq!(store.stat(d1).await.unwrap().unwrap().nlink, base_nlink + 1);

    store.rmdir(d1, "d2").await.unwrap();
    assert_eq!(
        store.stat(d1).await.unwrap().unwrap().nlink,
        base_nlink,
        "nlink restored after child directory removal"
    );

    store.rmdir(root, "d1").await.unwrap();
    assert_eq!(
        store.stat(root).await.unwrap().unwrap().nlink,
        base_nlink,
        "root nlink should be restored after directory removal"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_set_attr_flags_state_transitions() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "attr_test.txt".to_string())
        .await
        .unwrap();

    let req1 = SetAttrRequest {
        mode: Some(0o4755),
        ..Default::default()
    };
    let attr1 = store
        .set_attr(ino, &req1, SetAttrFlags::empty())
        .await
        .unwrap();
    assert_eq!(
        attr1.mode & 0o7777,
        0o755,
        "setuid bit should be stripped on persistence"
    );

    let req2 = SetAttrRequest {
        uid: Some(1000),
        gid: Some(1000),
        ..Default::default()
    };
    let attr2 = store
        .set_attr(ino, &req2, SetAttrFlags::empty())
        .await
        .unwrap();
    assert_eq!(attr2.uid, 1000);
    assert_eq!(attr2.gid, 1000);

    let old_ctime = attr2.ctime;
    let req3 = SetAttrRequest {
        size: Some(1234),
        ..Default::default()
    };
    let attr3 = store
        .set_attr(ino, &req3, SetAttrFlags::empty())
        .await
        .unwrap();
    assert_eq!(attr3.size, 1234);
    assert!(attr3.ctime >= old_ctime, "size change should update ctime");

    let attr4 = store
        .set_attr(ino, &SetAttrRequest::default(), SetAttrFlags::CLEAR_SUID)
        .await
        .unwrap();
    assert_eq!(attr4.mode & 0o4000, 0);

    let attr5 = store
        .set_attr(ino, &SetAttrRequest::default(), SetAttrFlags::CLEAR_SGID)
        .await
        .unwrap();
    assert_eq!(attr5.mode & 0o2000, 0);

    let attr6 = store
        .set_attr(ino, &SetAttrRequest::default(), SetAttrFlags::SET_ATIME_NOW)
        .await
        .unwrap();
    assert!(attr6.atime > 0);

    let attr7 = store
        .set_attr(ino, &SetAttrRequest::default(), SetAttrFlags::SET_MTIME_NOW)
        .await
        .unwrap();
    assert!(attr7.mtime > 0);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_deleted_node_query_behavior() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let ino = store
        .create_file(root, "del_query.txt".to_string())
        .await
        .unwrap();
    store.unlink(root, "del_query.txt").await.unwrap();

    assert!(
        store.stat(ino).await.unwrap().is_some(),
        "tombstone should be preserved, stat still visible"
    );
    let names = store.get_names(ino).await.unwrap();
    assert!(
        names.is_empty(),
        "get_names should return empty for deleted node"
    );
    let paths = store.get_paths(ino).await.unwrap();
    assert!(
        paths.is_empty(),
        "get_paths should return empty for deleted node"
    );
}

// --- Consistency tests ---

#[serial]
#[tokio::test]
#[ignore]
async fn test_write_consistency() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "write_consist.txt".to_string())
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();

    let slice = crate::chunk::SliceDesc {
        slice_id: 201,
        chunk_id,
        offset: 0,
        length: 8192,
    };
    store.write(ino, chunk_id, slice, 8192).await.unwrap();

    let attr = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr.size, 8192, "size should be consistent after write");

    let slices = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(slices.len(), 1);
    assert_eq!(slices[0].slice_id, 201);
    assert_eq!(slices[0].length, 8192);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_truncate_consistency() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "truncate_consist.txt".to_string())
        .await
        .unwrap();
    let chunk_size = 4096u64;
    let chunk_id0 = crate::vfs::chunk_id_for(ino, 0).unwrap();
    let chunk_id1 = crate::vfs::chunk_id_for(ino, 1).unwrap();

    store
        .append_slice(
            chunk_id0,
            crate::chunk::SliceDesc {
                slice_id: 301,
                chunk_id: chunk_id0,
                offset: 0,
                length: chunk_size,
            },
        )
        .await
        .unwrap();
    store
        .append_slice(
            chunk_id1,
            crate::chunk::SliceDesc {
                slice_id: 302,
                chunk_id: chunk_id1,
                offset: 0,
                length: chunk_size,
            },
        )
        .await
        .unwrap();
    store.set_file_size(ino, chunk_size * 2).await.unwrap();

    store
        .truncate(ino, chunk_size / 2, chunk_size)
        .await
        .unwrap();

    let attr = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(attr.size, chunk_size / 2);

    let slices0 = store.get_slices(chunk_id0).await.unwrap();
    assert!(
        !slices0.is_empty(),
        "partially truncated chunk should be preserved"
    );
    assert!(
        slices0
            .iter()
            .all(|s| s.offset + s.length <= chunk_size / 2 || s.offset <= chunk_size / 2)
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_delayed_slice_workflow_consistency() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "delayed_test.txt".to_string())
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();

    let old_slice = crate::chunk::SliceDesc {
        slice_id: 401,
        chunk_id,
        offset: 0,
        length: 1024,
    };
    store.append_slice(chunk_id, old_slice).await.unwrap();

    let new_slice = crate::chunk::SliceDesc {
        slice_id: 402,
        chunk_id,
        offset: 0,
        length: 1024,
    };
    let delayed_data = crate::chunk::SliceDesc::encode_delayed_data(&[old_slice], &[401]);
    store
        .replace_slices_for_compact(chunk_id, &[new_slice], &delayed_data)
        .await
        .unwrap();

    let slices_after = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(slices_after.len(), 1);
    assert_eq!(slices_after[0].slice_id, 402);

    let delayed = store.process_delayed_slices(10, 0).await.unwrap();
    assert_eq!(delayed.len(), 1, "one delayed slice should be processed");
    assert_eq!(delayed[0].0, 401);

    let delayed_ids: Vec<i64> = delayed.iter().map(|d| d.3).collect();
    store.confirm_delayed_deleted(&delayed_ids).await.unwrap();

    let delayed_after = store.process_delayed_slices(10, 0).await.unwrap();
    assert!(
        delayed_after.is_empty(),
        "no delayed slice should remain after confirmation"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_uncommitted_slice_workflow_consistency() {
    let store = new_test_store().await;
    let slice_id = 501u64;
    let chunk_id = 1001u64;

    let id = store
        .record_uncommitted_slice(slice_id, chunk_id, 2048, "write")
        .await
        .unwrap();
    assert_eq!(id, slice_id as i64);

    let orphans_before = store
        .cleanup_orphan_uncommitted_slices(3600, 10)
        .await
        .unwrap();
    assert!(
        orphans_before.is_empty(),
        "freshly recorded slice should not be orphan"
    );

    store.confirm_slice_committed(slice_id).await.unwrap();

    let orphans_after = store
        .cleanup_orphan_uncommitted_slices(0, 10)
        .await
        .unwrap();
    assert!(
        orphans_after.is_empty(),
        "no uncommitted records should remain after confirm"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_get_names_paths_hardlink_consistency() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_a = store.mkdir(root, "ha".to_string()).await.unwrap();
    let dir_b = store.mkdir(root, "hb".to_string()).await.unwrap();

    let ino = store.create_file(dir_a, "f.txt".to_string()).await.unwrap();
    store.link(ino, dir_b, "g.txt").await.unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert_eq!(names.len(), 2);

    let paths = store.get_paths(ino).await.unwrap();
    assert_eq!(paths.len(), 2, "hardlink should have two paths");
    assert!(paths.iter().any(|p| p == "/ha/f.txt"));
    assert!(paths.iter().any(|p| p == "/hb/g.txt"));
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_rename_hardlink_cross_dir_consistency() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_a = store.mkdir(root, "ra".to_string()).await.unwrap();
    let dir_b = store.mkdir(root, "rb".to_string()).await.unwrap();
    let dir_c = store.mkdir(root, "rc".to_string()).await.unwrap();

    let ino = store.create_file(dir_a, "f.txt".to_string()).await.unwrap();
    store.link(ino, dir_b, "g.txt").await.unwrap();

    store
        .rename(dir_a, "f.txt", dir_c, "h.txt".to_string())
        .await
        .unwrap();

    assert_eq!(store.lookup(dir_a, "f.txt").await.unwrap(), None);
    assert_eq!(store.lookup(dir_b, "g.txt").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_c, "h.txt").await.unwrap(), Some(ino));

    let link_parents = store.load_link_parents(ino).await.unwrap();
    assert_eq!(link_parents.len(), 2);
    assert!(link_parents.contains(&(dir_b, "g.txt".to_string())));
    assert!(link_parents.contains(&(dir_c, "h.txt".to_string())));

    let names = store.get_names(ino).await.unwrap();
    assert_eq!(names.len(), 2);
}

// --- Fallback tests (error handling & edge cases) ---

#[serial]
#[tokio::test]
#[ignore]
async fn test_stat_nonexistent_inode_fallback() {
    let store = new_test_store().await;
    let result = store.stat(999999).await.unwrap();
    assert!(
        result.is_none(),
        "stat on nonexistent inode should return None"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_set_attr_nonexistent_inode_fallback() {
    let store = new_test_store().await;
    let req = SetAttrRequest {
        mode: Some(0o644),
        ..Default::default()
    };
    let result = store.set_attr(999999, &req, SetAttrFlags::empty()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotFound(ino) => assert_eq!(ino, 999999),
        other => panic!("expected NotFound, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_truncate_nonexistent_inode_fallback() {
    let store = new_test_store().await;
    let result = store.truncate(999999, 1024, 4096).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotFound(ino) => assert_eq!(ino, 999999),
        other => panic!("expected NotFound, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_read_symlink_on_non_symlink_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "notalink.txt".to_string())
        .await
        .unwrap();

    let result = store.read_symlink(ino).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotSupported(msg) => assert!(msg.contains("not a symbolic link")),
        other => panic!("expected NotSupported, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_readdir_non_directory_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let ino = store
        .create_file(root, "file_for_readdir.txt".to_string())
        .await
        .unwrap();

    let result = store.readdir(ino).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotDirectory(i) => assert_eq!(i, ino),
        other => panic!("expected NotDirectory, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_readdir_nonexistent_inode_fallback() {
    let store = new_test_store().await;
    let result = store.readdir(999999).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotFound(ino) => assert_eq!(ino, 999999),
        other => panic!("expected NotFound, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_unlink_directory_rejected_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();
    let dir = store.mkdir(root, "unlink_me".to_string()).await.unwrap();

    let result = store.unlink(root, "unlink_me").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotSupported(msg) => assert!(msg.contains("not unlinkable")),
        other => panic!("expected NotSupported, got {:?}", other),
    }

    assert_eq!(
        store.lookup(root, "unlink_me").await.unwrap(),
        Some(dir),
        "directory should not be deleted"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_link_root_inode_rejected_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let result = store.link(root, root, "root_link").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotSupported(msg) => assert!(msg.contains("root inode")),
        other => panic!("expected NotSupported, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_stat_fs_batches_node_fetches_with_mget() {
    let store = new_test_store().await;
    let root = store.root_ino();

    for idx in 0..4 {
        let ino = store
            .create_file(root, format!("sf_batch_{idx}.txt"))
            .await
            .unwrap();
        store.set_file_size(ino, 1024 + idx).await.unwrap();
    }
    store.mkdir(root, "sf_batch_dir".to_string()).await.unwrap();

    reset_redis_commandstats(&store).await;
    let snap = store.stat_fs().await.unwrap();

    assert!(snap.used_inodes >= 6);
    let get_calls = redis_command_calls(&store, "get").await;
    let mget_calls = redis_command_calls(&store, "mget").await;
    assert!(
        get_calls <= 1,
        "stat_fs should batch node loads instead of issuing one GET per inode; observed {get_calls} GET calls"
    );
    assert_eq!(
        mget_calls, 1,
        "stat_fs should fetch all node payloads with one Redis MGET"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_stat_fs_accounting_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let f1 = store
        .create_file(root, "sf1.txt".to_string())
        .await
        .unwrap();
    store.set_file_size(f1, 1000).await.unwrap();

    let _d1 = store.mkdir(root, "sf_dir".to_string()).await.unwrap();

    let (s1, _) = store.symlink(root, "sf_link", "/target").await.unwrap();
    store.set_file_size(s1, 6).await.unwrap();

    let snap = store.stat_fs().await.unwrap();
    assert_eq!(
        snap.used_inodes, 4,
        "should count 4 non-deleted inodes (including root)"
    );
    let used_space = snap.total_space - snap.available_space;
    assert_eq!(
        used_space, 1536,
        "should count allocated file and symlink blocks"
    );
    assert!(snap.total_space > used_space);
    assert!(snap.available_inodes > 0);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_counter_operations_fallback() {
    let store = new_test_store().await;

    let id1 = store.next_id(crate::meta::INODE_ID_KEY).await.unwrap();
    let id2 = store.next_id(crate::meta::INODE_ID_KEY).await.unwrap();
    assert!(id2 > id1, "next_id should be monotonically increasing");

    let counter_name = "test_counter_ops";
    let v0 = store.get_counter(counter_name).await.unwrap();
    assert_eq!(v0, 0, "nonexistent counter should default to 0");

    let v1 = store.incr_counter(counter_name, 5).await.unwrap();
    assert_eq!(v1, 5);

    let v2 = store.incr_counter(counter_name, -3).await.unwrap();
    assert_eq!(v2, 2);

    let set_result = store
        .set_counter_if_small(counter_name, 100, 10)
        .await
        .unwrap();
    assert!(set_result, "current 2 < 90, should allow set");
    assert_eq!(store.get_counter(counter_name).await.unwrap(), 100);

    let set_result2 = store
        .set_counter_if_small(counter_name, 105, 10)
        .await
        .unwrap();
    assert!(!set_result2, "current 100 >= 95, should reject set");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_global_lock_ttl_and_reacquire_fallback() {
    let store = new_test_store().await;
    let lock_name = LockName::ChunkCompactLock(12345);

    let acquired1 = store.get_global_lock(lock_name.clone(), 1).await;
    assert!(acquired1, "first lock acquisition should succeed");

    let acquired2 = store.get_global_lock(lock_name.clone(), 1).await;
    assert!(!acquired2, "re-acquire within TTL should fail");

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let acquired3 = store.get_global_lock(lock_name.clone(), 1).await;
    assert!(acquired3, "should re-acquire after TTL expires");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_init_root_idempotent_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let attr_before = store.stat(root).await.unwrap().unwrap();
    store.initialize().await.unwrap();
    store.initialize().await.unwrap();
    let attr_after = store.stat(root).await.unwrap().unwrap();

    assert_eq!(attr_before.ino, attr_after.ino);
    assert_eq!(attr_before.mode, attr_after.mode);

    let counter = store.get_counter("nextinode").await.unwrap();
    assert!(counter >= 2, "counter should not be reset to 1");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_get_names_paths_deleted_inode_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let ino = store
        .create_file(root, "del_paths.txt".to_string())
        .await
        .unwrap();
    store.unlink(root, "del_paths.txt").await.unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert!(
        names.is_empty(),
        "get_names on deleted inode should be empty"
    );

    let paths = store.get_paths(ino).await.unwrap();
    assert!(
        paths.is_empty(),
        "get_paths on deleted inode should be empty"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_create_file_in_non_directory_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "not_a_dir.txt".to_string())
        .await
        .unwrap();
    let result = store.create_file(file_ino, "child.txt".to_string()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotDirectory(ino) => assert_eq!(ino, file_ino),
        MetaError::ParentNotFound(ino) => assert_eq!(ino, file_ino),
        other => panic!("expected NotDirectory or ParentNotFound, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_mkdir_in_file_rejected_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let file_ino = store
        .create_file(root, "file_for_mkdir.txt".to_string())
        .await
        .unwrap();
    let result = store.mkdir(file_ino, "child_dir".to_string()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::NotDirectory(ino) => assert_eq!(ino, file_ino),
        MetaError::ParentNotFound(ino) => assert_eq!(ino, file_ino),
        other => panic!("expected NotDirectory or ParentNotFound, got {:?}", other),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_lookup_path_empty_and_invalid_fallback() {
    let store = new_test_store().await;

    assert_eq!(
        store.lookup_path("").await.unwrap(),
        None,
        "empty path should return None"
    );
    assert_eq!(
        store.lookup_path("/a/b/c/d/e/f").await.unwrap(),
        None,
        "nonexistent path should return None"
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_batch_stat_empty_fallback() {
    let store = new_test_store().await;
    let result = store.batch_stat(&[]).await.unwrap();
    assert!(result.is_empty(), "empty input should return empty Vec");
}

/// Verifies that a directory with size 0 remains accessible after set_file_size.
#[serial]
#[tokio::test]
#[ignore]
async fn test_set_file_size_on_directory_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir = store.mkdir(root, "sized_dir".to_string()).await.unwrap();
    store.set_file_size(dir, 0).await.unwrap();
    let attr = store.stat(dir).await.unwrap().unwrap();
    assert_eq!(attr.size, 0);
    assert_eq!(attr.kind, crate::meta::store::FileType::Dir);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_list_chunk_ids_empty_and_zero_limit_fallback() {
    let store = new_test_store().await;

    let r1 = store.list_chunk_ids(0).await.unwrap();
    assert!(r1.is_empty(), "limit=0 should return empty");

    let r2 = store.list_chunk_ids(10).await.unwrap();
    assert!(r2.is_empty(), "should return empty when no chunks exist");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_extend_file_size_on_directory_fallback() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir = store.mkdir(root, "extend_dir".to_string()).await.unwrap();
    store.extend_file_size(dir, 0).await.unwrap();
    let attr = store.stat(dir).await.unwrap().unwrap();
    assert_eq!(attr.size, 0);
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_cleanup_orphan_uncommitted_slice_fallback() {
    let store = new_test_store().await;
    let slice_id = 601u64;
    let chunk_id = 2001u64;

    store
        .record_uncommitted_slice(slice_id, chunk_id, 4096, "write")
        .await
        .unwrap();

    let orphans = store
        .cleanup_orphan_uncommitted_slices(0, 10)
        .await
        .unwrap();
    assert_eq!(
        orphans.len(),
        1,
        "uncommitted slice not written to chunk should be orphan"
    );
    assert_eq!(orphans[0], (slice_id, 4096));

    let orphans2 = store
        .cleanup_orphan_uncommitted_slices(0, 10)
        .await
        .unwrap();
    assert_eq!(
        orphans2.len(),
        1,
        "rescan should still find orphan index record"
    );

    store.delete_uncommitted_slices(&[slice_id]).await.unwrap();
    let orphans3 = store
        .cleanup_orphan_uncommitted_slices(0, 10)
        .await
        .unwrap();
    assert!(
        orphans3.is_empty(),
        "should be fully cleaned after delete_uncommitted_slices"
    );
}

// ---------------------------------------------------------------------------
// Concurrent file lock tests — simulates generic/089 (t_mtab) scenario
// ---------------------------------------------------------------------------

/// Helper: acquire a write lock on the given file, optionally blocking.
async fn acquire_write_lock(
    store: &RedisMetaStore,
    inode: i64,
    owner: i64,
    block: bool,
    pid: u32,
) -> Result<(), MetaError> {
    store
        .set_plock(
            inode,
            owner,
            block,
            FileLockType::Write,
            FileLockRange { start: 0, end: 1 },
            pid,
        )
        .await
}

/// Helper: release a lock on the given file.
async fn release_lock(
    store: &RedisMetaStore,
    inode: i64,
    owner: i64,
    pid: u32,
) -> Result<(), MetaError> {
    store
        .set_plock(
            inode,
            owner,
            false,
            FileLockType::UnLock,
            FileLockRange { start: 0, end: 1 },
            pid,
        )
        .await
}

/// Simulate the t_mtab lock pattern: N concurrent tasks each doing
/// `iterations` lock-acquire-release cycles on the same file.
async fn run_concurrent_lock_tasks(
    store: Arc<RedisMetaStore>,
    inode: i64,
    num_tasks: usize,
    iterations: usize,
) -> Vec<usize> {
    let mut handles = Vec::with_capacity(num_tasks);

    for task_id in 0..num_tasks {
        let store = store.clone();
        let owner: i64 = 1001 + task_id as i64;
        let pid: u32 = 500 + task_id as u32;

        handles.push(tokio::spawn(async move {
            let mut completed: usize = 0;
            for _ in 0..iterations {
                // Acquire write lock (blocking, like F_SETLKW)
                acquire_write_lock(&store, inode, owner, true, pid)
                    .await
                    .expect("failed to acquire write lock");

                // Simulate work: write temp file + rename
                tokio::time::sleep(Duration::from_micros(50)).await;

                // Release lock
                release_lock(&store, inode, owner, pid)
                    .await
                    .expect("failed to release lock");

                completed += 1;
            }
            completed
        }));
    }

    let mut results = Vec::with_capacity(num_tasks);
    for handle in handles {
        match handle.await {
            Ok(n) => results.push(n),
            Err(e) => panic!("concurrent lock task panicked: {e}"),
        }
    }
    results
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_concurrent_write_lock_three_tasks_50_iterations() {
    let store = Arc::new(new_test_store_with_epoch().await);
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "lockfile_mtab".to_string())
        .await
        .expect("failed to create lock file");

    // 3 tasks × 50 iterations each = t_mtab 50 pattern
    let results = run_concurrent_lock_tasks(store, file_ino, 3, 50).await;

    assert_eq!(results.len(), 3, "all 3 tasks must complete");
    for (i, &completed) in results.iter().enumerate() {
        assert_eq!(
            completed, 50,
            "task {i} completed {completed}/50 iterations"
        );
    }
}

/// Verify that after all locks are released, get_plock returns UnLock (no conflict).
#[serial]
#[tokio::test]
#[ignore]
async fn test_write_lock_acquire_release_getlk() {
    let store = Arc::new(new_test_store_with_epoch().await);
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "lockfile_getlk".to_string())
        .await
        .expect("failed to create file");

    // Acquire write lock
    acquire_write_lock(&store, file_ino, 1001, false, 500)
        .await
        .expect("should acquire write lock");

    // get_plock should see the lock
    let query = FileLockQuery {
        owner: 1001,
        lock_type: FileLockType::Read,
        range: FileLockRange { start: 0, end: 1 },
    };
    let info = store.get_plock(file_ino, &query).await.unwrap();
    // Since the stored lock is a Write lock, querying with Read on the same
    // range should report the Write lock holder.
    assert_eq!(
        info.lock_type,
        FileLockType::Write,
        "get_plock should see the write lock"
    );
    assert_eq!(info.pid, 500, "pid should match");

    // Release
    release_lock(&store, file_ino, 1001, 500)
        .await
        .expect("should release lock");

    // After release, get_plock should return UnLock
    let info2 = store.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(
        info2.lock_type,
        FileLockType::UnLock,
        "no lock should remain after release"
    );
}

/// Verify that write locks are mutually exclusive:
/// Task B should not acquire the lock while Task A holds it.
#[serial]
#[tokio::test]
#[ignore]
async fn test_write_lock_mutual_exclusion() {
    let store = Arc::new(new_test_store_with_epoch().await);
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "lockfile_mutex".to_string())
        .await
        .expect("failed to create file");

    // Task A acquires write lock
    acquire_write_lock(&store, file_ino, 1001, false, 500)
        .await
        .expect("task A should acquire write lock");

    // Task B tries non-blocking write lock — must fail with conflict
    let result = acquire_write_lock(&store, file_ino, 1002, false, 501).await;
    assert!(
        matches!(result, Err(MetaError::LockConflict { .. })),
        "task B should get LockConflict but got: {result:?}"
    );

    // Task A releases
    release_lock(&store, file_ino, 1001, 500)
        .await
        .expect("task A should release");

    // Now Task B should succeed
    acquire_write_lock(&store, file_ino, 1002, false, 501)
        .await
        .expect("task B should acquire after A releases");

    release_lock(&store, file_ino, 1002, 501)
        .await
        .expect("task B should release");
}

/// Verify that read locks can be shared concurrently (read-read no conflict).
#[serial]
#[tokio::test]
#[ignore]
async fn test_read_lock_shared() {
    let store = Arc::new(new_test_store_with_epoch().await);
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "lockfile_shared".to_string())
        .await
        .expect("failed to create file");

    // Task A acquires read lock
    store
        .set_plock(
            file_ino,
            1001,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            500,
        )
        .await
        .expect("task A should acquire read lock");

    // Task B acquires read lock on same range — should succeed (read-read OK)
    store
        .set_plock(
            file_ino,
            1002,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            501,
        )
        .await
        .expect("task B should also acquire read lock (read-read is shared)");

    // Task C tries write lock — must fail (read-write conflict)
    let result = store
        .set_plock(
            file_ino,
            1003,
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            502,
        )
        .await;
    assert!(
        matches!(result, Err(MetaError::LockConflict { .. })),
        "write should conflict with held read locks"
    );
}

#[serial]
#[tokio::test]
#[ignore = "requires Redis server"]
async fn test_set_plock_succeeds_on_nonexistent_inode() {
    let store = new_test_store_with_epoch().await;

    // set_plock on a non-existent inode must succeed because POSIX
    // advisory locks (plocks) are stored independently of node data.
    // The Lua script only reads/writes plock keys; it never references
    // the node.  Returning NotFound here would cause fcntl(F_SETLKW)
    // to fail when a lock-file inode has been unlinked by the lock
    // holder before a waiter can acquire the lock.
    let result = store
        .set_plock(
            99999, // non-existent inode
            1001,  // owner
            false, // non-blocking
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await;

    assert!(
        result.is_ok(),
        "set_plock on non-existent inode should succeed (plocks are indep of node), got: {:?}",
        result
    );
}

#[serial]
#[tokio::test]
#[ignore = "requires Redis server"]
async fn test_set_plock_succeeds_on_deleted_inode_via_unlink() {
    let store = new_test_store_with_epoch().await;
    let parent = store.root_ino();

    // Create a file, then unlink it so nlink drops to 0 and the node
    // is tombstoned.  set_plock on the tombstoned inode must still
    // succeed — the Lua script for ReadLock/WriteLock only touches
    // plock hashes.
    let file_ino = store
        .create_file(parent, "lockfile".to_string())
        .await
        .unwrap();
    store.unlink(parent, "lockfile").await.unwrap();

    let result = store
        .set_plock(
            file_ino,
            1001,
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await;

    assert!(
        result.is_ok(),
        "set_plock on unlinked (tombstoned) inode should succeed, got: {:?}",
        result
    );
}

#[serial]
#[tokio::test]
#[ignore = "requires Redis server"]
async fn test_blocking_set_plock_succeeds_after_unlink_releases_lock() {
    let store_a = new_test_store_with_epoch().await;
    let parent = store_a.root_ino();

    let file_ino = store_a
        .create_file(parent, "lockfile".to_string())
        .await
        .unwrap();

    // Session A acquires the write lock
    store_a
        .set_plock(
            file_ino,
            1001, // owner A
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // A releases its lock via UnLock (simulating close(fd) → release → unlock_owner_locks)
    store_a
        .set_plock(
            file_ino,
            1001,
            false,
            FileLockType::UnLock,
            FileLockRange {
                start: 0,
                end: u64::MAX,
            },
            0,
        )
        .await
        .unwrap();

    // A unlinks the lock file (simulating unlock_mtab)
    store_a.unlink(parent, "lockfile").await.unwrap();

    // Session B now tries to acquire the lock on the tombstoned inode.
    // This must succeed — the plock from A was cleared, and node
    // existence is irrelevant for plock semantics.
    let store_b = new_test_store_with_epoch().await;
    let result = store_b
        .set_plock(
            file_ino,
            2002, // owner B
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            5678,
        )
        .await;

    assert!(
        result.is_ok(),
        "set_plock on tombstoned inode after unlock should succeed, got: {:?}",
        result
    );
}
