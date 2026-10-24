//! Namespace contracts exercise public operations against current-row storage.
//! The paused backend proves ordering/read predicates without timing benchmarks.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::{Barrier, Notify};
use uuid::Uuid;

use super::backend::{
    CommitOutcome, CreateAllocation, EntityBackend, EntityPlan, RowCheck, RowSnapshot, RowWrite,
    TransactionAuthority,
};
use super::codec;
use super::keys::EntityKeys;
use super::lifecycle::Lifecycle;
use super::memory::MemoryEntityBackend;
use super::model::{EntityDentry, EntityInode, EntityKind, EntityValue, UpperInode};
use super::namespace::{CreateRequest, Credentials, EntityNamespace};
use super::redis::RedisEntityBackend;
use super::resolver::test_support::{MemoryLower, inode_record};
use super::resolver::{self, ImmutableLower};
use super::{EntityError, Result};
use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR, PosixAcl};

struct Fixture<B> {
    backend: Arc<B>,
    lower: Arc<MemoryLower>,
    keys: EntityKeys,
    ns: Arc<EntityNamespace<B, MemoryLower>>,
}

fn root() -> Credentials {
    Credentials {
        uid: 0,
        gid: 0,
        groups: vec![0],
        umask: 0o022,
    }
}

fn user(uid: u32) -> Credentials {
    Credentials {
        uid,
        gid: uid,
        groups: vec![uid],
        umask: 0o022,
    }
}

fn request(parent: i64, name: &[u8], kind: EntityKind) -> CreateRequest {
    CreateRequest {
        parent,
        name: name.to_vec(),
        kind,
        mode: if kind == EntityKind::Directory {
            0o777
        } else {
            0o666
        },
        rdev: 0,
        symlink_target: None,
    }
}

async fn attach<B: EntityBackend + 'static>(
    backend: Arc<B>,
    lower: Arc<MemoryLower>,
    keys: EntityKeys,
) -> Fixture<B> {
    attach_with_ttl(backend, lower, keys, 1_000_000).await
}

async fn attach_with_ttl<B: EntityBackend + 'static>(
    backend: Arc<B>,
    lower: Arc<MemoryLower>,
    keys: EntityKeys,
    ttl_ns: i64,
) -> Fixture<B> {
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), ttl_ns).await.unwrap();
    let ns = EntityNamespace::new(backend.clone(), lower.clone(), keys.clone(), session);
    Fixture {
        backend,
        lower,
        keys,
        ns,
    }
}

async fn fixture(lower: MemoryLower) -> Fixture<MemoryEntityBackend> {
    attach(
        Arc::new(MemoryEntityBackend::new(1000)),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await
}

#[tokio::test]
async fn data_handles_keep_open_access_and_reject_foreign_namespaces() {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"file", 2, 10);
    let first = fixture(lower).await;
    let handle = first.ns.open(1, b"file", 4, &root()).await.unwrap();
    assert_eq!(first.ns.validate_handle(&handle, 4), Ok(2));
    assert_eq!(
        first.ns.validate_handle(&handle, 2),
        Err(EntityError::Permission)
    );
    // An already opened descriptor remains usable after a permission change
    // and unlink, until its last close releases the orphan's admission pin.
    first.ns.chmod(2, 0, root()).await.unwrap();
    first
        .ns
        .unlink(1, b"file".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(first.ns.validate_handle(&handle, 4), Ok(2));
    let second = fixture(MemoryLower::empty()).await;
    assert!(matches!(
        second.ns.validate_handle(&handle, 4),
        Err(EntityError::Invalid(_))
    ));
}

#[tokio::test]
async fn inode_data_guard_retains_registry_identity_without_a_handle() {
    let fixture = fixture(MemoryLower::empty()).await;
    let first = fixture.ns.lock_inode(2).await;
    let reopened_wrapper = EntityNamespace::new(
        fixture.backend.clone(),
        fixture.lower.clone(),
        fixture.keys.clone(),
        fixture.ns.session.clone(),
    );
    let waiting = reopened_wrapper.lock_inode(2);
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    // A different inode is independent while the same-inode operation waits.
    let other = fixture.ns.lock_inode(3).await;
    drop(first);
    let _second = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap();
    drop(other);
}

#[tokio::test]
async fn inode_read_guard_retains_identity_and_does_not_overtake_waiting_writers() {
    let fixture = fixture(MemoryLower::empty()).await;
    let first = fixture.ns.read_lock_inode(2).await;
    let reopened = EntityNamespace::new(
        fixture.backend.clone(),
        fixture.lower.clone(),
        fixture.keys.clone(),
        fixture.ns.session.clone(),
    );
    let writer = reopened.lock_inode(2);
    tokio::pin!(writer);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut writer)
            .await
            .is_err()
    );
    let reader = fixture.ns.read_lock_inode(2);
    tokio::pin!(reader);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut reader)
            .await
            .is_err()
    );
    drop(first);
    let writer = tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut reader)
            .await
            .is_err()
    );
    drop(writer);
    let _reader = tokio::time::timeout(Duration::from_secs(1), reader)
        .await
        .unwrap();
}

#[tokio::test]
async fn interrupted_truncate_fences_namespace_mutations_until_cleanup_finishes() {
    use super::recovery::{Recovery, prepare_resize};
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"file", 2, 20);
    let fixture = fixture(lower).await;
    let inode = current_inode(&fixture, 2).await;
    let authority = fixture.ns.authority().await.unwrap();
    let cut = prepare_resize(&fixture.keys, 2, None, &inode, 3, 8, 1000).unwrap();
    assert_eq!(
        fixture.backend.commit(&authority, &cut).await.unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        fixture.ns.chmod(2, 0o600, root()).await,
        Err(EntityError::Busy)
    );
    assert_eq!(
        fixture
            .ns
            .set_xattr(2, b"user.test".to_vec(), Some(vec![1]), root())
            .await,
        Err(EntityError::Busy)
    );
    assert_eq!(
        fixture.ns.link(2, 1, b"alias".to_vec(), root()).await,
        Err(EntityError::Busy)
    );
    assert_eq!(
        fixture.ns.unlink(1, b"file".to_vec(), false, root()).await,
        Err(EntityError::Busy)
    );
    assert_eq!(
        fixture
            .ns
            .rename(1, b"file".to_vec(), 1, b"moved".to_vec(), false, root())
            .await,
        Err(EntityError::Busy)
    );
    assert_eq!(current_inode(&fixture, 2).await.size, 3);
    assert!(
        Recovery::new(fixture.backend.clone(), fixture.keys.clone())
            .truncate_step(&authority, 2)
            .await
            .unwrap()
    );
    fixture.ns.chmod(2, 0o600, root()).await.unwrap();
    fixture
        .ns
        .link(2, 1, b"alias".to_vec(), root())
        .await
        .unwrap();
    assert_eq!(current_inode(&fixture, 2).await.nlink, 2);
}

async fn current_inode<B: EntityBackend + 'static>(fixture: &Fixture<B>, ino: i64) -> EntityInode {
    let snapshot = fixture
        .backend
        .read(&[fixture.keys.inode(ino)])
        .await
        .unwrap();
    let upper: Option<UpperInode> = snapshot.rows[0]
        .as_deref()
        .map(codec::decode)
        .transpose()
        .unwrap();
    resolver::resolve_inode(
        fixture.lower.as_ref(),
        fixture.lower.binding(),
        ino,
        upper.as_ref(),
    )
    .await
    .unwrap()
    .unwrap()
}

fn add_directory(lower: &mut MemoryLower, parent: i64, name: &[u8], ino: i64, mode: u32) {
    let mut inode = inode_record(ino, EntityKind::Directory, 0);
    inode.parent_hint = Some(parent);
    inode.mode = mode;
    lower.inodes.insert(ino, inode);
    lower.inodes.get_mut(&parent).unwrap().nlink += 1;
    lower.dentries.insert(
        (parent, name.to_vec()),
        EntityDentry::Present {
            ino,
            kind: EntityKind::Directory,
        },
    );
    lower.binding.max_ino = lower.binding.max_ino.max(ino);
}

#[tokio::test]
async fn lower_only_lookup_does_not_copy_up_and_siblings_isolate_recreation() {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"base", 2, 10);
    let lower = Arc::new(lower);
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let volume = Uuid::new_v4();
    let first = attach(
        backend.clone(),
        lower.clone(),
        EntityKeys::new(volume, Uuid::new_v4()),
    )
    .await;
    let second = attach(backend, lower, EntityKeys::new(volume, Uuid::new_v4())).await;
    assert_eq!(first.ns.lookup(1, b"base", &root()).await.unwrap().ino, 2);
    assert_eq!(
        first
            .backend
            .read(&[first.keys.inode(2)])
            .await
            .unwrap()
            .rows,
        vec![None]
    );
    first
        .ns
        .unlink(1, b"base".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(
        first.ns.lookup(1, b"base", &root()).await.unwrap_err(),
        EntityError::NotFound
    );
    let recreated = first
        .ns
        .create(request(1, b"base", EntityKind::File), root())
        .await
        .unwrap();
    assert!(recreated.ino > 2);
    assert_eq!((recreated.size, recreated.lower_visible_size), (0, 0));
    let original = second.ns.lookup(1, b"base", &root()).await.unwrap();
    assert_eq!((original.ino, original.size, original.nlink), (2, 10, 1));
    assert_eq!(current_inode(&first, 2).await.nlink, 0);
}

async fn exclusive_creates<B: EntityBackend + 'static>(
    ns: Arc<EntityNamespace<B, MemoryLower>>,
    create_open: bool,
) -> EntityInode {
    let barrier = Arc::new(Barrier::new(33));
    let tasks = (0..32)
        .map(|_| {
            let ns = ns.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                let request = request(1, b"exclusive", EntityKind::File);
                if create_open {
                    let (inode, handle) = ns.create_open(request, root(), 6).await?;
                    assert_eq!(ns.validate_handle(&handle, 6), Ok(inode.ino));
                    ns.close(handle).unwrap();
                    Ok(inode)
                } else {
                    ns.create(request, root()).await
                }
            })
        })
        .collect::<Vec<_>>();
    barrier.wait().await;
    // Join every writer before assertions/cleanup, including failed outcomes.
    let results = futures::future::join_all(tasks).await;
    let mut winners = Vec::new();
    let mut exists = 0;
    for result in results {
        match result.unwrap() {
            Ok(inode) => winners.push(inode),
            Err(EntityError::Exists) => exists += 1,
            other => panic!("unexpected O_EXCL outcome: {other:?}"),
        }
    }
    assert_eq!(winners.len(), 1);
    assert_eq!(exists, 31);
    assert_eq!(
        ns.lookup(1, b"exclusive", &root()).await.unwrap(),
        winners[0]
    );
    assert_eq!(winners[0].nlink, 1);
    winners.pop().unwrap()
}

#[tokio::test]
async fn concurrent_exclusive_creates_have_exactly_one_visible_winner() {
    exclusive_creates(fixture(MemoryLower::empty()).await.ns, false).await;
}

async fn mount_contract<B: EntityBackend + 'static>(backend: Arc<B>, volume: Uuid) {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"lower", 2, 8);
    let lower = Arc::new(lower);
    let keys = EntityKeys::new(volume, Uuid::new_v4());
    let ns = attach_with_ttl(backend, lower, keys, 60_000_000_000)
        .await
        .ns;
    let create = |name: &[u8], mode| CreateRequest {
        mode,
        ..request(1, name, EntityKind::File)
    };
    let root = root();
    let user = user(101);
    ns.chmod(1, 0o777, root.clone()).await.unwrap();
    let (inode, handle) = ns
        .create_open(create(b"private", 0), user.clone(), 6)
        .await
        .unwrap();
    assert_eq!(inode.mode, 0);
    assert_eq!(ns.validate_handle(&handle, 6), Ok(inode.ino));
    assert!(matches!(
        ns.open_inode(inode.ino, 4, &user).await,
        Err(EntityError::Permission)
    ));
    assert_eq!(ns.getattr(inode.ino, &user).await.unwrap().ino, inode.ino);
    assert!(matches!(
        ns.create_open(create(b"private", 0), user.clone(), 6).await,
        Err(EntityError::Exists)
    ));
    ns.unlink(1, b"lower".to_vec(), false, root.clone())
        .await
        .unwrap();
    let entries = ns.readdir(1, &user).await.unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|(name, _)| name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"private".as_slice()]
    );
    ns.unlink(1, b"private".to_vec(), false, user.clone())
        .await
        .unwrap();
    assert_eq!(ns.validate_handle(&handle, 6), Ok(inode.ino));
    assert_eq!(ns.getattr(inode.ino, &user).await.unwrap().nlink, 0);
    assert!(matches!(
        ns.open_inode(inode.ino, 4, &root).await,
        Err(EntityError::NotFound)
    ));
    ns.close(handle).unwrap();
    assert!(ns.readdir(1, &root).await.unwrap().is_empty());

    exclusive_creates(ns.clone(), true).await;
    assert_eq!(ns.readdir(1, &root).await.unwrap().len(), 1);
}

#[tokio::test]
async fn memory_entity_mount_namespace_operations() {
    mount_contract(Arc::new(MemoryEntityBackend::new(1)), Uuid::new_v4()).await;
}

#[tokio::test]
async fn rename_and_hardlinks_preserve_inode_identity_and_retire_replacements() {
    let fixture = fixture(MemoryLower::empty()).await;
    let original = fixture
        .ns
        .create(request(1, b"a", EntityKind::File), root())
        .await
        .unwrap();
    fixture
        .ns
        .link(original.ino, 1, b"b".to_vec(), root())
        .await
        .unwrap();
    assert_eq!(fixture.ns.lookup(1, b"a", &root()).await.unwrap().nlink, 2);
    fixture
        .ns
        .rename(1, b"a".to_vec(), 1, b"c".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(
        fixture.ns.lookup(1, b"a", &root()).await.unwrap_err(),
        EntityError::NotFound
    );
    assert_eq!(
        fixture.ns.lookup(1, b"b", &root()).await.unwrap().ino,
        original.ino
    );
    let replaced = fixture
        .ns
        .create(request(1, b"d", EntityKind::File), root())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .ns
            .rename(1, b"c".to_vec(), 1, b"d".to_vec(), true, root())
            .await
            .unwrap_err(),
        EntityError::Exists
    );
    fixture
        .ns
        .rename(1, b"c".to_vec(), 1, b"d".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(
        fixture.ns.lookup(1, b"d", &root()).await.unwrap().ino,
        original.ino
    );
    assert_eq!(current_inode(&fixture, replaced.ino).await.nlink, 0);
    assert!(
        fixture
            .backend
            .read(&[fixture.keys.orphan(replaced.ino)])
            .await
            .unwrap()
            .rows[0]
            .is_some()
    );
    fixture
        .ns
        .unlink(1, b"b".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(fixture.ns.lookup(1, b"d", &root()).await.unwrap().nlink, 1);
    fixture
        .ns
        .unlink(1, b"d".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(current_inode(&fixture, original.ino).await.nlink, 0);
    assert!(
        fixture
            .backend
            .read(&[fixture.keys.orphan(original.ino)])
            .await
            .unwrap()
            .rows[0]
            .is_some()
    );
}

#[tokio::test]
async fn directory_cycles_nonempty_removal_and_replacement_keep_parent_counts() {
    let fixture = fixture(MemoryLower::empty()).await;
    let a = fixture
        .ns
        .create(request(1, b"a", EntityKind::Directory), root())
        .await
        .unwrap();
    let child = fixture
        .ns
        .create(request(a.ino, b"child", EntityKind::Directory), root())
        .await
        .unwrap();
    let b = fixture
        .ns
        .create(request(1, b"b", EntityKind::Directory), root())
        .await
        .unwrap();
    assert_eq!(current_inode(&fixture, 1).await.nlink, 4);
    assert!(matches!(
        fixture
            .ns
            .rename(
                1,
                b"a".to_vec(),
                child.ino,
                b"cycle".to_vec(),
                false,
                root()
            )
            .await,
        Err(EntityError::Invalid(_))
    ));
    assert_eq!(
        fixture
            .ns
            .unlink(1, b"a".to_vec(), true, root())
            .await
            .unwrap_err(),
        EntityError::NotEmpty
    );
    fixture
        .ns
        .rename(1, b"a".to_vec(), 1, b"b".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(
        fixture.ns.lookup(1, b"b", &root()).await.unwrap().ino,
        a.ino
    );
    assert_eq!(current_inode(&fixture, 1).await.nlink, 3);
    assert_eq!(current_inode(&fixture, b.ino).await.nlink, 0);
    assert_eq!(
        current_inode(&fixture, child.ino).await.parent_hint,
        Some(a.ino)
    );
    fixture
        .ns
        .unlink(a.ino, b"child".to_vec(), true, root())
        .await
        .unwrap();
    fixture
        .ns
        .unlink(1, b"b".to_vec(), true, root())
        .await
        .unwrap();
    assert_eq!(current_inode(&fixture, 1).await.nlink, 2);
}

#[tokio::test]
async fn sticky_directory_and_ancestor_search_permissions_are_enforced() {
    let mut lower = MemoryLower::empty();
    lower.inodes.get_mut(&1).unwrap().mode = 0o1777;
    lower.add_file(1, b"owned", 2, 0);
    lower.inodes.get_mut(&2).unwrap().uid = 100;
    add_directory(&mut lower, 1, b"ancestor", 3, 0o777);
    add_directory(&mut lower, 3, b"parent", 4, 0o777);
    lower.add_file(4, b"readable", 5, 0);
    let fixture = fixture(lower).await;
    assert_eq!(
        fixture
            .ns
            .unlink(1, b"owned".to_vec(), false, user(200))
            .await
            .unwrap_err(),
        EntityError::Permission
    );
    assert_eq!(
        fixture
            .ns
            .rename(1, b"owned".to_vec(), 1, b"moved".to_vec(), false, user(200))
            .await
            .unwrap_err(),
        EntityError::Permission
    );
    let handle = fixture
        .ns
        .open(4, b"readable", 4, &user(200))
        .await
        .unwrap();
    fixture.ns.close(handle).unwrap();
    fixture.ns.chmod(3, 0o700, root()).await.unwrap();
    assert!(matches!(
        fixture.ns.open(4, b"readable", 4, &user(200)).await,
        Err(EntityError::Permission)
    ));
    fixture
        .ns
        .unlink(1, b"owned".to_vec(), false, user(100))
        .await
        .unwrap();
}

#[tokio::test]
async fn absent_name_open_checks_parent_search_permission_before_name_existence() {
    let mut lower = MemoryLower::empty();
    lower.inodes.get_mut(&1).unwrap().mode = 0o700;
    lower.add_file(1, b"existing", 2, 0);
    let fixture = fixture(lower).await;
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    for name in [b"absent".as_slice(), b"existing"] {
        assert!(matches!(
            fixture.ns.open(1, name, 4, &user(2000)).await,
            Err(EntityError::Permission)
        ));
        assert_eq!(
            fixture.ns.lookup(1, name, &user(2000)).await.unwrap_err(),
            EntityError::Permission
        );
    }
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
}

fn named_acl(uid: u32) -> Vec<u8> {
    let mut bytes = 2_u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (1_u16, 7_u16, u32::MAX),
        (2, 6, uid),
        (4, 0, u32::MAX),
        (16, 6, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&permissions.to_le_bytes());
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    PosixAcl::decode(&bytes).unwrap();
    bytes
}

#[tokio::test]
async fn default_acl_setgid_inheritance_and_chmod_mask_remain_consistent() {
    let mut lower = MemoryLower::empty();
    let parent = lower.inodes.get_mut(&1).unwrap();
    parent.mode = 0o2777;
    parent.gid = 77;
    lower
        .xattrs
        .insert((1, DEFAULT_XATTR.to_vec()), named_acl(1000));
    let fixture = fixture(lower).await;
    let mut owner = user(2000);
    owner.umask = 0o077;
    let file = fixture
        .ns
        .create(request(1, b"acl", EntityKind::File), owner.clone())
        .await
        .unwrap();
    assert_eq!((file.uid, file.gid, file.mode), (2000, 77, 0o660));
    let handle = fixture.ns.open(1, b"acl", 6, &user(1000)).await.unwrap();
    fixture.ns.close(handle).unwrap();
    assert!(matches!(
        fixture.ns.open(1, b"acl", 4, &user(3000)).await,
        Err(EntityError::Permission)
    ));
    fixture.ns.chmod(file.ino, 0o640, owner).await.unwrap();
    assert!(matches!(
        fixture.ns.open(1, b"acl", 2, &user(1000)).await,
        Err(EntityError::Permission)
    ));
    let handle = fixture.ns.open(1, b"acl", 4, &user(1000)).await.unwrap();
    fixture.ns.close(handle).unwrap();
    let snapshot = fixture
        .backend
        .read(&[fixture.keys.xattr(file.ino, ACCESS_XATTR)])
        .await
        .unwrap();
    let EntityValue::Present(bytes) = codec::decode(snapshot.rows[0].as_deref().unwrap()).unwrap()
    else {
        panic!("missing access ACL")
    };
    assert_eq!(
        PosixAcl::decode(&bytes).unwrap().mode_bits(),
        current_inode(&fixture, file.ino).await.mode & 0o777
    );
}

#[tokio::test]
async fn user_xattrs_follow_write_acl_and_group_permissions_including_removal() {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"named", 2, 0);
    lower.add_file(1, b"group", 3, 0);
    lower.add_file(1, b"readonly", 4, 0);
    for ino in [2, 3, 4] {
        lower.inodes.get_mut(&ino).unwrap().uid = 1000;
    }
    let acl = named_acl(2000);
    lower.inodes.get_mut(&2).unwrap().mode = PosixAcl::decode(&acl).unwrap().mode_bits();
    lower.xattrs.insert((2, ACCESS_XATTR.to_vec()), acl);
    let group_inode = lower.inodes.get_mut(&3).unwrap();
    group_inode.gid = 77;
    group_inode.mode = 0o620;
    lower.inodes.get_mut(&4).unwrap().mode = 0o444;
    lower
        .xattrs
        .insert((4, b"user.value".to_vec()), b"existing".to_vec());
    let fixture = fixture(lower).await;
    let mut member = user(3000);
    member.groups.push(77);
    for (ino, credentials) in [(2, user(2000)), (3, member)] {
        fixture
            .ns
            .set_xattr(
                ino,
                b"user.value".to_vec(),
                Some(b"written".to_vec()),
                credentials.clone(),
            )
            .await
            .unwrap();
        let row = fixture
            .backend
            .read(&[fixture.keys.xattr(ino, b"user.value")])
            .await
            .unwrap();
        assert_eq!(
            codec::decode::<EntityValue>(row.rows[0].as_deref().unwrap()).unwrap(),
            EntityValue::Present(b"written".to_vec())
        );
        fixture
            .ns
            .set_xattr(ino, b"user.value".to_vec(), None, credentials)
            .await
            .unwrap();
        let row = fixture
            .backend
            .read(&[fixture.keys.xattr(ino, b"user.value")])
            .await
            .unwrap();
        assert_eq!(
            codec::decode::<EntityValue>(row.rows[0].as_deref().unwrap()).unwrap(),
            EntityValue::Whiteout
        );
    }
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    for value in [Some(b"denied".to_vec()), None] {
        assert_eq!(
            fixture
                .ns
                .set_xattr(4, b"user.value".to_vec(), value, user(1000))
                .await
                .unwrap_err(),
            EntityError::Permission,
            "owning a read-only inode does not grant user-xattr write access"
        );
    }
    assert_eq!(
        fixture
            .ns
            .set_xattr(2, ACCESS_XATTR.to_vec(), Some(named_acl(2000)), user(2000))
            .await
            .unwrap_err(),
        EntityError::Permission,
        "write ACL access does not grant permission to change that ACL"
    );
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn sticky_directory_user_xattrs_require_directory_owner_or_root() {
    let mut lower = MemoryLower::empty();
    add_directory(&mut lower, 1, b"sticky", 2, 0o1777);
    add_directory(&mut lower, 1, b"ordinary", 3, 0o777);
    for ino in [2, 3] {
        lower.inodes.get_mut(&ino).unwrap().uid = 1000;
    }
    lower
        .xattrs
        .insert((2, b"user.value".to_vec()), b"existing".to_vec());
    let fixture = fixture(lower).await;
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    for value in [Some(b"denied".to_vec()), None] {
        assert_eq!(
            fixture
                .ns
                .set_xattr(2, b"user.value".to_vec(), value, user(2000))
                .await
                .unwrap_err(),
            EntityError::Permission
        );
    }
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
    for (ino, credentials) in [(2, user(1000)), (2, root()), (3, user(2000))] {
        fixture
            .ns
            .set_xattr(
                ino,
                b"user.value".to_vec(),
                Some(b"permitted".to_vec()),
                credentials,
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn symlink_user_xattrs_and_acls_fail_closed_without_mutation() {
    let fixture = fixture(MemoryLower::empty()).await;
    let mut symlink = request(1, b"link", EntityKind::Symlink);
    symlink.symlink_target = Some(b"destination".to_vec());
    let inode = fixture.ns.create(symlink, root()).await.unwrap();
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    for (name, value) in [
        (b"user.value".as_slice(), b"value".to_vec()),
        (ACCESS_XATTR, named_acl(2000)),
        (DEFAULT_XATTR, named_acl(2000)),
    ] {
        for value in [Some(value), None] {
            assert!(matches!(
                fixture
                    .ns
                    .set_xattr(inode.ino, name.to_vec(), value, root())
                    .await,
                Err(EntityError::Unsupported(_))
            ));
        }
    }
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn device_creation_requires_root_and_retains_kind_and_rdev() {
    let mut lower = MemoryLower::empty();
    lower.inodes.get_mut(&1).unwrap().mode = 0o777;
    let fixture = fixture(lower).await;
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    for kind in [EntityKind::CharDevice, EntityKind::BlockDevice] {
        let mut device = request(1, b"device", kind);
        device.rdev = 0x1234;
        assert_eq!(
            fixture.ns.create(device, user(2000)).await.unwrap_err(),
            EntityError::Permission
        );
    }
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
    for (name, kind) in [
        (b"char".as_slice(), EntityKind::CharDevice),
        (b"block".as_slice(), EntityKind::BlockDevice),
    ] {
        let mut device = request(1, name, kind);
        device.rdev = 0x1234;
        let created = fixture.ns.create(device, root()).await.unwrap();
        let looked_up = fixture.ns.lookup(1, name, &root()).await.unwrap();
        assert_eq!(looked_up, created);
        assert_eq!((looked_up.kind, looked_up.rdev), (kind, 0x1234));
    }
}

#[tokio::test]
async fn xattr_whiteout_does_not_reveal_lower_and_open_close_is_noatime() {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"file", 2, 10);
    lower
        .xattrs
        .insert((2, b"user.value".to_vec()), b"lower".to_vec());
    let fixture = fixture(lower).await;
    fixture
        .ns
        .set_xattr(2, b"user.value".to_vec(), None, root())
        .await
        .unwrap();
    let snapshot = fixture
        .backend
        .read(&[fixture.keys.xattr(2, b"user.value")])
        .await
        .unwrap();
    let upper: EntityValue = codec::decode(snapshot.rows[0].as_deref().unwrap()).unwrap();
    assert_eq!(upper, EntityValue::Whiteout);
    assert_eq!(
        resolver::resolve_xattr(
            fixture.lower.as_ref(),
            fixture.lower.binding(),
            2,
            b"user.value",
            Some(&upper)
        )
        .await
        .unwrap(),
        None
    );
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    let inode_before = current_inode(&fixture, 2).await;
    let handle = fixture.ns.open(1, b"file", 6, &root()).await.unwrap();
    assert_eq!(handle.ino(), 2);
    fixture.ns.close(handle).unwrap();
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
    assert_eq!(current_inode(&fixture, 2).await, inode_before);
}

#[tokio::test]
async fn expired_lease_rejects_namespace_mutations_without_writes() {
    let fixture = fixture(MemoryLower::empty()).await;
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    fixture.backend.set_time(1_001_000).unwrap();
    assert_eq!(
        fixture
            .ns
            .create(request(1, b"fenced", EntityKind::File), root())
            .await
            .unwrap_err(),
        EntityError::Fenced
    );
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
}

async fn inject_dentry(fixture: &Fixture<MemoryEntityBackend>, name: &[u8], row: EntityDentry) {
    // A deliberate storage fault, outside namespace APIs. Keep the descriptor
    // fixed so the tested failure is the corrupt row, not lost writer authority.
    let keys = [fixture.keys.descriptor(), fixture.keys.dentry(1, name)];
    let snapshot = fixture.backend.read(&keys).await.unwrap();
    let authority = TransactionAuthority::from_validated(
        vec![RowCheck {
            key: keys[0].clone(),
            expected: snapshot.rows[0].clone(),
        }],
        None,
    )
    .unwrap();
    let plan = EntityPlan {
        checks: vec![RowCheck {
            key: keys[1].clone(),
            expected: snapshot.rows[1].clone(),
        }],
        writes: vec![RowWrite::Put {
            key: keys[1].clone(),
            value: codec::encode(&row).unwrap(),
        }],
    };
    assert_eq!(
        fixture.backend.commit(&authority, &plan).await.unwrap(),
        CommitOutcome::Committed
    );
}

#[tokio::test]
async fn corrupt_dentry_kind_or_missing_inode_fails_without_namespace_writes() {
    for corrupt in [
        EntityDentry::Present {
            ino: 2,
            kind: EntityKind::Directory,
        },
        EntityDentry::Present {
            ino: 999,
            kind: EntityKind::File,
        },
    ] {
        let mut lower = MemoryLower::empty();
        lower.add_file(1, b"bad", 2, 10);
        let fixture = fixture(lower).await;
        inject_dentry(&fixture, b"bad", corrupt).await;
        let before = fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap();
        assert!(matches!(
            fixture.ns.lookup(1, b"bad", &root()).await,
            Err(EntityError::Corrupt(_))
        ));
        assert!(matches!(
            fixture
                .ns
                .create(request(1, b"bad", EntityKind::File), root())
                .await,
            Err(EntityError::Corrupt(_))
        ));
        assert!(matches!(
            fixture.ns.unlink(1, b"bad".to_vec(), false, root()).await,
            Err(EntityError::Corrupt(_))
        ));
        assert!(matches!(
            fixture
                .ns
                .rename(1, b"bad".to_vec(), 1, b"moved".to_vec(), false, root())
                .await,
            Err(EntityError::Corrupt(_))
        ));
        assert_eq!(
            fixture
                .backend
                .scan_prefix(fixture.keys.volume_prefix(), 4096)
                .await
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn namespace_with_wrong_lower_binding_cannot_read_or_mutate() {
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let original = MemoryLower::empty();
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(original.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let before = backend
        .scan_prefix(keys.volume_prefix(), 4096)
        .await
        .unwrap();
    for mismatch in 0..3 {
        let mut wrong = MemoryLower::empty();
        match mismatch {
            0 => wrong.binding.manifest_digest = [9; 32],
            1 => wrong.binding.manifest_key = b"other-authenticated-manifest".to_vec(),
            _ => wrong.binding.max_ino += 1,
        }
        let namespace = EntityNamespace::new(
            backend.clone(),
            Arc::new(wrong),
            keys.clone(),
            session.clone(),
        );
        assert_eq!(
            namespace.lookup(1, b"absent", &root()).await.unwrap_err(),
            EntityError::Fenced
        );
        assert_eq!(
            namespace
                .create(request(1, b"forbidden", EntityKind::File), root())
                .await
                .unwrap_err(),
            EntityError::Fenced
        );
        assert!(matches!(
            namespace.open(1, b"absent", 4, &root()).await,
            Err(EntityError::Fenced)
        ));
        assert_eq!(
            backend
                .scan_prefix(keys.volume_prefix(), 4096)
                .await
                .unwrap(),
            before
        );
    }
}

struct PausingBackend {
    inner: MemoryEntityBackend,
    pause_key: Mutex<Option<Vec<u8>>>,
    pause_read_key: Mutex<Option<Vec<u8>>>,
    entered: Notify,
    resume: Notify,
    lose_reply: AtomicBool,
    mutation_commits: AtomicUsize,
    reads: AtomicUsize,
    read_batches: Mutex<Vec<Vec<Vec<u8>>>>,
    commits: AtomicUsize,
    create_commits: AtomicUsize,
}

impl PausingBackend {
    fn new() -> Self {
        Self {
            inner: MemoryEntityBackend::new(1000),
            pause_key: Mutex::new(None),
            pause_read_key: Mutex::new(None),
            entered: Notify::new(),
            resume: Notify::new(),
            lose_reply: AtomicBool::new(false),
            mutation_commits: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            read_batches: Mutex::new(Vec::new()),
            commits: AtomicUsize::new(0),
            create_commits: AtomicUsize::new(0),
        }
    }
    fn pause(&self, key: Vec<u8>) {
        *self.pause_key.lock().unwrap() = Some(key);
    }
    fn pause_after_read(&self, key: Vec<u8>) {
        *self.pause_read_key.lock().unwrap() = Some(key);
    }
    async fn after_read(&self, keys: &[Vec<u8>]) {
        let pause = {
            let mut target = self.pause_read_key.lock().unwrap();
            if target.as_ref().is_some_and(|key| keys.contains(key)) {
                target.take();
                true
            } else {
                false
            }
        };
        if pause {
            self.entered.notify_one();
            self.resume.notified().await;
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
    fn resume(&self) {
        self.resume.notify_one();
    }
    fn reset_counts(&self) {
        self.read_batches.lock().unwrap().clear();
        self.reads.store(0, Ordering::SeqCst);
        self.commits.store(0, Ordering::SeqCst);
        self.create_commits.store(0, Ordering::SeqCst);
    }
    fn assert_requests(&self, reads: usize, commits: usize) {
        assert_eq!(self.reads.load(Ordering::SeqCst), reads);
        assert_eq!(self.commits.load(Ordering::SeqCst), commits);
        assert_eq!(self.create_commits.load(Ordering::SeqCst), 0);
    }
    async fn before_commit(&self, plan: &EntityPlan, generated_dentry: Option<&[u8]>) {
        let pause = {
            let mut target = self.pause_key.lock().unwrap();
            if target.as_ref().is_some_and(|key| {
                generated_dentry == Some(key.as_slice())
                    || plan
                        .writes
                        .iter()
                        .any(|write| write.key() == key.as_slice())
            }) {
                target.take();
                true
            } else {
                false
            }
        };
        if pause {
            self.entered.notify_one();
            self.resume.notified().await;
        }
    }
    fn mutation_committed(&self) -> Result<()> {
        self.mutation_commits.fetch_add(1, Ordering::SeqCst);
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(EntityError::Backend(
                "injected committed response loss".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl EntityBackend for PausingBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.read_batches.lock().unwrap().push(keys.to_vec());
        let snapshot = self.inner.read(keys).await?;
        self.after_read(keys).await;
        Ok(snapshot)
    }
    async fn read_checked(
        &self,
        keys: &[Vec<u8>],
        checks: &[RowCheck],
    ) -> Result<(RowSnapshot, bool)> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.read_batches.lock().unwrap().push(keys.to_vec());
        let result = self.inner.read_checked(keys, checks).await?;
        let mut observed = keys.to_vec();
        observed.extend(checks.iter().map(|check| check.key.clone()));
        self.after_read(&observed).await;
        Ok(result)
    }
    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.before_commit(plan, None).await;
        let outcome = self.inner.commit(authority, plan).await?;
        if outcome == CommitOutcome::Committed && !plan.writes.is_empty() {
            self.mutation_committed()?;
        }
        Ok(outcome)
    }
    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        self.create_commits.fetch_add(1, Ordering::SeqCst);
        let dentry = create
            .keys
            .dentry(create.inode.parent_hint.unwrap(), &create.name);
        self.before_commit(plan, Some(&dentry)).await;
        let inode = self
            .inner
            .commit_with_inode_allocation(authority, plan, create)
            .await?;
        if inode.is_some() {
            self.mutation_committed()?;
        }
        Ok(inode)
    }
    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let keys = self.inner.scan_keys_page(prefix, after, limit).await?;
        self.after_read(&[prefix.to_vec()]).await;
        Ok(keys)
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.inner.scan_prefix(prefix, limit).await?;
        self.after_read(&[prefix.to_vec()]).await;
        Ok(rows)
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.inner.scan_prefix_batch(prefix, limit).await?;
        self.after_read(&[prefix.to_vec()]).await;
        Ok(rows)
    }
}

#[tokio::test]
async fn cold_named_open_does_not_refetch_initial_metadata_rows() {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"file", 2, 10);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;

    backend.reset_counts();
    let handle = fixture.ns.open(1, b"file", 4, &root()).await.unwrap();
    assert_eq!(handle.ino(), 2);
    fixture.ns.close(handle).unwrap();

    let batches = backend.read_batches.lock().unwrap();
    for (index, batch) in batches.iter().enumerate() {
        eprintln!(
            "read {}: {:?}",
            index + 1,
            batch
                .iter()
                .map(|key| {
                    String::from_utf8_lossy(key.strip_prefix(fixture.keys.prefix()).unwrap_or(key))
                })
                .collect::<Vec<_>>()
        );
    }
    let parent_reads = batches
        .iter()
        .filter(|batch| batch.contains(&fixture.keys.inode(1)))
        .count();
    assert_eq!(
        parent_reads, 1,
        "cold open must retain its initial parent row"
    );
}

#[tokio::test]
async fn known_parent_create_and_create_open_use_one_read_and_one_atomic_publication() {
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(MemoryLower::empty()),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    let parent = fixture
        .ns
        .create(request(1, b"parent", EntityKind::Directory), root())
        .await
        .unwrap();
    for create_open in [false, true] {
        backend.reset_counts();
        let name = if create_open {
            b"opened".as_slice()
        } else {
            b"created".as_slice()
        };
        let inode = if create_open {
            let (inode, handle) = fixture
                .ns
                .create_open(request(parent.ino, name, EntityKind::File), root(), 2)
                .await
                .unwrap();
            assert_eq!(fixture.ns.validate_handle(&handle, 2).unwrap(), inode.ino);
            inode
        } else {
            fixture
                .ns
                .create(request(parent.ino, name, EntityKind::File), root())
                .await
                .unwrap()
        };
        assert_eq!(backend.reads.load(Ordering::SeqCst), 1);
        assert_eq!(backend.create_commits.load(Ordering::SeqCst), 1);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
        assert!(inode.ino > fixture.lower.binding.max_ino);
        assert_eq!(
            fixture
                .ns
                .lookup(parent.ino, name, &root())
                .await
                .unwrap()
                .ino,
            inode.ino
        );
    }
}

#[tokio::test]
async fn known_inode_and_warm_path_operations_batch_authority_and_observed_rows() {
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(MemoryLower::empty()),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    let inode = fixture
        .ns
        .create(request(1, b"file", EntityKind::File), root())
        .await
        .unwrap();

    backend.reset_counts();
    assert_eq!(fixture.ns.getattr(inode.ino, &root()).await.unwrap(), inode);
    backend.assert_requests(1, 0);

    backend.reset_counts();
    fixture.ns.access(inode.ino, 6, &root()).await.unwrap();
    backend.assert_requests(1, 0);

    backend.reset_counts();
    let handle = fixture.ns.open_inode(inode.ino, 6, &root()).await.unwrap();
    backend.assert_requests(1, 0);
    fixture.ns.close(handle).unwrap();

    backend.reset_counts();
    let (attr, handle) = fixture
        .ns
        .open_with_attr(1, b"file", 6, &root())
        .await
        .unwrap();
    assert_eq!(attr, inode);
    assert_eq!(handle.ino(), inode.ino);
    backend.assert_requests(2, 0);
    fixture.ns.close(handle).unwrap();

    backend.reset_counts();
    assert_eq!(fixture.ns.lookup(1, b"file", &root()).await.unwrap(), inode);
    backend.assert_requests(1, 0);

    backend.reset_counts();
    fixture.ns.chmod(inode.ino, 0o600, root()).await.unwrap();
    backend.assert_requests(1, 1);

    backend.reset_counts();
    fixture
        .ns
        .set_xattr(
            inode.ino,
            b"user.note".to_vec(),
            Some(b"value".to_vec()),
            root(),
        )
        .await
        .unwrap();
    backend.assert_requests(1, 1);

    backend.reset_counts();
    fixture
        .ns
        .link(inode.ino, 1, b"linked".to_vec(), root())
        .await
        .unwrap();
    backend.assert_requests(1, 1);
}

#[tokio::test]
async fn operation_snapshot_preserves_old_observations_and_conflicts_after_mutation() {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"file", 2, 10);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    let key = fixture.keys.inode(2);
    let (authority, mut prepared) = fixture.ns.snapshot(vec![key.clone()]).await.unwrap();
    let original = prepared.inode(2).await.unwrap();
    assert_eq!(prepared.observed(&key), Some(&None));
    fixture.ns.chmod(2, 0o600, root()).await.unwrap();
    backend.reset_counts();
    prepared
        .prefetch(vec![key.clone(), key.clone()])
        .await
        .unwrap();
    assert_eq!(prepared.inode(2).await.unwrap(), original);
    assert_eq!(prepared.raw(key.clone()).await.unwrap(), None);
    backend.assert_requests(0, 0);
    let current = fixture
        .backend
        .inner
        .read(std::slice::from_ref(&key))
        .await
        .unwrap()
        .rows
        .remove(0);
    assert_eq!(
        prepared.add(RowCheck {
            key: key.clone(),
            expected: current
        }),
        Err(EntityError::Conflict)
    );
    assert_eq!(prepared.observed(&key), Some(&None));
    assert_eq!(
        prepared.validate_read(true).await,
        Err(EntityError::Conflict)
    );
    assert_eq!(prepared.observed(&key), Some(&None));
    assert_eq!(
        fixture
            .backend
            .commit(&authority, &prepared.read_plan())
            .await
            .unwrap(),
        CommitOutcome::Conflict
    );
    assert_eq!(fixture.ns.getattr(2, &root()).await.unwrap().mode, 0o600);
}

#[tokio::test]
async fn dependent_prefetch_covers_matching_rows_but_preserves_changed_observations() {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"file", 2, 10);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    let key = fixture.keys.inode(2);
    for changed in [false, true] {
        let (_, mut prepared) = fixture.ns.snapshot(vec![key.clone()]).await.unwrap();
        if changed {
            fixture.ns.chmod(2, 0o600, root()).await.unwrap();
        }
        backend.reset_counts();
        prepared
            .prefetch(vec![fixture.keys.xattr(2, ACCESS_XATTR)])
            .await
            .unwrap();
        assert_eq!(prepared.observed(&key), Some(&None));
        if changed {
            assert_eq!(
                prepared.validate_read(false).await,
                Err(EntityError::Conflict)
            );
            backend.assert_requests(2, 0);
        } else {
            prepared.validate_read(false).await.unwrap();
            backend.assert_requests(1, 0);
        }
    }
}

#[tokio::test]
async fn appended_absence_predicate_requires_fresh_validation() {
    let fixture = fixture(MemoryLower::empty()).await;
    let (_, mut prepared) = fixture
        .ns
        .snapshot(vec![fixture.keys.inode(1)])
        .await
        .unwrap();
    let key = fixture.keys.dentry(1, b"created");
    prepared
        .add(RowCheck {
            key: key.clone(),
            expected: None,
        })
        .unwrap();
    fixture
        .ns
        .create(request(1, b"created", EntityKind::File), root())
        .await
        .unwrap();
    assert_eq!(
        prepared.validate_read(false).await,
        Err(EntityError::Conflict)
    );
    assert_eq!(prepared.observed(&key), Some(&None));
}

#[tokio::test]
async fn forced_read_validation_accepts_renewal_but_checks_backend_expiry() {
    let fixture = fixture(MemoryLower::empty()).await;
    let (_, mut prepared) = fixture
        .ns
        .snapshot(vec![fixture.keys.inode(1)])
        .await
        .unwrap();
    let lifecycle = Lifecycle::new(fixture.backend.clone(), fixture.keys.clone());
    fixture.backend.set_time(1100).unwrap();
    lifecycle.renew(&fixture.ns.session, 2000).await.unwrap();
    prepared.validate_read(true).await.unwrap();
    assert_eq!(prepared.now_ns(), 1100);
    fixture.backend.set_time(3100).unwrap();
    assert_eq!(prepared.validate_read(true).await, Err(EntityError::Fenced));
}

#[tokio::test]
async fn empty_directory_scan_rechecks_ancestor_permissions() {
    let mut lower = MemoryLower::empty();
    add_directory(&mut lower, 1, b"parent", 2, 0o777);
    add_directory(&mut lower, 2, b"empty", 3, 0o777);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    backend.pause_after_read(fixture.keys.dentry_prefix(3));
    let ns = fixture.ns.clone();
    let pending = tokio::spawn(async move { ns.readdir(3, &user(2000)).await });
    backend.entered().await;
    fixture.ns.chmod(2, 0o700, root()).await.unwrap();
    backend.resume();
    assert_eq!(pending.await.unwrap(), Err(EntityError::Permission));
}

#[tokio::test]
async fn batched_path_open_rechecks_parent_permissions_after_snapshot() {
    let mut lower = MemoryLower::empty();
    add_directory(&mut lower, 1, b"parent", 2, 0o777);
    lower.add_file(2, b"file", 3, 10);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    backend.pause_after_read(fixture.keys.inode(2));
    let ns = fixture.ns.clone();
    let pending = tokio::spawn(async move { ns.open(2, b"file", 4, &user(2000)).await });
    backend.entered().await;
    fixture.ns.chmod(2, 0o700, root()).await.unwrap();
    backend.resume();
    assert!(matches!(
        pending.await.unwrap(),
        Err(EntityError::Permission)
    ));
}

#[tokio::test]
async fn stale_dentry_hint_fetches_actual_target_and_its_current_permissions() {
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(MemoryLower::empty()),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    let first = fixture
        .ns
        .create(request(1, b"file", EntityKind::File), root())
        .await
        .unwrap();
    let replacement = fixture
        .ns
        .create(request(1, b"replacement", EntityKind::File), root())
        .await
        .unwrap();
    fixture
        .ns
        .chmod(replacement.ino, 0o600, root())
        .await
        .unwrap();
    let other = EntityNamespace::new(
        backend.clone(),
        fixture.lower.clone(),
        fixture.keys.clone(),
        fixture.ns.session.clone(),
    );
    other
        .rename(
            1,
            b"replacement".to_vec(),
            1,
            b"file".to_vec(),
            false,
            root(),
        )
        .await
        .unwrap();

    backend.reset_counts();
    assert!(matches!(
        fixture.ns.open(1, b"file", 4, &user(2000)).await,
        Err(EntityError::Permission)
    ));
    backend.assert_requests(2, 0);
    assert_eq!(
        fixture.ns.getattr(first.ino, &root()).await.unwrap().nlink,
        0
    );
    other.chmod(replacement.ino, 0o644, root()).await.unwrap();

    backend.reset_counts();
    let (inode, handle) = fixture
        .ns
        .open_with_attr(1, b"file", 4, &user(2000))
        .await
        .unwrap();
    assert_eq!(inode.ino, replacement.ino);
    assert_eq!(handle.ino(), replacement.ino);
    backend.assert_requests(2, 0);
    fixture.ns.close(handle).unwrap();
}

#[tokio::test]
async fn stale_create_topology_hint_checks_the_newly_denied_ancestor_after_rename() {
    let mut lower = MemoryLower::empty();
    add_directory(&mut lower, 1, b"allowed", 2, 0o777);
    add_directory(&mut lower, 2, b"parent", 3, 0o777);
    add_directory(&mut lower, 1, b"destination", 4, 0o777);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    fixture
        .ns
        .create(request(3, b"warm", EntityKind::File), user(2000))
        .await
        .unwrap();
    // A second wrapper shares authoritative rows/session locks but has its own
    // advisory topology cache. The original wrapper still hints parent 3 -> 2.
    let other = EntityNamespace::new(
        backend.clone(),
        fixture.lower.clone(),
        fixture.keys.clone(),
        fixture.ns.session.clone(),
    );
    other
        .rename(2, b"parent".to_vec(), 4, b"moved".to_vec(), false, root())
        .await
        .unwrap();
    other.chmod(4, 0o700, root()).await.unwrap();
    backend.reset_counts();
    assert_eq!(
        fixture
            .ns
            .create(request(3, b"denied", EntityKind::File), user(2000))
            .await
            .unwrap_err(),
        EntityError::Permission
    );
    assert!(backend.reads.load(Ordering::SeqCst) > 1);
    assert_eq!(backend.create_commits.load(Ordering::SeqCst), 0);
    assert_eq!(
        backend
            .inner
            .read(&[fixture.keys.dentry(3, b"denied")])
            .await
            .unwrap()
            .rows,
        vec![None]
    );
    other.chmod(4, 0o777, root()).await.unwrap();
    fixture
        .ns
        .create(request(3, b"allowed-again", EntityKind::File), user(2000))
        .await
        .unwrap();
}

#[tokio::test]
async fn unrelated_parent_create_completes_while_another_commit_is_paused() {
    let mut lower = MemoryLower::empty();
    add_directory(&mut lower, 1, b"a", 2, 0o777);
    add_directory(&mut lower, 1, b"b", 3, 0o777);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    backend.pause(fixture.keys.dentry(2, b"paused"));
    let ns = fixture.ns.clone();
    let paused = tokio::spawn(async move {
        ns.create(request(2, b"paused", EntityKind::File), root())
            .await
    });
    backend.entered().await;
    let independent = tokio::time::timeout(
        Duration::from_secs(5),
        fixture
            .ns
            .create(request(3, b"independent", EntityKind::File), root()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        fixture
            .ns
            .lookup(3, b"independent", &root())
            .await
            .unwrap()
            .ino,
        independent.ino
    );
    backend.resume();
    let completed = paused.await.unwrap().unwrap();
    assert_eq!(
        fixture.ns.lookup(2, b"paused", &root()).await.unwrap().ino,
        completed.ino
    );
}

#[tokio::test]
async fn ancestor_chmod_invalidates_a_prepared_create_before_visibility() {
    let mut lower = MemoryLower::empty();
    add_directory(&mut lower, 1, b"ancestor", 2, 0o777);
    add_directory(&mut lower, 2, b"parent", 3, 0o777);
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(lower),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    backend.pause(fixture.keys.dentry(3, b"must-not-exist"));
    let ns = fixture.ns.clone();
    let pending = tokio::spawn(async move {
        ns.create(request(3, b"must-not-exist", EntityKind::File), user(2000))
            .await
    });
    backend.entered().await;
    tokio::time::timeout(Duration::from_secs(5), fixture.ns.chmod(2, 0o700, root()))
        .await
        .unwrap()
        .unwrap();
    backend.resume();
    assert_eq!(pending.await.unwrap().unwrap_err(), EntityError::Permission);
    assert_eq!(
        fixture
            .ns
            .lookup(3, b"must-not-exist", &root())
            .await
            .unwrap_err(),
        EntityError::NotFound
    );
    assert_eq!(
        fixture
            .backend
            .read(&[fixture.keys.dentry(3, b"must-not-exist")])
            .await
            .unwrap()
            .rows,
        vec![None]
    );
}

#[tokio::test]
async fn uncertain_create_reply_does_not_replay_the_committed_mutation() {
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(MemoryLower::empty()),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    let before = backend.mutation_commits.load(Ordering::SeqCst);
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture
            .ns
            .create(request(1, b"uncertain", EntityKind::File), root())
            .await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(backend.mutation_commits.load(Ordering::SeqCst), before + 1);
    let inode = fixture.ns.lookup(1, b"uncertain", &root()).await.unwrap();
    assert_eq!(inode.nlink, 1);
    assert!(inode.ino > fixture.lower.binding.max_ino);
    assert_eq!(
        fixture
            .ns
            .create(request(1, b"uncertain", EntityKind::File), root())
            .await
            .unwrap_err(),
        EntityError::Exists
    );
    assert_eq!(backend.mutation_commits.load(Ordering::SeqCst), before + 1);
}

#[tokio::test]
async fn cancellation_retains_operation_locks_until_submitted_create_finishes() {
    let backend = Arc::new(PausingBackend::new());
    let fixture = attach(
        backend.clone(),
        Arc::new(MemoryLower::empty()),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    backend.pause(fixture.keys.dentry(1, b"cancelled"));
    let ns = fixture.ns.clone();
    let caller = tokio::spawn(async move {
        ns.create(request(1, b"cancelled", EntityKind::File), root())
            .await
    });
    backend.entered().await;
    let counter = backend
        .read(&[fixture.keys.counter("inode")])
        .await
        .unwrap();
    let before_allocation = std::str::from_utf8(counter.rows[0].as_deref().unwrap())
        .unwrap()
        .parse::<i64>()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let ns = fixture.ns.clone();
    let competitor = tokio::spawn(async move {
        ns.create(request(1, b"cancelled", EntityKind::File), root())
            .await
    });
    backend.resume();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), competitor)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        EntityError::Exists
    );
    assert_eq!(
        fixture
            .ns
            .lookup(1, b"cancelled", &root())
            .await
            .unwrap()
            .ino,
        before_allocation + 1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn redis_entity_namespace_contract() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let keys = EntityKeys::new(volume, Uuid::new_v4());
    let backend = Arc::new(RedisEntityBackend::connect(&url, volume).await.unwrap());
    let outcome = AssertUnwindSafe(async {
        let mut lower = MemoryLower::empty();
        lower.add_file(1, b"base", 2, 10);
        // The memory fixture's 1 ms lifetime is deliberately not reused with
        // a wall-clock Redis backend. This contract has a real 60 s lease.
        let fixture = attach_with_ttl(
            backend.clone(),
            Arc::new(lower),
            keys.clone(),
            60_000_000_000,
        )
        .await;
        assert_eq!(fixture.ns.lookup(1, b"base", &root()).await.unwrap().ino, 2);
        mount_contract(backend.clone(), volume).await;
        let ino = exclusive_creates(fixture.ns.clone(), false).await.ino;
        fixture
            .ns
            .link(ino, 1, b"alias".to_vec(), root())
            .await
            .unwrap();
        fixture
            .ns
            .rename(
                1,
                b"exclusive".to_vec(),
                1,
                b"renamed".to_vec(),
                false,
                root(),
            )
            .await
            .unwrap();
        assert_eq!(
            fixture
                .ns
                .lookup(1, b"exclusive", &root())
                .await
                .unwrap_err(),
            EntityError::NotFound
        );
        assert_eq!(
            fixture.ns.lookup(1, b"alias", &root()).await.unwrap().nlink,
            2
        );
        fixture
            .ns
            .unlink(1, b"alias".to_vec(), false, root())
            .await
            .unwrap();
        assert_eq!(
            fixture
                .ns
                .lookup(1, b"renamed", &root())
                .await
                .unwrap()
                .nlink,
            1
        );

        fixture
            .ns
            .unlink(1, b"base".to_vec(), false, root())
            .await
            .unwrap();
        let tombstone = backend.read(&[keys.dentry(1, b"base")]).await.unwrap();
        assert_eq!(
            codec::decode::<EntityDentry>(tombstone.rows[0].as_deref().unwrap()).unwrap(),
            EntityDentry::Whiteout
        );
        assert_eq!(
            fixture.ns.lookup(1, b"base", &root()).await.unwrap_err(),
            EntityError::NotFound
        );
        let recreated = fixture
            .ns
            .create(request(1, b"base", EntityKind::File), root())
            .await
            .unwrap();
        assert!(recreated.ino > 2);
        assert_ne!(recreated.ino, ino);
        assert_eq!((recreated.size, recreated.lower_visible_size), (0, 0));

        fixture.ns.chmod(ino, 0o600, root()).await.unwrap();
        assert!(matches!(
            fixture.ns.open(1, b"renamed", 4, &user(1000)).await,
            Err(EntityError::Permission)
        ));
        fixture
            .ns
            .set_xattr(ino, ACCESS_XATTR.to_vec(), Some(named_acl(1000)), root())
            .await
            .unwrap();
        let named = fixture
            .ns
            .open(1, b"renamed", 6, &user(1000))
            .await
            .unwrap();
        fixture.ns.close(named).unwrap();
        assert!(matches!(
            fixture.ns.open(1, b"renamed", 4, &user(2000)).await,
            Err(EntityError::Permission)
        ));

        let before = backend
            .scan_prefix(keys.volume_prefix(), 4096)
            .await
            .unwrap();
        let handle = fixture.ns.open(1, b"renamed", 6, &root()).await.unwrap();
        fixture.ns.close(handle).unwrap();
        assert_eq!(
            backend
                .scan_prefix(keys.volume_prefix(), 4096)
                .await
                .unwrap(),
            before,
            "ordinary linked open/close changed Redis rows"
        );
        fixture.ns.chmod(1, 0o700, root()).await.unwrap();
        assert!(matches!(
            fixture.ns.open(1, b"absent", 4, &user(1000)).await,
            Err(EntityError::Permission)
        ));
    })
    .catch_unwind()
    .await;

    // Remove only exact keys under this test's freshly generated volume UUID.
    // No FLUSHDB, glob deletion, or deletion of another test's namespace.
    let cleanup = async {
        let rows = backend.scan_prefix(keys.volume_prefix(), 4096).await?;
        let Some((key, value)) = rows.first() else {
            return Ok::<_, EntityError>(());
        };
        let authority = TransactionAuthority::from_validated(
            vec![RowCheck {
                key: key.clone(),
                expected: Some(value.clone()),
            }],
            None,
        )?;
        let checks = rows
            .iter()
            .map(|(key, value)| RowCheck {
                key: key.clone(),
                expected: Some(value.clone()),
            })
            .collect();
        let writes = rows
            .into_iter()
            .map(|(key, _)| RowWrite::Delete { key })
            .collect();
        match backend
            .commit(&authority, &EntityPlan { checks, writes })
            .await?
        {
            CommitOutcome::Committed => Ok(()),
            CommitOutcome::Conflict => Err(EntityError::Conflict),
        }
    }
    .await;
    cleanup.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
