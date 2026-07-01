# pjdfstest POSIX Compatibility Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce BrewFS `pjdfstest` failures by fixing POSIX-compatible errno, metadata cache, timestamp, and permission semantics before tackling larger special-node support.

**Architecture:** Keep the first pass inside the existing FUSE, VFS, and metadata-client boundaries. Fix small, independently verifiable POSIX mismatches first, then gate larger schema-level work such as FIFO/device/socket support behind a separate phase because it changes `FileType`, persisted metadata, and every metadata backend.

**Tech Stack:** Rust, `asyncfuse`, BrewFS metadata layer, Redis-backed pjdfstest Docker Compose runner, SQLite-backed unit tests.

---

## Baseline

Full command:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

Latest baseline artifact:

```text
docker/compose-pjdfstest/artifacts/run-1781357416-29879/results/pjdfstest.log
```

Latest baseline result:

```text
Files=238
Tests=8798
Result: FAIL
failed_files=65
failed_subtests=3867
```

Main failure groups:

```text
rename      failed_files=10 failed_subtests=2043
chown       failed_files=4  failed_subtests=884
unlink      failed_files=3  failed_subtests=209
link        failed_files=4  failed_subtests=164
open        failed_files=8  failed_subtests=146
chmod       failed_files=4  failed_subtests=138
mknod       failed_files=8  failed_subtests=124
mkfifo      failed_files=7  failed_subtests=70
rmdir       failed_files=5  failed_subtests=28
mkdir       failed_files=4  failed_subtests=26
utimensat   failed_files=2  failed_subtests=21
symlink     failed_files=3  failed_subtests=11
ftruncate   failed_files=2  failed_subtests=2
truncate    failed_files=1  failed_subtests=1
```

## File Structure

- Modify: `src/meta/store.rs`
  - Add a metadata error variant for name components longer than `NAME_MAX`.
  - Preserve `setuid`, `setgid`, and sticky bits in `chmod_request` when that task is enabled.
- Modify: `src/vfs/error.rs`
  - Map metadata filename-length errors to `VfsError::FilenameTooLong`.
  - Keep invalid names mapped to `VfsError::InvalidFilename`.
- Modify: `src/fuse/mod.rs`
  - Map `VfsError::FilenameTooLong` to `ENAMETOOLONG`.
  - Preserve special permission bits in FUSE setattr/create helpers.
  - Adjust `utimensat` permission checks if the failing path is confirmed in FUSE setattr handling.
- Modify: `src/meta/client/mod.rs`
  - Return a distinct filename-length error from `validate_entry_name`.
  - Invalidate parent inode attrs after successful namespace mutations.
  - Add unit tests for long filename classification and parent timestamp cache refresh.
- Potentially modify: `src/vfs/fs/mod.rs`
  - Return `FilenameTooLong` instead of `InvalidFilename` for VFS API path components longer than `NAME_MAX`.
- Later large phase: `src/meta/store.rs`, `src/meta/stores/{redis,database,etcd,tikv}/mod.rs`, `src/fuse/mod.rs`
  - Extend metadata model for FIFO, socket, char device, block device, and `rdev`.

## Task 1: Distinguish Long Names From Invalid Names

**Files:**
- Modify: `src/meta/store.rs`
- Modify: `src/meta/client/mod.rs`
- Modify: `src/vfs/error.rs`
- Modify: `src/fuse/mod.rs`

- [x] **Step 1: Write failing unit tests**

Add tests near the existing `src/meta/client/mod.rs` test module:

```rust
#[test]
fn validate_entry_name_returns_filename_too_long_for_long_component() {
    let long_name = "x".repeat(crate::posix::NAME_MAX + 1);
    assert!(matches!(
        MetaClient::<DatabaseMetaStore>::validate_entry_name(&long_name),
        Err(MetaError::FilenameTooLong)
    ));
}

#[test]
fn validate_entry_name_returns_invalid_filename_for_slash_and_nul() {
    assert!(matches!(
        MetaClient::<DatabaseMetaStore>::validate_entry_name("bad/name"),
        Err(MetaError::InvalidFilename)
    ));
    assert!(matches!(
        MetaClient::<DatabaseMetaStore>::validate_entry_name("bad\0name"),
        Err(MetaError::InvalidFilename)
    ));
}
```

Add tests near the existing `src/vfs/error.rs` test module:

```rust
#[test]
fn from_meta_preserves_filename_too_long_semantics() {
    let err = VfsError::from_meta(PathHint::some("/tmp/name"), MetaError::FilenameTooLong);
    assert!(matches!(err, VfsError::FilenameTooLong { .. }));
}
```

- [x] **Step 2: Verify red**

Run:

```bash
cargo test validate_entry_name_returns_filename_too_long_for_long_component from_meta_preserves_filename_too_long_semantics
```

Expected: fails because `MetaError::FilenameTooLong` does not exist and FUSE mapping does not yet expose `ENAMETOOLONG`.

- [x] **Step 3: Implement minimal error split**

Change `MetaError`:

```rust
#[error("Filename too long")]
FilenameTooLong,
```

Change `validate_entry_name`:

```rust
if name.is_empty() {
    return Err(MetaError::InvalidFilename);
}
if name.len() > NAME_MAX {
    return Err(MetaError::FilenameTooLong);
}
```

Change `VfsError::from_meta`:

```rust
MetaError::FilenameTooLong => VfsError::FilenameTooLong { path },
```

Change FUSE errno conversion:

```rust
VfsError::FilenameTooLong { .. } => libc::ENAMETOOLONG,
```

- [x] **Step 4: Verify green**

Run:

```bash
cargo test validate_entry_name_returns_filename_too_long_for_long_component validate_entry_name_returns_invalid_filename_for_slash_and_nul from_meta_preserves_filename_too_long_semantics
```

Expected: all selected tests pass.

- [x] **Step 5: Verify pjdfstest subset**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "ftruncate/02.t truncate/02.t symlink/02.t unlink/02.t"
```

Expected: long-name subtests that previously returned `EINVAL` or `ENOENT` now return `ENAMETOOLONG`. Any remaining failures in the selected files must be recorded before moving on.

Verified:

```text
cargo test filename_too_long
cargo test validate_entry_name_returns_invalid_filename_for_slash_and_nul
cargo test --release validate_fuse_name_returns_enametoolong_for_long_component
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "ftruncate/02.t truncate/02.t symlink/02.t unlink/02.t" --prove-args "-v"

Files=4, Tests=21
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781359926-1042
```

## Task 2: Invalidate Parent Inode Attr After Namespace Mutations

**Files:**
- Modify: `src/meta/client/mod.rs`

- [x] **Step 1: Write failing unit test**

Add a test near the metadata-client cache tests:

```rust
#[tokio::test]
async fn mkdir_invalidates_cached_parent_attr_after_store_updates_parent_timestamp() {
    let client = create_test_client().await;
    let before = client.stat(1).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;

    client.mkdir(1, "fresh-dir".to_string()).await.unwrap();

    let after = client.stat(1).await.unwrap().unwrap();
    assert_ne!(after.mtime, before.mtime);
}
```

- [x] **Step 2: Verify red**

Run:

```bash
cargo test mkdir_invalidates_cached_parent_attr_after_store_updates_parent_timestamp
```

Expected: fails because `stat(1)` returns the cached parent attr from before the `mkdir`.

- [x] **Step 3: Implement minimal cache invalidation helper**

Add a helper to `MetaClient`:

```rust
async fn invalidate_parent_after_namespace_mutation(&self, parent: i64) {
    self.inode_cache.invalidate_inode(parent).await;
    self.invalidate_parent_path(parent).await;
}
```

Call it after successful `mkdir`, `create_file`, `link`, `symlink`, `rmdir`, and `unlink` store mutations, replacing the path-only invalidation in those methods.

- [x] **Step 4: Verify green**

Run:

```bash
cargo test mkdir_invalidates_cached_parent_attr_after_store_updates_parent_timestamp
```

Expected: selected unit test passes.

- [x] **Step 5: Verify pjdfstest subset**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "mkdir/00.t symlink/00.t"
```

Expected: parent `mtime`/`ctime` assertions in `mkdir/00.t` and `symlink/00.t` improve. Record remaining failures.

Verified:

```text
cargo test --release --lib mkdir_invalidates_cached_parent_attr_after_store_updates_parent_timestamp
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "mkdir/00.t symlink/00.t" --prove-args "-v"

Files=2, Tests=50
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781360723-30579
```

## Task 3: Preserve Special Permission Bits

**Files:**
- Modify: `src/meta/store.rs`
- Modify: `src/fuse/mod.rs`

- [x] **Step 1: Update failing unit tests**

Replace the current stripping tests with preservation tests:

```rust
#[test]
fn sanitize_special_mode_bits_preserves_setuid_setgid_and_sticky() {
    assert_eq!(sanitize_special_mode_bits(0o1777), 0o1777);
    assert_eq!(sanitize_special_mode_bits(0o2755), 0o2755);
    assert_eq!(sanitize_special_mode_bits(0o4755), 0o4755);
}

#[test]
fn apply_creation_umask_preserves_special_bits_and_masks_permissions() {
    assert_eq!(apply_creation_umask(0o1777, 0), 0o1777);
    assert_eq!(apply_creation_umask(0o1777, 0o022), 0o1755);
    assert_eq!(apply_creation_umask(0o4755, 0o022), 0o4755);
}
```

Add a `chmod_request` test in `src/meta/store.rs`:

```rust
#[test]
fn chmod_request_preserves_special_permission_bits() {
    assert_eq!(chmod_request(0o4755).mode, Some(0o4755));
    assert_eq!(chmod_request(0o2755).mode, Some(0o2755));
    assert_eq!(chmod_request(0o1777).mode, Some(0o1777));
}
```

- [x] **Step 2: Verify red**

Run:

```bash
cargo test sanitize_special_mode_bits_preserves_setuid_setgid_and_sticky chmod_request_preserves_special_permission_bits
```

Expected: selected tests fail against current stripping logic.

- [x] **Step 3: Implement preservation**

Change `sanitize_special_mode_bits`:

```rust
fn sanitize_special_mode_bits(mode: u32) -> u32 {
    mode & 0o7777
}
```

Change `chmod_request`:

```rust
mode: Some(new_mode & 0o7777),
```

- [x] **Step 4: Verify green**

Run:

```bash
cargo test sanitize_special_mode_bits_preserves_setuid_setgid_and_sticky apply_creation_umask_preserves_special_bits_and_masks_permissions chmod_request_preserves_special_permission_bits
```

Expected: selected tests pass.

- [x] **Step 5: Verify pjdfstest subset**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chmod/00.t chmod/11.t"
```

Expected: failures related to setuid, setgid, and sticky preservation decrease. Permission-denied cases must be checked separately before declaring the chmod suite healthy.

Verified:

```text
cargo test --release --lib special_bits
cargo test --release --lib preserves_setuid
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chmod/00.t chmod/11.t" --prove-args "-v"

chmod/00.t: failed 61 -> 60
chmod/11.t: failed 64 -> 56
Result: FAIL due remaining mkfifo/mknod/socket unsupported special-node paths
artifact: docker/compose-pjdfstest/artifacts/run-1781361762-4399
```

## Task 4: Fix O_CREAT Mode 000 Write-Handle Semantics

**Files:**
- Modify: `src/fuse/mod.rs`
- Potentially modify: `src/vfs/fs/mod.rs`

- [x] **Step 1: Reproduce with pjdfstest subset**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "ftruncate/00.t" --prove-args "-v"
```

Expected current failure:

```text
-u 65534 open <file> O_CREAT,O_RDWR 0 : ftruncate 0 0 expected 0, got EACCES
```

- [x] **Step 2: Trace create/open path**

Inspect:

```bash
rg -n "async fn create|apply_new_entry_attrs|open_with_cached_attr|stat_for_open|ensure_access_allowed" src/fuse src/vfs src/meta
```

Expected finding: a newly created file with mode `000` gets a valid write-capable file handle from `create`, but later truncate/ftruncate path re-checks permissions against mode bits instead of honoring the already write-open handle.

- [x] **Step 3: Write failing test at the lowest useful layer**

If the bug is in the FUSE file-handle path, add a unit test for the helper that resolves write capability from the handle. If no helper exists, extract one small helper first through a red test:

```rust
#[test]
fn write_capable_create_handle_allows_ftruncate_even_when_mode_is_zero() {
    assert!(write_handle_allows_truncate(true, 0o000));
}
```

- [x] **Step 4: Implement minimal fix**

Make ftruncate honor the write flag recorded on the opened file handle. Keep path-based `truncate(2)` permission checks unchanged, because path truncate still needs permission validation.

- [x] **Step 5: Verify**

Run:

```bash
cargo test write_capable_create_handle_allows_ftruncate_even_when_mode_is_zero
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "ftruncate/00.t"
```

Expected: the mode-000 create/ftruncate assertion passes or disappears from verbose failures.

Verified:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "ftruncate/00.t" --prove-args "-v"

Initial failure:
not ok 24 - tried '-u 65534 open ... O_CREAT,O_RDWR 0 : ftruncate 0 0', expected 0, got EACCES
artifact: docker/compose-pjdfstest/artifacts/run-1781362138-11848

FUSE op log confirmed the real request included fh plus size/mtime/ctime:
artifact: docker/compose-pjdfstest/artifacts/run-1781362801-18884

cargo test --release --lib setattr_size
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "ftruncate/00.t" --prove-args "-v"

Files=1, Tests=26
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781363013-17675
```

## Task 5: Fix utimensat UTIME_NOW Permission Semantics

**Files:**
- Modify: `src/fuse/mod.rs`

- [x] **Step 1: Reproduce with pjdfstest subset**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "utimensat/06.t" --prove-args "-v"
```

Expected current failure:

```text
-u 65534 open . O_RDONLY : utimensat ... expected 0, got EACCES
```

- [x] **Step 2: Inspect setattr timestamp flags**

Inspect:

```bash
rg -n "SetAttrFlags|SET_ATIME_NOW|SET_MTIME_NOW|utimensat|ensure_access_allowed|setattr" src/fuse/mod.rs src/meta
```

Expected finding: `UTIME_NOW` should be allowed when the caller owns the file or has write permission on the file; it must not require write access to the parent directory or to the opened directory fd.

- [x] **Step 3: Add focused permission test**

Add a helper-level test in `src/fuse/mod.rs`:

```rust
#[test]
fn utime_now_permission_allows_file_owner_without_write_bits() {
    let attr = VfsFileAttr {
        ino: 2,
        size: 0,
        kind: VfsFileType::File,
        mode: 0o444,
        uid: 65534,
        gid: 65534,
        atime: 0,
        mtime: 0,
        ctime: 0,
        nlink: 1,
    };
    assert!(utime_now_permission_allowed(&attr, 65534, 65534));
}
```

- [x] **Step 4: Implement minimal permission helper**

Implement a helper that permits `UTIME_NOW` when:

```text
uid == 0
or caller uid == file uid
or caller has write access to the file
```

Use it only for timestamp updates that came from `UTIME_NOW`. Explicit timestamp changes must retain stricter owner/capability semantics.

- [x] **Step 5: Verify**

Run:

```bash
cargo test utime_now_permission_allows_file_owner_without_write_bits
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "utimensat/06.t"
```

Expected: owner `UTIME_NOW` failures in `utimensat/06.t` are resolved.

Verified:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "utimensat/06.t" --prove-args "-v"

Initial failure:
not ok 7 - tried '-u 65534 open . O_RDONLY : utimensat ... UTIME_NOW ...', expected 0, got EACCES
artifact: docker/compose-pjdfstest/artifacts/run-1781363234-9930

FUSE op log confirmed asyncfuse 0.0.8 converts FATTR_ATIME_NOW/FATTR_MTIME_NOW
to concrete timestamps before BrewFS sees SetAttr:
artifact: docker/compose-pjdfstest/artifacts/run-1781363419-15367

cargo test --release --lib timestamp_setattr
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "utimensat/06.t utimensat/07.t" --prove-args "-v"

Files=2, Tests=30
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781364020-2276
```

Note: because asyncfuse currently drops the raw `FATTR_*_NOW` flags, BrewFS uses a
small "near current time" heuristic for the non-owner write-permission branch.
Owner and root explicit timestamp semantics remain strict, and `utimensat/07.t`
guards non-owner explicit timestamp rejection.

## Task 6: Review Remaining Failures and Decide Special Node Scope

**Files:**
- Modify if implemented: `src/meta/store.rs`
- Modify if implemented: `src/meta/stores/redis/mod.rs`
- Modify if implemented: `src/meta/stores/database/mod.rs`
- Modify if implemented: `src/meta/stores/etcd/mod.rs`
- Modify if implemented: `src/meta/stores/tikv/mod.rs`
- Modify if implemented: `src/fuse/mod.rs`

- [x] **Step 1: Re-run the full suite**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

Expected: failure count decreases from the baseline. Record the new artifact path and totals in this document.

Verified:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh

Files=238
Tests=8798
Result: FAIL
failed_files=46
failed_subtests=3689
artifact: docker/compose-pjdfstest/artifacts/run-1781364245-5619
```

Delta from baseline:

```text
failed_files:    65 -> 46
failed_subtests: 3867 -> 3689
```

Category summary after Tasks 1-5:

```text
rename      failed_files=8 failed_subtests=1952
chown       failed_files=3 failed_subtests=854
unlink      failed_files=2 failed_subtests=192
link        failed_files=3 failed_subtests=160
open        failed_files=6 failed_subtests=143
chmod       failed_files=3 failed_subtests=128
mknod       failed_files=8 failed_subtests=121
mkfifo      failed_files=7 failed_subtests=69
mkdir       failed_files=2 failed_subtests=23
utimensat   failed_files=1 failed_subtests=20
rmdir       failed_files=2 failed_subtests=19
symlink     failed_files=1 failed_subtests=8
```

Resolved or improved groups:

```text
ftruncate   failed_files=2 -> 0
truncate    failed_files=1 -> 0
symlink     failed_files=3 -> 1
utimensat   failed_files=2 -> 1
mkdir       failed_files=4 -> 2
rmdir       failed_files=5 -> 2
```

- [x] **Step 2: Classify remaining special-node failures**

Run:

```bash
log="docker/compose-pjdfstest/artifacts/<latest>/results/pjdfstest.log"
awk '/^\/opt\/pjdfstest\/tests\// && /\(Wstat:/ { file=$1; failed=$0; sub(/^.*Failed: /,"",failed); sub(/\).*/,"",failed); print file, failed }' "$log" | sort
```

Expected: `mkfifo`, `mknod`, socket, block-device, and char-device failures remain if special nodes are still unsupported.

Verified remaining failures are split into two scopes:

```text
Small/medium permission semantics:
- mkdir/01.t, mkdir/10.t
- open/01.t, open/06.t, open/07.t, open/22.t, open/24.t
- rmdir/01.t, rmdir/06.t
- symlink/08.t
- unlink/00.t, unlink/11.t
- rename/00.t, rename/09.t, rename/10.t, rename/12.t, rename/13.t, rename/14.t, rename/20.t, rename/23.t
- link/00.t, link/01.t, link/10.t
- utimensat/00.t

Large metadata model scope:
- mkfifo/*.t
- mknod/*.t
- chmod/chown/open/link/rename/unlink subtests that exercise FIFO/socket/device nodes
```

- [x] **Step 3: Make explicit product decision**

Choose one:

```text
Option A: Support special nodes.
Implement FileType::Fifo, FileType::Socket, FileType::CharDevice, FileType::BlockDevice and rdev persistence across all metadata backends.

Option B: Keep special nodes unsupported.
Document unsupported POSIX surface and add pjdfstest expected-failure exclusions for special-node suites.
```

- [ ] **Step 4: If Option A, write a second implementation plan**

Create:

```text
doc/superpowers/plans/2026-06-13-pjdfstest-special-nodes.md
```

Expected: the special-node plan includes backend schema/data-format compatibility steps and migration strategy before any code changes.

Current decision for this plan: continue fixing small/medium permission semantics
first. Defer `Option A` special-node support to a separate plan because it needs
persisted metadata type/rdev compatibility across Redis, database, etcd, and
TiKV.

Decision after Task 8: choose Option A. The remaining failures are now dominated
by FIFO, socket, block-device, and char-device cases plus cross-operation
failures that cascade from unsupported special-node creation.

- [x] **Step 4: If Option A, write a second implementation plan**

Created:

```text
doc/superpowers/plans/2026-06-13-pjdfstest-special-nodes.md
```

## Task 7: Fix chmod Permission Checks for Mode-only setattr

**Files:**
- Modify: `src/fuse/mod.rs`

- [x] **Step 1: Reproduce with focused pjdfstest subset**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "open/06.t" --prove-args "-v"
```

Observed current pattern: `chmod` by the file owner fails with `EACCES` once the
file mode no longer grants owner write permission. That cascades into many
wrong `open` results in the same test.

- [x] **Step 2: Trace FUSE setattr path**

Inspect:

```bash
rg -n "async fn setattr|ensure_access_allowed|inode_mutation_access_mask|SetAttr" src/fuse/mod.rs
```

Finding: FUSE `setattr` used `inode_mutation_access_mask()` (`W_OK`) for generic
metadata updates. POSIX `chmod` does not require the current file mode to grant
write permission; it requires owner or root. Non-owner chmod should return
`EPERM`, even when the file's mode grants write access.

- [x] **Step 3: Write failing unit tests**

Add FUSE-level tests:

```rust
#[tokio::test]
async fn mode_setattr_allows_owner_without_write_bits() {
    let fs = new_fuse_test_vfs().await;
    fs.create_file("/file.txt").await.unwrap();
    let attr = fs.stat("/file.txt").await.unwrap();
    fs.chown(attr.ino, Some(1000), Some(1000)).await.unwrap();
    fs.chmod(attr.ino, 0o444).await.unwrap();

    let reply = Filesystem::setattr(
        &fs,
        user_request(),
        attr.ino as u64,
        None,
        SetAttr {
            mode: Some(0o600),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(reply.attr.perm, 0o600);
}
```

- [x] **Step 4: Implement minimal permission guard**

Add mode-only setattr detection and owner/root permission:

```rust
fn setattr_is_mode_with_optional_timestamps(req: &SetAttrRequest, flags: &SetAttrFlags) -> bool {
    req.mode.is_some()
        && req.uid.is_none()
        && req.gid.is_none()
        && req.size.is_none()
        && req.flags.is_none()
        && flags.is_empty()
}
```

```rust
async fn ensure_mode_setattr_allowed(&self, ino: i64, uid: u32) -> FuseResult<()> {
    let Some(attr) = self.stat_ino(ino).await else {
        return Err(libc::ENOENT.into());
    };

    if uid == 0 || uid == attr.uid {
        Ok(())
    } else {
        Err(libc::EPERM.into())
    }
}
```

- [x] **Step 5: Verify**

Run:

```bash
cargo test --release --lib mode_setattr
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "open/06.t" --prove-args "-v"
```

Verified:

```text
cargo test --release --lib mode_setattr
2 passed

open/06.t: failed 96 -> 47
remaining failed tests: 65-111
artifact: docker/compose-pjdfstest/artifacts/run-1781365109-13922
```

Review:

```text
Root cause fixed: mode-only setattr now uses chmod owner/root semantics instead
of write-permission semantics.

Regression check:
- open/07.t PASS
- chmod/00.t remains 60 failed, all tied to unsupported FIFO/device/socket paths
- chmod/11.t remains 56 failed, all tied to unsupported FIFO/device/socket paths
- cargo fmt --check PASS
```

Full-suite checkpoint after Task 7:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh

Files=238
Tests=8798
Result: FAIL
failed_files=46
failed_subtests=3628
artifact: docker/compose-pjdfstest/artifacts/run-1781365509-19135
```

Delta from Task 1-5 checkpoint:

```text
failed_files:    46 -> 46
failed_subtests: 3689 -> 3628
```

Category summary after Task 7:

```text
rename      failed_files=8 failed_subtests=1952
chown       failed_files=3 failed_subtests=851
unlink      failed_files=2 failed_subtests=192
link        failed_files=3 failed_subtests=160
chmod       failed_files=4 failed_subtests=134
mknod       failed_files=8 failed_subtests=121
open        failed_files=5 failed_subtests=79
mkfifo      failed_files=7 failed_subtests=69
mkdir       failed_files=2 failed_subtests=23
utimensat   failed_files=1 failed_subtests=20
rmdir       failed_files=2 failed_subtests=19
symlink     failed_files=1 failed_subtests=8
```

Review note: Task 7 fixed the owner/root chmod permission class and removed
`open/07.t` from the failure set. It also exposed `chmod/12.t` with 6 failures:
non-owner writes to setuid/setgid files should trigger kernel clearing of
setuid/setgid bits, but BrewFS rejects the resulting mode-only `setattr` with
`EPERM`.

## Task 8: Allow Kernel setuid/setgid Clearing on Write

**Files:**
- Modify: `src/fuse/mod.rs`

- [x] **Step 1: Reproduce and inspect the focused failure**

Run:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chmod/12.t" --prove-args "-v"
```

Observed:

```text
chmod/12.t: failed 6/14
artifact: docker/compose-pjdfstest/artifacts/run-1781365696-13405
```

FUSE operation log:

```text
artifact: docker/compose-pjdfstest/artifacts/run-1781365733-10121/brewfs_fuse_ops.log
```

Finding: the non-owner write itself opens successfully, then the kernel sends a
mode-only `setattr` to clear setuid/setgid bits before writeback:

```text
set_attr=SetAttr { mode: Some(33279), uid: None, gid: None, size: None, ... }
[setattr] - Error: Errno(1)
```

`33279` is `S_IFREG | 0o777`; BrewFS sanitizes this to `0o777`, so the request
changes only the setuid/setgid bits from the previous mode such as `0o4777`.

- [x] **Step 2: Write failing unit tests**

Add FUSE-level tests:

```rust
#[tokio::test]
async fn mode_setattr_allows_non_owner_to_clear_only_suid_sgid_bits() {
    let fs = new_fuse_test_vfs().await;
    fs.create_file("/file.txt").await.unwrap();
    let attr = fs.stat("/file.txt").await.unwrap();
    fs.chmod(attr.ino, 0o6777).await.unwrap();

    let reply = Filesystem::setattr(
        &fs,
        user_request(),
        attr.ino as u64,
        None,
        SetAttr {
            mode: Some(0o777),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(reply.attr.perm, 0o777);
}
```

Keep the existing non-owner arbitrary chmod rejection test as the regression
guard.

RED verification:

```text
cargo test --release --lib mode_setattr_allows_non_owner_to_clear_only_suid_sgid_bits

Result: FAIL
failure: Errno(1)
```

- [x] **Step 3: Implement minimal permission exception**

Update `ensure_mode_setattr_allowed` to accept the requested mode. For non-root,
non-owner callers, permit only mode requests that:

```text
- leave all non-setuid/setgid bits unchanged
- do not add setuid/setgid bits
- clear at least one existing setuid/setgid bit
```

This matches the kernel-generated suid/sgid clear operation while preserving
ordinary `chmod(2)` owner/root semantics.

Implementation notes:

```text
- FUSE mode-only setattr now passes the requested mode into the chmod permission guard.
- Non-owner callers may only clear existing setuid/setgid bits.
- All non-setuid/setgid bits must remain unchanged.
- `Permission::file_type_bits()` now keeps only `0o170000`; old special bits are no longer mistaken for file type bits during chmod.
```

- [x] **Step 4: Verify**

Run:

```bash
cargo test --release --lib mode_setattr
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chmod/12.t" --prove-args "-v"
```

Expected:

```text
mode_setattr tests pass
chmod/12.t PASS
```

Verified:

```text
cargo test --release --lib mode_setattr
4 passed

cargo test --release --lib test_chmod_replaces_old_special_bits
1 passed

bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chmod/12.t" --prove-args "-v"

Files=1
Tests=14
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781366490-23975
```

Review:

```text
Root cause fixed: kernel-generated mode-only setattr can clear suid/sgid after
non-owner writes, while ordinary non-owner chmod remains EPERM.

Regression guard:
- mode_setattr_rejects_non_owner_even_with_write_bits PASS
- mode_setattr_clear_suid_sgid_exception_is_narrow PASS
```

Full-suite checkpoint after Task 8:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh

Files=238
Tests=8798
Result: FAIL
failed_files=45
failed_subtests=3622
artifact: docker/compose-pjdfstest/artifacts/run-1781366695-14925
```

Delta from Task 7 checkpoint:

```text
failed_files:    46 -> 45
failed_subtests: 3628 -> 3622
```

Category summary after Task 8:

```text
rename      failed_files=8 failed_subtests=1952
chown       failed_files=3 failed_subtests=851
unlink      failed_files=2 failed_subtests=192
link        failed_files=3 failed_subtests=160
chmod       failed_files=3 failed_subtests=128
mknod       failed_files=8 failed_subtests=121
open        failed_files=5 failed_subtests=79
mkfifo      failed_files=7 failed_subtests=69
mkdir       failed_files=2 failed_subtests=23
utimensat   failed_files=1 failed_subtests=20
rmdir       failed_files=2 failed_subtests=19
symlink     failed_files=1 failed_subtests=8
```

Review note: `chmod/12.t` is removed from the failure list. Remaining `chmod`
failures are `chmod/00.t`, `chmod/01.t`, and `chmod/11.t`.

## Task 9: Finish chown/00.t Permission Semantics

**Files:**
- Modify: `src/fuse/mod.rs`
- Modify: `src/fuse/mount.rs`

Root causes found after special-node support reduced the full suite to one
failing file:

```text
artifact: docker/compose-pjdfstest/artifacts/run-1781368845-18669
failed_files=1
failed_subtests=103
only failing file: chown/00.t
```

- [x] **Step 1: Allow owner chown setattr with kernel suid/sgid clear mode**

RED:

```text
cargo test --release --lib chown_setattr_allows_owner_group_change_with_kernel_clear_mode -- --nocapture

Result: FAIL
reason: owner group-change chown carrying mode=0555 returned EPERM
```

GREEN:

```text
cargo test --release --lib chown_setattr_allows_owner_group_change_with_kernel_clear_mode -- --nocapture
1 passed

cargo test --release --lib chown_setattr -- --nocapture
4 passed
```

Result:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chown/00.t" --prove-args "-v"

artifact: docker/compose-pjdfstest/artifacts/run-1781370271-29091
failed_subtests: 93 -> 3
remaining failures: 291, 294, 300
```

- [x] **Step 2: Move permission checks to BrewFS userspace**

RED:

```text
cargo test --release --lib default_mount_options_leave_permission_checks_to_userspace -- --nocapture

Result: FAIL
reason: default mount options still included default_permissions: true
```

GREEN:

```text
cargo test --release --lib default_mount_options_leave_permission_checks_to_userspace -- --nocapture
1 passed
```

Result:

```text
artifact: docker/compose-pjdfstest/artifacts/run-1781371028-8632
chown/00.t still failed 3 subtests, but FUSE op logs confirmed permission-sensitive setattr requests now reached userspace.
```

- [x] **Step 3: Allow ctime-only setattr for chown(-1, -1) no-op**

RED:

```text
cargo test --release --lib timestamp_setattr_allows_non_owner_ctime_only_chown_noop -- --nocapture

Result: FAIL
reason: non-owner ctime-only setattr returned EPERM
```

GREEN:

```text
cargo test --release --lib timestamp_setattr_allows_non_owner_ctime_only_chown_noop -- --nocapture
1 passed

cargo test --release --lib timestamp_setattr -- --nocapture
4 passed, 1 ignored

cargo test --release --lib chown_setattr -- --nocapture
4 passed
```

Targeted pjdfstest:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chown/00.t" --prove-args "-v"

Files=1
Tests=1280
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781371974-16963
```

Review:

```text
Root cause fixed: chown(path, -1, -1) on Linux/FUSE may arrive as a ctime-only
setattr. BrewFS now permits ctime-only setattr for non-owners, while existing
tests still reject non-owner explicit atime/mtime changes.

Regression guards:
- timestamp_setattr_rejects_non_owner_explicit_time_even_with_write_bits PASS
- timestamp_setattr_allows_non_owner_current_time_with_write_bits PASS
- chown_setattr_rejects_non_owner_group_change PASS
```

Next checkpoint:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

Full-suite checkpoint after Task 9:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh

Files=238
Tests=8798
Result: FAIL
failed_files=13
failed_subtests=498
artifact: docker/compose-pjdfstest/artifacts/run-1781372269-18504
```

Remaining failures after Task 9:

```text
rename/09.t    failed_subtests=232  source sticky-directory rename checks
rename/10.t    failed_subtests=176  destination sticky-directory rename checks
unlink/11.t    failed_subtests=36   sticky-directory unlink checks
chown/07.t     failed_subtests=19
open/06.t      failed_subtests=15
rmdir/11.t     failed_subtests=6    sticky-directory rmdir checks
link/06.t      failed_subtests=4
chown/05.t     failed_subtests=3
ftruncate/05.t failed_subtests=2
truncate/05.t  failed_subtests=2
chmod/00.t     failed_subtests=1
chmod/05.t     failed_subtests=1
open/05.t      failed_subtests=1
```

Next priority: implement sticky-directory mutation checks for `rename`,
`unlink`, and `rmdir`, starting with `rename/09.t` and `rename/10.t`.

## Task 10: Enforce Sticky-Directory Namespace Mutation Rules

**Files:**
- Modify: `src/fuse/mod.rs`

Root causes:

```text
rename/09.t: source parent has sticky bit; non-root caller must own the sticky
parent or the source child.

rename/10.t: destination parent has sticky bit and destination exists; non-root
caller must own the sticky parent or the destination child.

unlink/11.t and rmdir/11.t: same sticky-parent child mutation rule.

Directory cross-parent rename: moving a directory to a different parent updates
its `..` entry, so the caller also needs namespace-mutation access to the
source directory itself.
```

- [x] **Step 1: Write RED tests**

```text
cargo test --release --lib rename_rejects_non_owner -- --nocapture

Result: FAIL
reason: sticky source-parent and destination-child rename cases returned Ok(())

cargo test --release --lib rename_rejects_cross_parent_directory_move_without_source_dir_mutation_access -- --nocapture

Result: FAIL
reason: cross-parent directory rename returned Ok(()) when the caller lacked source-directory mutation access
```

- [x] **Step 2: Implement sticky checks and directory cross-parent check**

Added:

```text
ensure_sticky_parent_allows_child_mutation(parent, child, uid)
```

Applied it to:

```text
unlink
rmdir
rename source child
rename existing destination child
```

Also required `write+execute` on the source directory when a directory is moved
across parents.

- [x] **Step 3: Verify unit tests**

```text
cargo test --release --lib rename_ -- --nocapture

16 passed
26 ignored
```

- [x] **Step 4: Verify pjdfstest subset**

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "rename/09.t rename/10.t" --prove-args "-v"

Files=2
Tests=4452
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781373452-1754

bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "unlink/11.t rmdir/11.t" --prove-args "-v"

Files=2
Tests=317
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781373646-23749
```

Review:

```text
Root cause fixed: sticky-directory namespace mutations now honor the POSIX
owner exceptions, and directory cross-parent moves require source-directory
mutation access.

Regression guards:
- rename_requires_namespace_access_on_source_parent PASS
- rename_requires_namespace_access_on_destination_parent PASS
- rename_rejects_non_owner_from_sticky_source_parent PASS
- rename_rejects_non_owner_over_sticky_destination_child PASS
- rename_rejects_cross_parent_directory_move_without_source_dir_mutation_access PASS
```

Next checkpoint:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

## Full Checkpoint After Sticky Namespace Fixes

Command:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

Result:

```text
artifact: docker/compose-pjdfstest/artifacts/run-1781373774-27881
Files=238
Tests=8798
Result: FAIL
failed_files=9
failed_subtests=48
```

Remaining failures:

```text
chmod/00.t: 1
chmod/05.t: 1
chown/05.t: 3
chown/07.t: 19
ftruncate/05.t: 2
link/06.t: 4
open/05.t: 1
open/06.t: 15
truncate/05.t: 2
```

Review:

```text
Sticky namespace fixes held in the full run:
- rename/09.t PASS
- rename/10.t PASS
- unlink/11.t PASS
- rmdir/11.t PASS

Next priority:
1. Path-prefix search permissions after cached dentries.
2. chown group-membership semantics.
3. open directory/FIFO permission edge cases.
4. chmod setgid clearing edge case.
```

## Task 11: Enforce Path-Prefix Search Permission For Cached Inode Entrypoints

Hypothesis:

```text
With non-zero FUSE entry TTL, pjdfstest resolves a path while the parent
directory is searchable, then chmods that parent to remove execute/search.
Subsequent path operations can arrive at FUSE as inode-based open/setattr/link
requests without another lookup. BrewFS was checking final inode permissions
but not the current search permission on the cached path ancestors.
```

RED tests:

```bash
cargo test --release --lib lookup_rejects_child_when_parent_lacks_search_access -- --nocapture
cargo test --release --lib "cached_inode_when_parent_lacks_search_access" -- --nocapture
cargo test --release --lib link_rejects_cached_source_inode_when_parent_lacks_search_access -- --nocapture
```

Expected RED observations:

```text
lookup returned ReplyEntry instead of EACCES.
open returned ReplyOpen instead of EACCES.
setattr returned ReplyAttr instead of EACCES.
link returned ReplyEntry and created the hardlink instead of EACCES.
```

Fix:

```text
- Add VFS::paths_of(ino) as a narrow wrapper around meta_get_paths.
- Add FUSE helper ensure_inode_paths_search_allowed().
- Keep lookup parent X_OK check.
- Enforce cached-path ancestor X_OK before inode-based open.
- Enforce cached-path ancestor X_OK before path-style setattr when fh is None.
- Enforce source cached-path ancestor X_OK before link_by_ino.
```

GREEN verification:

```text
cargo test --release --lib lookup_rejects_child_when_parent_lacks_search_access -- --nocapture

1 passed

cargo test --release --lib "cached_inode_when_parent_lacks_search_access" -- --nocapture
cargo test --release --lib link_rejects_cached_source_inode_when_parent_lacks_search_access -- --nocapture

open/setattr: 2 passed
link: 1 passed
```

pjdfstest subset:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh \
  --tests "chown/05.t link/06.t open/05.t chmod/05.t truncate/05.t ftruncate/05.t" \
  --prove-args "-v"

artifact: docker/compose-pjdfstest/artifacts/run-1781375223-19656
Files=6
Tests=92
Result: PASS
```

Review:

```text
Root cause fixed for single-path cached dentries. Hardlink path identity is
still approximate because FUSE inode requests do not carry the originally
resolved source path; BrewFS currently allows the operation if any known path
to the inode has searchable ancestors. This is acceptable for the current
pjdfstest failures and should be revisited if stricter hardlink path semantics
are required.
```

## Task 12: Enforce chown Group Membership

Hypothesis:

```text
BrewFS allowed a non-root owner to change an inode group to any gid as long as
the uid stayed unchanged. POSIX only allows this when the requested gid is one
of the caller's effective or supplementary groups.
```

Evidence:

```text
pjdfstest chown/07.t failures all attempted:
  -u 65534 -g 65534 -- chown <path> -1 65533

Expected: EPERM
Actual before fix: 0
```

Reference check:

```text
pjdfstest.c set_gids() calls setgroups(list) and then setegid(list[0]).
Therefore `-g 65533,65534` has effective gid 65533 and supplementary group
65534. FUSE Request carries the effective gid, so BrewFS must inspect the
request pid on Linux to see supplementary groups.
```

RED tests:

```bash
cargo test --release --lib chown_setattr -- --nocapture
```

Expected RED observation:

```text
chown_setattr_rejects_owner_group_change_to_non_member_group failed because
setattr returned gid=2000 instead of EPERM.
```

Fix:

```text
- Add parser for `/proc/<pid>/status` `Groups:` line.
- Add request_group_ids(pid, fallback_gid).
- In FUSE chown setattr permission checks, reject owner gid changes when the
  target gid is neither the current file gid nor a request group.
- Keep non-Linux fallback to the request gid only.
```

GREEN verification:

```text
cargo test --release --lib chown_setattr -- --nocapture

5 passed

cargo test --release --lib parse_proc_status_groups_reads_supplementary_groups -- --nocapture

1 passed
```

pjdfstest subset:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh \
  --tests "chown/07.t chown/05.t" \
  --prove-args "-v"

artifact: docker/compose-pjdfstest/artifacts/run-1781375834-3187
Files=2
Tests=150
Result: PASS
```

Review:

```text
The fix rejects non-member gid changes and preserves supplementary-group
success paths. This is Linux-specific for supplementary group discovery; other
platforms conservatively use the effective gid from the FUSE request.
```

## Task 13: Re-enable Kernel Permission Checks For Special Nodes

Hypothesis:

```text
The remaining open/06.t failures are all FIFO opens. FIFO open is handled by
the kernel as a special node path and does not consistently enter BrewFS'
Filesystem::open callback. With default_permissions disabled, the kernel also
skips mode checks for these special-node opens, allowing O_RDWR/O_RDONLY cases
that should fail.
```

Evidence:

```text
open/06.t failures before fix:
78, 82, 86, 88, 90, 92, 94, 96, 98, 100, 102, 104, 106, 108, 110

Mapped expectations:
- FIFO O_RDWR should fail when write or read permission is denied.
- FIFO O_RDONLY,O_NONBLOCK should fail when read permission is denied.
```

Fix:

```text
Set default mount options back to default_permissions(true), keeping BrewFS'
userspace checks as stricter refinements for setattr, path-prefix search, and
POSIX ownership semantics.
```

Verification:

```text
cargo test --release --lib default_mount_options_enable_kernel_permission_checks -- --nocapture

1 passed

bash docker/compose-pjdfstest/run_redis_pjdfstest.sh \
  --tests "open/06.t chown/00.t" \
  --prove-args "-v"

artifact: docker/compose-pjdfstest/artifacts/run-1781376266-3691
Files=2
Tests=1424
Result: PASS
```

Review:

```text
The earlier concern was that default_permissions might prevent chown(-1, -1)
ctime-only requests from reaching userspace. The paired open/06.t + chown/00.t
run disproved that regression on the current implementation: FIFO mode checks
are restored and chown/00.t remains green.
```

## Task 14: Clear SGID On chmod For Non-Member Regular Files

Hypothesis:

```text
chmod/00.t test 117 fails because BrewFS preserves SGID when a non-root owner
sets SGID on a regular file whose gid is not in the caller's effective or
supplementary groups. POSIX requires successful chmod to clear SGID in this
case.
```

RED tests:

```bash
cargo test --release --lib "mode_setattr_" -- --nocapture
```

Expected RED observation:

```text
mode_setattr_clears_sgid_for_owner_outside_file_group failed with perm 02755
instead of 0755.
```

Fix:

```text
Before applying mode setattr, derive an effective mode:
- only for regular files,
- only for non-root callers,
- only when requested mode includes SGID,
- clear SGID if the file gid is not in request_group_ids(pid, gid).
```

GREEN verification:

```text
cargo test --release --lib "mode_setattr_" -- --nocapture

6 passed
```

pjdfstest subset:

```text
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh \
  --tests "chmod/00.t" \
  --prove-args "-v"

artifact: docker/compose-pjdfstest/artifacts/run-1781376883-13328
Files=1
Tests=119
Result: PASS
```

Review:

```text
The fix intentionally does not clear SGID on directories, matching Linux and
pjdfstest TODO behavior. It also preserves SGID when the caller is in the file
group, covered by mode_setattr_preserves_sgid_for_owner_inside_file_group.
```

## Final Full Checkpoint

Command:

```bash
cargo fmt --check
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

Result:

```text
cargo fmt --check: PASS

artifact: docker/compose-pjdfstest/artifacts/run-1781377105-9313
Files=238
Tests=8798
Result: PASS
```

Review:

```text
All previously failing pjdfstest files are green:
chmod/00.t, chmod/05.t, chown/05.t, chown/07.t, ftruncate/05.t,
link/06.t, open/05.t, open/06.t, truncate/05.t.

The full suite now passes with Redis metadata backend and the compose
pjdfstest runner.
```

## Review Checkpoints

After each task:

```bash
git diff -- src/meta/store.rs src/meta/client/mod.rs src/vfs/error.rs src/fuse/mod.rs
cargo fmt --check
cargo test <task-specific-tests>
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "<task-specific-pjdfstest-files>"
```

Review questions:

```text
1. Did the new test fail before the implementation?
2. Does the implementation fix the root cause rather than the symptom?
3. Did the selected pjdfstest failures change in the expected direction?
4. Did unrelated failures appear?
5. Is the next task still valid, or did this result change the ordering?
```

## Commit Strategy

Commit after each verified task:

```bash
git add doc/superpowers/plans/2026-06-13-pjdfstest-posix-fixes.md <changed-files>
git commit -m "fix: return enametoolong for oversized names"
git commit -m "fix: refresh parent attrs after namespace changes"
git commit -m "fix: preserve special permission bits"
git commit -m "fix: honor writable create handles for ftruncate"
git commit -m "fix: align utimensat now permission checks"
```

Do not commit special-node work in the same commit as small POSIX semantic fixes.
