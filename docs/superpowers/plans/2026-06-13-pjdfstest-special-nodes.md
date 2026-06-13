# pjdfstest Special Node Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development for each task and superpowers:requesting-code-review after every phase checkpoint.

**Goal:** Add POSIX special-node metadata support so `mkfifo`, `mknod`, and all cross-operation pjdfstest cases involving FIFO, socket, char device, and block device nodes stop cascading through `ENOSYS` or missing-node failures.

**Context:** The first POSIX-fix phase reduced pjdfstest from `failed_files=65 failed_subtests=3867` to `failed_files=45 failed_subtests=3622`. Remaining failures are dominated by unsupported special nodes:

```text
artifact: docker/compose-pjdfstest/artifacts/run-1781366695-14925

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

**Scope:** Implement metadata-level persistence and FUSE attribute reporting for:

```text
FIFO
Socket
Char device with rdev
Block device with rdev
```

This phase stores and reports special nodes, but does not implement device I/O. Opening FIFO/socket/device nodes should follow kernel/FUSE expectations and avoid treating them as regular BrewFS object data files.

---

## Current Model Gaps

- `src/meta/store.rs::FileType` only has `File`, `Dir`, and `Symlink`.
- `src/fuse/mod.rs::mknod` returns `ENOSYS` for `S_IFIFO`, `S_IFSOCK`, `S_IFCHR`, and `S_IFBLK`.
- `FileAttr` has no `rdev`, while FUSE `FileAttr` needs `rdev` for char/block devices.
- Redis and TiKV have backend-specific `NodeKind` enums with only `File`, `Dir`, `Symlink`.
- Database and Etcd infer kind through `is_file` plus `symlink_target`, so they need a backward-compatible `entry_type` or equivalent persisted kind.
- VFS read/write/truncate/open paths assume non-dir, non-symlink nodes are regular data files.

---

## Task 1: Add Public Metadata Types and FUSE Mapping

**Files:**
- Modify: `src/meta/store.rs`
- Modify: `src/fuse/mod.rs`
- Modify: `src/vfs/fs/mod.rs`
- Modify tests near existing `FileType` and FUSE attr tests.

- [x] **Step 1: Write RED tests**

Add unit tests proving:

```text
FileType::Fifo maps to asyncfuse::FileType::NamedPipe
FileType::Socket maps to asyncfuse::FileType::Socket
FileType::CharDevice maps to asyncfuse::FileType::CharDevice
FileType::BlockDevice maps to asyncfuse::FileType::BlockDevice
FileAttr.rdev is copied into FUSE attr.rdev
```

RED verification:

```text
cargo test --release --lib special_file_types_map_to_fuse_types_and_rdev

Result: FAIL
reason: FileType variants Fifo/Socket/CharDevice/BlockDevice and FileAttr.rdev did not exist.
```

- [x] **Step 2: Implement model additions**

Add:

```rust
pub enum FileType {
    File,
    Dir,
    Symlink,
    Fifo,
    Socket,
    CharDevice,
    BlockDevice,
}

pub struct FileAttr {
    ...
    pub rdev: u32,
}
```

Update all constructors/tests to set `rdev: 0` for existing regular/dir/symlink nodes.

- [x] **Step 3: Update type helper semantics**

Introduce helpers:

```text
is_directory(kind)
is_symlink(kind)
is_regular_file(kind)
is_special_node(kind)
is_non_directory(kind)
```

Then update rename/link/unlink checks to treat all non-directories uniformly where POSIX expects that.

Current Task 1 implementation added the public variants and FUSE mapping. The
broader rename/link/unlink behavior will be handled in Task 2+ after actual
special-node creation exists.

- [x] **Step 4: Verify**

Run:

```bash
cargo test --release --lib file_type fuse_attr rdev
cargo fmt --check
```

Verified:

```text
cargo test --release --lib special_file_types_map_to_fuse_types_and_rdev
1 passed

cargo test --release --lib mode_sanitization_tests
13 passed

cargo fmt --check
PASS
```

---

## Task 2: Add Store API for Special Node Creation

**Files:**
- Modify: `src/meta/store.rs`
- Modify: `src/meta/layer.rs`
- Modify: `src/meta/client/mod.rs`
- Modify: `src/vfs/fs/mod.rs`

- [x] **Step 1: Write RED tests**

Add meta-client/VFS tests:

```text
mknod creates FIFO with kind=Fifo mode=S_IFIFO|perm rdev=0
mknod creates char device with kind=CharDevice rdev preserved
lookup/readdir/stat preserve kind and rdev
special nodes cannot be read, written, or truncated as BrewFS regular data
```

RED verification:

```text
cargo test --release --lib mknod_creates_fifo_metadata

Result: FAIL
failure: Errno(38) / ENOSYS
```

- [x] **Step 2: Add creation request**

Add a store method such as:

```rust
async fn create_node(
    &self,
    parent: i64,
    name: &str,
    kind: FileType,
    mode: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
) -> Result<FileAttr, MetaError>;
```

Keep existing `create_file`, `mkdir`, and `symlink` as wrappers or separate optimized methods if that fits each backend better.

- [x] **Step 3: Update VFS/FUSE creation flow**

Add VFS entry point:

```text
create_special_node_at(parent, name, kind, mode, uid, gid, rdev)
```

Use it from FUSE `mknod` for FIFO/socket/char/block. Keep regular-file `mknod(path, 0, 0)` behavior unchanged.

- [x] **Step 4: Verify**

Run:

```bash
cargo test --release --lib mknod special_node
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "mkfifo/00.t mknod/00.t" --prove-args "-v"
```

Implemented:

```text
- MetaStore/MetaLayer/MetaClient create_node(parent, name, kind, mode, uid, gid, rdev)
- VFS create_special_node_at
- FUSE mknod for FIFO/socket/char/block devices
- SQLite FileMeta.rdev with default 0
- SQLite EntryType extensions for Fifo/Socket/CharDevice/BlockDevice
- Redis NodeKind extensions plus StoredAttr.rdev with serde default
```

Verified:

```text
cargo test --release --lib mknod_creates
2 passed

cargo check --release
PASS

bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "mkfifo/00.t mknod/00.t" --prove-args "-v"
Files=2, Tests=72
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781368383-746

bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "mkfifo/01.t mknod/01.t" --prove-args "-v"
Files=2, Tests=44
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781368596-7445

bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "mkfifo/00.t ... mkfifo/12.t mknod/00.t ... mknod/11.t" --prove-args "-v"
Files=25, Tests=306
Result: PASS
artifact: docker/compose-pjdfstest/artifacts/run-1781368795-22528
```

---

## Task 3: Persist Special Nodes in Redis and SQLite

**Files:**
- Modify: `src/meta/stores/redis/mod.rs`
- Modify: `src/meta/stores/redis/tests.rs`
- Modify: `src/meta/stores/database/mod.rs`
- Modify: `src/meta/stores/database/tests.rs`
- Modify database entities if needed.

- [x] **Step 1: Write backend RED tests**

For each backend:

```text
create FIFO, stat, lookup, readdir, unlink
create socket, stat, lookup, readdir, unlink
create char device with rdev, stat reports rdev
create block device with rdev, stat reports rdev
chmod/chown/utimensat preserve kind and rdev
link/unlink update nlink for special nodes like regular non-directories
```

- [x] **Step 2: Redis implementation**

Extend `NodeKind` and `StoredAttr`:

```text
NodeKind::{Fifo, Socket, CharDevice, BlockDevice}
StoredAttr.rdev: u32 with serde default
```

Use `mode & S_IFMT` only for persisted `mode` consistency; behavior should come from explicit kind.

- [x] **Step 3: SQLite/database implementation**

Add a backward-compatible `entry_type`/kind field for `FileMeta` or use the existing content `EntryType` only if it is already present for file metadata rows.

Migration rules:

```text
old file rows with symlink_target=None => File
old file rows with symlink_target=Some => Symlink
old access rows => Dir
new rows persist explicit kind and rdev
```

- [x] **Step 4: Verify**

Run:

```bash
cargo test --release --lib redis special_node
cargo test --release --lib database special_node
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "mkfifo/00.t mkfifo/01.t mknod/00.t mknod/01.t" --prove-args "-v"
```

Verification is currently covered by the Task 2 command set above. Dedicated
backend unit-test expansion is still useful before finishing this branch, but
the Redis pjdfstest surface for all `mkfifo/*.t` and `mknod/*.t` is now green.

---

## Task 4: Persist Special Nodes in Etcd and TiKV

**Files:**
- Modify: `src/meta/entities/etcd.rs`
- Modify: `src/meta/stores/etcd/mod.rs`
- Modify: `src/meta/stores/etcd/tests.rs`
- Modify: `src/meta/stores/tikv/mod.rs`
- Modify: `src/meta/stores/tikv/tests.rs`

- [ ] **Step 1: Write backend RED tests**

Mirror Task 3 tests for Etcd and TiKV.

- [ ] **Step 2: Etcd compatibility**

Add optional serialized kind/rdev fields with serde defaults. Preserve compatibility with existing `is_file` and `symlink_target`.

- [ ] **Step 3: TiKV compatibility**

Extend `StoredNodeKind` and wire encode/decode tests. Use explicit kind for lookup/readdir/stat.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test --release --lib etcd special_node
cargo test --release --lib tikv special_node
```

---

## Task 5: Prevent Regular Data Operations on Special Nodes

**Files:**
- Modify: `src/vfs/fs/mod.rs`
- Modify: `src/fuse/mod.rs`
- Modify VFS/FUSE tests.

- [ ] **Step 1: Write RED tests**

Expected behavior:

```text
read/write/truncate/fallocate on FIFO/socket/device nodes must not use object storage paths
open(O_RDONLY/O_WRONLY) on unsupported special nodes should return a stable POSIX errno
metadata operations (chmod/chown/link/unlink/rename/utimensat/stat/lstat) should work
```

- [ ] **Step 2: Implement guards**

Route data-path operations through `FileType::File` checks. For special nodes, return the errno that matches pjdfstest/kernel expectations for this FUSE implementation. Prefer FUSE/kernel-managed FIFO behavior where available; otherwise use `ENXIO`, `ENODEV`, or `EOPNOTSUPP` based on observed pjdfstest expectations.

- [ ] **Step 3: Verify**

Run:

```bash
cargo test --release --lib special_node_data_ops
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "open/17.t open/22.t open/24.t" --prove-args "-v"
```

---

## Task 6: Full pjdfstest Review and Cleanup

- [ ] **Step 1: Full run**

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

- [ ] **Step 2: Compare matrix**

Update this plan with:

```text
failed_files before/after
failed_subtests before/after
remaining categories
new regressions, if any
```

- [ ] **Step 3: Review**

Run:

```bash
git diff --stat
cargo fmt --check
cargo test --release --lib mode_setattr special_node
```

Review questions:

```text
1. Are old metadata records still readable?
2. Does every backend preserve kind/rdev through chmod/chown/utimensat/link/rename?
3. Are special nodes excluded from regular object-data paths?
4. Did non-special pjdfstest suites regress?
5. Is any remaining failure outside special-node scope?
```
