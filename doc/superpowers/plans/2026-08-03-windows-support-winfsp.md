# BrewFS Windows Support (WinFsp Adapter) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add complete native Windows support to BrewFS so `brewfs mount` works on Windows through WinFsp while keeping the existing Linux FUSE path fully intact.

**Architecture (chosen Option A):** Add a new WinFsp adapter module (e.g. `src/fuse/winfsp/`) that translates WinFsp native filesystem callbacks into the existing VFS layer (`src/vfs/`). Linux/macOS keep using asyncfuse (fuse3-derived, `/dev/fuse` + fusermount3). Shared data/metadata layers (chunk, meta, cadapter, SDK) are cross-platform already and only need small `#[cfg]` fixes.

**Tech Stack:** Rust stable, WinFsp (winfsp-rs / winfsp-sys), tokio runtime feature (`fuse-tokio-runtime`) on Windows, GitHub Actions `windows-latest` CI job, existing pjdfstest/xfstests knowledge applied via WSL cross-validation.

## Why Option A

- Linux FUSE path (asyncfuse + io_uring, all existing perf/correctness baselines) is untouched.
- WinFsp native API is the officially supported Windows filesystem interface; a FUSE-compatibility layer is not required.
- Failure is contained: a Windows regression cannot break the Linux baseline.

## Current Linux-only barriers (from 2026-08-03 reconnaissance)

- `src/fuse/mount.rs`: `mount_vfs_unprivileged` / `mount_vfs_privileged` are `#[cfg(target_os = "linux")]`; `src/main.rs` imports them unconditionally (lines ~53/690/692) -> does not compile on Windows.
- Default feature `fuse-io-uring-runtime` (io_uring is Linux-only) -> Windows must use `fuse-tokio-runtime` or `fuse-async-io-runtime`.
- `src/cadapter/localfs.rs` uses `std::os::unix::fs::FileExt` (pread/pwrite) -> needs Windows `seek_read`/`seek_write` gate.
- `src/control/runtime.rs` + `control/protocol.rs`: Unix domain sockets -> Windows AF_UNIX (Rust std) or named pipes.
- `src/main.rs` `#[cfg(unix)]` `raise_nofile_limit` (no-op on Windows).
- FUSE-specific POSIX semantics (symlink, permissions, rename, mmap, locks) need a WinFsp semantic mapping.

## Status (2026-08-03, native Windows x86_64-msvc)

Phase 1 core is DONE and verified locally on Windows:
- `cargo check --no-default-features` green (lib + bin + workspace).
- `cargo test --no-default-features --lib --bins` green: 473 lib + 514 bin tests passed, 0 failed.
- `cargo fmt --all --check` and `git diff --check` green.
- `cargo clippy --no-default-features --lib --bins` green (only fuse-only dead-code warnings).
- Linux CI gate still must be re-validated in CI (asyncfuse/io_uring are Unix-only and cannot build here).

Phase 2 (WinFsp adapter) is DONE at the compile/test level and verified locally:
- Added `winfsp` (0.12.2+winfsp-2.1, delayload + async-io) as a Windows-only dependency; it
  compiles without WinFsp installed (winfsp-sys ships a built-in import library).
- New `fuse-winfsp` feature (pure code gate; winfsp dep is non-optional on Windows).
- New `src/winfsp/mod.rs` adapter: `FileSystemContext` + `AsyncFileSystemContext` wired to the
  VFS (open/create/read/write/readdir/rename/delete/truncate/flush/volume-info/times). Note:
  the module lives at `src/winfsp/` (not `src/fuse/winfsp/`) because `src/fuse/` is gated on
  the asyncfuse `fuse` feature which cannot compile on Windows.
- `brewfs mount` on Windows (`--no-default-features --features fuse-winfsp`) routes through
  the WinFsp adapter; Linux/macOS keep the asyncfuse path byte-for-byte unchanged.
- Sync callbacks bridge to the tokio VFS via `Handle::block_on` (validated with a standalone
  probe: works from a non-tokio thread, including tokio Mutex + timers).
- Tests: `cargo test --no-default-features --features fuse-winfsp --lib --bins` green
  (481 lib + 522 bin; +8 new pure-function winfsp unit tests). `cargo fmt --all --check`,
  `git diff --check`, and `cargo clippy --no-default-features --features fuse-winfsp --lib --bins`
  green. Phase 1 baseline (`cargo test --no-default-features --lib --bins`) stays green at
  473 lib + 514 bin.
- **Real WinFsp mount VERIFIED on this machine (WinFsp 2.1.25156 installed, non-admin OK):**
  - Drive-letter mount `Z:` works; folder mount works on NTFS (`C:\brewfs-mnt-test`).
    exFAT volumes (`E:`) reject folder mounts (no junction/reparse support) — documented.
  - File I/O verified: create/read/rename/delete/list + nested dirs all work through the mount
    (`cmd dir Z:\*`, Python, PowerShell Set-Content/Get-Content/Rename-Item).
  - Windows sees the drive-letter mount as a LOCAL disk (Win32_LogicalDisk DriveType=3), i.e.
    it shows under 此电脑→设备和驱动器, not 网络位置.
  - Known cosmetics: PowerShell `Remove-Item` on an empty dir and `cmd dir /b Z:\` (trailing
    backslash) report errors; Python/cmd alternatives work. Documented in windows-winfsp.md.

**Bugs found & fixed during real-mount bring-up (2026-08-03):**
1. `FspFileSystemCreate` 0xD000000D: `VolumeParams::prefix("BrewFS")` is invalid for
   WinFsp.Net (prefix must be `\Server\Share`). Removed the prefix entirely — folder and
   drive mounts both use WinFsp.Disk.
2. `FspFileSystemSetMountPoint` 0xD0000035: mount CLI pre-created the mount directory, but
   WinFsp's `FspMountSet_Directory` uses FILE_CREATE disposition and fails if it exists.
   `validate_mount_point` now skips creation for WinFsp (folder mounts must not pre-exist).
3. Create never reached the VFS: `VfsError::NotFound` mapped to ERROR_PATH_NOT_FOUND (3),
   but WinFsp's OpenIf/OverwriteIf fallback only retries create on STATUS_OBJECT_NAME_NOT_FOUND
   (ERROR_FILE_NOT_FOUND=2). Fixed the mapping.
4. Default meta `sqlite::memory:` is broken with sqlx pools (each connection gets a unique
   in-memory DB → "no such table: file_meta/access_meta" → I/O device errors on dir listing).
   Default changed to file-backed `sqlite://./data/brewfs-meta.db?mode=rwc`; sqlite file
   parent dir is auto-created on connect.

**Final local gate (2026-08-03, before commit):**
- `cargo test --no-default-features --features fuse-winfsp --lib --bins`: 522 passed, 0 failed.
- `cargo test --no-default-features --lib --bins`: 515 passed, 0 failed.
- `cargo fmt --all --check`, `git diff --check`, `cargo clippy --no-default-features --features fuse-winfsp --lib --bins`: green.
- Linux CI gate must be re-validated in GitHub Actions (asyncfuse/io_uring are Unix-only).

Key changes so far:
- `Cargo.toml`: `asyncfuse` is now optional behind a new `fuse` feature; `fuse-io-uring-runtime`/`fuse-tokio-runtime` imply `fuse`. `qlean` + `pprof` dev-deps moved to `[target.'cfg(unix)'.dev-dependencies]`.
- `lib.rs`/`main.rs`: `mod fuse` gated by `feature = "fuse"`; mount-only imports/functions gated; non-fuse `mount` subcommand returns a clear error.
- `vfs/fs/mod.rs`: no-op `FuseNotify` stub on non-fuse builds.
- `control/client.rs` + `control/server.rs`: Windows stubs (bail) until a named-pipe transport exists (Phase 3).
- `cadapter/localfs.rs`: portable `pread` (unix `read_at` / windows `seek_read`).
- `meta/file_lock.rs` + `database/mod.rs`: explicit portable constants for fcntl lock / xattr flags.
- `vfs/cache/write_back.rs`: fallocate preallocation and directory fsync gated to Unix (Windows dir `File::open`/`sync_all` -> access denied).
- `console/mod.rs`, `fs.rs`, `utils/num.rs` (test-only `makedev`): portable uid/gid/rdev helpers.
- Tests that require the Unix control plane, qlean QEMU VMs, or the FUSE session are gated `#[cfg(unix)]` / `#[cfg(feature = "fuse")]`.

## Phase 1 — Make the core library compile on Windows

- [x] Run `cargo check --workspace --no-default-features --features fuse-tokio-runtime` on Windows and capture the full error list.
- [x] Fix `cadapter/localfs.rs` FileExt portability (`#[cfg(unix)]` pread/pwrite vs Windows seek_read/seek_write).
- [x] Gate/port control-plane Unix sockets (Windows stubs for now; named-pipe transport is Phase 3).
- [x] Feature-gate the Linux-only mount command path in `main.rs` (`fuse` feature + non-fuse fallback error).
- [x] Verify `cargo check` / `cargo test --lib --bins` (SDK + data/meta layers) pass on Windows (473 lib + 514 bin tests green).
- [ ] Acceptance (pending): Linux CI re-validation of `cargo check --workspace --no-default-features --features fuse-tokio-runtime`; SDK demo run on Windows.
- [x] Windows build command verified locally: `cargo check/test --no-default-features --features fuse-winfsp` (WinFsp not required to compile).

## Phase 2 — WinFsp mount adapter (core)

- [x] Add `winfsp`/`winfsp-sys` as a Windows-only dependency behind the `fuse-winfsp` feature.
- [x] Implement `src/winfsp/mod.rs`: filesystem callbacks -> `VFS` operations (open/create/read/write/readdir/rename/delete/truncate/flush/volume-info/times) via `FileSystemContext` + `AsyncFileSystemContext`.
- [x] Map WinFsp semantics: handles/IRP model, file attributes, DOS name rules, rename/overwrite semantics, delete-on-close via set_delete + cleanup.
- [x] Wire `brewfs mount` on Windows to the WinFsp adapter; `--data-backend`/meta/S3 config identical (shared `mount_with_store`).
- [ ] Runtime validation on a WinFsp-equipped machine: install WinFsp 2.x, mount a drive/folder, run fsx/winfsp-tests, basic read/write/rename/delete smoke.
- [ ] Symlink/reparse-point and ACL mapping (currently `persistent_acls=false`, `reparse_points=false`; symlinks surface as reparse-point files without resolution).

## Phase 3 — Cross-platform data/meta hardening

- [x] Control plane on Windows: named-pipe transport (`\\.\pipe\brewfs-<pid>`) in
  `src/control/pipe.rs` + `client.rs`/`server.rs`; Unix domain sockets untouched.
  Length-prefixed JSON framing (named pipes have no half-close). Server uses a
  fresh pipe instance per connection to avoid the tokio disconnect/connect
  reconnect race (ERROR_PIPE_NOT_CONNECTED). Roundtrip test in `control/tests.rs`.
- [ ] Audit `src/chunk`, `src/meta`, `src/cadapter` for remaining `#[cfg(unix)]` leaks.
- [ ] Confirm Redis/TiKV/etcd/Postgres + S3 (incl. Aliyun OSS) backends work on Windows.
- [x] CI: `windows-latest` job in `.github/workflows/ci.yml` (fmt, check, test, clippy with `fuse-winfsp`).

## Phase 4 — Validation

- [ ] Re-run Linux CI gate (AGENTS.md) after every accepted change; no Linux regression.
- [ ] WSL cross-check: mount the WinFsp drive from WSL and run a reduced xfstests/pjdfstest subset.
- [x] Windows install/mount guide: `doc/operations/windows-winfsp.md` (中文), linked from
  `doc/operations/README.md` and `doc/README.md`; `doc/operations/control-plane.md` updated
  with the Windows named-pipe transport.

## Guardrails

- Every accepted code change passes the local CI gate from `.github/workflows/ci.yml` before commit.
- Do not change Linux FUSE behavior while adding the WinFsp adapter; keep the two paths independent behind `#[cfg]`/features.
- The Aliyun OSS credentials are stored only in `.local/oss-credentials.env` (git-ignored) and must be rotated before production use.
