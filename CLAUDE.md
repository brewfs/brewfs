# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

BrewFS is a high-performance distributed filesystem written in Rust. It exposes a
POSIX interface over FUSE, with pluggable transactional metadata (Redis, TiKV,
etcd, PostgreSQL, SQLite) and S3-compatible or local object data. Correctness,
POSIX behavior, and repeatable performance evidence are first-class requirements
— never accept a perf change on a single focused benchmark.

`AGENTS.md` is the authoritative contributor guide (development discipline, the
full local CI gate, performance acceptance rules, and known POSIX/FUSE
limitations). Read it before making behavior or performance changes; this file
is the shorter orientation map.

## Build, test, lint

Rust 1.85+, edition 2024. Default build enables `fuse-io-uring-runtime`
(Linux FUSE via `asyncfuse` + io_uring). `default-members = ["."]`, so bare
`cargo build` / `cargo test` build only the root `brewfs` crate — pass
`--workspace` (as CI does) to also cover `brewfs-tray` and `brewfs-stats`. CI
sets `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0`.

```bash
# Format / check / build / clippy
cargo fmt --all --check
cargo check --workspace
cargo build --workspace
cargo clippy --workspace

# Test — CI runs the lib+bins suite single-threaded:
cargo test --workspace --lib --bins -- --test-threads=1

# Run one package / one test:
cargo test -p brewfs
cargo test -p brewfs <test_name>

# Feature gates that must keep compiling (CI checks all of these):
cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
cargo check -p brewfs --no-default-features --features fuse-winfsp   # Windows WinFsp

# Workspace also contains: brewfs-tray (desktop/) and brewfs-stats (tools/stats/)
cargo test -p brewfs-tray
```

Benchmarks: `cargo bench` (single `brewfs_bench`, `harness = false`, criterion +
pprof flamegraph). Fuzzing lives in `fuzz/` (separate crate, `cargo-fuzz`).

### Run a mount

```bash
cargo build -p brewfs --release
target/release/brewfs mount /tmp/brewfs-mnt \
  --data-backend local-fs --data-dir /tmp/brewfs-data \
  --meta-backend sqlx --meta-url sqlite:///tmp/brewfs-meta.db
```

CLI subcommands: `mount`, `gc`, `info`, `unmount`, `console`, `object-put-bench`.
`gc`/`info`/`unmount` talk to a running daemon over its control-plane Unix
socket. Config can come from a YAML file plus CLI overrides (see
`src/config.rs` and `examples/mount-config.*.yaml`).

### POSIX / filesystem correctness (Docker Compose)

These are the acceptance gates for filesystem behavior; they need Docker and
`/dev/fuse`:

```bash
bash docker/compose-xfstests/run_redis_xfstests.sh --cases "generic/001"
bash docker/compose-xfstests/run_redis_pjdfstest.sh
bash docker/compose-xfstests/run_redis_ltp.sh
# TiKV / etcd / sqlite variants exist as run_{tikv,etcd,sqlite}_{xfstests,ltp,pjdfstest}.sh
```

### Performance acceptance

Use the Compose runners as the baseline; `tools/perf/` is only for diagnosing a
gap the runner already exposed. Treat `fio-randrw` as a hard gate and watch for
wins that merely shift cost from fio runtime into close/flush/drain. Full
acceptance criteria and the focused/broad fio commands are in `AGENTS.md`; the
current baseline numbers and reproduction live in `README.md` ("Performance vs
JuiceFS") and `doc/performance/`.

## Architecture

Six layers, top to bottom (see `doc/architecture/arch.md` for the full write-up
including read/write data-flow diagrams):

```
CLI/Daemon (main.rs)  →  fuse/  →  vfs/  →  meta/  →  chunk/  →  cadapter/
```

- **`src/main.rs`, `src/config.rs`** — entry point, CLI + YAML config, tracing,
  MetaStore/BlockStore construction, FUSE mount, signal/shutdown handling.
  `src/lib.rs` re-exports the public SDK surface (`Client`, `VFS`, stores, …).
- **`src/fuse/`** — FUSE protocol via `asyncfuse`; translates requests into VFS
  calls (`mod.rs`), privileged vs unprivileged mount (`mount.rs`). Records
  per-op stats to a `.stats` virtual file read by `brewfs-stats`.
- **`src/vfs/`** — POSIX semantics core. `fs/mod.rs` is the `VFS` entry for all
  ops; `io/reader.rs` / `io/writer.rs` are the read/write paths (readahead,
  slice state machine); `cache/` is page/read/write-back/prefetch caching;
  `memory.rs` enforces the global memory budget; `handles.rs`, `inode.rs`,
  `meta_ops.rs`, `stats.rs`.
- **`src/meta/`** — metadata. `store.rs` defines the `MetaStore` trait (70+
  ops). `client/` adds caching (inode cache, path trie, sessions) on top;
  `layer.rs` (`MetaLayer`) is what the VFS talks to. `stores/` has the four
  backends: `database` (SeaORM → SQLite/Postgres), `redis` (Version + Lua CAS),
  `etcd`, `tikv`. `entities/` = SeaORM schema, `migrations.rs`, `factory.rs`.
- **`src/chunk/`** — JuiceFS-style Chunk→Block layout (default 64 MiB chunk /
  4 MiB block). `layout.rs`, `slice.rs` (a write becomes a `SliceDesc`),
  `writer.rs` (upload), `reader.rs` (locate + reassemble), `store.rs`
  (`BlockStore` trait + `ObjectBlockStore`), `cache.rs` (`ChunksCache` hot-mem /
  cold-disk), `compact/` (compaction + GC).
- **`src/cadapter/`** — object backends behind the `ObjectBackend` trait:
  `localfs.rs` (dir-as-object-store) and `s3.rs` (S3-compatible: multipart,
  path-style, checksum control).

**Write path:** FUSE write → `VFS::write` → `FileWriter` splits by chunk →
appends to a `SliceState` → flush uploads blocks concurrently via `BlockStore`
→ `commit_chunk` → `MetaLayer::append_slice` (becomes visible to reads).
**Read path:** FUSE read → `FileReader` → `DataFetcher` loads `SliceDesc`s from
metadata → scans newest-first to find which blocks to read → concurrent
`BlockStore::read_range`, holes filled with zeros.

**Process model:** single daemon process containing the FUSE dispatch loop,
MetaClient (cache + session heartbeat), VFS (reader/writer caches), background
compaction/GC workers, and the control-plane server. Multi-process mounts share
the metadata backend and object store; consistency is close-to-open plus global
locks for compaction.

### Other workspace crates

- **`desktop/` (`brewfs-tray`)** — Slint 1.17 system-tray mount manager
  (Windows/macOS). Can also drive `ossmount` (`src/bin/ossmount.rs`), an
  OSS-direct mode that mounts an S3/OSS bucket straight to a drive/dir without
  BrewFS metadata.
- **`tools/stats/` (`brewfs-stats`)** — real-time TUI stats viewer (like
  `juicefs stats`) reading the FUSE `.stats` file.
- **`web/console/`** — web console assets.

## Conventions

- `MetaClient`/`MetaLayer` and `ObjectClient<B>` are generic over the store /
  backend traits — add a new metadata or object backend by implementing the
  trait in `src/meta/stores/` or `src/cadapter/`, then wiring it into the
  factory and `create_meta_store`/`mount_cmd` in `src/main.rs`.
- Integration tests in `tests/` exercise the public design contracts via the
  exposed `meta`/`vfs` modules (some need Docker services). FUSE-behavior tests
  go through the Compose runners, not plain `cargo test`.
- Documentation is canonical under `doc/` (architecture, operations, testing,
  performance). `doc/superpowers/plans/` holds dated historical execution plans
  — treat as history, prefer a new dated plan over rewriting old ones.
- Keep known FUSE/POSIX exclusions intact (e.g. xfstests `generic/075`, LTP
  `iogen01`) — they are deliberately excluded with documented evidence in
  `AGENTS.md`; don't re-enable without the re-validation described there.
