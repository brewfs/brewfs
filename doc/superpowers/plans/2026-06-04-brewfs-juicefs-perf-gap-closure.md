# BrewFS JuiceFS Perf Gap Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve BrewFS Redis + S3 large read/write performance and verify every effective change with `docker/compose-xfstests/run_redis_perf.sh`.

**Architecture:** Treat performance work as controlled experiments. Keep the JuiceFS `fa148be` run as the comparison target, run BrewFS with the same FIO tools, change one hypothesis per iteration, and commit only changes that produce a measured improvement without unacceptable regression.

**Tech Stack:** Rust BrewFS, Redis metadata backend, RustFS S3 object backend, Docker Compose perf runners, fio, xfstests `dirperf`/`metaperf`, JSON/TSV result parsing.

---

## Targets

JuiceFS comparison run:
- Artifact: `/mnt/slayerfs/docker/compose-xfstests/artifacts/juicefs-perf-run-1780562982-7892`
- Commit: `fa148be784179a9953e9ece156c700f4c6d5411b`
- Verified command: `bash /mnt/slayerfs/docker/compose-xfstests/run_juicefs_perf.sh --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirperf metaperf"`

Primary targets:

| Tool | JuiceFS target | Current BrewFS best | First milestone | Close-gap target |
| --- | ---: | ---: | ---: | ---: |
| `fio-bigwrite` | 1270 MiB/s | 224 MiB/s | >= 350 MiB/s | >= 760 MiB/s |
| `fio-bigread` | 2387 MiB/s | 1290 MiB/s | >= 1600 MiB/s | >= 1900 MiB/s |
| `fio-seqread` | 2651 MiB/s | 1572 MiB/s | >= 1800 MiB/s | >= 2100 MiB/s |
| `fio-seqwrite` | 205 MiB/s | 89 MiB/s | >= 120 MiB/s | >= 165 MiB/s |

Acceptance rule for each iteration:
- Accept and commit if at least one primary metric improves by >= 15% over the current BrewFS best and no other primary metric regresses by > 10%.
- Reject and revert if the change does not meet the acceptance rule.
- After 3 rejected code-level hypotheses, stop and document the architectural blocker before attempting another code change.

## Files

- Read/modify: `/mnt/slayerfs/src/chunk/store.rs` for block read/range read/cache strategy.
- Read/modify: `/mnt/slayerfs/src/vfs/io/reader.rs` for read session, prefetch submission, and prefetch pressure behavior.
- Read/modify: `/mnt/slayerfs/src/vfs/io/writer.rs` for upload/commit scheduling and writeback behavior.
- Read/modify: `/mnt/slayerfs/src/vfs/cache/write_back.rs` for dirty slice persistence overhead.
- Read/modify: `/mnt/slayerfs/docker/compose-xfstests/run_redis_perf.sh` only if perf runner propagation blocks measurement.
- Read-only comparison: `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/cached_store.go`, `/mnt/slayerfs/brewfs/juicefs/pkg/vfs/reader.go`, `/mnt/slayerfs/brewfs/juicefs/pkg/vfs/writer.go`.

## Task 1: Reproduce Current BrewFS Baseline

- [ ] **Step 1: Run large read/write baseline**

Run:

```bash
cd /mnt/slayerfs/brewfs/brewfs
BREWFS_S3_MAX_CONCURRENCY=32 \
BREWFS_FUSE_WORKERS=8 \
BREWFS_FUSE_MAX_BACKGROUND=256 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite"
```

Expected: all four tools are `pass` in `perf-summary.tsv`.

- [ ] **Step 2: Extract comparable numbers**

Run:

```bash
python3 - <<'PY'
import json, pathlib
root = max(pathlib.Path("/mnt/slayerfs/docker/compose-xfstests/artifacts").glob("perf-run-*"), key=lambda p: p.stat().st_mtime)
for tool in ["fio-bigwrite", "fio-bigread", "fio-seqread", "fio-seqwrite"]:
    data = json.loads((root / "results" / f"{tool}.json").read_text())
    job = data["jobs"][0]
    r = (job.get("read", {}).get("bw_bytes") or 0) / 1048576
    w = (job.get("write", {}).get("bw_bytes") or 0) / 1048576
    print(f"{root.name}\t{tool}\tread={r:.2f} MiB/s\twrite={w:.2f} MiB/s\terror={job.get('error')}")
PY
```

Expected: each `error=0`; record the artifact name.

## Task 2: Config-Only Search Before Code Changes

- [ ] **Step 1: Test no-writeback LZ4/default cache**

Run the Task 1 command with no `BREWFS_COMPRESSION`, no `BREWFS_WRITEBACK_MODE`, and no explicit prefetch/cache env vars.

Expected: this checks the previous best read shape with the fixed runner.

- [ ] **Step 2: Test writeback with restrained prefetch**

Run:

```bash
cd /mnt/slayerfs/brewfs/brewfs
BREWFS_S3_MAX_CONCURRENCY=32 \
BREWFS_FUSE_WORKERS=8 \
BREWFS_FUSE_MAX_BACKGROUND=256 \
BREWFS_COMPRESSION=lz4 \
BREWFS_WRITEBACK_MODE=commit_before_upload \
BREWFS_READ_MEMORY_BYTES=8589934592 \
BREWFS_WRITE_MEMORY_BYTES=1073741824 \
BREWFS_READ_SSD_BYTES=21474836480 \
BREWFS_WRITE_SSD_BYTES=21474836480 \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_PREFETCH_MAX_BYTES=67108864 \
BREWFS_PREFETCH_CONCURRENCY=16 \
BREWFS_MEMORY_BUDGET_BYTES=6442450944 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite"
```

Expected: tests whether prior read regressions were caused by over-aggressive prefetch.

- [ ] **Step 3: Accept only if measured**

If a config-only run beats the current best by the acceptance rule, record it as the recommended perf config. Do not commit code unless code or runner files changed.

## Task 3: Foreground Read Priority

- [ ] **Step 1: Inspect current prefetch pressure**

Read `/mnt/slayerfs/src/vfs/io/reader.rs:136` and `/mnt/slayerfs/src/vfs/fs/mod.rs:278`.

Expected: confirm whether each foreground read can enqueue global prefetch work that competes with subsequent foreground reads.

- [ ] **Step 2: Implement a single gating change**

Change only the prefetch admission rule so large sequential foreground reads do not enqueue more than one block of prefetch while object GETs are already in flight or memory pressure is elevated.

Expected: no write-path code changes in the same iteration.

- [ ] **Step 3: Verify**

Run Task 1 command. Accept only if `fio-bigread` or `fio-seqread` improves by >= 15% and writes do not regress by > 10%.

- [ ] **Step 4: Commit if accepted**

Run:

```bash
git add /mnt/slayerfs/src/vfs/io/reader.rs
git commit -m "perf: gate read prefetch under pressure"
```

## Task 4: Writeback/Staging Cost Reduction

- [ ] **Step 1: Inspect dirty slice fsync cost**

Read `/mnt/slayerfs/src/vfs/cache/write_back.rs:97`.

Expected: confirm whether every staged dirty slice pays file `sync_all` plus parent directory `sync_all`.

- [ ] **Step 2: Implement one durability-mode experiment**

Add a config-gated relaxed staging mode only for perf testing, preserving the existing safe default. The relaxed mode may skip parent directory fsync or batch it.

Expected: safe default unchanged unless the config is explicitly set.

- [ ] **Step 3: Verify**

Run Task 1 command plus `BREWFS_WRITEBACK_MODE=commit_before_upload`. Accept only if `fio-bigwrite` or `fio-seqwrite` improves by >= 15% and reads do not regress by > 10%.

- [ ] **Step 4: Commit if accepted**

Run:

```bash
git add /mnt/slayerfs/src/vfs/cache/write_back.rs
git commit -m "perf: add relaxed writeback staging durability mode"
```

## Task 5: Block Store Read Path

- [ ] **Step 1: Compare with JuiceFS cached store**

Read `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/cached_store.go:97` and `/mnt/slayerfs/src/chunk/store.rs:638`.

Expected: identify whether BrewFS compression unnecessarily disables range/page cache behavior for workloads that store incompressible or uncompressed blocks.

- [ ] **Step 2: Implement one read-path change**

If evidence shows compressed reads are forcing excessive full-block fetches, allow page cache population and reuse after decompressed full-block reads without changing object format.

Expected: object format remains compatible.

- [ ] **Step 3: Verify**

Run Task 1 command. Accept only if `fio-bigread` or `fio-seqread` improves by >= 15%.

- [ ] **Step 4: Commit if accepted**

Run:

```bash
git add /mnt/slayerfs/src/chunk/store.rs
git commit -m "perf: improve cached block read reuse"
```

## Task 6: Loop Control

- [ ] **Step 1: Track attempts**

For every run, record:
- artifact directory
- config/env vars
- changed files
- four primary FIO numbers
- accept/reject decision

- [ ] **Step 2: Revert rejected code**

For rejected code-level changes, use a targeted reverse patch or `git diff` plus `apply_patch`; do not run `git reset --hard`.

- [ ] **Step 3: Stop condition**

Stop when all close-gap targets are met or after 3 consecutive rejected code-level hypotheses. If stopped, document the blocker and next architectural option.
