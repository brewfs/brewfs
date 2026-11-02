# Sequential small-file packs (opt-b)

Branch: `perf/packed-smallfile-bundle-r2`, based on `main@1bd4d75`.
The historical `2be1c16` WIP is not the implementation or acceptance baseline.

## Measurement and pre-edit attribution

8,192 deterministic 4 KiB files, one lexicographic reader, full file reads
including stat/open/read/close and directory discovery. wire005,
sequential-small-file, inline on, size-only frames, raw metadata/data codecs,
256 MiB configured metadata cache, payload caches disabled, direct reads,
keep-cache disabled, no background payload prefetch.

The repository Compose runner performs explicit `packed-tree` prewarming
before `packed-smallfiles`, using three fresh mounts with no concurrent Rust
build. Current-main warm median: 2.973841 ms/file, 1.313537 MiB/s,
0.801758 backend GET/file, 201,822.98 received bytes/file. Prewarm takes
0.395575--0.402641 seconds. Artifact basename:
`main-20261102T121234Z-63638` under `/data/out/pack-compose-artifacts`.

Host diagnostic artifact: `pack-baseline-main-20261102T115247Z-38323`
under `/data/out`. Cold: 3.116042 ms/file; warm: 3.099142 ms/file.
Warm has 6,400 payload GETs and 126 GroupMeta GETs. Only 1,792 files
(7,340,032 bytes) are inline; 6,400 spill into 128 ordinary frames. Packed
format alone therefore does not imply a complete small-file bundle.

`BREWFS_REQUEST_TRACE=true`, sample rate 307. The 256-record ring retained
the tail of 320 sampled requests (43 READs). Exclusive durations partition
sampled FUSE time, not scan wall time. Estimated ms/file expands sampled
means by the full request count (98,376 / 8,192).

| Segment | Estimated ms/file | Sampled FUSE time |
| --- | ---: | ---: |
| queue_wait | 0.085551 | 3.188% |
| file_lock | 0.005250 | 0.196% |
| chunk_lock | 0 | 0% |
| dirty_merge | 0 | 0% |
| flush | 0.010945 | 0.408% |
| metadata_rpc | 0 | 0% |
| metadata_commit | 0 | 0% |
| object_put_queue | 0 | 0% |
| object_put | 0 | 0% |
| reply | 0.225869 | 8.418% |
| handler_residual | 2.181788 | 81.312% |
| handler_poll | 0.173815 | 6.478% |

The ten requested stages omit object GET wait. Direct packed metadata also
bypasses MetaClient RPC timing; zero metadata_rpc is not zero metadata object
I/O. Nested payload ledger views show 10.004 seconds backend-body,
10.286 seconds validated-fetch and 19.509 seconds logical-read lifetime;
these must not be added. GroupMeta authentication is 0.164 seconds and decode
0.018 seconds, so this evidence does not support a SHA optimization.

Attribution was published at 19:54:09 Asia/Shanghai, nine minutes after the
two-hour deadline. No source edits preceded it. The delay was the matching
observability release build. The six-hour task window ends around 23:45.

## Hypothesis and scope

Close sequential groups before eligible dense payloads overflow the existing
224 KiB inline budget. Reuse the retained GroupMeta cache and its ownership,
singleflight, identity validation and fencing. Reserve up to configured
metadata / 4, capped at 64 MiB and mount metadata budget / 4, for sequential
GroupMeta. Directory traversal populates this same cache, making a complete
inline group the prewarm unit. Keep one per-directory group owner for paging
when retention is unavailable; fence that owner by generation and epoch.

The importer and deterministic fixture use the same group boundary policy.
Repacking is required to receive the new layout. Existing snapshots retain
their authenticated layout and remain readable. Random/mixed frame targets,
codec choices and collection delays are unchanged.

Three tests first failed on main: the production importer inlined only
229,376 of 491,520 bytes; an 8.75 MiB inline corpus fetched all 40 groups on
its second scan; directory traversal also left 40 GETs for the next scan.
All three passed after the production change (3 passed, 0 failed).

A real non-root CLI smoke check caught a pre-existing ignored test helper
using the old generic envelope for WorkspaceRecord. The helper now recognizes
the current `BWSWR002` workspace schema, while leases/IDs still require
`BWSKV001`. No production metadata encoding was changed. The environment also
needed `user_allow_other` for the CLI's normal FUSE mount options.

## Validation and disposition

The three-round Compose candidate is 0.575641 ms/file, 6.785916 MiB/s and
0.001099 GET/file, versus main 2.973841 ms/file, 1.313537 MiB/s and
0.801758 GET/file. Latency falls 80.64%, GET/file falls 99.86%. Charging
prewarm as well gives a median 5.318266 seconds (0.649202 ms/file), with
159 total GETs / 8192 files. Fresh host cold cost falls from 3.018973 to
0.692314 ms/file (-77.07%). Full corpus SHA-256 and fsync/close → SIGKILL →
original-PVC recovery/remount SHA-256 pass with zero mismatches.

This remains an unaccepted draft. Metadata ownership increases 24.272 MiB;
median peak RSS increases 22.070 MiB. A fresh random 4 KiB read fetches
535,889 B (130.83×, or 57.07× for the GroupMeta alone), failing the ≤2×
anti-metric. Matched stat median/p99 regress 6.85%/11.65%, and the already
inline first-file read regresses 8.90%; the operation anti-metrics do not all
pass. Readdir improves 95.84%, last-file read improves 87.26%, fsynced write
is +1.21%, rename improves 13.56%. Only one matched guard cohort was run.

Default workspace lib/bin passes (1318 passed, 0 failed, 264 ignored).
Full overlay/observability including integration and doctests passes (2857
passed, 0 failed, 502 ignored across 32 result rows); the library subset is
2746 passed / 477 ignored. fmt, diff and all shell gates pass. The two-job
overlay compile was OOM-killed; the one-job retry completed. Strict clippy,
check/build/runtime gates and mixed fio rounds are pending final closeout.

The extra denied-PUT `real_redis_cli_pending_upload_kill_pvc_recovery` test
FAILED before its crash step: no persisted pending-upload PVC slice appeared
within 90 seconds (exit 101). Logs are preserved; this is not a recovery pass.
The 8192-file durable-close recovery and separate two-file recovered
publication both pass. No merge is authorized by the measured state while
these failures/gaps remain.

The implementation is a V1-derived bounded inline-group prewarm. V2's
no-prefetch range merger and V3's 1 MiB inline budget were not implemented or
measured before the stop-new-variants instruction; neither was disproved or
declared unnecessary. No change was made to a README comparison table.

The crash fixture writes and fsyncs/closes every 4 KiB file, kills the actual
production CLI, recovers the original PVC after lease expiry and remounts
through the real parser. It verifies all files against individual SHA-256
digests. Existing two-file tests retain recovered publication coverage; the
8,192-file case verifies recovery/remount without invoking the fixture's
512-row bounded publication helper.

Full machine-local report and artifact paths:
`/data/out/REPORT-PACK-SMALLFILE.md`. Performance acceptance requires warm
<=1 ms/file, <=0.05 GET/file and >=3 MiB/s; cold regression <=10%; read/write,
stat/readdir/rename regression <=3%; zero content or correctness failures.
Only a fully validated result can become ready and merge.

## Reproduction contract

Build both arms with `CARGO_TARGET_DIR=/data/target`, `CARGO_BUILD_JOBS=2`,
`CARGO_INCREMENTAL=0` and `CARGO_PROFILE_DEV_DEBUG=0`, in tmux without a shell
timeout. Preserve the main executable and fixture before building the
candidate. The matching performance build command is:

```bash
cargo rustc --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,observability \
  --bin brewfs -- -C opt-level=1 -C debuginfo=0
```

Use the same command with `--bin packed_v3_snapshot_fixture` for the generated
corpus. Publish to the repository Compose RustFS service:

```bash
packed_v3_snapshot_fixture --wire-version 5 --frame-policy size-only \
  --inline-data on --metadata-codec raw --data-codec raw \
  --access-profile sequential-small-file \
  --bucket brewfs-data --endpoint http://127.0.0.1:19000 \
  --region us-east-1 --force-path-style true --prefix "$run_id" \
  --manifest-output "$artifact/manifest-key.txt" \
  --dir-levels 0 --dirs-per-level 1 --files-per-dir 8192 \
  --small-file-size 4096 --small-file-min-size 4096 --small-file-max-size 4096
```

Run the original `run_perf_in_container.sh` using
`docker/compose-xfstests/docker-compose.redis-perf.yml` and a runtime override
mounting each preserved executable at `/usr/local/bin/brewfs`. The override
sets these explicit values (same on both arms):

```text
BREWFS_VOLUME_FORMAT=packed-metadata-v3
BREWFS_PACKED_METADATA_CACHE_BYTES=268435456
BREWFS_READ_MEMORY_BYTES=0
BREWFS_READ_SSD_BYTES=0
BREWFS_PREFETCH_ENABLED=false
BREWFS_RANGE_BACKGROUND_PREFETCH=false
BREWFS_COMPRESSION=none
BREWFS_FUSE_READ_DIRECT_IO=1
BREWFS_FUSE_KEEP_CACHE=0
BREWFS_CACHE_TTL_MS=0
BREWFS_REQUEST_TRACE=true
BREWFS_REQUEST_TRACE_SAMPLE_RATE=307
PERF_TOOLS=packed-tree packed-smallfiles
PERF_PACKED_DIR_LEVELS=0
PERF_PACKED_DIRS_PER_LEVEL=1
PERF_PACKED_FILES_PER_DIR=8192
PERF_PACKED_SMALLFILE_COUNT=8192
PERF_PACKED_SMALLFILE_SIZE=4096
PERF_PACKED_SMALLFILE_READ_BYTES=0
```

Set the generated `BREWFS_PACKED_MANIFEST_KEY`, use a distinct artifact
directory for each of three fresh mounts, and keep Rust builds outside the
measurement window. Record binary SHA-256 and the candidate source diff.
The machine-local drivers and exact runtime override are preserved in
`/data/out/run_pack_compose.sh`, `run_pack_baseline_main.sh` and
`compose-pack-runtime.yml`; the original repository runner and its manifest
helpers are mounted unchanged. No inert metadata-prefetch variable is used
as evidence that data was warmed.
