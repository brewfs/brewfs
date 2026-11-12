# FUSE read-cache investigation (2026-10-26)

Status: the local baseline reproduces the lost-page-cache mechanism. Native
and overlay reopen with FOPEN flags `0`, discard all 65,536 cached pages of a
256 MiB file, and issue 2,048 FUSE READs. JuiceFS reopens with KEEP_CACHE,
retains every page, and issues zero FUSE READs. Candidate measurements and CI
are pending. The overlay mixed-I/O guard fails on the unchanged baseline, so
there is no overall performance acceptance.

Source findings refer to baseline commit `b21654e`. The original requested
`STANDARD.md` and `bench2.py` remain unavailable. The new measurements below
are explicitly supplemental; the historical task-supplied numbers are separate.
The companion [public evidence JSON](fuse-read-cache-2026-10-26.json) preserves
all three repetitions, observed flags, readahead, cache eligibility, FUSE counts,
residency, failed-guard status, and SHA-256 hashes of the local source artifacts.

## Reported gap

The original experiment used an 8-vCPU host, Redis metadata, RustFS S3, verified
mounts, and medians of three runs:

| Operation (MiB/s as reported) | Native | Overlay | JuiceFS | Overlay / JuiceFS | Kernel data page-cache eligibility |
| --- | ---: | ---: | ---: | ---: | --- |
| fio sequential read, `direct=1` | 638.8 | 929.2 | 439.7 | 2.113x | No: application direct I/O |
| fio sequential read, `direct=0` | 223.8 | 328.1 | 592.9 | 0.553x | Eligible unless the mount forces direct I/O; original wire flags unavailable |
| bench2 `seq_read_warm` | 660.8 | 401.7 | 6,226.8 | 0.065x | Buffered warm reads can hit retained pages; original wire flags unavailable |
| bench2 `seq_read_cold` | 294.7 | 421.9 | 172.0 | 2.45x | Initial pages should be absent if cache eviction succeeded; readahead can populate later pages |

Original `read_ahead_kb` and observed OPEN/CREATE reply flags were not provided.
The speed difference motivates checking data retention across opens; it does
not by itself prove which FUSE flags the original mounts negotiated.

## Source diagnosis, in hypothesis order

### 1. OPEN and CREATE flags

The requested native and overlay `workspace-v1` mounts use the ordinary VFS
FUSE adapter. With no overrides, regular files return flags `0x0`: neither
`FOPEN_DIRECT_IO` (`0x1`) nor `FOPEN_KEEP_CACHE` (`0x2`). This permits kernel
buffered reads within an open, but reopening discards previously cached pages.
The relevant baseline locations are:

| Location | Behavior |
| --- | --- |
| `src/fuse/mod.rs:92-101` | Direct-I/O environment overrides; per-read and per-write defaults are false |
| `src/fuse/mod.rs:130-155` | `BREWFS_FUSE_KEEP_CACHE` defaults false; combines KEEP_CACHE and optional DIRECT_IO bits |
| `src/fuse/mod.rs:2855-2858` | Regular OPEN returns those flags |
| `src/fuse/mod.rs:3484-3493` | Regular CREATE returns those flags |
| `src/fuse/mod.rs:2790-2792` | Virtual `.stats` always uses DIRECT_IO for its per-open snapshot |
| `vendor/asyncfuse/src/raw/abi.rs:204-205` | FOPEN bit definitions |
| `vendor/asyncfuse/src/raw/session/utils.rs:43-46` | Mount-level direct-I/O option ORs DIRECT_IO into replies |
| `vendor/asyncfuse/src/raw/session/handlers.rs:817,2236` | Worker OPEN/CREATE apply that mount-level override |
| `vendor/asyncfuse/src/raw/session/mod.rs:3464,4728` | Single-dispatch OPEN/CREATE apply the same override |

There is a separate experimental `workspace-entity-v1` path. Its CREATE and
OPEN explicitly return DIRECT_IO at `src/fuse/entity.rs:458,634`; its mount
also forces direct I/O at line 990 and sets readahead to zero at line 995.
`src/cli.rs:379-381` selects it only for `WorkspaceEntityV1` with
`experimental_entity_mount`; these settings do not describe the requested
`workspace-v1` benchmark mounts.

### 2. Mount policy

`src/fuse/mount.rs:30-50` builds ordinary mount options: `default_permissions`
and `allow_other` enabled, max read/write 4 MiB, and kernel writeback caching
opt-in. It does not force direct I/O. The vendor default is `direct_io: false`
at `vendor/asyncfuse/src/mount_options.rs:98`. The existing global
`BREWFS_FUSE_DIRECT_IO` override and read/write-specific overrides affect the
regular open-reply helper described above.

The installed reference is `juicefs version 1.3.1+2025-12-02.e0032b2`.
Its actual `mount --help` reports `--writeback=false`, `--open-cache=0s`,
attribute/entry TTLs of 1s, a 300 MiB buffer, and `--max-readahead` as a
per-read-session userspace buffer in MiB, without a numeric default printed.
The run uses `-o allow_other`; no `direct_io` option or `--writeback` is passed.
The complete command, help and diagnostic wire capture are retained under
`/data/readcache/artifacts/juicefs/`. Its ordinary CREATE and first OPEN reply
flags were `0x0`, with repeated OPEN replies `0x2` (KEEP_CACHE). The timed
operations independently confirmed those stage-dependent OPEN flags.

### 3. Readahead

Ordinary BrewFS mounts configure INIT `max_readahead = 16 * 1024 * 1024` bytes
at `src/fuse/mount.rs:49`. The vendor copies that configured value into the
INIT reply (`vendor/asyncfuse/src/raw/session/mod.rs:2482-2486,2510`) and logs
the complete reply at line 2523. This is a requested ceiling, not evidence
that the mount's BDI actually uses 16 MiB.

`FUSE_AUTO_INVAL_DATA` is negotiated when offered by the kernel
(`vendor/asyncfuse/src/raw/session/mod.rs:2347-2350`). KEEP_CACHE does not
disable mtime-based data invalidation on attribute revalidation.

JuiceFS's userspace `--max-readahead` budget is a separate quantity from FUSE
INIT `max_readahead` and `/sys/class/bdi/<major>:<minor>/read_ahead_kb`. Record
all three independently; a 1024-MiB userspace budget does not establish a
1024-MiB kernel readahead window.

Measured baseline values:

| Mount | INIT request from kernel | INIT reply | Effective BDI `read_ahead_kb` | Timed mount BDI |
| --- | ---: | ---: | ---: | --- |
| Native BrewFS | 131,072 bytes | 16,777,216 bytes | 128 KiB | `0:46` |
| Overlay BrewFS | 131,072 bytes | 16,777,216 bytes | 128 KiB | `0:46` on its later mount |
| JuiceFS | 131,072 bytes | 131,072 bytes | 128 KiB | `0:47` |

The BrewFS diagnostic mounts logged complete INIT requests/replies; JuiceFS
used a separate untimed wire capture. Every timed row records its BDI value.
Thus BrewFS's configured 16 MiB ceiling did not produce a 16 MiB effective
kernel window on this host. Both filesystems actually used 128 KiB. Readahead
is left unchanged to isolate the page-retention hypothesis; candidate observed
values remain pending.

### 4. Existing cache knobs

| Knob | Applies to | Relevance to a just-written plain file |
| --- | --- | --- |
| `BREWFS_PACKED_METADATA_CACHE_BYTES` | Packed lower/read-only metadata; defaults to 256 MiB at `src/cli.rs:965-985,2299-2309` | Does not retain plain payload bytes or kernel data pages |
| `cache.range_background_prefetch` | Ordinary `ObjectBlockStore`; wired at `src/cli.rs:1434`, full-block prefetch on range/page misses at `src/chunk/store.rs:1423-1426,1554-1558` | Relevant to native and upper payload misses after crossing FUSE; cannot eliminate the FUSE request |
| `cache.read_memory_bytes`, `cache.read_ssd_bytes` | Userspace payload cache, wired at `src/cli.rs:1425-1429` | Can avoid an S3 GET; warm reads still cross FUSE when kernel pages were discarded |
| `cache.prefetch_max_bytes` | Userspace VFS read-ahead, `src/vfs/config.rs:303-305` | Helps misses; independent of kernel page-cache retention |

## Change and coherency contract

The change exposes the existing KEEP_CACHE behavior as an explicit per-mount
`--fuse-keep-cache true` / YAML `fuse.keep_cache: true` opt-in, with default
false and compatibility for `BREWFS_FUSE_KEEP_CACHE`. Regular OPEN/CREATE
then return `0x2` in the absence of direct-I/O overrides. Repeated buffered
reads can reuse pages across close/open. Direct I/O remains a separate policy.
Precedence is CLI > YAML > environment > false, with explicit false overriding
an enabled environment variable. The experimental entity adapter rejects this
opt-in rather than silently ignoring it.

Same-mount buffered writes use the existing write-through path. The VFS also
invalidates its userspace read cache after writes
(`src/vfs/fs/mod.rs:5599-5614`); existing truncate/fallocate/copy invalidation
paths remain in place. Keep permission checks, attribute TTLs and PR #332's
timestamp authorization unchanged. In particular, the lease-dependent
`KERNEL_WRITEBACK_TIMES` check in `src/fuse/mod.rs:1226-1234` is unrelated to
this data-cache knob.

The operator gives up unconditional data-page invalidation on reopen. Data
mutations should pass through this mount; other mounts or external APIs can
leave cached bytes stale. `FUSE_AUTO_INVAL_DATA` can invalidate data when
revalidated metadata exposes a changed mtime, but this is not a guarantee of
strict freshness for external changes. Permission checks still run on open.

Cached reads also bypass the userspace read operation's backend/lease fence:
the packed read path enters that fence in `src/vfs/fs/mod.rs:5106-5119` and
`src/workspace_overlay/meta_layer/packed_lower.rs:723-733` in the baseline.
A read-only reopen is not itself a replacement for the data-read fence:
frozen packed single-step metadata reads omit a per-operation remote fence
(`src/workspace_overlay/meta_layer/mod.rs:580-587,644-646`), and read-only
`record_open` omits an authenticated mutation CAS (lines 2324-2333).
Consequently a successful cached read does not prove backend availability or
a currently valid lease; immediate read failure on lease loss is not promised.
Baseline buffered reads already permit kernel hits within a live descriptor.
The opt-in extends reuse across reopen and leaves write fencing unchanged.
This is part of the coherency tradeoff behind the safe default of false.

## Local protocol and build provenance

All Cargo commands must run serially with:

```bash
export CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0
# Every build/check/test/clippy invocation uses -j 4.
```

Baseline source: `b21654e13b1ae1716e7e6d842f414eb4fe09bacd`.
The release binary was built on this 32-CPU host with
`cargo build --release -j 4 --features workspace-overlay --bin brewfs`.
Its SHA-256 is
`ec55e3f4b8b88c2f013501a5a8bb9258e82a07c195ac2630b701f4b59fcebdcd`.
The successful build's `/usr/bin/time -v` peak RSS was 19,807,012 KiB; the
serial fixture build peaked at 873,800 KiB. Neither was OOM-killed.

An earlier baseline attempt was deliberately SIGTERM-stopped after an external
checkout started concurrent Cargo work; its peak RSS was 20,295,404 KiB.
The task did not start two Cargo commands together. Subsequent build execution
paused during external compiler activity, and no performance row was measured
with a detected compiler. Build logs are `baseline-build.log`,
`baseline-build-resume.log` and `baseline-fixture-build.log` under the artifact
root. Candidate serialized CI/build results remain pending.

Requested benchmark setup: native `workspace-v1` and overlay
`init-packed-volume` + `workspace-v1`, Redis metadata and RustFS S3, matching
`/home/hxy/spot/STANDARD.md`. Assert `mountpoint -q` before **every** run. Run
`/home/hxy/bench2.py` three times per arm plus fio sequential `direct=0` and
`direct=1`. Preserve raw runs, exact commands, mount options, OPEN/CREATE
flags, INIT reply and per-mount BDI values. State eviction and cache eligibility
for each metric. Compare cold and direct-I/O results separately from warm hits.

The exact requested protocol remains unavailable: searches of this host and
repository history did not recover `STANDARD.md` or `bench2.py`. No replacement
is labeled as bench2, and no claim is made to reproduce the original 8-vCPU
configuration. The supplemental runner is
[`tools/perf/fuse_read_cache.py`](../../tools/perf/fuse_read_cache.py).

The supplemental setup uses isolated Redis 7.2 at `127.0.0.1:16389` and upstream
RustFS release `ceddcdd878d21829eb6f9b08153632e4fce026b5` at
`127.0.0.1:19090`. Native and overlay use separate `workspace-v1` catalogs and
buckets. Overlay was initialized with `init-packed-volume` and a one-file
immutable lower; measured files were newly written in its writable upper.
Both BrewFS arms use no compression, 4 MiB blocks, 64 MiB chunks, 300 MiB
userspace read-memory cache and 1 GiB SSD cache. JuiceFS uses no compression,
4 MiB blocks, a 300 MiB buffer and 1 GiB disk cache. These budget controls have
different meanings and do not establish identical total memory use.

Each repetition writes and fsyncs a fresh 256 MiB file. Cold means **kernel
data-page-cache cold**: `POSIX_FADV_DONTNEED` followed by `mincore` proves zero
resident pages. Userspace and object-store caches remain warm. The sequential
probe reads in 1 MiB calls and times only the reads, excluding open, close and
residency inspection. Warm closes and reopens before reading. fio 3.36 uses
sync I/O, 1 MiB reads, one job, depth one, a ten-second runtime and
`invalidate=0`; buffered fio is seeded before its OPEN. Every operation asserts
`mountpoint -q`, records the mount's BDI, and samples for Cargo/rustc processes
every 100 ms. No successful row recorded a compiler or monitor error.

Actual FOPEN flags were observed during each timed operation using
`kprobe:fuse_finish_open`, filtered by mount device and correlated by inode,
PID and monotonic timestamps. This probes OPEN only; no READ tracing or verbose
FUSE logs run during timing. Delivered/kernel event counts matched: native
55/55, overlay 51/51, JuiceFS 55/55. Separate diagnostic traces corroborate
the OPEN/CREATE policy and INIT reply.

## Supplemental baseline results

All throughput values below are MiB/s. Each baseline cell contains the three
repetitions in execution order; the adjacent median uses those three values.
Every listed number uses effective BDI `read_ahead_kb=128`. The flags and cache
eligibility column applies to every repetition and its median. Candidate
measurements are pending, and overlay read results are descriptive because its
mixed-I/O guard failed.

| Mount | Operation | Baseline repetitions (MiB/s) | Baseline median | Candidate median | Actual OPEN flags; could kernel pages serve the read? |
| --- | --- | --- | ---: | --- | --- |
| Native | Cold sequential | 111.458 / 108.270 / 95.079 | 108.270 | Pending | `0x0`; initially no pages, buffered readahead allowed |
| Native | Warm reopened sequential | 227.341 / 213.260 / 189.381 | 213.260 | Pending | `0x0`; reopen discarded prior pages, buffered readahead allowed |
| Native | fio `direct=0` | 911.409 / 849.915 / 859.014 | 859.014 | Pending | `0x0`; yes, later passes within the open can hit pages |
| Native | fio `direct=1` | 450.555 / 423.015 / 417.458 | 423.015 | Pending | `0x0`; no, application `O_DIRECT` bypasses cached pages |
| Overlay | Cold sequential | 150.309 / 136.667 / 143.716 | 143.716 | Pending | `0x0`; initially no pages, buffered readahead allowed |
| Overlay | Warm reopened sequential | 163.448 / 135.442 / 137.832 | 137.832 | Pending | `0x0`; reopen discarded prior pages, buffered readahead allowed |
| Overlay | fio `direct=0` | 1374.263 / 1322.768 / 1297.481 | 1322.768 | Pending | `0x0`; yes, later passes within the open can hit pages |
| Overlay | fio `direct=1` | 386.284 / 367.106 / 392.122 | 386.284 | Pending | `0x0`; no, application `O_DIRECT` bypasses cached pages |
| JuiceFS | Cold sequential | 562.777 / 537.346 / 561.573 | 561.573 | Reference | `0x0`; initially no pages, buffered readahead allowed |
| JuiceFS | Warm reopened sequential | 4585.705 / 4398.927 / 4200.085 | 4398.927 | Reference | `0x2`; yes, all pages retained and no FUSE READs |
| JuiceFS | fio `direct=0` | 5895.010 / 6006.099 / 5560.844 | 5895.010 | Reference | `0x2`; yes, no FUSE READs |
| JuiceFS | fio `direct=1` | 741.626 / 722.728 / 714.629 | 722.728 | Reference | `0x2`; no, application `O_DIRECT` still bypasses cached pages |

The cache evidence directly identifies reopen invalidation, independent of
throughput. Residency is sampled after OPEN and before the timed read; each
sequential read ends with all 65,536 pages resident. FUSE READ counter deltas
for repetitions 1/2/3 are:

| Mount | Cold: resident pages before read; FUSE READs | Warm reopen: resident pages before read; FUSE READs | fio `direct=0` FUSE READs | fio `direct=1` FUSE READs |
| --- | --- | --- | --- | --- |
| Native | 0/0/0; 2048/2048/2048 | 0/0/0; 2048/2048/2048 | 1024/1024/1024 | 4506/4231/4175 |
| Overlay | 0/0/0; 2048/2048/2048 | 0/0/0; 2048/2048/2048 | 1024/1024/1024 | 3864/3674/3922 |
| JuiceFS | 0/0/0; 2048/2048/2048 | 65536/65536/65536; 0/0/0 | 0/0/0 | 59336/57824/57176 |

The default therefore already supports kernel caching within an open. The
targeted gap is loss of retained pages when the application closes and reopens
the file. The fio loop and reopened sequential probe exercise different cache
lifetimes and should not be treated as interchangeable throughput results.

## Correctness checks and failed baseline guard

The real-mount correctness probe passed on baseline native and overlay mounts:
overwrite through concurrent descriptors, append, shrink/extend, O_TRUNC,
O_DIRECT overwrite, mmap write, rename replacement, and permission checks
after warming pages. Group/other writes and current-time utime remain permitted;
unauthorized reads and non-owner explicit timestamps are denied. Three warm
reopens of its 4 MiB test file issued 96 FUSE READs in each baseline arm.
Candidate probe/CI results remain pending.

The supplemental fio randrw guard uses 4 KiB requests, 70% reads, one job,
ten seconds, and end fsync. Native and JuiceFS completed all three guards;
baseline overlay failed every guard with read `EIO` (errno 5). No overlay
mixed throughput is accepted. The initial failure stopped the default
fail-fast runner. A separate diagnostic reproduced `workspace is busy` on the
unchanged baseline. An explicit collection run subsequently attempted all
three guards while preserving the independent read quartets; all three
failures remain rejected and the overall harness/phase exited nonzero.

The source explains the observed failure path: in baseline
`src/vfs/io/writer.rs:607-618`, `packed_pending_overlay` returns raw
`WorkspaceError::Busy` after metadata publication starts but before the slice
is marked committed. `canonical_dirty_snapshot` checks every slice before
filtering written-range overlap (lines 3333-3341). The packed-read retry at
`src/vfs/fs/mod.rs:5330-5338` recognizes typed `ReadViewChanged`, so it does not
retry that raw Busy error. Existing test
`packed_canonical_failed_publish_and_inflight_publish_fail_closed`
(`src/vfs/io/writer.rs:6613`) deliberately rejects ambiguous publication state.
KEEP_CACHE does not change this code or test; bypassing the guard's error would
require a separate coherency analysis. No mixed-workload acceptance or absence
of regression is claimed.

Evidence is retained under `/data/readcache/artifacts/`:

- `baseline-summary.json` and `before-native-phase.json`, `juicefs-phase.json`,
  `before-overlay-complete-phase.json` preserve results and overall status.
- Each `<label>-measurements/measurements-with-open-flags.jsonl` contains raw
  rates, cache eligibility, residency, FUSE counts, compiler monitoring and
  actual during-operation OPEN events. Raw fio JSON preserves latency data.
- `before-overlay-complete-measurements/rejected-measurements.jsonl` retains
  all three failed guards; `before-overlay-eio-debug/` retains the independent
  unchanged-baseline reproduction.
- `<label>-diagnostic/correctness.log` and `mount.log` preserve BrewFS
  correctness and INIT evidence. `juicefs/initial-diagnostic/` preserves its
  wire trace and decoded replies. All measurement mounts exited cleanly.
- `services/setup.json`, each mount's YAML/command record, and artifact
  `README.md` contain public setup provenance. Task credentials are excluded
  from this report and the PR.

The committed JSON is an allowlisted summary of those artifacts; it does not
include credentials or private environment files. Candidate entries are null
with an explicit pending status until measurements complete. Local artifact
hashes identify the inputs used for the summary; they are not remote download
links or a claim that raw artifacts have been published.

Still required: candidate measurements/flags/residency, completed CI and
candidate build RSS, candidate same-mount visibility/permission results, and
self-review. The original bench2/STANDARD protocol also remains unavailable.
