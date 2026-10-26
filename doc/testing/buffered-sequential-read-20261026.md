# Buffered sequential read diagnosis, 2026-10-26

TTL 1000 eliminates all FUSE traffic during the same-descriptor warm read and
raises its median from 843.90 to 4,922.41 MB/s (5.833x). The clean JuiceFS median
is 6,120.59 MB/s, so the measured ratio is 0.804x. This does not establish that
the historical buffered fio-bigread gap is closed. Reopening a file discards
BrewFS's cached pages: it returns FOPEN flags 0, whereas JuiceFS returns
FOPEN_KEEP_CACHE (2). No filesystem behavior change is proposed for acceptance
in this PR; the change is instrumentation of the existing campaign runner.

All rates below are decimal MB/s, medians of three 256 MiB trials. Counts are
from the trial with the median throughput, over the entire 256 MiB window.
L/G/R mean LOOKUP (1), GETATTR (3), READ (15). The raw JSON records every opcode
and all three samples, rather than combining throughput and independently
selected request counts.

| Buffered read | BrewFS TTL 0 MB/s; L/G/R | BrewFS TTL 1000 MB/s; L/G/R | JuiceFS MB/s; L/G/R |
|---|---:|---:|---:|
| Same warmed descriptor | 843.90; 0/257/0 | 4,922.41; 0/0/0 | 6,120.59; 0/0/0 |
| Reopen after warming, no eviction requested | 69.28; 2/261/2048 | 65.19; 2/4/2048 | 5,673.68; 2/0/0 |
| POSIX_FADV_DONTNEED, then reopen | 69.33; 2/261/2048 | 69.09; 0/3/2048 | 1,072.77; 2/0/2048 |

Every retained trial has client requested-size histogram `{1048576: 257}` and
returned-size histogram `{1048576: 256, 0: 1}`: **1 successful read/MiB**, or
**1.00390625 syscalls/MiB including EOF**. All daemon data reads in BrewFS and
all evicted JuiceFS trials are `{131072: 2048}`: **8 FUSE READs/MiB, each 128
KiB**. Warm reads issue no FUSE READs. There is no evidence of requests smaller
than the expected kernel 128 KiB size defeating readahead in this workload.
The 257 GETATTRs at TTL 0 correspond to client read calls, including EOF, not
to the 4 MiB stored-object size. There are no LOOKUPs in the warmed-descriptor
window. With TTL 1000, all three warm windows have zero GETATTRs too.

The remaining short-burst warm-rate difference cannot be assigned to daemon
READ processing: neither daemon receives any request in those windows. This
test does not isolate client allocation/copy cost, CPU placement, or kernel
page-cache overhead. On reopen, flags and request counts identify a concrete
cache-retention difference. The evicted data path also remains slower in this
unoptimized BrewFS build, independently of attribute TTL. No improvement is
claimed for that path.

The packed lower supports bounded reads already. [src/cli.rs](../../src/cli.rs) attaches
`PackedV3ReadonlyMeta` through `with_packed_v3_lower`.
[readonly.rs](../../src/workspace_overlay/packed_v3/readonly.rs) wraps its runtime object client
in `V3ReadonlyBackend`, whose `get_object` rejects whole-object GETs. The
prepared read path uses `prepare_inode_read_observed` with an explicit logical
range; the block-store adapter uses `read_inode_range`. Range reads may include
authenticated frame/metadata overhead; this is not a claim of exactly one
128 KiB HTTP transfer per FUSE READ. The timing corpus is newly written upper
data on the packed-v3-backed workspace, not an immutable packed-lower corpus.
The lower-path answer is a source/test finding, not inferred from these rates.

The next experiment should check data-version-validated cache retention on
read-only reopen, including local writes, truncation and external generation
changes, then replay the release compose matrix with explicit cache state.
A blanket KEEP_CACHE default is not established as coherent by this evidence.
Do not raise INIT maxima, shrink stored objects, or add metadata caching on
the strength of these results. No vendor/asyncfuse change is needed to observe
or set the existing open flag.

The original metadata harness was replayed at its original sizes: 256 names,
8192 stat calls per process, 1/8 processes, and 256 creates/unlinks. Comparison
against workspace-metadata-cache-20261026/measurement-summary.json:

| Operation | TTL 0 historical → current ops/s | TTL 1000 historical → current ops/s |
|---|---:|---:|
| stat, 1 process | 226.00 → 225.29 | 132,903.64 → 73,474.83 |
| stat, 8 processes | 522.28 → 532.23 | 2,393,385.32 → 2,297,555.69 |
| create | 57.70 → 57.04 | 68.57 → 66.98 |
| unlink | 74.90 → 80.66 | 96.75 → 96.75 |

The single-process enabled stat result is **0.553x its historical rate**;
therefore the complete no-regression acceptance is not claimed. Its timed
LOOKUP counts are 98, 105, 0 versus the earlier 69, 0, 40. These short bursts
are sensitive to the phase of the mount-wide cache expiry; the counts explain
why the elapsed time changes, but do not by themselves prove no regression.
The disabled third stat trial also contains unrelated directory traffic,
retained in the raw data. The first two disabled trials and all three
eight-process trials have the expected counts. No Rust behavior was changed.

The supplied historical JSON has no `smallfiles-stat` or `dirperf` rows.
Additional n=3 measurements on this same binary give `smallfiles-stat` wall
time 34.7633 s (TTL 0) versus 0.9191 s (TTL 1000), using the existing scanner,
256 4 KiB files, one worker and 32 epochs. The existing xfstests dirperf binary
takes 10.6019 s versus 6.0892 s with `-a 100 -f 100 -l 100 -c 16 -n 2 -s 5`.
These are supplementary comparisons, not invented historical baselines or a
substitute for the compose default's larger directory workload.

The current-main binary was built once, with workspace-overlay, and copied
before all formal measurements. SHA-256:
`d3790f53f3ffe8e3915c87a702f7fc959077b92c8c2b435dcde6c9e8aeff2573`.
Base commit: `1ad4c99`. Build: dev/unoptimized, `CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, `-j 2`, existing
`CARGO_TARGET_DIR=/data/asyncfuse/target-workspace`. This is a local diagnostic,
not a release/compose acceptance measurement or a replacement for its matrix.

Both filesystems used Linux 6.8.0-36 on this 31 GiB, 32-logical-CPU host, local
Redis/RustFS, no compression, 300 MiB memory/buffer and 1 GiB disk-cache limits,
4 MiB blocks and 64 MiB chunks. JuiceFS is 1.3.1+2025-12-02.e0032b2, SHA-256
`a9e5fcb0278df96de366f99ded6e347e64fcb2230d97d3ce61816cc54eefdab5`.
Its attr/entry/directory TTLs are 1 s; its writeback option is off. Default
concurrency settings are retained and recorded, rather than claimed identical:
BrewFS upload 10, prefetch 64, S3 multipart 32; JuiceFS uploads 20, prefetch 1.
Files are fsynced and closed before reads. Warm and reopen runs retain daemon
caches; DONTNEED only evicts kernel data pages. There is no asynchronous-drain
throughput claim. FUSE/direct/keep-cache environment overrides were removed
for BrewFS, and fio direct mode is not involved in this Python syscall test.

The existing `tools/perf/workspace_metadata_cache.py` and campaign mount script
were reused. Read-first invocation, once per verified mount:

```bash
python3 tools/perf/workspace_metadata_cache.py MOUNT OUTPUT \
  --label off --reads-only --read-diagnostics
```

Use `--label on` at TTL 1000 and `--label juicefs` on JuiceFS. Omit the final
two flags to replay the original metadata protocol. Repetitions default to 3.
BPF brackets the timing using the existing getpgid markers, counts mount-scoped
completed FUSE requests, records FUSE read-in sizes and fuse_finish_open flags,
and counts the parent client's read entry/exit tracepoints. Metadata workers'
FUSE requests are included; their syscalls are not presented as client read
measurements. A failed tracer cannot be presented as zero FUSE requests.
Every measurement checks the mount and rejects a running compiler. All
performance work finished before the serial Rust checks began.

The initial JuiceFS trials were excluded after an external recursive grep was
found reading its virtual .accesslog. That search both added unrelated READs
and held the mount open. Its command/fd diagnostic and all original trials are
retained, and the table uses a clean rerun with exactly 2048 evicted READs.
The orphaned read-only diagnostic was interrupted and unmount then completed.
A preliminary untimed 64 MiB probe used the prior campaign binary only to
calibrate the BPF argument layout; it is excluded from every comparison.

Readback checks outside timed windows verified all 268,435,456 bytes and the
written SHA-256 after each trial (nine checks). The file spans four 64 MiB
chunks and 64 logical 4 MiB blocks. Each 1 MiB region carries its index, so
same-length corruption and exchanging regions are detectable. Instrumentation
tests were run against the unmodified script from base commit 1ad4c99 first:

```text
test_failed_tracer_cannot_be_reported_as_zero_fuse_requests ... FAIL
AssertionError: RuntimeError not raised
test_trace_keeps_syscalls_eof_and_fuse_sizes ... ERROR
AttributeError: 'RequestCounts' object has no attribute 'maps'
test_verify_rejects_same_length_corruption_after_multiple_blocks ... ERROR
AttributeError: module 'workspace_metadata_cache' has no attribute 'verify_contents'
Ran 3 tests
FAILED (failures=1, errors=2)
```

With the instrumentation change:

```text
test_failed_tracer_cannot_be_reported_as_zero_fuse_requests ... ok
test_trace_keeps_syscalls_eof_and_fuse_sizes ... ok
test_verify_rejects_same_length_corruption_after_multiple_blocks ... ok
Ran 3 tests
OK
```

These are fail-first tests for the measurement change, not a claim that the
base filesystem returned corrupt data. The full feature-enabled workspace gate
passed with four test threads: 2,536 library tests plus 11 binary tests passed,
451 library tests ignored. The chunk suite passed 188 tests, with 3 ignored.
The first workspace run failed a 50 ms writeback grace assertion and returned
ENOMEM from the native export test. Both passed in isolation and then in the
complete rerun; no Rust source or test was changed. The failed run is retained.
The existing `fuse_cache_ttl` integration suite also passed all four tests,
including the explicitly enabled two-mount test. External chmod/truncate
became visible after 894.5 ms and same-mode ACL revocation after 980.6 ms.
The workspace build, workspace check, Tokio runtime check, formatting and
shell checks passed. The io-uring runtime check was interrupted to preserve
the 70-minute timebox; Clippy was not reached. Thus the complete local CI gate
is unfinished, and this instrumentation PR remains a draft. Exact commands,
statuses and retained test summaries are in `rust-validation.txt` and
`rust-gates.json`. No optimization or matrix update is accepted.

[Raw measurements and validation evidence](buffered-read-20261026/) include
[the computed summary](buffered-read-20261026/measurement-summary.json), exact
BPF programs/output bundled per arm, all checksum results, mount configuration,
the excluded JuiceFS trials, and the fail-first logs. Full build/test logs and
the frozen measurement binary remain under
`/data/metacache/artifacts/buffered-read-20261026/`.
