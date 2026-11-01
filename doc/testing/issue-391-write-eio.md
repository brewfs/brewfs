# Issue #391: bounded cold reads and slice-ID refill admission

This is a partial correctness fix for [#391](https://github.com/brewfs/brewfs/issues/391).
Both reported defects have regression coverage, but the TiKV/FUSE acceptance
workload still accumulates uploaded slices awaiting metadata commit and exceeds
the existing flush deadline. The two correctness fixes passed code self-review
on 2026-11-01 after rebasing onto `773d8e10` (including #399), and the full
rebased local gate passed. This delivers two partial correctness fixes. These
measurements are not an accepted performance baseline and do not close #391.

## Changes

- `packed_permissions.rs` budgets derived prefetch against the complete 32-key
  cold read. The authority/CONTROL snapshot remains one consistent read.
  Generation/reverse keys and complete deletion-key windows are prefetched only
  when they fit; other derived keys use the existing independent bounded reads.
  Every derived fence still participates in the final CAS. The TiKV cap is
  unchanged.
- `kv_store.rs` serializes slice-ID range refill per store, rechecks the local
  range after admission, and reserves the refill owner's ID before publishing
  the remaining IDs. The metadata concurrency of this allocator is one.
  Durable CAS, cross-store uniqueness, and error propagation are unchanged.
  This includes the bucket-specific allocator added by #386: its cold workspace
  lookup shares the same gate, while cached IDs retain a local fast path.

This does not change object aggregation, product deadlines, gap handling,
failure handling, or the writer's admission of all outstanding metadata work.

## Regression and local CI evidence

Initial base: `a8a95508458cd27152eb916d62df22f63dd9f771`. After the two failed
original-base runs, the branch was rebased onto
`060db0df089f916609ce55445db44a4876c40c4c` to cover the new bucket allocator.
That base also includes #385's packed authentication/cache improvements.
Later backend results measure that combined version, not the effect of this
patch in isolation. Subsequent upstream #392/#393 changes are outside that
historical E2E base; the rebased local gates are recorded separately below.

The cold-read regression covers 18 through 33 slices and 64 slices, each with
and without a dentry (34 cases). It checks actual bounded reads, the complete
authority snapshot, every final CAS fence, and persisted extent contents. The
original implementation fails at 19 slices plus a dentry with 33 keys. A second
test changes an unprefetched deletion marker before CAS and confirms that no
mutation is committed.

The refill regression parks the backend while 64 / 1,025 allocation futures
contend for an empty range. Only one enters the initial metadata read; all
allocations succeed with unique IDs using exactly 1 / 17 refill CAS calls.
Cancellation/error recovery and the existing cross-store uniqueness test pass.
The previously reported 2,065 CAS calls for 64 callers are historical evidence,
not a measurement repeated here.

The new bucket allocator regression first failed on the unpatched bucket path:
64 concurrent callers entered cold metadata lookup, versus the expected one.
The correction also tests cancellation, backend error, and legacy-workspace
fallback releasing the shared gate. Rebased validation is recorded below.

Commands use `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0` where applicable:

The following counts are historical, from the original-base validation. They
are not used as a substitute for the rebased validation below.

| Gate | Result |
| --- | --- |
| `cargo test --lib --features workspace-overlay` | 2,594 passed, 0 failed, 453 existing ignored |
| `cargo test --workspace --lib --bins` | 1,277 passed, 0 failed, 262 existing ignored |
| `cargo clippy --workspace --all-targets -- -D warnings` | Exit 0; no workspace warnings |
| Same strict Clippy with `--features workspace-overlay` | Exit 0; no workspace warnings |
| Workspace check/build, tokio/uring feature checks | Passed |
| fmt, four runner syntax checks, three report/runner script tests, diff check | Passed |

No assertions were removed and no tests were newly ignored. Clippy output still
contains 30 existing vendored dependency dead-code warnings (26 tikv-client,
4 asyncfuse), plus a proc-macro-error2 future-incompatibility reminder. Thus the
literal requirement of zero warnings in the entire output is not met, despite
the strict workspace Clippy commands succeeding. `rfuse3` is not a workspace
member, so the workflow's conditional checks do not apply.

## Real backend protocol

Original artifact path: `/root/issue-391-fix/` on the former test host; the
preserved archive is listed below. These historical backend runs used source
patch SHA-256 `a5cfa89a10a28d9b798cc72d29d1a452aa79c2018a8d129b40379035255980a0`.
The historical release binary SHA-256 is
`5f66307c0b2d2741f0384a2fd4fd65ea1bb1a0457b9a0ea37fe5db4d73cd7c2b`.

Dedicated Compose project `issue-391-fix`: TiKV/PD 8.5.0 and RustFS 1.0.4.
Each run has a fresh backend, bucket, workspace, and cache. The original four
512 MiB source files have matching SHA-256 checksums (2 GiB total). Each mount
passes 28 direct content/boundary checks and five 30-second random-read warmups.

Configuration matches the attribution experiment: 16 FUSE workers,
max_background 512, one Tokio worker, 768 MiB write RAM, 64 GiB write SSD,
upload concurrency 10, 300 MiB read RAM, 1 GiB read SSD, 2 GiB total memory
budget, no prefetch, no upper compression, 64 MiB chunks, 4 MiB blocks,
static-4mib packed lower frames, and `upload_before_commit`.

```sh
fio --name=issue391 --directory=<mount> --rw=randwrite --bs=4k \
  --numjobs=4 --iodepth=32 --ioengine=libaio --direct=1 \
  --runtime=20 --time_based --size=512m --filename_format='r.$jobnum' \
  --randseed=20261029 --group_reporting --lat_percentiles=1 \
  --output-format=json --output=<artifact>/fio.json
```

Drain requires buffer, dirty, live_dirty, recent_pending, and remote_inflight
bytes to be zero in three consecutive readable stats snapshots. Failure logs
are checked before and after cleanup. Stats PUT totals, captured HTTP PUT 200
responses, and the actual `chunks-v2/` object inventory are compared separately.
The observer timeout is distinct from the unchanged product flush deadline.

## Initial failed run

`round1` did not finish before the initial harness's 240-second observer timeout;
its fio JSON is empty, so no fio bandwidth or exit success is inferred. All
21,145 PUTs finished and the object inventory matched exactly: 87,210,596 bytes,
4,124.4075 bytes/PUT. At the product's 300-second flush deadline, 17,612 slices
were still pending across the four files, all shown as `Uploaded`.

Before cleanup, dirty/live_dirty bytes remained 1,074,528,256. The first unmount
attempt was busy; after fio workers exited, the next unmount finished. Teardown
also logged SDK owner closure and stale-authority failures, so this is not a
clean-drain result. No zero after process termination counts as successful drain.

A five-second diagnostic perf sample after the failed run found substantial
writer slice-queue scanning/cloning work. It is not sufficient to attribute the
entire remaining delay or to justify changing upload concurrency or deadlines.

## Preserved observations

The archive contains two failed observations; it does not establish three
successful repetitions. Keep the rounds separate: the valid fio result is from
round2, while 21,145 PUTs and four SDK owner errors are from round1.

| Observation | round1 | round2 |
| --- | --- | --- |
| fio | Observer timeout; empty JSON | Exit/error 0/0; 4.094157 MiB/s; 1,048.104301 IOPS; p99 354.418688 ms |
| PUT count / bytes (stats, HTTP and objects agree) | 21,145 / 87,210,596 | 20,856 / 86,025,696 |
| Final dirty/live_dirty bytes | 1,074,528,256 | 1,097,859,072 |
| Drain | Failed | Failed after a further 120.433 s observation |
| SDK owner closed/full, including teardown | 4 | 9 |
| Failed-slice drop / bounded-read deadline / 32-key error / literal EIO | All zero | All zero |

Round2 wall time was 322.949 s; its wall-plus-observation effective bandwidth was
0.184854 MiB/s, with debt still outstanding. This is not durable throughput.
The archive is now at
`/home/hxy/opt-reports/opt-attrib-final/optattrib-archive.tgz`, with per-round
`analysis.json` under `attrib-stage/issue-391-fix/root/issue-391-fix/`.

## Self-review on main including #399

- The cold authority read has 11 view slots plus CONTROL. The remaining budget
  is 20, including reverse state and extent generation. At 32 or 33 distinct
  slices the cold read has 13 keys, or 14 with a dentry; deletion checks fall
  back to independent windows of at most 32. The separate permission snapshot
  entry point is at most 29 keys. Cached prefetch only builds an in-memory map.
- The refill gate is not acquired recursively. Legacy fallback explicitly
  drops it before calling the legacy allocator. Cancellation releases the
  guard; an uncertain committed reservation can leave an allowed ID gap, but
  cannot roll back the durable counter or reuse an ID. The gate intentionally
  spans network I/O, so slow refills can delay cold allocators in other buckets
  within the same store. Legacy workspaces do not cache a missing upper bucket,
  so their workspace allocation entry point still performs a serialized lookup
  on every call. This is not a bound on all writer tasks or tail latency, and
  this review does not establish a legacy-workspace performance result.
- The first ID is reserved for the refill owner before publishing the other
  63. Durable CAS retains uniqueness across stores within each allocator's
  namespace. Different buckets intentionally may reuse numeric slice IDs.
- Authority stays in one consistent cold read, and all independently read
  generation/deletion values still fence the final live-lease CAS. The existing
  hot-cache authority selection is unchanged.
- The outstanding Uploaded-to-Committed backlog is an existing admission and
  backpressure issue outside these fixes: allocation finishes before upload,
  the upload semaphore does not cover metadata commit, and a 64-member commit
  batch does not bound the entire pending queue. This is a code-path attribution,
  not a new matched E2E performance result. No product deadline was relaxed.

Rebased local gate results on 2026-11-01, with Rust source identical to
`4629f838ec74c68de0abefc3dd57e0a86ab1cf8f`:

| Gate | Result |
| --- | --- |
| Overlay lib tests | 2,704 passed, 0 failed, 474 existing ignored, 0 filtered out |
| Workspace lib + bins | 1,302 passed, 0 failed, 264 existing ignored, 0 filtered out |
| Strict workspace Clippy, default and overlay, all targets | Both exit 0 with `-D warnings` |
| Workspace check/build, no-default-features tokio/uring checks | Passed |
| fmt, four shell syntax checks, three runner/report tests, diff check | Passed |

All 17 gate commands passed. `rfuse3` is not a workspace member, so its
conditional runtime matrix does not apply. Existing vendored dependency
warnings and the proc-macro-error2 future-compatibility reminder remain in the
logs; passing strict workspace Clippy does not mean the entire log is warning-free.

The host memory watchdog terminated the first overlay compilation before tests
ran. The full scope was retried successfully using
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
CARGO_BUILD_JOBS=1`, with Cargo overrides
`profile.dev.package.brewfs.codegen-units=256` and
`profile.test.package.brewfs.codegen-units=256`. No product setting, assertion,
or ignore marker changed. Logs and per-command exit codes are preserved in
`/home/hxy/pr396-selfreview/`, including the interrupted attempt. The source diff
against `773d8e10` has SHA-256
`6fe49740d62d16dc9d9f70e0527b7a552fa9ecf3383415ce9733b4456bc77b0d`.

## Remaining work

End-to-end durability/drain remains unresolved in #391. The ID refill admission bounds
only allocation metadata; it does not bound every writer task or the backlog of
uploaded slices awaiting publication. A separate, ordered admission change
requires regression tests and matched write/mixed-workload evidence.

Small objects remain: the initial failed run's mean is approximately 4 KiB,
compared with the historical original baseline's 4,167.15 B/PUT and the previous
refill-only failed candidate's 4,126.59 B/PUT. These are failed runs and are not
evidence of an aggregation improvement. No README performance table is updated.
