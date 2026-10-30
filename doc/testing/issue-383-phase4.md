# Issue 383 phase 4: smaller authenticated packed frames

Date: 2026-10-30. This continues [phase 2](issue-383-phase2.md). The full report
is `/work/REPORT-383-phase4.md`; raw fio, HTTP captures, object inventories,
build logs and scripts are retained under `/work/issue383-phase4/` and
`/work/logs/issue383-phase4/` on the experiment host.

## Step 0: existing static 256 KiB policy

Repacking the same four 512 MiB files with `--frame-policy static-256kib`
reduces median **received bytes/read from 4,194,304 to 262,144 B**. Each arm
uses two 30-second warmups and three 30-second measured fio passes: 4 KiB
random read, direct=1, libaio, four jobs, depth 32, seed 20261029. A warmup is
a timed run, not a complete corpus sweep. No compiler runs during fio.

| Arm / run | MiB/s | p99 seconds | Received body B/read | GET/read |
| --- | ---: | ---: | ---: | ---: |
| 4 MiB / 1 | 0.205606 | 2.667577 | 4,194,304 | 1.000 |
| 4 MiB / 2 | 0.205968 | 2.701132 | 4,194,304 | 1.000 |
| 4 MiB / 3 | 0.203738 | 2.701132 | 4,194,304 | 1.000 |
| 256 KiB / 1 | 1.241820 | 0.455082 | 262,144 | 1.000 |
| 256 KiB / 2 | 1.239601 | 0.455082 | 262,116.49 | 1.000 |
| 256 KiB / 3 | 1.244554 | 0.446693 | 262,144 | 1.000 |

Median throughput improves 6.04 times and p99 falls 83.15%. All three runs
fit the design's 0.69–1.35 MiB/s and 0.42–0.82 s scenarios. Received bytes
sum actual HTTP response Content-Length values; HTTP/TCP headers are excluded.
The second candidate run has 9,522 payload GETs and one 164 B GroupMeta GET
for 9,523 successful reads. Its actual average is preserved above. Concurrent
singleflight can share payload fetches, but this run was not correlated per
request to establish that explanation.

The historical 4,198,891 B example included cold GroupMeta and FD metadata.
These W arms already have A metadata caching: **this is not a 3-to-1 GET
improvement**. Both arms use the identical historical phase-2 A-arm release
binary, with its rejected B frame cache explicitly disabled. That executable
contains disabled prototype code; it is not presented as a rebuild of the
current source. A fresh current-source A-only binary will provide the next
matched control.

The compose TiKV 8.5.0 / RustFS 1.0.4 backend was recreated after prior cleanup.
The source hashes and fio parameters remain unchanged; each arm uses a fresh
upper over its own immutable manifest. Requested metadata/data codecs remain
Zstd, with all payload frames actually falling back to Raw. Upper remains
64 MiB chunks / 4 MiB blocks. Ordinary read budgets remain 300 MiB RAM / 1 GiB
SSD, with no prefetch and default packed budgets.

| Actual inventory | 4 MiB | 256 KiB |
| --- | ---: | ---: |
| Frames / FD records / external extent records | 512 / 512 / 512 | 8,192 / 8,192 / 8,192 |
| FD record bytes | 24,576 | 393,216 |
| External extent record bytes | 40,960 | 655,360 |
| Data objects / FD objects | 128 / 128 | 128 / 128 |
| Total objects | 270 | 302 |
| Total stored bytes | 2,148,735,549 | 2,149,858,442 |

The stored increase is 1,122,893 B (1.071 MiB, 0.0523% of the corpus), including
index pages and routing. FD/extent records alone add 0.9375 MiB. Experiment
prefix lengths differ by one byte, contributing a small routing overhead.
Inventory metadata objects are verified against their SHA-256 object keys;
FD and LE09 records are counted from actual bytes.

Both arms pass 60 direct content/boundary/EOF reads and 3×128 metadata rounds
(create/fsync/stat/rename/readdir/readback/unlink). Sequential 1 MiB reads are
49,187/49,374/48,834 KiB/s before and 111,976/112,953/111,118 KiB/s after.
Metadata rounds take 14.815/14.988/14.873 s before and 14.358/14.469/14.578 s
after. All read guards and W mounts exit zero and unmount.

Both mixed-write warmups still fail with fio EIO, as in phase 2; both mounts
exit 1 and fully unmount. There is **no mixed throughput or drain acceptance**.
No broad README performance table is updated. Read-only effective wall
bandwidth improves 481–510%; no post-write drain figure is synthesized.
Run3 CPU sampling estimates SHA work at 11.665 versus 0.797 ms/read.

## Step 1: explicit random frame cap

The fixture accepts `--max-random-frame-raw-bytes 524288`. It validates the
wire limit and `SizeClassTable::validate` before creating the backend. In this
step the minimum stayed 256 KiB, the default random cap stayed 4 MiB and the
sequential maximum stayed 8 MiB. The same table reaches directory capture,
single-file capture, and generated corpus packing. Phase 6 below changes the
default and restores the explicit 64 KiB experiment.

`build_provenance.size_classes` is read back from the authenticated manifest,
so recorded sizes describe the published layout. PM11 already authenticates
this table; the reader, format and authentication protocol are unchanged.
No p90 training input is fabricated. New tests build and reopen all three
fixture paths, checking actual frame counts and provenance, and reject invalid
tables and raw-corpus combinations. Existing test assertions are unchanged.

The old release CLI first failed this contract with exit 2 (`unexpected
argument`); the implementation then passes all 11 fixture tests. The current
workspace lib/bins gate and overlay gate pass (2578 passed, zero failed,
451 ignored). Full local checks are rerun before committing the entry point.
The exact current-source A-only release has a separate matched comparison:

| Three W runs | 4 MiB | 512 KiB |
| --- | --- | --- |
| MiB/s | 0.181770 / 0.175277 / 0.176027 | 0.909601 / 0.928701 / 0.905396 |
| p99 seconds | 3.070231 / 3.204448 / 3.103785 | 0.633340 / 0.599785 / 0.624951 |
| Received B/read | 4,194,304 each | 524,137.97 / 524,067.59 / 524,288 |
| GET/read | 1.000 each | 0.999714 / 0.999720 / 1.000 |

All three 512 KiB runs meet the design intervals. Median throughput improves
5.167 times; all fio and captured HTTP errors are zero. Run 2 misses the final
GET response header, so its received-byte value is an observed lower bound;
an independent capture with a longer tail is required for complete byte counts.
That confirmation is complete: W retains two warmups and three 30-second runs,
with passive capture extended to five seconds after fio. All response headers
and wire tails are complete. Each run receives **524,288 B/read at 1.000 GET/read**;
throughput is 0.868619/0.864122/0.863688 MiB/s, p99 is
0.616563/0.624951/0.624951 s. All fit the design intervals. Median throughput
improves 4.909 times over the matched 4 MiB control. These complete counts are
the confirmation result; the faster original samples remain preserved.
The slight GET/read reduction is reported as observed, not attributed to a new
GET optimization. Inventory has 4,096 frames/FD records/external extents, still
128 data and 128 FD objects. Total stored growth over this matched control is
526,344 B (0.024510%); FD+extent records alone add 458,752 B.

This control is slower than the historical step-0 binary; the cause is not
established. It passes 60 direct content/boundary reads but fails sequential
warmup with EIO at r.0 offset 65,011,712, length 1 MiB. All 936 HTTP GETs have
responses, no HTTP errors; mount exit is zero with complete unmount. The failure
is retained in `control4m-final-readguard` and is not classified as a harness
race. The old manifest with this new reader also fails on its third sequential
pass (r.2 offset 235,929,600, length 1 MiB), so the failure is not confined to
the new fixture. The reader cause remains unverified. The 512 KiB arm passes
60 content reads, sequential 134,041/132,971/129,118 KiB/s and metadata rounds
14.747/15.058/14.874 s with clean teardown; mixed still fails with EIO.

Rollback: pass `--max-random-frame-raw-bytes 4194304` or retain the old
immutable manifest. The 64 KiB experiment and small-file bundles are measured
separately below; Merkle and new wire semantics are outside this change.

## Step 2: rejected 64 KiB complete frames

The conditional experiment used min=random=64 KiB, sequential=8 MiB. The
complete local gate passed, including 2578/0/451 overlay tests and 13 fixture
tests. New build/readback tests verified exactly 256 extents stay in GroupMeta,
while a 17 MiB file and 300 sparse extents use existing external placement,
preserving holes and hardlinks. No existing limit or assertion was increased.

| W run | MiB/s | p99 seconds | Received B/read | GET/read |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1.760396 | 0.329253 | 65,536 | 1.000 |
| 2 | 1.772195 | 0.325059 | 65,536 | 1.000 |
| 3 | 1.752194 | 0.333447 | 65,536 | 1.000 |

All headers and response tails are captured. Actual frame/FD/external extent
counts are 32,768 each, with 128 data and 128 FD objects. Records grow by
3.9375 MiB (0.192261%); total stored growth including pages/routing is
4,684,171 B (4.46717 MiB, 0.218124%). Random-read estimates hold, but the
candidate is **rejected**: sequential reads fall to 48,552/48,715/48,183 KiB/s
from 134,041/132,971/129,118 KiB/s at 512 KiB, a 63.49% median regression.

The sequential cost assumption was wrong: capture shows about 16 GETs per
1 MiB read at 64 KiB, versus two at 512 KiB, with approximately the same total
received bytes. The available coalescing did not take effect in this FUSE
workload; the exact planner cause remains unverified. Metadata rounds pass in
14.292/14.362/14.517 s, but mixed still fails with EIO. Both mounts unmount.

At this step the added minimum-size CLI and its new tests were reverted
together; existing tests were unchanged. The complete candidate patch and measurements remain in
`rejected-64-source.diff` and `cap64k-*` on the experiment host. The validated
512 KiB entry point stays. Merkle remains a separate future investigation.

## Phase 6: default random frame decision

New volumes now default to 256 KiB random/mixed frames. The library behavior
change is one constant in `layout.rs`; the default minimum stays 256 KiB and
the sequential maximum stays 8 MiB. PM11 persists the table and frame directories
persist actual lengths. Reader logic, wire format, validation upper bounds,
ACL TTL (`ZERO`) and upper 64 MiB chunks / 4 MiB blocks are unchanged.

The fixture also permits an explicit `--max-random-frame-raw-bytes 65536`:
this lowers its build-time minimum to 64 KiB. The prior entry point rejected
that value after the step-2 rollback; phase 6 restores it only as an explicit
experiment, as requested. The default minimum remains 256 KiB. Existing tests
whose assertions require legacy frame shapes now request the old table
explicitly; their layout, budget and cancellation assertions are retained.

### Four-size evidence and decision

Values below are medians of three runs: random is 4 KiB direct I/O W (two
warmups and 3×30 s); sequential is 1 MiB direct I/O (3×20 s). Reader A is the
historical phase-2 executable with its rejected frame cache disabled. Reader B
is the phase-4 A-only build (`ed24ff6f…`). **These are two matched pairs, not
one four-arm matched benchmark.** Each row keeps random and sequential evidence
from its stated reader; no faster numbers are spliced between readers.
The historical 256 KiB arm used `static-256kib` with a legacy 4 MiB manifest
cap; its actual 8,192 frames are all 256 KiB. Phase 6 separately checks the
no-argument `size-only` policy and the new persisted 256 KiB cap.

| Actual random frame size | Reader | Random MiB/s | Random p99 ms | Sequential MiB/s | Mixed read/write |
| ---: | --- | ---: | ---: | ---: | --- |
| 4 MiB | A | 0.205606 | 2701.132 | 48.034 | EIO; no accepted throughput |
| 512 KiB | B, complete-capture rerun | 0.864122 | 624.951 | 129.854 | EIO; no accepted throughput |
| 256 KiB | A | 1.241820 | 455.082 | 109.352 | EIO; no accepted throughput |
| 64 KiB | B | 1.760396 | 329.253 | 47.414 | EIO; no accepted throughput |

The matched A comparison gives 6.04× random throughput and 83.15% lower p99.
The B 4 MiB random control is 0.176027 MiB/s / 3103.785 ms, but its sequential
guard fails with EIO, as preserved above. A bridge using the old 256 KiB
manifest with reader B reports 1.185452 / 1.188626 / 1.176509 MiB/s and
459.276 / 463.471 / 492.831 ms; its sequential run used DEBUG logging and is
not a matching throughput control. A universal 6.9× / −85% claim would mix
reader provenance and is therefore not used for this change.

The decision is 256 KiB: random reads improve while sequential performance
stays well above the 64 KiB experiment. **64 KiB is rejected as a default:**
its approximately 47 MiB/s sequential result is effectively tied with the
historical 4 MiB result and 63.49% below its matched 512 KiB control. It also
needs 16 GETs per 1 MiB read and more frame/index records. Mixed-write EIO
remains an unresolved guard, not a passed performance gate; this decision
does not claim broad mixed-workload acceptance.

### Reproducing the CLI/FUSE compatibility regression

`tools/testing/packed_frame_default_compat.py` packs real LocalFS objects on disk,
launches the supplied reader as a separate FUSE process, checks complete SHA-256
contents plus direct boundary/EOF reads, and requires clean unmount. Its corpus
has a 5 MiB+123 B file, a 17 MiB+7 B file and an empty file. The three arms use
an explicit old 4 MiB table, no frame-size/policy arguments, and explicit
64 KiB. The manifest table and actual FD records are checked against expected
frame counts 7, 90 and 354, respectively. Both nonempty files have partial
tail frames; the 64 KiB arm also exercises external extent placement.

```bash
cargo build --release --features workspace-overlay \
  --bin brewfs --bin packed_v3_snapshot_fixture
python3 tools/testing/packed_frame_default_compat.py \
  --fixture target/release/packed_v3_snapshot_fixture \
  --legacy-fixture /path/to/pre-change/packed_v3_snapshot_fixture \
  --reader target/release/brewfs \
  --artifact /tmp/packed-default-compat-new-directory
```

Linux FUSE and `fusermount3` are required. The artifact directory must not
already exist. Omitting `--legacy-fixture` tests an explicitly requested old
table produced by the current fixture; supplying the old executable additionally
proves cross-binary compatibility. Artifacts retain binary hashes, commands,
manifest/object bytes, full file hashes, direct-read results and teardown status.

The pre-change executable supplies the red check: it successfully packs and
mounts the explicit 4 MiB arm, then the no-argument arm fails the new table
assertion with `(262144, 4194304, 8388608)`. The retained log is
`/work/issue383-phase6/logs/compat-before.log`; its objects are in
`/work/issue383-phase6/before`. This checks that the process-level regression
detects the old default rather than merely reopening a synthetic fixture.

The overlay suite passes **2580 / 0 / 451** (passed / failed / ignored), and
the fixture binary suite passes **13 / 0 / 0**. Both legacy and new manifests
call `validate()` and pass authenticated encode/decode roundtrips. Directory,
single-file and generated-corpus tests all verify no-argument 256 KiB frame
counts and authenticated provenance. The initial default change exposed
legacy-shape assumptions in 33 existing tests; those fixtures now select the
legacy table explicitly while retaining their assertions. No test was ignored.

The real FUSE regression passes with the new dev binaries in
`/work/issue383-phase6/compat-debug`: the **pre-change fixture executable**
(`906bda2f…`) packs the legacy arm with `--max-random-frame-raw-bytes 4194304`;
the new reader (`1a5a355e…`) mounts and reads it. Its largest actual frame is
4,194,304 B, not merely a 4 MiB table over small frames. All three full-file
SHA-256 digests match the source, as do 21 O_DIRECT boundary/EOF checks per arm.

| Actual CLI arm | Manifest min / random / sequential bytes | Actual frames | Readback / unmount |
| --- | --- | ---: | --- |
| Old executable, explicit 4 MiB | 262144 / 4194304 / 8388608 | 7 | SHA-256 match; 21 checks; exit 0 |
| New executable, no size/policy arguments | 262144 / 262144 / 8388608 | 90 | SHA-256 match; 21 checks; exit 0 |
| New executable, explicit 64 KiB | 65536 / 65536 / 8388608 | 354 | SHA-256 match; 21 checks; exit 0 |

All local commands listed in `AGENTS.md` pass in this iteration (completed
2026-10-31 05:48 CST; logs in `/work/issue383-phase6/logs`). In particular,
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins`
passes 1275 library tests (0 failed, 262 ignored) and 2 binary tests (0 failed).
Formatting, the four shell syntax checks, three report/matrix shell tests,
workspace check/build/clippy, and the Tokio/io-uring feature checks all pass.
`rfuse3` is not a workspace member, so its conditional checks do not apply.
The extra overlay library/fixture clippy check with `-D warnings` also passes.
Long commands run in tmux with `CARGO_BUILD_JOBS=2`.
