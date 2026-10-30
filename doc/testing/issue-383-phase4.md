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
wire limit and `SizeClassTable::validate` before creating the backend. The
minimum remains 256 KiB and the sequential maximum remains 8 MiB. Omitting
the option preserves the existing 4 MiB random cap. The same table reaches
directory capture, single-file capture, and generated corpus packing.

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

Rollback: omit the new parameter or retain the old immutable manifest. No
default cap changes. 64 KiB frames and small-file bundles remain separate
experiments; Merkle and new wire semantics are outside this change.

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

The added minimum-size CLI and its new tests are reverted together; existing
tests are unchanged. The complete candidate patch and measurements remain in
`rejected-64-source.diff` and `cap64k-*` on the experiment host. The validated
512 KiB entry point stays. Merkle remains a separate future investigation.
