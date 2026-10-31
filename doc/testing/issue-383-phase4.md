# Issue 383 phase 4: smaller packed frames and optional small-file bundles

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
current source. Step 1 uses a fresh current-source A-only binary for its
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
451 ignored). Full local checks passed before committing the entry point.
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

## Step 3: optional container ranges for small files

`--small-file-bundles` applies only to directory imports with inline data on.
It starts another group before the existing 224 KiB inline budget fills,
separates inline groups from external/metadata-only groups, and batches up to
128 all-inline groups in an existing GC05 container. Existing GR05 locators
authenticate contiguous metadata plus inline-content ranges, each at most
256 KiB raw/stored. There is no per-bundle S3 object, self-referential FD digest,
new wire format, or reader change. The option is off by default.

Allocation and cold-attribute records publish after the batch's locators.
New tests cover all-inline coverage, adjacent authenticated ranges, 129-parent
rollover, hardlinks, xattrs, symlinks and st_blocks, plus invalid CLI combinations.
The separate FUSE mixed-layout guard passes 122 content reads, including pending
inline groups alongside a 17 MiB external file and its hardlink, then unmounts.
This content test is distinct from the failing fio mixed-write guard above.

The source-report flag records requested intent; authenticated PM11 supplies
the actual inline/frame counts. No authenticated policy field is invented.
Directory summaries and new bundle/FD formats remain unimplemented. Rollback
is to omit the flag and retain the previous immutable manifest.

The small-file experiment uses 8,192 distinct deterministic 4 KiB files
(32 MiB), Raw metadata/data on both arms, identical default frame sizes,
and the same A-only release reader. The offline fixture uses the tested dev
build; producer time is excluded. Each scenario has three repetitions:
16 independent fresh-mount cold open/read samples per repetition, a complete
shuffled scan, an adjacent scan, and pure readdir. All file reads use O_DIRECT
and verify SHA-256; readdir verifies the exact name set. Mount bootstrap and
passive capture tails are outside the active operation timer.
Cold means the first file access on a fresh mount; object-server and host page
caches are not cleared. The build-time flag cannot switch policies per operation
within an existing immutable manifest.

`tools/perf/packed_bundle_usefulness.py` reports verified consumed content
bytes divided by actual complete GC05 HTTP response bodies, including metadata.
Index bytes are included in total received bytes. Attribution requires an
authenticated all-inline layout; the baseline's mixed layout has no bundle
ratio. Pure readdir consumes zero file-content bytes, so its content ratio is
zero and directory time/GETs/bytes are assessed separately. Missing response
headers or body tails fail accounting rather than substituting requested ranges.

One baseline shuffle capture lacks the final response header despite 8,192
successful content checks, zero captured HTTP errors and zero kernel drops.
It remains in `small-control-shuffle-3`; only this invalid capture is repeated
under a new label. The passive capture tail increases from one to five seconds;
completed valid repetitions are retained. The capture cutoff cause is unverified.

| Actual small-file inventory | Default | Bundles |
| --- | ---: | ---: |
| Groups | 47 | 147 |
| Frames / FD records / external extents | 92 / 92 / 0 | 0 / 0 / 0 |
| FD record bytes / external extent bytes | 4,416 / 0 | 0 / 0 |
| Inline content bytes | 10,567,680 | 33,554,432 |
| GC05 containers / FD objects | 47 / 46 | 2 / 0 |
| All objects | 131 | 40 |
| All stored bytes | 40,296,141 | 39,793,827 |
| Stored bytes excluding original payload | 6,741,709 | 6,239,395 |

Storage decreases 502,314 B, or 1.497012% of the corpus. Newly inlined content
is still payload, not structural metadata. The largest bundle range is
233,764 B, within the existing bound.



Each row below is one complete repetition. Cold p99 is the empirical maximum
of 16 fresh-mount reads. Each readdir row is one full directory traversal;
its p99 column is that single traversal's duration, not a tail estimate.
Readdir bytes/GETs are per traversal, while bytes per entry are 4222.93→4300.37.

| Scenario / layout / run | MiB/s | p99 s | Received B/operation | GET/operation | bundle useful-byte |
| --- | ---: | ---: | ---: | ---: | ---: |
| cold / control / 1 | 0.154463 | 0.028664 | 748553.94 | 7.875000 | N/A |
| cold / control / 2 | 0.155465 | 0.028527 | 751625.94 | 7.875000 | N/A |
| cold / control / 3 | 0.149441 | 0.029988 | 769047.62 | 8.062500 | N/A |
| cold / bundle / 1 | 0.178609 | 0.023065 | 565070.00 | 6.000000 | 1.7522% |
| cold / bundle / 2 | 0.177557 | 0.022561 | 565069.62 | 6.000000 | 1.7522% |
| cold / bundle / 3 | 0.179324 | 0.023423 | 565069.88 | 6.000000 | 1.7522% |
| shuffle / control / 1 | 0.402282 | 0.022033 | 254398.53 | 1.028687 | N/A |
| shuffle / control / 2 | 0.398324 | 0.023451 | 252925.90 | 1.022705 | N/A |
| shuffle / control / 3 | 0.399572 | 0.021789 | 255690.84 | 1.033936 | N/A |
| shuffle / bundle / 1 | 0.461577 | 0.013715 | 180860.89 | 0.776001 | 2.2691% |
| shuffle / bundle / 2 | 0.459470 | 0.014527 | 181362.30 | 0.778320 | 2.2628% |
| shuffle / bundle / 3 | 0.461540 | 0.013750 | 180962.80 | 0.776611 | 2.2678% |
| adjacent / control / 1 | 0.465209 | 0.013155 | 173337.41 | 0.699097 | N/A |
| adjacent / control / 2 | 0.465691 | 0.013330 | 173337.41 | 0.699097 | N/A |
| adjacent / control / 3 | 0.462656 | 0.013220 | 173337.41 | 0.699097 | N/A |
| adjacent / bundle / 1 | 0.728032 | 0.009583 | 4520.44 | 0.020508 | 98.1238% |
| adjacent / bundle / 2 | 0.723895 | 0.009693 | 4520.44 | 0.020508 | 98.1238% |
| adjacent / bundle / 3 | 0.723988 | 0.009591 | 4520.44 | 0.020508 | 98.1238% |
| readdir / control / 1 | 0.000000 | 0.650187 | 34594216.00 | 146.000000 | N/A |
| readdir / control / 2 | 0.000000 | 0.658371 | 34594216.00 | 146.000000 | N/A |
| readdir / control / 3 | 0.000000 | 0.652634 | 34594216.00 | 146.000000 | N/A |
| readdir / bundle / 1 | 0.000000 | 0.581704 | 35228616.00 | 155.000000 | 0.0000% |
| readdir / bundle / 2 | 0.000000 | 0.581750 | 35228616.00 | 155.000000 | 0.0000% |
| readdir / bundle / 3 | 0.000000 | 0.564573 | 35228616.00 | 155.000000 | 0.0000% |

For this measured workload, cold open/read and shuffle support explicit opt-in
(15.63% and 15.51% median throughput gains), but retain only 1.75% and about
2.27% useful bundle bytes. Adjacent scans justify opt-in: throughput improves
55.63%, received bytes/read fall 97.39%, and useful content reaches 98.12%.
Pure readdir should keep the default: time improves 10.87%, but received bytes
increase 1.83%, GETs increase 6.16%, and no file content is consumed. The global
default remains off; these results do not establish a million-file policy.

The cold candidate's six GETs comprise one bundle plus five index requests;
about 331,308 B/read is index data. One bundle GET is not one full-path GET.

The predicted readdir amplification was wrong for this workload: the model
assumed each baseline group would be fetched once. Capture instead shows 141
fetches for 47 distinct baseline ranges, versus 150 for 147 candidate ranges.
That baseline repetition offsets most of the group-count growth. Its precise
cache/iterator cause remains unverified; this small-workload timing improvement
is not generalized to large directories. All valid repetitions have complete
response headers/body tails, no captured HTTP errors, and clean unmounts.

Final source validation passes the full local CI gate, including workspace
lib/bins, overlay **2578 passed / 0 failed / 451 ignored**, all **14 fixture
tests**, both FUSE runtimes, shell/report tests, fmt and strict Clippy. Logs are
in `/work/logs/issue383-phase4/step3/`. No prior assertions or ignore attributes
change. ACL permission TTL remains zero; existing layout limits and upper
64 MiB chunks / 4 MiB blocks remain intact.
