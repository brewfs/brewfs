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
The 512 KiB performance measurement is pending; the 256 KiB numbers above
must not be attributed to it.

Rollback: omit the new parameter or retain the old immutable manifest. No
default cap changes. 64 KiB frames and small-file bundles remain separate
experiments; Merkle and new wire semantics are outside this change.
