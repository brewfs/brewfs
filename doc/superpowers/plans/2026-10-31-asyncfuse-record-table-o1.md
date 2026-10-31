# Indexed FUSE OPEN records and complete reply observation

## Scope and invariants

The 16-worker, 512-background configuration allocated 8,192 OPEN records.
`ControllerHandle::reap` scanned the table on completion/capacity/drop paths,
and `wrap_packet` scanned it by FUSE unique, including unsuccessful lookups
for non-OPEN replies. This change keeps that capacity and replaces the scans
with a fixed hash index and a free list for each worker.

- Unique lookup is expected O(1), with randomized hashing and fixed collision
  chains. Removal of a known slot and free-list insertion/removal are O(1).
  No allocation or table growth occurs during lookup, insertion, or retirement.
- A Box retirement checks its own slot. Each tracked reply clone retains its
  unique; its destructor releases the actual Bytes before checking that unique.
- A record remains live until the Box has retired and the table owns the last
  packet reference. Actual packet allocation, original request/InflightGuard,
  and independent root grant retire in the original order, outside the mutex.
- Slots remain assigned to their original worker. The existing completion
  polling order, queue capacities, cancellation behavior, and admission limits
  are unchanged. `max_background` remains 512 by default.
- All storage is charged before allocation. At 16 x 512 the fixed startup
  storage charge changes from 1,442,816 to 1,639,624 bytes (+196,808 bytes).
  The budget limit is unchanged. The allocator witness now observes all six
  real allocations and tests all six startup allocation failure points.

The reply observer now reuses `reply_tracker::wire_header(&data,
extend_data.as_deref())`. Guarded metadata/close packets with an empty first
buffer therefore close their observation records after the existing reply
submission boundary. Three production reply-pump socket tests failed before
the fix and pass afterward, covering metadata, close/error, and READ layouts.

The required overlay gate also exposed an existing `2be1c16` regression:
extracting `decode_metadata_stored` removed GroupMetadata Authentication and
Decode timers. The original timer boundaries were restored in the shared
helper, including the bundle path; validation, budgets, caches, and assertions
were not changed. The initial failure and parent-source comparison are saved.

## Reproduction and provenance

Artifacts are under `/root/smallfile-attrib/`; summarized evidence and test
logs are under `/root/smallfile-attrib/record-table-o1/`. The original `run.py`,
`scan.py`, and `analyze.py` were not modified. `input-sha256.json` records their
digests. `acceptance-summary.json` contains every run's counters, cleanup,
timings, and trace distributions.

Baseline: supplied `2be1c16` plus staged observation prerequisite `bc9fd83`.
Its saved binary SHA-256 is
`7baf0fc03cf928882a4390788596437904791f0bbabf73b8aea5910e5f703364`.
Candidate binary SHA-256 is
`88cea059bf88b47b13f3a217dbac9b2f9a83fb1556ef90b0791775b16e9b5d75`.
Both binaries are retained as `brewfs-before` and `brewfs-after` in the evidence
directory. Candidate release build used `--features workspace-overlay`, matching
the supplied baseline, with release debug disabled.

The matched scan uses 8,192 x 4 KiB files, SHA-256 verification, workers=16,
background=512, RustFS 1.0.4, compression=none, direct reads, cache TTL=0,
kernel keep-cache disabled, root bundle prefetch, and bundle cache
`67108864` integer bytes. Decoded-frame/window caches and background prefetch
remain disabled. Observation is enabled at sample rate 1.0 in both binaries.
Mount startup warms the bundle; remaining workload GETs are measured as deltas
between the harness's first and last stats snapshots.

```bash
python3 /root/smallfile-attrib/run.py <label>
python3 /root/smallfile-attrib/run.py <trace-label> --batch 16
python3 /root/smallfile-attrib/run.py <perf-label> --perf
# Reference only: the old binary with --max-background 32
```

No build or other benchmark overlapped the accepted runs. Two earlier captures,
`o1-before-continuous` and `o1-before-perf`, overlapped compilation and are
preserved as diagnostics, excluded from all accepted comparisons.

## Small-file result: three groups

Times below are mean open + read + close + SHA-256 time per file. Continuous
and perf-instrumented results are shown separately to expose profiling cost.

| Metric | Before, background=512 | After, background=512 | Old binary, background=32 reference |
| --- | ---: | ---: | ---: |
| Continuous ms/file | 0.606600 (3-run median) | 0.307235 (3-run median) | 0.333391 (1 run) |
| Continuous daemon user CPU seconds | 4.66 (median) | 2.09 (median) | 2.33 |
| Perf-instrumented ms/file | 0.603256 | 0.369764 | 0.363719 |
| Perf daemon user CPU seconds | 4.69 | 2.45 | 2.42 |
| `reap` self CPU | 32.60% | 0.00% sampled | 3.69% |
| `wrap_packet` self CPU | 6.75% | 0.29% | 0.51% |
| Runtime GET delta | 20 | 20 | 20 |
| Runtime received-byte delta | 2,830,702 | 2,830,702 | 2,830,702 |
| SHA-256 verified files | 8,192 | 8,192 | 8,192 |
| Unmount / daemon exit | 0 / 0 | 0 / 0 | 0 / 0 |

Continuous repeats (ms/file): before `o1-baseline-{1,2,3}` =
0.638455, 0.589254, 0.606600; after `o1-after-{1,2,3}` =
0.306904, 0.308614, 0.307235. Median elapsed cost falls **49.35%**, corresponding
to **97.44% more files/s**. Against the task's 0.6031 ms/file reference, elapsed
cost falls **49.06%**, exceeding the 25% requirement. The historical 0.365977
background=32 number is a separate supplied reference, not a fresh measurement.

Perf artifacts: `o1-baseline-perf`, `o1-after-perf`, and
`o1-reference-32-perf`. Sampling uses cpu-clock at 499 Hz with DWARF call graphs;
reports use self overhead (`--no-children -g none`). No lost samples were
reported. The candidate `reap` symbol exists in `nm`; all 326 sampled BrewFS
symbol rows resolve, with no unknown BrewFS addresses. Zero means no sampled
hits in this short capture, not mathematically zero CPU cost. The old
background=32 report uses `baseline-symfs` and the saved old binary, avoiding
mis-symbolization after replacing the executable at the original path.

Every accepted scan, including trace/reference runs, independently verifies
GET=20, bytes=2,830,702, all 8,192 hashes, successful normal unmount, daemon
exit=0, and no remaining mount. No SIGTERM fallback was needed.

## Observer closure and READ distributions

Use `o1-baseline-trace` and `o1-after-trace`, both `--batch 16`:

| Metric | Before | After |
| --- | ---: | ---: |
| Received / sampled | 58,376 / 58,376 | 58,376 / 58,376 |
| Closed | 8,192 | 58,376 |
| Closure | 14.0332% | 100% |
| Active at final snapshot | 50,184 | 0 |
| `brewfs_request_observation_dropped_total` | 0 | 0 |
| Cancelled / error observations | 0 / 0 | 0 / 0 |
| Collected closed traces | 8,192 | 58,376 |
| READ traces | 8,192 | 8,192 |
| READ partition error / ambiguous ns / truncated intervals | 0 / 0 / 0 | 0 / 0 / 0 |

`brewfs_request_trace_records_dropped_total` separately counts overwrites of
the unchanged 256-record history ring: **7,936 -> 58,120**, exactly
`closed_total - 256`. Snapshots are non-destructive. This counter necessarily
increases when more requests close; it is not observation admission loss.
The batched collector recovered every closed record, including all 58,376
candidate records. No buffer cap, sampling rate, or drop-counter definition
was changed to make closure pass.

READ exclusive timings in nanoseconds (each distribution has 8,192 samples):

| Phase | Mean before -> after | p95 before -> after | p99 before -> after |
| --- | ---: | ---: | ---: |
| Total | 101,621 -> 61,218 | 131,443 -> 77,320 | 143,630 -> 85,543 |
| Queue wait | 12,982 -> 5,084 | 16,650 -> 5,657 | 28,895 -> 14,635 |
| File lock wait | 735 -> 572 | 860 -> 765 | 979 -> 863 |
| Chunk lock wait | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Dirty merge | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Flush wait | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Metadata RPC wait | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Metadata commit | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Object PUT queue wait | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Object PUT | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Reply | 42,152 -> 18,188 | 56,965 -> 22,648 | 72,395 -> 32,139 |
| Handler residual | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Handler poll | 45,752 -> 37,374 | 60,117 -> 50,437 | 74,475 -> 57,053 |

The ten requested phases retain their meaning. Handler poll/residual remain
separate partition components. This read-only workload does not exercise
write-only phases; zero rows are not claims about write-path performance.

## Correctness gates

- Workspace `cargo test --workspace --lib --bins`: 1,291 library tests and
  2 binary tests passed, 0 failed, 262 existing ignored.
- Required `cargo test -p brewfs --lib --features workspace-overlay`:
  **2,596 passed, 0 failed, 451 existing ignored**.
- Standalone asyncfuse Tokio: 66 passed, 0 failed, 0 ignored.
- Standalone asyncfuse io-uring: 102 passed, 0 failed, 15 existing ignored.
- New coverage includes hash collision removal, per-worker backpressure,
  delayed callbacks after slot reuse, simultaneous final packet drops,
  cancellation without a packet, zero capacity, and reentrant guard Drop.
- No tests were ignored or assertions relaxed. Existing allocation/refund
  assertions were extended to cover the new bucket/free-head allocations.
- All four shell syntax checks and the three required compose report/matrix
  tests passed; `test_request_observation.sh` also passed.
- `cargo check --workspace`, `cargo build --workspace`, both no-default-feature
  BrewFS runtime checks, standalone asyncfuse async-io check, workspace Clippy,
  and the workflow's `cargo clippy --workspace --all-targets -- -D warnings`
  all passed. Formatting and both staged/unstaged whitespace checks passed.
  `rfuse3` is not a workspace member, so the workflow's conditional rfuse3
  checks do not apply. Commands and exit codes are in `remaining-checks.json`.

## Compose regression

The first old-binary broad run, `perf-run-1793426357-32742`, exhausted the host
filesystem after the write scenes and produced EIO in subsequent operations.
It is **invalid and excluded**, with its complete artifacts and nonzero runner
exit retained. Compose services/volumes were removed. After all local CI gates
passed, rebuildable `target/debug` and `target/release` were removed, preserving
the saved before/after binaries and all measurements. The candidate executable
was restored to `target/release/brewfs`. Available host space recovered to
approximately 36 GiB before a fresh identical baseline run.

That second cumulative old-binary run, `perf-run-1793426686-20293`, also exhausted
the 59 GiB host filesystem during the mixed-workload stage. It is likewise
invalid and retained, with services/volumes cleaned. Consequently the broad
regression is run as paired, isolated compose invocations for each fio scene
and a final metadata group (`metaperf dirstress dirperf`), removing volumes
between invocations. The 512 MiB working set, 20-second runtime, per-scene
numjobs/block size, direct mode, cache budgets, and strict drain checks are
unchanged. Before/after order alternates by scene. Direct-IO write guard scenes
are similarly isolated. This changes cross-scene retained data, so only the
matched isolated artifacts are used for acceptance, never the failed combined
runs. `run-scene-matrix.py`, `run-compose.py`, and `scene-matrix.json` preserve
the orchestration and exact commands/binary hashes.

Compose regression results will be appended before commit.
Full native xfstests/pjdfstest and managed-backend JuiceFS comparisons
are outside this task's measured scope.
