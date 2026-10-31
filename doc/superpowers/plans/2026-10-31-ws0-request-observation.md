# WS-0 request observation: implementation audit

Status: **draft, not accepted; Bobs is not frozen**.

Source baseline: `80ddaf739e163c8933b0bb84390c4aa559bd2b40`.
Working branch: `perf/request-latency-observation`.
No P-W/P-S/P-M/P-C/P-O measurements or overhead ratios have been accepted.
The README performance tables are unchanged. Subsequent performance lines
must not treat this worktree as the accepted Bobs integration point.

## Implemented instrumentation and collection

- Detailed request observation is opt-in through `BREWFS_REQUEST_TRACE`, with
  `BREWFS_REQUEST_TRACE_SAMPLE_RATE` controlling lifecycle sampling. Existing
  metric names and units remain available. New durations use nanoseconds.
- FUSE receive/worker/handler/reply and terminal callbacks, operation
  histograms, write-path timing hooks, and bounded recent request records are
  wired into the mount. Background writes carry a diagnostic cause.
- Versioned records now include bounded raw intervals, a recomputable disjoint
  partition, explicit residual/ambiguous time and separate background durations.
  Poll wall time is labelled `handler_poll`, never CPU time. Nested waits take
  precedence over poll intervals. Overflow is explicit and fails validation.
- Mount UUIDs prevent reused FUSE unique values from colliding after remount.
  The collector merges rolling snapshots and measures actual collection loss;
  ring eviction alone is neither success nor proof of collection loss.
- Per-poll context restores on return/unwind and does not cross an await or
  propagate to spawned tasks. Metadata RPC timers stop before cache updates.
  Actual ordinary-write inode/handle locks and backpressure waits are timed.
- Reply completion is recorded before enqueue, so a fast reply pump cannot
  beat the handler boundary. Duplicate completion preserves the first timestamp.
  Reply error status is counted; `.stats` inode operations are excluded from
  the trace lifecycle denominator (parent LOOKUP remains a metadata operation).
- Writeback metadata in-flight and memory reservation retry/rejection counters
  are exported. The runner refuses missing debt fields instead of converting
  them to zero and retains both the original clear point and extended drain.
- The runner appends fixed fio random parameters and latency percentiles while
  keeping named-profile prefill, remount and completion steps. Each sample
  reads `.stats` once, and raw samples are retained under `stats-timeseries/`.
- The comparison utility retains read/write metrics separately, adds mixed
  read+write effective bandwidth, records group latency sample counts, and
  distinguishes legacy drain evidence from extended drain evidence.
- `perf_lines_observe.py protocol-env` emits the shared P-ENV values, including
  explicit json+ output. `probe` measures application write/pwrite, independent
  dirty-close/close-after-fsync and fsync calls, and the 256-file metadata hot
  set at depths 1/8 and 1/4/16 processes. Exact per-worker histograms are retained;
  merging uses counts, never an average of P99s. These are diagnostic tools,
  and their completion does not itself establish strict drain.
- The compose runner exposes `ws0-io` and `ws0-metadata`, captures actual mount
  argv/environment/binary SHA and generated YAML, and samples raw process,
  pressure, disk and network counters. Unsupported proc/cgroup files remain
  explicitly unavailable. The WS-0 protocol refuses a mount configuration
  mismatch. The default flat/workspace FUSE TTL is explicit in generated YAML.

## Regressions found during continuation

1. The initial observation patch passed an ordinary WRITE's diagnostic unique
   into `creation_unique`. That field participates in existing overlap/reuse
   ordering. The new `observation_id_does_not_enter_normal_write_ordering`
   test failed with `creation_unique=200` instead of the legacy zero, including
   when tracing was disabled. Attribution now uses a separate
   `observation_unique`; ordering inputs remain unchanged. The test passes
   with observation both off and on and verifies the flushed data.
2. The initial report could label disabled or incomplete evidence `valid`,
   ignore lost bounded trace records, and sum overlapping/background segments
   into a 200% request wait share. Six regression tests demonstrated these
   failures. The report now preserves unavailable/invalid/disabled states,
   refuses missing manifests and runner failure markers, and does not publish
   a wait fraction from inclusive durations.
3. The initial sampler reread `.stats` for each metric. It now uses one raw
   snapshot per sample, avoiding inconsistent per-column windows. A runner
   regression verifies missing-field rejection, metadata debt rejection,
   three stable clear samples, and named-profile prefill/remount retention.
4. Rustfmt/clippy were initially absent. Both components were installed. The
   fixed original main also failed `cargo fmt --all --check` at two test-only
   expressions in `src/config.rs`; these were formatted without changing
   behavior. The B0 failure log is retained separately.
5. The first strict clippy run found eight warnings in the observation patch.
   Arguments were grouped into explicit request identity/context values and
   the suggested control-flow fixes were applied; no lint was suppressed.
   A subsequent feature check caught the new identity type's missing `Debug`
   derive, which was added. Initial failed logs remain part of the record.
6. Repeated handler completion changed the reply boundary and double-counted
   the handler histogram. A regression first failed, then passed with the
   idempotent boundary. Foreground timers at cancellation are clipped, and a
   late timer drop cannot change the already terminal request's wall time.
7. A named fio failure could be hidden by a successful later drain when the
   shell function was invoked through `||`. The exit-7 regression demonstrated
   the failure; the runner now preserves fio failure while retaining diagnostics.

## Evidence still required before acceptance

This is an implementation checklist, not a list of passed gates:

- The exclusive partition, cancellation/error boundaries, background split
  and <=5% residual gate have deterministic coverage. They still require a
  measured >=99% closure/<1% loss/<=5% residual pass at the chosen sample rate.
  The ring remains bounded to 256 records; its suitability under full workloads
  is not established by the collection unit tests. Tail groups with fewer than
  1000 samples are explicitly unreliable. A complete measurement window must
  exclude prefill; entire-mount summaries are labelled as such.
- Backend RPC/CAS/TiKV transaction stages, complete queue-domain coverage and full
  file/chunk/slice/lease and multi-request background causality need auditing.
  Having a context field or setter does not prove that a real lease was linked.
- App-side write/pwrite, independent 1000-call close/fsync samples, metadata
  per-operation histograms and the P-C/P-O deterministic probes have not run
  on the required compose matrix (small local probe tests are not acceptance).
  FUSE operation histograms are not application syscall histograms.
- Backend-side successful PUT/attempt/retry counters, active+close+drain+60s
  maintenance accounting and complete object/metadata ledgers remain incomplete.
  Process/IO/network sampling and actual mount argv/environment/YAML validation
  have been added, but full matrix evidence is still absent. Client
  PUT counters alone are not backend-side quiescence evidence.
- The workspace helper saves a hash of canonical `fork_base` JSON. This is an
  identity of that JSON, **not proof of the actual lower manifest hash** or a
  fully verified native/packed lower fixture recipe.
- B0/Bobs off/on paired AB/BA/AB runs and all required performance matrices
  are **not run**. The full pytest-full-02 trace and source-tree snapshot were
  not found at the supplied locations; that historical input is **unavailable**.
  No substitute trace or timeout is counted as completion.

## Validation artifacts and rollback

Local logs are in
`docker/compose-xfstests/artifacts/ws0-validation-20261031/`.
`gates.tsv` records command exit codes; a failed gate is not a pass even if
other focused tests succeeded. The red/green regression logs are preserved.

The xfstests archive initially contained only a Git LFS pointer. The real
archive was downloaded and retained in the validation artifact's
`assets/xfstests-prebuilt.tar.gz`; its SHA256 is
`fded9bc39c8b8f569efef91caff07601bf0cc344a630f328f92543c135261cf7`, matching
the pointer. `gzip -t` passes, and the archive contains the actual metaperf,
dirstress and dirperf executables. This is tool preparation, not a P-M run.
The Redis `7.2-alpine` image also passed the pull preflight; its digest is
recorded in `redis-image-preflight.log`.

An isolated `brewfs-ws0-diagnostic` compose project ran a **debug-binary
infrastructure smoke**, with 8 MiB/job, one job and one second. It is deliberately
not P-W or an acceptance baseline; builds were running concurrently. The actual
mount argv and all implemented P-ENV checks passed, fio JSON+ and snapshots were
collected, and the client-only extended drain completed. The final snapshot
reported 41 sampled/41 closed/0 dropped/0 active requests. That is lifecycle
smoke evidence only, not a tail or overhead gate. Its RustFS image is pinned to
`sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2`.
The unauthenticated RustFS `/metrics` endpoint returned 403; backend PUT
quiescence has not been established. The host kernel is Ubuntu
`6.8.0-147-generic`; the historical FUSE io_pages patch has not been verified.
The first diagnostic image build failed because `.dockerignore` excludes the
artifact directory; the retry used an isolated build context containing the
verified xfstests archive and preserved the original LFS pointer in the tree.

Default-off tracing is the diagnostic switch rollback. It does not establish
the <=2% B0/Bobs overhead gate. There is no accepted performance candidate to
roll back and no accepted Bobs baseline. Any rejected candidate must be removed with a
targeted patch while preserving existing work and measurement artifacts.

## Continuation: measurement validity and reusable envelope

The continuation artifacts are under
`docker/compose-xfstests/artifacts/ws0-validation-20261031/continuation/`.
`initial.patch` preserves the tracked worktree on entry; all earlier diagnostics
remain intact. This continuation still does **not** freeze Bobs.

Additional fixes and evidence:

- A real compose sample exposed missing GETXATTR timing: eight requests exceeded
  the 5% residual limit despite complete lifecycle collection. An SQLite-backed
  regression first reproduced missing xattr RPC attribution on both success and
  EEXIST, then passed after adding timers around xattr/ACL calls. Rename and
  attribute-lock timing gaps were also filled without changing their operations.
  FUSE inode identities are now attached at receipt for metadata requests too.
- The `strace` failure was an executable lookup bug: `require_tool_bin` accepts
  a path, while `strace` was passed as a bare name. Resolve it through PATH and
  preserve the exact effective argv. The image does contain strace.
- Backend evidence now validates the counter's own sample timestamp, cumulative
  epoch, monotonic count and distinct sample times. A freshly received retry of
  an old OTLP export cannot count as another drain sample. Tests cover stale
  exports, mixed epochs, resets, decreasing counters and backwards sample times.
- Phase snapshots wait for a backend sample newer than the requested boundary.
  Without this, the diagnostic ledger counted one preflight PUT in the measured
  numerator. The ledger now checks client/backend successful-PUT deltas, uses
  fio logical write bytes for PUT/GiB, and separates active+close+drain,
  maintenance, the intervening gap and the complete cost. Missing final
  snapshots cannot produce a normalized total. The per-batch partial-tail ratio
  keeps its own denominator; per-object tail and complete publication/GC costs
  remain explicitly unavailable.
- Prefill keeps its original fsync and remount behavior and now records argv,
  raw fio JSON+, samples and phase boundaries. Raw snapshots are retained even
  if required fields are missing. Measured request breakdowns select requests
  by receipt inside each tool window instead of mixing prefill requests into
  the slowest 1% group. WS-0 refuses Python metadata fallback as a real P-M run.
- `perf_lines_observe.py compose-run` snapshots the existing runner, collector,
  manifest helper and supplied binary, resolves image IDs, records modified
  and untracked source hashes, and stores every actual Docker argv/exit code.
  It generates a dedicated Redis/RustFS/client/collector topology with private
  volumes and no shared container names or published ports. It never builds
  during measurement, monitors compiler overlap for non-smoke runs, and cleans
  only its own project. D-state `request_wait_answer` diagnostics prevent a
  blind cleanup retry. A seed PUT outside the client dataset establishes a real
  backend counter before measurement; absent series are never converted to zero.

`envelope-smoke` and `envelope-smoke-v2` failed during envelope bring-up (copied
script execution permission and absent first backend PUT series). Their artifacts
and cleanup results are retained. `envelope-smoke-v3` completed the small debug
seqwrite diagnostic with the strict writeback profile: 48 sampled, 48 closed,
48 collected, zero loss, and no partition residual over 5%. It recorded 118
successful measured backend PUTs, matching the client delta, and no new PUTs in
the 60-second maintenance window. The sample count is insufficient for P99
acceptance, and concurrent CI compilation makes this infrastructure evidence
only. It is not P-W and not an overhead comparison.

The shared envelope entry point, after preparing a perf image containing the
real xfstests executables and recording a matching build, is:

```bash
python3 -m pip install --target /experiment/ws0-otel-python \
  opentelemetry-proto==1.38.0 protobuf==6.33.6
python3 tools/perf/perf_lines_observe.py compose-run \
  --artifact /experiment/ws0/bobs-off-pw-1 \
  --binary /experiment/ws0/bin/bobs --source-tree /root/brewfs \
  --image <prepared-perf-image> --rustfs-image <pinned-rustfs-image> \
  --otlp-python-path /experiment/ws0-otel-python \
  --matrix p-w --arm bobs-off --sample-rate 128 --features default
```

The helper currently covers the flat Redis/RustFS matrices `p-w`, `p-s`, `p-m`,
`p-broad`, `client-io` and `client-metadata`. It does not claim to provide the
native/packed writable workspace fixture recipe or the P-C/P-O matrix. The
existing workspace helper's fork-base JSON hash is still not a lower manifest
hash. Image identity and file hashes are preserved, but the supplied binary's
relationship to its source requires the corresponding build log; a caller's
feature string is not independent build proof.

### Baseline evidence blocker

The fixed original main exports the existing dirty/live/stage/remote counters,
but **does not export `brewfs_writeback_metadata_inflight`** or request trace
metrics. This was checked against the pinned source and saved in
`b0-metric-capabilities.json`. Under the task's rule that missing debt fields
are not zero, the unchanged B0 cannot satisfy this collector's complete strict
drain proof. An independent B0 pending-metadata observation source is needed;
patching B0 with this gauge would change its specified identity. This is an
evidence limitation, not a measured B0 performance failure. No B0/Bobs ratios
or baseline acceptance claims are made. The historical full trace remains
unavailable at the provided paths.

## WS-0 wrap-up scope

The wrap-up starts from the existing 28-file observation worktree on
`perf/request-latency-observation`, with source baseline `80ddaf7`. Its scope
is validation and a draft PR; it does not freeze a performance baseline.
Logs for this iteration are retained under
`docker/compose-xfstests/artifacts/ws0-wrapup-20261031/`.

The audit found that the inherited patch also changed the general
`--writeback-throughput-profile` defaults for stage-before-commit, persistence,
metadata cache TTL, direct I/O and Tokio worker count. Those default changes were removed to keep
this PR observational. A regression first failed on the inherited defaults
and passed after restoring the original values. The separate WS-0 compose
envelope still supplies its experiment settings explicitly.

`test_request_observation.sh` exercises collection and runner correctness; it
does not measure throughput or p99. Its success must not be reported as an
overhead result. Background upload/commit durations remain separate from the
foreground request wall-time partition. `object_put` measures the block-store
write future, including its internal preparation/cache/backend work, rather
than an isolated HTTP PUT latency.

The ten explicit `*_ns` JSON fields are exported as aliases of the existing
foreground segment array, without extra per-request storage. The export
regression first failed because `queue_wait_ns` was absent, then passed in the
complete overlay suite. The original `segments`, `exclusive_ns`,
`background_ns` and interval data remain available.

The requested `cargo build -p brewfs --features workspace-overlay --bins`
passed. With `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
RUSTFLAGS="-C debuginfo=0"`, the complete overlay library suite passed:
**2585 passed, 0 failed, 451 ignored**. No ignore attributes were added and
no existing assertions were removed. The named-field regression's expected
initial failure is retained in `named-fields-red.log`; its passing result is
in `overlay-test.log`. Runner regressions, all four shell syntax checks,
artifact comparison fixtures and 18 Python observation tests also passed.

The remaining local CI gates passed in the same iteration: workspace check
and build, both BrewFS FUSE runtime checks, `cargo test --workspace --lib
--bins` (1291 library tests plus 2 binary tests passed, 0 failed, 262 ignored),
workspace clippy, strict workspace/all-targets clippy, strict overlay clippy,
and `git diff --check`. `rfuse3` is not a workspace member, so its conditional
feature checks do not apply. Exact commands, exit codes and timings are in
`run-gates.sh` and `gates.tsv` in the wrap-up artifacts.

### Off/on attempt: diagnostic numbers only, overhead unverified

Both arms used the same debug/unoptimized workspace-overlay binary, SHA256
`dd2ac5bb16d92bfa070d7fbf759254c82b2a94fb18ffed114dc9f02642c3be9c`, in fresh
Redis/RustFS compose projects. The existing P-W envelope used 512 MiB/job,
four jobs, 4 MiB blocks, buffered io_uring at depth 1, 20 seconds, no
compression, fixed seed 20261031 and the same explicit cache/upload/drain
configuration. Observation was off or enabled with 1/128 lifecycle sampling.

| Seqwrite diagnostic | Off | On, 1/128 |
| --- | ---: | ---: |
| fio active throughput, MiB/s | 345.943 | 329.870 |
| fio total-latency p99, ms | 884.998 | 868.221 |
| latency samples | 1733 | 1651 |
| tool wall throughput, MiB/s | 135.922 | 137.583 |
| active + extended drain throughput, MiB/s | 103.404 | 103.155 |
| tool wall + extended drain throughput, MiB/s | 70.735 | 71.783 |
| extended drain, seconds | 47 | 44 |

Raw active throughput changed by -4.65% and p99 by -1.90%, but **neither
overhead threshold is verified**. Both matrices failed collection validity:
the off arm had stale backend/missing writeback samples and randwrite EIO;
the on arm had stale backend/missing samples during randwrite. The EIO root
cause is unresolved. During the already invalid off arm, low disk space
required `cargo clean --profile dev`, reclaiming 15.3 GiB of rebuildable
outputs; CompilerWatch recorded that cargo activity too. All binaries used
for measurement and all validation/performance artifacts were retained.

Both randwrite drains eventually completed (off 38 seconds, on 50 seconds).
Each perf container was stopped during its randwrite maintenance window;
randrw was not reached. Both envelope exits were 1 with cleanup=0 and no
`request_wait_answer` task in the preserved abort diagnostics. The on arm
recorded no compiler overlap. These interrupted matrices cannot establish
close/drain/maintenance parity or a mixed-I/O regression gate. There was no
matched original-B0 run and no release-build overhead measurement. The
<=2% disabled and <=5% enabled claims remain **unverified**; the README
comparison table is unchanged and this candidate remains a draft.

The raw fio JSON, `overhead-summary.json`, `overhead-comparison.md`,
`overhead-gates.tsv`, both `exit.json` files and `run-notes.md` preserve the
numbers and failure reasons. Successful fio seqwrite JSON is not treated as
a successful collection/acceptance result.
