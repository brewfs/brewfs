# Packed frame verification memo — 2026-11-01

Status (2026-11-02 gate repair): the single candidate release, three paired
A/B rounds per workload and all eight full-corpus SHA-256 passes are complete.
The two overlay gate failures are resolved as recorded below. All 17 fresh
local build/test/check command groups and both focused tests passed. The
reported performance results are accepted for these two measured read scenes;
the immutable-storage safety condition remains unchanged.
Artifacts: `/home/hxy/perf-packed-verify-20261101/`.

## Scope and safety contract

The proposed default is `BREWFS_PACKED_VERIFY_MEMO=1`; `0` restores SHA-256/128
verification of every fetched frame. Values other than `0` and `1` are errors.
The worker chooses a `const MEMO: bool` specialization once at startup. In the
disabled specialization the frame identity is always `None`, verification is
unconditional, and identity construction, LRU lookup/update, timestamp reads
and per-frame switch branches are eliminated. No memo allocation or Metadata
reservation is made when disabled. The first off profile is checked for
`verify_memo` symbols in both self samples and call chains before B runs;
all later off profiles receive the same check. Sampling alone cannot establish
instruction-for-instruction equivalence to a separately linked old binary.
The mount's wire005 demand coordinator retains at most 8,192 successfully
verified frame identities in an LRU. An entry contains only a BLAKE3 identity
of the container digest and physical object key, frame ordinal, expected frame digest, stored offset/length and a last
access timestamp. No stored or decoded payload is retained by this memo.

The container identity and ordinal identify a frame in an immutable v3 object.
Binding the physical key prevents evidence for one object from skipping the
first verification of a different object that claims the same digest. The
extra BLAKE3 hashes only the bounded key and 32-byte container digest; payload
hashing and the wire format remain SHA-256/128.
The expected digest and stored range also participate in the key, so a changed
descriptor cannot reuse old evidence. The first read, every miss and every read
after eviction must compare SHA-256/128 of the received stored bytes with the
authenticated descriptor. Failed verification must never insert an entry.
Concurrent misses may hash more than once; no in-progress verification is
treated as successful. Memo lifetime ends with its owning coordinator; there
is no process-global or persistent evidence.

**The argument is conditional on immutable storage, not just immutable names.**
If an object remains byte-for-byte identical throughout the mount, a successful
first verification establishes the same digest for subsequent reads. A corrupt
first response is rejected. Content addressing alone does not enforce this
premise: a same-length overwrite after the first successful verification, or a
corrupt later response, can evade the memo. It would be incorrect to claim that
the first verification detects future external tampering. Use the off switch
when each response needs independent integrity authentication. A bounded
payload-free memo cannot provide both per-response authentication and removal
of the payload hash without an additional trusted storage guarantee.

Every hit still obtains a complete range response, checks its exact length and
terminal EOF, checks frame bounds, validates codec/decoded length and uses the
existing budget, cancellation and delivery machinery. The memo does not alter
the SHA algorithm or wire format, and does not memoize metadata pages or the
publication verification path. Payload RAM/SSD, bundle, decoded and window
caches stay at zero in both arms.

The metadata-only LRU has a conservative 1.5 MiB Metadata reservation, including
allocator/hash-table/node overhead; it is not accurately described as a
256 KiB structure. Actual resident memory is measured separately.
This memory caches no payload: only the 32-byte BLAKE3 identity of the 32-byte
container digest plus physical object key, and frame metadata. The declared
index ceiling is 2 MiB; it is separate from the zero-payload-cache invariant.
The control/statistics test subtracts only the declared memo reservation and
still requires exactly 24 KiB of request/handle Metadata before the last
consumer drops, 16 KiB afterwards, and zero after the file handle closes.
The memo reservation itself must disappear at metadata-session shutdown.
A dedicated enabled-memo test reads two frames twice, requires four GETs and
decodes but only two authentications, and checks that every released frame
owner disappears and Stored/Raw/Workspace/Output usage returns to zero after
each read. The entire budget must return to its initialized baseline, so
payload cannot be hidden by moving its accounting to Metadata.
The existing 256 MiB Metadata and 16 MiB Roots capacities stay unchanged in
both arms. Roots cannot safely hold the memo: the historical seqread round
already used 16,589,672 of 16,777,216 Roots bytes. The corrected constructor
reserves memo ownership from Metadata atomically with its queue reservation;
the regression test fills all remaining Roots and still reads successfully,
then checks that both reservations are released on shutdown.

## Reproduction and acceptance protocol

The working branch was already `perf/packed-verify-memo` at `918deb9`, identical
to local main. Its only change since the requested `ed4930a` base is the #400
documentation commit. The pre-existing untracked
`tools/perf/measure_observability.py` is unrelated and is preserved.

The historical campaign `/home/hxy/perf-full-20261101/` is preserved. Its results
are context, not an A/B arm. Its frozen protocol and harness supply the workload
shape, corpus, compose image identities, drain and profiling behavior. The
snapshot builder sources are unchanged since that campaign; its preserved
fixture executable prepares new objects in this task's isolated compose bucket.

The new harness is `tools/perf/packed_verify_memo.py`. It uses one pinned release
daemon binary for both switches, records its SHA-256, and predeclares three
pairs: off/on, on/off, off/on. Each arm gets a fresh mount, two 20-second warmups
and one 20-second measured round. Seqread remains buffered 1 MiB, one job,
depth 8; randread remains direct 4 KiB, four jobs, depth 32. Both retain the
512 MiB per-job working set, `libaio`, fixed seed, zero payload budgets,
256 MiB metadata cache, one Tokio worker and no global `drop_caches`.

Profiles use `perf record -F 499 -g --call-graph dwarf,8192 -e cpu-clock -z`.
Offline processing produces per-round and pooled top-20 self CPU tables,
folded stacks and inferno SVGs. SHA share sums the leaf-symbol CPU periods for
SHA-256 implementations; it is not inclusive call-chain time. Compilers and
offline profile processing do not run during measured workloads.

To preserve the three-hour task window, the CI cargo/rustc processes are
SIGSTOP during all sampling. Their process start times and CPU ticks are
checked before and after every arm; resident compiler memory remains present
equally for A and B. They resume immediately after the final measured arm.
Full local CI is still required before accepting performance conclusions or
committing. The two completed full-corpus checks are preserved; remaining
content checks run after sampling alongside CI and offline profile processing.


Report three raw rounds and per-arm medians. For each matched pair calculate
on-minus-off differences, then mean difference ±
`t(0.975, df=2) * sample_stddev / sqrt(3)` with `t=4.3026527299`.
Only a confidence interval excluding zero establishes significance under this
paired t model; three pairs give limited precision and do not justify a
normality claim. Preserve wall/drain bandwidth and GET amplification alongside
fio active bandwidth.

## Results and local gates

The sole candidate release completed at 15:49:17 UTC, exit 0, after a
91m 51s retry using the completed dependency artifacts. Its SHA-256 is
`c0629e69b37a3ed2beaccca3386f6a8b2748855b8bce2db4e3a42febbf0c8064`.
The frozen source checksums all match. The binary contains the explicit
`MEMO=false` specializations; runtime perf and counter checks remain required.
No clean-main comparison binary was built in this corrected task.

The artifact audit passes every measurement check: all 36 rounds, all six
disabled-path audits, all per-round SHA thresholds, identical arm environments,
zero PUTs, clean drain/teardown, and unchanged paused compiler CPU counters.
The later content audit completed all eight passes: 65,568 file verifications,
17,448,304,640 bytes and zero SHA-256/length mismatches; all output manifests
match. Evidence: `validation/finalize/content-audit.json`. The original
`validation/provisional-artifact-audit.json` records the earlier snapshot.

## Same-binary A/B results

A is `BREWFS_PACKED_VERIFY_MEMO=0`; B is `=1`. The executable is identical:
`c0629e69b37a3ed2beaccca3386f6a8b2748855b8bce2db4e3a42febbf0c8064`. All three pairs are retained; order is A/B, B/A, A/B.

Each row below is a matched pair of 20-second measurements, after two
20-second warmups on each fresh mount. SHA is the sum of SHA-related
leaf CPU periods, not inclusive call-chain time.

| Workload | Pair | A MiB/s | B MiB/s | B/A change | A SHA self % | B SHA self % |
|---|---:|---:|---:|---:|---:|---:|
| seqread | 1 | 7.4668 | 15.9996 | +114.28% | 72.3064 | 0.5217 |
| seqread | 2 | 7.6981 | 15.6608 | +103.44% | 70.7919 | 0.2036 |
| seqread | 3 | 7.7253 | 16.1427 | +108.96% | 70.3383 | 0.4078 |
| randread | 1 | 1.1020 | 1.5994 | +45.14% | 37.6081 | 1.7981 |
| randread | 2 | 1.1022 | 1.5978 | +44.96% | 37.6580 | 2.3221 |
| randread | 3 | 1.1091 | 1.5610 | +40.75% | 37.2969 | 1.5901 |

Per-arm medians and paired mean differences (Student t, n=3, df=2):

| Workload | A median MiB/s | B median MiB/s | Mean paired difference [95% CI], MiB/s | Mean paired change [95% CI] |
|---|---:|---:|---|---|
| seqread | 7.6981 | 15.9996 | +8.3044 [+7.5557, +9.0530] | +108.89% [+95.43%, +122.36%] |
| randread | 1.1022 | 1.5978 | +0.4816 [+0.4177, +0.5455] | +43.61% [+37.45%, +49.78%] |

CPU periods pooled across all three formal rounds, matching the historical
70.25% / 36.18% AVX-symbol statistic. Ring overlaps SHA and must not be added
to it. BLAKE3 includes the bounded physical-key identity hash, not a replacement
payload checksum.

| Workload | Arm | SHA AVX self % | All SHA self % | Ring self % | BLAKE3 self % |
|---|---|---:|---:|---:|---:|
| seqread | memo-off | 71.0680 | 71.1417 | 71.1509 | 0.0000 |
| seqread | memo-on | 0.1359 | 0.3775 | 0.3020 | 0.0529 |
| randread | memo-off | 36.6351 | 37.5210 | 37.1723 | 0.0000 |
| randread | memo-on | 0.3661 | 1.9027 | 1.0350 | 0.1582 |

| Workload | A median all-SHA self % | B median all-SHA self % | Paired difference, percentage points [95% CI] |
|---|---:|---:|---|
| seqread | 70.7919 | 0.4078 | -70.7679 [-73.1031, -68.4326] |
| randread | 37.6081 | 1.7981 | -35.6175 [-36.2369, -34.9981] |

Bandwidth including close/wall time and drain, plus read latency and object
amplification. These are per-arm medians; throughput units are MiB/s.

| Workload | Arm | Active | Wall | Active + drain | Wall + drain | Read clat P99 ms | GET bytes / fio bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| seqread | memo-off | 7.6981 | 7.5689 | 7.3080 | 7.1915 | 985.6614 | 16.1039 |
| seqread | memo-on | 15.9996 | 15.7146 | 15.1894 | 14.9323 | 505.4136 | 16.0499 |
| randread | memo-off | 1.1022 | 1.0834 | 1.0464 | 1.0294 | 526.3852 | 64.0000 |
| randread | memo-on | 1.5978 | 1.5699 | 1.5165 | 1.4914 | 383.7788 | 64.0000 |

| Workload | Paired wall + drain change, MiB/s [95% CI] | A / B median RSS MiB |
|---|---|---|
| seqread | +7.7510 [+7.0748, +8.4273] | 77.06 / 77.59 |
| randread | +0.4488 [+0.3895, +0.5081] | 61.44 / 62.70 |

## Disabled-path evidence

All six A profiles must pass the leaf and call-chain search before they are
accepted. This is combined with the `const MEMO=false` source specialization
and actual per-frame authentication/decode counters:

| Workload | Arm | Payload authentication calls | Payload decode calls |
|---|---|---:|---:|
| seqread | memo-off | 924 | 924 |
| seqread | memo-on | 1 | 1922 |
| randread | memo-off | 16972 | 16972 |
| randread | memo-on | 142 | 24379 |

All six off profiles contain zero `verify_memo` matches in sampled leaves
and call chains. Their payload authentication and decode counts match.

## Local validation

PR #401's repair chooses option A, retaining 8,192 evidence entries. The
control/statistics assertion originally compared 1,597,440 bytes of total
Metadata against 24,576 bytes of request/handle ownership. Their difference
is exactly the 1,572,864-byte memo reservation. The repaired test preserves
the exact request/handle amounts and shutdown ownership checks, bounds the
memo independently by 2 MiB, and asserts zero payload-pool usage. The new
enabled-memo test described above protects against retaining payload on hits.
The repair changes tests, a test-only accessor and comments; production memo
behavior and the measured release binaries are unchanged.

The other failure,
`kernel_cache_refill_discards_external_upper_rows_and_checks_binding`, passed
five isolated reruns of the original full-overlay test binary on 2026-11-01
UTC: 19:09:08.781207, 19:09:09.050556, 19:09:09.301136, 19:09:09.551629 and
19:09:09.796052. Each exited 0 with one test passed. It is classified as timing
flaky and its source/assertions are unchanged. The previous deadline difference
was 20.951247 **ms**, exceeding its 20 ms TTL. Logs and timestamps:
`validation/fix401/isolated-before-fix.json` and `ttl-rerun-{1..5}.log`.

Both fresh strict clippy commands exited 0 with the same 31 dependency diagnostics:
26 `dead_code` warnings in vendored `tikv-client`, four in vendored `asyncfuse`,
and one Cargo future-compatibility notice for `proc-macro-error2 2.0.1`
(E0365, private `proc_macro` re-export). Every implicated source blob and all
three lockfile package records match PR base `918deb9`, so none was introduced
by this PR. `strict_clippy_zero_warnings` remains literally false; no suppression
was added. The detailed list and provenance are in
`validation/fix401/warning-provenance.json` and `/home/hxy/REPORT-PACKED-VERIFY.md`.

The fresh serial gate run completed on 2026-11-01 at 20:02:34 UTC. All 17
build/test/check command groups exited 0. The repaired control/statistics test
and the new enabled-memo zero-payload test also each passed in an exact run.
The complete overlay suite ran without added skips, ignores or filters:
2,714 passed, zero failed, 474 existing ignored. Default tests passed 1,300
tests; workspace `--lib --bins` passed those 1,300 plus two binary tests.
Both strict clippy commands, workspace clippy/check/build, both runtime
checks, fmt/diff, four shell syntax checks and three report regression scripts
passed. `rfuse3` is not a workspace member. Commands, exit codes, per-log
SHA-256 and warning lists are in `validation/fix401/gate-summary.json`.

`all_required_commands_complete=true`, `all_required_commands_pass=true`;
`strict_clippy_zero_warnings=false` is retained honestly for the pre-existing
dependency notices. The no-default-feature Tokio check additionally reports
the pre-existing unused `ReadResult::Fatal` variant in
`vendor/asyncfuse/src/raw/session/utils.rs`; this vendored file is unchanged.
No compiler was killed. The host memory watchdog was stopped for local
compilation and restored after the gates finished. Both preserved release
binaries still match the original measured SHA-256, and
`validation/fix401/repair-source-audit.json` confirms that production Rust
behavior and the TTL test are unchanged by the repair.

Raw fio JSON, exact commands, before/after metrics, 1 Hz process samples,
drain records, perf data, top-20 tables, folded stacks and SVGs are retained
under `/home/hxy/perf-packed-verify-20261101/`. `paired-summary.json` contains every raw value and interval.
Results cover this host, the frozen working set and repeated reads after two
warmups; three pairs provide limited precision. No cold-read, writeback or
general metadata performance claim is inferred from these two read scenes.

The first task-local RustFS startup failed due to bind-directory permissions.
No performance round ran. Logs are preserved in
`attempts/setup-permissions/` and `environment/compose-*-retry.log` records the
retry after fixing the task-owned data directory.

The earlier clean-main release attempts received SIGTERM. The corrected task
initially attributed this to timeout wrappers, but the current investigation
identified the host's `/home/hxy/watch-mem.sh`: it calls `pkill -x rustc` when
available RAM falls below 2,048 MiB. Its log records kills at 12:37:39 and
13:26:44 UTC, matching both earlier failures, and at 14:14:19 UTC during this
run (1,735 MiB available). The latter killed the concurrent release and feature
test compilers even though neither had a timeout wrapper. They are interrupted
builds, not compiler errors; no test assertion ran. Original logs and the host
guard evidence are retained in `attempts/memory-guard-sigterm-141419/`.
The retry preserves the host protection and all completed dependency artifacts.
A task-specific scheduler pauses only the CI compiler below 7 GiB available
or once CI RSS exceeds 7 GiB, reserving headroom for the observed release peak;
it resumes below that RSS cap with over 11 GiB available, or after the release
compiler finishes. It never terminates a compiler. The retry makes no Rust
source changes.
After the library completed and the final binary passed its largest initial
units with a 12.5 GiB high-water mark, the CI RSS cap was raised to 9 GiB for
that binary-only phase; the 7 GiB available-memory guard remained in force.
The corrected run builds only the candidate release in a detached tmux session,
without a timeout wrapper, using `/home/hxy/brewfs-pkv-target`. Both A/B arms
use that exact executable. Local CI uses a package-only 256 codegen
unit override for BrewFS to bound individual LLVM modules; features, test
selection and optimization levels are unchanged. Release flags remain exactly
those recorded by the measurement protocol. The full feature test code passes
`cargo check --tests --features workspace-overlay,observability`.

Optional decoded-frame cache B has not been implemented. Its default remains
zero; no 64 MiB/256 MiB cache performance claim is made.
