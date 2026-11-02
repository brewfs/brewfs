# Small-write shared object experiment

Status: candidate v1 was built and measured, but failed one full workspace
regression. A minimal production fix is under fresh full CI; v1 performance
and FUSE results are not acceptance evidence for this final source. Keep draft.
Base: a41b1266f4aed5e40a59f07257e1b7b96499e6f7.
Artifacts: /home/hxy/perf-write-coalesce-20261102/.
Report: /home/hxy/REPORT-WRITE-COALESCE.md.

## Hypothesis and scope

Independent random extents cannot be concatenated into one existing SliceDesc:
the descriptor has a logical offset but no physical offset. Keep the logical
extents and metadata publication order, and put their immutable framed payloads
in one indexed object. This avoids reading or overwriting holes and older data.

This experiment covers flat-v1, upload-before-commit, Redis and SQL metadata.
Workspace-native-v2, packed-v3, etcd/TiKV, and commit-before-upload keep their
existing upload/publication/reclamation paths. No cross-format performance claim
or acceptance is made.

## Format and lifecycle

- `chunks-coalesced-v1/{group}/0`: 64-byte header and up to 32 48-byte index
  entries, followed by existing framed payloads. The index and each stored
  payload have SHA-256 checks. Decoded members are at most 64 KiB.
- Logical IDs encode a seed and 5-bit member slot in [2^52, 2^53), remaining
  exact in Redis/Lua. Seeds use the existing global slice allocator and must be
  below 2^47. Ordinary allocations in the reserved range fail explicitly.
  Old high-ID chunks/chunks-v2 objects retain the missing-group read fallback.
- Register every member reservation and the durable group ledger before PUT.
  Publish a logical extent only after PUT succeeds and atomically consume its
  pending reservation. An orphan claim fences late publication. Publication
  retries are idempotent.
- Existing GC reachability checks authorize retirement of individual logical
  slices. Only the last retired member allows physical object deletion. Group
  retirement is idempotent and never recreates a missing ledger.
- Frozen local slices remain resident until normal metadata commit, preserving
  read-your-writes. Existing fsync/flush/close waits include shared uploads.
- No S3 tombstone or mapping object is created per logical member.

## Configuration and bounds

`BREWFS_WRITE_COALESCE_BYTES=65536` is the candidate default, not yet an accepted
performance choice. It targets 16 ordinary 4 KiB writes per PUT. Zero disables
new coalesced writes; it never disables the reader for already stored groups.

`BREWFS_WRITE_COALESCE_MS=0` selects the mandatory 100 ms queue safety deadline;
a positive value supplies a deadline up to 5000 ms. The safety deadline remains
when no custom time window is requested, reconciling the byte-window default
with the requirement that a low-rate writer cannot wait forever.

At most 32 members share an object. Additional encoding/assembly allocations
have an 8 MiB admission semaphore, and the index cache holds at most 4096 groups.
Payload caching uses the existing configured cache. Acquire global and per-file
upload permits after grouping, in a stable order for groups spanning files.
Large slices keep the original uploader. Existing adjacent append assembly is
retained, and immutable member frames are copied once into the final object.

## Evidence so far

Before changing the uploader, an unchanged main binary failed the physical PUT
assertion: 8192 independent 4 KiB writes (32 MiB) produced 8192 PUTs, average
4100 bytes, or 262144 PUT/GiB, against a limit of 812 PUTs. All immediate reads
and all 8192 final block SHA-256 comparisons passed. See
`correctness/tdd-main/test-result.json` and `validation/red-fuse.log`.

The unchanged implementation also passed fsync and close followed immediately
by SIGKILL, remount with a new cache, and all-block SHA-256. Two earlier harness
attempts mishandled the dead FUSE descriptor during unmount; their logs remain
preserved and are excluded from passing evidence. The corrected harness checks
the actual mount table and unmount return status.

The exact candidate Redis Lua passed registration/publication/retirement and
concurrent retirement checks in isolated Redis DB 1. The Rust focused, candidate
FUSE and candidate crash results are recorded below.

Self-review found that Redis Lua runtime errors do not roll back earlier
writes: a malformed chunk key could leave the new publication marker set
before RPUSH failed, making a retry incorrectly report success. A fault test
first reproduced this in `validation/redis-publication-red.log`. Publication
now validates key types, version, and inode fields before mutating its ledger;
the same fault test and the other ledger cases pass.

Additional fault injection reproduced false publication after a non-finite
number outside the validated inode fields made cjson encoding fail. An ACL
denial also reproduced partial registration and retirement. The new paths now
check serialization and all required write permissions before mutation.
`validation/redis-encode-atomicity-red.log`,
`validation/redis-registration-acl-red.log`, and
`validation/redis-retirement-acl-red.log` preserve the failures;
`validation/redis-atomicity-green.log` records the passing assertions.

The first executable Rust coalescing run passed 19 tests, including the 8192
random-write PUT bound with complete block/hole verification, the 512 MiB
sequential bypass, overlapping writes, and orphan/truncate GC. Three additional
regressions failed as intended: a tiny whole-file explicit flush entered the
queue, a native reader probed the flat shared namespace, and failed uploads
made three attempts instead of the existing five. The fixes bypass that tiny
flush, share an explicit flat-object support predicate between reads and
writes, and restore five attempts. Full logs and the test executable SHA-256
are in `validation/focused-red/`. The green rerun passed all 22 selected tests
(15 newly added tests plus seven existing matches), with zero failures and
zero ignored tests, in 127.26 seconds. This does not replace the full workspace
or overlay gate.

## Acceptance still required

Fresh main performance uses an isolated worktree and target. Candidate A/B uses
one binary, off/on alternating, two warmup pairs and three measured pairs per
workload, 20 seconds per round. The protocol fixes working set, compression,
cache budgets, seeds, upload limits and strict drain. Report paired differences
and Student t 95% CI, PUT count/bytes, latency, RSS, and perf samples. Include
wall and active-plus-drain bandwidth, seqwrite/randrw and direct-I/O guards.

No performance result is accepted until the same iteration passes the complete
local workspace test gate, overlay tests, strict clippy, format and shell gates.
No README comparison table has been changed. The PR must remain draft unless
all requested acceptance conditions and scope requirements are satisfied.

The 90-minute baseline deadline was missed because the isolated main release
build took 108m 06s. Fresh baseline measurements completed at 22:42 UTC.
The three-hour A/B deadline (23:48:18 UTC on November 1) was also missed:
the candidate release, started at 23:28:47 UTC after the focused tests and
strict clippy passed, was still compiling. Neither deadline is reported as met.

## Candidate executable verification

Release build passed in 54m 13s. SHA-256:
`6e3e9d0ac6bae33aba4a8d3c1b1a22984e77600c711bcebaf48b5ba7276b34a7`.
Production sources, runner changes and tests match its saved source manifest.
The host memory watchdog remained active throughout compilation.

The real FUSE test passed: 8192 random 4 KiB writes produced 512 PUTs and
34,013,184 stored bytes, averaging 66,432 bytes per PUT, or 16,384 PUT/GiB.
All 8192 immediate reads and final block SHA-256 checks passed, as did strict
drain and unmount. See `correctness/tdd-on/test-result.json`.

Both durability cases passed: successful fsync with the descriptor still open,
and close without fsync, each immediately followed by SIGKILL. Fresh-cache
remounts read the entire 32 MiB file and verified all 8192 block hashes. Empty
and one-byte files also passed. Both kill return codes were -9; all four
unmounts completed without remaining mounts. See `correctness/on-summary.json`
and the per-case teardown records. The old implementation passed the same
cases before the change. Thus these tests show unchanged successful-fsync/
close semantics for a mount-process crash; they do not simulate power loss of
the Redis or object servers.

## Paired performance and review

Two off/on warmup pairs and three measured pairs completed for each workload.
Each round lasts 20 seconds; one pinned candidate executable serves both arms.

| Workload | off median MiB/s | on median MiB/s | Change | Paired difference 95% CI, MiB/s |
|---|---:|---:|---:|---|
| randwrite | 2.4651 | 5.0878 | +106.40% | [+2.3940, +2.8497] |
| seqwrite | 133.9744 | 133.3800 | -0.44% | [-3.4414, +5.4744] |
| randrw | 2.7750 | 3.3475 | +20.63% | [+0.2618, +0.9401] |

Randwrite PUT/GiB falls from 261512.07 to 16385.24 (15.96x fewer); average
PUT rises from 4099.04 to 66249.75 bytes. p99 falls from 471.859 to 200.278 ms.
Wall-plus-drain bandwidth rises from 2.2000 to 4.6484 MiB/s; RSS peak medians
are 287.01 and 300.70 MiB. Seqwrite retains approximately 4 MiB PUTs. Mixed
read/write p99 also improves. All strict drains and unmounts pass.

The fresh main median was 2.5828 MiB/s, so it is reported separately from the
same-binary off control. No historical result replaces the new baseline.
Single-pair direct-I/O guards pass for the three workloads. Their measurements
are excluded from the primary CI. No seqwrite acceleration is claimed.

Perf self-period analysis of the three randwrite measurements per arm reports
SDK/HTTP sampled CPU of 49.179 -> 5.558 ms per written MiB; total sampled CPU
is 421.840 -> 218.193 ms/MiB. This diagnoses on-CPU work, not remote or off-CPU
latency. All nine main/off/on formal profiles have zero lost samples.
Self-review corrected the analysis-only category order: SHA transform functions
named `compress` must be counted as checksums before the compression regex.
SHA-1 symbols are now included too. The original classifier output is retained
in `validation/profile-classifier-before.json`; SDK/HTTP and total CPU values
are unchanged. No source change or performance rerun was involved.

Review checked frozen-data lifetime, metadata ordering, queue generation,
bounded admission, stable upload semaphore ordering, reservation fencing,
last-member GC, retry count, SQL restart and disabled-mode reads. Native and
packed upload/publication paths remain excluded. Existing adjacent slice
assembly is retained; grouping copies each encoded frame into the final body
once. The new reader supports old objects, but old binaries cannot read newly
grouped objects. Cold grouped reads can fetch a full group before the index is
cached; this is a read-amplification tradeoff, not a free format conversion.

Full local CI started at 00:48 UTC. The actual repository compose runner broad
guard follows CI so compiler contention cannot affect its numbers. The pinned
container passed a binary/helper compatibility smoke check. Final gate results
must be recorded before acceptance; no README comparison table is updated.

Protocol, all-round measurement summaries, CPU attribution and correctness
results are copied under `doc/performance/evidence/write-coalesce/` for review.
Raw fio JSON, perf data, sampler streams, crash hash lists, command lines and
logs remain in the host artifact root above.

## Full-workspace regression and second iteration

At 01:03 UTC the first full workspace run finished with 1314 passed, one failed
and 264 existing ignored tests. The existing
`test_fs_regression_fs_ops_pwrite_slice_relative_upload_offset` uses a 64-byte
VFS block layout with the default object-store configuration. The eligibility
test only checked the latter's block limit, so a slice spanning multiple VFS
blocks entered the single-block shared-object format and reading failed with
`coalesced slice has only one block`.

The smallest fix also bounds slice length by `s.data.block_size()` before
enqueuing. Such slices use the unchanged ordinary multi-block uploader. No
test assertions or ignore annotations changed. The first complete CI logs,
source manifest and patch are preserved under `validation/full-ci-attempt1/`.
The entire local gate was restarted at 01:04 UTC with this fix.

The measured release is now an earlier source revision. Even if the full gate
passes, its A/B and FUSE results cannot be transferred to the final executable.
A new release and matched measurements are required before readiness/merge.
No accepted README performance table is updated.

The second workspace run passed 1315 library tests and two binary tests, with
zero failures and the same 264 existing ignored tests. The previously failing
pwrite test passes without changing its assertions. Overlay and final clippy
remain in progress at this checkpoint.

The final dev binary (`fbe0f9f64c6718fa8e8f988b6ae33e3be96f857fa692843a1687ee742fe64e91`)
also passed real Redis/RustFS FUSE fsync/close -> immediate SIGKILL -> fresh-cache
remount checks. Each case verified the entire 32 MiB file, all 8192 SHA-256
blocks and immediate reads, empty/single-byte files, -9 kill status and clean
unmounts. Off, v1 release and final dev hash lists match exactly. The first
attempt used an unsupported `--metrics` flag in this default-feature binary;
that harness startup failure is preserved separately. Removing only that CLI
flag allowed the unchanged content/crash assertions to run and pass. This is
functional evidence for the final source, not a replacement performance build.

## Delivery validity boundary

Final workspace check/build, runtime checks, 1315 + 2 workspace tests, format,
shell/report scripts, Redis fault injection and diff checks pass. The final
overlay test compile was terminated by the unchanged host memory watchdog:
`2026-11-02T01:34:04Z avail=1775MiB < 3072 -> killing rustc` (SIGTERM; Cargo 101).
It never reached test execution. The failure is archived under
`validation/overlay-watchdog-attempt1/` and is not reported as a test pass.

The same source/features/codegen profile is being retried with one Cargo job
instead of two, followed serially by final strict workspace/overlay clippy.
The watchdog was not disabled and no Rust code or assertions were changed to
address the memory termination. `validation/resume-ci.sh` runs in tmux
`coalesce-gates`. Separate final-source auxiliary gates passed while that
compile proceeded; their commands/statuses are in `aux-gates.json`.

The final release has zero of three required measured pairs per primary scene,
zero of three direct guard scenes and zero of two broad compose arms. These
missing measurements, the pending overlay run and pending final clippy prevent
readiness and merge. The v1 gains remain diagnostic, not accepted performance.
The final dev binary's content and SIGKILL evidence is retained separately.
No accepted README comparison has changed. Delivery is a draft with explicit
gaps within the five-hour timebox; the earlier 90-minute and three-hour
milestones were missed as recorded above.

## PR 404 adversarial review

Review of `386ce60` reproduced a Redis publication atomicity defect. Chunk
versions `00`/`01` pass the original decimal-digit preflight but Redis INCR
rejects them. The script has already set the published marker, deleted the
reservation and appended the slice. A retry then reports success while the
durable inode size remains zero. This is a fault-injection case, not evidence
that ordinary Redis INCR generates malformed versions.

The regression test failed on the original code with `invalid INCR marked
publication successful`. Preflight now accepts only Redis's canonical
nonnegative integer spelling (and retains the existing exact-number limit).
The actual embedded Lua passes the expanded Redis fault tests, including
unchanged reservations, chunk contents and inode size on failure, followed by
a successful publication starting from version `0`.

Evidence: `/home/hxy/review-404-20261102/redis-false-success.json`,
`redis-version-red.log`, `redis-version-green.log`. The fresh local minimum CI
gate passed against the fix: workspace check/build; tokio/io-uring checks;
1315 library + 2 binary tests (264 existing ignored); strict workspace clippy
with all targets; format, shell/report scripts, Redis fault tests and diff check.
Commands, source hashes and all 13 exit statuses are in `review-results.json`
under that evidence directory. Dependency diagnostics remain as before.
The original release measurements still do not match final production sources.
Final-release paired measurements, direct guards and broad compose acceptance
remain missing. Keep the PR draft; the small atomicity fix does not waive those
requirements. No performance table is promoted to accepted status.

The original status summary showing sixteen passing gates was incomplete:
`/home/hxy/check-404-gates.sh` sorts status files and applies `head -16`, hiding
later gate names. An interrupted overlay status can also remain on disk while
a retry runs; inspect started/finished times and logs, not only the status file.
The full self-review is `/home/hxy/REPORT-404-SELFREVIEW.md`.

The earlier overlay retry subsequently finished with 2728 passed, one failed
and 474 ignored. `writable_acl_independent_sqlite_instances_keep_mode_acl_during_atime_writes`
returned EBUSY at `meta_layer/tests.rs:1477`. Its test and direct implementation
files are unchanged from the PR base. The same executable failed once in an
isolated rerun and then passed three focused repeats; these later passes do
not erase the full-suite failure. This remains unresolved and blocks acceptance.

The fixed debug binary additionally passed Redis/RustFS FUSE fsync/close ->
SIGKILL -> fresh-cache remount in both direct and buffered modes. Each of the
four cases checked all 8192 blocks and clean unmounts. A real Redis allocator
boundary test set `nextchunk=2^52-1`: buffered pwrite returned 4096, then fsync
returned EIO (5), matching the sticky writeback error path. The subsequent
close returned 0; applications must check the fsync result. These are functional
checks, not new release performance measurements. Task compose services were
cleaned up, and accepted artifacts were preserved.

## PR 404 round-two acceptance (2026-11-02)

The two remaining merge blockers are cleared using production source
`d5e53e1ca532762cf7ffb361430d939a0b97eb7c`; this round changes documentation
and evidence only. Both full, unfiltered overlay suites passed 2729 tests
with zero failures and 474 existing ignored tests. The second finished at
03:36:06 UTC, 47m58s after this round started. Five focused repetitions of
the unchanged SQLite ACL/atime test produced pass/fail/pass/pass/pass: the
existing EBUSY flake is retained, and is not described as a 5/5 pass. No
assertion, ignore annotation or production implementation was changed.

All 17 local CI checks passed, including 1315 library plus two binary tests
and strict all-targets workspace clippy. The final release SHA-256 is
`e2f12ee08c23f29369be8982ecf87b416d2f190a8cbf85e652c773dfffb32313`.
The 1858-file source manifest SHA-256 is
`5e954a6ff76ac0c6fa3828d2148620c32d7eb16022d718a8f9a2f82c8fde26bf`.
Manifest identity was checked before/after release construction and through
the end of measurements, including writer.rs and the Redis fix. The build
ran in tmux without timeout; the intentional compiler scheduling restart and
the final successful build are both archived.

The final-source buffered matrix contains two warmup pairs and three
measured pairs for each of randwrite, seqwrite and randrw. Random-write
bandwidth rose 108.33%, with paired 95% difference CI [+2.5726,+2.7899]
MiB/s; PUT/GiB fell to 16379.61 and average PUT rose to 66272.36 B. Its p99
fell 39.19%; sequential write changed -1.02%; mixed I/O rose 23.23% with
improved read/write p99. Full wall-plus-drain changes were +112.44%, -0.60%
and +23.87%, respectively. All 36 warmup/measured/direct arms drained and
unmounted normally. Direct guards passed, and final-release content plus
eight direct/buffered fsync/close -> SIGKILL -> fresh-cache recovery cases
verified every one of 8192 blocks per case.

The complete ten-workload compose run passed for off and the second full on
attempt. The first on attempt had an empty stats sample and a sticky invalid
marker; its complete failure artifact is preserved. The rerun kept the same
binary, workloads, timeouts, sampling and assertions. Original broad read
declines are disclosed: three predeclared seqread follow-up pairs gave
-0.37% median bandwidth and +7.18% p99; one additional full 4 GiB bigread
pair gave -1.86% bandwidth. Bigread's fio latency histogram is capped, so
its reported p99 cannot prove the true p99 unchanged. Broad create ops/s
fell 5.16% in one pair; open/stat/readdir/rename and mixed I/O improved.
The report retains these limits and all unfavorable observations.

The authoritative report is
[the round-two performance report](../../performance/write-coalesce-2026-11-02.md),
with [published evidence](../../performance/evidence/write-coalesce/r2/README.md).
Raw artifacts remain in `/home/hxy/perf-write-coalesce-r2-20261102/`.
Both acceptance summaries pass. README tables now use these final-source
numbers, while v1 remains explicitly historical. Task compose services and
volumes were removed; no task mount or D-state process remains. The unrelated
JuiceFS mount and the pre-existing untracked observability script were left
untouched.
