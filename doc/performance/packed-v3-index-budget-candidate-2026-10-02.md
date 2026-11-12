# Packed metadata index-budget candidate closeout (2026-10-02)

## Outcome

**The production candidate was withdrawn; no throughput improvement is accepted.**
The candidate passed its pure budget and authenticated page-residency tests, but
local mounted-FUSE validation did not complete without errors. Both the exact
baseline and candidate exhibited EIO in the constrained-budget workload. The
experiment therefore fails the correctness/performance acceptance gate; it does
not establish a throughput regression caused by the split policy. The original
production allocation has been restored with a targeted patch. No wire or frame
layout change is retained.

## Hypothesis and proposed change

At `abcdda0`, a 512 MiB metadata budget reserves 32 MiB each for group-index and
inode-index pages, while the observed group-index resident set is only 714,776 B.
The candidate retained the total 64 MiB index pool but allocated it from the
manifest's conservative 2x encoded-page-size estimates. A small index would fit
first, and the other index received the rest. Index capacity unused by both
indexes would return to GroupMeta. It did not change locator, inode-entry,
descriptor, window or decoded-payload policies.

The valid external shuffled warm artifact records 1,284,176 inode-index GETs and
1,236,238 GroupMeta GETs across two epochs. Their bytes total about 933.3 GiB,
not "tens of thousands" of GETs. Moka resident bytes are logical weights, not
daemon RSS; scanner peak RSS is a different process and cannot stand in for it.

## Verification and failed FUSE trials

A real-page test compared the old and proposed split on eight authenticated II05
pages. Under equal index pool bytes, the baseline evicted pages, whereas the
candidate retained all eight and incurred no new inode-page GET in the second
permuted pass. This only proves request-graph behavior for that test corpus.

For mounted-FUSE tests the baseline was built from `git archive abcdda0`, not
selected by an unverified binary timestamp. Baseline and candidate used the same
unoptimized Cargo configuration (`workspace-overlay`, incremental/debug info
disabled), the same fixture publisher and shared scanner. Consequently these
are local diagnostics, not optimized release/cloud throughput measurements.

Trial protocol: fresh RustFS Compose volume and fixture, fresh mount, host
`sync`/`drop_caches=3`, 10k independent files, 16 workers, 8 MiB total metadata
budget, auto metadata warm-up, no payload/window/decoded cache or data prefetch.
The scanner performs two deterministic shuffled stat passes; a failure stops
that trial before any comparison is calculated.

| Trial | Arm reached | FUSE TTL | File size | First-pass outcome |
|---|---|---:|---:|---|
| Initial | baseline | 0 ms | 100 KiB | 5,615 / 10,000 files, 4,385 errors |
| Default-TTL diagnostic | baseline | 1,000 ms | 100 KiB | 6,728 / 10,000 files, 3,272 errors |
| Tiny diagnostic | candidate | 1,000 ms | 4 KiB | 7,485 / 10,000 files, 2,515 errors |

Reported scanner rates from these rows are invalid as performance evidence.
The fixture, server and mount ran until the scanner itself failed; the failures
are not accepted as a cleanup-only race. Their precise cause is not established
by these logs. Diagnose the existing FUSE error path before retrying.

Artifacts (generated locally, not committed):

- `docker/compose-xfstests/artifacts/local-packed-index-budget-20261002/`
- `docker/compose-xfstests/artifacts/local-packed-index-budget-20261002-ttl1/`
- `docker/compose-xfstests/artifacts/local-packed-index-budget-20261002-tiny/`

These preserve invocation scripts/profiles, build logs, binary SHA256, fixture
summary, scanner errors, mount counters and cleanup logs. All three traps
unmounted the filesystem, stopped the daemon, removed their generated temporary
fixture directories, and ran Compose `down -v --remove-orphans`. No cloud
server, credential object, Redis or TiKV service was created.

## Retained correctness and measurement work

1. A new catalog regression executes the dynamic size-class corpus through the
   builder, manifest serialization, GroupMeta placement and remote catalog read.
   It covers 200 KiB, 512 KiB, 1 MiB, 10 MiB and 32 MiB files under random and
   sequential profiles, inline and non-inline Tiny files, file tails, frame
   boundaries and a 200 KiB p90 hint. This is library/request-path validation,
   not full FUSE/lifecycle or external large-object acceptance.
2. The packed cloud runner previously advertised
   `BREWFS_METADATA_CACHE_TTL_MS`, but FUSE reads `BREWFS_CACHE_TTL_MS`. The runner
   now explicitly passes the effective variable to the mount process. The new
   `tools/perf/test_packed_runner_ttl.sh` invokes the actual `start_mount`
   function with a test CLI and checks 0/1,000/60,000 ms forwarding, including
   overriding a conflicting inherited value. Historical TTL symmetry claims
   need remeasurement; the old numeric rows remain historical observations.

The three-innovation implementation audit and staged experimental matrix are in
`doc/superpowers/plans/2026-10-02-brewfs-three-innovations-experiment-plan.md`.

## Final local gate for the retained work

fmt/diff checks, required shell syntax/report tests, TTL forwarding regression,
scanner unit tests, workspace check/build, four FUSE runtime feature checks and
`cargo clippy --workspace` passed (clippy retains existing warnings).
The packed suite reports 66 passed; the complete workspace lib/bins gate reports
1097 passed, 225 ignored and zero failures. The initial chained gate hit its
background time limit; completed independent workspace/clippy logs supersede it.
This is the repository's local minimum gate, not a claim that all-feature,
operator or GitHub `-D warnings` checks passed.

## Next gate, not a promised win

Resolve the mounted-FUSE EIO evidence with a small reproducer before more cache
or frame tuning. Authenticate the range descriptor chain before claiming full
container integrity. Then test packed metadata and static/dynamic frames with
inline/cache/prefetch factors separated. P5 packed lower binding and publish
integration must exist before treating the three innovations as an end-to-end
workspace lifecycle result.
