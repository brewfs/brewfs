# Hot Workspace handle profiling

The A355 CPU profile of repeated warm opens showed copies in the FUSE request
and mutation-driver futures, including FLUSH on read-only handles. The hot
handle path now allocates the O_TRUNC and writeback futures only when those
branches are selected. A read-only FLUSH polls the same owned operation once;
if it waits, the existing driver takes it over before yielding to the caller.
Dirty-state inspection, cancellation ownership, permission checks, versioned
commits, orphan cleanup and error propagation remain in the original paths.

`record_open` also clones the layer vector only for the authenticated writable
commit. Read-only opens still decode and check the retained permission snapshot.
No cache capacity, TTL, lease validation or consistency rule changes here.

The mixed-I/O guard exposed repeated `ReadViewChanged` conflicts exhausting
the old three-attempt limit. Such conflicts now start a shared 10 s retry
budget with local-publication waits, with yields followed by 1–20 ms backoff.
Each attempt still captures a fresh authoritative fence, writes into private
output, and validates before delivery. Fencing, corruption and transport
errors remain terminal. The budget bounds retries; individual backend calls
retain their own timeouts. Deterministic KV tests cover five consecutive final
validation conflicts, continuous conflict timeout, and fatal errors after retry.

## Stage timings

Build with `observability`, then opt in on a diagnostic mount:

```bash
BREWFS_HOT_PATH_PROFILE=1 brewfs --metrics --metrics-addr 127.0.0.1:8910 \
  mount --config /absolute/brewfs.json /absolute/mountpoint
curl --fail http://127.0.0.1:8910/metrics
```

`brewfs_hot_path_calls_total{stage="..."}` and
`brewfs_hot_path_nanoseconds_total{stage="..."}` have eleven fixed labels:

| Stage | Timed work |
|---|---|
| `open_admission` | Handle memory reservation |
| `open_stat` | Fresh attributes used to prepare OPEN |
| `open_record` | Metadata open, including permission preparation and any commit |
| `open_handle` | Inode and handle registry updates |
| `permission_gate` | Waiting for the local metadata mutation gate |
| `permission_snapshot` | Cached permission lookup, including a miss if needed |
| `permission_decode` | Resolving inode and ACL rows from the snapshot |
| `permission_miss` | Preparing and fetching an uncached permission snapshot |
| `extent_query` | Fetching bounded upper extent rows |
| `directory_fence` | Capturing and validating the directory view |
| `directory_query` | Fetching the initial upper dentry page per layer |

These are elapsed times, including awaits and failed/cancelled operations.
Nested stages overlap: do not sum `open_record` and its permission children.
The FUSE handler histogram excludes kernel/dispatch queueing; a caller's
`open(2)` can also include LOOKUP and GETATTR. Request counts at these stages
are logical calls, not Redis commands or network round trips.

Profiling is disabled by default, and absent without `observability`.
The counters occupy fixed memory and never retain paths or inode IDs.
Compare normal builds with profiling off; use separate diagnostic runs for
these counters, Redis MONITOR, and CPU sampling.

## Reproduction and tests

The Redis/RustFS FUSE harness, raw calls, binary hashes, matching-cache
comparisons and paired mixed-I/O guard are in the
[experiment repository](https://github.com/Ivanbeethoven/ax-multi-agent-experiments/tree/codex/a355-hot-open-evidence-20261114/experiments/overlay-open-profile-a355-20261114).

`tools/perf/check_overlay_hot.sh` runs the selected KV, permission, lease,
publication, cache and driver tests without SQLite. The packed open tests
also bound the hot futures to 4 KiB and check that clean read-only FLUSH can
finish without a scheduling round trip while rejecting a retired handle.
The opt-in profiling test is `hot_path_profile_is_opt_in_and_has_only_fixed_labels`.
The SQL-inclusive full test suite is a separate gate; these selected tests do
not claim to cover it.

## A355 result

With the same optimized release settings, three paired runs over 10,000 real
files reduced median total time from 41.779 s to 40.476 s (3.12%). Mean open
time across the three runs fell by 772 ms and close by 658 ms. The two-file
case improved in all three pairs; the 32-file case did not establish a stable
gain. All measured lower object reads/bytes remained zero.

Redis query counts did not decrease in this change. The hot small workloads
still perform two directory-view captures per round. The large workload also
refills rows after the unchanged 60 s TTL; a separate 61 s idle probe reproduced
permission/extent queries while payload data remained hot. Full per-pair
results, raw calls, CPU profiles and the rejected first attempt are linked above.

Both the 5 s and follow-up 30 s paired mixed-I/O checks completed without EIO.
The 30 s throughput medians differed by at most 1.22%; the 32-file group's read
and write P99 medians increased 5.59% and 4.33%. Individual read tails varied
in both directions; this is not evidence of a tail-latency improvement or of
long-run stability. Final-fsync wall time did not materially increase.
