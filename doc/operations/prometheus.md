# Compile-time observability

Measurements, raw rounds, validation results, and scope limits are recorded in
the [acceptance report](../testing/observability-2026-10-31.md). After rebasing
onto `cba857b5`, five interleaved pairs with the same executable and five with
the baseline/default builds place the ordinary default build within measured
noise. See the [noise experiment](../testing/observability-overhead-2026-11-01.md).

## JuiceFS comparison

Research reference: JuiceFS `ddf515c5ee190fb469a17cf054273f8fcc4909dd`,
`pkg/vfs/vfs.go`, `pkg/chunk/cached_store.go`, `pkg/meta/base.go`,
`cmd/mount.go`, `cmd/stats.go`, and `cmd/profile.go`.
The pinned [stats implementation](https://github.com/juicedata/juicefs/blob/ddf515c5ee190fb469a17cf054273f8fcc4909dd/cmd/stats.go)
and [profile implementation](https://github.com/juicedata/juicefs/blob/ddf515c5ee190fb469a17cf054273f8fcc4909dd/cmd/profile.go)
define the terminal behavior used in this comparison.

| Capability | JuiceFS | Existing BrewFS | Required addition |
|---|---|---|---|
| Prometheus endpoint | HTTP metrics handler and `.stats` | `.stats` text only | Feature-gated, opt-in HTTP endpoint |
| FUSE operations | Duration histograms labelled by method | Some operation counts and cumulative microseconds | All implemented handlers, buckets and errno counters |
| Object traffic | Request duration histograms and transferred bytes | Block-store GET/PUT counts, bytes and total latency | Client boundary, streaming bodies and object kinds |
| Caches | Block and metadata cache metrics | Block/page and metadata counters | Stable cache labels; no fabricated kernel page-cache hits |
| Metadata | Operation and transaction histograms | Aggregate timings and client cache metrics | Operation/backend labels and histogram buckets |
| Background work | Flush, compaction and GC statistics | Detailed writeback stage and debt gauges | Publish debt in the HTTP scrape |
| Mount identity | Build/version and uptime | Uptime and control-plane information | Version/profile/backend info gauge |
| Terminal stats | Counter deltas; latency columns use histogram sum/count (mean) | Separate `brewfs-stats` utility | `brewfs stats`, including estimated P50/P99 |
| Profile | Reads `.accesslog`, groups operations and latency | Existing tracing/profiling feature and perf scripts | CPU signal profiling is optional, outside P0/P1 |

JuiceFS `stats` does not itself provide exact per-operation P50/P99 in this
revision. BrewFS estimates these from interval histogram bucket deltas.

All new metrics, their names, HTTP serving, and the stats subcommand require
`--features observability`. This feature is excluded from `default`.

## Build and run

```sh
cargo build --release --features observability
target/release/brewfs mount --config examples/mount-config.local.yaml --metrics
curl --fail http://127.0.0.1:9567/metrics
target/release/brewfs stats --interval 1
target/release/brewfs stats --address 127.0.0.1:19567 --interval 2 --count 10
```

`--metrics` (or `BREWFS_METRICS=true`) is required to start the listener.
`--metrics-addr 127.0.0.1:19567` (or `BREWFS_METRICS_ADDR`) overrides its
address. Merely setting the address does not enable it. A bind failure fails
the mount startup; the listener is closed when mount execution finishes.
The endpoint is process-scoped to the CLI's single mount. SDK users do not
automatically start a server. No path, key, inode, bucket name, URL, or
credential is included in metric labels.

Without the feature, these flags and the `stats` subcommand are absent.
The legacy virtual `.stats` file and its periodic collection task also require
the feature. A real file named `.stats` continues to work normally.
Existing legacy statistics assertions still execute in unit-test builds;
`cfg(test)` is never set by a normal debug or release build.
Functional clocks and counters used for deadlines, memory admission, cache
policy, and writeback scheduling remain in place.

The zero-symbol and overhead evidence covers the ordinary default release.
Legacy read observers used by optional packed/native profiles have not been
comprehensively feature-gated in this change. Their full gating remains an
unresolved requirement; the default-release evidence does not validate them.

## Metric reference

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `brewfs_fuse_op_total` | counter | `op` | Completed FUSE handlers, successes and errors |
| `brewfs_fuse_op_latency_seconds` | histogram | `op` | Handler time; includes metadata and I/O waits, excludes kernel queue time |
| `brewfs_fuse_op_errors_total` | counter | `op`, `errno` | Failed handlers; positive numeric errno; series appears on first failure |
| `brewfs_object_get_total` | counter | `kind` | Object client GET attempts, including errors, misses and cancellations |
| `brewfs_object_get_bytes_total` | counter | `kind` | Actual received object body bytes, including consumed bytes before an error |
| `brewfs_object_put_total` | counter | none | Object client PUT attempts, including create-only and vectored PUT |
| `brewfs_object_put_bytes_total` | counter | none | Payload bytes acknowledged by successful PUT calls |
| `brewfs_object_op_latency_seconds` | histogram | `op` | GET/PUT call lifetime; streaming GET includes body consumption, errors and cancellation |
| `brewfs_cache_hits_total` | counter | `cache` | Userspace cache hits |
| `brewfs_cache_misses_total` | counter | `cache` | Userspace cache misses |
| `brewfs_metadata_op_total` | counter | `op`, `backend` | Metadata store API calls ending in success, error or cancellation |
| `brewfs_metadata_op_latency_seconds` | histogram | `op`, `backend` | Time in the metadata store API, including transactions and network waits |
| `brewfs_writeback_dirty_bytes` | gauge | none | Writer memory held by dirty data |
| `brewfs_writeback_pending_upload_bytes` | gauge | none | Committed recent slices awaiting upload |
| `brewfs_writeback_inflight_bytes` | gauge | none | Payload bytes in remote uploads |
| `brewfs_mount_info` | gauge, value 1 | `version`, `profile`, `backend` | Package version, lowercased volume-format variant, metadata backend |
| `brewfs_mount_uptime_seconds` | gauge | none | Seconds since the endpoint's registry was created |

Histograms expose the conventional `_bucket{le="..."}`, `_count`, and `_sum`
series, including `le="+Inf"`. Counters reset on process restart.
FUSE operations are bounded by the compiled Filesystem trait; the decorator
forwards admission, cancellation, lifecycle, and default-method overrides.
The `rename2` handler shares the `rename` operation label; the notify/umask
variants likewise share their underlying operation labels.
Metadata `op` is a compiled MetaStore method name, and `backend` is one of
`sqlx`, `redis`, `etcd`, `tikv`, `other`. Timers surround the store implementation,
so metadata cache hits do not masquerade as remote metadata requests.

Object `op` is `get` or `put`. `kind` comes from typed read context:
`packed` for PackedV3 objects, `large` for external payload, and `upper` for
native/flat/unclassified objects. Counts are client API attempts, not hidden
HTTP retries by the S3 SDK. HEAD and DELETE are outside these GET/PUT families.
Transferred bytes are physical object payload bytes, before decompression.

`cache` is `block`, `page`, or `meta`. `page` is BrewFS's userspace read-page
cache, not Linux's page cache; userspace cannot count kernel cache hits.
`meta` combines stat, lookup, slice-list, and open-file cache lookups. No
unsupported bundle-cache series is synthesized. Cache and debt snapshots
refresh every 100 ms. Debt categories overlap and must not be summed.
The existing detailed writeback stage/batch/flush metrics remain available in
the feature-enabled `.stats` interface; this HTTP endpoint publishes the
stable families listed above.
Cache and debt families require the normal VFS statistics source. The
experimental EntityFuse mount currently contributes FUSE handlers only;
its direct backend calls bypass the ObjectClient traffic counters.

Bucket bounds (all are emitted in seconds):

| Operations | Bounds |
|---|---|
| Read and GET | 100, 250, 500 µs; 1, 2.5, 5, 10, 25, 50, 100, 250, 500 ms; 1 s |
| Write, flush, fsync, release, fallocate, copy-file-range and PUT | 1, 2.5, 5, 10, 25, 50, 100, 250, 500 ms; 1, 2.5, 5, 10 s |
| Other FUSE and metadata | 50, 100, 250, 500 µs; 1, 2.5, 5, 10, 25, 50, 100 ms |

`brewfs stats` computes interval counter deltas divided by measured elapsed
wall time. P50/P99 use linear interpolation of interval cumulative bucket
deltas, like Prometheus `histogram_quantile`; the `~` heading marks estimates.
An idle interval shows `--`. Quantiles in the overflow bucket are capped at
the last finite boundary. Object throughput is MiB/s (1 MiB = 1,048,576 bytes).
Ctrl-C exits the viewer; `--count` supports finite captures.

## Prometheus

```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: brewfs
    static_configs:
      - targets: ['127.0.0.1:9567']
```

Use a reachable mount address if Prometheus runs in a different network
namespace. The default bind is loopback.

```promql
sum by (op) (rate(brewfs_fuse_op_total[1m]))
histogram_quantile(0.99, sum by (op, le) (rate(brewfs_fuse_op_latency_seconds_bucket[5m])))
sum(rate(brewfs_object_get_bytes_total[1m])) / 1048576
sum by (cache) (rate(brewfs_cache_hits_total[1m]))
  / (sum by (cache) (rate(brewfs_cache_hits_total[1m]))
   + sum by (cache) (rate(brewfs_cache_misses_total[1m])))
```

CPU profiling by SIGUSR1 is not implemented in this change. Existing optional
`profiling` builds and `tools/perf/` remain the profiling entry points.

## Repeat the acceptance checks

Preserve three separate release binaries: the clean baseline, the candidate
without the feature, and the candidate with `observability`. Use identical
compiler/profile settings for all three. Run the repository's Rust CI gate
and both workspace-overlay library test variants before measuring.

```sh
sudo python3 tools/perf/check_feature_observability.py \
  --default /path/to/default --enabled /path/to/enabled \
  --output /tmp/observability-smoke
python3 tools/perf/measure_feature_observability.py \
  /path/to/baseline /path/to/default /path/to/enabled \
  --output /tmp/observability-perf --runtime 30 --rounds 3
```

The smoke check uses actual FUSE mounts and a Prometheus container. Run it as
root because fresh metadata roots are owned by UID 0. It checks both runtime
switch states, the default and a custom address, file data, counters, `stats`,
Prometheus `up=1`, and listener cleanup. Output directories must be empty.

The performance helper uses the compose runner with Redis metadata and LocalFS
objects, direct I/O, fixed 512 MiB contents verified by SHA256, and rotating
baseline/default/enabled order. It includes mixed I/O and metaperf guards.
The enabled process receives a scrape every second. It records all raw rounds,
binary hashes, parameters and medians, and cleans its own compose services and
volumes. Upload-before-commit makes completion independent of the diagnostic
debt counters that are unavailable in the default binary. This measures local
instrumentation overhead; it is not an S3 or managed-backend comparison.

For a default-build noise control, use the same default executable in both
arms, then repeat with the clean baseline and candidate. Each command below
runs five fixed A1 B1 … A5 B5 pairs with a fixed fio seed, fresh mounts and
volumes, no scraping, and no periodic writeback sampler. Keep every round;
the analyzer reports raw P99, medians, ranges, sample standard deviations,
and paired log-ratio confidence intervals. Choose new output directories.

```sh
python3 tools/perf/measure_feature_observability.py \
  /path/to/default /path/to/default --two-arm --rounds 5 --runtime 30 \
  --tools 'fio-randread fio-seqread fio-randrw' --output /tmp/obs-same
python3 tools/perf/summarize_observability_noise.py /tmp/obs-same
python3 tools/perf/measure_feature_observability.py \
  /path/to/baseline /path/to/default --two-arm --rounds 5 --runtime 30 \
  --tools 'fio-randread fio-seqread fio-randrw' --output /tmp/obs-different
python3 tools/perf/summarize_observability_noise.py /tmp/obs-different
```
