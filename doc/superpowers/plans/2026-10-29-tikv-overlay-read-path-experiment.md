# TiKV overlay read-path experiment, 2026-10-29

## Scope and provenance

This experiment adds opt-in read-stage observability and records a local TiKV
read-policy experiment. It does not change metadata transactions, ACL caching,
key layout, or the default data-fetch policy.

The single configuration candidate was rejected for general use: mixed-I/O
p99 regressed and most active-bandwidth gains disappeared when including the
tool wall tail. The committed code adds diagnostics only.

The source starts at `2b85a38` (open PR #380, packed unified read-plan cache).
The draft PR targets main and therefore includes that prerequisite commit.
Do not interpret the combined main diff as entirely new work from this task.

Full local report: `/home/hxy/REPORT-tikv.md`.
Preserved evidence: `/home/hxy/evidence-tikv/`, including launcher argv, YAML,
fio JSON, host CPU/memory/disk snapshots, TiKV/PD counters, stage snapshots,
source hashes, native fixture, and all build/test logs.

## Deployment and validation

PD and TiKV use `pingcap/{pd,tikv}:v8.5.0`, host networking, and
`/home/hxy/tikv-data/{pd,tikv}`. PD listens on 127.0.0.1:2379/2380;
TiKV listens on 127.0.0.1:20160 with status 20180. The store is Up.
There is one store; PD's default max-replicas=3 was not changed. TiKV has
a 4 CPU / 5 GiB container limit. The task-owned RustFS is version 1.0.4,
endpoint http://127.0.0.1:9000; image digests and complete deployment argv
are preserved in `deployment-versions.json` and `*-deployment.json`.

The actual TiKV CLI argument is `--meta-tikv-pd-endpoints`, not a tikv://
`--meta-url`. Successful initialization:

```bash
target/release/brewfs workspace --meta-backend tikv \
  --meta-tikv-pd-endpoints 127.0.0.1:2379 \
  --workspace-namespace tikv-task-smoke init-volume
```

Both arms were written through TiKV-backed FUSE. For native overlay, a
task-owned offline fixture converted the populated seed upper into a sealed
lower, followed by production `workspace create --from <revision>` to create
an empty upper. This validates lower reads, not the production seal lifecycle.
Full 2 GiB SHA-256 readback succeeded. Before measurement, the new upper had
zero delta/extent rows, owned bytes, and owned slices. The 74 immutable lower
records have fingerprint
`7f1b751ef9ac508dc6fead0e7aeee4e90d7df9d8dfa8962176801515103bd9ce`.
Each arm has 512 S3 data objects, 2,147,485,696 bytes including object headers.

## Measurement method

The task-owned Docker Compose runner uses the existing
`brewfs-workspace-xfstests:ttl-knob` image, host networking, and privileged FUSE.
The corpus is four 512 MiB files. Each file repeats a deterministic 4 MiB
`Random(20261029 + file_index).randbytes(...)` block 128 times. Compression
is off. Block/chunk sizes are 4/64 MiB; read RAM budget is 4 GiB, SSD is zero,
prefetch is off, writeback mode is upload_before_commit. FUSE uses 16 workers,
max_background=512, read direct I/O, keep_cache=0, and cache TTL=0.

```bash
fio --name=r --directory=<arm>/mnt --rw=randread --bs=4k \
  --size=512m --numjobs=4 --ioengine=libaio --iodepth=32 --direct=1 \
  --runtime=30 --time_based --group_reporting \
  '--filename_format=r.$jobnum' --randseed=20261029 \
  --output-format=json --output=<arm>/fio-<round>.json
```

W uses at least two complete 2 GiB random passes, until adjacent bandwidth
differs by less than 10%, followed by three 30-second runs without dropping
caches. C is one 30-second run after sync/drop_caches on the same mount;
process and object-service caches are retained. Each phase checks that no
compiler is running. Completed fio reads equal FUSE reads and fuse_dispatch
counts, with fio errors zero.

## Primary validation with historical fio 3.36

The formal repeated comparison uses the host fio 3.36 binary and its
libraries mounted read-only into the same Compose container. BrewFS, the
corpus, TiKV/S3 namespaces, cache budgets, and W/C procedure are unchanged.
The four current rows below share fio 3.36. Historical Redis still has
unverified corpus/binary/object-service parity; do not attribute the full
difference to the metadata backend. W is the median of three runs; C is one
kernel-cache-drop run retaining the client caches.

| Backend / policy / arm | W MiB/s | W p99 ms | C MiB/s | C p99 ms |
|---|---:|---:|---:|---:|
| Redis historical flat | 214.263 | 4.293 | 73.110 | 80.216 |
| Redis historical native | 188.630 | 5.079 | 102.223 | 52.691 |
| TiKV default flat | 27.407 | 46.399 | 9.941 | 233.832 |
| TiKV default native | 21.107 | 46.399 | 9.272 | 214.958 |
| TiKV whole-object flat | 43.317 | 18.219 | 42.106 | 18.481 |
| TiKV whole-object native | 39.317 | 20.840 | 40.128 | 19.268 |

All W samples (MiB/s), preserving every sample:

- default flat: 27.407 / 31.830 / 16.135.
- default native: 26.342 / 21.107 / 14.462.
- whole-object flat: 44.257 / 43.066 / 43.317.
- whole-object native: 38.673 / 41.395 / 39.317.

| Stage, median us/read | Default flat | Default native | Whole flat | Whole native |
|---|---:|---:|---:|---:|
| fuse_dispatch | 152.046 | 238.916 | 21.378 | 31.251 |
| resolve | 0.000 | 5.119 | 0.000 | 3.640 |
| row_cache | 0.000 | 2.525 | 0.000 | 1.652 |
| meta_backend | 0.480 | 5.301 | 0.200 | 1.886 |
| extent_plan | 0.000 | 9.171 | 0.000 | 6.687 |
| data_path | 132.202 | 209.977 | 5.935 | 9.729 |

| W median counter/read | Default flat | Default native | Whole flat | Whole native |
|---|---:|---:|---:|---:|
| tikv_rpc_per_read | 0.000204277 | 0.001511208 | 0.000129251 | 0.000808041 |
| tikv_read_rpc_per_read | 0.000204277 | 0.000993079 | 0.000129251 | 0.000579537 |
| tikv_read_keys_per_read | 0.000209027 | 0.000962238 | 0.000126246 | 0.000509993 |

Native W improves 86.3% in this measured working set. Whole-object W
has zero additional object GETs in every arm/run; the full-block cache holds
the corpus. Decreased RPC/read partly reflects more reads amortizing the
same maintenance work, not new metadata RPC coalescing. Default W has large
variance, so its arm medians do not isolate a fixed overlay penalty.
Spans are nested and include background metadata work; do not add them.
This remains a scoped configuration result, not acceptance of a new default.
Cold sparse remote read amplification and complete candidate broad guards
remain separate requirements. Exact artifacts: `fio336-default/` and
`fio336-whole-object/` under the evidence directory.

## Initial fio 3.39 results and historical reference

The first Compose image used fio 3.39, while historical raw JSON identifies
fio 3.36. The initial self-comparison below has matching versions before/after,
but is not a version-matched historical comparison. The host fio 3.36 binary
and its libraries can be mounted read-only into the same Compose container
using `compose-fio336.yaml`; completed version-aligned primary results are above.
Do not silently combine samples from these runs.

| Backend / arm | W median MiB/s | W p99 median ms | C MiB/s | C p99 ms |
|---|---:|---:|---:|---:|
| Redis historical flat | 214.263 | 4.293 | 73.110 | 80.216 |
| Redis historical native | 188.630 | 5.079 | 102.223 | 52.691 |
| TiKV default flat | 21.025 | 45.351 | 12.072 | 130.548 |
| TiKV default native | 24.991 | 49.021 | 9.426 | 219.152 |

Historical Redis data was not rerun. Its W samples include slow third runs:
flat 224.334/214.263/79.260, native 194.269/188.630/51.675 MiB/s.
TiKV default W samples are flat 19.501/30.355/21.025 and native
24.991/30.691/15.698 MiB/s. Keep every sample.

This is not a backend-only causal comparison: historical corpus byte identity,
binary identity, object-service version, and actual cache-hit parity are
unverified. Historical raw data is preserved in `redis-reference/stage-v3`.
The large variance also prevents interpreting native > flat as an overlay win.

| W stage, median wall us/read | TiKV flat | TiKV native |
|---|---:|---:|
| fuse_dispatch | 257.870 | 190.149 |
| resolve | 0 | 4.521 |
| row_cache | 0 | 2.169 |
| meta_backend | 0.356 | 3.468 |
| extent_plan | 0 | 8.070 |
| data_path | 236.097 | 164.242 |

These are nested spans; do not add them. Metadata spans include background
maintenance. Flat zeros indicate that workspace probe boundaries were unused.
Probe details are documented in `doc/operations/observability.md`.

W median read RPC/read: flat 0.000266279, native 0.000911762.
Native all kv RPC/read is 0.001266047. Server read-key counts/read are
flat 0.000284856, native 0.000854451. RPC counts use TiKV gRPC kv_* counters;
read RPCs are get/batch_get/scan. Keys use the server key-read histogram sum,
not an exact client requested-key trace. Native lease maintenance generates
commit/prewrite/lock RPCs; these are not per-read write transactions.

Source inspection found existing inode batch_get, SDK region caching,
lease-bound row caching, and native read-plan caching. An optimistic rollback
without mutations returns locally. These measurements do not justify adding
another TiKV cache or changing transaction consistency.

## Single hypothesis and configuration candidate

Before changing the policy, `pre-optimization-decision.json` recorded the first
complete 524,288-read passes: flat 85 read RPCs and 38,605 object GETs;
native 841 read RPCs, 1,108 all kv RPCs, and 38,338 object GETs. Native data
time was 365.34 us/read versus metadata 7.66 us/read.

Small range reads use an independent ReadPageCache: 64 KiB pages, 2 GiB
capacity, TTL 120 seconds, idle expiry 30 seconds. A stable adjacent warmup
bandwidth does not prove all-hit data caching. Default W GET counts were
10,253/3,694/8,404 (flat) and 6,979/3,404/12,245 (native).

The only candidate is existing `cache.read_fetch_policy: whole-object`,
with the same binary, corpus, budget, fio, and warm/cold procedure. It can
populate the full-block cache instead of repeatedly fetching expiring range
pages. It may amplify cold sparse remote reads, so it is not a new default.

Reproduce on the task machine after all compilers exit:

```bash
docker compose -f /home/hxy/evidence-tikv/compose.yaml run --rm --no-deps perf \
  bench --evidence-root /home/hxy/evidence-tikv/whole-object \
  --read-fetch-policy whole-object
python3 /home/hxy/evidence-tikv/analyze.py \
  --root /home/hxy/evidence-tikv/whole-object
```

The candidate's rejection and broad/mixed guard results are recorded below.
README performance tables are not changed by this experiment.

## Build and validation

```bash
CARGO_PROFILE_RELEASE_DEBUG=0 RUSTFLAGS='-C debuginfo=0' \
  cargo build --release -p brewfs --bin brewfs --features workspace-overlay -j 6
```

The build completed in 39m37s. Measured SHA-256:
`76e3fcacaa2bc3424f01810c09b96ec4446acda2309dc227045956ea89896678`.
Probe-disabled calls skip clocks/atomic updates, but retain a gate check;
literal zero CPU overhead is not claimed.

The hard workspace lib/bins test gate passed. `cargo test -p brewfs --lib`
passed with 1,275 tests, zero failures, and 262 preexisting ignored tests.
No new ignores or ACL permission-cache TTL changes were introduced.
An extra full `--features workspace-overlay` lib-test build received SIGTERM
after 756.8 seconds, before tests executed. Kernel logs contained no OOM
record; the cause is unknown and this extra suite is not reported as passed.
Remaining CI results are preserved in `gates-remaining.json`.

Packed, multi-store TiKV, remote network latency, production seal lifecycle,
and historical byte-for-byte corpus parity are unverified. Probe off/on
overhead is unverified unless an independent overhead artifact is present.

## Rejection, mixed guard, and final integrity

Reject `whole-object` as a general performance optimization. Its warm-read
gain does not satisfy the first-class mixed-I/O latency and tail requirements.
The matched guard uses fio 3.39, direct libaio, depth 32, one job, 512 MiB,
20 seconds, 4 KiB randrw, 70% reads, identical budgets, strict zero-byte drain,
and fresh namespaces/buckets. This guard is separate from primary fio 3.36.

| Mixed metric | Default | Whole-object |
|---|---:|---:|
| Read active MiB/s | 0.786 | 1.909 |
| Write active MiB/s | 0.350 | 0.824 |
| Read p99 ms | 173.015 | 244.318 |
| Write p99 ms | 177.209 | 244.318 |
| Fio active seconds | 20.003 | 20.001 |
| Tool wall seconds | 27 | 62 |
| Read effective wall MiB/s | 0.582 | 0.616 |
| Write effective wall MiB/s | 0.259 | 0.266 |
| Post-write drain seconds | 0 | 0 |

Read/write p99 regress by 41.2%/37.9%. Effective wall read/write bandwidth
improves only 5.8%/2.5%, despite active gains of 142.9%/135.5%. This is a single
regression screen, sufficient to withhold acceptance, not repeatability proof.
The complete artifact comparator output is `guard/comparison.{md,tsv}`.
PUT/GiB is 261,998/261,398; average PUT and upload batch size are about 4 KiB,
with partial-tail ratio 1 in both arms.

The default guard passes all seven core fio scenes and metaperf/dirstress/
dirperf, with zero dirty/pending/buffer tails and normal teardown. Default
randwrite wall is 210 seconds; its 209.338 seconds is already included in fio
runtime/BW. The candidate's remaining metaperf and directory guards were
intentionally stopped after the completed randrw rejection; exit 143 is not
reported as a natural pass. Candidate metadata/directory and remaining broad
scenes are unverified. The exact stop reason and diagnostics are preserved in
`guard/rejection.json`. No second optimization was attempted, and no failed
new performance algorithm or default-policy change remains in the tree.

All 55 primary phases pass fio error/count/version checks and do not overlap
the recorded CI commands; source/binary hashes match the release build.
After primary measurements the sealed lower fingerprint remains unchanged,
upper delta/extent rows and owned bytes/slices remain zero, and both original
512-object S3 inventories retain identical keys, sizes, and ETags. See
`measurement-validity.json` and `final-integrity.json`.

All required local CI gates pass, including both FUSE runtime checks,
workspace check/build/test, shell checks, and all-target Clippy with warnings
denied. The separate overlay-feature lib-test build interruption remains
unverified as described above. Draft PR: https://github.com/brewfs/brewfs/pull/381.
