# BrewFS Test Script Reference

This document is the script-level index for BrewFS testing. It explains which
entry point to run, what it starts, where results are written, and which files
are implementation details rather than user-facing commands.

The default build uses the async-fuse io_uring runtime:

```toml
default = ["fuse-io-uring-runtime"]
```

Use `--no-default-features --features fuse-tokio-runtime` only for an explicit
runtime comparison. Do not combine both runtime features in one build.

## Quick Selection

Run commands from the repository root.

| Goal | Recommended command | Typical duration |
| --- | --- | ---: |
| Format and unit tests | `cargo fmt --all --check && cargo test --workspace --lib --bins -- --test-threads=1` | 2-10 min |
| POSIX path semantics | `bash docker/compose-xfstests/run_redis_pjdfstest.sh` | 5-15 min |
| FUSE correctness smoke | `bash docker/compose-xfstests/run_redis_xfstests.sh --cases "generic/001 generic/002 generic/100"` | 5-15 min |
| Full Redis + RustFS xfstests | `bash docker/compose-xfstests/run_redis_xfstests.sh` | hours |
| LTP filesystem coverage | `bash docker/compose-xfstests/run_redis_ltp.sh` | 30-90 min |
| CI-safe stress | `bash docker/compose-xfstests/run_redis_stress_ng.sh --profile smoke` | a few min |
| Complete performance run | `bash docker/compose-xfstests/run_redis_perf.sh --read-throughput-profile` | 10-30 min plus build |
| Metadata performance smoke | `bash docker/compose-xfstests/run_redis_meta_perf.sh --quick` | 2-5 min plus build |
| Validate metrics artifacts | `bash docker/compose-xfstests/run_redis_observability.sh` | 5-15 min |
| Compare with JuiceFS | `bash docker/compose-xfstests/run_juicefs_perf.sh --tools "metaperf dirperf"` | workload-dependent |
| Multi-node deployment | `bash tests/scripts/distributed-tests/run-distributed-tests.sh all` | environment-dependent |

## Common Requirements

The Compose runners require Linux, Docker Engine, Docker Compose V2, FUSE
access, and enough disk for Rust build output, container layers, RustFS data,
and artifacts. The runner containers are privileged because they mount FUSE.

Useful checks:

```bash
docker compose version
test -e /dev/fuse
df -h . /var/lib/docker
```

Host wrappers build `target/release/brewfs`, copy the stripped binary to
`target/docker/brewfs`, build the suite image, start dependencies, run the
container-side driver, and normally execute `docker compose down -v` on exit.
Use `--keep` only while debugging and clean the resources afterward.

Most Compose artifacts are stored below:

```text
docker/compose-xfstests/artifacts/
  run-*/             xfstests and LTP
  pjdfstest-run-*/   pjdfstest
  perf-run-*/        fio, metadata tools, stress-ng, and observability
```

## Rust And Static Checks

### Standard PR Checks

```bash
cargo fmt --all --check
cargo test --workspace --lib --bins -- --test-threads=1
cargo clippy --workspace
```

The serialized test setting matches CI and reduces interference between tests
that share process-global state. To test the alternative FUSE runtime:

```bash
cargo test --workspace --lib --bins \
  --no-default-features --features fuse-tokio-runtime \
  -- --test-threads=1
```

### Integration Test Targets

Run one target with `cargo test --test <name> -- --nocapture`.

| Target | Purpose |
| --- | --- |
| `rename_integration_test` | Rename behavior and error handling. |
| `compactor_test` | Core compactor behavior without external services. |
| `compaction_worker_test` | Compaction worker scheduling and state. |
| `compaction_perf_test` | Compaction performance instrumentation. |
| `gc_test` | Block-store garbage collection. |
| `gc_compact_e2e_test` | End-to-end GC and compaction integrity. |
| `redis_compact_conflict_test` | Redis compaction conflicts; requires Redis. |
| `native_fsstress_redis_rustfs_docker` | Native fsstress-style Redis + RustFS regression. |
| `test_brewfs_kvm_integration` | qlean VM/KVM filesystem integration. |
| `test_brewfs_qlean_multinode_smoke` | qlean multi-node smoke test. |

`benches/brewfs_bench.rs` is the Criterion benchmark entry point:

```bash
cargo bench --bench brewfs_bench
```

`fuzz/fuzz_targets/fs_ops.rs` is run with cargo-fuzz. See
[fuzz_testing_guide.md](fuzz_testing_guide.md) for setup and corpus guidance.

## Compose Correctness Suites

The scripts in `docker/compose-xfstests/` are the preferred filesystem test
entry points. They use the same host-binary build path and artifact layout.

### xfstests

| Backend | Host wrapper | Object store |
| --- | --- | --- |
| Redis | `run_redis_xfstests.sh` | RustFS, fixed |
| SQLite | `run_sqlite_xfstests.sh` | local by default; `--s3` selects RustFS |
| etcd | `run_etcd_xfstests.sh` | local by default; `--s3` selects RustFS |
| TiKV | `run_tikv_xfstests.sh` | RustFS, fixed |

Examples:

```bash
# Smoke set
bash docker/compose-xfstests/run_redis_xfstests.sh \
  --cases "generic/001 generic/002 generic/100"

# One case with raw ./check arguments and no default exclude selection
bash docker/compose-xfstests/run_redis_xfstests.sh \
  --check-args "-fuse generic/091"

# Keep containers and mount for diagnosis
bash docker/compose-xfstests/run_tikv_xfstests.sh \
  --cases "generic/001" --namespace debug-001 --keep
```

Common options are `--cases`, `--check-args`, `--skip-cases`, and `--keep`;
backend support differs slightly, so use `<script> --help` before automation.
Default exclusions are in `tests/scripts/xfstests_slayer.exclude`. The
container-side implementation is `run_xfstests_in_container.sh`; do not call
it directly on the host.

Generate or refresh a report without rerunning the suite:

```bash
bash docker/xfstests_report.sh \
  docker/compose-xfstests/artifacts/run-XXXXXXXX
```

### pjdfstest

| Backend | Host wrapper |
| --- | --- |
| Redis + RustFS | `docker/compose-xfstests/run_redis_pjdfstest.sh` |
| TiKV + RustFS | `docker/compose-xfstests/run_tikv_pjdfstest.sh` |

The supported corpus covers permissions, links, special nodes, rename,
unlink, path errors, and timestamps. The default skip file is
`docker/compose-xfstests/pjdfstest_skip_tests.txt`; it is currently expected
to remain empty for the required corpus.

```bash
# Required full corpus
bash docker/compose-xfstests/run_redis_pjdfstest.sh

# Recheck without repository defaults
bash docker/compose-xfstests/run_redis_pjdfstest.sh --no-default-skip

# Select or exclude paths using prove-compatible arguments and regexes
bash docker/compose-xfstests/run_tikv_pjdfstest.sh \
  --skip-patterns "tests/open/.*" --namespace pjdfs-debug
```

`run_pjdfstest_in_container.sh` is the internal driver. The separate
`docker/compose-pjdfstest/` directory is an older Redis-only implementation;
use the `compose-xfstests` wrapper for new runs because it shares the current
binary build, RustFS setup, skip policy, and artifact format.

### Linux Test Project

| Backend | Host wrapper |
| --- | --- |
| Redis + RustFS | `run_redis_ltp.sh` |
| SQLite + RustFS | `run_sqlite_ltp.sh` |
| etcd + RustFS | `run_etcd_ltp.sh` |
| TiKV + RustFS | `run_tikv_ltp.sh` |

```bash
# Default filesystem scenario and documented skips
bash docker/compose-xfstests/run_redis_ltp.sh

# Run one diagnostic case without the built-in skip file
bash docker/compose-xfstests/run_redis_ltp.sh \
  --no-default-skip --extra-args "-s iogen01"

# Add temporary skips without editing the repository
bash docker/compose-xfstests/run_etcd_ltp.sh \
  --skip-tests "case_a case_b"
```

The default exclusions are documented in
`docker/compose-xfstests/ltp_skip_tests.txt`. In particular, buffered FUSE
`iogen01` remains skipped; the direct-I/O diagnostic shape passes, but the
normal buffered profile still exposes page-cache coherency behavior. Internal
execution is handled by `run_ltp_in_container.sh`.

Refresh an LTP report with:

```bash
bash docker/ltp_report.sh \
  docker/compose-xfstests/artifacts/run-XXXXXXXX
```

### stress-ng

`run_redis_stress_ng.sh` reuses the Redis performance stack and writes a
`perf-run-*` artifact.

| Profile | Intended use |
| --- | --- |
| `smoke` | Short CI-safe directory, rename, unlink, and small-I/O mix. |
| `metadata-heavy` | Longer metadata pressure without link long tails. |
| `link-symlink-heavy` | Manual hard-link and symlink regression hunting. |
| `write-smallfile` | Small-block write and unlink pressure. |

```bash
bash docker/compose-xfstests/run_redis_stress_ng.sh --profile smoke
bash docker/compose-xfstests/run_redis_stress_ng.sh \
  --profile metadata-heavy -- --keep
```

Set `PERF_STRESS_NG_ARGS` to replace the profile-generated stress-ng command.

## Performance Scripts

### BrewFS Runners

| Backend | Script | Default tools |
| --- | --- | --- |
| Redis | `run_redis_perf.sh` | Seven fio modes, dirstress, dirperf, metaperf, looptest |
| etcd | `run_etcd_perf.sh` | fio, directory, metadata, and loop tools |
| TiKV | `run_tikv_perf.sh` | Same broad set as Redis |
| Redis metadata-only | `run_redis_meta_perf.sh` | dirstress, dirperf, metaperf, looptest |

The Redis runner is the reference implementation. Supported tools include:

- `fio-bigwrite`, `fio-bigread`
- `fio-seqread`, `fio-seqwrite`
- `fio-randread`, `fio-randwrite`, `fio-randrw`
- `dirstress`, `dirperf`, `metaperf`, `looptest`
- `object-put-bench` and `stress-ng` for specialized runs

Examples:

```bash
# Complete suite with the read throughput profile
bash docker/compose-xfstests/run_redis_perf.sh --read-throughput-profile

# Small metadata run
bash docker/compose-xfstests/run_redis_meta_perf.sh --quick

# One workload with explicit parameters
PERF_FIO_RANDRW_RUNTIME=120 PERF_FIO_RANDRW_SIZE=4g \
  bash docker/compose-xfstests/run_redis_perf.sh \
  --tools "fio-randrw"

# Buffered/direct matrix
PERF_FIO_DIRECT_MATRIX="0 1" \
  bash docker/compose-xfstests/run_redis_perf.sh \
  --tools "fio-seqread fio-seqwrite"
```

Important profiles:

| Profile | Effect |
| --- | --- |
| `--read-throughput-profile` | Enables read-only `FOPEN_DIRECT_IO`; improves large-read throughput but disables mmap on affected handles. |
| `--metadata-throughput-profile` | Enables the single-client write-open attr cache; weakens cross-client close-to-open freshness. |
| `--bigwrite-throughput-profile` | Alias for the tested writeback throughput profile. |
| `--writeback-throughput-profile` | Enables commit-before-upload, larger caches and concurrency, drain checks, and read throughput settings. |

The generated `perf-profile.env` is the source of truth for the effective
configuration. Compare it before interpreting two reports. `report.md`
contains normalized throughput, latency percentiles, wall/runtime accounting,
Redis diagnostics, and BrewFS counters.

`run_perf_in_container.sh` and `perf_metadata_fallback.py` are internal
implementation files. The fallback parser is used when a tool emits metadata
output that the main report path cannot parse directly.

The top-level `docker/run_perf_redis.sh` and `docker/run_perf_etcd.sh` files
are compatibility aliases for the corresponding `compose-xfstests` wrappers.
They add no configuration of their own.

### JuiceFS Comparison

`run_juicefs_perf.sh` mounts JuiceFS with Redis + RustFS and invokes the same
workload/reporting model. It supports cached-read, metadata, and writeback
profiles. Compare only runs with matching `PERF_*` workload settings and state
whether each mount used buffered or direct I/O.

```bash
bash docker/compose-xfstests/run_juicefs_perf.sh \
  --metadata-throughput-profile --tools "dirperf metaperf"
```

`run_juicefs_perf_in_container.sh` is internal. Do not invoke it directly.

### Observability Smoke

`run_redis_observability.sh` runs a small xfstests/perf combination and checks
that `.stats`, Redis diagnostics, warning summaries, and reports were emitted.
It can also validate an existing artifact without rerunning workloads:

```bash
bash docker/compose-xfstests/run_redis_observability.sh
bash docker/compose-xfstests/run_redis_observability.sh \
  --validate docker/compose-xfstests/artifacts/perf-run-XXXXXXXX
```

## Harness Self-tests

These scripts test the test infrastructure and do not mount BrewFS:

| Script | Checks |
| --- | --- |
| `tests/scripts/test_perf_profile_harness.sh` | Profile manifests, required knobs, console capture, and warning summaries. |
| `docker/compose-xfstests/test_perf_report_delta.sh` | Performance report delta calculations. |
| `docker/compose-xfstests/test_juicefs_direct_matrix.sh` | JuiceFS buffered/direct matrix command generation. |
| `docker/compose-xfstests/test_juicefs_perf_report.sh` | JuiceFS report parsing and normalized output. |
| `tests/scripts/test_release_install_metadata.sh` | Release workflow, download path, installer, README, and configuration-document contract. |

Run them after changing scripts or release metadata:

```bash
bash tests/scripts/test_perf_profile_harness.sh
bash docker/compose-xfstests/test_perf_report_delta.sh
bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
bash docker/compose-xfstests/test_juicefs_perf_report.sh
bash tests/scripts/test_release_install_metadata.sh
```

## Local Integration And Distributed Tests

### Metadata Services

`tests/scripts/test_meta_store.sh` creates Redis, etcd, and PostgreSQL
containers, runs their metadata-store test groups, and removes the containers
and private network. It predates the Compose filesystem suites but remains a
convenient MetaStore-only entry point.

`docker/run_integration_tests.sh` starts the integration services, builds the
demo binary, runs qlean multi-node smoke coverage, and can optionally run the
`fs_ops` fuzz target:

```bash
bash docker/run_integration_tests.sh --skip-deps
bash docker/run_integration_tests.sh --fuzz --fuzz-time 300
```

### Distributed Hosts

`tests/scripts/distributed-tests/run-distributed-tests.sh` manages real or
remote client nodes using `cluster.env`:

```bash
cp tests/scripts/distributed-tests/cluster.env.example \
  tests/scripts/distributed-tests/cluster.env
bash tests/scripts/distributed-tests/run-distributed-tests.sh prepare
bash tests/scripts/distributed-tests/run-distributed-tests.sh all
bash tests/scripts/distributed-tests/run-distributed-tests.sh cleanup
```

The files under `tests/scripts/distributed-tests/lib/` are sourced libraries,
not standalone commands: `common.sh` provides logging and shared state,
`ssh.sh` wraps remote execution and transfer, `brewfs.sh` manages remote
builds/mounts, and `tests.sh` defines the distributed workloads.

## Shared Test Infrastructure

The following files support the host wrappers. They are documented here to
make the script inventory complete, but normally should not be called as test
suites.

| File | Role |
| --- | --- |
| `docker/build_brewfs_host_binary.sh` | Builds the release binary, strips a Docker copy, and supports `BREWFS_REUSE_HOST_BINARY=1`. |
| `docker/compose-xfstests/run_xfstests_in_container.sh` | Internal xfstests mount, execution, diagnostics, and artifact driver. |
| `docker/compose-xfstests/run_pjdfstest_in_container.sh` | Internal `prove`/pjdfstest driver. |
| `docker/compose-xfstests/run_ltp_in_container.sh` | Internal LTP mount and `runltp` driver. |
| `docker/compose-xfstests/run_perf_in_container.sh` | Internal BrewFS workload and report driver. |
| `docker/compose-xfstests/run_juicefs_perf_in_container.sh` | Internal JuiceFS workload and report driver. |
| `docker/compose-xfstests/perf_metadata_fallback.py` | Metadata-output fallback parser. |
| `docker/entrypoint.sh` | General BrewFS container entrypoint, also used by integration environments. |
| `docker/etcd-maintenance.sh` | etcd compaction/maintenance sidecar command. |
| `docker/xfstests_report.sh` | Rebuilds xfstests reports from an artifact directory. |
| `docker/ltp_report.sh` | Rebuilds LTP reports from an artifact directory. |
| `tests/scripts/xfstests-prebuilt/help.sh` | Minimal helper shipped with the prebuilt xfstests archive. |

The `docker-compose.*.yml` files under `docker/compose-xfstests/` describe the
service graphs. Prefer their host wrappers because wrappers set project names,
binary paths, cleanup traps, profile variables, and artifact directories.

## KVM And Legacy xfstests

There are two xfstests families. Prefer Compose for routine work.

1. `docker/compose-xfstests/run_*_xfstests.sh` mounts BrewFS directly in a
   privileged container and is the current local/CI path.
2. `docker/run_xfstests_{sqlite,redis,etcd}.sh` delegates to
   `docker/kvm-xfstests/` and runs qlean/KVM integration targets. Use it when
   VM isolation or kernel-level reproduction is required.

The KVM implementation consists of:

| File | Role |
| --- | --- |
| `docker/kvm-xfstests/run_xfstests_backend.sh` | Main sqlite/Redis/etcd VM orchestrator. |
| `run_xfstests_sqlite.sh`, `run_xfstests_redis.sh`, `run_xfstests_etcd.sh` | Backend aliases. |
| `install_xfstests_deps.sh` | Host package and xfstests dependency setup. |
| `manage_xfstests_backend_services.sh` | Redis/etcd service lifecycle. |

The top-level `docker/run_xfstests_backend.sh` and backend-specific files are
compatibility delegates to this KVM directory. The top-level
`docker/install_xfstests_deps.sh` and
`docker/manage_xfstests_backend_services.sh` are delegates as well.

`tests/scripts/xfstests_slayer.sh` and `xfstests_slayer_s3.sh` are destructive
legacy host scripts: they install packages, recreate `/tmp/xfstests-dev`,
write `/usr/sbin/mount.fuse.brewfs`, and manipulate `/tmp/data` and
`/tmp/mount`. Use them only on a disposable host. The S3 variant currently
runs only `generic/001`; neither is the recommended regression runner.

## CI Mapping

`.github/workflows/ci.yml` currently maps the scripts as follows:

| CI job | Command |
| --- | --- |
| `rust` | fmt, harness shell checks, unit/bin tests, and clippy |
| `docker-pjdfstest` | `run_redis_pjdfstest.sh` |
| `docker-xfstests-smoke` | Redis `generic/001 generic/002 generic/100` |
| `docker-stress-ng` | Redis stress-ng selected profile |
| `docker-ltp` | Redis LTP default set |
| `docker-tikv-xfstests-smoke` | TiKV xfstests smoke |
| `docker-tikv-pjdfstest` | TiKV pjdfstest corpus |
| `docker-tikv-ltp` | TiKV LTP default set |

Full xfstests and full performance runs remain manual because of duration and
resource use. See [fs-test-suite-matrix.md](fs-test-suite-matrix.md) for the
required/manual policy and current exclusions.

## Artifact Review And Cleanup

For every heavy run, check more than the process exit code:

1. Read `report.md` or the suite summary.
2. Check `runner-warning-summary.tsv` for warnings, timeouts, and slow calls.
3. Inspect `brewfs.log` and backend diagnostics.
4. Confirm post-write dirty/pending data drained when the profile requires it.
5. Compare `perf-profile.env` before comparing performance numbers.

Normal wrappers clean their Compose project automatically. After interrupted
or `--keep` runs, inspect and remove only the matching test resources:

```bash
docker ps -a --format '{{.Names}}'
docker volume ls --format '{{.Name}}' | grep compose-xfstests
docker compose -f docker/compose-xfstests/docker-compose.redis.yml down -v
```

Do not delete `docker/compose-xfstests/artifacts/` until the report and failure
evidence have been preserved. Large disposable build output can be measured
with `du -sh target docker/compose-xfstests/artifacts` before cleanup.
