# Million-file observed campaign: historical sources and offline verification

The [report](../../../doc/testing/entity-packed-million-r2.md) records two
complete operation pairs, a third standalone BrewFS cell and an interrupted
JuiceFS recovery cell. Full observability and uninterrupted six-cell acceptance
failed. All workload operations and correctness/exit checks completed; the
owned paid resources were deleted after archival verification.

## Reproduce the published evidence

Python 3.11+ on Linux or Windows checks exact source hashes and public tables:

```sh
python3 tools/perf/entity-packed-million-r2/verify_evidence.py --sources-only
```

With authorized access to the three original private final archives:

```sh
python3 tools/perf/entity-packed-million-r2/verify_evidence.py --archives /path/to/cloud-results
```

The audit streams every archive member, verifies outer/member hashes and
manifest completeness, validates all raw sample records and true component
exits, compares reported timings/latencies to original results, and checks the
actual gaps and missing catalog entries. It does not extract archives, start
loads, contact cloud services or change resources. Run without Python `-O`.
Exit zero means the evidence matches this **failed-acceptance campaign**;
the output explicitly keeps `strict_acceptance_passed=false`.

Full archives contain generated credentials and deployment/service logs and
are kept privately. Published hashes, sizes and member counts identify them.
Public JSON/CSV removes account, resource, endpoint, PID and local-path
identifiers. Raw original sample bytes and service/host logs remain unchanged
in the archives. The source-hash receipt records exact measured source bytes.
Narrow Git attributes retain the original CRLF provenance JSON and two original
EOF blank lines, keeping their measured hashes stable across checkout platforms.

## What is preserved

- `frozen/`: all 26 final deployed source/configuration/provenance files,
  byte-for-byte, plus their measured `frozen-files.json`. Before bootstrap,
  `cloud_storage.py` was adapted for exact ECS NVMe serial representation and
  refrozen; source digests retain its original-transfer hash too. Final archives
  omit the image-lock/provenance JSON files; those two unchanged metadata files
  come from the original frozen transfer and match the final source manifest.
  The deterministic
  source, trace, read and COW algorithms are in `harness/scale_workloads.py`.
- `object_agent_repaired.py`: the isolated measured collection adapter; its
  same-mount rename/reopen and checked cross-mount copy preserve original logs.
- `resume_unstarted_cells.py`: original guarded continuation using saved
  onboarding. It never calls `prepare` or `driver.main`.
- `finish_existing_last_workspace.py`: historical once-only attachment to
  exact original components, preserving interrupted timing and missing offset.
- `verify_cloud_results.py`: unchanged original strict validator, including
  the final-catalog assumption that failed. The separate offline audit calls
  its unchanged archive scanner and retains the failed acceptance result.
- `test_rotation_repair.py`: the actual Linux `/tmp` to `/dev/shm` EXDEV and
  immutable-copy test. The seven original observation tests are in
  `frozen/harness/test_observation.py`.

These are historical execution sources, including the defects described in the
report. The node bootstrap formats the explicitly configured data disk and
installs system services; it is not a generic safe launcher. Recovery sources
require original durable authorization, state, component identity and hashes.
They must not be used to restart this completed campaign. Account-specific
cloud provisioning, credentials, manager processes and deletion scripts are
excluded from the public package.

## Reconstructing the original protocol on separate infrastructure

The frozen contract specifies one client, one RustFS server and one PD/TiKV
server in a single AZ; each has the documented fixed-IOPS disk. A fresh,
separately authorized campaign requires empty dedicated data disks, private
network connectivity, FUSE, systemd, Docker, fio, nginx, and Python with boto3.
The staged root is `/opt/entity-packed-million-r2` with `harness`, `bin`,
`private`, `artifacts` and a canonical bind-mounted `work` on the data disk.
The exact binaries and backend image IDs are recorded in `frozen/`.

Build the measured BrewFS binary from source commit
`918deb90e046889d94eb8ff633822e275792ce28`:

```sh
cargo build --locked --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,rustfs-ec-block-size-hint,observability \
  --bin brewfs
cargo build --locked --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,rustfs-ec-block-size-hint,observability \
  --bin packed_v3_snapshot_fixture --example entity_metadata_probe
```

Use JuiceFS source `e0032b2ae5e9603403ca955eed7d05426f6f2f8c` and the pinned
backend IDs, not just mutable image tags. Match the contract and effective
cache/sync/FUSE configuration in `frozen/harness/cell.py` and `juicefs_tools.py`.
Record compiler, binary and environment identities; rebuilding can change binary
hashes. The measured hashes identify this run, rather than a reproducible-build
guarantee.

Private inputs supplied at staging are `private/runtime.env` (generated AWS
test credentials), `private/environment.json`, per-node `private/role.json`,
and `harness/settings.json` (S3/PD/agent endpoints, bucket and manifest). Use
fresh resource identifiers and a new campaign namespace. Read the bootstrap
and storage sources before formatting anything. Observers sampled at five
seconds and the local manager polled every ten seconds. The original driver
contains the exact native import/clone commands and arm order.

The historical bootstrap needs the documented RustFS/nginx port compatibility
handling; the original logger crosses filesystems incorrectly at checkpoint.
The measured adapter also has a restart-baseline assumption. A future runner
must address these explicitly, retain append-only counters/catalog history,
isolate service maintenance, and validate rotation/restart behavior before new
measurement. This package preserves the measured failures rather than silently
substituting a corrected campaign. No additional cloud run is needed for the
offline reproduction above.

The original protocol has no workload execution deadline or automatic deletion
on failure. On a failure it preserves components, data and diagnostics; explicit
resource closure follows complete archive/evidence verification. It includes
ordinary drains/unmounts and actual detached-worker exits, not only launcher
exit. Existing legacy runners and runtime defaults are untouched.
