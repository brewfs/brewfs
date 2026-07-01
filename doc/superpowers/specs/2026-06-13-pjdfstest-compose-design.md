# pjdfstest Compose Integration Design

## Goal

Add a containerized pjdfstest runner for BrewFS that mirrors the existing
containerized xfstests workflow. The first supported backend is Redis metadata
with RustFS/S3 object storage, because that is the highest-value path already
used by the xfstests and LTP compose stacks.

## Scope

This change adds a new test entrypoint. It does not change BrewFS filesystem
behavior, existing xfstests semantics, or the LTP runner.

In scope:

- Build pjdfstest from the upstream `pjd/pjdfstest` repository inside a Docker
  image.
- Mount BrewFS inside a privileged container using the same environment and
  config shape as xfstests.
- Run pjdfstest from inside the BrewFS mount point.
- Save BrewFS logs, generated backend config, pjdfstest console output, and
  pjdfstest result logs into the compose artifacts directory.
- Provide a host-side wrapper script that builds the BrewFS binary, builds the
  test image, starts Redis and RustFS, initializes the S3 bucket, and runs the
  pjdfstest service.

Out of scope for this phase:

- SQLite, etcd, and TiKV pjdfstest compose files.
- CI wiring.
- Refactoring the existing xfstests and LTP runner scripts.
- Changing BrewFS runtime code.

## Architecture

The new files live under `docker/compose-pjdfstest/` to keep the suite separate
from `docker/compose-xfstests/`. The compose file follows the same topology as
`docker/compose-xfstests/docker-compose.redis.yml`:

- `redis`: metadata service.
- `rustfs`: S3-compatible object storage service.
- `rustfs-init`: one-shot bucket initializer.
- `pjdfstest`: privileged FUSE test runner.

The test runner image copies the host-built `target/release/brewfs` binary,
installs runtime dependencies, builds pjdfstest in a builder stage, and copies
the built suite into `/opt/pjdfstest`.

The container runner writes `/run/brewfs/config.yaml`, installs a
`mount.fuse.brewfs` helper, mounts BrewFS at `/mnt/brewfs`, and then runs
`prove -r /opt/pjdfstest/tests` from that mount point. The runner accepts
environment overrides for selecting tests and passing additional prove
arguments.

## User Interface

Host entrypoint:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

Optional filters:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chmod chown"
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --prove-args "-v"
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --keep
```

Artifacts:

```text
docker/compose-pjdfstest/artifacts/run-*/
  backend.yml
  brewfs.log
  brewfs_fuse_ops.log
  pjdfstest.console.log
  results/pjdfstest.log
```

## Error Handling

- The host wrapper rejects unknown flags and missing option values.
- The container runner exits early if required tools or directories are absent.
- Redis metadata requires `BREWFS_META_URL`.
- The mount helper waits for an existing stale FUSE mount to disappear before
  starting a new BrewFS mount.
- Exit status is the pjdfstest/prove exit status.
- Cleanup always attempts to copy artifacts and unmount BrewFS.

## Testing

Fast local checks:

- `bash -n` for all new shell scripts.
- `docker compose -f docker/compose-pjdfstest/docker-compose.redis.yml config`
  to validate compose syntax and environment expansion.
- A targeted Docker build for the `pjdfstest` service when Docker is available.

Full validation:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "chmod"
```

This full validation requires Docker with privileged FUSE support.
