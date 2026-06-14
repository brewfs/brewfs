# pjdfstest Compose Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Redis-backed Docker Compose runner for pjdfstest against a BrewFS FUSE mount.

**Architecture:** Create a new `docker/compose-pjdfstest/` suite parallel to the existing xfstests suite. The host wrapper builds BrewFS, starts Redis/RustFS, initializes the bucket, and runs a privileged pjdfstest container. The container runner writes BrewFS config, mounts BrewFS, runs pjdfstest via `prove`, and copies artifacts.

**Tech Stack:** Bash, Docker Compose, Debian trixie-slim, FUSE3, Redis, RustFS/S3, upstream `pjd/pjdfstest`, Perl `prove`.

---

### Task 1: Document the Design and Plan

**Files:**
- Create: `docs/superpowers/specs/2026-06-13-pjdfstest-compose-design.md`
- Create: `docs/superpowers/plans/2026-06-13-pjdfstest-compose.md`

- [ ] Add the design document with scope, architecture, CLI, artifacts, and verification commands.
- [ ] Add this implementation plan with exact files and validation steps.
- [ ] Run `grep -R -E "T[B]D|TO[D]O|fill[[:space:]]+in" docs/superpowers/specs/2026-06-13-pjdfstest-compose-design.md docs/superpowers/plans/2026-06-13-pjdfstest-compose.md` and confirm it prints nothing.

### Task 2: Add the pjdfstest Container Runner

**Files:**
- Create: `docker/compose-pjdfstest/run_pjdfstest_in_container.sh`

- [ ] Add logging helpers, environment normalization, and artifact directory setup.
- [ ] Add BrewFS config generation for `local-fs` and `s3` data backends.
- [ ] Add metadata config generation for `redis`, `sqlite`, `etcd`, and `tikv` to match existing xfstests support.
- [ ] Add the FUSE mount helper installer and mount/cleanup logic.
- [ ] Add pjdfstest selection support:
  - `PJDFSTEST_TESTS="chmod chown"` runs `/opt/pjdfstest/tests/chmod` and `/opt/pjdfstest/tests/chown` when those paths exist.
  - Empty `PJDFSTEST_TESTS` runs the full `/opt/pjdfstest/tests` tree.
  - `PJDFSTEST_PROVE_ARGS="-v"` is split into additional `prove` arguments.
- [ ] Run `bash -n docker/compose-pjdfstest/run_pjdfstest_in_container.sh`.

### Task 3: Add the pjdfstest Docker Image

**Files:**
- Create: `docker/compose-pjdfstest/Dockerfile`

- [ ] Add a builder stage that installs `autoconf`, `automake`, `build-essential`, `ca-certificates`, `git`, and `perl`.
- [ ] Clone `https://github.com/pjd/pjdfstest.git` with a shallow checkout.
- [ ] Build pjdfstest with `autoreconf -ifs`, `./configure`, and `make`.
- [ ] Add a runtime stage with BrewFS runtime dependencies, Perl/prove, and FUSE3.
- [ ] Copy `/build` into `/opt/pjdfstest` and copy `target/release/brewfs` into `/usr/local/bin/brewfs`.
- [ ] Copy and execute `run_pjdfstest_in_container.sh` as the image entrypoint.

### Task 4: Add Redis Compose Wiring

**Files:**
- Create: `docker/compose-pjdfstest/docker-compose.redis.yml`

- [ ] Add `redis`, `rustfs`, and `rustfs-init` services using the same health and bucket initialization patterns as xfstests.
- [ ] Add the `pjdfstest` service with privileged FUSE permissions.
- [ ] Mount `./artifacts:/artifacts` and a named BrewFS state volume.
- [ ] Expose `PJDFSTEST_TESTS` and `PJDFSTEST_PROVE_ARGS` through environment variables.
- [ ] Run `docker compose -f docker/compose-pjdfstest/docker-compose.redis.yml config`.

### Task 5: Add the Host Wrapper

**Files:**
- Create: `docker/compose-pjdfstest/run_redis_pjdfstest.sh`
- Modify: `docker/build_brewfs_host_binary.sh`

- [ ] Parse `--tests`, `--prove-args`, `--keep`, and `--help`.
- [ ] Build the host BrewFS binary with `docker/build_brewfs_host_binary.sh`.
- [ ] Update `docker/build_brewfs_host_binary.sh` to resolve the repository root with `git rev-parse --show-toplevel`, so the helper still works after the repository flattening.
- [ ] Build the `pjdfstest` image through Docker Compose.
- [ ] Start Redis and RustFS, run `rustfs-init`, then run the `pjdfstest` service.
- [ ] Preserve the pjdfstest exit code.
- [ ] Run `bash -n docker/compose-pjdfstest/run_redis_pjdfstest.sh`.

### Task 6: Update Docker Documentation

**Files:**
- Modify: `docker/README.md`

- [ ] Add a pjdfstest section after the LTP section.
- [ ] Document the default run command, filtered test command, verbose command, and artifacts directory.
- [ ] Keep the existing xfstests, LTP, and perf sections unchanged except for nearby table-of-contents flow.

### Task 7: Verify and Review

**Files:**
- All files changed in Tasks 1-6.
- Modify: `.gitignore`

- [ ] Run `bash -n docker/compose-pjdfstest/run_pjdfstest_in_container.sh docker/compose-pjdfstest/run_redis_pjdfstest.sh`.
- [ ] Run `docker compose -f docker/compose-pjdfstest/docker-compose.redis.yml config >/tmp/brewfs-pjdfstest-compose.yml`.
- [ ] Add `docker/compose-pjdfstest/artifacts/` to `.gitignore` so local test output is not staged.
- [ ] If Docker build dependencies and network are available, run `docker compose -f docker/compose-pjdfstest/docker-compose.redis.yml build pjdfstest`.
- [ ] Run one or more small pjdfstest cases through `bash docker/compose-pjdfstest/run_redis_pjdfstest.sh --tests "<case>" --prove-args "-v"` and record whether failures are test-infrastructure failures or BrewFS POSIX semantic failures.
- [ ] Review `git diff --check`.
- [ ] Review `git diff -- docker/compose-pjdfstest docker/README.md docs/superpowers`.
- [ ] Commit with `test: add pjdfstest compose runner`.
- [ ] Push `main` to `origin`.
