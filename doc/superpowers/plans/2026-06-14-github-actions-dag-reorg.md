# GitHub Actions DAG Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize all GitHub Actions into a staged DAG that gives fast Rust feedback, gates Docker/FUSE smoke tests behind Rust health, moves long filesystem/perf suites into manual/nightly jobs, and publishes images only after main CI succeeds.

**Architecture:** Keep quick PR feedback in `.github/workflows/ci.yml`, move expensive xfstests/LTP/perf suites into `.github/workflows/fs-heavy.yml`, and make `.github/workflows/docker-images.yml` publish after successful main CI. Add small local composite actions under `.github/actions/` to remove repeated setup logic without changing repository build behavior.

**Tech Stack:** GitHub Actions YAML, local composite actions, Rust stable toolchain, `Swatinem/rust-cache`, Docker Compose, FUSE, existing BrewFS scripts under `docker/compose-*` and `docker/kvm-xfstests/`.

---

## Target DAG

```text
ci.yml

workflow-lint
  |
  +------------------+
                     |
rust-fmt             |
rust-check ----------+--> rust-gate --> docker-pjdfstest-smoke ----+
rust-feature-matrix -+                 docker-stress-ng-smoke -----+--> ci-summary
rust-test -----------+
rust-clippy ---------+

fs-heavy.yml

heavy-preflight --> xfstests backend jobs
                --> ltp backend jobs
                --> perf backend jobs

docker-images.yml

CI on main success --> publish runtime image
                   --> publish operator image
workflow_dispatch --> publish runtime image
                  --> publish operator image
```

## File Structure

- Modify: `.github/workflows/ci.yml`
  - Owns PR/push fast checks, Rust matrix checks, Docker smoke jobs, and a final required `ci-summary` status.
- Create: `.github/workflows/fs-heavy.yml`
  - Owns manual and scheduled heavy suites: xfstests, LTP, BrewFS perf, JuiceFS comparison perf.
- Modify: `.github/workflows/docker-images.yml`
  - Publishes runtime/operator images after successful CI on `main`, with manual dispatch fallback.
- Create: `.github/actions/setup-rust/action.yml`
  - Installs Rust, protobuf compiler, and Cargo cache for Rust jobs.
- Create: `.github/actions/setup-fuse-docker/action.yml`
  - Installs FUSE/Docker prerequisites and verifies `/dev/fuse`.
- No Rust production code should change in this plan.

---

### Task 1: Add Shared Rust Setup Composite Action

**Files:**
- Create: `.github/actions/setup-rust/action.yml`

- [ ] **Step 1: Create the local composite action**

Create `.github/actions/setup-rust/action.yml` with this content:

```yaml
name: Setup Rust
description: Install Rust, protobuf compiler, and Cargo cache for BrewFS CI jobs.

inputs:
  components:
    description: Comma-separated Rust components to install.
    required: false
    default: ""

runs:
  using: composite
  steps:
    - name: Install system dependencies
      shell: bash
      run: |
        sudo apt-get update
        sudo apt-get install -y protobuf-compiler

    - name: Install Rust
      uses: dtolnay/rust-toolchain@stable
      with:
        components: ${{ inputs.components }}

    - name: Cache Cargo
      uses: Swatinem/rust-cache@v2
```

- [ ] **Step 2: Validate composite action YAML**

Run:

```bash
ruby -e 'require "yaml"; YAML.load_file(ARGV[0])' .github/actions/setup-rust/action.yml
```

Expected: exit code `0`.

- [ ] **Step 3: Commit**

```bash
git add .github/actions/setup-rust/action.yml
git commit -m "ci: add shared rust setup action"
```

---

### Task 2: Add Shared FUSE/Docker Setup Composite Action

**Files:**
- Create: `.github/actions/setup-fuse-docker/action.yml`

- [ ] **Step 1: Create the local composite action**

Create `.github/actions/setup-fuse-docker/action.yml` with this content:

```yaml
name: Setup FUSE Docker
description: Install FUSE dependencies and verify Docker Compose for BrewFS filesystem tests.

runs:
  using: composite
  steps:
    - name: Install FUSE dependencies
      shell: bash
      run: |
        sudo apt-get update
        sudo apt-get install -y protobuf-compiler fuse3

    - name: Enable FUSE
      shell: bash
      run: |
        sudo modprobe fuse
        test -e /dev/fuse
        sudo chmod 666 /dev/fuse

    - name: Show Docker versions
      shell: bash
      run: |
        docker version
        docker compose version
```

- [ ] **Step 2: Validate composite action YAML**

Run:

```bash
ruby -e 'require "yaml"; YAML.load_file(ARGV[0])' .github/actions/setup-fuse-docker/action.yml
```

Expected: exit code `0`.

- [ ] **Step 3: Commit**

```bash
git add .github/actions/setup-fuse-docker/action.yml
git commit -m "ci: add shared fuse docker setup action"
```

---

### Task 3: Rewrite Quick CI as a DAG

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Replace `.github/workflows/ci.yml` with staged jobs**

Use this workflow content:

```yaml
name: CI

on:
  push:
    branches: [main, "codex/**"]
  pull_request:
    branches: [main]
  workflow_dispatch:
    inputs:
      docker_suite:
        description: Docker/FUSE smoke suite to run
        type: choice
        default: smoke
        options:
          - none
          - smoke
          - pjdfstest
          - stress-ng
          - all
      stress_ng_profile:
        description: stress-ng profile for the Docker stress job
        type: choice
        default: smoke
        options:
          - smoke
          - metadata-heavy
          - link-symlink-heavy
          - write-smallfile

concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

env:
  CARGO_TERM_COLOR: always
  CARGO_INCREMENTAL: "0"
  CARGO_PROFILE_DEV_DEBUG: "0"

jobs:
  workflow-lint:
    name: Workflow lint
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Run actionlint
        run: |
          docker run --rm --user 0:0 -v "$PWD":/repo -w /repo rhysd/actionlint:latest -no-color \
            .github/workflows/ci.yml \
            .github/workflows/docker-images.yml \
            .github/workflows/fs-heavy.yml

  rust-fmt:
    name: Rust fmt
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Rust
        uses: ./.github/actions/setup-rust
        with:
          components: rustfmt

      - name: Check formatting
        run: cargo fmt --all --check

  rust-check:
    name: Rust check
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Rust
        uses: ./.github/actions/setup-rust

      - name: Check workspace
        run: cargo check --workspace --all-targets

  rust-feature-matrix:
    name: Rust feature check (${{ matrix.package }} / ${{ matrix.features }})
    runs-on: ubuntu-latest
    needs: rust-check
    strategy:
      fail-fast: false
      matrix:
        include:
          - package: brewfs
            features: fuse-tokio-runtime
          - package: brewfs
            features: fuse-io-uring-runtime
          - package: rfuse3
            features: tokio-runtime
          - package: rfuse3
            features: io-uring-runtime
          - package: rfuse3
            features: async-io-runtime

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Rust
        uses: ./.github/actions/setup-rust

      - name: Check package features
        shell: bash
        run: |
          package="${{ matrix.package }}"
          features="${{ matrix.features }}"
          metadata="$(cargo metadata --no-deps --format-version 1)"
          if ! python3 -c 'import json, sys; package = sys.argv[1]; data = json.loads(sys.stdin.read()); members = set(data["workspace_members"]); found = any(pkg["name"] == package and pkg["id"] in members for pkg in data["packages"]); sys.exit(0 if found else 1)' "$package" <<< "$metadata"
          then
            echo "$package is not a workspace member in this checkout; skipping $features."
            exit 0
          fi
          cargo check -p "$package" --no-default-features --features "$features"

  rust-test:
    name: Rust tests
    runs-on: ubuntu-latest
    needs: rust-check

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Rust
        uses: ./.github/actions/setup-rust

      - name: Test workspace
        run: cargo test --workspace --lib --bins

  rust-clippy:
    name: Rust clippy
    runs-on: ubuntu-latest
    needs: rust-check

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Rust
        uses: ./.github/actions/setup-rust
        with:
          components: clippy

      - name: Clippy
        run: cargo clippy --workspace

  rust-gate:
    name: Rust gate
    runs-on: ubuntu-latest
    needs:
      - workflow-lint
      - rust-fmt
      - rust-check
      - rust-feature-matrix
      - rust-test
      - rust-clippy
    if: always()

    steps:
      - name: Require Rust and workflow jobs
        shell: bash
        run: |
          for result in \
            "${{ needs.workflow-lint.result }}" \
            "${{ needs.rust-fmt.result }}" \
            "${{ needs.rust-check.result }}" \
            "${{ needs.rust-feature-matrix.result }}" \
            "${{ needs.rust-test.result }}" \
            "${{ needs.rust-clippy.result }}"
          do
            if [[ "$result" != "success" ]]; then
              echo "Required CI job finished with result: $result"
              exit 1
            fi
          done

  docker-pjdfstest-smoke:
    name: Docker pjdfstest smoke
    runs-on: ubuntu-latest
    timeout-minutes: 30
    needs: rust-gate
    if: >-
      ${{
        github.event_name != 'workflow_dispatch' ||
        inputs.docker_suite == 'smoke' ||
        inputs.docker_suite == 'pjdfstest' ||
        inputs.docker_suite == 'all'
      }}

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          lfs: true

      - name: Setup Rust
        uses: ./.github/actions/setup-rust

      - name: Setup FUSE and Docker
        uses: ./.github/actions/setup-fuse-docker

      - name: Run pjdfstest
        run: bash docker/compose-pjdfstest/run_redis_pjdfstest.sh

      - name: Upload pjdfstest artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: pjdfstest-artifacts
          path: docker/compose-pjdfstest/artifacts/**
          if-no-files-found: warn

  docker-stress-ng-smoke:
    name: Docker stress-ng smoke
    runs-on: ubuntu-latest
    timeout-minutes: 30
    needs: rust-gate
    if: >-
      ${{
        github.event_name != 'workflow_dispatch' ||
        inputs.docker_suite == 'smoke' ||
        inputs.docker_suite == 'stress-ng' ||
        inputs.docker_suite == 'all'
      }}

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          lfs: true

      - name: Setup Rust
        uses: ./.github/actions/setup-rust

      - name: Setup FUSE and Docker
        uses: ./.github/actions/setup-fuse-docker

      - name: Select stress-ng profile
        shell: bash
        run: |
          profile="${{ inputs.stress_ng_profile }}"
          if [[ -z "$profile" ]]; then
            profile="smoke"
          fi
          echo "STRESS_NG_PROFILE=$profile" >> "$GITHUB_ENV"

      - name: Run stress-ng profile
        run: bash docker/compose-xfstests/run_redis_stress_ng.sh --profile "$STRESS_NG_PROFILE"

      - name: Upload stress-ng artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: stress-ng-artifacts-${{ env.STRESS_NG_PROFILE }}
          path: docker/compose-xfstests/artifacts/**
          if-no-files-found: warn

  ci-summary:
    name: CI summary
    runs-on: ubuntu-latest
    needs:
      - rust-gate
      - docker-pjdfstest-smoke
      - docker-stress-ng-smoke
    if: always()

    steps:
      - name: Require DAG result
        shell: bash
        run: |
          if [[ "${{ needs.rust-gate.result }}" != "success" ]]; then
            echo "rust-gate result: ${{ needs.rust-gate.result }}"
            exit 1
          fi
          for result in \
            "${{ needs.docker-pjdfstest-smoke.result }}" \
            "${{ needs.docker-stress-ng-smoke.result }}"
          do
            if [[ "$result" != "success" && "$result" != "skipped" ]]; then
              echo "Docker smoke job finished with result: $result"
              exit 1
            fi
          done
```

- [ ] **Step 2: Validate workflow syntax**

Run:

```bash
docker run --rm --user 0:0 -v "$PWD":/repo -w /repo rhysd/actionlint:latest -no-color .github/workflows/ci.yml
```

Expected: exit code `0`.

- [ ] **Step 3: Run local Rust checks that mirror the DAG**

Run:

```bash
cargo fmt --all --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace --all-targets
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace
```

Expected: all commands exit `0`. Existing clippy warnings may remain unless the implementation intentionally makes clippy strict.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: split quick checks into dag"
```

---

### Task 4: Add Heavy Filesystem CI Workflow

**Files:**
- Create: `.github/workflows/fs-heavy.yml`

- [ ] **Step 1: Create the heavy workflow**

Create `.github/workflows/fs-heavy.yml` with this content:

```yaml
name: Filesystem Heavy CI

on:
  workflow_dispatch:
    inputs:
      suite:
        description: Heavy suite to run
        type: choice
        default: xfstests-redis
        options:
          - xfstests-redis
          - xfstests-sqlite
          - xfstests-etcd
          - ltp-redis
          - ltp-sqlite
          - ltp-etcd
          - perf-redis
          - perf-etcd
          - perf-tikv
          - juicefs-perf
          - all
      perf_workloads:
        description: Comma-separated perf workloads passed through to PERF_FIO_WORKLOADS
        required: false
        default: "seqread,seqwrite,randread,randwrite,randrw,dirstress,dirperf,metaperf"
  schedule:
    - cron: "20 3 * * *"

concurrency:
  group: fs-heavy-${{ github.workflow }}-${{ github.ref }}-${{ inputs.suite || 'nightly' }}
  cancel-in-progress: false

env:
  CARGO_TERM_COLOR: always
  CARGO_INCREMENTAL: "0"
  CARGO_PROFILE_DEV_DEBUG: "0"

jobs:
  heavy-preflight:
    name: Heavy preflight
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          lfs: true

      - name: Setup Rust
        uses: ./.github/actions/setup-rust

      - name: Check workspace
        run: cargo check --workspace --all-targets

  xfstests:
    name: xfstests (${{ matrix.backend }})
    runs-on: ubuntu-latest
    timeout-minutes: 240
    needs: heavy-preflight
    strategy:
      fail-fast: false
      matrix:
        include:
          - backend: redis
            selector: xfstests-redis
            script: docker/compose-xfstests/run_redis_xfstests.sh
          - backend: sqlite
            selector: xfstests-sqlite
            script: docker/compose-xfstests/run_sqlite_xfstests.sh
          - backend: etcd
            selector: xfstests-etcd
            script: docker/compose-xfstests/run_etcd_xfstests.sh
    if: >-
      ${{
        github.event_name == 'schedule' ||
        inputs.suite == 'all' ||
        startsWith(inputs.suite, 'xfstests-')
      }}

    steps:
      - name: Checkout
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: actions/checkout@v4
        with:
          lfs: true

      - name: Setup Rust
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: ./.github/actions/setup-rust

      - name: Setup FUSE and Docker
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: ./.github/actions/setup-fuse-docker

      - name: Run xfstests
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        run: bash "${{ matrix.script }}"

      - name: Upload xfstests artifacts
        if: ${{ always() && (github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector) }}
        uses: actions/upload-artifact@v4
        with:
          name: xfstests-${{ matrix.backend }}-artifacts
          path: docker/compose-xfstests/artifacts/**
          if-no-files-found: warn

  ltp:
    name: LTP (${{ matrix.backend }})
    runs-on: ubuntu-latest
    timeout-minutes: 180
    needs: heavy-preflight
    strategy:
      fail-fast: false
      matrix:
        include:
          - backend: redis
            selector: ltp-redis
            script: docker/compose-xfstests/run_redis_ltp.sh
          - backend: sqlite
            selector: ltp-sqlite
            script: docker/compose-xfstests/run_sqlite_ltp.sh
          - backend: etcd
            selector: ltp-etcd
            script: docker/compose-xfstests/run_etcd_ltp.sh
    if: >-
      ${{
        github.event_name == 'schedule' ||
        inputs.suite == 'all' ||
        startsWith(inputs.suite, 'ltp-')
      }}

    steps:
      - name: Checkout
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: actions/checkout@v4
        with:
          lfs: true

      - name: Setup Rust
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: ./.github/actions/setup-rust

      - name: Setup FUSE and Docker
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: ./.github/actions/setup-fuse-docker

      - name: Run LTP
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        run: bash "${{ matrix.script }}"

      - name: Upload LTP artifacts
        if: ${{ always() && (github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector) }}
        uses: actions/upload-artifact@v4
        with:
          name: ltp-${{ matrix.backend }}-artifacts
          path: docker/compose-xfstests/artifacts/**
          if-no-files-found: warn

  perf:
    name: Perf (${{ matrix.backend }})
    runs-on: ubuntu-latest
    timeout-minutes: 180
    needs: heavy-preflight
    strategy:
      fail-fast: false
      matrix:
        include:
          - backend: redis
            selector: perf-redis
            script: docker/compose-xfstests/run_redis_perf.sh
          - backend: etcd
            selector: perf-etcd
            script: docker/compose-xfstests/run_etcd_perf.sh
          - backend: tikv
            selector: perf-tikv
            script: docker/compose-xfstests/run_tikv_perf.sh
          - backend: juicefs
            selector: juicefs-perf
            script: docker/compose-xfstests/run_juicefs_perf.sh
    if: >-
      ${{
        github.event_name == 'schedule' ||
        inputs.suite == 'all' ||
        startsWith(inputs.suite, 'perf-') ||
        inputs.suite == 'juicefs-perf'
      }}

    steps:
      - name: Checkout
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: actions/checkout@v4
        with:
          lfs: true

      - name: Setup Rust
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: ./.github/actions/setup-rust

      - name: Setup FUSE and Docker
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        uses: ./.github/actions/setup-fuse-docker

      - name: Run perf
        if: ${{ github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector }}
        env:
          PERF_FIO_WORKLOADS: ${{ inputs.perf_workloads || 'seqread,seqwrite,randread,randwrite,randrw,dirstress,dirperf,metaperf' }}
          PERF_FIO_DIRECT: "1"
        run: bash "${{ matrix.script }}"

      - name: Upload perf artifacts
        if: ${{ always() && (github.event_name == 'schedule' || inputs.suite == 'all' || inputs.suite == matrix.selector) }}
        uses: actions/upload-artifact@v4
        with:
          name: perf-${{ matrix.backend }}-artifacts
          path: docker/compose-xfstests/artifacts/**
          if-no-files-found: warn
```

- [ ] **Step 2: Validate workflow syntax**

Run:

```bash
docker run --rm --user 0:0 -v "$PWD":/repo -w /repo rhysd/actionlint:latest -no-color .github/workflows/fs-heavy.yml
```

Expected: exit code `0`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/fs-heavy.yml
git commit -m "ci: add heavy filesystem workflow"
```

---

### Task 5: Reorganize Docker Image Publishing

**Files:**
- Modify: `.github/workflows/docker-images.yml`

- [ ] **Step 1: Replace image workflow with CI-gated publish workflow**

Use this workflow content:

```yaml
name: Docker Images

on:
  workflow_run:
    workflows: ["CI"]
    branches: [main]
    types: [completed]
  workflow_dispatch:

permissions:
  contents: read
  packages: write

concurrency:
  group: docker-images-${{ github.ref }}
  cancel-in-progress: false

env:
  CARGO_TERM_COLOR: always

jobs:
  publish:
    name: Build and publish ${{ matrix.title }}
    runs-on: ubuntu-latest
    if: >-
      ${{
        github.event_name == 'workflow_dispatch' ||
        github.event.workflow_run.conclusion == 'success'
      }}
    strategy:
      fail-fast: false
      matrix:
        include:
          - title: BrewFS runtime image
            image: brewfs
            context: .
            file: docker/Dockerfile.runtime
            cache_scope: brewfs-runtime
          - title: BrewFS operator image
            image: brewfs-operator
            context: operator/brewfs-operator
            file: operator/brewfs-operator/Dockerfile
            cache_scope: brewfs-operator

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          ref: ${{ github.event.workflow_run.head_sha || github.sha }}

      - name: Set image names
        shell: bash
        run: |
          owner="${GITHUB_REPOSITORY_OWNER,,}"
          source_sha="${{ github.event.workflow_run.head_sha || github.sha }}"
          short_sha="${source_sha::12}"
          echo "IMAGE_PREFIX=ghcr.io/${owner}" >> "$GITHUB_ENV"
          echo "SHORT_SHA=${short_sha}" >> "$GITHUB_ENV"
          echo "SOURCE_SHA=${source_sha}" >> "$GITHUB_ENV"

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GHCR_TOKEN || secrets.GITHUB_TOKEN }}

      - name: Build and push image
        uses: docker/build-push-action@v6
        with:
          context: ${{ matrix.context }}
          file: ${{ matrix.file }}
          push: true
          tags: |
            ${{ env.IMAGE_PREFIX }}/${{ matrix.image }}:latest
            ${{ env.IMAGE_PREFIX }}/${{ matrix.image }}:${{ env.SHORT_SHA }}
          labels: |
            org.opencontainers.image.source=https://github.com/${{ github.repository }}
            org.opencontainers.image.revision=${{ env.SOURCE_SHA }}
            org.opencontainers.image.title=${{ matrix.image }}
          cache-from: type=gha,scope=${{ matrix.cache_scope }}
          cache-to: type=gha,scope=${{ matrix.cache_scope }},mode=max
```

- [ ] **Step 2: Validate workflow syntax**

Run:

```bash
docker run --rm --user 0:0 -v "$PWD":/repo -w /repo rhysd/actionlint:latest -no-color .github/workflows/docker-images.yml
```

Expected: exit code `0`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/docker-images.yml
git commit -m "ci: gate image publishing on main ci"
```

---

### Task 6: Validate the Whole Actions Set

**Files:**
- Test only: `.github/workflows/*.yml`, `.github/actions/*/action.yml`

- [ ] **Step 1: Run actionlint over all workflows**

Run:

```bash
docker run --rm --user 0:0 -v "$PWD":/repo -w /repo rhysd/actionlint:latest -no-color \
  .github/workflows/ci.yml \
  .github/workflows/docker-images.yml \
  .github/workflows/fs-heavy.yml
```

Expected: exit code `0`.

- [ ] **Step 2: Parse local composite action manifests**

Run:

```bash
ruby -e 'require "yaml"; YAML.load_file(ARGV[0])' .github/actions/setup-rust/action.yml
ruby -e 'require "yaml"; YAML.load_file(ARGV[0])' .github/actions/setup-fuse-docker/action.yml
```

Expected: both commands exit `0`.

- [ ] **Step 3: Run local Rust commands represented by quick CI**

Run:

```bash
cargo fmt --all --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace --all-targets
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace
```

Expected: all commands exit `0`. If `rfuse3` is not a workspace member, do not run standalone `rfuse3` feature commands locally.

- [ ] **Step 4: Run one Docker smoke locally if FUSE is available**

Run:

```bash
test -e /dev/fuse
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
```

Expected: both commands exit `0`. If `/dev/fuse` is unavailable on the local machine, record that Docker smoke was deferred to GitHub Actions.

- [ ] **Step 5: Inspect final DAG**

Run:

```bash
git diff --stat HEAD~3..HEAD -- .github
git status --short
```

Expected: only intended GitHub Actions files are changed or committed. The worktree may still show unrelated untracked files, but none should be staged.

---

### Task 7: Push and Observe First CI Run

**Files:**
- No file changes.

- [ ] **Step 1: Push the branch**

Run:

```bash
git push origin codex/perf-tune-integration
```

Expected: push exits `0`.

- [ ] **Step 2: Watch CI jobs**

Run:

```bash
gh run list --branch codex/perf-tune-integration --workflow CI --limit 3
```

Expected: a new `CI` run appears for the pushed commit.

- [ ] **Step 3: Inspect failures by stage**

If a job fails, run:

```bash
gh run view --log-failed
```

Expected: failure is isolated to one DAG stage, such as `workflow-lint`, `rust-feature-matrix`, `docker-pjdfstest-smoke`, or `docker-stress-ng-smoke`.

- [ ] **Step 4: Commit any CI-only follow-up fix**

If the first remote run exposes a GitHub-only syntax or environment issue, fix the narrow workflow problem, then run:

```bash
docker run --rm --user 0:0 -v "$PWD":/repo -w /repo rhysd/actionlint:latest -no-color \
  .github/workflows/ci.yml \
  .github/workflows/docker-images.yml \
  .github/workflows/fs-heavy.yml \
  .github/actions/setup-rust/action.yml \
  .github/actions/setup-fuse-docker/action.yml
git add .github
git commit -m "ci: fix dag workflow execution"
git push origin codex/perf-tune-integration
```

Expected: actionlint exits `0`, commit succeeds, push succeeds.

---

## Self-Review

- Spec coverage: This plan reorganizes both existing workflows and adds a heavy workflow for the filesystem/perf suites already present under `docker/compose-xfstests`, `docker/compose-pjdfstest`, and `tools/perf`.
- Placeholder scan: No task uses TBD/TODO/fill-in placeholders. Every changed workflow/action has explicit YAML content.
- Type consistency: Job names used in `needs` match job identifiers in the same workflow. The image publish workflow references `CI`, matching `.github/workflows/ci.yml` `name: CI`.
- Risk note: `fs-heavy.yml` is intentionally manual/nightly because xfstests/LTP/perf can be slow and noisy on hosted runners. PR branch protection should require `ci-summary`, not each matrix child.
