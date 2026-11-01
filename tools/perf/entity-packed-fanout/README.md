# Frozen entity packed fanout protocol

Entry point: `focus.py`, launched by `cloud-run.sh`. This directory archives
the original experiment, whose 100k cohort completed and whose 1M JuiceFS
baseline import timed out. See the [report](../../../doc/testing/entity-packed-fanout.md).
Do not treat the recorded campaign as a complete 14-cell acceptance result.

`campaign.py`, `scale_driver.py` and their helpers were imported from earlier
diagnostics. Their other functions and standalone entry points are not part
of this experiment. Only `focus.py` defines its 14-cell schedule. The frozen
contract and analysis plan retain the original 1M pilot/accounting rules;
none of those 1M workspace cells ran.

## Stage an independently owned Linux test host

The runner uses the fixed root `/opt/entity-packed-tikv-fanout-v1`. It expects
root access, FUSE3/fusectl, Python 3 with boto3 in `venv/`, Docker Compose,
nginx, curl and fio, and no active swap. Provision the host separately; this
harness does not provision Aliyun resources. The recorded host had 8 vCPU,
32 GiB and a 200 GiB disk, with all backends on loopback.

Build from `9832ddd1ae11a3861e38c14fa659e8160e151743` using the compiler/flags
in `results.json`. Build the measured filesystem separately from development
examples so Cargo feature unification does not change its dependency graph:

```bash
export CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_BUILD_JOBS=2
features=fuse-tokio-runtime,workspace-overlay,rustfs-ec-block-size-hint
cargo build --locked --release --no-default-features --features "$features" --bin brewfs
# Save this filesystem binary before building examples.
cargo build --locked --release --no-default-features --features "$features" --bin packed_v3_snapshot_fixture
cargo build --locked --release --no-default-features --features "$features" --example entity_metadata_probe
```

Prepare this layout on the host, copying files from the measured revision:
Stage text from a Linux checkout or `git archive` to preserve LF line endings;
the source hashes refer to LF Git-blob bytes.

```text
/opt/entity-packed-tikv-fanout-v1/
  bin/brewfs-baseline
  bin/packed_v3_snapshot_fixture
  bin/entity_metadata_probe
  bin/juicefs                         # measured JuiceFS 1.3.1 build
  harness/                           # files from this directory
  repo/docker/compose-xfstests/docker-compose.packed-lifecycle-backends.yml
  repo/tools/testing/wait_entity_tikv.py
  venv/
  artifacts/
```

`cloud-runtime-files.sha256` identifies the original measured binaries. Keep
independent build hashes with the new run rather than overwriting the recorded
experiment's provenance. Supply the image filesystems/configurations recorded
in `backend-image-lock.json`; `python harness/verify-backend-images.py` checks
their fingerprints across Docker image-ID formats. In particular, the RustFS
image is an imported test image, not a claim that its alias is publicly pullable.

Run the relevant local repository correctness/CI checks first. After validating
sources, binaries, service images and host prerequisites, create the local
gate markers `local-validation-passed.txt` and `runtime-ready` in the run root.
The root must not contain an existing `work/` directory. Then run:

```bash
bash /opt/entity-packed-tikv-fanout-v1/harness/cloud-run.sh
```

The runner creates a private compose project, random local object credentials
and bucket, then owns their cleanup. It generates `harness/settings.json` at
runtime; never publish that file or raw logs as part of a public result.

## Collect each pair while the run is active

Use a separate controller, with pinned host keys and noninteractive SSH
(`ssh -n` when attached to a hidden Windows console). Poll the run's
`checkpoint-ready.txt`. For each new `focus-NNNN` token:

1. Obtain the remote SHA-256 of `checkpoint-focus-NNNN.tar.gz`.
2. Download that archive and verify its local SHA-256.
3. Only after verification, create `checkpoint-focus-NNNN-collected` in the
   run root. The cloud harness then removes its temporary checkpoint archive.

The harness waits at most 600 seconds for acknowledgement after each pair;
do not run it without a collector. Historical acknowledged checkpoints need
not be fetched again. A failed transfer must not restart a test cell. Retain
all successful and unsuccessful runs and their original configuration.

On exit, preserve `harness/`, `artifacts/`, `focus.log` and the final
`performance-result.txt`, record the archive digest, and verify service cleanup
before releasing the owned host. Missing or timed-out cells remain missing;
do not treat them as favorable measurements.

## Clocks and interpretation

Each product's preparation is measured once per size. Every timed workspace
lifecycle includes bootstrap, native clone, prewarm, direct reads, COW/fsync,
remote drain, ordinary unmount and process exit. Correctness scans and
checkpoint waits occur outside these clocks. Keep common source generation
separate and also show the total including it.

New workspaces retain their metadata after unmount. Operational rounds share
one base, so summing all executed batches describes a retained-workspace
deployment. It does not describe independent cold deployment repeats. The
original 1M pilot would belong in that size's all-batch total if it completed.

Half the selected paths are pre-read, not a claimed 50% cache hit rate. Both
payload caches are disabled, but metadata cache policies differ. Do not turn
the workflow result into a pure TiKV-versus-S3 metadata or random-read claim.
