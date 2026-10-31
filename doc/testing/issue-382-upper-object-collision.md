# Issue 382: shared-bucket upper object collision

Status: reproduced on `80ddaf7`; phase 2 implements one upper bucket per workspace. Real dual-write and legacy cold-remount checks pass; final local CI is recorded below. The earlier namespace-prefix candidate remains rejected. The first-stage sections below are preserved as historical evidence.

## Reproduction

The baseline release binary was rebuilt after a host reboot with:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' \
  CARGO_BUILD_JOBS=2 cargo build --release -j 2 -p brewfs \
  --bin brewfs --bin packed_v3_snapshot_fixture --features workspace-overlay
```

TiKV 8.5.0 and RustFS 1.0.4 were accessed over loopback. The packed-v3 source contained four 512 MiB files and two small files. Both workspace mounts exposed all four large files. The failing configuration used two independent metadata namespaces, one S3 bucket, the same packed manifest, and separate local caches:

| Setting | A | B |
| --- | --- | --- |
| Namespace | `issue382-confirmed-baseline-a` | `issue382-confirmed-baseline-b` |
| Bucket | `issue382` | `issue382` |
| Endpoint | `http://127.0.0.1:9000` | `http://127.0.0.1:9000` |
| PD | `127.0.0.1:2379` | `127.0.0.1:2379` |
| `alloc/slice`, before / after | 1 / 65 | 1 / 65 |
| First upper object key | `chunks-v2/1/0` | `chunks-v2/1/0` |

The manifest was `issue382/lower/objects/0/6b5f14c7c7e0edba75673fd0290a6ed3babfbe22223db19d1866209a67cfe063`. Its 270 objects occupy 540,097,773 stored bytes. None uses a `chunks-v2` key.

Each namespace was initialized independently with `workspace init-packed-volume --manifest-key <key> --data-config <yaml>`. Mounts used `volume_format: workspace-v1` and the returned workspace ID. An initial attempt to fork the root's native `fork_base` lacked the lower files and was excluded from acceptance.

After A successfully wrote, fsynced and read back 41 bytes, B produced these actual kernel syscall results:

```text
openat(..., "mnt-b/b-only.txt", O_RDWR|O_CREAT|O_TRUNC|O_CLOEXEC, 0644) = 5
write(5, ..., 41) = 41                  # offset 0
fsync(5) = -1 EIO (Input/output error)
close(5) = 0
```

`sync <B-path>` also failed. Both names remained invisible from the opposite workspace. Buffered fstat/stat returned 41 bytes, so this reproduction does not establish the original issue's separate zero-size observation.

With `RUST_LOG=debug`, B logged:

```text
pipeline upload failed, retrying, chunk_id: 8000000000,
slice_id: 1, offset: 0, len: 41,
error: create-only object put failed: chunks-v2/1/0, service error
Caused by: unhandled error (PreconditionFailed)
commit_chunk dropping failed slice after retry budget exhausted
```

Both catalogs reserved `[1,65)` and used slice 1. A's 41-byte payload occupied the 45-byte framed object; B's different payload was correctly rejected by conditional PUT. The failing path is fsync waiting for upload, S3 create-only rejection, and failed-slice propagation. It fails before the proposed metadata-commit/TiKV-CAS path. This is an upper-versus-upper collision, not a write into this fixture's lower.

The original incident's configuration and `coverage.tgz` remain unavailable. This demonstrates the independent-namespace/shared-bucket failure class; it does not establish the behavior of two valid packed workspaces within one catalog.

## Controls and rejected change

Copying the same lower objects into dedicated buckets `issue382-control-a` and `issue382-control-b` allowed both writes, fsyncs, syncs and readbacks to succeed with the same overlapping ID ranges. Whiteout, open-unlink and forced-discard lease revocation passed both this control and the failing shared-bucket run. The copies are a configuration workaround, not shared-physical-lower acceptance.

The regression probes have specific scope: whiteout removes A's lower `r.0` while B still sees it; open-unlink keeps a 4096-byte read of A's lower `r.1` valid through the existing fd; forced discard rejects the next `openat(O_CREAT)` on B with EIO. The latter probe accepts EIO or ESTALE and checks that writes are fenced, not a particular errno contract. Both the dedicated-bucket baseline and the rejected candidate returned EIO at that create; no existing assertion was changed.

An experimental change scoped writable keys by metadata namespace. Fresh catalogs sharing the original bucket passed dual writes and all three regression probes. The candidate was then tested against an existing baseline upper, using a fresh cache on each remount:

| Binary | File size | `pread(fd=4, offset=0, len=41)` |
| --- | --- | --- |
| Baseline | 41 | 41 correct bytes |
| Candidate | 41 | EIO |

Both mounts exited normally. The candidate was rejected and all Rust edits were reverted. A safe fix must persist a storage identity, preserve existing upper ownership, and use consistent addressing across reads, caches, recovery, snapshot/fork and GC. Blindly renaming keys or falling back to another catalog's global keys is insufficient.

The issue-383 offset `360042496` returned the expected 4096 bytes in these runs; no common cause with #383 is established.

## Evidence and validation

On the investigation host, `/work/REPORT-382.md` retains the prior report and the appended continuation. `/work/issue382/confirmed-baseline/artifacts/` contains complete strace, debug logs, allocator snapshots, configuration/command records, and object inventories. `eio-syscalls.log` and `eio-debug-context.log` are short excerpts. The first failing fsync occurred 60m35s after resumption; the prescribed rebuild consumed 58m10s, so the P1 deadline was exceeded by about 35 seconds.

Other artifacts:

- `/work/issue382/independent-split-buckets/artifacts/`: dedicated-bucket control.
- `/work/issue382/fixed-shared-3/artifacts/`: rejected candidate's fresh-state success.
- `/work/issue382/reopen-{baseline,rejected}.log`: cold-remount comparison.
- `/work/issue382/reopen-rejected.strace`: actual failing pread.
- `/work/issue382/artifacts/final-ci/`: restored-baseline CI logs and exit codes.

The host's `target/release/brewfs` was verified byte-for-byte by SHA-256 against `/work/issue382/baseline-bin/brewfs` (`e5f85029944509321e6d1087503f7edf0c99a211ac9fc78bd84fa37a237608b0`). The rejected binary remains separately archived. All investigation FUSE mounts have been unmounted; the task's PD, TiKV and RustFS services and their persisted data remain available for continuation.

The restored-baseline checks finished at 09:17:11 UTC on 2026-10-30. Rust compilation used `CARGO_BUILD_JOBS=2`, explicit `-j 2`, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, and `RUSTFLAGS='-C debuginfo=0'`, sequentially in tmux. Results:

| Check | Result |
| --- | --- |
| `cargo test -p brewfs --lib --features workspace-overlay -j 2` | **2569 passed, 0 failed, 451 ignored**; 103.46 seconds running tests |
| `cargo test -j 2 --workspace --lib --bins` | Library: **1275 passed, 0 failed, 262 ignored**; binary: **2 passed, 0 failed**; exit 0 |
| Workspace `check`, `build`, and `clippy` | All exit 0; clippy reports baseline warnings |
| BrewFS `--no-default-features` checks for `fuse-tokio-runtime` and `fuse-io-uring-runtime` | Both exit 0 |
| Four shell syntax checks and three shell report tests from `AGENTS.md` | All exit 0 |
| `git diff --check` | Exit 0 |
| `cargo fmt --all --check` | Exit 1: two pre-existing formatting differences in `src/config.rs`, at lines 2263 and 2284 |

`rfuse3` is not a workspace member, so the conditional rfuse3 feature matrix does not apply. ACL `permission_cache_ttl()` remains `Duration::ZERO`, and existing assertions and ignore attributes are unchanged. These results validate the restored baseline; no candidate test result is counted as an accepted-fix gate. There is no production-code change in this investigation.

## Phase 2: independent upper buckets

New S3 workspaces persist `upper_bucket` and create that bucket before writing the workspace record. Packed lower objects stay in the configured base bucket. Writable keys remain `chunks-v2/<slice-id>/<block-index>`; no prefix or object migration is introduced. The default `data.workspace_bucket_template` is `{bucket}-{workspace_id}`, where `{bucket}` is `data.s3.bucket`. The template must include `{workspace_id}` and render a valid S3 name of at most 63 bytes. A long base bucket name can use a shorter template such as `ws-{workspace_id}`.

```yaml
data:
  backend: s3
  workspace_bucket_template: "{bucket}-{workspace_id}"
  s3:
    bucket: brewfs-data
    region: us-east-1
```

`init-volume`, `create`, and `fork` require `--data-config <yaml>`; `init-packed-volume` already takes that argument. CreateBucket accepts success, `BucketAlreadyOwnedByYou`, and HTTP 409, except that explicit `BucketAlreadyExists` returns a naming-conflict error. Regions other than `us-east-1` supply a LocationConstraint. Failed metadata creation leaves the bucket intact.

SQLite and KV slice allocators use `slice/<upper_bucket>`; each KV scope reserves 64 IDs at a time. Legacy records without the field retain the original catalog allocator and shared bucket. SQLite can read an old schema without adding the column. Creating a new workspace may add the nullable metadata column, while old rows remain null. KV decoding accepts the old record envelope and supplies `None` in memory; the new envelope prevents an old binary from silently discarding the bucket field.

Mount and packed recovery select the stored upper bucket, with independent clients for the shared lower. Clean block caches include the workspace identity; durable writeback ownership directories retain their existing layout. The legacy GC cannot identify physical objects by bucket and therefore rejects destructive scans in catalogs containing isolated upper buckets. This deliberately retains objects and metadata until explicit cleanup; automatic GC for these catalogs is not implemented here.

Discard retains the bucket and prints its name. Inspect remains usable after discard, returning the durable identity with live layer/lease counters zero or absent; those counters do not imply an empty bucket. Explicit cleanup requires a discarded workspace and `--force`:

```bash
brewfs workspace <metadata-options> inspect <workspace-id>
brewfs workspace <metadata-options> discard <workspace-id>
brewfs workspace <metadata-options> drop-upper-bucket <workspace-id> \
  --data-config <yaml> --force
```

Cleanup refuses legacy shared buckets and the configured lower bucket. It deletes current objects in batches and then deletes the bucket; versioned or protected contents require explicit S3 administration.

### Real backend acceptance

TiKV 8.5.0, RustFS 1.0.4 and real FUSE mounts shared the original physical packed lower. Separate catalogs recreated the confirmed failing configuration. A barrier started both 41-byte writers concurrently.

| Check | Before | Independent upper buckets |
| --- | --- | --- |
| Write / fsync / sync / readback | A passes; B fsync EIO | Both write 41B, fsync errno 0, sync exit 0, exact readback |
| A upper bucket | `issue382` | `issue382-01a18c94-c380-7341-ad71-91d33bd8c3ec` |
| B upper bucket | `issue382` | `issue382-01a18c94-ca01-7f73-98a0-c2704cf06a1f` |
| First key, both writers | `chunks-v2/1/0` | `chunks-v2/1/0` |
| HTTP result | B receives 412 | Both PUTs 200; zero 412 |
| Existing upper, cold remount | 41B readable | Same old workspace and fresh cache: 41B readable |
| New upper, cold remount | Not applicable | 41B readable |
| Whiteout / open-unlink / lease revocation / naming isolation | Pass | 4/4 pass |
| Single-workspace smoke | 41B write/fsync/sync/readback passes | Same smoke passes; no fio throughput claim |

Evidence is under `/work/issue382/phase2-final/artifacts/`: `acceptance.json`, `probe.jsonl`, `probe.log`, `s3.pcap`, `s3-http.txt`, `upper-bucket-objects.json`, and both `*-cold.log` files. Packet text lines 656/676 and 698/718 pair the two PUTs with HTTP 200. The two objects contain different 41-byte payloads in 45-byte frames. The base bucket's 277 key/ETag pairs remained identical, including all 270 packed-lower objects. Lease revocation rejects a subsequent create with EIO, matching the original smoke's EIO-or-ESTALE contract.

The first three acceptance checks completed at 2026-10-30 11:32:50 UTC, about 2h09m after phase 2 started, exceeding the requested two-hour window. An earlier `strace -f -yy` run blocked while the tracer resolved a FUSE fd path; the saved kernel stack is `/work/issue382/phase2/artifacts/tracer-stack.log`. Subsequent probes used direct syscall return values and independent tcpdump capture; stale disconnected mounts were removed after preserving diagnostics.

The first phase-2 library run found two failures (2574 passed / 2 failed / 451 ignored): an extra GC scan shifted an existing concurrent-revalidation probe, and the SQLite old-schema fixture reused a connection after changing its schema. The GC check now uses the existing workspace root scan, and the fixture opens a fresh store. Existing assertions remain intact. Final CI logs and exit codes are in `/work/issue382/phase2-final/artifacts/ci/`: overlay **2577 passed / 0 failed / 451 ignored**, overlay build, workspace check/build, tokio/uring, workspace lib/bin tests, clippy, shell checks, fmt and diff all exit 0. The final binary repeat is under `/work/issue382/phase2-head/artifacts/`.
