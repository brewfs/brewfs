# Entity overlay native and packed writeback

Both paths use `workspace-entity-v1` with `--experimental-entity-mount` and the
same `EntityData` upper. Production admission remains experimental. The default
is `cache.writeback_mode: upload_before_commit`. The legacy `workspace-v1`,
`workspace-native-v2`, and packed v1/v2 formats are unchanged.

Select exactly one lower:

* `--packed-manifest-key KEY` uses the existing authenticated packed-v3 graph.
* `--entity-native-source DIR` captures a private, quiescent tree before mount.
  Its frozen metadata consists of individually authenticated Redis rows; its
  data consists of immutable raw S3 objects. It preserves hardlinks, symlinks,
  attributes and xattrs. The source can be an offline exported native tree or
  a drained native mount with all writers stopped. This is a fresh capture, not
  an attachment to a live mutable Redis namespace. Source fingerprints are
  rechecked, and detected concurrent changes fail capture.
* `--entity-native-manifest-key KEY` reuses a captured native lower in the same
  entity volume. The first mount prints its key in `entity_experiment`.

The entity FUSE adapter reads captured symlink targets and `user.*`/POSIX ACL
xattrs through the entity namespace, including upper xattr overrides and
whiteouts. Other existing experimental FUSE limitations still apply.

Native snapshot admission is bounded to 32,768 metadata rows, 4 MiB of manifest,
and 4,096 names per directory. Exceeding a bound fails explicitly. Metadata and
data live in separate new domains; legacy collectors cannot reclaim them.
Native lower objects remain retained, including failed-capture objects; automatic
native snapshot garbage collection is not introduced here.

Example opt-in cache section (YAML):

```yaml
cache:
  cache_root: /home/hxy/.cache/brewfs-entity
  writeback_mode: commit_before_upload
  write_ssd_bytes: 1073741824
  writeback_persist_sync: true
  writeback_require_stage_before_commit: true
```

Use a persistent Linux filesystem for staging. The acknowledgement sequence is:
reserve a unique object ID; write and fsync the staging file and its directory;
persist remote debt; publish inode/maps/ownership atomically; acknowledge locally.
The uploader sends the original immutable object, then clears its debt. Local
reads authenticate staging data. A remote-only read of a missing object fails;
it does not synthesize zeros. Losing the staging device before upload can lose
acknowledged data.

`fsync` and release retain local durability semantics in this opt-in mode.
Orderly unmount stops admission, drains retained operations and remote objects,
then enters maintenance. A timeout returns failure, preserves debt and staging,
and does not report `remote_ready`. Persistent debt also blocks maintenance,
resume and discard after a crash. Opt-in namespaces carry a distinct descriptor
format so older clients cannot ignore that debt.

After restarting on the same staging directory, use the same volume UUID,
workspace UUID, Redis/S3 configuration and cache root:

```bash
brewfs mount /unused --config entity.yaml \
  --volume-format workspace-entity-v1 --experimental-entity-mount \
  --entity-volume-id VOLUME_UUID --workspace NAMESPACE_UUID \
  --entity-recover-uploads
```

This exits after upload recovery without mounting. It only uploads original
objects and clears their authenticated debt; it never replays writes, truncates
or names. An expired lease does not permit takeover of a namespace: this command
does not restore mount admission or claim old readers have stopped. Missing or
corrupt staging data fails recovery and retains the debt. A local file lock
excludes concurrent users of the same staging directory.

`EntityData::writeback_status()` and the `entity_writeback`/`entity_upload_recovery`
JSON records expose pending object/byte counts, successfully uploaded bytes,
retry count and the most recent upload error. `remote_ready` is emitted only
after the strong drain and maintenance transition succeed.

Functional validation covers blocked uploads, local reads/fsync, capacity
failure, overwrite/truncate/unlink before upload, restart with an expired lease,
missing/corrupt staging, lifecycle races and native lower authentication/COW.
These are correctness checks, not performance measurements.

The owned loopback/FUSE smoke runner checks both modes on both lowers, then
blocks RustFS and kills each asynchronous mount to test upload-only recovery:

```bash
ENTITY_SMOKE_BINARY=/absolute/path/to/brewfs \
ENTITY_SMOKE_FIXTURE=/absolute/path/to/packed_v3_snapshot_fixture \
ENTITY_SMOKE_ARTIFACTS=/absolute/path/to/artifacts \
bash tools/testing/run_entity_writeback_smoke.sh
```

It creates its own Redis/RustFS Compose project and removes those containers and
volumes on exit. Retained source/cache directory paths are recorded in
`local-work-directory.txt` for diagnosis. It requires Linux FUSE, Docker Compose, and the AWS CLI container;
image overrides are `ENTITY_SMOKE_REDIS_IMAGE`, `ENTITY_SMOKE_RUSTFS_IMAGE`, and
`ENTITY_SMOKE_AWS_IMAGE`. Set `ENTITY_SMOKE_TEST_BINARY` to a compiled library
test executable to run the entity and packed-lower integration tests against the
same owned services first. The existing entity integration suite remains available
through `tools/testing/run_entity_data_tests.sh`.
Set `ENTITY_SMOKE_TEST_ONLY=1` with `ENTITY_SMOKE_TEST_BINARY` to run just those
integration tests without building the mount or fixture executables.

The existing `tools/perf/run_entity_mount_comparison.sh` now wires all three
arms: the unchanged flat native baseline, entity overlay-native, and entity
overlay-packed. `ENTITY_MOUNT_WRITEBACK_MODE` selects the two entity arms' mode
and defaults to `upload_before_commit`; the native baseline stays unchanged.
Use `ENTITY_MOUNT_SMOKE_ONLY=1` to validate this entry point without collecting
performance results. Every entity stop must report `remote_ready`, and shutdown
duration is recorded separately. In opt-in writeback mode, per-file fsync and
phase throughput cover local durability and exclude the final remote drain;
they must not be presented as end-to-end remote-durable throughput.
