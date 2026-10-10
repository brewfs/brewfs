# Packed-v3 G16 resource journal validation (2026-10-10)

This checkpoint covers the local runner resource-accounting contract. It does
not claim the Redis/TiKV, Kubernetes, or real-FUSE lifecycle exits.

## Implementation

`tools/perf/run_packed_local.sh` owns a unique Compose project, mount worker,
temporary work directory, and artifact prefix. It writes
`resource-journal.json` using schema `packed-v3-resource-journal-v1` and records
`init`, `compose_started`, `mount_ready`, `mount_unmounted`,
`compose_stopped`, `work_removed`, and `finalize` events. The manifest verifier
requires this file for successful and failed runs. A successful run is accepted
only when mount, worker, Compose, and temporary directory are all gone; a
failed run remains consumable only with an explicit terminal cleanup decision.

## Verification

```text
python3 -m unittest \
  tools.perf.test_packed_resource_journal \
  tools.perf.test_packed_run_manifest \
  tools.perf.test_packed_local_runner \
  tools.perf.test_packed_p90_policy
25 passed

bash -n tools/perf/run_packed_local.sh
python3 -m py_compile tools/perf/*.py
git diff --check
```

The tests cover complete success cleanup, failed termination with live-resource
facts, tampered status, manifest fail-closed behavior, runner event wiring, and
p90 policy binding. No cloud or FUSE campaign was run in this checkpoint.
