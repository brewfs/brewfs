# Manager lifecycle API (v1)

The Manager adds catalog lifecycle operations to the existing Console HTTP
service. It runs without Kubernetes and uses the same bearer-token
authentication. The existing `/api/volumes` endpoints still manage Console
configuration records; only `/api/v1` performs the lifecycle operations below.

## Supported scope

This first version manages **`workspace-v1` catalogs through SQLite**. It can
initialize a real catalog, register a verified existing catalog, inspect its
resources, fork an exact immutable revision, and mark a workspace for deletion.
The HTTP service does not mount filesystems or certify remote object readiness.

`workspace-native-v2`, packed formats, Redis/TiKV Manager connections, checkpoint,
node execution, attachments, drain, handoff, volume deletion, and physical object
reclamation are unsupported. The independent CLI and Operator retain their own
supported backends and behavior. In particular, the Manager does not advertise
pending writeback support or turn an active workspace into an implicit snapshot.
Read `/api/v1/capabilities` for the build's actual capabilities. Without the
`workspace-overlay` feature, lifecycle requests return `unsupported`.

## Start one instance

Build the existing binary with lifecycle support:

```sh
cargo build --no-default-features --features fuse-tokio-runtime,workspace-overlay
```

Configure catalog references locally in `STATE_DIR/manager-catalogs.json`:

```json
{
  "catalogs": {
    "development": {
      "backend": "sqlite",
      "path": "/var/lib/brewfs/catalogs/development.sqlite"
    }
  }
}
```

The catalog parent directory must already exist. Use a private, durable state
directory and private catalog files. Clients provide a configured `catalog_ref`,
never a filesystem path, connection URL, credential, or shell command. The
Console token is an administrator credential for this Manager's configured
catalogs and registered volumes; owner labels do not implement separate tenant
credentials. Do not give the same catalog to competing lifecycle controllers.

```sh
./target/debug/brewfs console \
  --listen 127.0.0.1:8080 \
  --state-dir /var/lib/brewfs/console \
  --auth-token-file /etc/brewfs/console-token
```

The Manager uses a persistent SQLite operation journal and a process lock in
the state directory, plus a lock for each configured catalog. Run **one Manager
instance**, keep its journal across
restarts, and keep catalog references stable. Multiple replicas and a shared
SQLite file on a network filesystem are not supported. A missing frontend
bundle does not prevent API use. Use the existing Console deployment's TLS
termination when exposing the service beyond loopback.

## API contract

All routes below are relative to `/api/v1` and require the existing Console
authentication. Every mutating request requires exactly one `Idempotency-Key`
header containing 1–128 visible ASCII characters. Unknown top-level request fields and
unsupported formats are rejected.

| Method and path | Behavior |
| --- | --- |
| `GET /capabilities` | Version, enabled formats/backends, and explicit unsupported capabilities |
| `POST /volumes` | `initialize` a new catalog or `register` a verified existing catalog |
| `GET /volumes` | List volumes registered with this Manager |
| `GET /volumes/{volume_id}` | Inspect the authoritative catalog identity and root revision |
| `GET /volumes/{volume_id}/revisions/{layer_id}` | Read and verify an immutable revision tuple |
| `POST /volumes/{volume_id}/forks` | Fork an exact revision into 1–100 independent workspaces |
| `GET /volumes/{volume_id}/workspaces` | List workspaces; `limit=1..100`, optional `after=UUID` |
| `GET /volumes/{volume_id}/workspaces/{workspace_id}` | Inspect workspace, base revision, head and leases |
| `DELETE /volumes/{volume_id}/workspaces/{workspace_id}` | Request a lease-checked logical deletion; preserve shared lower layers |
| `GET /operations/{operation_id}` | Query durable operation and per-item results |

A mutation returns `202 Accepted`, a `Location` pointing to its operation, and
the operation record. Acceptance means the request and target IDs have been
recorded durably. It does not mean metadata is ready. Inspect the operation's
state and each item; a successful catalog operation reports metadata readiness,
not a mount or `remote_ready` guarantee.

The key is scoped to the operation's resource/action. The same key with the
same normalized request returns the original operation and resource IDs; reuse
with a different request returns `idempotency_conflict`. Batch forks are not
atomic: completed items remain completed, and recoverable unfinished items resume
under their original allocated IDs. Query or resubmit the original request after a lost
response; do not invent a new key to retry an uncertain mutation.
After correcting a failure's cause, resubmitting the same request and key retries
unfinished items in the original operation while preserving completed items.

`initialize` requires `confirm_empty: true`, rejects `expected_volume_id`, and
refuses an existing catalog or other data. `register` requires the expected real
volume UUID and validates its catalog header, required schema, and active
workspace views without repairing missing tables. It does not accept `owner` or
`confirm_empty: true`. A Console `volumes.json` entry alone is not a filesystem
volume.

Deletion marks the workspace and its writable head `Deleting` only after the
catalog atomically checks that no active, unexpired workspace lease blocks the
transition. Shared immutable lower layers remain unchanged, including when a
child workspace or snapshot references them. Those references protect lower
layers from physical reclamation by the existing catalog/GC rules; they do not
prevent the parent workspace's logical marker. No `force` option is exposed,
and no S3 bucket, Redis database, shared lower layer, or object data is deleted.
A successful delete has `physical_reclaimed: false`.

## Separate request examples

Set `API` to the running Console URL and `TOKEN` to its bearer token in your
shell. The examples are individual requests; they do not mount workloads, run
experiments, provision resources, or perform cleanup automatically.

```sh
export API=http://127.0.0.1:8080
export TOKEN='your-console-token'
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" \
  "$API/api/v1/capabilities"
```

Initialize the configured empty target:

```sh
curl --fail-with-body -sS -X POST "$API/api/v1/volumes" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: development-initialize-1' \
  --data '{"mode":"initialize","catalog_ref":"development","owner":"alice","confirm_empty":true,"volume_format":"workspace-v1"}'
```

Alternatively, register a catalog that already exists. Set `EXISTING_VOLUME_ID`
to its actual header UUID; this operation must not initialize it:

```sh
curl --fail-with-body -sS -X POST "$API/api/v1/volumes" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: development-register-1' \
  --data "{\"mode\":\"register\",\"catalog_ref\":\"development\",\"expected_volume_id\":\"$EXISTING_VOLUME_ID\",\"volume_format\":\"workspace-v1\"}"
```

Set `OPERATION_ID` and `VOLUME_ID` from the response. Query completion and inspect
the volume separately:

```sh
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" \
  "$API/api/v1/operations/$OPERATION_ID"
```

```sh
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" \
  "$API/api/v1/volumes/$VOLUME_ID"
```

For an initialized volume, save the returned exact `root_revision` object as
`revision.json`. Registered volumes have no Manager-assigned root; list and
inspect an existing workspace using the requests below, then save its
`base_revision` instead. You can also read a known immutable layer at
`GET /api/v1/volumes/{volume_id}/revisions/{layer_id}`. The revision object contains
`layer_id`, `sealed_version`, and the 32-byte `root_hash` array. Create two
workspaces from that tuple using `jq` to preserve its JSON types:

```sh
jq '{revision: ., count: 2, owner: "alice"}' revision.json | \
  curl --fail-with-body -sS -X POST "$API/api/v1/volumes/$VOLUME_ID/forks" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: development-fork-1' --data-binary @-
```

List and inspect workspaces; set `WORKSPACE_ID` to an ID in the workspace list or
a completed fork's operation results:

```sh
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" \
  "$API/api/v1/volumes/$VOLUME_ID/workspaces?limit=20"
```

```sh
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" \
  "$API/api/v1/volumes/$VOLUME_ID/workspaces/$WORKSPACE_ID"
```

Request logical deletion, then query the returned operation ID separately:

```sh
curl --fail-with-body -sS -X DELETE \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: development-delete-1' \
  "$API/api/v1/volumes/$VOLUME_ID/workspaces/$WORKSPACE_ID"
```

## Recovery boundaries

Operation IDs and target resource IDs are persisted before catalog mutations.
The operation journal and catalog are different stores, so there is no claimed
cross-store atomic commit. On restart, the Manager reconciles unfinished work
against the real catalog before resuming. A metadata commit whose reply was lost
is recognized when its persisted creation identity still matches, instead of
creating another workspace. Conflicting owners, head IDs, revisions, or an
already-advanced head fail closed for operator review; recovery does not reset
an active workspace to make it match an old request.
Startup resumes `accepted`, `running`, and `recovery_required` operations before
serving requests. A `failed` operation requires explicit resubmission of its
original request and key.
If the catalog file disappears after initialization's preflight was recorded,
recovery requires restoring the original catalog. Retrying must not substitute
a new empty database for a catalog whose contents may have been committed.

Operations distinguish `accepted`, `running`, `succeeded`, `failed`, and
`recovery_required`; inspect errors and per-item state before acting. An uncertain
failure is not proof that the catalog mutation did not happen. Keep the journal,
catalogs, and configured references together when restoring service. This phase
does not recover missing node state, staging files, unflushed client writes, or
cross-node mounts, and it does not make an offline client's data remotely ready.
