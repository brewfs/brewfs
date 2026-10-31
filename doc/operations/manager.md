# Manager lifecycle API (v1)

The Manager adds catalog lifecycle operations to the existing Console HTTP
service. It runs without Kubernetes and uses the same bearer-token
authentication. The existing `/api/volumes` endpoints still manage Console
configuration records; only `/api/v1` performs the lifecycle operations below.

## Supported scope

The Manager can initialize a real catalog, register a verified existing catalog,
inspect its resources, fork an exact immutable revision, and mark a workspace for
deletion. The supported combinations are:

| Volume format | Metadata backends | Required build feature | Immutable lower |
| --- | --- | --- | --- |
| `workspace-v1` | SQLite, Redis, TiKV | `workspace-overlay` | Exact catalog revision |
| `workspace-native-v2` | Redis, TiKV | `native-packed-base` | Native P1 catalog revision |

The native profile uses the real CLI's workspace-v1 catalog carrier plus its
separate native-v2 volume, namespace, head, and ownership-domain records. It does
not change that catalog to schema 2. Native `PublishedRevision`, frozen metadata,
packed catalog bindings, and `workspace-entity-v1` are unsupported. A native
namespace cannot be registered as workspace-v1 to bypass native validation.

The HTTP service does not mount filesystems, run benchmarks, certify remote
object readiness, checkpoint active workspaces, execute on nodes, manage
attachments, drain writes, schedule or migrate workloads, hand off mounts, delete
volumes, or reclaim physical objects. Pending writeback is not supported. The
independent CLI and Operator retain their own supported behavior; an accepted
Manager operation is not evidence that a workload has run successfully.

Read `/api/v1/capabilities` for this build's format/backend combinations and
limits. Without `workspace-overlay`, lifecycle requests return `unsupported`.
Native requests require `native-packed-base`; SQLite is never a native backend.

## Start one instance

Build the existing binary with lifecycle support:

```sh
cargo build --no-default-features --features fuse-tokio-runtime,workspace-overlay
```

For the native P1 profile, use `fuse-tokio-runtime,native-packed-base` instead;
that feature also enables workspace-overlay support.

Configure catalog references locally in `STATE_DIR/manager-catalogs.json`:

```json
{
  "catalogs": {
    "development": {
      "backend": "sqlite",
      "path": "/var/lib/brewfs/catalogs/development.sqlite"
    },
    "shared-redis": {
      "backend": "redis",
      "url_env": "BREWFS_MANAGER_REDIS_URL",
      "namespace": "campaign-a",
      "stable_cluster_id": "redis-service-a"
    },
    "shared-tikv": {
      "backend": "tikv",
      "pd_endpoints": ["pd-a.internal:2379", "pd-b.internal:2379"],
      "namespace": "campaign-b",
      "stable_cluster_id": "tikv-service-b"
    }
  }
}
```

The SQLite catalog parent directory must already exist. Use a private, durable
state directory and private catalog files. Redis accepts exactly one local `url`
or `url_env`; an environment variable lets the service resolve a connection URL
without putting it in the catalog configuration. It must use `redis://`; this
adapter does not enable Redis TLS. Redis database number is part of the immutable
scope. Use a standalone server or the direct address of a Sentinel-managed data
node. Sentinel discovery and Redis Cluster are unsupported for all Manager
profiles: this is a direct-node client, and strict namespace initialization and
native control checks can span Redis hash slots.

TiKV workspace-v1 connections may include a local `tls` object with absolute
`ca_path`, `cert_path`, and `key_path`. HTTPS endpoints require those files.
Native TiKV TLS is explicitly unsupported; the Manager never falls back to an
unencrypted native control connection when TLS was requested.

`namespace` and `stable_cluster_id` are nonempty, at most 128 characters, using
ASCII letters, digits, `.`, `_`, or `-`. Assign `stable_cluster_id` once to the
actual backend deployment and preserve it when rotating endpoints or credentials.
Changing backend, stable cluster identity, namespace, or Redis database rebinds
the catalog and is rejected for existing operations and volumes. A new address
must still resolve to the same real volume and persisted Manager ownership row.
Do not reuse one `catalog_ref` for both v1 and native profiles.

Clients provide a configured `catalog_ref`,
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

The Manager uses a persistent local SQLite operation journal and a process lock
in the state directory. Local catalogs also have process locks. Remote catalogs
have a persistent journal-owner identity and an expiring, renewed session claim;
mutations check exact ownership and deadline atomically in the backend. Native
control writes use the same claim. This fences a stale Manager session and does
not create a distributed journal or multi-replica service.

The Manager's shutdown method closes mutation admission, waits for its workers
to stop, and releases its controller sessions. Queued operations remain in the
journal for recovery with their original IDs. This does not drain mounted
clients or release their workspace leases. After abrupt process termination,
the old controller session must expire before another process can resume it.

Run **one Manager instance**, keep its journal across restarts, and keep catalog
references stable. A different journal cannot take over an existing ownership
row by waiting for its session to expire. Multiple replicas and a shared SQLite
file on a network filesystem are unsupported. A missing frontend
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
| `GET /volumes/{volume_id}/revisions/{layer_id}` | Read and verify the exact v1 or native P1 revision object |
| `POST /volumes/{volume_id}/forks` | Fork an exact revision into 1–100 independent workspaces |
| `GET /volumes/{volume_id}/workspaces` | List workspaces; `limit=1..100`, optional `after=UUID` |
| `GET /volumes/{volume_id}/workspaces/{workspace_id}` | Inspect workspace, base revision, leases, mount identity, and native control state when applicable |
| `DELETE /volumes/{volume_id}/workspaces/{workspace_id}` | Request a lease-checked logical deletion; preserve shared lower layers |
| `GET /operations/{operation_id}` | Query durable operation and per-item results |

A mutation returns `202 Accepted`, a `Location` pointing to its operation, and
the operation record. Acceptance means the request and target IDs have been
recorded durably. It does not mean metadata is ready. Inspect the operation's
state and each item; a successful catalog operation reports metadata readiness,
not a mount or `remote_ready` guarantee.

The key is scoped to the operation's resource/action. The same key with the
same normalized request returns the original operation and resource IDs; reuse
with a different request returns `idempotency_conflict`. Batch items execute
serially and batches are not atomic: completed items remain completed, and
recoverable unfinished items resume
under their original allocated IDs. Query or resubmit the original request after a lost
response; do not invent a new key to retry an uncertain mutation.
After correcting a failure's cause, resubmitting the same request and key retries
unfinished items in the original operation while preserving completed items.

Each fork request admits 1–100 workspaces. A plan needing 128 must define two
requests of 64 before submission, each with its own stable idempotency key. Track
both operations and all 128 item results. Do not shrink a partially completed
request's count or allocate a replacement key. Burst submission does not make
item execution parallel.

The service admits at most 64 concurrent mutation requests. New operations are
rejected when 128 operations are pending (`accepted`, `running`, or
`recovery_required`). Overload returns HTTP **429** with error code `overloaded`;
back off and retry the original request and key. A rejected request is not a
completion receipt. Original-operation retries retain a recovery path through
the pending-operation limit; history reads do not consume mutation admission.
Invalid input returns 400, unsupported capabilities 501,
identity/idempotency conflicts 409, missing resources 404, and unavailable or
uncertain catalog outcomes 503.

`initialize` requires `confirm_empty: true`, rejects `expected_volume_id`, and
refuses an existing catalog or other data. `register` requires the expected real
volume UUID and validates its catalog header, required schema, and active
workspace views without repairing missing tables. Remote registration then
claims that namespace for the journal; native registration additionally validates
the native header and control records. It does not accept `owner` or
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

For native P1, deletion also checks the native writer lease and ownership-domain
state before marking the catalog, then checks native control state atomically
when applying its native fence. This two-stage transition is not a cross-store
atomic delete. If the catalog marker succeeded but the native phase failed, the
operation stays `recovery_required`; a failure is not evidence that nothing was
marked. Retry the same operation and key after resolving the reported cause.

This supplies no drain, flush, or unmount evidence. A caller must establish its
own workload provenance, preserve results, and complete client drain/unmount and
lease release before requesting
deletion. Deletion is not a substitute for releasing a mount during migration.

Workspace inspection retains `record`, `base_revision`, `layer_depth`, and
`leases`, and adds `mount_identity`. That object contains configured catalog
reference, metadata backend, volume format, volume/workspace IDs, and applicable
namespace identities. Connection addresses, credentials, and SQLite paths are
not returned. Combine these identities with separately configured client
connection and object-storage settings; they are not an executable mount command.

Native inspection adds `native`, including the actual head and domain IDs,
epoch/generation, commit sequence, lease presence, and deletion state. The native
`mount_identity.upper_bucket` is always `null`: the current CLI does not route a
per-workspace bucket. Native workspaces use a shared object pool with isolated
object keys, not an independent bucket per workspace. These fields do not certify
remote durability; resource and operation-item `remote_ready` remains `null`.

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

To initialize a configured Redis or TiKV native P1 namespace, use that remote
`catalog_ref` and `"volume_format":"workspace-native-v2"`. The same format must be
specified when registering an existing native volume. This is a metadata
operation; it does not create an object bucket or a populated source corpus.

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
`GET /api/v1/volumes/{volume_id}/revisions/{layer_id}`. The v1 revision object keeps
its original `layer_id`, `sealed_version`, and 32-byte `root_hash` array, without
an added discriminator. Native P1 uses this shape:

```text
{
  "kind": "native_p1_catalog",
  "volume_id": "<actual volume UUID>",
  "storage_namespace_id": <actual 16-byte integer array>,
  "catalog_revision": {
    "layer_id": "<actual immutable catalog layer UUID>",
    "sealed_version": <actual integer>,
    "root_hash": <actual 32-byte integer array>
  }
}
```

Copy the full returned object; do not synthesize namespace IDs, hashes, or a
`PublishedRevision`, and do not strip the native wrapper to request a v1 fork.
The native revision verifies both the native volume/namespace identity and the
catalog lower. It is not a frozen snapshot of mutable native head data. An empty
initialized root is not a populated workload source. The Manager does not
implicitly checkpoint a running workspace to produce one.

Create two workspaces from the saved object using `jq` to preserve JSON types:

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
Startup opens the journal and resumes `accepted`, `running`, and
`recovery_required` operations. SQLite recovery completes before requests are
served. Remote recovery runs in the background so an offline Redis or TiKV
service cannot block local operation queries; poll each operation to observe its
outcome. A listening server does not mean recovery is complete. A `failed`
operation requires explicit resubmission of its original request and key.
An unexpired claim from the previous process reports `session_busy` (HTTP 503
when returned by a request). The recovery worker retries that condition at
five-second intervals for up to eight attempts, preserving the same IDs. If the
claim remains held, the operation stays `recovery_required`; inspect the old
process and retry the original request and key after the claim expires.
If the catalog file disappears after initialization's preflight was recorded,
recovery requires restoring the original catalog. Retrying must not substitute
a new empty database for a catalog whose contents may have been committed.

The same rule applies to a previously established remote ownership claim:
recovery requires the original namespace and owner record. Removing those rows
does not authorize a fresh initialization. Before the original identity and
ownership have been established, an ambiguous initialization is also pinned to
its original transport fingerprint. Credential rotation does not change that
fingerprint; routing to a different Redis host/port or TiKV endpoint set does.
After binding, new connection settings must still pass real identity and
ownership checks. Missing credentials or an offline backend do not erase the
journal; original operation queries and idempotent replay preserve their IDs.

Bound resource queries also verify the permanent journal owner without renewing
or acquiring a session. A matching volume header alone is insufficient: a copied
catalog missing its original ownership row cannot be presented as ready. An
expired session does not erase that permanent owner, and operation-history reads
remain independent of the remote backend.

Operations distinguish `accepted`, `running`, `succeeded`, `failed`, and
`recovery_required`; inspect errors and per-item state before acting. An uncertain
failure is not proof that the catalog mutation did not happen. Keep the journal,
catalogs, and configured references together when restoring service. This phase
does not recover missing node state, staging files, unflushed client writes, or
cross-node mounts, and it does not make an offline client's data remotely ready.
