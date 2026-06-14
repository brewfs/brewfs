# BrewFS Web Console Design

## Summary

BrewFS should provide a small network console for managing BrewFS volumes and running instances through a browser and HTTP API. The console is not a data-path replacement for FUSE, SDK, S3 Gateway, or WebDAV. It is the operational control plane: create and register filesystems, inspect runtime health, browse namespace metadata, view trash, configure ACLs when supported, and surface Kubernetes CSI state.

The first implementation should be a single `brewfs console` server with a REST API and a static web UI. It should reuse the existing runtime registry and Unix socket control plane for mounted instances, and introduce a lightweight volume registry for filesystems that are not currently mounted.

## Goals

- Add a browser-accessible BrewFS management surface.
- Manage filesystem definitions: create, list, inspect, update labels/config notes, and delete registry entries.
- Show mounted instance status, build/version, metadata backend capabilities, jobs, and basic stats.
- Browse files and directories through metadata/VFS APIs without requiring users to shell into a mount host.
- Show trash entries once trash support exists; before then, expose a disabled/unsupported state.
- Configure ACL entries when the selected metadata backend reports ACL capability.
- Support a CSI dashboard mode that summarizes Kubernetes PV/PVC/StorageClass/Pod mount relationships.
- Keep the first server local/admin-oriented and explicit about unsupported capabilities.

## Non-Goals

- Do not implement a full enterprise console with multi-tenant billing, organizations, audit exports, or hosted SaaS flows.
- Do not edit file contents in the browser in the first version.
- Do not make the console a high-throughput file transfer path; S3 Gateway and SDK remain the data APIs.
- Do not require Kubernetes for the base console.
- Do not hide backend capability gaps. Unsupported ACL/trash/quota operations must be visible and return stable errors.

## Recommended Approach

Use a built-in `brewfs console` command that serves both REST API and static assets.

Alternative approaches considered:

1. Built-in console server, recommended. It shares BrewFS config parsing, runtime registry, and control-plane types. It is easiest to ship and test as part of the existing binary.
2. Separate dashboard crate or service. This gives cleaner packaging but duplicates config and auth plumbing too early.
3. External dashboard only, consuming CLI JSON. This is fast for prototypes but brittle and poor for interactive operations.

The built-in approach is the right first step because BrewFS already has a local runtime registry and instance-scoped control plane. The console can become a better client of those primitives before new distributed management infrastructure exists.

## Chosen Stack

### Backend

Use Rust inside the existing `brewfs` binary:

- `axum 0.8` for HTTP routing and JSON handlers.
- `tower-http` for static file serving, tracing, and request utilities.
- `serde` and `serde_json` for API models.
- Existing `tokio`, `clap`, `RuntimeRegistry`, and control-plane client code.

`brewfs` already uses Rust for CLI, runtime registry, and control-plane transport. Keeping the console server in the same binary avoids a second deployment artifact and lets the server reuse existing build metadata, config parsing, and Unix socket discovery.

### Frontend

Use a Vite React single-page app:

- Vite for fast development and static production builds.
- React with TypeScript for typed UI state and API models.
- Plain CSS variables and component-scoped CSS for styling.
- `lucide-react` for icons.

Avoid Tailwind, shadcn, Next.js, and a heavy component framework in the first scaffold. The console should feel like a dense operational tool: sidebar navigation, compact tables, status strips, detail panels, and explicit disabled states.

### Development And Release Model

- Development: run Vite on its own port and proxy `/api` to `brewfs console`.
- Release: build the frontend to `web/console/dist`, and have `brewfs console` serve the static files.
- Tests: keep backend API tests in Rust; keep frontend smoke and component tests in the web project once the first interactive state is added.

## Initial Scaffold Scope

The first implementation should create a runnable skeleton, not the full console:

- Add `brewfs console --listen 127.0.0.1:18080 --dev-no-auth`.
- Serve `GET /api/health`.
- Enforce bearer-token auth for `/api/*` unless `--dev-no-auth` is set.
- Serve the frontend static app.
- Add frontend routes for Overview, Filesystems, Browser, Trash, ACL, Jobs, CSI, and Settings.
- Add a small frontend API client for `/api/health`.
- Render empty states and unsupported states for features that are not wired yet.

The first scaffold should not implement volume registry persistence, live instance discovery, ACL writes, trash reads, CSI Kubernetes queries, or file browser control-plane extensions. Those belong to later phases after the server and UI shell are stable.

## Architecture

```text
Browser
  |
  | HTTP/JSON
  v
brewfs console
  |-- static UI assets
  |-- REST API router
  |-- auth middleware
  |-- volume registry
  |-- runtime registry reader
  |-- control-plane client
  |-- optional Kubernetes client adapter
  |
  +--> mounted BrewFS instance Unix sockets
  +--> metadata/object config records
  +--> Kubernetes API server, optional
```

The console server should have three data sources:

- Volume registry: persisted JSON/YAML records under a configurable state directory, defaulting to `$XDG_STATE_HOME/brewfs/console` or `/var/lib/brewfs/console` when running as root.
- Runtime registry: existing `RuntimeRegistry` records under the BrewFS runtime directory.
- Instance control plane: existing Unix socket requests, extended over time with stats, jobs, namespace browsing, ACL, and trash operations.

When a volume is mounted, the console should prefer live control-plane reads. When it is not mounted, the console can still show registered configuration but should mark runtime-dependent actions as unavailable.

## CLI

Add a new subcommand:

```bash
brewfs console \
  --listen 127.0.0.1:8080 \
  --state-dir /var/lib/brewfs/console \
  --runtime-dir /run/user/1000/brewfs \
  --auth-token-file /var/lib/brewfs/console/token \
  --kubeconfig ~/.kube/config \
  --enable-csi-dashboard
```

Default behavior:

- Listen on `127.0.0.1:8080`.
- Require a bearer token unless `--dev-no-auth` is set.
- Read runtime records from `RuntimeRegistry::default_root()`.
- Disable CSI dashboard unless explicitly enabled.
- Serve static UI and `/api/*` from the same listener.

## REST API

All API responses should use JSON. Errors should use:

```json
{
  "error": {
    "code": "unsupported",
    "message": "ACL is not supported by this metadata backend"
  }
}
```

### Health

- `GET /api/health`
  - Returns server version, git build metadata, auth mode, and enabled integrations.

### Volumes

- `GET /api/volumes`
  - Lists registered filesystems and whether each has a live instance.
- `POST /api/volumes`
  - Creates a registry entry from mount-like config fields.
  - MVP validates config shape but does not provision external object stores or databases.
- `GET /api/volumes/{volume_id}`
  - Returns config summary, capabilities, runtime status, and last known jobs.
- `PATCH /api/volumes/{volume_id}`
  - Updates display name, labels, description, and non-format runtime hints.
- `DELETE /api/volumes/{volume_id}`
  - Deletes only the registry entry unless a future `destroy=true` flag is added.

Volume IDs should be stable UUIDs assigned by the console registry until BrewFS gains a persistent volume format record.

### Runtime Instances

- `GET /api/instances`
  - Lists live mount instances from `RuntimeRegistry`.
- `GET /api/instances/{pid}`
  - Returns `GetInfo`, capabilities, and runtime record details.
- `POST /api/instances/{pid}/jobs/gc`
  - Calls existing `RunGc`.
- `GET /api/instances/{pid}/jobs/{job_id}`
  - Calls existing `GetJob`.

Future control-plane additions:

- `GET /api/instances/{pid}/stats`
- `POST /api/instances/{pid}/jobs/compact`
- `POST /api/instances/{pid}/jobs/fsck`
- `POST /api/instances/{pid}/jobs/warmup`
- `POST /api/instances/{pid}/jobs/{job_id}/cancel`

### File Browser

- `GET /api/volumes/{volume_id}/files?path=/dir`
  - Lists directory entries with name, inode, type, size, mode, uid, gid, mtime, and ACL/trash flags when available.
- `GET /api/volumes/{volume_id}/files/stat?path=/dir/file`
  - Returns file metadata.
- `GET /api/volumes/{volume_id}/files/readlink?path=/link`
  - Returns symlink target.

MVP should be metadata-first. Downloading file content is out of scope. Subsequent versions may add small-file preview behind a size limit and explicit config.

### Trash

- `GET /api/volumes/{volume_id}/trash`
  - Returns trash entries if the volume format and metadata backend support trash.
- `POST /api/volumes/{volume_id}/trash/{entry_id}/restore`
- `DELETE /api/volumes/{volume_id}/trash/{entry_id}`

Before BrewFS implements trash, these endpoints must return `unsupported` with a clear message. The UI should show an empty disabled state, not a broken page.

### ACL

- `GET /api/volumes/{volume_id}/acl?path=/dir/file`
  - Returns ACL entries and inherited/default ACL state if supported.
- `PUT /api/volumes/{volume_id}/acl?path=/dir/file`
  - Replaces ACL entries atomically.
- `DELETE /api/volumes/{volume_id}/acl?path=/dir/file`
  - Clears extended ACL and falls back to mode bits.

The first ACL model should be POSIX ACL oriented:

```json
{
  "entries": [
    { "scope": "access", "tag": "user_obj", "perm": "rwx" },
    { "scope": "access", "tag": "group_obj", "perm": "r-x" },
    { "scope": "access", "tag": "other", "perm": "---" },
    { "scope": "access", "tag": "user", "id": 1001, "perm": "rw-" }
  ]
}
```

If the backend capability reports `acl=false`, writes must be rejected with `unsupported`.

### CSI Dashboard

CSI support should be an optional integration:

- `GET /api/csi/summary`
  - Counts StorageClasses, PVs, PVCs, pods using BrewFS volumes, and unhealthy mounts.
- `GET /api/csi/storageclasses`
- `GET /api/csi/persistentvolumes`
- `GET /api/csi/persistentvolumeclaims?namespace={namespace}`
- `GET /api/csi/pods?namespace={namespace}&volume={volume_name}`

The CSI adapter should discover BrewFS resources by CSI driver name, labels, annotations, or StorageClass provisioner configured in `console.csi.driver_name`. It should never mutate Kubernetes resources in the MVP.

## UI

The first screen should be the console workspace, not a marketing page.

Primary navigation:

- Overview
- Filesystems
- File Browser
- Trash
- ACL
- Jobs
- CSI
- Settings

Overview should show:

- Registered volumes
- Live mounts
- Recent jobs
- Backend capability warnings
- CSI health summary when enabled

Filesystems should support:

- Create filesystem registry entry
- Inspect config and runtime state
- See data/meta backend type
- See capability matrix
- Start/stop commands as copyable shell snippets, not remote process control in MVP

File Browser should support:

- Breadcrumb path navigation
- Directory table
- Metadata side panel
- Disabled download/edit actions in MVP

Trash should support:

- Disabled state when unsupported
- Entry table and restore/delete actions when implemented

ACL should support:

- Path selector
- ACL table editor
- Backend capability warning
- Dry-run validation before saving when possible

CSI should support:

- Cluster summary
- StorageClass/PV/PVC tables
- Pod-to-volume mapping
- Warnings for missing node mounts or stale PVs

## Auth And Security

MVP auth should be simple and explicit:

- Bearer token from `--auth-token-file` or `BREWFS_CONSOLE_TOKEN`.
- `--dev-no-auth` allowed only with loopback listeners.
- Redact secrets in all API responses: S3 access keys, Redis passwords, database passwords, session tokens.
- Bind to loopback by default.
- Require explicit `--listen 0.0.0.0:{port}` for network exposure.
- Log mutating operations with user identity when auth is enabled.

Future work:

- TLS config.
- OIDC.
- Role-based access control.
- Audit log export.

## Data Model

### Console Volume Record

```json
{
  "id": "uuid-v7",
  "name": "dev-local",
  "description": "Local development filesystem",
  "labels": { "env": "dev" },
  "created_at": "2026-06-11T00:00:00Z",
  "updated_at": "2026-06-11T00:00:00Z",
  "mount_config": {
    "mount_point": "/mnt/brewfs",
    "data_backend": "local-fs",
    "data_dir": "/var/lib/brewfs/data",
    "meta_backend": "sqlx",
    "meta_url_redacted": "sqlite:///var/lib/brewfs/meta.db",
    "chunk_size": 67108864,
    "block_size": 4194304
  }
}
```

This registry is a console convenience layer. It is not a substitute for the future BrewFS volume format record. When volume format lands, the console should store only references and display metadata loaded from the volume itself.

## Capability Handling

The console must use backend capabilities to decide whether actions are enabled.

Initial capability sources:

- `ControlResponse::Info.capabilities` for live instances.
- Registry config for offline volumes, marked as "unknown until mounted".

Expected UI behavior:

- Supported: action enabled.
- Unsupported: action disabled with explanation.
- Unknown: action visible but requires a live mount.

This is important for ACL and trash because BrewFS currently has interfaces and partial implementations, not a complete product contract across every backend.

## Error Handling

- Invalid config returns `400 invalid_config`.
- Auth failure returns `401 unauthorized`.
- Missing volume returns `404 not_found`.
- Live instance unavailable returns `409 instance_unavailable`.
- Unsupported backend capability returns `422 unsupported`.
- Control-plane failures return `502 control_plane_error`.
- Kubernetes API failures return `502 kubernetes_error`.

All errors should include a stable `code` and a human-readable `message`.

## Testing

Unit tests:

- Volume registry create/list/update/delete.
- Secret redaction.
- Capability-to-action mapping.
- API error envelope serialization.
- CSI resource classifier using fixture objects.

Integration tests:

- Start `brewfs console` on an ephemeral port.
- Query `/api/health`.
- Register a local SQLite/local-fs volume.
- Start a mounted instance and verify `/api/instances` discovers it.
- Call `info` and `gc --dry-run` through the console API.

UI tests:

- Overview renders with no volumes.
- Filesystems table renders registered volume.
- Trash and ACL pages render unsupported states.
- CSI page renders disabled state when not configured.

Manual smoke:

```bash
cargo run -p brewfs -- console --listen 127.0.0.1:18080 --dev-no-auth
curl http://127.0.0.1:18080/api/health
curl http://127.0.0.1:18080/api/volumes
```

## Implementation Phases

### Phase 1: Console API Skeleton

- Add `brewfs console`.
- Serve `/api/health`.
- Implement token auth.
- Implement volume registry with redaction.
- Add tests for registry and auth.

For the first scaffold pass, split Phase 1 into two commits or tasks:

- Phase 1A: `brewfs console`, `/api/health`, API token auth, static asset serving, and the Vite React shell.
- Phase 1B: volume registry with redaction.

### Phase 2: Runtime Integration

- Expose `/api/instances`.
- Reuse existing `RuntimeRegistry`.
- Call `GetInfo`, `RunGc`, and `GetJob` through existing control-plane client.
- Add integration tests with a local mounted instance where available.

### Phase 3: Web UI MVP

- Serve static UI assets from the binary or a packaged asset directory.
- Implement Overview, Filesystems, Jobs, and Settings pages.
- Keep UI restrained and operational, optimized for repeated admin use.

### Phase 4: File Browser And Capability-Aware Pages

- Add file listing/stat control-plane requests or a safe server-side VFS reader.
- Implement File Browser.
- Implement Trash and ACL pages with unsupported states first.
- Wire ACL actions only after backend capability and semantics are verified.

### Phase 5: CSI Dashboard

- Add optional Kubernetes client adapter.
- Implement read-only CSI summary and resource tables.
- Add fixture-based tests.

## MVP Decisions

- Filesystem creation creates a console registry entry and a copyable `brewfs mount` command. It does not remotely start mount processes in the MVP.
- File browsing requires a live mounted instance and should go through new instance control-plane requests. Offline VFS construction is out of scope for the MVP.
- CSI discovery defaults to driver name `csi.brewfs.io` and can be overridden with `console.csi.driver_name`.
- ACL uses the POSIX ACL-oriented JSON model in this spec. A BrewFS-specific ACL model is out of scope for the MVP.

## Acceptance Criteria

- A user can start `brewfs console` and open a browser to see BrewFS status.
- A user can create a filesystem registry entry without exposing secrets in API responses.
- A user can see live mounted instances discovered from the runtime registry.
- A user can run existing GC dry-run through the console API.
- Unsupported trash and ACL states are explicit and stable.
- CSI dashboard can be enabled without affecting non-Kubernetes deployments.
- Tests cover registry behavior, auth, redaction, runtime discovery, and unsupported capability handling.
