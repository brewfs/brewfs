# Workspace kernel metadata cache: bounded refresh

This change addresses the TTL-only behavior in #349. Default TTLs are unchanged.
The explicit option now requires an authoritative workspace refresh, and all
LOOKUP/GETATTR cache fills share its absolute expiry. Mutation replies and
READDIRPLUS grant no positive writable-workspace TTL. There is no new backend
wire protocol, FUSE reply ABI, or INIT size change.

## Writer inventory

Line references below are to this revision. These are supported transactional
writers; arbitrary edits to backend rows or immutable object bytes are not a
supported mutation interface.

| Mutation | Writer paths | Existing notification and protection | Positive-TTL mechanism |
|---|---|---|---|
| Another workspace client: create, unlink, rename, link, mkdir, chmod/chown/ACL/xattrs, truncate, data size/timestamps | `src/workspace_overlay/meta_layer/packed_permissions.rs` (`commit_with_written`), `stores/kv_store.rs` (`apply_versioned_mutation`, `apply_packed_versioned_mutation`, legacy `apply_*_mutation`), `stores/database.rs` (same methods) | No remote notification. Transactions authenticate head, lease and holder, and advance the head sequence. Local commits invalidate affected upper rows. | Refresh validates authority and compares both layer records; a changed sequence clears daemon rows. Kernel cache entries expire at the existing checked deadline. |
| A second CLI mount / expired lease takeover / explicit lease release | `stores/kv_store.rs` (`acquire_lease`, `release_lease`), `stores/database.rs` (same), `lifecycle/mod.rs` (`WorkspaceMountSession`) | A second independent live writer lease is rejected. No notification on takeover/release. A local heartbeat alone cannot prove remote force-fencing did not occur. | The remote lease/holder check fails after deadline expiry. The old mount fails closed; adopting another holder requires remount. |
| `workspace init-volume`, `init-packed-volume`, `create`, `fork`, `migrate` | `src/cli.rs` (`run_workspace_command`, `init_packed_volume`), `lifecycle/mod.rs` (`fork_revision_inner`) | New IDs do not edit an existing mounted namespace. Initial packed installation is guarded. Migration checks the catalog format. No mount broadcast. | Existing workspace and binding identity are revalidated at expiry; incompatible catalog changes fail closed. |
| `workspace discard --force` | `src/cli.rs` (`WorkspaceCommand::Discard`), `lifecycle/mod.rs` (`discard`), `stores/kv_store.rs` and `stores/database.rs` (`mark_workspace_deleting`) | Non-force discard refuses a live lease. Force marks the workspace/head deleting and releases/fences the lease. **No notification to the mounted kernel.** | Cached attributes can survive only the remaining checked TTL; the next refresh returns an error. |
| Packed lower installation/publication or mounted recovery | `stores/kv_store.rs` / `stores/database.rs` (`install_packed_lower_binding`, `publish_packed_lower_binding`); `src/cli/packed_mount_recovery.rs`; `stores/kv_store/packed_admin/packed_mounted_recovery.rs` | Independent binding/head/lease CAS; immutable packed memos alone cannot detect replacement. **No mount invalidation broadcast.** | Refresh authenticates the exact attached binding through `WorkspacePackedBindingAuthority::validate`; changed binding or holder fences the old view. |
| Packed mount / reader retention renewal | `stores/kv_store/packed_admin/packed_mounted_session.rs` (`renew_owned`), `packed_reader_lifecycle.rs` (`validate`), `lifecycle/mod.rs` (heartbeat) | Renewal extends authority/retention; it does not mutate immutable lower file metadata. Failed renewal revokes local proofs. No kernel broadcast. | Each new cache interval rechecks remote authority and packed reader/binding. Renewal cannot slide a previously granted kernel deadline. |
| Workspace and packed/native GC | `src/workspace_overlay/gc.rs` (`run_selected_at`), `stores/kv_store.rs` (`delete_layer_metadata`, `finalize_layer_metadata_deletion`), `stores/database.rs` (`delete_layer_metadata`), `stores/kv_store/native_finalization.rs` and packed deletion reservations | Live workspace roots, leases, packed reader pins and durable owners protect reachable metadata/objects. Deletion rechecks roots under CAS. No inode/dentry broadcast. | Live reachable views must survive GC. A discarded/fenced view is rejected by refresh. Metadata TTL does not authorize reads of reclaimed data. |
| S3/WebDAV or SDK mutations through the same workspace VFS | `src/gateway/s3/store.rs` (rename/unlink/xattr), `src/gateway/webdav/fs.rs` (rename/unlink/setattr), ordinary VFS metadata commits | Same transactional generation changes; local upper-row invalidation. No general remote broadcast. | The shared deadline bounds kernel staleness even when no local FUSE namespace request updates the kernel. |

Numbered writer references (all under `src/workspace_overlay/` unless qualified):

- Upper commits: `meta_layer/packed_permissions.rs:248`; KV permission,
  versioned, packed, namespace, inode, data, xattr and ACL writers at
  `stores/kv_store.rs:3680`, `:3779`, `:3854`, `:4041`, `:4093`, `:4141`,
  `:4203`, `:4225`; SQLite equivalents at `stores/database.rs:2516`, `:2533`,
  `:2803`, `:2864`, `:2928`, `:3014`, `:3038`.
- Lease acquire/renew/release/reap: `stores/kv_store.rs:3183`, `:3276`, `:3366`,
  `:3418`; SQLite `stores/database.rs:1848`, `:2006`, `:2042`, `:2074`.
  The live-lease exclusion is `stores/kv_store.rs:3221`; the writer guard is
  `stores/kv_store.rs:5458`.
- CLI initialization, migration, create, fork and discard:
  `src/cli.rs:2640`, `:2658`, `:2670`, `:2674`, `:2695`, `:2733`.
  Fork/discard orchestration: `lifecycle/mod.rs:63`, `:85`. Forced deletion:
  `stores/kv_store.rs:4270`, `stores/database.rs:3089`.
- Packed installation/publication: `stores/kv_store.rs:2562`, `:2674`;
  `stores/database.rs:1096`, `:1197`; CLI install `src/cli.rs:2601`.
  Recovery: `stores/kv_store/packed_admin/packed_mounted_recovery.rs:461`.
  Mounted renewal: `stores/kv_store/packed_admin/packed_mounted_session.rs:822`;
  reader authority: `packed_reader_lifecycle.rs:442`.
- GC entry/reachability walk: `gc.rs:78`; metadata deletion/finalization:
  `stores/kv_store.rs:4471`, `:4542`, `stores/database.rs:3320`, `:3460`;
  native finalization `stores/kv_store/native_finalization.rs:206`.
  Reader reaping: `stores/kv_store.rs:2210`.
- Gateway writers: `src/gateway/s3/store.rs:457` (rename), `:541` (unlink),
  `:263` (xattr); `src/gateway/webdav/fs.rs:609` (rename), `:602` (unlink),
  `:706` (attributes).

This inventory does not claim that a notification channel exists merely because
asyncfuse can send INVAL_ENTRY.

## Freshness contract and limitations

The interval begins before remote validation, not when each file is looked up.
A mount-wide mutex coalesces refreshes. Local commits keep their existing key
invalidation, and cache tickets reject inserts racing invalidation. At expiry,
both layer records and the writer guard are checked; the exact packed binding
is checked separately. A failure never extends the expired deadline. A slow
metadata read receives only the remaining interval, possibly zero.

Kernel hits can use old mode/ownership/ACL-related attributes until expiry.
FUSE reply delivery delay is additional to its relative TTL; this is not a
hard real-time revocation protocol. Immediate revocation would require a writer
broadcast with acknowledged cache revocation and ordering against in-flight
replies. The feature remains opt-in. Raw backend edits without generation bumps,
coherent shared dirty writeback under copied lease credentials, and cached file
data are not made safe by this metadata change.

Linux 6.8 clears cached POSIX ACLs on attribute expiry, including permission
refresh, even when the new mode bits equal the old ones:
[`fs/fuse/dir.c:1310`](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dir.c#L1310)
and [`:1479`](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dir.c#L1479).
The mounted regression includes named-user ACL revocation with unchanged mode.

## Measurement and verification

Pending. Required rows: warm stat (one/eight processes), create, unlink,
seq_read_warm_MBps, seq_write_MBps; TTL off/on, three trials and FUSE opcode
counts. The harness asserts `mountpoint -q` before each timed run. BPF counts
completed requests at `fuse_request_end`, scoped to the mount device and timed
window. Existing GETATTR/CREATE/UNLINK `.stats` fields are not incremented in
main and cannot validate these counts.

The requested generic/087, generic/097, generic/014, generic/100 and explicit
two-mount test will be reported individually. The two-mount integration fixture
uses independent FUSE connections/resolvers with one deliberately shared test
lease; ordinary CLI mounts cannot acquire two concurrent independent leases.
It tests chmod, size, rename and unlink visibility, plus a kernel-hit stat loop.
It must not be presented as support for concurrent independent writer leases.
