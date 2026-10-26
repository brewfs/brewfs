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
| `workspace init-volume`, `init-packed-volume`, `create`, `fork`, `migrate` | `src/cli.rs` (`run_workspace_command`, `init_packed_volume`), `lifecycle/mod.rs` (`fork_revision_inner`) | New IDs do not edit an existing mounted namespace. Initial packed installation is guarded. Migration checks the catalog format. No mount broadcast. | No file metadata changes in an existing mounted view; workspace and binding identity are revalidated at expiry. Arbitrary incompatible schema edits are outside the supported protocol. |
| Snapshot creation/deletion through the catalog | `stores/kv_store.rs:3104`, `:3158`; `stores/database.rs:1759`, `:1835` | Changes retention roots, not the contents of an existing mounted view. No mount broadcast. | Reachability/reader pins protect mounted views; GC uses the protection below. |
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
  Extent append dispatch is `stores/kv_store.rs:4111` and delegates to the
  versioned data mutation path.
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
  Packed collector dispatch is
  `stores/kv_store/packed_journal/registry/admin_gc.rs:131`; durable history
  deletion checks and queue consumption are in
  `stores/kv_store/packed_journal/registry/history_retirement.rs:251`, `:1032`.
  Packed deletion reservation, dispatch, owned-object deletion and physical
  deletion are in `stores/kv_store/packed_journal/registry/collector.rs:939`,
  `:991`, `:1120`, `:1141`; retiring-object collection starts at `:1150`.
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

Medians of three trials, TTL **0 versus 1000 ms**, October 26, 2026. All rates
are higher-is-better. These are focused diagnostic results, not an accepted
replacement for the campaign's release/compose measurements.

| Workload | TTL 0 | TTL 1000 | On/off |
|---|---:|---:|---:|
| Warm stat, 1 process (ops/s) | 226.00 | 132,903.64 | 588.07x |
| Warm stat, 8 processes (aggregate ops/s) | 522.28 | 2,393,385.32 | 4582.54x |
| Warm create (ops/s) | 57.70 | 68.57 | 1.188x |
| Warm unlink (ops/s) | 74.90 | 96.75 | 1.292x |
| `seq_read_warm_MBps` | 740.52 | 4739.74 | 6.401x |
| `seq_write_MBps` | 121.32 | 127.51 | 1.051x |

The same **unoptimized dev binary** was used for both arms on a 32-logical-CPU,
31 GiB host, Linux 6.8.0-36, Redis 7.2 and RustFS `ceddcdd878d2`. This is not the
8-vCPU campaign host/profile. Binary SHA256:
`70610b43234c3bacfc9d41175310ffe19cea893f89647eb476bb643b75f647a9`.
The packed-v3 workspace used no compression, 300 MiB memory and 1 GiB SSD cache,
64 MiB chunks and 4 MiB blocks. Off ran before on; neither arm overlapped a
compiler or test suite. Both mounts unmounted and drained cleanly.

The harness asserts `mountpoint -q` before **every timed run**, and again after
each trace. It rejects a running compiler. Saved mountinfo identifies actual
`fuse brewfs` mounts. BPF counts completed requests at `fuse_request_end`, scoped
to the mount device and bracketed by syscall markers around the timed action.
All 36 BPF windows were checked against the raw measurement rows. Existing
GETATTR/CREATE/UNLINK `.stats` fields are not incremented in main and were not
used as evidence.

Counts below are medians of three; L/G means LOOKUP/GETATTR. Sequential counts
are per **256 MiB run**; metadata counts are per syscall/operation.

| Workload | L/G off | L/G on | Other FUSE requests and kernel eligibility |
|---|---:|---:|---|
| stat, 1 process | 2 / 3 | 0.004883 / 0 | Kernel metadata cache can serve on; off crosses FUSE every time. |
| stat, 8 processes | 2 / 3 | 0 / 0 | All three on trials produced zero FUSE requests. |
| create | 2 / 4 | 1.015625 / 1.996094 | CREATE 1, FLUSH 1, RELEASE 255/256 in both arms; cannot be completed by kernel cache. |
| unlink | 2 / 3 | 1.011719 / 1 | UNLINK 1 in both arms; cannot be completed by kernel cache. |
| sequential warm read | 0 / 257 | 0 / 0 | READ 0 in both arms; page cache serves all data. On GETATTR counts were 0, 1, 0. |
| sequential write | 2 / 4 | 2 / 1 | WRITE 256, SETXATTR 257, FSYNC 1, FLUSH 1 in both arms; buffered writes include fsync/close and require FUSE. |

Stat uses a warmed 256-file corpus, 8,192 calls per process. Paths have two FUSE
components, explaining two baseline LOOKUPs. Enabled single-process bursts
lasted 16.6–63.7 ms and issued 69, 0, 40 LOOKUPs; eight-process bursts lasted
25.5–34.0 ms. These demonstrate kernel hits within a TTL, not sustained rates
amortizing many expiry cycles. One baseline eight-process trial contained extra
mount traffic (263 LOOKUPs, 531 GETATTRs and directory/xattr operations); its
source was not attributed. It is retained in the raw data, and the other two
trials determine the median request counts.

Create/unlink each use 256 warmed names. Sequential runs transfer 256 MiB with
1 MiB syscalls; MB/s uses decimal MB. Warm read times read syscalls on the same
warmed descriptor, so it does not claim cross-open page-cache retention.
Write timing includes open, writes, fsync and close. No measured guardrail
regressed, but three ordered dev-profile trials do not establish production
performance acceptance.

Raw measurements, BPF programs/output, environment, mountinfo, and selected
correctness output are preserved in
[`workspace-metadata-cache-20261026/`](workspace-metadata-cache-20261026/).
Full local logs remain under `/data/metacache/artifacts`.

The two-mount integration fixture uses independent FUSE connections/resolvers
with one deliberately shared test lease and a SQLite native upper. Ordinary CLI
mounts cannot acquire two concurrent independent leases. It tests chmod, size,
rename/unlink visibility, unchanged-mode ACL revocation and a kernel-hit stat
loop. This does not establish support for concurrent independent writer leases.

Verification:

| Check | Result |
|---|---|
| Workspace build with `workspace-overlay`, `-j 2`, both debug profiles 0 | Pass |
| Focused `fuse_cache_ttl` tests | 3 passed; mounted test run separately |
| Packed upper-row expiry and independent binding regression | Pass; external size change replaces a stale daemon row after expiry; revoked binding rejects refill |
| Two FUSE mounts, same test lease, TTL 1000 | Pass; 1000 warm stats added no LOOKUPs; chmod/truncate visible after 0.879705 s; same-mode named-user ACL revocation after 0.996197 s; rename/unlink visible after expiry |
| `generic/014` | Pass, 233 s |
| `generic/087` | Pass, 1 s |
| `generic/097` | Pass, 2 s |
| `generic/100` | Pass, 221 s |
| BPF calibration, TTL 0, 10 stat calls | 10 LOOKUP + 20 GETATTR; throughput deliberately not measured during compilation |

xfstests used the project container, a fresh packed-v3 workspace on Redis DB 5,
local-fs objects, and TTL 1000 for every mount/remount. It was correctness-only
and ran during the test build. The first attempt executed **no tests** because
the image's Docker VOLUME at `/mnt/brewfs` was already a mount; the successful
run used `/mnt/metacache`. No result from the failed setup is counted as a pass.
The performance fixture uses a separate Redis DB 4 / RustFS S3 bucket.

The full `cargo test --workspace --lib --bins --features workspace-overlay`
command did not finish: compilation was stopped to preserve the requested
measurement window. Its completed library executable is being run separately;
this does not replace the full workspace gate. Full clippy/check/runtime-feature
gates, release compose acceptance (including mixed read/write), real TiKV/etcd,
and concurrent packed publication/GC against a cached kernel mount are **not
verified**. Keep this PR draft and the feature opt-in until those gates pass.
