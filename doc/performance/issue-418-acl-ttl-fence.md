# Issue #418: writable ACL TTL fence audit

Audit base: `fb03c2363d813aed9fccc68855573616f576dc6a`, 2026-11-04.
Implementation branch: `fix/issue-418-strict-ttl`. The audit examined observation
by client B immediately after client A's completed mutation, without waiting
for TTL expiry. The #417 TTL attribution is accepted as given and is not rerun.

## Decision and cache contract (B)

The existing fence provides **bounded refresh**, not immediate revocation.
Adding generation increments cannot make a kernel cache hit contact the daemon.
The missing part is a cross-client revocation protocol with acknowledgement and
ordering against replies already in flight. That protocol is design issue
[#419](https://github.com/brewfs/brewfs/issues/419), outside this change.

The accepted [#418 B decision](https://github.com/brewfs/brewfs/issues/418#issuecomment-6621314154)
**retains all existing defaults** and fixes the environment override. This
supersedes the audit's earlier proposal to change the workspace default to zero.
There are three distinct policies in the audited revision:

* `permission_cache_ttl()` and an unconfigured VFS use zero for writable ACLs.
* `MountConfig::from_sources()` already defaults **WorkspaceV1 to 1000 ms**
  (introduced by `e2288fb4`). The CLI installs it in the VFS; LOOKUP/GETATTR
  consequently return positive TTLs. Other volume formats retain a zero
  configuration default.
* Non-writable-ACL implementations use the legacy one-second environment
  policy independently of the per-mount writable-ACL setting. Before this fix,
  `BREWFS_CACHE_TTL_MS=0` affected that policy only and could not override the
  WorkspaceV1 configuration default.

**Contract:** attr/entry caching has bounded-staleness semantics (cache lifetime
≤ TTL); it does not promise immediate cross-client visibility. For strict
kernel metadata caching, use `--fuse-cache-ttl-ms 0` or
`BREWFS_CACHE_TTL_MS=0`. Zero TTL prevents positive LOOKUP/GETATTR cache fills
on writable ACL mounts. It does not implement acknowledged remote revocation,
order in-flight replies, or strengthen upstream daemon caches and unpublished
writeback visibility. Relative FUSE TTLs start when the kernel consumes the
reply; the audit below records the delayed-reply gap for #419.

Precedence, from highest to lowest:

| Source | Value / behavior |
| --- | --- |
| Explicit CLI `--fuse-cache-ttl-ms N` | `N`, including zero |
| Explicit YAML `fuse.cache_ttl_ms` | Existing explicit-configuration precedence is preserved |
| `BREWFS_CACHE_TTL_MS` | Parsed unsigned milliseconds, including zero |
| Volume-format default | WorkspaceV1: **1000 ms**; all others: **0**, unchanged |

Thus with no explicit YAML, CLI > environment > volume default. An explicit
YAML value still wins over the environment; use CLI zero to override it.
Missing, non-Unicode, invalid, negative or overflowing environment values fall
back to the format default, matching the legacy parser's unsigned-integer
behavior. Legacy reply TTLs and all mutation/READDIRPLUS policies are unchanged.

The Compose perf runner also writes explicit `fuse.cache_ttl_ms: 1000` for
workspace-v1 when generating its worker configuration
(`docker/compose-xfstests/run_perf_in_container.sh:456`). That is an explicit
benchmark configuration, not evidence of the product default. The benchmark
and scanner are not modified. No new performance claim is made by this fix.

## Reply-path inventory

Line numbers in this table refer to the audit base. “Safe positive TTL” means
safe for the requested immediate cross-client contract, not just bounded
staleness. A zero-TTL reply is conservative, but does not itself prove freshness
of every upstream daemon cache or remote dirty write.

| Reply path in `src/fuse/mod.rs` | Current generation/authority check | Current writable ACL TTL | Safe positive TTL? | Missing protection / invalidation |
| --- | --- | --- | --- | --- |
| LOOKUP ordinary child, 751 / 3794–3831 | `fenced_metadata_read` checks workspace/layers/lease/binding only when the shared deadline expires | Remaining configured interval | No | Remote mutation/revocation cannot evict B's cached dentry or attr; no reader registration, writer broadcast or acknowledgement |
| LOOKUP real root `.stats` entry, 3752–3764 | Same wrapper as an ordinary entry | Remaining configured interval | No | Same as ordinary LOOKUP; a real `.stats` file is mutable |
| LOOKUP virtual `.stats`, 3767–3791 | Wrapper can check authority, but excludes this inode from positive TTL | 0 | No | Snapshot length/times change; a real entry can later shadow the synthetic name |
| GETATTR linked inode, 1079 / 3871–3908 | Same shared-deadline wrapper | Remaining configured interval | No | Remote write/mtime/chmod/chown/truncate/ACL changes cannot revoke cached attrs |
| GETATTR unlinked open-handle fallback (`fh` or inode handle), 3885–3898 | Same wrapper; handle attrs are local fallback only after genuine absence | Remaining configured interval | No | Read fence does not serialize changing handle attrs, writes or response delivery |
| GETATTR virtual `.stats`, 3842–3866 | Bypasses wrapper | 0 | No | Dynamic stats snapshot |
| SETATTR empty request, 1135–1145 | No cache-fill generation fence | 0 | No | Read can race a mutation; remote attrs/authority can change after reply |
| SETATTR mode/uid/gid/times, 1206–1264 | Mutation authorization/CAS; not a read/reply revocation fence | 0 | No | Local inode notification only, queued without kernel acknowledgement; no remote broadcast or stale-reply exclusion |
| SETATTR size/truncate, 1254–1264 | Mutation authorization/CAS; explicit zero-TTL size exception | 0 | No | Same remote gap; retain size/page-cache protection independently of permission policy |
| READDIRPLUS `.` / `..` (including offset 1), 1464–1511 | No shared-deadline or snapshot validation across reads | Both 0 | No | Directory handle entries and separately fetched attrs may span mutations; stream delivery adds another race |
| READDIRPLUS children from a directory handle, 1516–1557 | No cache-fill fence; per-child stat after handle enumeration | Both 0 | No | Validate directory snapshot and all child attrs, then order stream delivery/revocation; revoke affected names remotely |
| READDIRPLUS stateless fallback children, 1519–1557 | No cache-fill fence; enumeration and stat are separate | Both 0 | No | Same namespace/attr consistency and remote revocation gaps |
| MKNOD writable ACL branch, 3121–3157 | Atomic creation/permissions, no cache-fill fence | 0 | No | New inode can immediately change/unlink/rename; revoke parent/name and attrs on every reader |
| MKDIR writable ACL branch, 3269–3296 | Atomic creation/permissions, no cache-fill fence | 0 | No | Parent/name, directory metadata, traversal permissions and remote revocation |
| CREATE new/existing/O_TRUNC, 3346–3456 | Creation/open/truncate authorization; no positive cache-fill fence | 0 | No | Existing mandatory size-zero exception, later writes, handle state and remote mutation |
| LINK, 3460–3527 | Transactional namespace change, no cache-fill fence | 0 | No | Existing aliases/nlink/ctime and new parent/name need revocation on every reader |
| SYMLINK, 3530–3578 | Transactional creation, no cache-fill fence | 0 | No | Target bytes are immutable but ownership/times/name/authority remain mutable |
| MKNOD/MKDIR non-writable-ACL branches, 3254–3258 / 3326–3330 | No workspace fence; existing legacy policy | Legacy policy | Not newly certified | Preserve existing behavior; mutable legacy backends are not an immediate-coherency proof |

The actor/umask wrapper callbacks delegate to the listed helpers and add no
reply TTL. UNLINK/RMDIR/RENAME return `()`; WRITE returns only the byte count;
OPEN/OPENDIR return handles; READDIR returns names without attrs. These cannot
fill attr/entry TTLs themselves. Lookup errors return errno, not a cached
negative-entry reply. Experimental `src/fuse/entity.rs` uses zero for all its
attr/entry/created/readdirplus replies and has no positive-TTL path to open here.

`ReplyEntry.generation` is zero in these paths. It identifies an inode
incarnation for the FUSE protocol; it is **not** a metadata version that the
kernel polls. `ReplyAttr` has no generation field. Changing either the backend
sequence or the entry generation cannot revoke an already cached attribute.

## Mutation and authority audit

The supported workspace transactions already allocate monotonically increasing
layer sequences. `commit_with_written` invalidates local rows before and after
the transaction (`meta_layer/packed_permissions.rs:248`); versioned mutation
implementations allocate sequences in both `stores/database.rs` and
`stores/kv_store.rs`. Namespace, inode, data, xattr and ACL operations use those
transaction paths. Lease/head/binding authority has separate validation.

| Change | Existing protection | What B's kernel lacks |
| --- | --- | --- |
| Published data write / size / mtime | Versioned inode/data transaction; local dirty/handle state | Notification after publication to all cached readers; definition and fencing of unpublished writeback visibility |
| chmod/chown / POSIX or control ACL / xattrs | Atomic permissions transaction and layer sequence; local inode invalidation | Acknowledged remote attr/ACL revocation before mutation completes; ancestor traversal revocation too |
| truncate / fallocate / copy | Versioned metadata plus local data/handle invalidation | Remote attr/data revocation and ordering with in-flight size replies |
| create/link/unlink/mkdir/rmdir/rename | Versioned namespace transaction and local row invalidation; initiating kernel processes its own operation | Old and new parent/name revocation, victim/alias attrs, and parent metadata on other mounts |
| Forced discard, lease release/takeover, packed binding replacement | Remote authority checks at next shared-deadline refresh | Revocation while a reader uses kernel hits, including stalled or disconnected readers |

`kernel_cache_deadline()` explicitly reuses a live deadline without rereading
the remote generation (`meta_layer/kernel_cache.rs:30–40`). At expiry it checks
the layer pair, writer authority and packed binding, and drops stale daemon
rows. `fenced_metadata_read()` obtains that deadline **before** the read; it
does not lock mutations through kernel response consumption. Relative FUSE
TTLs also start on kernel consumption, so a delayed reply can outlive the
daemon's absolute deadline.

`notify_kernel_invalidate_inode_all_and_wait()` is misleadingly named: it calls
`Notify::invalid_inode()`, which only enqueues. Vendor asyncfuse also offers
`try_invalid_inode` / `try_invalid_entry` with kernel completion, but VFS does
not use them for this policy. There is no remote subscription/broadcast, and
the VFS has no namespace `INVAL_ENTRY` call site. Awaiting entry invalidation
inside a related request can deadlock on the request's own kernel locks; simply
switching to the acknowledged API is not a fix. See the existing `generic/075`
teardown limitation in `AGENTS.md`.

## Design input for #419

The following gaps were also posted as a
[#419 design-input comment](https://github.com/brewfs/brewfs/issues/419#issuecomment-6621386772).

| Gap | Required design input |
| --- | --- |
| LOOKUP/GETATTR kernel hits bypass daemon checks | Reader registration, writer broadcast and acknowledged inode/attr/ACL revocation |
| Namespace mutations and READDIRPLUS snapshots | Revoke old/new parent/name, aliases, victims and parent metadata; validate directory enumeration and child attrs together |
| Published writes, truncate, handle attrs and dirty writeback | Define publication visibility and order data/size replies against revocation |
| In-flight replies and relative TTLs | Exclude stale replies after ACK; a backend sequence or FUSE inode generation alone cannot do this |
| Lease release/takeover, forced discard and packed binding changes | Define authority revocation and stalled/disconnected-reader behavior |
| Notification completion and kernel locks | Queueing is not ACK; send acknowledged invalidations outside related handlers and verify teardown/D-state behavior |

Reuse existing mutation sequences as protocol identifiers. No additional
writable-ACL reply path is certified for immediate cross-client visibility by
the current fence. A read-only handle, symlink, cached lower inode or exclusive
writer lease does not make a writable view immutable. Fixed-manifest immutable
views can retain their legacy caching, without promising immediate forced
authority revocation.

Strict mode here means **no positive kernel metadata TTL**. It does not change
the existing zero-TTL path's authority checks, copied-lease support, local dirty
handles, or packed upper-row cache contract. In particular, the existing test
`cache_fill_rejects_released_lease_but_default_reply_is_unchanged` distinguishes
positive-TTL authority refresh from the zero-TTL path. The existing mounted
expiry fixture shares a lease between two SQLite resolvers; it is not proof of
a complete packed multi-client or immediate authority-revocation protocol.

## Verification and delivery status

Baseline `fb03c23` with the new regression tests: **7 passed, 3 failed, 1 existing
ignored**. Environment zero and 500 both resolved to 1000 ms; environment zero
also produced a positive LOOKUP TTL (about 996 ms). CLI/YAML precedence,
unconfigured defaults, invalid-value fallback and the original unit fixtures
passed. The raw failures are in `plan-b/baseline-tests.log`. The inherited
audit's remaining CLI compilation was interrupted after running the completed
regression-test executable and saving its failure log; that interrupted build
is not counted as a green gate.

Pending: candidate strict-mode tests, local release gates and final evidence.
All compiler invocations use
`CARGO_TARGET_DIR=/home/hxy/brewfs-pkv-target`, `--release`, `-j 3`, one Cargo
process at a time in tmux. No debug build or GitHub Actions acceptance is used.
Evidence goes under
`/mnt/brewfs-data/home/hxy/smallfile-warm/issue418-20261104/plan-b/`.

The five required cases are environment zero, environment 500 ms, CLI 300 ms
over environment 500 ms, unchanged defaults without either input, and zero-TTL
LOOKUP/GETATTR replies through the existing `tests/fuse_cache_ttl.rs` fixture.
Additional cases cover YAML precedence and invalid-environment fallback.
Environment-sensitive tests use separate processes so they do not race with
other tests or contaminate the legacy TTL `OnceLock`.

Immediate-revocation and performance acceptance are outside B's scope and are
not claimed. Any SQLite ACL/atime failures are recorded without altering,
ignoring or silently retrying those tests. Ready/merge remains conditional on
the requested local acceptance gates.
