# Attribute and entry cache design review

Design recorded on 2026-10-26, before implementation, on
`perf/attribute-cache-design`, cut from main
`3c7c580e04c4c9a606da4cc1bfaac4ad10794560` (PR #344).

**Result: stop at design review under the approved negative-result provision.**
The incremental TTL/polling or owner-notification change cannot establish the
required permission and takeover guarantees with the current interfaces.
No production cache change is accepted, and no after median is available.
This is not a claim that coherent caching is impossible: a larger owner and
cache-grant protocol is outlined below, with its costs and remaining proofs.

## Design and measurement contract

The desired cache contains size, timestamps, inode number and link count,
plus positive directory entries needed to resolve a repeated pathname without
FUSE LOOKUP. Permission decisions must continue to use coherent mode, UID,
GID and POSIX ACLs, including ancestor-directory search permissions and the
timestamp authorization introduced in PR #332.

Linux FUSE cannot give those fields independent cache lifetimes. Its one
`attr_valid` lifetime covers mode/UID/GID together with size/times/nlink.
Negotiating POSIX ACL support also enables kernel permission checks. Thus
"cache stat fields, keep permissions uncached" is not implementable using
the existing userspace FUSE reply ABI. Keeping `attr_valid=0` preserves fresh
permission checks but also retains the GETATTR round trip.

A candidate with positive attribute and entry lifetimes must therefore use
coherent cache grants for the complete attribute snapshot. The proposed
strict mechanism is owner-coordinated revocation: register every mount that
holds a grant, stop grants for a generation before a conflicting mutation,
deliver `FUSE_NOTIFY_INVAL_INODE` (offset -1, length 0, metadata and ACLs only)
and necessary `FUSE_NOTIFY_INVAL_ENTRY` messages, confirm their delivery to
the kernel, and order mutation completion and subsequent replies against
that revocation. Every writer, lease release and takeover must participate.
New inode/dentry installation and notification lock ordering need explicit
proof; the existing inode `attr_version` protection must be preserved.

The required permission coherency window is **0 ms of additional cache
staleness after a completed permission mutation**, subject to the ordinary
ordering of overlapping operations. A poll period or positive TTL is not a
permission-revocation barrier. A finite TTL can be a fallback for ordinary
metadata, but does not satisfy this strict permission contract by itself.
No positive production TTL or relaxed window is selected.

The current writable-ACL policy is retained after design review:
`attr_valid=entry_valid=0`. This is a kernel metadata-cache policy, not a
claim that data pages, existing file descriptors, or every backend operation
have zero staleness. Legacy `MetaClient`, which does not advertise writable
POSIX ACL support, already uses the existing default one-second TTL. That
legacy path is not the historical benchmark's column named "native": both
measured BrewFS columns use `workspace-v1` and Redis, so both are expected to
advertise writable ACLs and return zero TTL.

## Before/after medians

The following **before** numbers are independently recomputed medians of
three preserved runs of `/home/hxy/bench2.py`, rather than measurements of
this new branch. Implementation stopped at design review, so **after** is
not measured. The table does not assert unchanged throughput or a speedup.
An unsafe TTL experiment must not be reported as an accepted after result.

| Operation, ops/s | Native before | Native after | Overlay before | Overlay after |
| --- | ---: | ---: | ---: | ---: |
| Create, 1 thread | 417.2 | Not measured | 598.1 | Not measured |
| Stat, 1 thread | 937.1 | Not measured | 1,836.6 | Not measured |
| Unlink, 1 thread | 456.7 | Not measured | 850.6 | Not measured |
| Create, 8 threads | 738.6 | Not measured | 1,107.3 | Not measured |
| Stat, 8 threads | 2,910.7 | Not measured | 4,903.1 | Not measured |
| Unlink, 8 threads | 792.5 | Not measured | 1,666.7 | Not measured |

Stat acceptance thresholds are 1,874.2 / 5,821.4 ops/s for native and
3,673.2 / 9,806.2 ops/s for overlay (one/eight threads), with a visible
create/unlink improvement and all correctness gates passing.

Baseline artifacts:
`/home/hxy/spot/results/spot-bench-corrected-20261026-0436/verified/`.
Portable captures: [nine observations](doc/performance/attribute-cache-evidence/baseline-metadata-observations.tsv)
and [medians](doc/performance/attribute-cache-evidence/baseline-metadata-medians.tsv).
The audit checked all nine client/repetition TSVs, 99 in-workload
`mountpoint -q` successes, 36 corpus checks, 18 before/after mount snapshots
and nine mount-identity/write/fsync/list/readback probes. All 30 reported
medians, including the JuiceFS comparison, match their raw observations.

The historical BrewFS executable reports `d13365c`, dirty renewal branch;
its SHA-256 is
`9766dee2129250245b1ec94b03a3c188a999fc41bb2b3fe5ccec8c8237fa9fd5`.
The backend was colocated Redis + RustFS S3 on an 8-vCPU spot instance with
an empty authenticated packed lower for overlay. This is not a matched
performance comparison against current main.

Mount evidence does not count FUSE requests. On both measured writable-ACL
workspace configurations, zero TTL means repeated stat is expected to cross
FUSE. Do not confuse the native workspace in this comparison with the legacy
`MetaClient` path, which already has a one-second default TTL.
The workload creates 5,000 files, verifies them with a directory listing,
then stats each file once. Short TTLs may expire before that reuse. Any
accepted measurement must run the unchanged `bench2.py` workload three
times per binary and separately verify the repeated-stat request counts.

## Why the incremental candidate was rejected

### Permission caching cannot be separated from stat caching

[`permission_cache_ttl`](src/fuse/mod.rs) returns zero for writable ACLs.
[`ReplyAttr` and `ReplyEntry`](vendor/asyncfuse/src/raw/reply.rs) encode one
attribute lifetime; the entry reply also uses its TTL for the directory
entry. The Linux kernel uses the attribute lifetime for mode/UID/GID checks
as well as stat. Negotiating `FUSE_POSIX_ACL` forces `default_permissions`;
simply disabling the mount option cannot move these checks to the daemon.

Concrete counterexample: mount B caches mode 0755 on a directory. A completes
chmod to 0700 immediately after B's generation poll. Before the next poll or
invalidation, a nonowner on B can stat a cached child through that directory.
That operation does not ask the daemon to consult the generation.
Fresh checks in OPEN, WRITE or SETATTR do not protect `access(2)` checks
performed entirely in the kernel or cached pathname traversal. This
introduces a permission-revocation window
where the existing zero-TTL path would ask for current attributes.

Caching entries alone leaves GETATTR on repeated pathname stat; caching
attributes alone while leaving entries at zero leaves LOOKUP. Neither is
the requested VFS-only operation.

### An enqueued notification is not a revocation barrier

[`Notify::invalid_inode` and `invalid_entry`](vendor/asyncfuse/src/notify.rs)
enqueue bytes and discard enqueue errors. They do not return the result of
the eventual `/dev/fuse` write. The existing VFS helper whose name ends in
`_and_wait` therefore does not prove kernel delivery. The
[`reply_fuse` transport](vendor/asyncfuse/src/raw/session/mod.rs) has multiple
reply queues; the mount-wide invalidation handle uses queue zero, and its
notifications are excluded from the request-reply completion tracker. An
operation reply can overtake a queued notification.

Adding an acknowledgement is necessary but insufficient. Ordinary entry
invalidation takes the parent inode lock in Linux. A namespace callback can
already be waiting for its FUSE reply while holding that lock; awaiting an
entry notification from that callback can deadlock. Two connections can
also have concurrent callbacks holding their respective parent locks.
The protocol must account for the originating kernel's namespace updates
and remote connection ordering without this cycle.

An inode notification with offset -1 invalidates attributes and ACLs without
the page-cache invalidation associated with the existing mmap/014 hazards.
It also advances Linux's `attr_version`, protecting existing inodes against
many delayed GETATTR replies. It would be incorrect to claim every late
reply necessarily restores stale attributes. New inode/dentry installation,
other reply types and lease transitions still require a transport-level
proof; a bare notifier call does not provide one.
For example, Linux's SETATTR reply path uses `fuse_change_attributes_common`
without the GETATTR attr-version guard; it cannot be assumed to have the
same delayed-reply protection.

### The writer lease is not a kernel cache lease

[`LeaseValidity`](src/workspace_overlay/lifecycle/lease_validity.rs) is a
userspace proof checked before daemon row-cache hits. A kernel VFS hit does
not consult it. Backend generation/CAS fencing stops an old writer from
committing metadata; it does not withdraw mode/ACL grants already held by
that writer's kernel.

For example, an old daemon can prepare a reply with a relative positive TTL,
pause, lose authority, and resume after a new owner changes permissions.
If the old reply can still reach its FUSE connection, the TTL starts at kernel
receipt. Capping the TTL when the reply was originally constructed does not
bound its lifetime by the old lease deadline. Grants and reply completion
must be included in release, force-fence and takeover rules. A periodic
poller or heartbeat can itself be paused; its wakeup is not a hard deadline.

### The concrete two-mount requirement is not currently supported

Ordinary mount acquisition takes a distinct exclusive writer lease.
[`lease_singleton_renewal_and_release_are_generation_fenced`](src/workspace_overlay/stores/tests.rs)
asserts that a second live holder is rejected. Packed mount acquisition
also rejects an attached writer/active lease in
[`grant_packed_mounted_session_owned`](src/workspace_overlay/stores/kv_store/packed_admin/packed_mounted_session.rs).
Existing two-workspace fork tests prove isolation, not shared-workspace
coherence. An immutable readonly snapshot also cannot observe A's changes.

This does **not** rule out one owner serving two real FUSE connections.
It does rule out using the existing mount constructor twice as the proof:

- Cloning VFS shares one notification slot and one mount-device slot in
  [`VfsState`](src/vfs/fs/mod.rs). The second INIT replaces the first
  notifier. Request cancellation keys use per-connection unique IDs, and
  unmounting one connection shuts down shared request state.
- Creating two separate VFS instances over one metadata layer splits inode
  sizes, dirty writers and read state. `stat_ino_checked` replaces fresh
  metadata size with local inode size when present. A second VFS that keeps
  an opened zero-length inode can therefore continue returning its cached
  size after A grows the file, even after kernel invalidation. Bypassing the
  mount lease does not establish data coherence.
- Sharing the lease token between independent daemons invalidates the
  single-writer assumption behind daemon row caches and PR #332's authority
  for kernel timestamp decisions. Local invalidations miss the other daemon.

No same-workspace mounted A/B proof is claimed. Removing the lease check to
make that test launch would weaken an existing correctness invariant.

## Executed permission counterexample

A standalone Linux FUSE protocol fixture was mounted on this host with
POSIX ACL capability and `default_permissions,allow_other`. The caller was
nonroot UID/GID 1000. Each mount passed `mountpoint -q` and recorded its FUSE
identity. The fixture changes backing permissions outside FUSE, waits for
the change to complete, then issues the next tested syscall.

| Observation | TTL 0 ms | Diagnostic TTL 2,000 ms |
| --- | ---: | ---: |
| Repeated stat calls | 10,000 | 10,000 |
| FUSE requests in that stat loop | 30,000 | 0 |
| `access(R_OK)` allowed after backing chmod 000 | No | **Yes** |
| Child stat allowed after ancestor chmod 000 | No | **Yes** |
| Access allowed after acknowledged metadata invalidation | No | No |
| Stale successful access probes with notification omitted | 0 | 185 |
| Time from chmod until denial with notification omitted | 0.000467 s | 1.883389 s |

The TTL-zero loop produced 10,000 LOOKUP and 20,000 GETATTR requests; the
positive-TTL loop produced neither. No FUSE ACCESS requests were received:
Linux made the access decisions. Explicit invalidation used offset -1,
length 0 and acknowledged the synchronous device write. Both diagnostic
mounts were cleanly unmounted.

The 2,000 ms TTL is **rejected**, not the chosen coherency window. These
results demonstrate the shared lifetime of stat and permission data on
Linux `6.8.0-139-generic`. They are not BrewFS throughput measurements,
before/after medians, or a substitute for the requested A/B integration test.
The [structured probe result](doc/performance/attribute-cache-evidence/kernel-probe.json)
is included with this report.

Evidence and reproducer are retained under
`/home/hxy/perf-bench/owner/artifacts/attr-cache-feasibility-r1/`:
`kernel-probe.json`, `attr-cache-kernel-probe.c`,
`attr-cache-kernel-probe.py`, `permission-design-audit.md`, the copied Linux
source files, `baseline-audit.json`, `baseline-audit.py`,
`baseline-metadata-observations.tsv`, `baseline-metadata-medians.tsv`, and
`baseline-provenance.md`. The detailed audit records source hashes and
reproduction commands. The baseline provenance note distinguishes the
measured native workspace from legacy `MetaClient`.

Relevant upstream Linux v6.8 sources (the mounted probe independently checks
the behavior of this host's Ubuntu kernel):

- [Attribute expiry and permission checks](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dir.c#L1496)
- [Metadata/ACL invalidation and attr-version protection](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/inode.c#L507)
- [POSIX ACL capability enabling kernel permissions](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/inode.c#L1261)
- [Entry invalidation's parent lock](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dir.c#L1339)

## Alternatives and their costs

| Alternative | Permission guarantee | Cost and limitation |
| --- | --- | --- |
| Retain zero TTL for writable ACL workspaces | Preserves current kernel permission refresh policy | Repeated normal stat continues to cross FUSE; no claimed speedup |
| Generation polling plus finite TTL | Permissions can remain stale between polls/expiry | Small implementation, but fails the stated #332 constraint; rejected |
| Improve daemon metadata caching only | Can retain the present permission contract with existing lease fences | Reduces backend work but still pays a FUSE request; does not meet the VFS-hit goal |
| One owner with multiple FUSE attachments and cache grants | Plausibly meets the contract when all grant/recovery obligations are enforced | Split connection-local state from shared inode/writer state; add grant tracking, completed-write acknowledgements, reply ordering and takeover fencing |
| Independent frontend daemons subscribed to an owner | Can support real distributed clients with coordinated revocation | Adds routing/subscriber state, remote revoke acknowledgements and recovery; an unreachable cache holder must block conflicting mutations until safely fenced |
| Independent permission/data lifetimes in Linux FUSE | Could separate the cache domains | Requires kernel/ABI changes and deployment; pathname permission checks still have a cost |

The narrowest credible positive project is the single-owner/multiple-
attachment option. It preserves one store writer, but must keep one shared
data state and separate each connection's notifier, request IDs, cancellation
and teardown. Conflicting mutations must stop old grants, handle pending
replies, invalidate every affected attachment, and wait for acknowledged
revocation before returning. Clean release and recovery must retire every
attachment and outstanding grant. Failing or disconnected holders must not
be silently treated as revoked. This adds mutation latency and may reduce
write availability during failures; it is a new coherence protocol, not a
TTL adjustment.

That protocol is a feasible alternative, not an implemented or proven result.
The present review rejects the smaller candidate because it cannot establish
these conditions. In particular, no bounded stale-permission interval is
silently substituted for the requested zero additional revocation window.

## Validation boundary

This branch changes the report and captured evidence only. No `src/`, `vendor/`, mount default,
permission check or test skip list is changed. The design/audit and mounted
kernel counterexample are complete; the performance acceptance line is not
met. There is no candidate for new `bench2.py` after measurements.

Under the requested stop-and-write-up provision, `generic/087`, `generic/097`,
`generic/014`, `generic/100`, `cargo build --workspace`,
`cargo test --workspace --lib` and
`cargo clippy --workspace --lib -- -D warnings` were not rerun for a cache
implementation. The successful #344 gates belong to that PR and are not
relabeled as attribute-cache acceptance. A future implementation must pass
all those gates, the actual same-workspace two-mount test and the unchanged
three-run `bench2.py` comparison before claiming success.
