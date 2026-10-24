# Workspace Overlay Architecture

Status: **entity experiment with opt-in benchmark mount wiring; production
mount remains disabled** (2026-10-21).
This document and the [implementation spec](../superpowers/specs/overlay-workspace.md)
now specify an ordinary per-entity writable namespace over one immutable packed
lower. They supersede the previous mutation/version architecture as the target,
not as a claim about the current binary.

The established production/default overlay path is r31, `0d4e36c` / PR #41;
`6b54270` delivered its
documentation on `codex/packed-metadata-aliyun-20260930`.
Its actual design, validation gaps and reproduction commands remain in the
[r31 delivery handoff](../superpowers/plans/2026-10-17-overlay-no-seal-handoff.md).
The unvalidated r32 candidate has been dropped in favor of this direction.
The `workspace-entity-v1` experiment began on
`codex/overlay-entity-namespace-20261019`, branched from `b6f297e`.
Its WIP was merged into `main` by [PR #42](https://github.com/Ivanbeethoven/brewfs/pull/42)
at `3cb2082500aac0476f275b107228f7dd14a087ce`. The structural
continuation used `codex/overlay-entity-simplification-20261020`; that merge did
not establish correctness or performance acceptance. The historical integration
branch remains separate. See the
[stage 2 checkpoint](../superpowers/plans/2026-10-19-overlay-entity-stage2.md)
and [file-I/O continuation](../superpowers/plans/2026-10-20-overlay-entity-data.md)
for the completed API-stage gates. The
[mount experiment checkpoint](../superpowers/plans/2026-10-20-overlay-entity-mount.md)
tracks the fresh fixture bootstrap, dedicated FUSE adapter and their own
validation; earlier API results do not validate those additions.
There is no conversion, production mount or accepted replacement performance
result. The mount checkpoint records preliminary measurements for available arms.
The [entity batching checkpoint](../superpowers/plans/2026-10-20-overlay-entity-batching.md)
records the subsequently frozen diagnostic baseline and the preceding request
batching refactor, committed as `df846766c14860ef97ea1fdb79a0bb8d3ef88179`.
Its local correctness gate passes with the documented overlay exclusions;
release smokes and matched measurements are complete. The comparison is
preliminary and covers combined WIP plus refactor against frozen `cfa000a`;
no isolated PR #42 throughput measurement exists.

The [read-batching checkpoint](../superpowers/plans/2026-10-21-overlay-entity-read-batching.md)
records the next change on `codex/overlay-entity-rtt-20261020`: `8a0f8b8`
validates read-only operations using existing per-operation row batches;
`49da510` aligns the service gate with the workspace test graph. The stage 1
local gate, release smokes, diagnostics and matched comparison are complete and
audited, with the documented test exclusions. Measurements remain preliminary.
The separate `stage2-fsync` follow-up was rejected and reverted: it reduced
requests, but the original comparison and adjacent matched recheck did not
establish a consistent throughput/tail improvement. Both preliminary results
and the unresolved original latency outlier are preserved in the checkpoint.
The retained script follow-up, `16c2aca`, uses STRLEN to check row type and size
without a separate TYPE command. Its full gate and fresh-control comparison
are complete. Application request counts stay unchanged; inclusive Lua service
time falls. The preliminary comparison improves raw entity throughput, but
does not prove a relative 32-worker WRITE win or native parity.

## Contract and boundaries

A workspace has one immutable packed lower and one writable upper namespace.
Missing upper entries fall through to the lower; explicit tombstones suppress
that fallback. Metadata changes and new data slices belong to the upper.
Creating a workspace pins its lower and creates an empty namespace; it does
not copy the lower's metadata or data.

The experimental packed adapter authenticates the manifest key/digest, root
inode and maximum inode before exposing metadata or payloads. First attach
promotes a live packed reader pin into a durable owner for the volume and
namespace; reopening reconstructs that owner from persistent metadata. These
owners participate in the existing packed/native collector census and its root
generation CAS. They survive reader-lease expiry and are released only with a
terminal deletion proof after upper cleanup. The current bridge admits at most
256 active owners per store and retains terminal owner tombstones; creating a
new owner still requires a live source reader pin.

The experimental SDK uses trusted adapter composition. Callers must pair the
packed owner with the same volume, namespace, lower binding and authoritative
metadata backends used by the entity lifecycle. The experiment has no production
factory enforcing that pairing and reader quiescence. The
benchmark bootstrap is trusted wiring over those existing APIs; it adds no
general backend-pairing or external-writer qualification.

Keep sole-writer lease safety, atomic namespace operations, crash consistency,
correct object reclamation and fsync durability. Preserve ordinary permissions,
ACLs, hardlinks, sparse-file behavior and O_EXCL. Those filesystem semantics
do not require a workspace-wide mutation order.

The target has no writable head/layer record, per-layer sequence allocator,
expected-head CAS, row sequence stamps, append-only extent history, or mutable
workspace digest. There is no live-layer seal, overlay-stack flattening or
strict-lower mode. Publishing an immutable packed export remains a separate
administrative operation. Packed manifests, payload checksums and immutable
object authentication remain; removing a workspace digest does not remove
data integrity checks.

Default flat volumes retain their existing path. A new versioned namespace
format and explicit dispatch separate this target from existing workspace-v1
records. Old binaries must reject the new format. The format identifier and
new APIs are experimental. Selecting `workspace-entity-v1` without the explicit
`--experimental-entity-mount` opt-in fails closed. The experimental dispatch uses
its own FUSE adapter and never falls back to the old workspace path.

## Experimental benchmark mount

`entity/experiment.rs` wires a fresh packed catalog and fresh entity namespace
to the existing Redis, packed-owner and file-I/O APIs. Its input is a standalone
packed manifest in a dedicated experimental bucket and unused metadata scopes.
The bootstrap authenticates the manifest through the existing packed reader,
installs it under a temporary setup lease, promotes a source reader pin into
the existing durable lower owner, then provisions/acquires the entity writer.
The source pin and setup lease are released after setup; the durable lower
owner and source catalog remain. Legacy catalog head records serve this setup
and lower ownership only; entity data writes use the new current-map APIs.

The CLI selects `--volume-format workspace-entity-v1` with
`--experimental-entity-mount`, a packed manifest key, a non-nil
`--entity-volume-id` and a non-nil workspace UUID. `--workspace-namespace` is
optional and defaults to `brewfs`; its selected metadata scope must be unused.
Explicit bounded geometry is also needed: the runner sets 4 MiB chunks and
blocks; the normal 64 MiB chunk default exceeds the entity API's 16 MiB maximum.
The isolated harness supplies a fresh scope explicitly. A dedicated FUSE adapter
calls `EntityNamespace` and `EntityData` directly. Orderly unmount stops request
admission, drops its handle registry and calls the existing local session drain. It retains
the namespace, lower ownership and data; the isolated harness owns service,
volume and bucket teardown. This path provides no reopen/resume, namespace
discard wrapper, process-crash takeover, importer or atomic cutover. Dedicated
fixture scopes are a caller/harness precondition, not proof that unrelated
catalogs sharing an object service have no writers. Enforced backend/owner
pairing, combined-stack fault qualification and paginated production cleanup
remain outside this benchmark wiring.

### Direct entity mount path and shared request execution

The structural continuation removes the unused SDK bridge through the native
VFS. `EntityFuse` retains its own handle table containing `Arc<EntityHandle>`
and access/append flags. Namespace callbacks call `EntityNamespace`; read,
write, truncate, flush and fsync call `EntityData` with that handle directly.
The former `src/vfs/fs/entity_io.rs`, `EntityFileIo`, `BoundFile` and native-VFS
entity-handle slots/dispatch are removed. The native VFS no longer accepts
entity handles; the explicit experimental mount uses only the dedicated adapter.
Entity handles therefore do not enter native inode indexes, slice caches or
writeback paths. The adapter retains direct I/O, zero metadata reply TTL and
rejection of kernel writeback requests.

Namespace create, lookup, getattr, access, open, directory listing and mutations
use one typed `Request`/`Reply` execution engine in `entity/namespace.rs`.
The separate `namespace_create.rs` and `namespace_mount.rs` implementation
wrappers have been folded into that engine; `namespace_snapshot.rs` still
provides shared bounded preparation. Named operations retain discovery
observations through ordered affected-inode lock acquisition. The engine
validates permissions and exact row/absence predicates, handles known conflicts,
and installs open handles only after the corresponding successful validation or
create commit. This consolidates control flow without a shared mutable head or
global mutation gate. Current structural tests pass within the local gate's
documented coverage; release smokes and the preliminary comparison also complete.
Historical SDK tests do not validate the new path.

### Immutable lower metadata and create requests

The entity adapter retains an in-process, byte-weighted 64 MiB LRU memo for
lower inodes, dentries (including absence), cold xattr/ACL attributes, current
chunk maps and directory listings. It follows the shipped packed-lower memo
approach and keeps the existing authenticated packed reader for cache fills.
Asynchronous fills for the same key coalesce. Mount warmup loads the root and
prefetches up to 32 children, including cold attributes and the first file chunk
map, with at most four concurrent child loads. Payload bytes are not in this
metadata budget. The packed index reader retains its separate 4 MiB page cache.

The mount binds the memo to the exact lower identity and writer session after
validating the durable packed owner. Each bound lookup/fill retains local session
admission; terminal owner release requires that session to close and drain.
Cache hits therefore need neither catalog-owner RPCs nor object requests.
Standalone, unbound SDK access retains remote owner validation. Cold or evicted
metadata uses authenticated asynchronous fills that can require network/object
I/O; the bounded cache does not promise that
an arbitrarily large lower is entirely resident. Errors are never cached as
absence. Upper values and tombstones resolve first. A copied-up inode can still
inherit an absent xattr or chunk, subject to its lower visibility boundary.

Runtime authority reads the five canonical descriptor/lease/pin/owner/registry
rows in one snapshot, using the immutable lower identity retained by the session
to choose keys. Every commit still checks the exact rows and backend-time lease
deadline. No mutable permission result is cached.

Create combines that snapshot with parent, ancestor, ACL, name and truncate
predicates. Bounded directory-parent hints select the batch; fresh inode and ACL
rows determine permissions. A warm create uses one read and one commit. A cold
or stale ancestor hint can require extra reads, and conflicts recompute the full
decision. The commit checks effective absence and allocates with INCR inside the
same script, then installs the inode, dentry, inherited ACLs and parent update.
The script validates generated keys and bounds before its first write and uses
exact decimal IDs above 2^53. It does not compare-and-swap the allocator counter.
Only the affected parent is locally locked; unrelated parents remain independent.

### Per-operation batched rows and grouped publication

Namespace and data operations use the existing operation-local `Prepared`
read set. The existing atomic backend read batches authority, backend TIME and
the entity rows needed by that operation; data reads include the actual stored
geometry row. Directory-parent and dentry-to-inode hints are bounded to 4,096
entries each and nominate keys only. A dependent read includes all prior
observations and newly discovered keys. It preserves earlier observations on
disagreement and marks the view uncovered. A covered read-only operation's
result or error needs no redundant read-only commit; an uncovered view is fully
revalidated, and conflicts recalculate the operation. Mutations retain their
exact transaction predicates. This reuses existing reads and introduces no
public/backend API, object type, authoritative cache, persistence/lifecycle
protocol or snapshot feature.

Upper-data reads also keep advisory slice identities for at most 4,096 chunks,
with at most 16 identities per chunk. Successful writes and freshly decoded
maps seed these hints. A warm read batches maps, ownership, and object
descriptors in its first snapshot, then retains full post-transport validation:
two metadata reads instead of three. Cold, stale, evicted, or more fragmented
maps use dependent reads. All row values still come from fresh atomic snapshots.
Unused hinted rows are removed before dependent reads and final validation;
failure of a speculative snapshot retries without hints, so stale rows cannot
exhaust the operation's metadata budget or make unrelated corruption fatal.

Path open retains its observed ancestry and name through inode-lock acquisition,
then rereads authority and those predicates before returning the handle.
Directory SCAN also invalidates coverage, including an empty result. Payload
reads retain full validation after object transport. Open fallback and truncate
return the attributes established by their own operation so the FUSE reply need
not start another attribute read. This is same-operation reuse, not an authoritative
cross-operation cache of mutable attributes or permissions.

A complete atomic batch of authority and entity rows defines the read result
and its lease check. Cold immutable lower metadata resolution can finish after
that lease expires; the previous trailing commit could instead have returned `Fenced`.
There is no guarantee of lease validity at the end of metadata resolution.
Retained admission and lower ownership protect that resolution. This does not
remove payload validation after transport or mutation-time fencing.

READ_LUA and TRANSACTION_LUA preflight row types and byte bounds with protected
STRLEN calls. WRONGTYPE maps to the existing corruption error; other Redis
errors propagate. GET, exact predicates, backend TIME and validation before
the first write remain. This removes one nested TYPE command per checked row,
not an application request, and adds no feature, API, cache, persistent object
or filesystem behavior. The Redis TYPE permission is no longer required by
these preflights; identical ACL failure timing is not claimed.

Data writes reuse the operation's authority to allocate slice IDs and register
all Pending upload owners for an admitted group in one transaction, before any
object PUT. A successful conditional durable PUT produces a local receipt; it
does not require a separate Uploaded metadata transaction. Final publication
checks the exact Pending bytes, aggregates slice membership/refcount changes
across the group's chunk maps, and atomically installs those maps, immutable
object descriptors and the inode size/times/data version. The same inode lock
still covers the work, and an uncertain reservation/publication is never blindly
replayed. Only an acknowledged prefix can be returned after partial work.
This is one visibility publication per bounded write group, not one total
Redis write transaction: durable slice reservation is a separate transaction
before upload, followed by the group's map/inode/ownership publication.

Each publication group must honor row, byte and fragmentation limits before its
commit. Known aggregate preparation limits split a group using fresh observations;
uncertain commit results never trigger a split or retry. Bounded grouping and
the hidden-tail read fallback pass focused memory/Redis and real RustFS tests.
Release smokes and the ordinary write/readback workload also pass; they do not
exercise every fragmented-map case. Unused standalone
allocation, upload registration and Uploaded APIs are
removed. Truncate/discard/orphan cleanup retains its asynchronous membership
wrapper over the shared pure planner and its existing durable cleanup state.

In committed stage 1 (`8a0f8b8`), fsync still waits on the shared FIFO inode lock,
retains admission, reports sticky errors and validates live lease/backend-time
predicates. Its initial read includes the truncate marker: observed absence
takes the validated read-only commit path, while an actual intent invokes the
existing recovery walker and then refreshes the snapshot. Ordinary I/O no longer performs an
unconditional empty-intent recovery walk. Removing the upper-fsync/flush
read-only commit reduced each callback from two metadata requests to one in
the rejected `stage2-fsync` diagnostic. That change is not retained; stage 1
still uses two. The historical diagnostic below measures `df846766`.

## Audit of the current design

The current overlay is not uniformly an append-only log: dentry, inode, xattr
and ACL delta keys already hold their current value. Extent keys include a
sequence and accumulate overlapping records. Every nonempty row mutation advances the
same head sequence, which provides a coarse version for the entire workspace.

| Current component | Classification | Target disposition |
| --- | --- | --- |
| Authenticated immutable lower and fallthrough reads | Required contract | Keep the packed reader and a durable lower pin |
| Dentry/inode/xattr whiteouts | Required contract | Keep tombstones; absence must remain distinct |
| Lease, sole-writer authority, backend-time expiry | Required contract | Check in every metadata-changing transaction |
| Atomic create/rename/link/unlink and permission decisions | Required contract | Transactions over affected entities; O_EXCL absence check inside create |
| Upload-before-publication and fsync completion | Required contract | Keep; independent of workspace ordering |
| Reachability protection, orphan recovery and deletion reservation | Required contract | Replace census fencing with object ownership and safe retirement |
| Head record, next_sequence, expected_layers and global mutation CAS | Artifact of chosen version model | Remove from the runtime format |
| Sequence-stamped extent keys and newest-first overlay folding | Artifact of extent history | Replace with current normalized chunk maps |
| Layer delta digest/root hash for mutable-overlay identity | Artifact of revision model | Remove; immutable packed integrity remains |
| Per-layer extent generation and whole-layer census fencing | Artifact of current GC proof | Remove only after per-object deletion fencing is implemented |
| Global mutation mutex | Artifact of coarse version/cache coordination | Replace with affected-inode/parent coordination and backend atomicity |
| Commit-view cache and cached head/derived-counter bytes | Optimization of that model | Drop |
| Data group commit | Optional optimization | Drop from the initial target; reconsider only with measurements |
| Lease-bound row/path/permission caches and created-path seeding | Optional optimization with coherence obligations | Not required for the initial target; add only if measured and correct |
| Packed-lower metadata memos and prefetch | Required hot-path policy for the entity experiment | Bound asynchronous immutable memo; not mutation authority. Payload caching remains separate |
| Per-inode data version / directory revision | Local coherence mechanism | Keep only where reads, caches or directory cookies need it |
| Publication/discard recovery intent | Required crash protocol when work spans transactions | Keep bounded operation state; no general mutation replay log |

The shared head was a convenient replacement for precise read sets: a changed
head forced inode-copy, permission and namespace decisions to be recalculated.
It also supplied ordering for overlapping extent records, read-plan fences,
accounting and administrative empty-head checks. It was not required by the
packed file format, lower fallback, leases or fsync.

Diff/export used delta rows, sequence-aware resolution and canonical layer
digests. The target obtains the current effective state by walking upper keys
and merging with the pinned lower. Sequence-based comparison/identity is removed;
reading and exporting remain. Current scalar rows already overwrite history,
so r31 does not provide complete historical transaction replay either.

## Proposed data model

Use workspace prefixes in one volume metadata transaction domain, for example
`{volume}/workspace/<id>/...`. Prefer prefixes over separate Redis databases:
chunk references and shared-object ownership must participate in the same
transaction. Redis Cluster keys involved in such transactions share a hash slot.
This provides independent client operations, not parallel execution of Lua on
one Redis server. TiKV support requires equivalent transactions and separate
qualification; it is not assumed from Redis results.

| Entity | Current value |
| --- | --- |
| Workspace descriptor | Format, namespace identity, immutable lower manifest, lifecycle state and lease identity; changed only by administration |
| Lease | Sole writer, holder generation, state and backend deadline |
| Inode | Present/deleted, attributes, nlink, size, xattrs/ACLs, lower visibility boundary and any needed local data version |
| Dentry `(parent, name)` | Inode/type or tombstone; no upper key means consult lower |
| Xattr/ACL `(inode, name)` | Value or tombstone, so removal masks a lower value |
| Chunk `(inode, chunk_index)` | Current complete, disjoint spatial map of upper Data slices, immutable LowerRange references and Holes |
| Slice/object ownership | Pending-upload owner, upper reference count/edges, immutable-publication ownership, read protection and deletion state |
| Allocators | Plain INCR/INCRBY uniqueness counters, unrelated to mutation order |
| Recovery intent | In-progress truncate, publication or discard with an idempotent cleanup cursor |

The stage 2 schema uses `{brewfs-entity:<volume UUID>}/workspace/<namespace UUID>/`
for inode, dentry, xattr, chunk, orphan and lifecycle rows. Raw names are
hex-encoded injectively; slice records and the slice allocator use the shared
volume prefix. Rows use bounded typed JSON; allocator values are canonical
decimal integers. The experiment is not yet a production wire-format promise.
There is no per-commit workspace counter. A shared descriptor/lease may be read
and checked by every operation without being rewritten by unrelated file mutations.

Inode allocation starts above imported lower IDs. Object/slice IDs are unique
across sharing namespaces, never reused after deletion, restore or failover.
Redis's signed-64-bit range is explicit; counters fail on exhaustion rather than
rounding through Lua doubles. Allocation gaps are harmless.

### Reads, copy-up and tombstones

Lookup first checks the upper dentry. A tombstone returns absence; a missing key
consults the packed lower. Inodes and xattrs follow the same rule. Hardlinks in
either namespace resolve the same effective inode, so a copied-up inode changes
all its aliases. Readdir merges names and removes whiteouts, with per-directory
cookie/version semantics rather than a workspace generation.

A missing chunk inherits the immutable file only below the inode's
`lower_visible_size` (initially lower EOF, or zero for a new file); other bytes
are holes or beyond EOF. On first modification,
materialize that chunk's mapping metadata, then replace the written range with
new slices. A present chunk map covers the entire chunk domain: gaps are explicit holes, never an
implicit invitation to reveal lower bytes. LowerRange entries retain the
workspace's immutable lower identity. Spatial interval order is not mutation
history; overwritten intervals are removed from the current map.

Same-inode writes serialize their map/size changes, preserving overlap and
append semantics. Unrelated inodes need no common ordering. Multi-chunk writes
retain the filesystem's existing partial-write semantics; no new file-wide
atomic-write guarantee is implied.

### Namespace transactions and open/close

Create reserves a unique inode ID and atomically checks parent permissions and
effective name absence while installing inode/dentry/parent changes before
returning. The reference implementation allocates with INCR inside the namespace
publication transaction; O_EXCL rejection does not advance the counter.
The immutable lower lookup may be prepared outside Lua, but its binding must be
unchanged and the final transaction must recheck upper absence and mutable policy inputs.
Never merge create visibility with the first data fsync.

Rename, link and unlink transact over the affected parents, names, inodes and
ACL inputs. Same-entity local locks are acquired in a fixed order; stale
client-prepared decisions use per-entity validation and full recalculation.
Redis Lua can instead calculate from current entity rows inside the transaction.
Neither approach requires a shared-head CAS. Every mutating transaction checks
sole-writer authority, lifecycle admission and backend-time lease expiry.
Checks include mutable ancestor search-permission inputs used by the operation.
Lua errors do not roll back earlier writes: types, bounds, overflow, refcount
underflow and key collisions must be validated before the first entity write.

Ordinary open checks current permissions/lease with zero metadata writes and
retains a local handle. Linked close releases that handle locally, including
after lease expiry. O_TRUNC still performs a data mutation. Unlink of an open
inode records a recoverable orphan; closing its final handle allows proven
quiescent recovery to complete retirement. The initial target uses explicit
noatime semantics; adding a different timestamp policy is a separate decision.

### Truncate, object lifetime and fsync

Shrinking must hide both lower bytes and old upper chunks permanently across
later extension. Decrease `lower_visible_size` monotonically. For cleanup that
does not fit one transaction, set the new size/mask and a durable Truncating
intent atomically, hold same-inode mutation admission, prune the boundary and
later chunk mappings with their reference changes in bounded transactions,
then clear the intent. Boundary bytes beyond EOF become explicit holes. Advance
each cleanup cursor atomically with its map/reference changes, so a crash cannot
skip work or apply a decrement twice. Drain earlier buffered writes/uploads or
fence their publications with a per-inode truncate generation; they cannot land
after pruning and resurrect data. Block writes/re-extension until cleanup finishes,
including after restart. An interrupted shrink cannot expose old data merely
because its cleanup has not completed. Hole punching needs the same atomic
mapping/lifetime discipline.

Publish a chunk replacement and its slice-reference deltas in one transaction.
Count distinct current chunk-to-slice memberships, not file names or nlink.
LowerRange references are protected by the immutable lower pin; zero upper
references never authorizes deleting lower-owned data. Fork takes one manifest
pin, not an eager increment on every lower slice.

Fresh upper slices initially belong to one workspace; direct sharing between
live uppers is outside the initial target. Sharing through immutable publication
and lower pins remains supported. Publication that reuses upper objects establishes durable immutable ownership
before upper ownership is removed. Staged publication keeps its source protected
until this transfer is complete.

The initial implementation defers physical reclamation of retired upper slices
until proven namespace quiescence or fenced discard. Stop new I/O admission,
drain writes and active read plans (including handles retaining object pins),
invalidate obsolete plans and exclude live/recoverable reader sessions before
deletion. Refcounts and retirement queues update during normal operation, but
zero references alone do not delete an object a reader may still hold. This
avoids new per-read distributed pin traffic at the cost of retaining dead bytes
until maintenance. A future online collector would need an explicit read-pin
protocol; elapsed grace time alone is insufficient for stalled reads.

Reclaim an upper object only after atomically reserving Live-to-Deleting with
zero references, proven quiescence and no upload or immutable-publication protection.
Reference birth checks the same object state and cannot resurrect Deleting.
The durable reservation precedes object DELETE; retries are idempotent.
Pending uploads have durable owners and are reaped only when their authority
can no longer publish. Namespace sweeps enumerate ownership and reconcile leaks;
they do not infer safe deletion from an unfenced SCAN.
Collectors and crash recovery use explicitly fenced lifecycle authority, not
an expired runtime writer lease.

Complete durable object uploads before publishing their metadata references.
File fsync waits for that file's preceding uploads and metadata publications;
directory sync covers its documented namespace boundary. Both honor the
configured metadata/object-store persistence policy. A successful Redis reply
alone is not a universal power-loss guarantee. Lost commit replies require
reconciliation or a proven idempotent operation, never blind replay.

Strict entity uploads use the production S3 adapter's conditional-create
capability. At the recorded API-stage checkpoint, packed-lower, upper-data and
VFS object-I/O tests used an isolated RustFS service through the shared
`entity/test_objects.rs` helper.
Memory backends remain suitable for metadata transactions and fault injection;
temporary local files hold producer metadata spools only. These service tests
are ignored in the default suite and must be selected with `--include-ignored`;
missing explicit service configuration fails the test. The real-service gate
passed six packed-lower, 78 entity and two VFS tests at the recorded API-stage
source snapshot; exact evidence and remaining qualification gaps are in the
file-I/O continuation. That result does not cover the later benchmark bootstrap
or dedicated mount adapter. The two VFS tests covered the subsequently removed
SDK bridge and are historical evidence, not qualification of this refactor.
The structural continuation's own service gate passes seven packed-lower and
100 entity tests, including all four publication tests that failed in the
pre-merge WIP gate. Its source-verified manifest records 720 present Rust files
and four deletions; service cleanup leaves zero owned containers or volumes.
The broad overlay run passes 2,334 library and nine binary tests, with 405
ignored and five explicit exclusions: the three established legacy failures
and two source-file flakes. The mandatory default workspace run passes 1,179
tests with 225 ignored and zero filtered. Formatting, both strict all-targets
Clippy gates, workspace checks/default build, four runtime combinations and
tooling checks also pass. The batching checkpoint records commands, artifacts
and the first failed Clippy attempt. Separate release evidence completes eight
diagnostic and 16 uninstrumented phases, four smokes and 16 clean unmounts with
zero errors and no owned mounts/containers/volumes left. These results do not
provide production qualification or broad performance acceptance.
Metadata recovery oracles and actual object-I/O tests have separate coverage;
they do not yet qualify a complete Redis entity + real packed lower + VFS
stack, physical packed-graph retirement, or recovery after process/host failure.

## Export, diff and lifecycle

Pin one immutable lower when creating a workspace. Discard first fences the
writer, then walks upper ownership with resumable idempotent cleanup, releases
the lower pin last and retires the namespace. Reader admission and outstanding
read plans must also be drained/fenced before their protection is removed.
Never FLUSHDB a shared backend.

A consistent dump/export stops mutation admission, drains writes/fsync and
walks a stable namespace while retaining its lease and lower pin. Redis SCAN
over a live changing namespace is not a consistent snapshot. This administrative
pause is not a lock on every ordinary mutation. Build and authenticate the new
packed lower before publishing it; never seal or rotate a live upper.

Diff becomes a semantic comparison of effective namespaces/content. An
upper-only walk against the same fixed lower can still identify changed
entities, but a cheap arbitrary-workspace/history diff and total-order replay
are not provided. Export can sort records for reproducible output if desired;
a canonical writable-layer digest is no longer an identity contract.

## Experiment scope and deferred migration

1. **Design and format boundary.** R31 binaries, schema and accepted benchmark
   artifacts remain the baseline. R32's partial implementation stays archived,
   not deployed. The entity format has separate dispatch and storage.
2. **Isolated implementation.** Current-entity storage/resolution and atomic
   namespace operations are implemented behind explicit new-format dispatch.
   The experiment includes a bounded namespace/Redis substrate, an authenticated
   packed adapter and durable lower owners in the existing collector census.
   Its file-I/O continuation adds write-through current maps and bounded
   truncate/discard/orphan recovery. The structural continuation replaces the
   former SDK VFS bridge with direct `EntityFuse` calls to `EntityNamespace` and
   `EntityData`; its local gate passes with the coverage limits above. Operations
   require their namespace lower owner and active writer authority.
3. **Fresh-only mount benchmark: current scope.** The explicit opt-in wires
   isolated fresh fixtures. Normal mount probes, local gates and matched
   performance evidence belong to its checkpoint. It adds no new fault-injection
   or production-admission work. R31 gates do not validate the new path.
4. **Offline conversion: DEFERRED.** No importer is implemented or included in
   the current stage. The experiment does not adopt an existing mutable workspace.
5. **Atomic cutover and recovery: DEFERRED.** No format switch, migration intent
   or rollback is implemented. Rewinding an old pointer after entity writes
   would lose those writes.
6. **Production qualification and retirement: DEFERRED.** Enforced pairing,
   external crash-takeover authority, paginated production cleanup, full
   mounted/POSIX qualification and safe retirement of old ownership remain
   known limitations. They are not additional work items for this benchmark.

## Evidence, decision and trade-offs

The r31 stage-only diagnostic measured 91.2% gate occupancy at 32 workers,
3.703 ms of held service per file: create 60.2%, open+close 29.9%, data 9.9%.
At 16 workers occupancy was 82.0%, with 3.785 ms/file. These are one-repetition
attribution runs, not acceptance results for this proposal.

Completed r31 acceptance means (six samples per arm at 16/32 workers):

| Workers | Overlay WRITE files/s | Native WRITE files/s | Overlay/native |
| --- | ---: | ---: | ---: |
| 16 | 216.89 | 344.01 | 0.63 |
| 32 | 236.89 | 414.74 | 0.57 |

Completed r31 ratios for stat/full/write/readback/mixed are respectively
1.00/0.71/0.86/1.06/1.02 at one worker,
—/—/0.63/0.91/0.85 at 16, and —/—/0.57/0.88/0.87 at 32.
Missing stat/full results are not filled from the interrupted final sweep.

The 2026-10-20 fresh-entity mount experiment completed 24 available WRITE and
READBACK phase rows at 1/16/32 workers, with two opposite-order samples per
cell and zero workload errors. Preliminary packed-overlay WRITE medians were
20.08/111.90/135.00 files/s versus native 51.63/351.03/419.83; READBACK medians
were 32.04/173.05/199.05 versus native 113.40/602.50/616.47. `overlay-native`
was skipped because the frozen native lower adapter, binding and mount wiring
are missing. These are new-upper-file measurements, not lower-read/copy-up
results or acceptance. They do not demonstrate removal of the write plateau.
The [mount checkpoint](../superpowers/plans/2026-10-20-overlay-entity-mount.md)
records sample values, smoke/cleanup evidence, configuration differences and
exact reproduction commands. No historical accepted table is replaced.

The later combined WIP/refactor comparison uses frozen entity `cfa000a` as its
baseline and retains that baseline executable for the native arm. Each cell has
two opposite-order samples of 2,000 new 16 KiB upper files with per-file fsync;
means equal medians for two samples. All results below are preliminary.

| Status | Workers | WRITE entity / native files/s | Entity/native | READBACK entity / native files/s | Entity/native |
| --- | ---: | ---: | ---: | ---: | ---: |
| Preliminary | 16 | 272.210 / 359.643 | 0.7569 | 342.434 / 615.472 | 0.5564 |
| Preliminary | 32 | 311.518 / 431.146 | 0.7225 | 378.386 / 636.813 | 0.5942 |

Entity WRITE improves 137.71%/121.22% over the frozen entity baseline at
16/32 workers; READBACK improves 93.89%/84.20%. WRITE remains 24.31%/27.75%
below current native and READBACK 44.36%/40.58% below. Native control drift and
two-sample spread are retained in the batching checkpoint; no claim isolates
the structural cleanup or proves that a scaling plateau is eliminated.

For the measured `df846766` baseline, diagnostic external commands/file fall
from 113.770/114.400 to 15.720/15.780 for WRITE and 75.880/76.640 to
16.015/16.015 for READBACK; that comparison's native arm uses
7.115 and 5.005 respectively. CREATE/WRITE/FSYNC/FLUSH attempt 2/3/2/2 metadata
RTTs per operation, down from 15/25/12/12. WRITE still has a separate durable
slice reservation before publication. READBACK retains 8.005 READ_LUA and
8.005 read-only validation transactions per file, with no metadata mutations
from those validations. No packed-owner catalog calls occur in these diagnostic
windows; cold/evicted lower fills can still require authenticated I/O. This
quantifies reduced amplification and remaining work without assigning all
residual throughput loss to one cause. The batching checkpoint records the
source/binary hashes, full timers, smoke/cleanup proof and reproduction.

Native CREATE_ENTRY_LUA executes one atomic script without a client mount-wide
mutex; the server still serializes Lua and updates a shared allocator/parent.
Native open/close bookkeeping does not commit metadata. The r31 measured
CREATE/record_open/record_close path also did not update atime: the separate
legacy MetaLayer::open does. Atime is not an explanation of the measured gap.

**Drop r32.** Its backend-assigned global sequences and per-key checks could
reduce client serialization, but preserve the very sequence/head model being
retired. Neither it nor the prefetch-only precursor has a candidate performance
comparison. The prefetch gate was interrupted; the later atomic-sequence draft
was never built or validated. This is a design supersession, not a measured
performance rejection. Draft sources and evidence are preserved in the
[r32 investigation](../superpowers/plans/2026-10-19-overlay-write-r32.md).

Trade-offs are explicit: more expensive diff/export and an export pause;
no total-order replay or mutable-layer digest; current chunk-map rewriting and
possible fragmentation costs; per-object reference bookkeeping and crash
recovery; retired upper-byte retention until maintenance/discard and lower-object
retention until the last immutable pin;
and a format migration with no instant rollback after new writes. Hot inode or
parent transactions and Redis server execution still serialize. The completed
comparison demonstrates a preliminary improvement for the combined experiment;
broader correctness and performance acceptance remain unestablished.
