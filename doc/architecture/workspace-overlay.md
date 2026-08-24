# Workspace Overlay Architecture

- Status: implemented, maintained with `src/workspace_overlay/`
- Cargo feature: `workspace-overlay` (off by default)
- Volume format: `workspace-v1`
- Production metadata backends: Redis and TiKV

This document is the maintained architecture reference for BrewFS agent
workspaces. The detailed API, state-machine fields, and implementation checklist
remain in the
[workspace overlay implementation spec](../superpowers/specs/2026-08-23-brewfs-workspace-overlay-implementation-spec.md).
Changes to persisted records, transaction boundaries, fencing, layer reachability,
or mount integration must update this document in the same pull request.

## Goals and boundaries

Workspace Overlay lets multiple agents share immutable base files and object
blocks while keeping each agent's namespace, inode attributes, and data extents
private. Forking creates metadata records and a new writable head; it does not
copy the sealed base or clean object data.

The feature is deliberately isolated from existing BrewFS volumes:

- flat volumes continue through the existing `MetaStore -> MetaClient -> VFS` path;
- workspace volumes use `WorkspaceStore -> WorkspaceMetaLayer -> VFS`;
- the default build does not compile `src/workspace_overlay/`;
- object keys and the existing `SliceDesc` representation are unchanged;
- a workspace error never falls back to flat-volume semantics.

## Component architecture

```mermaid
flowchart TB
    AgentA[Agent A mount] --> FuseA[FUSE / VFS]
    AgentB[Agent B mount] --> FuseB[FUSE / VFS]

    FuseA --> MetaA[WorkspaceMetaLayer<br/>workspace A]
    FuseB --> MetaB[WorkspaceMetaLayer<br/>workspace B]

    MetaA --> Resolver[Layer resolver<br/>newest record wins]
    MetaB --> Resolver
    MetaA --> Catalog[WorkspaceStore state machine]
    MetaB --> Catalog

    Catalog --> KV{Redis or TiKV}
    Resolver --> KV

    KV --> Topology[control<br/>low-frequency topology]
    KV --> Hot[hot/workspace<br/>hot/layer<br/>hot/lease<br/>hot/allocator]
    KV --> Delta[delta/dentry · inode<br/>xattr · acl · extent]

    FuseA --> Chunk[Existing chunk read/write path]
    FuseB --> Chunk
    Chunk --> Objects[(S3-compatible or local<br/>immutable blocks)]

    Shared[Sealed base layers] --> Resolver
    Delta --> Resolver
    Shared -. shared by both agents .-> Objects
```

The resolver walks a workspace's writable head followed by sealed parents.
For a given logical identity, the newest delta wins; whiteouts stop lookup in
older layers. Extents are clipped and combined into a read plan before the
existing chunk reader fetches object blocks.

## Persisted graph

```mermaid
flowchart LR
    WS1[Workspace A] --> HA[Writable head A]
    WS2[Workspace B] --> HB[Writable head B]
    HA --> Base[Sealed base revision]
    HB --> Base
    Base --> Parent[Older sealed parent]

    LeaseA[Lease A] -. fences writes .-> WS1
    LeaseB[Lease B] -. fences writes .-> WS2
    Snap[Snapshot] -. GC root .-> Base
    Journal[Seal journal] -. recovery root .-> HA

    HA --> DA[Private deltas A]
    HB --> DB[Private deltas B]
    Base --> Blocks[Shared immutable blocks]
```

Workspace heads, snapshots, active leases, and incomplete seal journals are GC
roots. A layer or object slice may be deleted only after it is unreachable from
all roots and the configured lease grace period has elapsed.

## Transaction architecture

### Hot data path

Ordinary namespace, inode, xattr, ACL, and extent mutations do not read or CAS
the global `control` record. They atomically check exactly three entity records
and update the current layer plus delta keys:

```mermaid
sequenceDiagram
    participant M as Workspace mount
    participant S as KvWorkspaceStore
    participant K as Redis / TiKV

    M->>S: mutation + HeadGuard
    S->>K: server time (Redis TIME / PD TSO)
    S->>K: read hot/workspace, hot/layer, hot/lease
    S->>S: validate workspace, head epoch,<br/>owner, lease generation and expiry
    S->>K: atomic CAS(three expected values)<br/>put hot/layer + delta keys
    alt CAS conflict
        S->>K: bounded retry from fresh values
    else committed
        S-->>M: allocated sequence range
    end
```

The writable layer owns its `next_sequence`, so writers in the same workspace
serialize correctly while sibling workspaces do not contend. Lease renewal CASes
only `hot/lease/<lease-id>`. Each ID allocator CASes only its own
`hot/allocator/<name>` record.

### Low-frequency topology path

Seal, fork, snapshot, commit, compaction, and GC retain a versioned `control`
document so graph changes remain all-or-reject. Before committing, the store
hydrates and exact-value-checks the hot records used by the transition. The same
backend transaction updates `control`, changed hot mirrors, journals, and any
required delta keys. A concurrent writer or heartbeat therefore makes a stale
topology transaction retry instead of silently committing from an old view.

Redis implements multi-key CAS with a binary-safe Lua script. All workspace keys
use one fixed cluster hash tag. TiKV uses pessimistic transactions and
`get_for_update` for the checked entity keys.

## Authoritative lease time

Lease expiry must be based on metadata-backend time, never mount-host wall time:

- Redis uses `TIME`;
- TiKV calls `TransactionClient::current_timestamp()` and converts the PD TSO
  physical millisecond component to nanoseconds with checked arithmetic;
- SQLite is a local semantic and fault-injection backend and uses its local
  transaction clock.

Every mutation also checks workspace ID, head layer ID, head epoch, lease ID,
holder generation, writable state, and expiry. A stale mount is fenced before
any delta or sequence update is committed.

## State transitions

Writable heads follow `Writable -> Sealing -> Sealed`; abort is permitted only
before hashing and restores the old writable view. A successful seal creates a
new writable head, increments the workspace head epoch, and records the sealed
revision's digest and root hash. The durable journal makes recovery idempotent
across crashes between quiesce, data drain, hashing, and head switch.

Fast-forward commit is deliberately restricted: the source revision and target
fork base must still match, the target head must be empty, and the target must
have no active writable lease. Conflicts preserve the child workspace for
inspection or retry.

## Code map

- `src/workspace_overlay/meta_layer.rs`: POSIX-facing workspace metadata layer.
- `src/workspace_overlay/resolver.rs`: layered namespace and extent resolution.
- `src/workspace_overlay/catalog.rs`: backend-neutral storage contract.
- `src/workspace_overlay/stores/kv_store.rs`: shared Redis/TiKV state machine and
  hot-record transaction boundaries.
- `src/workspace_overlay/stores/redis.rs`: Redis key scoping, Lua CAS, server time.
- `src/workspace_overlay/stores/tikv.rs`: TiKV transactions, scans, and PD TSO.
- `src/workspace_overlay/lifecycle.rs`: seal, fork, snapshot, commit, recovery.
- `src/workspace_overlay/gc.rs` and `compaction.rs`: reachability and flattening.
- `src/workspace_overlay/cache_scope.rs`: workspace-aware cache identities.

## Required maintenance and verification

Any architecture change must preserve and test these invariants:

1. sibling workspaces share sealed bases but never observe each other's deltas;
2. hot mutations never CAS `control`;
3. same-head sequence allocation is gap-safe under backend CAS retries;
4. heartbeat and allocators remain entity-local;
5. stale epochs and lease generations cannot partially write;
6. active leases and incomplete journals prevent premature GC;
7. default flat-volume behavior and serialized metadata remain unchanged;
8. Redis and TiKV both pass the distributed two-store concurrency contract.

The feature test suite, resolver oracle tests, real Redis/TiKV backend contracts,
dual-mount isolation tests, pjdfstest, and xfstests gates are the acceptance
evidence for this subsystem.
