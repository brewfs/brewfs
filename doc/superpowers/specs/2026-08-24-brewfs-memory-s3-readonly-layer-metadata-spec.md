# BrewFS Memory + S3 Read-Only Layer Metadata Spec

Status: Architecture archive; committed-base format superseded by
[`2026-08-24-brewfs-clustered-frozen-metadata-v2.md`](2026-08-24-brewfs-clustered-frozen-metadata-v2.md)

Target: optional BrewFS agent-workspace subsystem

Format family: BrewFS Layer (`BRL`)

For committed filesystems that are never modified, the normative implementation specification is the
clustered v2 document above. It replaces the single-image `FrozenImage` with independently uploadable
file clusters, union-mounted directory contributions, pageable indexes, and evictable metadata
batches. The material below is retained only for the private MemLayer and historical architecture
context; where it conflicts with clustered v2, v2 wins.

## 1. Decision

BrewFS agent workspaces use three different metadata representations:

```text
Shared committed base    -> immutable streaming FrozenImage in S3
Host read cache          -> Arc<FrozenImage> shared by all local agents
Per-agent writable head  -> private in-memory MemLayer + optional immutable WAL segments
```

Committed filesystem metadata is not stored in Redis, etcd, TiKV, SQLite or another row-oriented KV
store. A FrozenImage contains the complete materialized namespace; committed reads do not follow a
parent chain.

S3 is the durable source of truth for committed images. Memory is the serving representation. One
small workspace head pointer is conditionally updated to publish a new generation; FrozenImages, WAL
segments, head records and snapshots are all write-once objects.

The compact persisted representation is a parent-first page stream:

- root and parent directory entries precede child directory groups;
- new child inode attributes are inline with the introducing parent entry;
- namespace pages are ordered breadth-first and can be published progressively;
- extent and cold-attribute pages are separately indexed for on-demand Range GET;
- sorted and prefix-compressed names;
- canonical unsigned varints and ZigZag signed varints;
- independently compressed and checksummed pages;
- no JSON and no repeated field names;
- runtime compilation into direct arrays and immutable directory hash indexes.

The canonical persistence format is not a raw Rust struct dump. `rkyv` may be used for a disposable
same-build process cache, but it is not the S3 format: persisted bytes must remain versioned,
validated and readable across compiler and crate upgrades.

## 2. Scope

This spec defines:

- the compact streaming format for a committed read-only image;
- the private in-memory writable layer;
- optional active-head WAL persistence;
- S3 object naming and immutable publication;
- workspace head CAS and fencing;
- restart, cache, snapshot and GC behavior;
- integration boundaries that keep normal BrewFS behavior unchanged.

File data remains in BrewFS block objects. Layer metadata contains only extent references to
`slice_id` and `slice_offset`.

The following state is not stored in a FrozenImage:

- active leases and open file handles;
- POSIX locks and flock state;
- dirty data-upload buffers;
- quota reservations and transient counters;
- mutable writer ownership;
- uncommitted or delayed-delete data-object journals.

## 3. Required invariants

1. A published layer pack is immutable and content-addressed.
2. A workspace has at most one publish-capable writer epoch.
3. Agent-private mutations are visible only through that agent's `MemLayer` until published.
4. All agents may share the same `Arc<FrozenLayer>` and data objects without copy-up.
5. A write never mutates a sealed parent; it inserts a Put, Whiteout, Hole or Data delta into the
   private head.
6. A head pointer never references an object that has not already been uploaded and verified.
7. Equal logical deltas have equal `delta_digest` regardless of compression or block boundaries.
8. Corrupt S3 bytes never cause unchecked access, panic or unbounded allocation.
9. No KV service is required to mount or read an already published workspace revision.
10. When the feature is disabled, existing BrewFS mounts, metadata stores and wire formats are
    unchanged.

## 4. Runtime architecture

```text
                         S3-compatible object storage
                  +---------------------------------------+
                  | immutable layer packs (.brlp)         |
                  | immutable WAL segments (.brlw)        |
                  | immutable head records (.brlh)        |
                  | immutable snapshots (.brls)           |
                  | mutable CAS pointer per workspace     |
                  +--------------------+------------------+
                                       |
                               range GET / PUT
                                       |
                  +--------------------v------------------+
                  | LayerRuntime                           |
                  |                                       |
                  | LayerIndexCache -> Arc<FrozenLayer>    |
                  | WorkspaceActor -> private MemLayer     |
                  | WriterEpoch / group-commit WAL         |
                  +--------------------+------------------+
                                       |
                              WorkspaceMetaLayer
                                       |
                                      VFS
```

`LayerRuntime` is a process-local service. A `WorkspaceActor` serializes mutations for one workspace
and owns its `MemLayer`. Reads can execute concurrently against an immutable view consisting of:

```rust
pub struct WorkspaceView {
    pub writable: Arc<MemLayerSnapshot>,
    pub sealed: Arc<[Arc<FrozenLayer>]>, // newest first
    pub head_generation: u64,
    pub writer_epoch: u64,
}
```

Publishing a new in-memory snapshot uses `ArcSwap` or an equivalent atomic pointer. Readers never
hold the actor mutation lock while traversing sealed layers.

## 5. S3 object namespace

```text
brewfs-meta/v1/volumes/{volume_id}/
  layers/{first-byte-hex}/{pack_hash}.brlp
  wal/{workspace_id}/{writer_epoch}/{first_seq}-{last_seq}-{wal_hash}.brlw
  head-records/{workspace_id}/{generation}-{record_hash}.brlh
  heads/{workspace_id}.brhp
  snapshots/{snapshot_id}-{snapshot_hash}.brls
  tombstones/{object_hash}.brlt
```

Objects under `layers`, `wal`, `head-records`, `snapshots` and `tombstones` are created with
`If-None-Match: *`. Existing objects are accepted only when their length and content hash match.
They are never overwritten.

`heads/{workspace_id}.brhp` is the only mutable object. It is a small pointer updated with `If-Match`
against the previously read ETag. A backend without conditional PUT support cannot enable durable
multi-process workspace publication.

No S3 LIST operation occurs on lookup, readdir, write, seal or mount hot paths. Object locations are
derived from hashes or reached from explicit references.

## 6. Persistent references

```rust
pub struct LayerRef {
    pub layer_id: [u8; 16],
    pub root_hash: [u8; 32],
    pub pack_hash: [u8; 32],
    pub pack_len: u64,
}

pub struct WalRef {
    pub writer_epoch: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub previous_wal_hash: Option<[u8; 32]>,
    pub wal_hash: [u8; 32],
    pub object_len: u64,
}
```

`pack_hash` and `wal_hash` determine object keys. A sealed layer manifest embeds its complete parent
`LayerRef`; no external catalog lookup is needed to walk the chain.

Logical hashes remain separate from physical hashes:

```text
delta_digest = BLAKE3(canonical logical layer stream)
root_hash    = BLAKE3(schema_version_be || parent_root_hash || delta_digest)
pack_hash    = BLAKE3(pack bytes before footer)
wal_hash     = BLAKE3(WAL bytes before footer)
```

Compression or repacking may change `pack_hash`. It must not change `delta_digest` or `root_hash`.

No global sealed-version allocator is required. External revision identity is:

```rust
pub struct LayerRevision {
    pub layer: LayerRef,
    pub workspace_generation: u64,
}
```

The existing inode and slice ID allocation contract is outside this format. The builder accepts the
IDs assigned by the workspace engine and validates their ranges; it does not depend on a KV counter.

## 7. Compact scalar codec

Sections 7-13 are an architectural overview of compact block ideas. They are not the BRLP byte-level
contract. In particular, the normative format uses five specialized grouped tables, inode field-mask
patches and normalized sequence-free sealed extents as specified in the linked physical-format
document. Implementations MUST use that document when field layouts differ from this overview.

Persisted record values use `CompactCodecV1`:

```text
uvarint  = unsigned LEB128, shortest possible encoding only
svarint  = ZigZag(i64) followed by uvarint
bytes    = uvarint(length) followed by raw bytes
option   = represented by an operation tag or a presence bitmap
enum     = stable u8 discriminant
```

Decoders reject:

- overlong/non-canonical varints;
- more than 10 bytes for a `u64` varint;
- overflow during ZigZag conversion;
- length values above the configured field/record limit;
- unknown required enum values;
- non-zero reserved bits.

Keys use fixed-width big-endian integers where byte order must match logical order. They are compacted
at the block level by prefix compression, which is more useful than varints for repeated parent inode
and chunk prefixes.

## 8. Prefix-compressed record blocks

Each section is sorted by its canonical key and divided into target 64 KiB uncompressed blocks.
Records never cross block boundaries. A record larger than the target occupies one block; the v1
maximum decoded record size is 16 MiB.

Within a block, each entry is:

```text
shared_key_bytes   uvarint
key_suffix_len     uvarint
value_len          uvarint
key_suffix         [key_suffix_len]
value              [value_len]
```

The full key is:

```text
previous_key[0..shared_key_bytes] || key_suffix
```

Every 16th entry is a restart entry with `shared_key_bytes=0`. The block ends with:

```text
restart_offset_0   u32 big-endian
...
restart_offset_n   u32 big-endian
restart_count      u32 big-endian
```

Restart offsets point to entry starts and are strictly increasing. The first offset is zero. A reader
binary-searches restart keys and scans at most one restart interval for a point lookup.

Each raw block is optionally encoded as one independent zstd frame. The block index stores codec,
stored length, raw length and CRC32C. The reader verifies stored CRC32C before decompression and
requires exactly `raw_len` output with no trailing compressed bytes.

## 9. Layer pack layout

```text
+----------------------------------+ offset 0
| fixed header                     |
+----------------------------------+
| Dentry data blocks               |
| Dentry Bloom filter + index      |
+----------------------------------+
| Inode data blocks                |
| Inode Bloom filter + index       |
+----------------------------------+
| Xattr data blocks                |
| Xattr Bloom filter + index       |
+----------------------------------+
| ACL data blocks                  |
| ACL Bloom filter + index         |
+----------------------------------+
| Extent data blocks               |
| Extent Bloom filter + index      |
+----------------------------------+
| section directory               |
+----------------------------------+
| fixed footer                     |
+----------------------------------+ end
```

Stable section tags:

```text
InodePatch=1, Dentry=2, Xattr=3, ACL=4, Extent=5
```

Every v1 section is present, including empty sections. Sections are stored in tag order. The header
contains:

- format major/minor and required feature bits;
- volume ID and layer ID;
- optional parent `LayerRef`;
- workspace schema version and layer depth;
- creation/seal time and chunk size;
- `delta_digest`, parent `root_hash` and this `root_hash`;
- section-directory offset and length;
- header CRC32C.

The parent pack hash in the parent `LayerRef` makes chain traversal self-contained. Root packs use an
empty parent.

The footer contains the complete object length, directory location, `pack_hash` and footer CRC32C.
The hash excludes the footer to avoid self-reference.

The section directory contains, for each section:

```rust
pub struct SectionHandle {
    pub kind: SectionKind,
    pub section_version: u16,
    pub first_block_offset: u64,
    pub stored_len: u64,
    pub record_count: u64,
    pub block_index_offset: u64,
    pub block_index_len: u64,
    pub filter_offset: u64,
    pub filter_len: u64,
    pub section_digest: [u8; 32],
}
```

Directory bytes have their own CRC32C. A normal range-open validates footer, directory and header
without downloading record blocks. A full scrub also verifies every section digest and `pack_hash`.

## 10. Block index and Bloom filters

A block-index entry contains:

```rust
pub struct BlockHandle {
    pub first_key: Bytes,
    pub last_key: Bytes,
    pub offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub record_count: u32,
    pub codec: CompressionCodec,
    pub stored_crc32c: u32,
}
```

Index keys use the same prefix-compressed block encoding. The reader loads section indexes at open
time but fetches data blocks lazily.

InodePatch and Dentry sections normally include a blocked Bloom filter. Xattr, ACL and Extent filters
are optional. Bloom-filter negatives are authoritative; positives still require index lookup.

Filters and indexes are covered by directory CRC/section digest and by `pack_hash`. Their decoded
sizes are bounded before allocation.

## 11. Canonical keys

| Section | Ordered key |
|---|---|
| Dentry | `parent_ino:u64_be || name:bytes` |
| Inode | `ino:u64_be` |
| Xattr | `ino:u64_be || name:bytes` |
| ACL | `ino:u64_be || acl_type:u8 || ordered_acl_id:u64_be` |
| Extent | `ino:u64_be || chunk_index:u64_be || sequence:u64_be` |

`ordered_acl_id = (acl_id as u64) XOR 0x8000_0000_0000_0000` preserves signed numeric order.

Dentry names are raw bytes, non-empty and cannot contain NUL or `/`. Xattr names are raw bytes,
non-empty and cannot contain NUL. Existing BrewFS maximum lengths remain in force.

Keys are strictly increasing inside a section. Duplicate keys, unsorted restart keys or overlapping
block key ranges are corruption.

## 12. Compact values

Persisted enum discriminants are append-only:

```text
DentryOp:   Put=0, Whiteout=1
InodeState: Present=0, Deleted=1
ValueOp:    Put=0, Whiteout=1
ExtentKind: Data=0, Hole=1
```

### 12.1 Dentry

```text
op          u8
sequence    uvarint
if Put:
  ino       uvarint
  kind      u8
```

Whiteout has no inode payload.

### 12.2 Inode

```text
state             u8
kind              u8
presence_bitmap   uvarint
mode              uvarint
uid               uvarint
gid               uvarint
rdev              uvarint
nlink             uvarint
size              uvarint
atime_ns          svarint
mtime_ns          svarint
ctime_ns          svarint
data_version      uvarint
sequence          uvarint
if HAS_PARENT:  parent_hint uvarint
if HAS_SYMLINK: symlink_target bytes
```

V1 presence bits are `HAS_PARENT=0` and `HAS_SYMLINK=1`; all others are reserved. A symlink must have
a target. A non-symlink must not. Deleted inode records retain complete attributes for deterministic
diff and validation.

### 12.3 Xattr and ACL

```text
op          u8
sequence    uvarint
if Put:
  value     bytes
```

An empty Put value is valid. Whiteout has no value. ACL type and ID are in the key.

### 12.4 Extent

```text
logical_offset  uvarint
length          uvarint
kind            u8
if Data:
  slice_id      uvarint
  slice_offset  uvarint
```

Sequence is already part of the extent key. Required validation:

- `length > 0`;
- logical and slice ranges do not overflow;
- `logical_offset + length <= chunk_size`;
- Data uses a non-zero slice ID;
- Hole has no slice payload;
- sequences are increasing within equal `(ino, chunk_index)`.

The layer resolver keeps logical offset separate from slice offset. It creates the final read plan
before adapting visible Data extents to BrewFS `SliceDesc` operations.

### 12.5 GC slice references

Core BRLP bytes do not repeat slice IDs in another table. GC streams normalized Extent records. An
optional, safely regenerable `.brlg` sidecar may contain delta-encoded unique slice IDs.

## 13. Logical digest

Physical prefix compression, varints, zstd blocks, indexes and Bloom filters are not the logical hash
contract. The canonical logical encoder hashes the minimal InodePatch, Dentry, Xattr, ACL and
normalized Extent streams defined by the BRLP logical schema.

The pack builder feeds each decoded logical record into that encoder while it writes the compact
physical form. It refuses to finish if the computed digest does not match the expected seal digest.
A full scrub decodes records and repeats the same check.

The layer ID is physical identity stored once in the pack header. It is not repeated in records and is
not part of the semantic delta digest.

## 14. In-memory representations

### 14.1 Private writable MemLayer

```rust
pub struct MemLayer {
    pub layer_id: LayerId,
    pub parent: LayerRef,
    pub next_sequence: u64,
    dentries: BTreeMap<DentryKey, DentryDelta>,
    inodes: HashMap<u64, InodeDelta>,
    xattrs: BTreeMap<XattrKey, XattrDelta>,
    acls: BTreeMap<AclKey, AclDelta>,
    extents: BTreeMap<ExtentKey, DataExtentDelta>,
    byte_arena: BytesArena,
}
```

Only the owning `WorkspaceActor` mutates it. Read snapshots are immutable `Arc`s. Copy-up clones only
the affected lower metadata record and retains data extent references until a write creates a new
extent.

Each agent has its own MemLayer. Two agents based on the same revision therefore share all lower
`FrozenLayer`s and block objects while keeping different names, attributes and extents in memory.

### 14.2 Shared FrozenLayer

```rust
pub enum FrozenLayer {
    Hot(HotFrozenLayer),
    Cold(LayerPackReader),
}
```

`HotFrozenLayer` decodes a frequently used pack into arena-backed ordered indexes and is shared as
`Arc<FrozenLayer>` across workspaces. `Cold` keeps only manifest, Bloom filters, block indexes and a
bounded decoded-block cache. A pack may be promoted to Hot based on access count and memory budget.

The compact S3 bytes and the optimized in-memory layout are deliberately different. Persistence
optimizes transfer/storage; memory layout optimizes lookup and sharing.

## 15. Active-head durability modes

Sealed packs are always durable before publication. The writable MemLayer supports three explicit
modes:

```rust
pub enum HeadDurability {
    Ephemeral,
    LocalJournal,
    S3Journal,
}
```

### Ephemeral

Mutation acknowledgement means visible in process memory. Process or host loss discards unsealed
changes. `seal`, `snapshot` and `commit` still upload and publish a durable pack before returning.
This mode suits disposable agents and gives the lowest write latency.

### LocalJournal

Mutations are group-committed to an fsynced local WAL before acknowledgement. Restart on the same
durable host recovers them. Host loss may lose unsealed changes.

### S3Journal

Mutations are encoded into an immutable WAL segment, uploaded, verified and attached to the workspace
head using CAS before acknowledgement. This survives host loss but adds an S3 group-commit boundary.
Batch size and maximum delay are configurable; correctness never depends on a timing assumption.

The selected mode is recorded in the workspace head and surfaced by status APIs. A caller must not
infer durable acknowledgement in Ephemeral or LocalJournal mode.

## 16. Compact WAL format

A `.brlw` segment contains:

```text
fixed WAL header
compact RecordBatch blocks
footer with wal_hash
```

The header includes volume/workspace/layer IDs, writer epoch, first/last sequence, previous WAL hash,
record count and schema version. Records use the same CompactCodecV1 values as packs, preceded by a
section tag. They are in mutation sequence order, not section-key order.

Every logical transaction is framed:

```text
TxnBegin(txn_id, expected_sequence, operation_count)
Operation(section_tag, key, value)
...
TxnCommit(txn_id, txn_digest)
```

Recovery applies only complete transactions with a valid commit digest. A segment is accepted only
if its sequence range is contiguous with the previous published tail. WAL objects are never appended
or overwritten; a new group commit creates a new segment.

Seal replays the committed tail into the frozen MemLayer snapshot, produces one sorted pack, then
starts a new writer epoch with no WAL tail. Old WAL becomes collectible after the new pack and head
generation are durable.

## 17. Head record and CAS pointer

The immutable head record contains:

```rust
pub struct WorkspaceHeadRecord {
    pub volume_id: VolumeId,
    pub workspace_id: WorkspaceId,
    pub generation: u64,
    pub writer_epoch: u64,
    pub writer_id: [u8; 16],
    pub lease_expires_at_ns: i64,
    pub base: LayerRef,
    pub writable_layer_id: LayerId,
    pub wal_tail: Option<WalRef>,
    pub next_sequence: u64,
    pub durability: HeadDurability,
    pub previous_head_record_hash: Option<[u8; 32]>,
}
```

It is encoded with CompactCodecV1, checksummed and written to `head-records/...` as an immutable
object.

The mutable `.brhp` pointer is intentionally tiny:

```text
magic                   [8]
format_version          u16
generation              u64
head_record_hash        [32]
head_record_len         u64
crc32c                  u32
```

Publication writes and verifies the immutable head record, then conditionally replaces `.brhp` with
`If-Match: old_etag`. CAS failure fences the caller. A writer with an old epoch may upload unreachable
objects, but it cannot publish or acknowledge them as S3-durable.

Lease expiry permits another writer to CAS-publish a higher writer epoch. Every S3Journal flush, seal,
snapshot and commit checks the current pointer ETag/generation. The old writer is harmless after a
successful steal because it cannot satisfy that precondition.

The actor also stops acknowledging mutations when its locally observed lease deadline is reached.
It must renew the lease through head-pointer CAS before resuming. This prevents Ephemeral and
LocalJournal modes from continuing to report successful private writes indefinitely after ownership
has moved to another host.

## 18. Seal protocol

1. Stop accepting new workspace mutations and drain in-flight operations.
2. Publish or fsync the active WAL according to durability mode.
3. Atomically freeze the MemLayer at sequence `N`.
4. Stream its sorted sections into a staging `.brlp` file.
5. Derive Bloom filters, block indexes, logical digest and pack hash.
6. Locally reopen and fully verify the staging pack.
7. Upload the content-addressed pack with create-only semantics.
8. HEAD and range-read the remote footer, directory and header.
9. Create an immutable head record whose base is the new pack and whose writable layer is empty.
10. Upload and verify that head record.
11. CAS the workspace `.brhp` pointer from the captured ETag/generation to the new head record.
12. Publish the new in-memory `WorkspaceView` and resume mutations.

Step 11 is the visibility point. Any failure before it leaves the old view authoritative. Retrying is
idempotent because all prior objects are content-addressed or generation-addressed.

If step 11 succeeds but the process exits before step 12, restart reads the new pointer and reconstructs
the correct view. No multi-object S3 transaction is required.

## 19. Mount and recovery

Mount performs:

1. GET workspace head pointer and capture ETag;
2. GET and validate the referenced immutable head record;
3. follow `base.parent` references to the sealed root, enforcing the depth limit;
4. open packs cold or reuse matching `Arc<FrozenLayer>` cache entries;
5. if a WAL tail exists, walk its hash chain to the epoch start and replay forward;
6. rebuild MemLayer, verify sequence continuity and publish the initial view;
7. acquire or renew writer epoch before accepting mutations.

Recovery never discovers correctness-critical state by listing a prefix. Missing referenced objects,
hash mismatch, a parent cycle, depth overflow, WAL gap or writer-epoch mismatch fails the mount with a
typed corruption/fencing error.

## 20. Reader and transport APIs

```rust
#[async_trait]
pub trait LayerObjectStore: Send + Sync {
    async fn put_create_only(&self, key: &str, body: ReplayableBody)
        -> Result<CreateOutcome, LayerError>;
    async fn get_range(&self, key: &str, range: Range<u64>)
        -> Result<Bytes, LayerError>;
    async fn head(&self, key: &str) -> Result<ObjectMeta, LayerError>;
    async fn get_pointer(&self, key: &str) -> Result<(Bytes, EntityTag), LayerError>;
    async fn put_pointer_conditional(
        &self,
        key: &str,
        expected: Option<&EntityTag>, // None means If-None-Match: *
        body: Bytes,
    ) -> Result<EntityTag, LayerError>;
}
```

```rust
pub trait ReadonlyLayer {
    async fn lookup_dentry(&self, parent: u64, name: &[u8]) -> Result<Option<DentryDelta>>;
    async fn list_dentries(&self, parent: u64, cursor: DentryCursor, limit: usize)
        -> Result<RecordPage<DentryDelta>>;
    async fn get_inode(&self, ino: u64) -> Result<Option<InodeDelta>>;
    async fn list_xattrs(&self, ino: u64) -> Result<Vec<XattrDelta>>;
    async fn list_acls(&self, ino: u64) -> Result<Vec<AclDelta>>;
    async fn get_extents(&self, ino: u64, chunk: u64) -> Result<Vec<DataExtentDelta>>;
    async fn visit_slice_refs(&self, visitor: &mut dyn SliceVisitor) -> Result<()>;
}
```

For RPC transfer, the service returns one or more complete CompactCodecV1 record blocks plus a small
binary batch header. The receiver can validate and decode the same framing used in S3. Metadata is not
converted to JSON, MessagePack or per-record protobuf on the hot path.

## 21. Resolution behavior

The view resolves newest to oldest:

```text
private MemLayer -> sealed pack -> sealed parent pack -> ... -> sealed root pack
```

- point lookup stops at the first matching Put/Whiteout or Present/Deleted record;
- readdir performs a k-way ordered merge and keeps the newest layer's value for each name;
- xattr and ACL Whiteout hide all lower values for the same key;
- the writable MemLayer resolves mutation sequence before publication; sealed packs contain sorted,
  normalized, sequence-free extents and fill uncovered ranges newest-layer first;
- any mutation of a lower inode first writes its effective attributes into MemLayer;
- lower metadata and block objects remain untouched.

Read-only `FrozenLayer` objects can be shared across all agent workspaces because they contain no
workspace-local cache invalidation state.

## 22. Cache and memory policy

```text
ManifestCache: pack_hash -> validated header/directory/index/filter
BlockCache:    (pack_hash, section, block_no) -> Arc<decoded block>
HotLayerCache: pack_hash -> Weak<FrozenLayer>
```

All caches are weighted and bounded. Pack hash is always part of the key. Immutable entries need no
mutation invalidation.

Memory accounting separates:

- shared frozen bytes, charged once per host;
- private MemLayer bytes, charged to one workspace/agent;
- decoded block-cache bytes;
- WAL buffers and staging-pack bytes.

When an agent is deleted, its private MemLayer and WAL buffers can be freed immediately. Shared base
layers remain cached while referenced by other agents.

## 23. Snapshot, fork and commit

Snapshot writes an immutable `.brls` object containing a `LayerRef`, source workspace generation,
optional name/owner metadata and checksum. The snapshot ID or returned object reference is the durable
handle; no KV row is needed.

Fork creates a new workspace head record whose base is the snapshot/revision `LayerRef` and whose
MemLayer/WAL are empty, then creates its `.brhp` pointer with `If-None-Match: *`.

Fast-forward commit requires the target head pointer still reference the expected fork base and
generation. It writes a new immutable target head record and CAS-updates the target pointer. A conflict
returns the current generation and performs no merge.

Naming and listing snapshots are control-plane conveniences. They may be maintained in a separate
append-only registry object stream, but filesystem correctness uses explicit snapshot references.

## 24. GC

GC roots are explicit workspace head pointers and retained snapshot references. Marking follows:

```text
head pointer -> head record -> base pack -> parent packs
                          \-> WAL tail -> previous WAL segments
snapshot -> layer pack -> parent packs
pack -> normalized Extent records -> BrewFS data objects
```

S3 Inventory or a rate-limited prefix listing supplies sweep candidates; listing is not used for
marking relationships. An object is deleted only if:

1. it is absent from the completed mark set;
2. it predates the configured orphan grace period;
3. no active publication journal or local upload reports it in flight;
4. a tombstone has been written and survived a second GC observation.

Shared packs and slices are marked once by hash/ID. WAL segments superseded by a sealed pack are
collectible after the head pointer and all snapshots no longer reference their epoch.

## 25. Feature and module isolation

```toml
[features]
workspace-overlay = []
workspace-memory-s3 = ["workspace-overlay"]
```

```text
src/workspace_overlay/memory_s3/
  mod.rs
  compact_codec.rs
  mem_layer.rs
  frozen_layer.rs
  pack/
    format.rs
    builder.rs
    reader.rs
    index.rs
    filter.rs
  wal/
    format.rs
    writer.rs
    recovery.rs
  head/
    record.rs
    pointer.rs
    lease.rs
  runtime.rs
  object_store.rs
  cache.rs
  gc.rs
  error.rs
```

Rules:

- the module is absent without `workspace-memory-s3`;
- the feature is disabled by default;
- flat BrewFS mounts never instantiate `LayerRuntime` or call the layer object store;
- enabling the feature requires a dedicated metadata S3 prefix and conditional-write capability;
- existing `MetaStore`, `MetaClient`, metadata serialization and block layout are not changed;
- no silent fallback from corrupt S3 layer bytes to another metadata source is allowed.

## 26. Limits

V1 defaults and hard limits:

| Item | Default | Hard limit |
|---|---:|---:|
| raw record block target | 64 KiB | 16 MiB |
| one decoded record | — | 16 MiB |
| layer depth | soft 8 | 32 |
| restart interval | 16 | 256 |
| Bloom bits/key | 10 | 32 |
| RPC record batch | 1 MiB | 16 MiB |
| one WAL segment | 4 MiB | 64 MiB |
| readdir page | 1,024 records | 16,384 records |
| symlink target | existing BrewFS limit | existing BrewFS limit |
| xattr value | existing BrewFS limit | existing BrewFS limit |

All stored lengths are checked against both object bounds and configured hard limits before memory
allocation.

## 27. Error behavior

```rust
pub enum LayerError {
    NotFound,
    AlreadyExistsMismatch,
    UnsupportedVersion { major: u16, minor: u16 },
    UnsupportedFeature(u64),
    UnsupportedCodec(u8),
    InvalidVarint,
    BoundsExceeded { field: &'static str, value: u64, limit: u64 },
    InvalidHeader(String),
    InvalidDirectory(String),
    InvalidBlock(String),
    InvalidRecord(String),
    ChecksumMismatch(ChecksumScope),
    DigestMismatch { expected: [u8; 32], actual: [u8; 32] },
    ParentCycle,
    SequenceGap,
    Fenced { expected_generation: u64, actual_generation: u64 },
    ConditionalWriteUnavailable,
    Backend(String),
    Io(std::io::Error),
}
```

Malformed data always returns a typed error. Decoding uses checked offsets, checked arithmetic and safe
slice access. Logs size-limit and escape names; they never print xattr values, symlink targets or raw
WAL payloads.

## 28. Verification

### Compact codec and pack

- golden bytes for every scalar, record type, restart block, index, filter, header and footer;
- non-canonical and overflow varint rejection;
- deterministic pack bytes for identical records and build options;
- identical logical digest across block size and compression choices;
- insertion-order independence after sorting;
- point/prefix/range lookup across restart and block boundaries;
- empty sections, oversized single-record block and maximum limits;
- arbitrary-byte fuzzing of every decoder;
- truncation/bit-flip tests for every structural region.

### Memory and isolation

- two agents share the same `Arc<FrozenLayer>` but different MemLayers;
- one agent's create/write/rename/unlink/xattr/truncate is invisible to another;
- copy-up never modifies lower metadata or data objects;
- dropping one agent releases only private accounting;
- hot and cold FrozenLayer return byte-for-byte equivalent logical records.

### S3 persistence

- create-only pack/WAL/head-record upload is idempotent;
- mismatched existing object is fatal;
- head CAS has exactly one winner;
- stale writer cannot publish after writer-epoch steal;
- crash before and after every seal step recovers one complete view;
- WAL replay rejects gaps, forks, partial transactions and wrong epochs;
- mount reconstructs a workspace from S3 with no KV service;
- no hot-path S3 LIST calls;
- GC retains shared packs, shared slices, head records and WAL tails.

### BrewFS behavior

- resolver tests cover whiteouts, hardlinks, symlinks, ACLs, xattrs and overlapping Data/Hole extents;
- pjdfstest and selected xfstests run against memory+S3 workspaces;
- existing flat-volume test gates run with the feature absent and enabled-but-unused;
- disabled mounts make zero LayerObjectStore calls.

### Performance

Report separately:

- pack size versus uncompressed logical record bytes;
- metadata bytes transferred for cold lookup, readdir and extent lookup;
- warm lookup latency for MemLayer, Hot FrozenLayer and cached Cold FrozenLayer;
- base-layer memory shared across 1, 10 and 100 agents;
- private metadata bytes per agent;
- seal throughput and peak staging memory;
- S3Journal group-commit latency and requests per mutation;
- mount/recovery time by layer depth and WAL length.

Acceptance requires bounded builder/recovery memory, no material flat-volume regression, and no hidden
durability downgrade. Ephemeral-mode numbers must not be presented as S3-durable write latency.

## 29. Implementation sequence

1. Implement and fuzz `CompactCodecV1` plus prefix-compressed blocks.
2. Implement pack header/directory/index/filter/footer and golden fixtures.
3. Implement streaming pack builder and local full verifier.
4. Implement range-based reader and `FrozenLayer` hot/cold modes.
5. Implement private MemLayer snapshots and resolver integration behind the feature.
6. Implement content-addressed S3 pack upload and immutable layer-chain mount.
7. Implement immutable head records and conditional head pointer updates.
8. Add Ephemeral seal/fork/snapshot first; it has the smallest persistence surface.
9. Add LocalJournal and then S3Journal with transaction framing and recovery.
10. Add GC, crash injection, POSIX suites and matched performance gates.

Steps 1-4 are standalone format work and do not alter mounted filesystem behavior. Step 5 is the first
runtime integration and remains disabled by default.

## 30. Definition of done

1. A sealed base can be mounted from S3 packs with no KV/database service.
2. Multiple agent workspaces share one in-memory FrozenLayer while keeping private MemLayers.
3. Compact blocks can be stored, range-read and transferred without JSON reserialization.
4. Seal publishes a complete pack and new workspace generation through one CAS visibility point.
5. Restart reconstructs the exact selected durability state from explicit S3 references.
6. Stale writers cannot publish after fencing.
7. Corruption is detected without panic, excessive allocation or silent fallback.
8. GC preserves every reachable layer, WAL and data slice.
9. Existing BrewFS behavior and tests remain unchanged when the feature is unused.
