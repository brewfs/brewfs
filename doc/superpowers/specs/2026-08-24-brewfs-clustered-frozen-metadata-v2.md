# BrewFS Clustered Frozen Metadata v2

Status: **RFC / implementation pending** (documentation only; not supported by the current runtime)

Scope: immutable committed base metadata, file-cluster ingestion, S3 persistence, progressive loading,
and private agent overlays

This document supersedes the single-image BRFI v1 design. Its `MUST`/`SHALL` language is normative
for a future implementation, not a statement that the described formats or workflows exist today.

> [!IMPORTANT]
> The current BrewFS runtime cannot read, write, mount, ingest, resume, or publish the `.brfsm`,
> `.brfc`, `.brfds`, or `.brfdp` formats specified here. The modules in section 29, dedicated
> implementation tracking issues, golden fixtures, and executable acceptance tests have not been
> added yet.

## Implementation status and acceptance tracking

This RFC is the design record, not an implementation progress report. No dedicated implementation
issue, clustered-metadata fixture, or executable acceptance test exists yet. Nothing in this
document is evidence that implementation work has started.

The table below is the repository-owned acceptance manifest until implementation begins. It maps
every major unit to its required acceptance contract while recording that implementation tracking is
still missing. A `Missing` evidence state is intentional and must not be interpreted as a passing
test. Planned paths are rendered as code, rather than broken links, because those files do not exist.
When work begins, each row must gain a dedicated implementation issue and a link to committed fixture
or test evidence before its status can change from **RFC / implementation pending**.

| Major format or state transition | Normative definition | Tracking record | Executable evidence status |
|---|---|---|---|
| Snapshot manifest `.brfsm` and immutable head publication | [§3](#3-immutable-publication-boundary) and [§8](#8-snapshot-manifest-format-brfsm) | dedicated implementation issue: **Missing** | **Missing:** [format/determinism](#format-and-determinism) and [equivalence/isolation](#equivalence-and-isolation) are requirements only; planned fixture root: `tests/fixtures/clustered_snapshot/v2/manifest/` |
| Frozen cluster metadata `.brfc`, Merkle indexes, and independently decodable batches | [§9](#9-frozen-cluster-metadata-format-brfc) through [§14](#14-streaming-and-arbitrary-batch-loading) | dedicated implementation issue: **Missing** | **Missing:** [format/determinism](#format-and-determinism) and [streaming/cold replacement](#streaming-and-cold-replacement) are requirements only; planned fixture root: `tests/fixtures/clustered_snapshot/v2/cluster/` |
| Data Seal `.brfds` and the unsealed-to-visible boundary | [§19](#19-metadatadata-split-and-the-data-seal) and [§24](#24-metadata-first-upload-state-machine) | dedicated implementation issue: **Missing** | **Missing:** [upload/crash injection](#upload-and-crash-injection) is a requirement only; planned fixture root: `tests/fixtures/clustered_snapshot/v2/data_seal/` |
| DataPack `.brfdp` encoding and verified frame publication | [§25](#25-data-packing-and-upload) | dedicated implementation issue: **Missing** | **Missing:** [format/determinism](#format-and-determinism) and [upload/crash injection](#upload-and-crash-injection) are requirements only; planned fixture root: `tests/fixtures/clustered_snapshot/v2/data_pack/` |
| Namespace readiness, targeted loading, eviction, and union transitions | [§5](#5-namespace-union-semantics), [§14](#14-streaming-and-arbitrary-batch-loading), and [§15](#15-lookup-readdir-and-getattr-algorithms) | dedicated implementation issue: **Missing** | **Missing:** [cluster union](#cluster-union) and [streaming/cold replacement](#streaming-and-cold-replacement) are requirements only; no executable test exists |
| Source inventory and safe local/archive ingestion | [§20](#20-single-cluster-local-ingestion-interface) through [§23](#23-local-session-layout) | dedicated implementation issue: **Missing** | **Missing:** [ingestion/archives](#ingestion-and-archives) is a requirement only; no executable test exists |
| Upload WAL, multipart reconciliation, and monotonic resume states | [§24](#24-metadata-first-upload-state-machine) and [§26](#26-power-loss-and-multipart-resume-protocol) | dedicated implementation issue: **Missing** | **Missing:** [upload/crash injection](#upload-and-crash-injection) is a requirement only; no executable test exists |
| Private `MemLayer`, shared snapshot caches, and CAS publication | [§17](#17-memory-tiers-and-cold-data-replacement), [§18](#18-agent-overlay-boundary), and [§24](#24-metadata-first-upload-state-machine) | dedicated implementation issue: **Missing** | **Missing:** [equivalence/isolation](#equivalence-and-isolation) is a requirement only; no executable test exists |

Section 30 specifies the acceptance behavior but is not executable evidence. Section 31 is an intended
sequence, not a completed-work checklist. A future implementation PR must replace every **Missing**
state and every planned path above with durable links to its issue and committed evidence.

## 1. Goals

The design must provide all of the following:

1. split a very large committed filesystem into independently managed **file clusters**;
2. allow several clusters to contribute entries to the same mounted directory;
3. keep each committed cluster immutable and independently content-addressable;
4. expose parent directories and their entries before deeper metadata during sequential streaming;
5. load an arbitrary metadata batch without loading all preceding batches;
6. evict decoded cold metadata and reload it without changing inode or directory semantics;
7. keep one private mutable `MemLayer` per agent while sharing all committed cluster caches;
8. build and upload one cluster from a local directory or supported archive;
9. upload cluster metadata before uploading file data;
10. resume safely after process crash, power loss, or a partially completed S3 multipart upload;
11. publish no cluster or snapshot until every referenced data object is durably verified.

The design does not provide in-place mutation of a committed cluster, cross-cluster directory
hardlinks, or visibility of a partially uploaded cluster.

## 2. Terms and object model

```text
WorkspaceHead (CAS mutable pointer)
        |
        v
ClusteredSnapshotManifest (.brfsm, immutable)
        |
        +-- MountTrie
        +-- MergeRouteIndex
        +-- ClusterDescriptor[...]
                 |
                 +-- FrozenClusterMetadata (.brfc, uploaded first)
                 +-- DataSeal             (.brfds, uploaded last)
                             |
                             +-- DataPack / large data objects (.brfdp)
```

Definitions:

- **file cluster**: one immutable namespace fragment plus the data referenced by that fragment;
- **cluster contribution**: one cluster-local representation of a logical directory and some of its
  entries;
- **virtual directory**: the union of one or more cluster contributions with the same `DirKey`;
- **metadata batch**: the independently authenticated, fetched, decoded, and evicted unit;
- **snapshot manifest**: the immutable object that selects clusters and defines their mount points;
- **Data Seal**: the immutable mapping from logical `SliceId`s in metadata to verified S3 data ranges;
- **resident**: decoded metadata is currently in RAM; absence from RAM never means logical absence.

The mounted committed view is one snapshot, not a precedence stack:

```text
private agent MemLayer
          |
          v
union(snapshot clusters selected by one manifest)
```

Cluster union is defined in section 5. A cluster never shadows another committed cluster.

## 3. Immutable publication boundary

A committed snapshot is identified by:

```rust
pub struct SnapshotRef {
    pub manifest_hash: [u8; 32],
    pub superblock_digest: [u8; 32],
    pub object_len: u64,
}
```

The workspace head stores `SnapshotRef` and is updated with compare-and-swap. The manifest references
only sealed clusters. A sealed cluster is:

```rust
pub struct SealedClusterRef {
    pub cluster_id: ClusterId,
    pub metadata: ObjectRef,
    pub data_seal: ObjectRef,
    pub combined_semantic_hash: [u8; 32],
}
```

`combined_semantic_hash` authenticates both namespace structure and file contents:

```text
BLAKE3("BrewFS.SealedCluster.v2" || metadata.semantic_hash || data_seal.semantic_hash)
```

Uploading metadata does not make a cluster mountable. Uploading data does not make it mountable. Only
a verified Data Seal followed by inclusion in a manifest crosses the publication boundary.

No object is rewritten after publication. A new grouping, mount layout, file value, or directory entry
produces new immutable objects and a new manifest.

## 4. Identities

### 4.1 ClusterId and local NodeId

`ClusterId` is 128 bits. It is assigned when the local ingestion session is created and is permanently
recorded in the cluster metadata.

Within one cluster, `LocalNodeId` is a dense non-zero `u32`; root is 1. Dense IDs keep local tables and
varints compact and remove the former whole-snapshot `u32` node limit.

```rust
pub struct NodeRef {
    pub cluster_slot: u32,
    pub local_node_id: u32,
}
```

`cluster_slot` is the descriptor ordinal in one snapshot manifest. It is runtime/snapshot-local and is
not persisted inside a `.brfc` object.

All directory entries for one hardlinked non-directory inode must be placed in the same cluster.
Cross-cluster file hardlinks are rejected by the builder. Directory hardlinks remain invalid.

### 4.2 DirKey

The same logical directory may have a local NodeId in many clusters. `DirKey` is its global merge key:

```rust
#[repr(transparent)]
pub struct DirKey([u8; 16]);
```

For a directory reached through a normal name:

```text
DirKey = BLAKE3-128(
  "BrewFS.DirKey.v2" || volume_id || parent_DirKey || u32_le(name_len) || raw_name
)
```

The volume root key is derived from `volume_id` and a fixed root tag. Raw POSIX name bytes are used;
locale and Unicode normalization are not applied.

Every contribution for one `DirKey` must encode identical directory hot attributes, xattrs, ACLs, and
symlink-invalid status. Repeating directory attributes in the normally small number of contributions
is intentional: any contribution can be loaded independently. A mismatch is snapshot corruption.

A v2 cluster is built for one mount target and cannot be rebound to another path, because descendant
DirKeys include the mounted parent key.

### 4.3 Runtime FUSE inode

FUSE inode identity must not require a ten-billion-row interner. v2 uses collision-free snapshot-local
packing:

```text
bit 63 = 1  private MemLayer inode

bit 63 = 0  committed inode
bits 62..61 committed tag:
  00 non-directory: bits 60..32 cluster_slot (29 bits), bits 31..0 LocalNodeId
  01 virtual directory backed by clusters: canonical contributor in the same 29+32 packing
  10 manifest-only mount directory: bits 31..0 MountTrieNodeId, upper payload bits zero
  11 reserved
```

The manifest is limited to fewer than `2^29` cluster slots. For a merged directory, the canonical
contributor is the lowest unsigned `(cluster_slot, LocalNodeId)` in its authenticated route record.
For a single-contributor directory, that contributor is canonical. A MountTrie node takes precedence
as the identity when the directory is explicitly represented by the manifest. These choices are
deterministic for one SnapshotRef and require no hash or collision table.

An active kernel lookup entry may copy `InodeHot` and a reloadable `RecordLocator`; it never contains a
raw pointer into an evictable batch. It is released after the kernel sends FORGET and all open-handle
references reach zero. Readdir can emit deterministic inode numbers without permanently retaining all
visited files.

## 5. Namespace union semantics

### 5.1 Contributions

The manifest may attach many cluster roots to the same virtual mount directory. Each attached root is
a contributor. Descending into a child directory carries forward every matching directory
contribution.

Example:

```text
cluster A at /workspace: src/a.rs, data/a.parquet
cluster B at /workspace: src/b.rs, models/m.bin

mounted result:
/workspace/src/a.rs
/workspace/src/b.rs
/workspace/data/a.parquet
/workspace/models/m.bin
```

Both `src` entries have the same `DirKey`, so they form one virtual directory.

### 5.2 Collision rules

For the same `(parent DirKey, raw name)`:

- zero matches means absent;
- exactly one non-directory match selects that file;
- one or more directory matches with the same derived child `DirKey` merge;
- two non-directory matches are invalid, even if their data bytes are equal;
- directory plus non-directory is invalid;
- differing directory attributes or child DirKeys are invalid.

There is no cluster priority and no last-writer-wins rule. This makes grouping independent of visible
filesystem semantics. The private MemLayer may intentionally replace or delete the merged result.

The snapshot builder performs a complete conflict validation before publication. Runtime repeats the
relevant check when lazily materializing a merged name and returns `EIO`, never an arbitrary winner, if
the authenticated objects disagree.

### 5.3 Ancestor skeletons

If a cluster contributes `/a/b/file`, it must contain directory contribution records for its mounted
root, `a`, and `b`. These skeleton records make contributor discovery recursive and remove the need for
a global entry-per-file routing database.

The cluster packer should minimize duplicated skeletons, but they are semantically required.

## 6. Merge routing without per-file duplication

Querying every root cluster for every filename is correct but can become expensive. The manifest has a
paged `MergeRouteIndex` only for virtual directories with more than one contributor.

```rust
pub struct MergeRouteRecord {
    pub dir_key: DirKey,
    pub contributors: Vec<DirContributor>,
    pub route_bucket_bits: u8,       // 0..=12
    pub bucket_offsets: Vec<u32>,
    pub candidate_ordinals: Vec<u16_or_u32>,
}

pub struct DirContributor {
    pub cluster_slot: u32,
    pub local_dir_node_id: u32,
}
```

The route hash is:

```text
low_bits(BLAKE3("BrewFS.Route.v2" || snapshot_route_seed || raw_name))
```

For each hash bucket, the record contains every contributor that owns at least one name in that
bucket. It is an exact candidate set at bucket granularity, not a probabilistic filter, so it cannot
create false negatives. Hash collisions may create extra cluster lookups but never incorrect results.

Rules:

- a multi-contributor directory must have one authenticated route record;
- contributor lists in the route record must equal contributor discovery from the cluster data;
- a single-contributor directory uses the contributor directly and has no route record;
- `route_bucket_bits=0` means query all contributors;
- the default is 8 bits; a builder may choose up to 12 bits for high fan-out directories;
- the route index is derived acceleration data and is included in the manifest physical hash;
- collision validation still runs independently of routing.

For large merged directories, the packer should assign names to clusters by contiguous route-hash
bucket ranges. Then a lookup normally selects one cluster while related directory paths can still be
distributed across different cluster objects. Arbitrary cluster grouping remains correct but can
degrade to fan-out reads.

The route index stores one record per merged directory, not one record per file.

## 7. Cluster planning and placement

The packing unit is an inode equivalence class, not an individual pathname. All hardlinks, inode hot
attributes, extents, and cold attributes for one file move together.

Default planner objectives:

- target 256 MiB to 2 GiB of stored metadata per cluster;
- cap local nodes below `u32::MAX - 1`;
- cap one cluster's page-index height and batch count;
- preserve subtree locality when requested;
- use route-hash ranges for very large shared directories;
- avoid placing all hot roots into one oversized cluster;
- keep directory skeleton overhead below a configured fraction, default 5%;
- create a new cluster rather than violate a hard limit.

Planner modes:

```text
Subtree        keep subtrees together until the target size
HashRange      partition names in selected directories by route hash
SizeBalanced   distribute inode groups by estimated metadata/data size
Explicit       caller supplies include rules and mount root
```

The planner writes a deterministic `ClusterPlan` before encoding. Repeated builds with identical
source inventory, options, and ClusterIds produce identical metadata bytes.

## 8. Snapshot manifest format (`.brfsm`)

Magic: `BRFSM002`

The manifest contains:

```text
4096-byte superblock
MountTrieIndexRoot
ClusterTableIndexRoot
MergeRouteIndexRoot
paged MountTrie
paged ClusterTable
paged MergeRouteIndex
footer
```

The superblock includes:

```rust
pub struct SnapshotSuperblock {
    pub volume_id: [u8; 16],
    pub snapshot_id: [u8; 16],
    pub semantic_hash: [u8; 32],
    pub route_seed: [u8; 16],
    pub cluster_count: u32,
    pub mount_count: u32,
    pub merged_directory_count: u64,
    pub root_dir_key: DirKey,
    pub index_roots: [MerkleRootRef; 3],
}
```

Each cluster descriptor records:

```rust
pub struct ClusterDescriptor {
    pub cluster_id: ClusterId,
    pub metadata_ref: ObjectRef,
    pub data_seal_ref: ObjectRef,
    pub combined_semantic_hash: [u8; 32],
    pub mount_dir_key: DirKey,
    pub root_local_node_id: u32,       // always 1 in v2
    pub flags: u32,
}
```

The mount trie maps raw path components to an ordered, duplicate-free list of cluster slots. Its root
record is small and pinned for the mount lifetime. Deeper trie pages load on demand.

Object key:

```text
brewfs-meta/v2/volumes/{volume_id}/snapshots/{manifest_hash[0]:02x}/{manifest_hash}.brfsm
```

## 9. Frozen cluster metadata format (`.brfc`)

Magic: `BRFCL002`

One `.brfc` object contains no file data and no final S3 data locations. Extents refer only to
cluster-local `SliceId`s.

Physical order:

```text
+--------------------------------------+ offset 0
| 4096-byte ClusterSuperblock          |
+--------------------------------------+
| Merkle index nodes                   | small roots first
+--------------------------------------+
| Namespace batches                    | BFS parent-first
+--------------------------------------+
| Extent batches                       | LocalNodeId order
+--------------------------------------+
| Attribute batches                    | LocalNodeId order
+--------------------------------------+
| Footer                               |
+--------------------------------------+
```

The superblock includes:

```rust
pub struct ClusterSuperblock {
    pub format_major: u16,             // 2
    pub format_minor: u16,
    pub cluster_id: ClusterId,
    pub volume_id: [u8; 16],
    pub mount_dir_key: DirKey,
    pub metadata_semantic_hash: [u8; 32],
    pub build_options_digest: [u8; 32],
    pub node_count: u32,
    pub directory_contribution_count: u32,
    pub dentry_count: u64,
    pub extent_count: u64,
    pub slice_count: u64,
    pub namespace_batch_count: u32,
    pub extent_batch_count: u32,
    pub attribute_batch_count: u32,
    pub chunk_size: u64,
    pub index_roots: [MerkleRootRef; 4],
}
```

The four Merkle indexes are:

1. namespace `(parent LocalNodeId, raw name) -> BatchLocator`;
2. node introduction `LocalNodeId range -> namespace BatchLocator`;
3. extent `LocalNodeId range -> BatchLocator`;
4. attribute `LocalNodeId range -> BatchLocator`.

Object key:

```text
brewfs-meta/v2/clusters/{cluster_id}/metadata/{object_hash[0]:02x}/{object_hash}.brfc
```

## 10. Merkle indexes

An entire index must not be loaded to open a cluster. Every index is a 4096-byte-node immutable B+
tree. The superblock stores the root offset, length, level, key kind, and BLAKE3 digest. Each internal
entry authenticates its child node's offset, length, first/last key, and digest.

Index properties:

- root nodes are pinned while the cluster handle is open;
- internal and leaf nodes are cached independently;
- names in namespace leaf nodes use restart-point prefix compression;
- a restart point occurs at least every 16 entries;
- one node can be decoded without its previous sibling;
- leaf sibling offsets permit sequential scan but are not trusted without their parent digest;
- all lengths and offsets are checked before allocation or Range GET;
- root-to-leaf validation is sufficient to trust one `BatchLocator` without loading other leaves.

`BatchLocator` contains:

```rust
pub struct BatchLocator {
    pub kind: BatchKind,
    pub batch_id: u32,
    pub stream_ordinal: u32,
    pub object_offset: u64,
    pub total_stored_len: u32,       // 128-byte header plus stored payload
    pub raw_payload_len: u32,
    pub first_new_node_id: u32,
    pub new_node_count: u32,
    pub digest: [u8; 32],
}
```

For namespace leaves, the logical range also includes full first and last `(parent, name)` keys. For
extent and attribute leaves, it is a LocalNodeId range.

## 11. Metadata batch format

A batch is the S3 fetch, authentication, decompression, decoded-cache, and eviction unit. It is not a
database page and never changes after upload.

Default raw targets:

| Batch kind | Default target | Hard maximum |
|---|---:|---:|
| Namespace | 1 MiB | 16 MiB |
| Extent | 1 MiB | 16 MiB |
| Attribute | 512 KiB | 16 MiB |

Each batch has a 128-byte header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic `BRFCBT02` |
| 8 | 2 | batch kind |
| 10 | 2 | batch version |
| 12 | 4 | flags |
| 16 | 16 | ClusterId |
| 32 | 4 | batch ID within kind |
| 36 | 4 | physical stream ordinal |
| 40 | 4 | predecessor ordinal or `0xffffffff` |
| 44 | 4 | logical record/group count |
| 48 | 8 | raw payload length |
| 56 | 8 | stored payload length |
| 64 | 4 | first newly introduced LocalNodeId |
| 68 | 4 | newly introduced node count |
| 72 | 1 | codec: None=0, Zstd=1 |
| 73 | 7 | reserved zero |
| 80 | 4 | stored payload CRC32C |
| 84 | 4 | header CRC32C over bytes `[0,84)` |
| 88 | 8 | first logical-key fingerprint |
| 96 | 32 | BLAKE3 of header `[0,96)` plus stored payload |

The authenticated index contains the complete logical boundary; the header fingerprints catch wrong
range responses cheaply. Decoders verify index digest, header CRC, payload CRC, decompressed length,
and complete payload consumption before publication.

Every batch is self-contained:

- integer delta state resets at batch start;
- filename prefix compression resets at batch start and at restart points;
- the full parent LocalNodeId appears in the first directory segment;
- a namespace batch declares its exact new-node range;
- targeted decode does not require the predecessor batch;
- predecessor is required only to advance a sequential streaming frontier.

Large directory segments may continue in adjacent batches. A targeted segment carries its parent,
first name, entry count, continuation flags, and the directory's total entry count or authenticated
unknown marker. It can answer indexed lookup without materializing earlier name ranges.

## 12. Namespace encoding and parent-first order

Within one cluster, LocalNodeIds and directory groups are assigned breadth-first:

1. root contribution is LocalNodeId 1 and is the first logical record;
2. directory groups are processed in LocalNodeId order;
3. names are raw-byte sorted within each contribution;
4. a new child inode record immediately follows its introducing dentry;
5. a child directory group occurs after its parent dentry;
6. hardlinks refer only to an earlier introduced local node;
7. all segments of one local directory are physically contiguous;
8. each batch boundary resets its compact decoding state.

A directory inode record includes its `DirKey`. Non-directory inode records do not.

```text
directory_segment:
  parent_local_node_id  uvarint
  parent_dir_key        16 bytes
  flags                 u8       // START, END, CONTINUATION
  total_entry_count     uvarint   // on START
  first_name            bytes
  entry_count           uvarint

  repeated entries:
    shared_name_bytes   uvarint
    name_suffix         bytes
    entry_tag           u8
    if NEW_NODE:
      inode_record      bytes
    else:
      existing_local_node_id uvarint
```

For a new directory, `inode_record` contains the derived child DirKey and complete directory
attributes. The loader verifies derivation from the parent DirKey and raw name.

The namespace stream is useful immediately:

```text
cluster root
root name prefixes + inline child attributes
depth-1 contribution prefixes
depth-2 contribution prefixes
...
```

Parent-first is a physical streaming property, not a residency promise. Previously streamed batches
may later be evicted.

## 13. Extent and attribute encoding

Extent groups remain complete final file maps ordered by LocalNodeId:

```text
file_group:
  local_node_id
  extent_count
  repeated:
    gap_from_previous_end
    logical_length
    slice_id
    slice_offset
```

Gaps are holes. No layer, transaction, Whiteout, or historical overwrite is stored. An extent cannot
cross the configured BrewFS logical chunk boundary. `SliceId` is cluster-local, non-zero, and resolved
through the Data Seal only after publication.

Attribute groups contain symlink target, xattrs, and ACLs in LocalNodeId order. Directory cold
attributes are repeated in each contribution and must compare equal during snapshot validation.

Extent and attribute batches are independently indexed and may be loaded or evicted without their
namespace batch. Namespace hot inode records state whether a corresponding cold group exists.

## 14. Streaming and arbitrary batch loading

The loader exposes two complementary paths over the same format.

### 14.1 Sequential stream path

Used for mount warm-up, root-first traversal, prefetch, and full scans:

1. open the manifest and root contributors;
2. open selected cluster superblocks and index roots;
3. Range-stream namespace batches in physical ordinal order;
4. validate a batch before exposing any of it;
5. publish sorted directory prefixes;
6. continue while the warm-up byte, depth, or time budget remains;
7. optionally stream extent/attribute batches for nodes selected by the prefetch policy.

For a virtual directory with several contributors, its globally publishable prefix frontier is the
minimum raw name frontier reached by all contributors. A sequential warm-up must not return negative
answers beyond that global frontier.

### 14.2 Targeted batch path

Used for cache misses and random lookup:

1. use `DirKey` to obtain the merge route record when needed;
2. hash the name and select exact candidate contributors;
3. traverse each cluster's pageable namespace index;
4. fetch the indicated batch through one Range GET;
5. validate and decode the complete batch;
6. resolve a matching dentry or authoritative absence in that indexed name interval;
7. load an introduction batch if a hardlink target's hot inode is not resident;
8. insert the decoded batch into the shared cache.

Targeted loading never advances an unrelated stream frontier. Sequential and targeted loads use the
same singleflight cache key, so they cannot decode duplicate copies concurrently.

### 14.3 Residency is not readiness

The old `Introduced/Streaming/Complete` state is replaced with separate concepts:

```rust
pub struct DirectoryKnowledge {
    pub contributors: Arc<[DirContributor]>,
    pub authenticated_route: Option<RouteLocator>,
    pub stream_frontiers: FrontierSet,
}

pub enum BatchState {
    Absent,
    Loading(SharedWait),
    Resident(Arc<DecodedBatch>),
    Failed(RetryableError),
}
```

An immutable directory is logically complete as soon as its authenticated index and contributor set
are known. Its contents need not be resident. `Complete` must never be used as a synonym for “all
batches are in RAM.”

## 15. Lookup, readdir, and getattr algorithms

### 15.1 Lookup

```text
1. consult the private MemLayer (including Whiteout)
2. resolve the virtual directory's contributors
3. consult MergeRouteIndex if contributor count > 1
4. load the target namespace batch from every exact route candidate
5. zero matches after all candidates were checked => ENOENT
6. one file match => intern NodeRef and return it
7. one or more matching directories => verify equality, intern DirKey, return merged directory
8. any committed collision => EIO
```

An authenticated empty candidate set is an immediate negative answer. Otherwise negative lookup is
cached only after all candidates for that hash bucket were checked. The negative cache key includes
`SnapshotRef`, so entries remain valid for that immutable snapshot and cannot leak across head changes.

### 15.2 Readdir

Each contributor supplies a sorted batch cursor. Readdir performs a k-way merge by raw name:

- equal directory names collapse into one result after equality validation;
- equal file names or file/directory pairs are corruption;
- private MemLayer additions/replacements/Whiteouts are merged last according to overlay semantics;
- only current and next contributor batches are pinned;
- batches behind all cursors are immediately eligible for eviction;
- reaching an unloaded boundary fetches the next indexed batch.

A FUSE directory handle stores a stable bookmark `(last_raw_name, tie_state)` rather than an arena
offset. Handle-local cookies map to bookmarks. After eviction or retry, the cursor resumes by indexed
lower-bound search. The namespace is immutable, so a bookmark cannot be invalidated during the mount.

### 15.3 Getattr and read

The mount inode table copies the small `InodeHot` record for identities already returned to FUSE.
Therefore getattr for an active inode does not pin its namespace batch. The inode table also retains a
`RecordLocator` for revalidation and cold attributes.

Read resolves `SliceId` through the pinned Data Seal index root, loads only extent batches needed for
the requested file/range, creates a bounded read plan, and releases the extent batch pin after the plan
has copied its spans.

## 16. Shared runtime structures

```rust
pub struct ClusteredSnapshot {
    pub identity: SnapshotIdentity,
    pub manifest: Arc<SnapshotManifest>,
    pub clusters: ClusterRegistry,
    pub batch_cache: Arc<MetadataBatchCache>,
    pub index_cache: Arc<MerkleIndexCache>,
    pub data_seals: DataSealRegistry,
}

pub struct ClusterHandle {
    pub descriptor: ClusterDescriptor,
    pub superblock: Arc<ClusterSuperblock>,
    pub index_roots: ClusterIndexRoots,
}

pub struct RecordLocator {
    pub cluster_slot: u32,
    pub batch_kind: BatchKind,
    pub batch_id: u32,
    pub record_ordinal: u32,
}
```

All agents using the same SnapshotRef share `Arc<ClusteredSnapshot>`, cluster handles, index nodes,
decoded batches, Data Seal nodes, and S3 singleflight requests. Only `MemLayer`, active kernel
lookup/open-handle state, and private dirty data are agent-specific.

No long-lived structure points directly into a decoded batch without holding its `Arc` pin. Stable
references are IDs and locators.

## 17. Memory tiers and cold-data replacement

Metadata uses four tiers:

```text
L0 pinned RAM: manifest roots, mount root, open cluster superblocks, active kernel inode hot records
L1 RAM cache: decoded Merkle nodes and decoded metadata batches
L2 local disk cache: authenticated compressed batch bytes
L3 S3: immutable source objects
```

L1 has separate accounting for namespace, extent, attribute, route, and index data. Default soft
shares are 55%, 25%, 10%, 5%, and 5%; unused capacity is borrowable. Operators configure a single hard
byte budget plus optional per-class floors.

Eviction uses size-aware segmented LRU:

- a first demand hit enters probationary;
- a second hit promotes to protected;
- sequential scan/prefetch batches enter probationary and do not promote merely because the scanner
  touched them;
- index roots and current directory cursor batches are pinned;
- an entry is evictable only when no operation/handle owns an `Arc` pin and no load waiter exists;
- eviction drops decoded data but retains its compact locator and optional L2 bytes;
- dropping a batch never changes route knowledge, inode identity, or negative-answer validity;
- failed entries have bounded exponential retry state and are not retained as permanent negatives.

L2 is an optional byte-bounded clock/LRU cache keyed by `(object hash, offset, stored length, digest)`.
Files are written to a temporary name, fsynced when configured, verified, then atomically renamed. A
torn cache file is a miss, not corruption of the snapshot.

Admission control prevents one large readdir or background scrub from evicting the active working set.
Background reads stop when either the RAM cache or S3 concurrency budget is saturated.

## 18. Agent overlay boundary

Every agent sees:

```text
private MemLayer -> immutable ClusteredSnapshot
```

The MemLayer may add and delete names, replace a merged base entry, override inode fields and cold
attributes, and add Data/Hole extents. It does not mutate a cluster cache or manifest.

Committing an agent view freezes its MemLayer, resolves the effective namespace, replans affected file
clusters, uploads new sealed clusters, creates a new snapshot manifest, and finally CAS-switches the
workspace head. Unchanged sealed cluster descriptors are reused byte-for-byte.

The entire clustered implementation remains behind a dedicated Cargo feature and module boundary; the
existing BrewFS metadata path is unchanged when the feature is disabled.

## 19. Metadata/data split and the Data Seal

Metadata-first upload is possible because `.brfc` extents refer to logical `SliceId`, not an S3 key or
physical pack offset. After data upload, `.brfds` binds every SliceId to verified bytes.

Magic: `BRFDS002`

```rust
pub struct SliceDescriptor {
    pub slice_id: u64,
    pub logical_len: u64,
    pub spans: Vec<DataSpan>,
}

pub struct DataSpan {
    pub frame_ordinal: u32,
    pub raw_offset_in_frame: u32,
    pub raw_len: u32,
}

pub struct FrameDescriptor {
    pub object_ordinal: u32,
    pub object_offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub codec: u8,
    pub frame_checksum: [u8; 16],
}
```

The Data Seal contains a pageable Merkle index from SliceId to compact span lists, a pageable frame
table, and an object table:

```rust
pub struct DataObjectDescriptor {
    pub object_key: bytes,
    pub object_len: u64,
    pub object_checksum: [u8; 32],
    pub etag: bytes,                 // diagnostic, not trusted as content hash
}
```

The seal validates:

- every SliceId referenced by `.brfc` exists exactly once;
- no unreferenced SliceId is present unless explicitly marked shared;
- logical lengths agree with extents;
- spans are ordered, in frame bounds, and sum to logical length;
- object and frame checksums are present;
- all data objects passed remote verification before seal creation.

A checksum is stored once per DataPack object and once per independently readable frame, not once per
file. Slice spans inherit frame authentication. An optional per-slice content hash may be enabled for a
deduplication workflow, but it is absent in the compact default because 32 bytes per small file would
add 32 GB for one billion files.

The final snapshot semantic hash includes Data Seal semantic hashes. Metadata-only identity is not
treated as file-content identity.

## 20. Single-cluster local ingestion interface

All sources implement:

```rust
#[async_trait]
pub trait ClusterSource {
    async fn inventory(&mut self, sink: &mut dyn InventorySink) -> Result<SourceFingerprint>;
    async fn open_range(&self, token: &ReopenToken, offset: u64, len: u64)
        -> Result<Box<dyn AsyncRead + Unpin + Send>>;
    async fn revalidate(&self, token: &ReopenToken, expected: &EntryFingerprint)
        -> Result<()>;
}
```

The inventory stream produces raw path bytes, inode kind, mode/uid/gid/timestamps, size, hardlink key,
symlink target, sparse ranges, xattrs/ACLs according to policy, and a durable reopen token.

Built-in adapters:

```text
LocalFsSource       local directory tree
TarSource           uncompressed seekable tar
ZipSource           seekable ZIP using its central directory
TarZstdSource       seekable-zstd when an index is present; otherwise spool mode
TarGzipSource       sequential compressed archive; spool mode by default
```

The first implementation must support local directory, tar, tar.gz, tar.zst, and ZIP. Encrypted
archives, multi-volume archives, device-node payloads, and archive formats without a safe bounded
decoder are rejected in v2.

The local command surface is:

```text
brewfs cluster ingest --source <dir-or-archive> --mount <path> --session-dir <dir>
brewfs cluster resume --session-dir <dir>
brewfs cluster status --session-dir <dir>
brewfs cluster verify --session-dir <dir>
brewfs cluster publish --session-dir <dir> --workspace <id>
```

`ingest` stops at a complete sealed but unpublished cluster unless `--publish` is explicitly supplied.

## 21. Local filesystem inventory rules

Local filesystem ingestion uses descriptor-relative traversal (`openat` style on Unix) and does not
follow symlinks by default. It records a fingerprint sufficient to detect mutation between inventory
and data read:

```text
device, inode, type, size, mtime_ns, ctime_ns, generation when available
```

Rules:

- read raw filename bytes and reject only NUL, slash within a component, `.` and `..`;
- group hardlinks by `(device, inode, generation)`;
- record sparse ranges using `SEEK_DATA/SEEK_HOLE` where supported;
- re-stat before and after reading each regular file;
- abort sealing if the fingerprint changes;
- preserve symlink text without following it;
- reject sockets; device/FIFO handling is disabled unless explicitly allowed;
- cap xattr, ACL, path depth, and single-value sizes before allocation;
- prefer a filesystem snapshot as source for a large production ingest.

Metadata-first requires an inventory pass before any data PUT. Reading file bytes for optional
prehashing is allowed during the inventory phase, but no data object may be uploaded until remote
metadata verification succeeds.

## 22. Archive inventory and safe extraction

Archive ingestion reads metadata directly from headers; it does not materialize the original directory
tree. Paths are normalized as raw relative components and validated before insertion:

- reject absolute paths, drive prefixes, NUL, and any `..` escape;
- define an explicit duplicate-path policy; the default is reject, not last entry wins;
- resolve archive hardlinks only within the same archive and cluster;
- do not follow archive symlinks while scanning;
- enforce maximum entries, logical bytes, path depth, name length, expansion ratio, and decoder memory;
- verify archive checksums/CRC where provided;
- reject encrypted entries unless a future authenticated adapter implements them;
- preserve ZIP UTF-8 names; non-UTF-8 ZIP names require an explicit decoding policy recorded in build
  options.

Metadata-first plus crash-resumable data upload requires every file payload to have a durable
`ReopenToken` after inventory:

- uncompressed tar uses archive byte offset and length;
- ZIP uses central-directory entry plus local-header validation;
- seekable zstd uses frame/checkpoint plus uncompressed offset;
- non-seekable tar.gz/tar.zst uses a local durable spool by default;
- `--no-spool` may deterministically replay from archive start, but resume cost is O(compressed prefix)
  and is not the production default.

Spool mode writes normalized data frames into the session directory while scanning. Frames are
checksummed and fsynced before their reopen token is committed to the upload journal. This is still
metadata-first with respect to S3: local spooling is preparation, not remote data publication.

## 23. Local session layout

```text
<session-dir>/
  session.header                 fixed identity and source fingerprint
  cluster.brfc                   complete local metadata object
  upload.plan                    deterministic SliceId/DataPack plan
  upload.snapshot               compact journal checkpoint
  upload.wal                    append-only framed journal
  spool/                        optional archive/data frames
  parts/                        optional reconstructed multipart parts
```

The session directory is never the source directory. Creation requires an empty dedicated directory.
Every durable file is written through temp-file + flush + optional fsync + atomic rename. The parent
directory is fsynced on platforms that support it.

`upload.wal` records are:

```text
magic | record_len | sequence | record_kind | payload | CRC32C
```

Replay stops at the first torn or invalid tail record. A compact snapshot is written atomically, then a
new WAL generation begins. The uploader never edits a previous record in place.

## 24. Metadata-first upload state machine

```text
NEW
  -> INVENTORY_COMPLETE
  -> METADATA_LOCAL_VERIFIED
  -> METADATA_REMOTE_VERIFIED
  -> DATA_UPLOADING
  -> DATA_REMOTE_VERIFIED
  -> SEAL_LOCAL_VERIFIED
  -> SEAL_REMOTE_VERIFIED
  -> CLUSTER_SEALED
  -> SNAPSHOT_PUBLISHED (optional)
```

Transitions are monotonic and journaled. `DATA_UPLOADING` is illegal before
`METADATA_REMOTE_VERIFIED`.

Detailed pipeline:

1. allocate UploadSessionId, ClusterId, SliceIds, source options, and a session directory;
2. inventory/revalidate namespace metadata and create the deterministic ClusterPlan;
3. build `cluster.brfc` locally, fully scrub it, and fsync it;
4. PUT `.brfc` with create-only semantics, using the resumable multipart engine when it exceeds the
   single-PUT threshold;
5. HEAD/range-read the remote footer, superblock, index roots, and sampled/all batches according to
   verification policy;
6. only then begin DataPack or large-object multipart uploads;
7. verify every completed data object against its strong checksum;
8. build and scrub `.brfds` from the completed object table;
9. PUT and remotely verify `.brfds` create-only, also using resumable multipart when needed;
10. construct `SealedClusterRef` and mark the local session `CLUSTER_SEALED`;
11. when requested, build a conflict-validated new snapshot manifest and CAS-publish WorkspaceHead.

If any phase fails, no manifest references the incomplete cluster. Uploaded immutable metadata and
data remain staging/orphan candidates until resumed or garbage-collected.

## 25. Data packing and upload

Small files and file ranges are packed into immutable DataPack objects. Large files may use dedicated
objects but use the same frame and seal model.

Magic: `BRFDP002`

Defaults:

| Item | Default |
|---|---:|
| DataPack target size | 4 GiB |
| raw frame target | 8 MiB |
| multipart part target | 64 MiB |
| small-file threshold | 8 MiB |
| compression | per-frame adaptive Zstd/None |

Each frame is independently checksummed and optionally compressed. A file may span frames; one frame
may contain many small files. The Data Seal, rather than an in-object global index, supplies the exact
SliceId-to-frame ranges needed for reads.

Packing is deterministic from `upload.plan`:

- SliceIds and logical lengths are assigned before metadata upload;
- pack/object ordinals and input order are fixed before data upload;
- stored frame offsets and checksums are learned while encoding and placed in the later Data Seal;
- a multipart part is reproducible from source reopen tokens and the deterministic codec options;
- frames do not cross multipart part boundaries unless a single frame exceeds the target;
- a sparse hole emits no data bytes and remains a metadata gap.

Two object-key modes are supported:

```text
SessionKey    one-pass upload under UploadSessionId; Data Seal provides content checksums
ContentHash   prehash/spool first, deduplicate with HEAD, upload under content hash
```

`SessionKey` is the default for PB-scale local ingestion because it avoids reading all data twice.
Objects remain immutable and are permanently authenticated by the seal. S3 ETag is never used as the
sole content checksum, especially for multipart uploads.

## 26. Power-loss and multipart resume protocol

For every remotely uploaded object, including `.brfc`, every DataPack/large object, and `.brfds`, the
journal stores the same multipart state. Metadata and seal objects use their local immutable file as
the deterministic part source:

```rust
pub struct MultipartState {
    pub object_ordinal: u32,
    pub object_key: bytes,
    pub upload_id: bytes,
    pub deterministic_plan_digest: [u8; 32],
    pub completed_parts: Vec<CompletedPart>,
}

pub struct CompletedPart {
    pub part_number: u32,
    pub raw_source_digest: [u8; 32],
    pub stored_len: u64,
    pub strong_checksum: [u8; 32],
    pub etag: bytes,
}
```

Resume procedure:

1. replay the last valid local snapshot and WAL;
2. verify session identity, build options, `.brfc` hash, and source/archive fingerprint;
3. HEAD the metadata object; upload/verify it again if the previous phase was not durable;
4. call `ListParts` for every open multipart upload;
5. merge remote parts with locally journaled parts by part number, length, ETag, and strong checksum;
6. accept a remote-only part only after its deterministic source/encoded checksum is reconstructed or
   verified through S3 checksum metadata;
7. re-upload missing or ambiguous parts; multipart PUT is idempotent by part number;
8. after all parts exist, journal completion intent, call CompleteMultipartUpload, then HEAD and verify
   the final object;
9. if power failed after remote completion but before local acknowledgement, HEAD reconstructs the
   completed state;
10. continue seal construction/publication from the first incomplete monotonic state.

If power fails before `INVENTORY_COMPLETE`, resume discards any uncommitted derived metadata records,
revalidates the source root/archive, and resumes from the last durable inventory/spool checkpoint or
restarts inventory. No data PUT can exist at this point. If the local source fingerprint has changed,
the old session cannot be sealed and a new session is required.

The local WAL is sufficient for ordinary power loss where the session disk survives. To survive loss
of the uploader host, the uploader periodically writes immutable remote checkpoint objects:

```text
brewfs-upload/v2/sessions/{upload_session_id}/checkpoints/{sequence}.checkpoint
```

The checkpoint contains no credentials and is authenticated by the session secret/digest. A new host
loads the greatest valid sequence, then reconciles S3 multipart state. Checkpoints are advisory; the
final truth is immutable objects plus S3 multipart/HEAD state.

An expired session is not automatically aborted by a normal resume path. A separate GC policy may
abort multipart uploads and delete unreferenced SessionKey objects only after a configured retention
window and a manifest reachability check.

## 27. Source consistency and failure behavior

- Source mutation before sealing aborts the affected cluster; it is never silently accepted.
- A remote metadata mismatch permanently fails that object key and requires a new ClusterId/session.
- A corrupt batch is never published to readers or inserted into L1/L2 as valid.
- A missing cold batch blocks only operations that require it.
- Cancellation releases pins but does not discard a valid shared load needed by other waiters.
- Partial directory decoding never creates an authoritative negative outside the validated key range.
- A Data Seal whose object HEAD/checksum validation fails cannot enter a snapshot manifest.
- After publication, loss of a referenced S3 object is an I/O integrity failure, not an empty file.
- The loader never falls back to a mutable KV metadata backend for a clustered snapshot.

## 28. Resource limits

Defaults are configurable; hard limits are decoder safety boundaries.

| Item | Default | Hard limit |
|---|---:|---:|
| nodes per cluster | planner target | `u32::MAX - 1` |
| cluster slots per snapshot | planner target | `2^29 - 1` |
| contributors per merged directory | 16 target | 65,535 |
| route bucket bits | 8 | 12 |
| metadata batch raw bytes | kind default | 16 MiB |
| Merkle index node | 4096 B | 64 KiB |
| raw name bytes | platform limit | 64 KiB format limit |
| path depth | 1,024 | 65,535 |
| xattr value | policy | 16 MiB |
| archive expansion ratio | 100:1 | configured mandatory limit |
| concurrent S3 GET | 64 | configured |
| concurrent multipart PUT | 16 | configured |

The builder estimates route and skeleton overhead before accepting a plan. Decoders perform checked
integer arithmetic and bound all allocations before decompression.

## 29. Required implementation modules

The following paths are planned. They are not present in the current runtime and must not be read as
an inventory of implemented modules.

```text
src/workspace_overlay/clustered_snapshot/
  mod.rs
  identity.rs
  snapshot_manifest.rs
  mount_trie.rs
  merge_route.rs
  cluster_format.rs
  batch.rs
  merkle_index.rs
  namespace.rs
  extent.rs
  attribute.rs
  data_seal.rs
  data_pack.rs
  stream_loader.rs
  targeted_loader.rs
  batch_cache.rs
  disk_cache.rs
  fuse_inode.rs
  union_directory.rs
  reader.rs
  scrub.rs
  error.rs

src/workspace_overlay/cluster_ingest/
  mod.rs
  source.rs
  local_fs.rs
  archive_tar.rs
  archive_zip.rs
  archive_compressed.rs
  inventory.rs
  planner.rs
  metadata_builder.rs
  pack_builder.rs
  upload_plan.rs
  upload_wal.rs
  multipart.rs
  resume.rs
  publisher.rs
```

Both modules are behind dedicated Cargo features. Format parsing uses checked safe byte access. Source
adapters are isolated from namespace publication and cannot bypass validation.

## 30. Required tests

This section is the acceptance contract referenced by the manifest above. It lists requirements for
future implementation, not passing tests. None is implemented by this documentation-only change.
Golden bytes must eventually be committed under the planned
`tests/fixtures/clustered_snapshot/v2/` root and linked from the manifest before the corresponding
evidence state changes from **Missing**.

### Format and determinism

- golden manifest, cluster superblock, batch header, index node, Data Seal, and DataPack bytes;
- deterministic metadata and plan across repeated identical builds;
- independent targeted batch decode with no predecessor resident;
- parent-first physical namespace order;
- exact DirKey derivation from raw names;
- per-cluster LocalNodeId density and backward-only hardlinks.

### Cluster union

- two roots mounted at the same path;
- directory-directory merge at several depths;
- file-file and file-directory collision rejection;
- mismatched directory attributes rejection;
- route buckets contain every true contributor;
- arbitrary grouping produces the same visible namespace;
- hash-range planning avoids O(cluster_count) lookup in large merged directories;
- cross-cluster hardlinks rejected or co-located by planner.

### Streaming and cold replacement

- root visible before deeper directories;
- global merged prefix never advances past the slowest contributor;
- targeted lookup beyond stream frontier is correct;
- one-billion-entry synthetic directory uses bounded resident memory;
- readdir pins only cursor windows and survives eviction/refetch;
- active FUSE inode survives eviction of its source namespace batch;
- ten billion emitted deterministic inode numbers require no global inode interner;
- scan traffic does not evict protected hot lookup batches;
- no false ENOENT during load, eviction, retry, or head replacement.

### Ingestion and archives

- local directory with raw names, hardlinks, symlinks, sparse files, xattrs, and ACLs;
- tar, tar.gz, tar.zst, and ZIP inventory without extracting a tree;
- archive path traversal, duplicates, bombs, corrupt CRC, and unsupported encryption rejected;
- non-seekable compressed archive resumes from durable spool;
- source mutation between inventory and upload prevents sealing.

### Upload and crash injection

Inject process/power failure before and after every WAL fsync, metadata PUT, UploadPart, ListParts,
CompleteMultipartUpload, data HEAD, seal PUT, manifest PUT, and head CAS. For every injection point:

- resume reaches exactly one correct sealed result or a clear unrecoverable source-change error;
- no incomplete cluster becomes visible;
- an acknowledged part is reused or safely re-uploaded;
- torn WAL tails are ignored;
- remote-only parts reconcile correctly;
- repeated resume is idempotent;
- orphan GC never deletes an object reachable from any manifest.

### Equivalence and isolation

For generated effective trees:

```text
resolve(single logical tree)
  == resolve(arbitrarily clustered snapshot)
  == resolve(streamed/evicted/reloaded clustered snapshot)
```

Run with many agents sharing one snapshot and independent MemLayers. Verify no committed mutation,
cross-agent visibility, KV access, or unbounded cache growth occurs.

## 31. Implementation order

This order is prospective; no numbered item is marked complete by this RFC.

1. Scalar codec, batch header, 4096-byte Merkle index node, and arbitrary-byte fuzzing.
2. DirKey/LocalNodeId assignment and one-cluster parent-first namespace builder.
3. Targeted namespace index lookup plus independently decodable batches.
4. Snapshot manifest, same-directory multi-cluster union, and strict collision validation.
5. Route records and hash-range cluster planner.
6. Deterministic FUSE inode packing, readdir merge cursor, and bounded L1 batch cache with eviction.
7. Extent/attribute batches, Data Seal reader, and data-range planning.
8. Local filesystem inventory and deterministic `.brfc` builder.
9. DataPack encoder, metadata-first S3 uploader, append-only WAL, and multipart resume.
10. tar/ZIP adapters, compressed-archive spool mode, and security limits.
11. Shared caches across agents, private MemLayer integration, and feature gating.
12. Snapshot publication, crash injection, full scrub, POSIX, scale, and performance gates.

Milestone 1 is one independently uploaded sealed cluster mounted read-only with bounded batch caching.
Milestone 2 is collision-safe union of several clusters at one directory. Milestone 3 is resumable local
and archive ingestion followed by private agent overlays.

## 32. Normative decisions summary

- A file cluster is an immutable namespace fragment, not a mutable database shard.
- Multiple clusters union by `DirKey`; they do not form an override stack.
- Directory-directory duplicates merge; all other committed name collisions are errors.
- The manifest routes only merged directories and does not duplicate every file entry.
- Metadata is physically parent-first but every batch is independently decodable.
- RAM residency is replaceable; IDs and locators remain stable across eviction.
- `.brfc` metadata is uploaded and remotely verified before any data PUT begins.
- `.brfc` refers to SliceIds; the post-data `.brfds` seal binds them to verified S3 bytes.
- A cluster is invisible until its seal is complete and a manifest references it.
- Local WAL plus S3 multipart reconciliation makes upload restartable after power loss.
- Existing BrewFS behavior remains unchanged when the clustered feature is disabled.
