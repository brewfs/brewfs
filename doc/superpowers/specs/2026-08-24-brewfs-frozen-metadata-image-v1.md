# BrewFS Streaming Frozen Metadata Image v1

Status: **Historical RFC / not implemented**; superseded by
[BrewFS Clustered Frozen Metadata v2](2026-08-24-brewfs-clustered-frozen-metadata-v2.md)

> [!CAUTION]
> The current BrewFS runtime has no `BRFIM001` codec or golden fixtures. The state machines and
> mandatory rules below describe a prototype proposal, not shipped support. Any successor work is
> gated by the [v2 implementation status and acceptance table](2026-08-24-brewfs-clustered-frozen-metadata-v2.md#implementation-status-and-acceptance-tracking).

This document is retained as the single-image prototype. New implementation work must use v2, which
adds independently uploadable file clusters, union-mounted directory contributions, batch-addressable
loading, and evictable runtime metadata.

Format: BrewFS Frozen Image (`BRFI`)

File suffix: `.brfi`

Magic: `BRFIM001`

## 1. Immutable-image contract

A committed BRFI object is a complete, materialized filesystem namespace. It has no parent layer and
no mutable records.

```text
effective FrozenImage + private MemLayer
                    |
                    | commit/build (offline and potentially expensive)
                    v
          complete immutable BRFI image
```

After publication:

- no inode, dentry, extent, xattr or ACL is updated in place;
- no Whiteout, Patch, transaction sequence or mutable version is stored;
- an agent modification exists only in its private MemLayer;
- a later commit builds another complete image;
- all agents using one commit share one `Arc<FrozenImage>`.

This format deliberately trades commit speed and cross-image metadata deduplication for read speed,
simple recovery and streaming startup.

## 2. Parent-first streaming invariant

Namespace metadata is stored in breadth-first parent order. These rules are mandatory:

1. root inode is the first logical record;
2. directory groups are ordered by ascending parent NodeId;
3. NodeIds are allocated breadth-first while processing those directory groups;
4. a new child's complete hot inode attributes occur inline immediately after its parent dentry;
5. a child directory group occurs only after the dentry that introduced that directory;
6. a hardlink may refer only to a NodeId already introduced earlier in the stream;
7. all segments of one directory are contiguous before the next directory begins;
8. a loader may publish each validated name-prefix segment, but marks the directory Complete only
   after its final segment.

Therefore a sequential reader sees:

```text
root inode
root entries + immediate child inode attributes
depth-1 directory entries + their new child attributes
depth-2 directory entries + their new child attributes
...
```

The namespace becomes usable incrementally. Extents, xattrs, ACLs and symlink payloads are placed after
the namespace pages and fetched in the background or on demand.

## 3. Node IDs and FUSE inode numbers

`NodeId` is a non-zero `u32`, densely assigned from `1..=node_count`. Root is always 1.

```rust
#[repr(transparent)]
pub struct NodeId(u32);
```

Runtime array index is `NodeId - 1`. The FUSE inode namespace is:

```text
bit 63 = 0  frozen image inode; low 32 bits contain NodeId
bit 63 = 1  private MemLayer inode; remaining bits contain agent-local ID
```

Bits 32..62 are zero for v1 frozen nodes. NodeId/FUSE inode zero is invalid.

NodeIds remain stable for one mounted image but may change in a later commit. Hardlinks remain correct
because all names for one file reuse the same NodeId. Stable inode identity across image replacement
is outside v1.

## 4. Deterministic breadth-first assignment

The builder assigns NodeIds deterministically:

1. assign root NodeId 1 and push it into a FIFO directory queue;
2. pop directories in NodeId order;
3. enumerate each directory by raw filename byte order;
4. the first encounter of a node assigns the next NodeId;
5. enqueue a newly assigned directory;
6. later hardlink encounters reuse the existing NodeId.

Directory hardlinks/cycles are invalid under existing BrewFS namespace rules. For multiply-linked
non-directory inodes, the builder precomputes the earliest breadth-first `(parent NodeId, name)` key,
so every later occurrence can be encoded as a backward reference.

Equivalent namespaces and build options produce the same NodeId assignment and physical bytes.

## 5. Readiness during streaming

The loader tracks:

```rust
pub struct StreamingReadiness {
    pub sequential_node_high_watermark: u32,
    pub loaded_node_ranges: Arc<NodeRangeSet>,
    pub directory_states: Arc<DirectoryReadinessTable>,
    pub namespace_complete: bool,
    pub extent_node_high_watermark: u32,
    pub attribute_node_high_watermark: u32,
}
```

An inline child inode is appended before its dentry is made visible. Directory state is:

```text
Introduced
Streaming { loaded_through_name }
Complete
```

Each complete segment advances `loaded_through_name`. END changes the state to Complete and finalizes
the runtime lookup index.

Operations behave as follows while loading:

- getattr for an introduced node succeeds immediately;
- lookup on a Streaming directory is authoritative when the target name is at or before
  `loaded_through_name`; a later name triggers its indexed page fetch;
- readdir may return the validated loaded prefix; reaching its frontier fetches/awaits the next page;
- Complete directories answer all lookup/readdir requests without more namespace IO;
- a file read whose extent group is absent triggers indexed Range GET and awaits that page;
- xattr/ACL/readlink similarly trigger an attribute-page fetch;
- incomplete state is never reported as NotFound or an empty directory.

FUSE request cancellation may stop the wait, but it does not roll back already validated pages.

## 6. Physical file order

```text
+---------------------------------------+ offset 0
| Superblock + section directory        | 4096 bytes
+---------------------------------------+
| NamespacePageIndex                    | uncompressed
+---------------------------------------+
| NamespacePage 0 (root first)          |
| NamespacePage 1                       | breadth-first parent order
| ...                                   |
+---------------------------------------+
| ExtentPageIndex                       | uncompressed
+---------------------------------------+
| ExtentPages                           | ascending NodeId
+---------------------------------------+
| AttributePageIndex                    | uncompressed
+---------------------------------------+
| AttributePages                        | ascending NodeId
+---------------------------------------+
| Footer                                | 72 bytes
+---------------------------------------+ object length
```

Indexes precede the pages they address. A loader may first GET the superblock and three indexes, then
sequentially stream namespace pages while performing targeted extent/attribute range reads.

All pages are self-contained. Page regions begin at 8-byte-aligned offsets. Padding is zero.

## 7. Scalar encoding

Page payloads use:

```text
uvarint(x)  unsigned LEB128, shortest representation
svarint(x)  ZigZag i64 followed by uvarint
bytes(x)    uvarint(length) || raw bytes
bool        0x00 or 0x01
enum        stable u8 discriminant
```

Fixed numeric fields in headers and indexes use little-endian encoding. Hashes and UUIDs are raw bytes.

Decoders reject overlong varints, overflow, unknown required discriminants, non-zero reserved bits and
any size exceeding page/object/configured bounds.

## 8. Superblock

The first 4096 bytes contain a 256-byte header, 32 section slots of 96 bytes, reserved zeros and one
final CRC32C.

### 8.1 Header

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: ASCII `BRFIM001` |
| 8 | 2 | format major: `1` |
| 10 | 2 | format minor: `0` |
| 12 | 4 | superblock length: `4096` |
| 16 | 8 | required feature flags |
| 24 | 16 | volume ID |
| 40 | 16 | deterministic image ID |
| 56 | 32 | source revision hash |
| 88 | 32 | semantic filesystem root hash |
| 120 | 4 | logical image schema version: `1` |
| 124 | 4 | reserved |
| 128 | 8 | BrewFS chunk size |
| 136 | 4 | root NodeId: `1` |
| 140 | 4 | node count |
| 144 | 8 | dentry count |
| 152 | 8 | extent count |
| 160 | 8 | logical attribute-value bytes |
| 168 | 16 | deterministic runtime hash seed |
| 184 | 2 | used section slots: `6` |
| 186 | 2 | section slot size: `96` |
| 188 | 2 | page header size: `64` |
| 190 | 1 | NodeId width: `4` |
| 191 | 1 | default page codec |
| 192 | 32 | build-options digest |
| 224 | 8 | total raw metadata bytes |
| 232 | 4 | namespace page count |
| 236 | 4 | extent page count |
| 240 | 4 | attribute page count |
| 244 | 4 | reserved |
| 248 | 4 | header CRC32C over bytes `[0,248)` |
| 252 | 4 | reserved |

Feature flags:

```text
bit 0 DENSE_BFS_NODE_IDS
bit 1 PARENT_FIRST_NAMESPACE
bit 2 FULLY_MATERIALIZED_EXTENTS
bit 3 PAGE_ZSTD
bit 4 HAS_XATTR
bit 5 HAS_ACL
bits 6..63 reserved
```

Bits 0..2 are required.

Image ID is the first 16 bytes of
`BLAKE3("BRFIID1" || volume_id || semantic_root_hash || build_options_digest)`. Runtime hash seed is
derived from `BLAKE3("BRFIHASH1" || semantic_root_hash)`. Commit timestamps are stored in the workspace
head record, not BRFI, so identical logical input and build options reproduce identical bytes.

The workspace head stores the complete reference needed for authenticated streaming:

```rust
pub struct FrozenImageRef {
    pub image_hash: [u8; 32],
    pub semantic_root_hash: [u8; 32],
    pub superblock_digest: [u8; 32],
    pub object_len: u64,
}
```

The loader verifies the superblock digest before trusting section slots. Section slots authenticate
page-index bytes, and page indexes authenticate individual page payloads.

### 8.2 Section slot

Slots begin at offset 256. Each is 96 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 2 | section kind |
| 2 | 2 | section version |
| 4 | 4 | flags |
| 8 | 8 | object offset |
| 16 | 8 | stored length |
| 24 | 8 | total raw page bytes |
| 32 | 8 | page/index-entry count |
| 40 | 4 | fixed entry size, zero for page stream |
| 44 | 4 | required alignment |
| 48 | 32 | BLAKE3 section digest |
| 80 | 8 | first logical key |
| 88 | 8 | last logical key |

Section kinds:

```text
1 NamespacePageIndex
2 NamespacePages
3 ExtentPageIndex
4 ExtentPages
5 AttributePageIndex
6 AttributePages
```

Slots are ordered by kind. Sections are ordered, non-overlapping and before the footer. CRC32C over
superblock bytes `[0,4092)` is stored at offset 4092.

## 9. PageHeader

Every page has a 64-byte header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: ASCII `BRFIPG01` |
| 8 | 2 | page kind |
| 10 | 2 | page version: `1` |
| 12 | 4 | page flags |
| 16 | 4 | page ordinal |
| 20 | 4 | required predecessor page ordinal, `0xffffffff` for none |
| 24 | 8 | first logical key |
| 32 | 8 | last logical key |
| 40 | 4 | logical group count |
| 44 | 4 | raw payload length |
| 48 | 4 | stored payload length |
| 52 | 1 | codec: None=0, Zstd=1 |
| 53 | 3 | reserved |
| 56 | 4 | CRC32C of stored payload |
| 60 | 4 | header CRC32C over bytes `[0,60)` |

Page kinds:

```text
Namespace=1
Extent=2
Attribute=3
```

Page flags:

```text
bit 0 FIRST_IN_SECTION
bit 1 LAST_IN_SECTION
bit 2 STARTS_WITH_CONTINUATION
bit 3 ENDS_WITH_CONTINUATION
bits 4..31 reserved
```

Namespace page `required_predecessor` is the preceding namespace ordinal when advancing the sequential
frontier. A detached targeted read may validate/decode a later page through its authenticated index
entry and NodeId range, but it does not advance the sequential frontier. Extent and Attribute pages
use `0xffffffff` because their groups can always be independently range-fetched.

Stored payload CRC is checked before decompression. Decoding must consume the full zstd frame and
produce exactly `raw_len`, bounded by the v1 16 MiB hard limit.

## 10. Page indexes

Each page-index section is uncompressed and begins:

```text
magic       [8] = BRFIIDX1
kind        u16
version     u16 = 1
entry_size  u16 = 64
reserved    u16
entry_count     u32
key_arena_len   u32
entries     [PageIndexEntry]
key_arena   [key_arena_len]
crc32c      u32
reserved    u32
```

`PageIndexEntry` is 64 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | first logical key |
| 8 | 8 | last logical key |
| 16 | 8 | object offset |
| 24 | 4 | total page length including header, excluding padding |
| 28 | 4 | raw payload length |
| 32 | 4 | group count |
| 36 | 4 | page flags |
| 40 | 4 | first newly introduced NodeId; Namespace only |
| 44 | 4 | newly introduced node count; Namespace only |
| 48 | 16 | first 16 bytes of authenticated page BLAKE3 |

For Namespace index entries, each 64-bit logical-key field packs:

```text
high 32 bits  parent NodeId
low 32 bits   offset into the index key arena
```

The key-arena offset points to `name_len:u16_le || raw_name`. Empty name is the boundary key for an
empty directory or root bootstrap page. Namespace index ordering compares parent NodeId, then raw name
bytes. This permits binary search directly to the page containing `(parent,name)`, including one page
in a billion-entry directory.

Extent and Attribute indexes set `key_arena_len=0`; their logical key is owning NodeId in the low 32
bits with the high bits zero, and their new-node fields are zero. Namespace new-node ranges are ordered
and non-overlapping. They let a targeted loader locate the page that introduced a hardlink target
without decoding every preceding namespace page. Entries are sorted; ranges may touch only when a
logical group continues across adjacent pages.

Index CRC32C covers its complete header, entry array and key arena, excluding the trailing
CRC/reserved words.
Authenticated page BLAKE3 covers PageHeader bytes `[0,60)` followed by the stored payload. When a page
is fetched, header fields must match its index entry and the complete authenticated bytes must match
both CRC32C scopes and BLAKE3-128 before decompression or publication.

## 11. Namespace stream

Every namespace page begins with an authenticated NodeId range. The first page then contains one root
record before its directory segments:

```text
first_new_node_id uvarint
new_node_count    uvarint
root_record_len uvarint
root_inode      [root_record_len]
segment_count   uvarint
directory_segments...
```

Later pages replace the root fields with `segment_count` immediately after the NodeId range. Page 0
uses first NodeId 1 and counts root. A page introducing no node encodes both range fields as zero. The
range must exactly match its authenticated PageIndexEntry.

### 11.1 Directory segment

```text
parent_node_delta   uvarint
segment_flags       u8
if START:
  total_directory_entries uvarint
entry_count         uvarint

repeated entries in raw-name order:
  shared_name_bytes uvarint
  name_suffix_len   uvarint
  name_suffix       [name_suffix_len]
  dentry_tag        u8
  if NEW_NODE:
    inode_record_len uvarint
    inode_record     [inode_record_len]
  else:
    existing_node_id uvarint
```

At page start, previous parent NodeId and previous name are zero/empty. Parent deltas are non-negative
because groups are in ascending parent order. Previous name resets at every segment.

Segment flags:

```text
bit 0 START
bit 1 END
bits 2..7 reserved
```

A normal directory has START|END. A large directory has one START segment, zero or more middle
segments and one END segment in consecutive pages. Entry name order continues across segments.
`total_directory_entries` lets the loader preallocate the final arena/hash table and is checked against
the accumulated count at END.

`dentry_tag`:

```text
bit 7 NEW_NODE
bits 0..3 child inode kind
bits 4..6 reserved
```

If NEW_NODE is set, child NodeId is implicit: the next value in the page's authenticated new-node
range. The inline inode record is decoded before publishing the dentry. Otherwise, `existing_node_id`
must have been introduced by an earlier logical namespace page and represents a hardlink.

No parent NodeId, child NodeId, layer ID or filename is duplicated outside this compact stream.

### 11.2 Compact inode record

```text
kind          u8
flags         uvarint
size          uvarint
mode          uvarint
uid           uvarint
gid           uvarint
nlink         uvarint
atime_ns      svarint
mtime_ns      svarint
ctime_ns      svarint
if HAS_RDEV:  rdev uvarint
```

Flags:

```text
bit 0 HAS_RDEV
bit 1 HAS_EXTENTS
bit 2 HAS_XATTR
bit 3 HAS_ACL
bit 4 HAS_SYMLINK
bit 5 IS_SPARSE
bits 6..63 reserved
```

The first parent is implicit from the introducing directory. `parent_hint` is that parent only when
`nlink=1`; otherwise it is None. Root uses itself as the mount-time parent hint.

Runtime decoding appends one fixed 64-byte `InodeHot` record. Persisted bytes remain compact and
stream-oriented; runtime arrays remain direct-index and CPU-efficient.

## 12. Directory readiness and runtime indexes

The persisted namespace contains no duplicate hash-slot table. While streaming one directory group,
the loader appends its names/dentries into arenas and builds the immutable runtime lookup strategy:

```text
0 Empty
1 Linear       1..8 entries
2 Binary       9..64 entries
3 FrozenHash   more than 64 entries
```

FrozenHash is built incrementally with maximum load factor 0.80. It stores relative dentry ordinal + 1
in `u16` or `u32` slots. There is no insertion/deletion/tombstone API after END.

After each validated segment, the loader publishes its sorted dentry prefix and advances the directory
frontier. Before END, lookup uses the persisted page-name ranges plus binary search in loaded segments;
it never returns NotFound for a name beyond the frontier. After validating END, entry ordering, child
kinds and the completed FrozenHash, the loader marks the directory Complete. Persisting no hash slots
reduces S3 bytes and lets build thresholds change without changing semantic image identity.

## 13. Extent pages

Extent groups are ordered by NodeId and may be fetched independently:

```text
file_group_count uvarint

repeated file groups:
  node_id_delta  uvarint
  extent_count   uvarint
  previous_end   = 0

  repeated extents:
    gap_from_previous_end uvarint
    length                uvarint
    slice_id              uvarint
    slice_offset          uvarint
```

The image stores a complete final file map. There is no chunk index, layer ID, sequence, kind or Hole
record. Gaps represent sparse holes/zeros. File size is in the inline inode record.

Invariants:

- NodeId refers to an introduced regular file with HAS_EXTENTS;
- groups and extents are strictly ordered;
- length and slice ID are non-zero;
- logical/slice arithmetic does not overflow;
- logical end is at most file size;
- no extent crosses a BrewFS file-chunk boundary; the builder splits it at chunk boundaries so
  `chunk_index = logical_offset / chunk_size` is reconstructable;
- adjacent extents referencing one contiguous slice range are merged;
- a file group may continue only in immediately adjacent pages and index ranges reflect continuation.

When decoded, a file's extents append to one contiguous runtime extent arena and its direct
`InodeRanges` entry is filled. A file read can fetch only the page range covering its NodeId.

## 14. Attribute pages

Attribute groups are ordered by NodeId:

```text
node_group_count uvarint

repeated node groups:
  node_id_delta uvarint
  group_flags   u8

  if HAS_SYMLINK:
    symlink_target bytes

  if HAS_XATTR:
    xattr_count uvarint
    names in sorted prefix-compressed order:
      shared_name_bytes uvarint
      name_suffix_len   uvarint
      name_suffix       [name_suffix_len]
      value             bytes

  if HAS_ACL:
    acl_count uvarint
    sorted ACL entries:
      acl_type          u8
      acl_id_delta      svarint
      value             bytes
```

Group flags must equal the corresponding inline inode flags. There are no Whiteouts. Empty xattr/ACL
values are valid. ACL ID delta resets to zero whenever ACL type changes.

Attribute pages are cold and normally zstd-compressed. A value never crosses a page unless it alone
exceeds the target; such a value receives one dedicated page within existing BrewFS size limits.

Decoded names and values append into shared byte arenas, and direct per-NodeId ranges are filled.

## 15. Semantic root hash

Physical pages, compression and page boundaries are not semantic. The logical stream is:

```text
magic BRFILOG1
logical schema version u32_le

root inode
namespace directory groups in BFS parent order
extent groups in NodeId order
attribute groups in NodeId order
```

It uses the decoded compact scalar representation with no page headers/padding. Runtime hash seed,
zstd level and page target are excluded.

```text
semantic_root_hash = BLAKE3(canonical logical stream)
```

Equivalent effective filesystems built with identical deterministic NodeId assignment produce the same
semantic root hash even if physical page sizes/codecs differ.

## 16. Footer and physical identity

Footer is exactly 72 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: ASCII `BRFIEND1` |
| 8 | 8 | complete object length |
| 16 | 8 | superblock offset: `0` |
| 24 | 8 | superblock length: `4096` |
| 32 | 32 | image hash over all bytes before footer |
| 64 | 4 | CRC32C over bytes `[0,64)` |
| 68 | 4 | reserved |

Object key:

```text
brewfs-meta/v1/volumes/{volume_id}/images/{image_hash[0]:02x}/{image_hash}.brfi
```

Upload uses create-only semantics. Semantic root identifies filesystem content; image hash identifies
one physical paging/compression representation.

## 17. Streaming loader

```rust
pub struct StreamingFrozenImageBuilder {
    identity: ImageIdentity,
    inode_hot: FillOnceNodeTable<InodeHot>,
    inode_ranges: FillOnceNodeTable<InodeRanges>,
    dentries: AppendOnlyArena<Dentry>,
    names: ByteArena,
    directory_indexes: DirectoryIndexArena,
    extents: AppendOnlyArena<Extent>,
    xattrs: AppendOnlyArena<Xattr>,
    acls: AppendOnlyArena<Acl>,
    values: ByteArena,
    readiness: Arc<StreamingReadiness>,
}
```

Loading sequence:

1. GET footer and verify length/image hash against `FrozenImageRef`.
2. GET the 4096-byte superblock and verify `superblock_digest`.
3. GET/validate the three page indexes, normally as parallel ranges.
4. Range-stream NamespacePages from first to last while within the configured preload budget.
5. Publish each validated directory prefix and Complete transition.
6. Use `(parent,name)` index ranges for targeted namespace pages beyond the preload frontier.
7. If a targeted page references an unloaded hardlink NodeId, locate its introduction page through the
   authenticated new-node ranges.
8. Prefetch Extent/Attribute pages for recently introduced hot nodes.
9. Serve other on-demand range fetches using page indexes.
10. Retain cold pages in S3/local page cache when the full image exceeds the memory budget.
11. Verify complete section/image digests during a background scrub or full-cache pass.

Sequential arena snapshots use append-only chunks plus `ArcSwap`; targeted pages fill authenticated
NodeId slots exactly once in sparse chunks. Publishing does not copy already decoded nodes, and a slot
can never be replaced with different content. All readers see immutable validated regions.

## 18. Persisted versus runtime representation

The S3 format is compact and stream-ordered. Runtime is direct-index and read-optimized:

```rust
pub struct FrozenImage {
    pub inode_hot: FillOnceChunkedNodeTable<InodeHot>,
    pub inode_ranges: FillOnceChunkedNodeTable<InodeRanges>,
    pub dentries: ChunkedArena<Dentry>,
    pub directory_indexes: DirectoryIndexArena,
    pub names: ChunkedByteArena,
    pub extents: ChunkedArena<Extent>,
    pub xattrs: ChunkedArena<Xattr>,
    pub acls: ChunkedArena<Acl>,
    pub values: ChunkedByteArena,
    pub readiness: Arc<StreamingReadiness>,
}
```

This separation permits compact transfer and progressive construction without paying varint/prefix
decode cost on every lookup. All agents for one image hash share the same `Arc<FrozenImage>` and loader.

## 19. Agent overlay boundary

The mounted view is always:

```text
one private MemLayer -> one fully materialized FrozenImage
```

The MemLayer may override frozen inode fields, add/delete names, change xattrs/ACLs and add Data/Hole
extents. The frozen image has no mutation method and no invalidation path.

Commit freezes the MemLayer, resolves the complete two-level view, builds a new BRFI and atomically
switches the workspace head only after remote verification. The old image remains valid forever while
referenced.

## 20. Build pipeline

1. Freeze private mutations.
2. Resolve the complete effective namespace.
3. remove deleted/unreachable nodes and obsolete slice mappings;
4. validate root reachability, directory acyclicity, hardlinks and nlink;
5. compute deterministic breadth-first NodeIds;
6. emit namespace directory groups in parent NodeId order;
7. inline each newly introduced child's hot inode record after its first dentry;
8. emit normalized complete extent groups by NodeId;
9. emit cold attribute groups by NodeId;
10. choose page boundaries without violating directory/group contiguity rules;
11. write indexes before their page regions;
12. compute semantic, page, section and image hashes;
13. reopen and fully scrub the local artifact;
14. upload create-only and remotely validate footer/superblock/indexes;
15. CAS-publish the new workspace head.

The build may be O(total metadata), use temporary disk and perform external sorting. This is accepted:
committed files are immutable and the cost is paid once, while reads are shared across agents.

## 21. Performance behavior

After a directory has streamed:

- getattr is direct NodeId array access;
- readdir is one contiguous arena range;
- small lookup is linear/binary over that range;
- large lookup uses a completed immutable FrozenHash;
- no varint is decoded again on the hot path;
- no parent layer, KV service or mutable metadata cache is consulted.

Startup behavior:

- root inode is available from namespace page 0;
- root directory becomes available after its END segment;
- child inode attributes arrive inline with root entries;
- deeper directories become ready breadth-first;
- extent/attribute pages do not block namespace streaming;
- early file reads trigger targeted range fetch instead of waiting for the full image.

## 22. Redundancy policy

The persisted stream omits:

- repeated layer/parent IDs and KV prefixes;
- database primary/secondary index pages;
- full fixed inode rows in S3;
- parent NodeId on every dentry;
- explicit NodeId for newly introduced children;
- inode ID on every extent/xattr/ACL value;
- Whiteouts, tombstones, transaction sequences and mutable versions;
- Hole extents and historical overwrites;
- persisted directory hash slots.

Intentional runtime-only redundancy includes fixed InodeHot arrays and FrozenHash slots. It is built
once while streaming, kept only in shared host memory/cache and never increases S3 transfer bytes.

## 23. Failure and retry

- A page failing CRC/hash/decompression is never published.
- A partially decoded segment is discarded or retained privately but never advances the published
  directory frontier.
- Retrying the same page is idempotent because arenas commit only at validated group boundaries.
- A missing extent/attribute page blocks only operations requiring its NodeId range.
- Namespace predecessor mismatch stops sequential publication and reports corruption.
- Background full-hash failure marks the image unhealthy and rejects new requests; it never falls back
  silently to KV metadata.
- Loader cancellation preserves already published immutable prefixes for other active waiters.

## 24. Limits

| Item | Default | Hard limit |
|---|---:|---:|
| raw page target | 256 KiB | 16 MiB |
| namespace page 0 target | 1 MiB | 16 MiB |
| NodeIds | — | `u32::MAX - 1` |
| page count per section | — | `u32::MAX - 1` |
| one directory segment | page target | 16 MiB |
| one decoded value | existing limit | 16 MiB format limit |
| layer/image parents | none | none |

Large root directories may span consecutive namespace pages. Validated root name prefixes are usable
before END; arbitrary lookup jumps through the namespace name index, while Complete status is reached
only at END.

## 25. Validation

Fast open validates footer and expected image reference, superblock BLAKE3/CRC, required flags,
section bounds/alignment and all three authenticated page indexes.

Each namespace page validates:

- ordinal and predecessor;
- parent NodeIds already introduced;
- ascending parent group order and contiguous segments;
- sorted names across page boundaries;
- sequential new NodeIds are exactly next-high-watermark; targeted new NodeIds exactly match their
  authenticated disjoint range and fill empty slots only;
- hardlinks reference existing non-directory nodes;
- kind hints match inline/existing inode kinds;
- END counts match accumulated entries.

Extent/attribute pages validate NodeId ownership, sort order, continuation, size/range arithmetic and
inode flags.

Full scrub additionally validates semantic root hash, every section digest, image hash, directory
acyclicity, nlink counts, all expected extent/attribute groups and unreachable-node absence.

## 26. Required tests

### Format golden tests

- exact 4096-byte superblock and 96-byte section slot;
- exact 64-byte PageHeader, 64-byte PageIndexEntry and 72-byte footer;
- compact root/child/hardlink inode records;
- single and multi-page directory groups;
- extent and attribute group encodings;
- deterministic bytes across repeated builds.

### Streaming-order tests

- root record is first;
- parent dentry precedes child directory group;
- inline child inode commits before visible dentry;
- BFS NodeIds are deterministic;
- hardlink is always a backward reference;
- a targeted later namespace page decodes from its authenticated NodeId range without earlier pages;
- an unloaded hardlink target resolves through the introduction-page index;
- validated directory prefixes are visible before END without false NotFound beyond the frontier;
- a huge root supports partial readdir and indexed lookup into an unloaded later segment;
- extent/attribute targeted fetch works before namespace completion;
- incomplete directory waits and never returns false NotFound.

### Equivalence property

For generated base plus MemLayer:

```text
resolve(base + MemLayer)
    ==
resolve(progressively loaded build_brfi(base + MemLayer))
```

Run comparison after every published directory prefix and after complete load, covering namespace,
inode attributes, hardlinks, xattrs, ACLs, symlinks and sampled file ranges.

### Isolation/performance

- 100 agents share one loader/FrozenImage;
- agents maintain independent MemLayers;
- root lookup/readdir latency measured at first-ready and fully-loaded states;
- bytes-to-root-ready, bytes-to-depth-N-ready and total metadata bytes are reported;
- cold extent fetch request/byte count is reported;
- hot path contains no CompactCodec decode after readiness;
- no KV calls occur.

### Corruption/fuzzing

- arbitrary superblocks, indexes, page headers and payloads;
- invalid predecessor/continuation and parent-before-child violations;
- non-canonical/overflow varints;
- truncated zstd and decompression bombs;
- invalid hardlinks, cycles, NodeId gaps and nlink mismatch;
- no panic, unbounded allocation, false readiness or unchecked cast.

## 27. Implementation modules

```text
src/workspace_overlay/frozen_image/
  mod.rs
  format.rs
  superblock.rs
  scalar.rs
  page.rs
  page_index.rs
  namespace.rs
  extent.rs
  attribute.rs
  bfs_builder.rs
  stream_loader.rs
  readiness.rs
  runtime_arena.rs
  directory_hash.rs
  reader.rs
  scrub.rs
  cache.rs
  error.rs
```

The module is feature-gated and independent of concrete KV/SQL metadata backends. Persisted parsing
uses safe checked byte access. Runtime arenas expose immutable snapshots only.

## 28. Implementation order

1. Golden structs: superblock, section slots, page headers, indexes and footer.
2. Canonical scalar decoder plus arbitrary-byte fuzzing.
3. Deterministic BFS NodeId assignment and namespace-page builder.
4. Streaming namespace loader with directory readiness barriers.
5. Runtime direct inode/dentry arenas and Linear/Binary/FrozenHash lookup.
6. Indexed Extent pages and on-demand read-plan loading.
7. Indexed Attribute pages and cold value loading.
8. Shared loader/cache across multiple agent workspaces.
9. S3 sequential/range transport, retry and background scrub.
10. Commit publication, crash injection, POSIX and performance gates.

The first milestone is root-first streaming of a read-only image with getattr, lookup and readdir. It
does not implement any mutation inside BRFI.
