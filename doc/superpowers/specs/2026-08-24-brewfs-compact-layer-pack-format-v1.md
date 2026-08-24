# BrewFS Compact Layer Pack Format v1

Status: Superseded for immutable committed bases by `2026-08-24-brewfs-frozen-metadata-image-v1.md`;
retained as the delta-pack alternative

File suffix: `.brlp`

Magic: `BRLPM001`

## 1. Storage model

A BRLP file stores one sealed layer as a minimal semantic delta relative to one immutable parent.
It is not a dump of mutable metadata rows and it is not a mutation log.

The seal pipeline first reduces the writable state:

```text
mutation history
    -> final effective private state
    -> difference against effective parent
    -> normalized grouped tables
    -> compact BRLP blocks
```

The pack contains five core tables:

```text
1  InodePatch
2  Dentry
3  Xattr
4  ACL
5  Extent
```

The layer ID, parent reference and hashes occur once in the file header. They are never repeated per
record. Mutable sequence numbers and transaction IDs do not appear in a sealed pack.

## 2. Redundancy eliminated at seal

The pack builder MUST perform these reductions before encoding:

### 2.1 Key collapse

For Dentry, Xattr and ACL, retain only the final operation for each logical key. Earlier Put/Whiteout
operations disappear.

### 2.2 Inode patching

The writable layer may keep a full effective inode for simple mutation handling. The pack stores only
fields whose final values differ from the parent inode. A one-bit field mask replaces a full copied-up
`FileAttr`.

### 2.3 Extent normalization

For every `(ino, chunk_index)`, apply all private writes in sequence order, then emit sorted,
non-overlapping final Data/Hole intervals. Adjacent compatible intervals are merged. Sequence numbers
are unnecessary after this normalization.

### 2.4 Parent no-op elimination

After normalization:

- Put equal to the parent value is omitted;
- Whiteout for a key absent from the parent is omitted;
- an inode patch with an empty field mask is omitted;
- a Data interval with the same visible physical slice mapping as the parent is omitted; seal does not
  read or compare file payload bytes;
- a Hole over an already absent/hole parent range is omitted;
- create followed by delete before seal leaves no inode/dentry record;
- data slices no longer referenced by the normalized result are not included and become GC candidates.

No-op elimination compares logical values, not their physical encoding.

## 3. Scalar encoding

All variable-width scalar encodings are canonical:

```text
uvarint(x)  unsigned LEB128, shortest representation
svarint(x)  ZigZag signed i64 followed by uvarint
bytes(x)    uvarint(byte_length) || raw bytes
bool        0x00 or 0x01
enum        stable u8 discriminant
```

Fixed-width integers in headers, directories, checksums and ordered keys use big-endian byte order.

Decoder requirements:

- reject overlong/non-shortest varints;
- reject a `u64` varint longer than ten bytes;
- reject overflow before allocation or conversion;
- reject lengths beyond both object bounds and configured limits;
- reject unknown required enum values and non-zero reserved bits;
- never use unchecked archived-struct access.

## 4. File layout

```text
+------------------------------------+ 0
| PackHeader, 256 bytes              |
+------------------------------------+
| InodePatch data blocks             |
| optional filter block              |
| index block                        |
+------------------------------------+
| Dentry data blocks                 |
| optional filter block              |
| index block                        |
+------------------------------------+
| Xattr data blocks                  |
| optional filter block              |
| index block                        |
+------------------------------------+
| ACL data blocks                    |
| optional filter block              |
| index block                        |
+------------------------------------+
| Extent data blocks                 |
| optional filter block              |
| index block                        |
+------------------------------------+
| TableDirectory, 584 bytes          |
+------------------------------------+
| PackFooter, 72 bytes               |
+------------------------------------+ object length
```

Every region begins at an 8-byte-aligned offset. Alignment padding is zero and is excluded from table
digests but included in `pack_hash`.

Tables occur in stable tag order. Every table is present even when empty. An empty table has zero data
blocks and a zero first-data offset, but still has a valid empty index block. Its group, record and
raw-byte counts are zero.

## 5. PackHeader

`PackHeader` is exactly 256 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: ASCII `BRLPM001` |
| 8 | 2 | format major: `1` |
| 10 | 2 | format minor: `0` |
| 12 | 4 | header length: `256` |
| 16 | 8 | flags |
| 24 | 16 | volume ID |
| 40 | 16 | layer ID |
| 56 | 16 | parent layer ID, zero for root |
| 72 | 4 | logical layer schema version: `1` |
| 76 | 4 | layer depth |
| 80 | 8 | BrewFS chunk size |
| 88 | 32 | parent pack hash, zero for root |
| 120 | 32 | parent root hash, zero for root |
| 152 | 32 | this layer delta digest |
| 184 | 32 | this layer root hash |
| 216 | 8 | table-directory offset |
| 224 | 8 | table-directory length: `584` |
| 232 | 8 | total uncompressed logical table bytes |
| 240 | 8 | creation time, signed Unix nanoseconds |
| 248 | 2 | table count: `5` |
| 250 | 1 | default block codec |
| 251 | 1 | name restart interval, default `16` |
| 252 | 4 | CRC32C over bytes `[0, 252)` |

Header flags:

```text
bit 0 HAS_PARENT
bit 1 HAS_ZSTD_BLOCKS
bit 2 HAS_FILTERS
bit 3 EXTENTS_NORMALIZED
bit 4 INODES_ARE_PATCHES
bits 5..63 reserved
```

Flags 3 and 4 are required in v1. For a root layer, `HAS_PARENT` is zero and all parent fields are
zero. For a non-root layer, the parent layer ID, pack hash and root hash must all be non-zero.

## 6. TableDirectory

The directory is exactly 584 bytes:

```text
magic             [8] = BRLTDIR1
directory_version u16 = 1
entry_size        u16 = 112
entry_count       u16 = 5
reserved          u16 = 0
entries           [TableEntry; 5]
directory_crc32c  u32
reserved          u32
```

CRC32C covers the first 576 bytes. `TableEntry` is exactly 112 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 2 | table kind |
| 2 | 2 | table format version: `1` |
| 4 | 4 | table flags |
| 8 | 8 | first data-block offset |
| 16 | 8 | total data-block bytes including alignment |
| 24 | 4 | data-block count |
| 28 | 4 | logical group count |
| 32 | 8 | logical record count |
| 40 | 8 | index-block offset |
| 48 | 8 | index-block length |
| 56 | 8 | filter-block offset, zero when absent |
| 64 | 8 | filter-block length, zero when absent |
| 72 | 8 | uncompressed logical bytes |
| 80 | 32 | BLAKE3 table digest |

Table flags:

```text
bit 0 HAS_FILTER
bit 1 GROUPED
bit 2 PREFIX_COMPRESSED
bits 3..31 reserved
```

The table digest covers every non-padding byte belonging to its data, filter and index blocks in
physical order.

## 7. Physical blocks

Every data, index and filter block has a self-contained 32-byte header followed by `stored_len` bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic: ASCII `BRLB` |
| 4 | 1 | table kind |
| 5 | 1 | codec: None=0, Zstd=1 |
| 6 | 2 | block flags |
| 8 | 4 | block ordinal |
| 12 | 4 | group count |
| 16 | 4 | logical record count |
| 20 | 4 | raw payload length |
| 24 | 4 | stored payload length |
| 28 | 4 | CRC32C of stored payload |

Block flags:

```text
bit 0 DATA
bit 1 INDEX
bit 2 FILTER
bit 3 GROUP_CONTINUATION
bits 4..15 reserved
```

Exactly one of DATA, INDEX or FILTER is set. `codec=None` requires `raw_len == stored_len`. Zstd output
must decode to exactly `raw_len`, consume the complete input and remain within the 16 MiB raw-block
hard limit.

Block headers allow an object-store range response or RPC message to carry the block unchanged. No
enclosing JSON/protobuf object is needed on the metadata hot path.

## 8. Index blocks

Each table has one uncompressed index block. Its payload is a prefix-compressed sequence:

```text
entry_count uvarint

repeated entry_count times:
  shared_first_key_bytes uvarint
  first_key_suffix_len   uvarint
  first_key_suffix       bytes without length prefix
  shared_last_key_bytes  uvarint  // relative to full first key
  last_key_suffix_len    uvarint
  last_key_suffix        bytes without length prefix
  block_offset_delta     uvarint  // relative to previous data block
  block_total_len        uvarint  // header + stored bytes, excluding padding
  block_ordinal_delta    uvarint

restart offsets [u32_be]
restart_count   u32_be
```

Every 16th index entry is a restart: both shared lengths reset to zero and `block_offset_delta` is
encoded as an absolute object offset. Non-restart offsets are relative to the preceding data block.
First and last keys are inclusive. Block key ranges are ordered and do not overlap.

Canonical index keys:

| Table | Key |
|---|---|
| InodePatch | `ino:u64_be` |
| Dentry | `parent_ino:u64_be || name` |
| Xattr | `ino:u64_be || name` |
| ACL | `ino:u64_be || acl_type:u8 || ordered_acl_id:u64_be` |
| Extent | `ino:u64_be || chunk_index:u64_be || logical_offset:u64_be` |

A point lookup loads one index block, binary-searches restart keys, then reads at most one candidate
data block. Very large logical groups may continue into several blocks; their full-key ranges remain
distinct in the index.

## 9. Filter blocks

Filters are optional because a fully memory-loaded pack does not need them. Cold packs SHOULD include
filters for InodePatch and Dentry and MAY include them for Xattr, ACL and Extent.

The filter payload is:

```text
algorithm       u8      // BlockedBloomV1=1
probes          u8      // default 7
bits_per_key    u8      // default 10
reserved        u8
logical_keys    uvarint
filter_bytes    bytes
```

Hash input is the complete canonical key. Extent filters hash `(ino, chunk_index)` rather than each
logical interval. Filter negatives are authoritative; positives still require index lookup.

Filters duplicate a small amount of information for remote negative-lookup performance. They can be
disabled to produce the minimum-size archival pack.

## 10. Group framing

Dentry, Xattr, ACL and Extent tables group records by their shared filesystem key. A data block begins:

```text
group_segment_count uvarint

repeated group_segment_count times:
  group_key_delta    table-specific compact key
  group_flags        u8
  group_payload_len  uvarint
  group_payload      [group_payload_len]
```

Group flag bit 0 is CONTINUATION. A group is kept in one block when its raw encoding fits the block
target. A group larger than the target is segmented only at entry boundaries; every segment repeats
the compact group key and has its own entry restart table. One segment may grow to the 16 MiB limit.

Within name-keyed groups, entries use:

```text
entry_count uvarint

repeated entries:
  shared_name_bytes uvarint
  name_suffix_len   uvarint
  name_suffix       [name_suffix_len]
  compact value

restart offsets [u32_be]
restart_count   u32_be
```

Every header-configured `restart_interval` entry has `shared_name_bytes=0`. The previous name resets
at each group segment.

## 11. InodePatch table

The table is sorted by inode. A data block payload is:

```text
inode_count uvarint

repeated inode_count times:
  ino_delta     uvarint  // previous inode is zero at block start
  inode_op      u8
  field_mask    uvarint
  selected fields in stable bit order
```

Inode operations:

```text
Create=0
Patch=1
Delete=2
```

Field-mask bits:

```text
0  kind            u8
1  size            uvarint
2  mode            uvarint
3  uid             uvarint
4  gid             uvarint
5  rdev            uvarint
6  nlink           uvarint
7  atime_ns        svarint delta from PackHeader.creation_time
8  mtime_ns        svarint delta from PackHeader.creation_time
9  ctime_ns        svarint delta from PackHeader.creation_time
10 parent_hint     option<uvarint>: tag u8, then value when Some
11 data_version    uvarint
12 symlink_target  option<bytes>: tag u8, then bytes when Some
13..63 reserved
```

Create requires the fields needed to construct a valid inode for its kind. Patch requires a non-zero
mask and contains only fields different from the effective parent inode. Delete requires a zero mask.
A hardlink removal emits Delete only when the effective inode reaches zero links and has no remaining
visible dentry. Otherwise it emits the required `nlink`/parent patch and dentry Whiteout only.

Resolving an inode walks newest to oldest, filling fields not already supplied by newer patches. It
stops at Delete or once a Create/base inode supplies all required fields. A patch whose parent cannot
supply missing required fields is corrupt.

This representation turns a chmod-only layer into approximately:

```text
ino delta + Patch tag + one-byte mask + mode varint
```

rather than another complete inode row.

## 12. Dentry table

The group key is `parent_ino`, delta-encoded from the previous group in the block. Entries are sorted
by raw filename bytes.

Each compact value starts with one byte:

```text
0x00           Whiteout
0x80 | kind    Put; followed by child_ino uvarint
```

Bits 4..6 in a Put tag are reserved; `kind` occupies the low four bits. Put requires a valid child
inode and inode kind. The hint must match the resolved child inode kind. Whiteout has no payload.

Parent inode, layer ID, filename length field names and sequence are not repeated per database-style
row. Filename prefixes are shared within the directory group.

## 13. Xattr table

The group key is inode, delta-encoded from the previous group in the block. Names use the group entry
prefix encoding.

Value encoding:

```text
op u8              // Put=0, Whiteout=1
if Put:
  value bytes
```

Empty Put values are valid. Whiteout carries no value. No sequence or repeated inode is stored.

## 14. ACL table

The group key is inode. Entries are sorted by `(acl_type, acl_id)`. Each entry is:

```text
acl_type       u8
acl_id_delta   svarint  // previous ID for the same acl_type, otherwise from zero
op             u8       // Put=0, Whiteout=1
if Put:
  value        bytes
```

ACL values remain opaque to the pack codec but are size-bounded and validated by the BrewFS ACL
adapter.

## 15. Extent table

Extents are grouped first by inode and then by chunk:

```text
inode_group_count uvarint

repeated inode groups:
  ino_delta          uvarint
  chunk_group_count  uvarint

  repeated chunk groups:
    chunk_index_delta  uvarint
    interval_count     uvarint

    repeated intervals:
      gap_from_previous_end uvarint
      length                uvarint
      kind                  u8  // Data=0, Hole=1
      if Data:
        slice_id            uvarint
        slice_offset        uvarint
```

At the start of each chunk, `previous_end=0`. For every interval:

```text
logical_offset = previous_end + gap
end            = logical_offset + length
previous_end   = end
```

Required invariants:

- intervals are non-overlapping and strictly ordered;
- `length > 0` and all additions are checked;
- interval end is at most the header chunk size;
- Hole has no slice payload;
- Data uses a non-zero slice ID and a valid slice range;
- adjacent Hole intervals are merged;
- adjacent Data intervals are merged when they reference the same slice with contiguous slice offsets.

A gap means inherit from the parent. A Hole explicitly hides parent data. Because intervals already
represent the final private result, no write sequence is stored.

## 16. Optional GC reference sidecar

BRLP v1 has no SliceRef table: GC can stream the Extent table and read every Data slice ID. This avoids
storing the same slice IDs twice and keeps the core directory fixed at five entries.

Deployments that require faster GC may create an optional derived `.brlg` sidecar keyed by the pack
hash. Its payload begins with the source pack hash followed by sorted unique slice IDs encoded as:

```text
count          uvarint
first_slice_id uvarint
following ID deltas as uvarint
```

The sidecar is not referenced by filesystem reads and is excluded from both `pack_hash` and
`delta_digest`. GC validates its source pack hash before use and falls back to streaming Extent when
the sidecar is absent or corrupt. It is always safe to regenerate or delete.

## 17. PackFooter

The footer is exactly 72 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: ASCII `BRLPEND1` |
| 8 | 8 | complete object length |
| 16 | 8 | table-directory offset |
| 24 | 8 | table-directory length |
| 32 | 32 | BLAKE3 pack hash over all bytes before footer |
| 64 | 4 | CRC32C over bytes `[0, 64)` |
| 68 | 4 | reserved, zero |

A cold reader opens a pack with three range reads at most: footer, directory and header. It validates
their duplicate offsets, lengths, IDs and hashes before trusting indexes.

## 18. Logical digest stream

The logical digest identifies the minimal semantic layer, not its compression. It is encoded in this
order:

```text
magic "BRLPDEL1"
logical schema version u32_be
parent root hash [32]

for table in InodePatch, Dentry, Xattr, ACL, Extent:
  table tag u8
  logical record count uvarint
  canonical decoded records in logical-key order
```

Canonical decoded values use CompactCodecV1 without block/group prefix compression. The digest stream
does not contain indexes, filters, padding, codecs, timestamps from the physical header or an optional
GC reference table.

```text
delta_digest = BLAKE3(logical digest stream)
root_hash    = BLAKE3("BRLPROOT1" || parent_root_hash || delta_digest)
```

Two packs with different block sizes, zstd levels, filters or GC indexes therefore describe the same
revision when their logical records and parent are equal.

## 19. Builder algorithm

```text
Input: immutable MemLayer snapshot + parent ReadonlyLayer

1. Collapse each logical key to its final operation.
2. Resolve parent values required for comparison.
3. Convert full in-memory inodes to Create/Patch/Delete records.
4. Normalize each dirty (ino, chunk) interval map.
5. Remove values and ranges equal to parent.
6. Sort/group the five table streams.
7. Feed decoded records to the logical digest encoder.
8. Encode grouped data blocks, flushing near the target block size.
9. Build each table index and optional filter.
10. Write TableDirectory and finalize PackHeader.
11. fsync staging bytes and perform a full local reopen/scrub.
12. Hash pre-footer bytes, append PackFooter and fsync again.
13. Return a closed, immutable LayerPackArtifact.
```

The builder streams output and does not assemble the pack in one `Vec<u8>`. Extent normalization may
use an interval map per dirty chunk. Very large tables use sorted paged iterators or an external sort.

The builder returns no artifact when parent lookup fails, no-op comparison is incomplete, a required
inode field is unresolved or the reconstructed effective view differs from the frozen input view.

## 20. Read algorithms

### Inode

Bloom test `ino`, index-search candidate block, find the inode delta, then merge InodePatch records
newest to oldest until complete or deleted.

### Lookup

For each layer newest-first: Bloom test `(parent,name)`, find the directory group/block, binary-search
its name restart table, return Put/Whiteout. Stop at the first result.

### Readdir

Open the directory group stream from every layer containing `parent_ino`, then perform a k-way merge
by name. For equal names, the newest layer wins; Whiteout is not returned.

### Xattr and ACL

Use the inode group and merge keys newest-first. Put supplies the value; Whiteout terminates lookup for
that key.

### Extents

Open `(ino, chunk)` groups newest-first. Fill uncovered logical ranges from each normalized group.
Data produces slice-backed reads; Hole marks covered zero/absent ranges; gaps continue to lower layers.

## 21. Memory loading

For a frequently used base pack, decode into arena-backed structures:

```rust
pub struct HotFrozenLayer {
    inode_patches: SortedArena<InodePatch>,
    directory_groups: SortedGroupArena<u64, DentryValue>,
    xattr_groups: SortedGroupArena<u64, XattrValue>,
    acl_groups: SortedGroupArena<u64, AclValue>,
    extent_groups: SortedExtentArena,
    byte_arena: Arc<[u8]>,
}
```

Names, symlink targets and xattr/ACL values are stored once in `byte_arena`; indexes contain offsets
and lengths rather than separate heap allocations. The resulting `Arc<HotFrozenLayer>` is shared by
all agents based on the same pack hash.

Cold mode retains header, directory, filter and index blocks and caches decoded data blocks by
`(pack_hash, table, ordinal)`.

## 22. Binary transport

A metadata server may return complete physical blocks unchanged:

```text
BatchHeader:
  magic            BRLPBAT1
  version          u16
  pack_hash        [32]
  table_kind       u8
  first_ordinal    u32
  block_count      u32
  payload_len      u64
  header_crc32c    u32

Payload:
  one or more 8-byte-aligned BRLB blocks
```

The client verifies each BRLB header and CRC, optionally decompresses, and inserts decoded blocks into
the same cache used for S3 range reads. There is no per-record JSON, field-name repetition or
encode/decode through a second RPC schema.

## 23. Space characteristics

The compact format removes the following common row/KV costs:

| KV-style cost | BRLP representation |
|---|---|
| layer ID in every key/row | once in header |
| parent inode in every dentry | once per directory group/segment |
| inode in every xattr/ACL | once per inode group/segment |
| `(ino, chunk)` in every extent | once per extent group |
| full inode copy-up | changed-field bitmap patch |
| mutation sequence in sealed rows | removed after normalization |
| overwritten extent history | reduced to final non-overlapping intervals |
| repeated filename prefixes | prefix-compressed with restart points |
| textual field names and JSON numbers | stable tags and varints |
| database page/index overhead | one compact table/index per layer |
| deleted temporary files | eliminated when parent has no corresponding object |

Filters, indexes and the four-bit dentry type hint are the intentional lookup redundancies. They are
bounded; filters can be disabled, and the dentry hint avoids loading an inode merely to return a
directory-entry type.

## 24. Validation limits

| Item | Default | Hard limit |
|---|---:|---:|
| raw data-block target | 64 KiB | 16 MiB |
| decoded record | — | 16 MiB |
| group segment | 64 KiB target | 16 MiB |
| restart interval | 16 | 256 |
| table count | 5 | 16 with recognized feature flags |
| layer depth | soft 8 | 32 |
| filename | current BrewFS/POSIX limit | current limit |
| xattr value | current BrewFS limit | current limit |

Every `offset + length`, delta reconstruction, interval end and allocation is checked. Reserved fields
are zero on write. Unknown major versions or required flags are rejected.

## 25. Required tests

### Golden format tests

- exact PackHeader, TableDirectory, BRLB and PackFooter bytes;
- every scalar's minimum and maximum canonical encoding;
- Create/Patch/Delete inode variants and every field bit;
- grouped directory names with restart boundaries;
- normalized Data/Hole extent bytes;
- physical repacking preserves logical hash.

### Reduction tests

- create then delete emits nothing;
- Put then Whiteout over absent parent emits nothing;
- chmod then restore parent mode emits no inode patch;
- chmod-only emits only the mode bit/value;
- repeated overwrite emits only final visible extents;
- adjacent compatible extents merge;
- parent-equal data intervals disappear;
- truncate/hole/extend never exposes hidden parent data;
- hardlink count and dentry changes reconstruct exactly.

### Query equivalence

For generated parent and MemLayer states:

```text
resolve(parent + mutable MemLayer)
    ==
resolve(parent + decoded BRLP)
```

Run this property for lookup, readdir, inode attributes, xattrs, ACLs and reads across all chunk
boundaries.

### Corruption and fuzzing

- arbitrary bytes for every decoder;
- non-canonical varints;
- invalid restart tables and key prefixes;
- block/index key-range disagreement;
- oversized zstd output and trailing compressed input;
- truncated header, table, index, directory and footer;
- bit flips in every checksum/hash scope;
- parent cycle, wrong parent hash and excessive chain depth.

No case may panic, allocate past limits or silently fall back to another metadata source.

## 26. Implementation types

```text
src/workspace_overlay/memory_s3/pack/
  format.rs          PackHeader, TableEntry, BlockHeader, PackFooter
  varint.rs          canonical uvarint/svarint
  block.rs           group/restart encoding and checked decoding
  inode.rs           InodePatch schema and resolver
  dentry.rs          directory-group codec
  xattr.rs           xattr-group codec
  acl.rs             ACL-group codec
  extent.rs          normalized extent codec
  index.rs           block index
  filter.rs          optional Bloom filter
  reduce.rs          parent-aware no-op elimination
  builder.rs         streaming pack writer
  reader.rs          range reader
  scrub.rs           full verification
```

The physical-format module depends only on stable layer model types, BLAKE3, CRC32C, bytes and the
selected compression codec. It must not depend on a concrete KV backend, SQL entity, FUSE request or
process-local Rust archive layout.
