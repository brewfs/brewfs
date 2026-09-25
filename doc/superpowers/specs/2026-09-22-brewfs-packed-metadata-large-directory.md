# BrewFS Packed Metadata: Large-Directory And Training-Data-Informed Design

Status: **superseded working note; see `2026-08-24-brewfs-clustered-frozen-metadata-v2.md`**

This document refines the clustered frozen metadata v2 RFC for the workload
that motivated the current packed-metadata work: immutable, read-only trees
with very large directories and many small files. It does not change the
existing `packed-metadata-v1` format or its reader.

The revised v2 SPEC is the only normative source for snapshot publication,
cluster union, `DirKey`, `LocalNodeId`, data seals, and crash-safe upload. This
document records workload motivation and examples only. Where its example
targets differ, the revised v2 SPEC wins; this note must not be used to infer
an implemented format or a performance result.

## 1. Design decision

Use a **clustered, range-partitioned namespace** with independently decodable
metadata batches:

```text
Snapshot manifest
  -> small mount/cluster/route indexes
  -> immutable metadata clusters (one or more objects)
       -> pageable directory range index
       -> namespace batches
       -> extent batches
       -> cold-attribute batches
  -> data seal -> data packs
```

The unit of S3 transfer, authentication, decompression, RAM admission, and
eviction is a metadata batch, not a directory and not the whole snapshot.

The key invariant is:

```text
resident metadata = O(cache_budget + active_readdir_windows)
```

It must not be proportional to the number of entries in the largest directory,
the number of files in the snapshot, or the number of preceding batches.

## 2. What training-data formats teach us

Large training systems deliberately avoid a file-per-sample layout. Their
common shape is a small control/index layer followed by byte-sized shards and
restartable record batches:

| Format/family | Physical organization | Relevant property |
| --- | --- | --- |
| WebDataset | numbered `.tar` shards; files with one basename form one sample | sequential object-store reads and shard-level parallelism |
| Mosaic Streaming MDS | MDS shards with an offset table and per-sample payloads | random sample access without enumerating all earlier samples; the current writer default is a 64 MiB shard size limit |
| Hugging Face Arrow/Parquet exports | split -> `data-xxxxx-of-yyyyy` shards -> record batches/row groups | the current `save_to_disk` documentation uses a 500 MB default maximum shard size and permits explicit shard counts |
| Megatron indexed datasets | a data stream plus a separate `.idx` file | mmap-friendly sample lookup; the index is not the sample payload |

References:

- [WebDataset format](https://github.com/webdataset/webdataset#the-webdataset-format)
- [Mosaic MDS writer](https://github.com/mosaicml/streaming/blob/main/streaming/base/format/mds/writer.py)
- [Hugging Face `Dataset.save_to_disk`](https://github.com/huggingface/datasets/blob/main/src/datasets/arrow_dataset.py)
- [Megatron indexed dataset](https://github.com/NVIDIA/Megatron-LM/blob/main/megatron/core/datasets/indexed_dataset.py)
- [Apache Arrow record batches](https://arrow.apache.org/docs/format/Columnar.html)

There is no universal training-data file count. A corpus can contain millions
or billions of logical samples and trillions of tokens, while the object store
contains a manageable number of byte-sized shards. Shard size is selected by
read bandwidth, worker parallelism, restart cost, and cache budget; sample
count is a consequence of the encoded byte size. BrewFS should use the same
rule for directories: **bytes and bounded records are hard limits; entry count
is only a secondary guard**.

## 3. Problems in packed-metadata-v1

The current v1 reader is progressively loaded at the page level, but its
public operations still have whole-directory semantics:

- `FrozenCatalog::readdir` returns `Vec<FrozenDirectoryEntry>`;
- `scan_namespace_prefix` accumulates every matching row before returning;
- `lookup_inode_for_readdir` must resolve attributes for the entire returned
  vector;
- the producer stores generic key/value rows (`i` and `d` keys), so the
  directory boundary is a key prefix rather than an explicit directory
  descriptor and range map;
- the old eager catalog still loads a complete object closure;
- one giant namespace object remains a publication and object-lifecycle unit.

Increasing the range GET size does not fix the first two points. A directory
with one billion entries can still exhaust RAM even if each individual page is
small. The v2 format already addresses object-level clustering, but the
runtime must expose a page/cursor API before that format can be used safely.

## 4. Snapshot and cluster layout

Keep the v2 object model:

```text
BRFSM002 manifest
  - mount trie root
  - cluster descriptor table
  - merge-route index root

BRFCL002 metadata cluster
  - cluster superblock
  - Merkle index roots
  - directory range descriptors
  - namespace batches
  - extent batches
  - cold-attribute batches

BRFDS002 data seal
BRFDP data objects
```

One logical tree is planned into multiple immutable clusters. A cluster is a
packing and upload unit, not a mutable database shard. Unchanged clusters can
be reused by a later snapshot.

Recommended defaults for the first implementation:

| Unit | Target | Hard limit | Purpose |
| --- | ---: | ---: | --- |
| cluster stored metadata | 256 MiB-1 GiB | 2 GiB | enough sequential bandwidth without making one object irreplaceable |
| namespace batch raw payload | 512 KiB-1 MiB | 4 MiB | bounded lookup latency and RAM admission |
| extent batch raw payload | 512 KiB-1 MiB | 4 MiB | independent file read planning |
| attribute batch raw payload | 256-512 KiB | 4 MiB | cold xattrs, ACLs, and symlink data |
| entries per namespace batch | 8K-32K | 64K | secondary protection for very short names |
| Merkle index node | 4 KiB | 64 KiB | one bounded Range GET and decode unit |

The byte limit wins over the record limit. A directory containing long names
will split earlier; a directory containing tiny names will not create huge
entry counts in one batch.

For an object-store deployment that benefits from fewer, larger sequential
requests, adjacent batches may remain in one `.brfc` object and be fetched with
one bounded Range GET. A cluster larger than the hard object limit is split
into independently authenticated cluster parts referenced by the manifest.
There is never a requirement to download all parts at mount time.

## 5. Directory representation

### 5.1 Directory descriptor

Each local directory contribution has a small authenticated descriptor:

```text
DirDescriptor {
  dir_key:        16 bytes
  local_node_id:  u32
  total_entries:  u64
  segment_count:  u32
  first_segment:  SegmentLocator
  last_segment:   SegmentLocator
  attributes:     InodeHot
}
```

The descriptor is not the directory contents. It is enough to validate a
directory identity, locate its range index, and report its size without
materializing entries.

### 5.2 Lexicographic range segments

Entries in one contribution are sorted by raw POSIX name bytes and split into
contiguous name ranges. Each segment contains:

```text
SegmentLocator {
  first_name:     bytes
  last_name:      bytes
  batch_id:       u32
  batch_offset:   u32
  batch_length:   u32
  entry_count:    u32
  digest:         [u8; 32]
}
```

The namespace index is a Merkle B+ tree keyed by:

```text
(DirKey, first_name) -> SegmentLocator
```

The index leaf also stores the segment's last name, so a lookup can determine
whether the selected segment is authoritative without fetching its neighbor.
Restart points occur at least every 16-32 names, allowing a leaf or batch to be
decoded independently of its predecessor.

This is intentionally a **range** partition, not only a hash partition. It
preserves sorted `readdir` order and makes a cursor advance through adjacent
segments with sequential Range GETs. A small optional hash route table is still
allowed for a merged directory with many contributors, but it is an exact
candidate filter and never the source of namespace truth.

### 5.3 Namespace batch encoding

Each batch is independently decodable:

```text
BatchHeader {
  magic/version/cluster_id
  batch_id/stream_ordinal/predecessor
  first_name/last_name fingerprints
  first_local_node/new_node_count
  raw_len/stored_len/codec
  CRC32C + BLAKE3 digest
}

DirectorySegment {
  local_node_id          uvarint
  flags                  START | CONTINUE | END
  total_entry_count      uvarint on START
  first_name             bytes
  entry_count            uvarint
  entries:
    shared_name_len      uvarint
    name_suffix          bytes
    child_kind           packed bits
    child_id_delta or existing_local_node_id
    inline InodeHot when this is a new inode
}
```

The first segment carries the full parent ID and first name. Later entries use
front-coded names and local-ID deltas. A new directory carries its derived
`DirKey`; regular files do not repeat it. Extents and cold attributes remain in
separate LocalNodeId-range batches.

Recommended compactness rules:

- front-code names with restart points so a target entry is decodable without
  the whole segment;
- use uvarint deltas for local IDs, sizes, timestamps, extent gaps, and slice
  offsets;
- dictionary-code repeated uid/gid/mode classes within a batch;
- omit default fields using a presence bitmap;
- store one authenticated digest per batch and one per data frame, not one
  32-byte content hash per small file;
- compress each batch independently with Zstd; never require decompression of
  an entire cluster to read one directory range.

## 6. Very large directories and cluster planning

The planner first keeps hardlinks and their inode records together. It then
partitions directories by contiguous name ranges until both the batch and
cluster byte targets are satisfied.

For a directory with a billion entries, the expected shape is:

```text
one DirKey / one virtual directory
  -> tens of thousands of lexicographic segments
  -> segments distributed over many immutable clusters
  -> one small range index, not one giant in-memory map
```

The directory can be split across clusters without changing its visible
identity. Every contribution repeats only the small directory skeleton and
hot directory attributes. A file hardlink cannot cross cluster boundaries;
the planner co-locates its inode equivalence class or rejects the plan.

For a directory with multiple contributions, the manifest stores one
authenticated merge-route record per `DirKey`. It maps name ranges to exact
candidate contributors. `readdir` performs a k-way merge of contributor
cursors; equal names are validated according to the v2 collision rules.

There is no `Vec` containing the whole directory. A cursor retains at most the
current and next segment per contributor, plus the caller's bookmark.

## 7. Runtime read path

### Mount

1. Fetch and authenticate the manifest superblock and index roots.
2. Fetch only the root mount/route records and selected cluster superblocks.
3. Do not fetch namespace batches, extents, or cold attributes speculatively.

### `lookup(parent, name)`

1. Resolve the virtual directory's contributors.
2. Traverse the pageable namespace B+ root-to-leaf index.
3. Fetch one exact segment batch per route candidate, with single-flight.
4. Return a hot inode record and a stable `RecordLocator`.
5. Release the segment pin after the lookup result owns what it needs.

An absent name is authoritative only after every exact route candidate covering
that name has been checked. The negative cache is keyed by the immutable
snapshot identity.

### `readdir`

Replace the current whole-vector operation with a cursor operation:

```rust
async fn readdir_page(
    &self,
    dir: NodeRef,
    cursor: Option<ReadDirCursor>,
    max_entries: usize,
) -> Result<ReadDirPage, FrozenReadError>;
```

`ReadDirCursor` contains the immutable snapshot ID, directory key, contributor
positions, the last emitted raw name, and a format version. It does not contain
a pointer into an evictable batch. A page response owns only its returned
entries and the next cursor.

The FUSE adapter requests a bounded page sized for its output buffer. It keeps
only the active cursor window pinned. After a batch is evicted, the cursor
resumes with an indexed lower-bound search by `(DirKey, last_name)`.

### `getattr` and `read`

Active FUSE inode state copies the compact hot attributes and keeps a locator,
not a pointer into a namespace batch. A read fetches only the extent batches
covering the requested file/chunk range, resolves SliceIds through the Data
Seal, and releases the extent batch after the read plan is built.

## 8. Cache and memory policy

Use the four v2 tiers:

```text
L0 pinned RAM: manifest roots, active inode hot records, cursor windows
L1 RAM: decoded index nodes and decoded metadata batches
L2 disk: authenticated compressed batch bytes (optional)
L3 S3: immutable source
```

Account namespace, extent, attribute, route, and index bytes separately under a
single hard byte budget. Sequentially scanned batches enter probationary LRU;
they do not promote merely because a full directory scan touched them. Current
cursor batches, active lookup waiters, and index roots are pinned. Eviction
drops decoded state but keeps the compact locator and snapshot identity.

The memory bound for one large directory is therefore approximately:

```text
contributors * 2 * namespace_batch_target
  + index/cache budget
  + FUSE output buffer
```

not `entry_count * sizeof(FrozenDirectoryEntry)`.

## 9. Compatibility and rollout

1. Keep `packed-metadata-v1` read-only and unchanged for existing manifests.
2. Add a separate `packed-metadata-v2` feature/module for `.brfsm`/`.brfc`.
3. Do not fall back from a v2 snapshot to Redis/TiKV.
4. Build v2 snapshots offline from a source inventory, verify all cluster and
   Data Seal references, then publish the manifest by CAS.
5. Reuse unchanged sealed clusters when creating a later snapshot.

The first implementation milestone should support one cluster and one
contributor. Multi-cluster directory union comes before private overlays and
resumable ingestion.

## 10. Acceptance gates

Format and reader tests:

- independent targeted decode of any namespace batch with its predecessor
  absent;
- restart-point lookup for raw names containing arbitrary bytes;
- deterministic front-coded encoding and authenticated range boundaries;
- one directory split across many clusters yields the same sorted namespace;
- hardlinks remain valid and cross-cluster hardlinks are rejected;
- a corrupt or missing batch never becomes an authoritative ENOENT.

Bounded-memory tests:

- synthetic one-million-entry and one-billion-entry directories;
- peak resident metadata remains within the configured cache budget;
- `readdir_page` survives eviction and refetch from any cursor position;
- a full scan does not evict protected random-lookup batches;
- active inode handles survive eviction of their source batch.

Performance tests:

- random lookup: one index path plus one batch fetch;
- sequential readdir: bounded active windows and coalesced adjacent ranges;
- 36,000, 1,000,000, and synthetic 100,000,000-entry scans;
- cold read with packed v2, ordinary BrewFS + Redis, and JuiceFS under the
  same S3, block, compression, cache, FUSE, and fio settings;
- record metadata GET count, data GET count, range bytes, cache hits, peak
  decoded bytes, and p50/p99 lookup/readdir latency separately.

## 11. Implementation order

1. Introduce `ReadDirCursor`/`readdir_page` in the frozen facade without
   changing v1 semantics.
2. Implement the 4 KiB Merkle index node and independently decodable batch
   header/codec with golden fixtures.
3. Implement one-cluster directory descriptors, range segments, and targeted
   lookup.
4. Implement bounded cursor readdir and eviction/refetch.
5. Add extent/attribute batches and Data Seal resolution.
6. Add clustered manifest union and exact merge routes.
7. Add deterministic planner, offline producer, S3 upload verification, and
   crash-resume state.
8. Run the large-directory and three-way cold-read gates before claiming a
   performance advantage.

The existing v2 RFC already specifies the publication boundary and cluster
identity rules. This proposal's non-negotiable implementation change is the
cursor-based directory API: without it, no on-disk packing strategy can make a
one-billion-entry directory safe to mount.
