# Native Packed Metadata Large-Scale Validation (2026-09-24)

## Scope

This is the current post-optimization validation for the read-only
`packed-metadata-v1` mount. It uses a 100,000-file corpus and a matched
zero-data-cache ordinary BrewFS control. The control is a runtime A/B on the
same worktree; it is not an older checkout.

Artifacts:

- Packed: `docker/compose-xfstests/artifacts/perf-run-1790233049-1043/`
- Flat/Redis control: `docker/compose-xfstests/artifacts/perf-run-1790233979-13612/`

## Cold-Read Contract

Both runs used RustFS, a 4 MiB block size, 64 MiB fio working data, `direct=1`,
20-second time-based fio jobs, and a fresh mount for each measured tool. The
runner forced:

- `BREWFS_READ_MEMORY_BYTES=0` and `BREWFS_READ_SSD_BYTES=0`;
- VFS and range prefetch disabled;
- FUSE read direct I/O and `keep-cache=0`;
- BrewFS cache-root removal before every read tool;
- a successful kernel `drop_caches` request (the run fails closed otherwise);
- before/after stats checks requiring zero data-cache hit deltas.

The packed fixture has 16 directories with 6,250 4 KiB files each, plus a
64 MiB `bench/read.bin` file and POSIX special entries. The fixture publisher
does not start Redis or TiKV. The flat control creates its fio file during the
prefill phase, drains it, remounts, and only then measures reads; its measured
read phase also reports zero cache hits.

## Results

| Workload | Packed | Flat/Redis | Packed / flat |
| --- | ---: | ---: | ---: |
| 100k stat + first-byte scan | 586.51 files/s (170.50 s) | not run | n/a |
| Sequential fio read | 379.71 MiB/s | 182.59 MiB/s | 2.08x |
| Four-job random fio read | 1.23 GiB/s (1,257.11 MiB/s) | 1.13 GiB/s (1,161.22 MiB/s) | 1.08x |

The packed scan passed all 100,000 files with 0 errors and read 390.6 MiB.
Its counters show `FUSE read=100000`, `S3 GET=100001`, `range GET=100000`,
`full GET=0`, and zero data/page/background cache hits. The packed POSIX check
passed 11 semantic checks and rejected all 10 attempted mutations.

The fio reports also show zero data-cache hits on both sides. The matched fio
shape issued 7,612 packed versus 3,668 flat sequential GETs and 24,078 packed
versus 23,300 flat random GETs. The extra packed sequential requests are
consistent with the packed read path's exact object ranges; the measured
RustFS GET latency was 2.09 ms packed versus 4.17 ms in this flat run, so the
single-run bandwidth ratio includes backend scheduling variance. Repeat A/B
runs are required before publishing a universal throughput claim.

## Why The Earlier Packed Run Lost

The earlier 100k scan ran at about 223 files/s. The hot path was not Redis:
packed had no metadata client and no KV operations. The main costs were:

1. Each 4 KiB file still needs one remote data range request. The metadata is
   compact, but it cannot remove 100,000 independent data-object RTTs.
2. A streaming catalog cache hit copied the complete decoded index page and
   its entry vectors. That made a metadata-heavy scan pay avoidable allocation
   and clone cost.
3. An offset-zero small read was previously classified as a full-block read,
   which could download a 4 MiB object for a 4 KiB file.

The accepted fixes are:

- `ReaderPageCache` stores `Arc<IndexPage>` and lookup copies only the selected
  value/child (`src/native_base/frozen/mod.rs` and `catalog.rs`).
- Uncompressed offset-zero small reads use the exact S3 range path
  (`src/chunk/store.rs`), while full-block reads retain the existing
  single-flight behavior.
- The cold-read runner disables all data caches and rejects incomplete cache
  evidence instead of producing an invalid artifact.

The page-cache change raised the 100k scan from roughly 223 to 586 files/s.
The remaining small-file limit is remote request amplification, not a packed
metadata lookup or Redis bottleneck. A future clustered v2 format can reduce
metadata page/object overhead further, but it cannot make independent 4 KiB
payload reads free; batching or a data-object layout change is needed for that.

## Packed Read Path And Storage

The production reader is `StreamingFrozenMetadataCatalog`:

1. It fetches and validates the manifest `.brfsm` once. The manifest contains
   immutable namespace, data, and inventory object references.
2. It range-fetches only the fixed container header and referenced BNPG index
   pages. Adjacent page ranges are coalesced up to the configured bound and
   concurrent requests for one page use single-flight.
3. Every page is digest-checked, Zstd-decoded, structurally validated, and
   retained in a process-local metadata budget (512 MiB by default). Inode
   attributes and immutable extent rows have separate bounded caches.
4. The manifest and decoded metadata are not persisted under
   `BREWFS_CACHE_ROOT`; that root controls the ordinary data-block cache. A
   remount drops the process-local catalog and the cold-read runner removes the
   data cache root before measuring.

The current v1 objects are authenticated `.brfc` containers with BNPG leaves,
shared-prefix keys, and compressed pages. This is compact compared with one KV
record per object, although it is still the generic v1 layout rather than the
parent-first/delta/arena clustered v2 layout.

## Million-File Hierarchy Validation (2026-09-25)

The packed benchmark profile now has a fixed million-small-file shape:

- `1,000,000` files, each `100 KiB`;
- three directory levels, with fanout `10` at every level;
- `1,000` files in each of the `1,000` leaf directories;
- `1,113` directories including the root, benchmark directory, and POSIX
  verification directory.

The fixture publisher generated and uploaded this exact corpus to local RustFS
without materializing 100 GiB of duplicate payload. All small files reference
one immutable data block, while the scan still reads the full logical payload
when `ReadMode=full` is selected. The local publication evidence is in
`docker/compose-xfstests/artifacts/local-million-fixture/fixture.log` and its
manifest key is in `manifest-key.txt`.

The updated in-container scanner validates the hierarchy independently of the
file total: every internal level must have the configured fanout, the leaf
count must match `fanout^levels`, and every leaf must contain the configured
number of files. A WSL Compose cold-read smoke run with the same scanner passed
with `1,000` files over two levels (`10 x 10 x 10`), `102,400,000` logical and
payload bytes, zero cache hits, and no Redis service. Its artifact is
`docker/compose-xfstests/artifacts/perf-run-1790328572-19014/`.

The Aliyun ECS runner is ready to execute the full profile on a 32 GiB / 100
GiB ESSD instance, but no cloud performance number is recorded yet: the
configured Aliyun AccessKey currently returns `AccessKeyDisabled`/
`InvalidAccessKeyId.Inactive`. The runner must receive a pushed commit/ref and
valid ECS, VPC, and Cloud Assistant credentials before the million-file cold
read can be accepted as a cloud result.

## Conclusion

After the read-path fixes, packed is ahead in this strict no-data-cache A/B:
2.08x sequential fio and 1.08x four-job random fio in the recorded run, while
the 100k metadata-heavy scan is correct and bounded. The result is not a claim
that packed always wins: data RTT and backend variance dominate these tests,
and a matched flat 100k scan is still a separate experiment. Any future table
update must retain the zero-hit evidence and report the effective wall-time
bandwidth alongside fio active bandwidth.
