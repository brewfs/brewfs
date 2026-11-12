# Packed-v3 completion: correctness and measurement checkpoint (2026-10-02)

Latest follow-up: [cold attributes / hardlinks and direct VFS validation](packed-v3-cold-hardlink-validation-2026-10-03.md).
The stage-specific counts below describe their own code tips, not later changes.

## Scope and status

This checkpoint records the staged correctness/wire/producer implementation of
the full packed spec completion task. **It is not a declaration that wire 005, P5 workspace lower,
publish/recovery/GC or the entire spec is complete. No new performance win is
accepted.** The current base is `a429b0e`; the implementation remains in the
working tree until the current-iteration CI gate completes.

The spec now separates legacy 004 from the planned 005 contract, identifies
manifest content digest rather than snapshot-id as the authentication anchor,
and provides a completion/evidence checklist. Shared-frame overscan is compatible
with strict demand-only fetching, but active fetching of an unrequested frame is
not. Inline payload still costs GroupMeta transfer and retention.

## Constrained-budget FUSE diagnosis

The inherited diagnostic source changes were retained and expanded to report
only safe S3 status, allowlisted service code, request class or stream kind. Raw
SDK error source chains, signatures, headers and endpoint URLs are not logged.

Same diagnostic binary invocation and fixture/scanner protocol: 10,000 independent 100 KiB files, shuffled
stat, 16 workers, 8 MiB metadata budget with auto warm-up, effective FUSE TTL 0,
direct I/O, keep-cache 0 and zero payload/window/decoded/prefetch budgets.

- Inherited-proxy reproduction:
  `docker/compose-xfstests/artifacts/local-packed-completion-classified-20261002-134724/`.
  The mount logged 8,557 backend failures with HTTP 502 and unknown service code,
  no validation-class failures. RustFS server summary contained no error/panic/
  checksum/timeout/502 messages. These scans are **invalid performance evidence**.
- Proxy-cleared local process reproduction:
  `docker/compose-xfstests/artifacts/local-packed-completion-noproxy-20261002-135130/`.
  All 10,000 stat calls completed, errors=0, backend failures=0, with normal
  teardown and Compose/volume removal. No catalog or wire interpretation was
  relaxed between the two trials.

The old reproducer's copied `binary-sha256.txt` lists historical baseline/
candidate filenames, not the diagnostic executable actually invoked. Treat those
files as provenance-incomplete diagnostics, not hash-verified matched performance
artifacts. The new runner hashes the actual executable and fixture it launches.

This isolates the observed local reproducer to its inherited proxy environment;
it does not prove that every future EIO has the same cause or justify disabling
proxy transport globally. The new `tools/perf/run_packed_local.sh` clears proxy
variables **only within the loopback test invocation**, records the proof, refuses
an already-running fixed-name RustFS service, bounds runtime and preserves failed
artifacts. It does not edit operator configurations or credentials. An earlier
runner attempt failed before scanning due to embedded-Python syntax; its artifact
is retained and the added runner syntax/timing tests cover that failure.

## Complete payload validation

`docker/compose-xfstests/artifacts/packed-local-20261002T060113Z-1166741/`:

- 10,000 / 10,000 independent files, 1,024,000,000 validated payload bytes;
- checksum 1,273,096, errors=0;
- scan 51.706 s; runner active 51.801 s, drain 0.109 s, total 52.335 s;
- diagnostic active 18.852 MiB/s, active+drain 18.813 MiB/s;
- fresh mount, page cache dropped, metadata 8 MiB/auto, zero persistent payload,
  decoded and window caches, no data prefetch, 16 workers, direct I/O, TTL 0;
- cleanup exit 0; no mounts/containers from the run remain.

These are **unoptimized debug/local correctness diagnostics**, not release OSS
benchmarks, not a paired 250µs-vs-1ms result, and not a comparison against JuiceFS.
Existing JuiceFS reference rows remain historical observations with the documented
TTL and inline-byte caveats. README comparison tables are unchanged.

## Tests and retained implementation

- Range consumer previously stopped at exact requested length and could miss an
  additional stream chunk or a terminal stream error. New tests reproduce both
  failures; consuming to actual EOF rejects them without dropping length bounds.
  Short/chunk-crossing/interrupted responses remain fatal. Red: 4 passed/2 failed;
  green: 6 passed/0 failed.
- An explicit independent raw/zstd block codec checks stored/raw bounds, rejects
  concatenated or trailing frames, enforces content length and limits the zstd
  history window. Red: 1 passed/2 failed; green: 3 passed/0 failed.
- Explicit GM07 encodes independent 32-entry front-coded runs with a canonical
  offset/length restart directory; GM05/GM06 acceptance is unchanged. It rejects
  malformed restart counts/ranges, trailing bytes and >256 embedded extents.
  Red: 2 failed; green: 2 passed. These APIs now feed the GC05 producer described
  below. Legacy writers and previously published 004 objects remain unchanged;
  005 FUSE/workspace integration is still pending.
- Mount-scoped `.stats` extension exports existing packed metadata/runtime/cache/
  size-class snapshots without issuing remote requests or duplicating caches.
  Declared range lengths are labeled `requested_bytes_total`, not actual received
  bytes. Header/manifest/inline/failed-body counters and complete request-graph
  instrumentation remain pending. A generic extension isolation regression passes.
  Actual updated-binary FUSE export was also checked on 100-file inline-only and
  1,000-file inline+frame corpora. See the evidence below.
- Local runner Python syntax, multi-epoch timing and failure validation: 3 tests
  pass; existing shared scanner: 4 pass; actual mount TTL forwarding passes.

## Actual `.stats` FUSE export

- `docker/compose-xfstests/artifacts/packed-local-20261002T061516Z-1170464/`:
  100 files / 10,240,000 payload bytes / errors=0, 253 metric lines, packed logical
  bytes exactly equal scanner payload bytes. `data_range_gets=0` is expected:
  each leaf has only one 100 KiB inline file. The first verification script
  incorrectly required a positive data-range count; the filesystem run itself
  and cleanup passed. This is not a frame-path test.
- `docker/compose-xfstests/artifacts/packed-local-20261002T061733Z-1171472/`:
  1,000 files / 102,400,000 payload bytes / errors=0. Ten files per leaf exercise
  both inline and ordinary frames. The verification requires positive data/group
  GET counters, ordinary FUSE counters and exact packed-logical/scanner-payload
  agreement, with cleanup exit 0. Daemon before/after RSS/PSS and peak RSS are
  recorded separately from scanner RSS.

## Wire-005 authentication primitives (not full publication)

`packed_v3/wire005.rs` and `wire005/frame_directory.rs` now implement:

- independent 005 object magics, fixed 4 KiB zero-padded header with CRC, bounded
  raw body and 64 B footer (magic/length + 32 B body digest + 16 B header digest);
- `V3ObjectRef` pins kind/key/length/full SHA-256 and rejects a substituted object
  even if its own internal hashes have been recomputed;
- bounded remote metadata-page reads, invalid key rejection before transport,
  exact page count/record budget, contiguous descriptor ordinals and disjoint
  in-object ranges, profile/size-class/codec validation;
- independently authenticated frame-page container identity and strict exact
  payload range reads with stored digest verification before raw/zstd decoding.
  Stored+raw allocation and decoder history window are bounded. This is not yet
  the mount-wide retained-byte/permit lifecycle required by the full spec.

Envelope tests first fail (3 failures) and then pass. The initial envelope/page/
directory/frame chain has 7 focused passing tests in `wire005-frame-tests-final.log`.
The authenticated manifest ref supplied to these APIs **must originate from a
trusted control record/operator**, not be reconstructed from untrusted response
bytes. PM07 and its authenticated index chain are now implemented below. Actual
catalog/FUSE dispatch, full cold/large/reverse-index semantics and workspace-head
publication remain pending. New 005 APIs do not change the legacy 004 demand-read
security boundary.

## PM07 / GC05 / streaming producer stage

Implemented in `packed_v3/wire005/{manifest,index,index_builder,container,inode,spool,producer}.rs`:

- PM07 is a bounded 64 KiB manifest body with seven independently authenticated
  pageable roots. It does not embed every container/page ref. Read generation
  binds the full manifest content digest, not the snapshot id.
- IP05 leaf/branch pages cap body bytes, counts, key/value lengths and height.
  Demand lookup follows only the selected child and checks parent/child height
  and exact fences; zero-byte cache skips retention. Nonzero cache uses actual
  decoded structure sizes as logical weights and rejects invalid refs even on hits.
- A streaming index builder keeps one bounded page per level. Pages are CAS
  objects; uploads are read back and authenticated before their refs are used.
  Invalid ordering, upload failure or cancellation cannot yield a successful
  incomplete root.
- GC05 consumes the existing deterministic group packer output, preserves raw
  placements, emits GM07 plus independent raw/zstd blocks, and supplies authenticated
  descriptor pages. Group refs and IL05 inode values check names, ordinal, POSIX
  hot attributes and placement consistency. Library inode reads use the existing
  unified executor with the true authenticated descriptor fields.
- Producer traversal may arrive unordered. A private, exclusively created SQLite
  spool sorts records on disk with a 2 MiB cache, `temp_store=FILE` and 128-row
  paging. Cross-connection tests prove it is not a disguised in-memory database.
  An exclusively created private directory is guarded before the first await,
  so cancelling connection/schema initialization also removes only this run's
  database/sidecars. Database permissions are 0600 on Unix. Producer objects are
  verified through bounded streams after create-only PUT; descriptor/index/
  manifest objects follow them. Returning the final manifest ref
  is **not** an atomic workspace-head publication.
- Failure/cancellation poison the producer; later `add_container`/`finish` reject
  it. The spool owns and removes only its generated database/sidecars. Fault tests
  cover corrupted upload, failed CAS upload and cancellation.

Evidence: `docker/compose-xfstests/artifacts/packed-v3-completion-20261002-manifest/`.
PM07 TDD: 1 passed/2 failed before implementation, then 3 passed. Index TDD:
2 failed then 2 passed. Streaming builder TDD: 2 failed then 2 passed. The first
GC05 test attempt had a packer-argument compilation error; it is not behavioral
red evidence. After correcting the arguments, its raw/zstd round-trip passes.
The wire005 focused suite has **22 passing tests** (`cancellation-final-tests.log`),
including published inline/partial/sparse-boundary/10 MiB cross-frame reads and
failure isolation. The sparse corpus constructs placements explicitly: **real
SEEK_DATA/SEEK_HOLE inventory is not implemented yet**.

The counting-backend manifest→indexes→descriptor→payload test observes exactly
five demand requests and exactly one required payload range, rejecting full GET.
Too-small allocation limits fail before a payload request; tampered descriptors
with recomputed internal hashes are rejected by the manifest-bound ref.

Remaining producer boundaries: duplicate inode/hardlink handling, cold attributes,
external large-file placement, root-source POSIX metadata and source revalidation
are not complete. Actual 005 FUSE validation is now recorded below; no full
workspace lifecycle or matched performance campaign is claimed. The fixture
still defaults to 004; 005 generation requires the explicit wire-version option.

## Actual 005 FUSE and transport-observation stage

- Added authenticated index scan cursors and bounded directory pagination; the
  scan regression fails before implementation and passes after it. The directory
  adapter uses paged raw entries, not a namespace-sized Vec.
- Existing `PackedV3ReadonlyMeta`/`PackedV3BlockStore` now dispatch a pinned 005
  context without changing 004 decoding. **The existing slice facade remains
  transitional:** library data execution uses the unified executor, but removing
  synthetic slices from the VFS provider boundary is still a separate pending item.
- CLI probes the header, requires the caller-selected SHA-256 CAS key for 005,
  and verifies a bounded manifest range. It does not manufacture a trust anchor
  by hashing the response. The 005 entrypoint requires zero persistent payload
  budgets/data prefetch; the new fixture is explicitly selected with
  `--wire-version 5` while 004 stays the default.
- A readonly observing backend forbids whole-object GET and counts runtime
  backend range calls, declared and actually consumed response-body bytes, and
  transport failures. Logical bytes increment only after successful read delivery.
  The failed-stream regression records 4 requested/2 received bytes and zero
  logical success bytes. Counters are exported through actual `.stats`.
- Counters intentionally say `runtime_backend`: initial manifest/probe requests
  and SDK-internal retries are not included, and metadata/payload/inline traffic
  is not yet separately classified. These fields are not a claim of complete
  request-graph instrumentation or raw-frame overscan accounting.

### Mounted correctness artifacts

All rows use fresh mounts/drop-caches, TTL 0/direct-I/O/keep-cache 0, 8 MiB
index-cache budget, metadata prefetch off, zero payload/window/decoded caches,
no data prefetch, 16 workers and one shuffled full-read epoch. New containers use
zstd; the deterministic fixture is compressible. These debug/local measurements
are **not** a matched raw-codec baseline comparison or a JuiceFS win.

| Artifact | Files | Validated bytes | Checksum | Errors |
| --- | ---: | ---: | ---: | ---: |
| `packed-local-20261002T132444Z-1356288` | 100 | 10,240,000 | 5,050 | 0 |
| `packed-local-20261002T133246Z-1357717` | 1,000 | 102,400,000 | 124,948 | 0 |
| `packed-local-20261002T133706Z-1359072` | 10,000 | 1,024,000,000 | 1,273,096 | 0 |

Paths are under `docker/compose-xfstests/artifacts/`. All cleanup exits are 0.
The 1k row records 12,357 runtime backend ranges / 5,653,180 consumed bytes;
the 10k row records 120,067 ranges / 87,185,618 consumed bytes, requested bytes
match received bytes, backend failures=0 and packed logical bytes match scanner
payload bytes. Bytes smaller than logical bytes reflect compression, **not**
zero data transfers or hidden decoded-cache hits.

10k scanner active is 31.584 s; runner active 31.678 s, drain 0.124 s, total
32.022 s. Diagnostic active / active+drain bandwidth is 30.828 / 30.708 MiB/s.
Daemon RSS/PSS before: 84,128 / 82,038 KiB; after: 124,144 / 121,945 KiB;
reported daemon peak RSS: 124,144 KiB. Scanner RSS is separately recorded in its
summary. Codec/fairness/request-classification gaps preclude performance acceptance.

The legacy comparison runner remains compatible with an old 004 fixture binary;
only the 005 arm adds the wire-version argument. Source hashes now include
untracked `src/` files (excluding `.claude/`) as well as the actual binary hashes.
Earlier artifacts without this extra provenance remain diagnosed as such.

An additional real boundary regression found that zstd expansion of an
incompressible 8 MiB sequential frame could exceed the strict stored-range cap.
The writer now records raw codec for a frame when compression does not shrink it;
the failed regression then passes. Existing published 004 interpretation is unchanged.
The 10k mount above predates this last boundary fix, which affects the random-data
8 MiB case rather than this compressible 100 KiB corpus. Its full gate completed
under `packed-v3-completion-20261002-mounted-gate/`: packed 99, overlay 256,
workspace lib 1,015/bin 1,099 passed; all minimum and extra runtime checks passed.
The direct-provider and cold/hardlink follow-ups have independent newer gates.

## Current-iteration gate and next steps

The initial stage's full AGENTS gate completed in
`docker/compose-xfstests/artifacts/packed-v3-completion-20261002-gate/`:
fmt, required script/report tests, check/build, both runtime checks with/without
workspace-overlay, workspace lib/bins tests, packed tests and both clippy checks
all passed. Workspace lib: 1,014 passed/225 ignored; brewfs bin: 1,098 passed/225
ignored; packed suite: 74 passed; workspace-overlay suite: 231 passed/2 ignored.
Clippy retains pre-existing warnings; this is not a GitHub all-feature/operator/
`-D warnings` claim.

The initial 005-envelope gate also completed in
`docker/compose-xfstests/artifacts/packed-v3-completion-20261002-wire005-gate/`:
all minimum and extra overlay checks passed. Workspace lib: 1,015 passed/225
ignored; brewfs bin: 1,099 passed/225 ignored; packed: 81 passed; overlay:
238 passed/2 ignored. This gate predates PM07/GC05/producer additions.

The PM07/GC05/producer gate completed under
`docker/compose-xfstests/artifacts/packed-v3-completion-20261002-producer-gate/`:
all required and extra overlay steps passed. Workspace lib: 1,015 passed/225
ignored; brewfs bin: 1,099 passed/225 ignored; packed: 95 passed; overlay:
252 passed/2 ignored. This gate predates the final private-directory cancellation
cleanup regression.

The final same-iteration gate completed successfully under
`docker/compose-xfstests/artifacts/packed-v3-completion-20261002-final-gate/`:

- fmt, required syntax/report tests, TTL/scanner/local-runner tests: passed;
- workspace check/build: passed;
- tokio/io-uring runtime checks with/without workspace-overlay: passed;
- workspace lib: 1,015 passed/225 ignored; brewfs bin: 1,099 passed/225 ignored;
- packed feature suite: 96 passed/0 failed; overlay: 253 passed/2 ignored;
- default and overlay clippy: exit 0 with existing warnings; diff check passed.

This is the repository minimum gate plus focused/overlay checks, not an
all-feature/operator/`-D warnings` GitHub CI claim. No cloud resources or new
performance-win claim were created. No commit/push has been made in this
completion iteration yet; the full task remains open.

At this historical checkpoint, source inventory/large/sparse/cold/hardlink,
full memory/traffic accounting, direct VFS provider and workspace generation-fenced
lower/publish/recovery/GC remained pending. The linked follow-up supersedes the
cold/hardlink/provider status; inventory/large/sparse/workspace remain incomplete. Only after these gates can
the planned same-executor ablations and bounded release/cloud measurements be
accepted. A 250µs performance verdict is still pending.
