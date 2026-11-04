# Explicit tiered and range-heavy RustFS object hints

`tiered` is an explicit, deterministic policy for **new** S3 objects. It does not change BrewFS chunking, encode data differently, learn online, rewrite historical objects or infer access patterns. Product default remains unset. The existing `auto` rule and fixed 64/256/1024/4096 KiB hints remain available. Experiments opting into tiered must use new experiment IDs.

`range-heavy` is a separate explicit profile for sparse Range workloads. Its
policy version is `range-heavy-v2`. It omits the hint for finalized N ≤ the
inline bound (default 262144 bytes), requests **65536 bytes up to 8388608
bytes** and **262144 bytes above 8388608 bytes**. With the qualified
unversioned default 2+2 geometry, 128 KiB objects stay inline and external
objects use B/q = 64/16 KiB or 256/64 KiB. This is a client length policy:
backend versioning, custom inline budgets and MPU can change which objects are
external, as described below. It does not discover inline eligibility or
automatically detect the read shape.

The 8 MiB boundary comes from the first-round single-disk mechanism data:
fixed 64 KiB cost 4.2%/4.5% hot/cold full-GET throughput at 8 MiB but
6.6%/7.1% at 32 MiB, while fixed 256 KiB cost at most 2.2% at 8 and 32 MiB
and still read 4 KiB Ranges 1.8–3.7× faster than auto at 8/32 MiB. v1 (64 KiB for every
external object) exceeded a 5% full-GET budget at 32 MiB. The boundary is a
candidate that needs re-measurement, not an acceptance.

`range-heavy:INLINE_BYTES` raises the no-hint bound for deployments whose
RustFS inline budget is larger than the default (for example an explicit
`RUSTFS_STORAGE_CLASS_INLINE_BLOCK`). It accepts decimal 262144–4194304; RustFS
never inlines an object larger than its EC block, so a larger bound would only
withhold the hint from external objects. A lower bound is rejected because it
would make default unversioned 2+2 objects of 64–256 KiB external.

Use the same `--s3-rustfs-ec-block-size-hint range-heavy` CLI argument or
`data.s3.rustfs_ec_block_size_hint: range-heavy` YAML field. The feature gate,
S3 requirement, CLI-over-YAML precedence and explicit `off` override are shared
with the other profiles. Unknown profile names fail at startup; enabling a
profile without the feature reports `feature not compiled:
rustfs-ec-block-size-hint`. The independent Pack uploader uses the same parser
and S3 constructor validation.

`range-heavy` is the only profile that changes the read side by default. With
`read_fetch_policy` unset, an enabled hint makes the backend default fetch
whole objects, so small EC blocks would only add full-GET cost. When neither
the CLI nor YAML sets `read_fetch_policy`, a mount using `range-heavy`
therefore reads with `sparse`. An explicit `whole-object` or `backend-default`
is kept and logged as a warning at startup. Sparse keeps whole-block
background prefetch for reads that continue a sequential stream or are at least
128 KiB (kernel readahead), and for misses on a slice that is being read densely
(at least ten page misses per second).

**Default remains auto for configurations already choosing auto; the absent
option remains unset.** Neither tiered nor range-heavy changes defaults or the
selection rules of auto, tiered, custom tables or numeric fixed hints. Smaller
B/q can improve sparse Range while reducing full-GET throughput; this profile
is an explicit workload choice, not a general performance recommendation.

## Policy version and input

`tiered-v1` selects from the final upload length N **after** zero padding, compression and the format header. A file length, preallocated SSD extent, FUSE request, slice length or MPU part length is not N. This table was specified before implementation; all thresholds are **candidates without performance acceptance**.

| Inclusive final object interval (bytes) | Hint B (bytes) | Rationale |
|---|---:|---|
| 0–262144 | omitted | Preserve default small-object admission |
| 262145–1048576 | 65536 | Candidate smaller external objects / sparse Range requests |
| 1048577–4194304 | 262144 | Candidate intermediate layout |
| 4194305–16777216 | 1048576 | Keep the common 4 MiB + 4-byte object at 1 MiB |
| 16777217 and above | 4194304 | Candidate large-object layout, requiring separate validation |

The historical [auto comparison](rustfs-auto-object-hint.md) found a roughly 10% cold whole-file regression for a 4 MiB EC layout in one condition. This table preserves the old 1 MiB choice for the usual uncompressed 4 MiB BrewFS block; it does not claim that any other interval improves throughput. Full GET and sparse Range have different costs and must be measured separately. Larger B alone is not evidence of improvement.

## CLI, YAML and validation

Compile `rustfs-ec-block-size-hint` and use `--s3-rustfs-ec-block-size-hint tiered` for mount or object-put-bench. The independent `packed_v3_snapshot_fixture` uploader accepts the same argument when targeting S3. Its default is unset, so C2 producers must pass the value explicitly. Pack is only for the read-only ML small-file cohort.

```yaml
data:
  backend: s3
  s3:
    rustfs_ec_block_size_hint: tiered
cache:
  read_fetch_policy: sparse
```

An explicit table can replace `tiered` in the same CLI or YAML field:

```text
tiered:262144=off,1048576=65536,4194304=262144,16777216=1048576,*=4194304
```

Exactly five rows cover all possible lengths. The four finite, inclusive upper bounds must increase strictly and leave a nonempty terminal interval; the final bound must be `*`. The first row must be `off` and cover at least 262144 bytes. Each later B must be one of 65536, 262144, 1048576, 4194304. Later B values may repeat: a sparse workload can explicitly choose 65536 in all four external intervals. Malformed tables, overflow and unsupported sizes fail at configuration parsing. Private validated table fields protect programmatic callers. Selection never uses mutable counters or request order.

`off` sends no hint and overrides an enabled YAML value when supplied on the CLI. Enabled policies require S3 and the Cargo feature; unavailable features fail before creating the AWS client. `off` works without the feature. CLI takes precedence over YAML. An absent option retains the old default. `auto` still omits hints through 1048576 bytes and requests 1048576 above that cutoff.

## Actual inline and bitrot geometry

Reviewed RustFS branch: `feat/dynamic-block-size-rebased`, source `ff001942c72f85b553887ea72b0020d9fd352ba9`. SHA records this review, not an instruction to build a historical version. Recheck the branch at the start of a new qualification batch.

The ordinary PUT writer computes `erasure.shard_file_size(N)`, compares it with the storage-class inline budget, and also requires N≤B. With D data shards, the default per-shard budget is `min(ceil(262144/D), 131072)`; versioning divides this budget by eight. Under default 2+2, no server-side transforms, and the unhinted 1 MiB layout, unversioned N≤262144 is inline; versioned N≤32768 is inline. At boundary+1 the object is external. Thus even unhinted objects below 1 MiB can be external. The bitrot checksum bytes are not included in inline admission. MPU stores external parts and does not take ordinary PUT inline admission.

Inline writes retain historical full-shard framing. For external, unprotected objects with both block-hint and read-quantum rollout gates enabled, the current D=2 geometry is:

| B | Shard size | q | Persisted q marker |
|---:|---:|---:|---|
| 65536 | 32768 | 16384 | 16384 |
| 262144 | 131072 | 65536 | 65536 |
| 1048576 | 524288 | 524288 | absent (legacy full-shard framing) |
| 4194304 | 2097152 | 1048576 | 1048576 |

BrewFS does not discover or configure D, versioning, object protection or storage-class budgets. Outside the qualified D=2 defaults it sends the same deterministic hints, with **no assertion** about inline or q. RustFS may fall back to full-shard framing for unsupported q geometry. Explicit larger inline budgets may make an object eligible under the default B but ineligible under a requested smaller B; qualify custom tables and backend settings together. The 256 KiB no-hint floor protects the reviewed defaults, not every deployment configuration.

Actual readers use each object's persisted B and q. Missing q means full-shard framing, not zero. Rollout gates, an echoed user hint, an `applied` response and a changed persisted layout are different facts. Verify all shard metadata and check the response status/reason, including disabled and fleet-unconfirmed controls. Never infer actual B solely from the request.

## Upload and read scope

The existing S3 selection helper serves ordinary PUT, vectored PUT, create-only PUT and both MPU implementations. The additional durable create-only entry point now uses the same helper; previously it omitted configured hints. Its conditional-write and durability behavior is unchanged, and it now honors auto/fixed/tiered consistently. MPU selects at CreateMultipartUpload from the complete finalized body; parts do not carry the hint. Direct PUT retries reuse the same selected value. Failed MPU retries/recovery select from the same final bytes under the same immutable configuration. There is no persisted client policy in the writeback journal: changing configuration between recovery sessions can affect objects that have not yet been uploaded, so freeze the policy for repeatable recovery comparisons. Already uploaded objects keep their layout.

Flat-v1 data objects pass through ObjectBlockStore's final encoder. Workspace-native-v2 has its own native encoder: `BackendObjectRepository` sends its completed immutable object through S3 create-only and verifies exact readback. Both reach the same S3 hint selector with their actual final bytes; they do not necessarily share a wire format. The Pack fixture's independent S3 constructor now receives its explicit option; external Pack producers are not implicitly configured. Restoring sealed SSD data routes through the existing uploader and final encoder. Forks, read-only remounts and ordinary reads issue no hint-triggered rewriting.

#362 added independent `read_fetch_policy`: explicit `whole-object` or `sparse` controls GET/Range and prefetch regardless of hint value. It preserves the existing cache admission policy. Auto, tiered, range-heavy and all fixed hints retain the same historical LRU/whole-object backend preference; unset/off retains TinyLFU. Freeze explicit fetch policy, all prefetch settings, cache budgets, compression and concurrency for mechanism comparisons. Off versus enabled product combinations include this cache difference and need separate reporting; direct S3 controls avoid BrewFS cache confounding. Selecting range-heavy without an explicit `read_fetch_policy` selects `sparse` for the mount (see above); it does not change prefetch settings or cache admission.

## Evidence and acceptance

Unit/HTTP tests cover boundary ±1, empty objects, trailing small objects, all six creation routes (including durable create-only), PUT retry bytes, MPU reassembly, real framed/compressed bytes and recovered sealed data. Feature-disabled tests exercise CLI, YAML and SDK constructors. The first two tiered tests were run before implementation and failed because `tiered` was not accepted.

The debug selection event records the complete effective table, policy version, final object bytes, selected hint and reason. Object identities may appear in evidence files or existing tracing spans, but must never be metric labels. Aggregate evidence using bounded policy/version, selected B, upload route and normalized applied/ignored reason; retain per-object request, HEAD and every shard's layout separately.

Qualification must additionally run real-service upload/GET/Range, backend restart, FUSE remount, old-object reads and fork/read-only layout immutability. A single disk with four directories is functional evidence only. Local workspace tests, build, runtime feature checks, Clippy and formatting are mandatory before accepting code. Compose mixed read/write and drain gates are mandatory before accepting performance claims. This implementation does not update accepted performance tables or claim performance acceptance.

The opt-in service tests are separate supervised steps. Set `BREWFS_S3_ENDPOINT`, `BREWFS_S3_BUCKET` (an isolated, existing bucket), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `BREWFS_TIERED_EVIDENCE` (an existing empty output directory). With all four RustFS rollout gates enabled, run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --lib --features rustfs-ec-block-size-hint tiered_live_upload_evidence -- --ignored --nocapture
```

Inspect every shard and retain metadata before proceeding. Restart the backend explicitly with the same data and binary, confirm readiness, then run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --lib --features rustfs-ec-block-size-hint tiered_live_readback_evidence -- --ignored --nocapture
```

The second step only performs GET/Range with the client policy set to off. Compare persisted layout snapshots before and after. The tests retain uploaded objects and write evidence with exclusive creation; use a fresh bucket/output directory for versioned qualification. They do not manage services, mounts, credentials or resource deletion. FUSE recovery/remount and fork checks are additional steps, not implied by direct S3 readback.

## Recorded functional qualification

On 2026-11-01, the reviewed RustFS branch above was built locally and run on one disk with four directories, EC 2+2, with all four rollout gates enabled. This is **functional evidence only**, using unoptimized binaries. In each of an unversioned and a versioned bucket, 74 tiered objects plus six old-policy controls passed full GET/Range before and after backend restart. Each tiered cohort actually selected omitted/64/256/1024/4096 KiB in counts 28/12/12/16/6, with 46 applied and zero ignored hints. All four shard metadata and part-hash snapshots remained identical. Separate backend controls observed disabled, fleet-unconfirmed, unsupported-size and unsupported-write-mode; legacy full-shard objects remained readable after enabling finer q framing.

Actual FUSE and independent-producer samples demonstrate why the finalized length matters:

| Upload path / sample | Final N | Requested B |
|---|---:|---:|
| Flat, 262140 logical bytes + header | 262144 | omitted |
| Flat, 262141 logical bytes + header | 262145 | 65536 |
| Flat, 1048573 logical bytes + header | 1048577 | 262144 |
| Flat, full 4 MiB block + header | 4194308 | 1048576 |
| Flat, compressible 4 MiB with LZ4 | 16468 | omitted |
| Native-v2, padded small logical file | 4194308 | 1048576 |
| Read-only ML Pack container | 13482601 | 1048576 |
| Read-only ML Pack containers | 16827624 / 16926033 | 4194304 |

Flat and native files passed full/pread checks, explicit kernel read-only remount, EROFS checks and unchanged layout snapshots. A flat commit-before-upload mount acknowledged fsync while an owned proxy rejected 15 PUT attempts with 503. After killing the client, one sealed SSD record was recovered by a new mount; final N and hint stayed 1048577/262144, content matched, and the dirty record drained. The same configuration was retained across recovery sessions.

The Pack fixture uploaded 320 ML small files; its standalone read-only mount verified all files and rejected writes. Current wire-005 admission requires zero payload RAM/SSD cache and disabled prefetch. Creating a workspace fork left all existing Pack layouts unchanged, but **Pack fork-mounted content readback did not pass**: `workspace fork` creates a native BaseRevision overlay and does not inherit the separate workspace/head-specific PackedLowerBindingRecord, so that mount was empty. This EC change does not qualify that publication/fork path. No TiKV, historical-server-binary, independent-disk or performance acceptance is inferred from these results; no C1–C4 campaign was run.

## Recorded range-heavy mechanism probe (2026-11-02)

Fresh auto/tiered/range-heavy/fixed64k objects were compared on the same single
disk and retained RustFS dev binary. Three paired rounds covered 8/32 MiB
objects with cold/hot 4 KiB Range requests. Range-heavy/fixed64k ratios of
median throughput were 0.967/1.071 for 8 MiB cold/hot and 1.043/1.024 for
32 MiB cold/hot, meeting the predefined 0.9 descriptive threshold. This does
**not** establish cold-read equivalence: the 32 MiB cold first-round ratio was
0.508, and its paired geometric mean was 0.874 with 95% CI [0.272, 2.807].
All rounds and outliers are retained. One full-GET/hot round per size observed
range-heavy costs of 3.8%/6.0% relative to auto; no full-GET CI is claimed.

All 3096 shard metadata CRC checks and 6144 main-matrix readback checks passed,
with another 12 boundary readbacks confirming default inline admission through
256 KiB and 64/16 KiB external B/q. Default and feature workspace tests, builds,
checks, strict Clippy and runtime checks passed before measurements. This is
direct S3 mechanism evidence, without FUSE, compose, C1-C4, TiKV, independent
disks or Pack fork qualification. Reports and frozen raw evidence are retained
at `/mnt/brewfs-data/REPORT-402-R2.md` and `/mnt/brewfs-data/402-r2/`.

## Recorded FUSE acceptance of range-heavy-v2 (2026-11-02)

One bounded Aliyun run (`ecs.u1-c1m2.2xlarge`, Ubuntu 26.04, RustFS EC 2+2 on
four directories of one PL1 ESSD, the reviewed RustFS branch at `2809f5a`)
compared a real FUSE mount with the hint and read policy unset against
`range-heavy` with the policy unset (implied sparse), on a 3.75 GiB sparse set
and a 256 MiB hot set of 4 MiB files, read cache 1 GiB, compression none.
Eight ABBA cycles, page cache dropped before every cold cell. Ratios are
range-heavy/unset geometric means:

| Cell | Ratio |
|---|---:|
| 4 KiB random read, 1 job, cold sparse set | 3.94 |
| 4 KiB random read, 8 jobs, cold sparse set | 23.85 |
| 64 KiB random read, 4 jobs, cold sparse set | 18.12 |
| 4 KiB random read, 8 jobs, warm | 1.00 |
| 4 KiB random read, 8 jobs, hot set cold start | 0.84 |
| 1 MiB sequential read, cold | 0.55 |
| cold read of a freshly written 512 MiB file | 0.58 |
| 4 KiB randrw 70/30, 4 jobs: read / write IOPS | 0.94 / 0.94 |
| 4 KiB randrw: read p50 | 13.7 (worse) |
| 1 MiB sequential write with fsync | 0.96 |

**range-heavy-v2 failed the per-cell 5% regression gate.** Sequential reads
issued one range GET per readahead window (5.7x the GETs for the same bytes),
because sparse mode also suppressed whole-block prefetch for sequential
streams; that is addressed by the sequential-stream rule above and needs
re-measurement. The hot-set and randrw cells, where most pages of every block
are eventually read, favored whole-object fetching; the slice-density rule
above targets them and also needs re-measurement. The
layout-isolation arm (1 MiB hint with explicit sparse) was truncated by the
deadline and not measured. All data, verification reads and layout checks
passed.
