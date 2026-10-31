# Issue 383 phase 2: authenticated packed-read caches

Experiment date: 2026-10-30. The phase-1 report and its original scalar
SHA profile are preserved; see [phase 1](issue-383-packed-read.md). The full
local phase-2 report is `/work/REPORT-383-phase2.md`, raw artifacts are under
`/work/issue383-phase2/`, and CI/build logs are under
`/work/logs/issue383-phase2/`.

**Delivery: A metadata caches only. B failed a sequential guard and was
removed; C is deferred; D remains a design.** Focused B numbers below are
rejected experiment evidence and are not accepted performance results.

## Changes and integrity

A retains authenticated GroupMeta and frame-directory pages in bounded LRUs.
The key includes canonical authenticated object references, with the sealed
manifest digest in `ReadGeneration` and a mount-wide invalidation epoch.
GroupMeta capacity is `min(index_cache_bytes/16, 8 MiB, Metadata/4)`; directory
capacity is `min(index_cache_bytes/32, 4 MiB, Metadata/8)`. Both decoded owners
and entry bookkeeping retain V3 mount-budget permits. Cache hits still check
allocation bounds and container/profile/ordinal bindings.

Heartbeat authority loss, failed lease validation, view replacement,
stop-admission, and close advance the shared epoch. All retained caches reject
old epochs, and old in-flight fills cannot publish. Reclamation occurs on the
next access or shutdown; live consumers retain their charged owners.

The rejected B prototype retained authenticated raw frames in a bounded LRU,
including payload, observer coverage, immutable keys and owner overhead. Its
configured cap was 1 GiB, with Raw/Workspace/Control/Plans headroom for active
operations. Hits reused the same verified allocation and skipped GET and SHA;
every newly fetched response still authenticated through EOF. The prototype
and its configuration path have been removed. No frame cache is delivered.

C is deferred after that rejection. A digest-only remembered verdict cannot
authenticate newly fetched bytes; no verification bypass or digest-only trust
of a future response is substituted. The delivered payload path remains the
phase-1 ring implementation and still checks each response.

## Matched experiment

The environment remains 8 vCPU / 30 GiB RAM, Xeon Platinum 8269CY (AVX2,
without SHA-NI), compose TiKV 8.5.0 / RustFS 1.0.4, on loopback. The same
four 512 MiB files comprise 512 external 4 MiB frames and 128 external 16 MiB
chunks. Each arm uses a fresh upper workspace over one phase-2 manifest:
`issue383-lower/objects/0/ea2cce3b826e3718e82da87d77ca3f96f34d5889270896ab2bef3adde47478fa`.
The old disposable backend volumes had been removed, so this manifest was
regenerated. Source contents and fio seed remain unchanged. Its GroupMeta is
164 bytes rather than the earlier manifest's 155 bytes; the descriptor is
4,432 bytes and payloads are still 4 MiB.

Each W arm runs two 30-second fio warmups and three measured 30-second passes
on the same mount. These are timed warmups, not complete corpus sweeps. Fio is
4 KiB random read, direct=1, libaio, four jobs, depth 32, 512 MiB/job,
seed 20261029, as in the phase-1 command. Capture counts actual HTTP GETs;
run3 has a perf task-clock profile. No compiler runs during measurement.
Read-only W has no post-write drain claim.

The independent asm arm precedes the cache changes. The existing phase-1
implementation actually uses ring, which remains selected. Asm source and
dependency substitutions were restored before the delivered cache build.

| Arm | KiB/s median | p99 ms median | GET/read |
| --- | ---: | ---: | ---: |
| Independent sha2/asm | 138.38 | 4009.755 | 3.000 |
| Ring, no new caches | 183.23 | 3003.122 | 3.000 |
| A, default V3 budgets | 198.22 | 2936.013 | 1.000 |
| A, expanded V3 budgets | 199.65 | 2868.904 | 1.000 |
| **Rejected B**, same expanded budgets, 1 GiB frame cache | 325.98 | 1937.768 | 0.62869 |

A throughput is 196.37 / 198.24 / 198.22 KiB/s and p99 is
2969.567 / 2868.904 / 2936.013 ms. Expanded-budget A throughput is
197.77 / 204.11 / 199.65 KiB/s, with p99
2868.904 / 2902.458 / 2868.904 ms. Each measured A pass has exactly one GET
per read. A improves median throughput 8.18% over ring and reduces p99 2.23%.

B throughput is 324.69 / 326.42 / 325.98 KiB/s; p99 is
1904.214 / 1937.768 / 1971.323 ms; GET/read is
0.628689 / 0.626020 / 0.630977. Against matched A, B improves median
throughput **63.27%** and p99 **32.46%**. Against the ring control, A+B improves
throughput 77.91% and p99 35.47%. All final W passes have zero fio/HTTP
errors, zero new TCP connections, and clean teardown.

The matched A control sets Raw=2 GiB, Workspace=1 GiB, frame cache=0; B uses
the same pools and frame cache=1 GiB. The ordinary VFS RAM/SSD budgets remain
300 MiB/1 GiB, compression disabled and prefetch disabled.

B's measured hit rate is 37.13% / 37.40% / 36.90%. The lifetime ledger,
including warmups, records 4465 hits, 7671 misses, 7486 evictions and 185
resident frames (36.79% hit rate). Cache weight is 1068033305 bytes
(1018.56 MiB), including 740 MiB of payload and coverage/ownership overhead.
Peak mount RSS is 1111.69 MiB; the cache cap is not a total-process RSS limit.
The known stats-extension registration defect hides packed pool counters in
`.stats`, so occupancy uses the explicit cache shutdown ledger and pidstat.

The first B execution was interrupted after fio during shell tail parsing by
an in-place local harness edit. Its artifact is preserved as
`b-harness-interrupted-w` and excluded. The harness was then frozen and hashed,
and all five passes were rerun in a fresh workspace as the complete `b-w`.
The later sequential failure still rejects B. The Rust source and binary
were unchanged between its W runs. Repository artifact comparisons
are saved as `compare-ring-a.md`, `compare-a-a-wide.md` and
`compare-a-wide-b.md`; normalized layouts symlink unchanged source JSON/stats.
Effective wall bandwidth also improves. Its ISO timestamps have one-second
resolution; no read-only post-write drain measurement is synthesized.

## SHA experiment and limits

The reused scalar profile has 73.26% self CPU in SHA. Independent asm has
71.09% in `sha256_compress` (71.41% total SHA). Ring has 58.19% in its AVX
payload routine (58.52% total), about 11.78 ms SHA CPU/read. A has 64.44%
total, about 11.72 ms/read: metadata requests disappear while the same full
payload hash remains. Percentages alone do not measure saved SHA work.

Against matched A, B reduces SHA CPU/read from 11.82 to 7.35 ms (**37.82%**),
but total SHA self CPU is 64.94% to 65.01%. Misses still dominate CPU after
cache hits skip hashing. This full-working-set result does not demonstrate
elimination of the global SHA sampling fraction, and no such claim is made.
The optional hot-frame profile was canceled after B failed its guard. No
global SHA-elimination result is claimed or delivered.

A local-only audited patch to a scratch ring executable bypassed payload SHA.
It measured 472.46 / 466.29 / 475.39 KiB/s, p99
1199.571 / 1233.125 / 1199.571 ms, and 3.000 GET/read throughout. Median
throughput is 2.579 times ring-on; p99 falls 60.06%. The fixed-workload
completion interval falls `1 - 183.2266/472.4600 = 61.22%`. This is an
experimental end-to-end effect, not an additive CPU/network decomposition.

**Bypassing SHA is a security downgrade and cannot be delivered.** No bypass
source or flag is included. The scratch executable has mode `000`. The
24-byte binary patch audit is local-only. Caching authenticated bytes and
hardware-adapted SHA preserve integrity.

## Validation

The A+B overlay Cargo gate passed **2582 / 0 / 451** (passed/failed/ignored)
before measurements. After B removal, the final A-only overlay gate passes
**2578 / 0 / 451**; the workspace library/binary gate also passes. All local
checks, including strict workspace/all-target and overlay Clippy, were rerun
successfully after B withdrawal. Final CI completed at 17:38:50 CST, 73 seconds
past the four-hour boundary; performance experiments had already stopped.
Workspace library/binary tests, workspace check/build, both FUSE runtime
checks, shell/report checks, formatting, strict workspace/all-target and
overlay Clippy, and diff checks pass before cache measurements. The new
metadata regression test was first linked against the pre-change release
library and failed with 3 GETs instead of 1.

Delivered tests cover whole-cache generation/epoch invalidation, actual
backend-clock lease expiry, stale fills, failed authentication, metadata LRU
eviction, retained budget ownership and payload-only repeat GET counts.
The prototype's four new frame-cache tests were removed with B; pre-existing
coordinator assertions remain intact. ACL `permission_cache_ttl()` is unchanged at
`Duration::ZERO` for ReadWrite POSIX ACLs. No ignore annotation or removed
assertion is introduced. No broad README comparison table is updated from
this focused experiment.

## Broader guards and rejected B

Ring and A both pass 36/36 direct boundary/integrity reads, all sequential
passes and 3 x 128 metadata rounds (create, fsync, stat, rename, readdir,
readback, unlink). Ring sequential throughput is
44213.54 / 44151.56 / 43639.55 KiB/s; A is
52583.37 / 48657.90 / 46958.43 KiB/s. Median sequential throughput improves
10.21%, and median p99 improves 4.19%. Metadata rounds are
14.53 / 15.06 / 14.81 seconds versus A's 14.10 / 14.50 / 14.35 seconds.
Both read-guard mounts exit zero and fully unmount.

B passes its 36 integrity reads, then **fails sequential warmup with fio
error=5** on `r.1` and `r.2`, offset 273678336, length 1048576. Capture has
803 GETs and responses, zero new connections, no HTTP error statuses, and one
response without complete captured wire coverage. The mount exits zero and
fully unmounts. Root cause remains unresolved; this is not relabeled as a
harness race. `b-readguard-readguard/` retains the evidence.

The targeted B rollback and source are saved outside the PR as
`rejected-b-source.diff`, `rejected-b-frame-cache.rs` and `reject-b.patch`.
The delivered pipeline is restored to the parent implementation. Another B
attempt needs error/admission diagnostics and repeated sequential/mixed gates.

Both the ring and A final mixed-write guards fail their warmup with fio
error=5, consistent with the phase-1 packed write/sync limitation associated
with #382. Both mount processes exit 1 after write errors and fully unmount;
no hang occurs. Artifacts are `ring-final-mixedguard` and
`a-final-mixedguard`. No mixed throughput, post-write-drain or mixed-regression
conclusion is claimed. This limitation is explicit in the draft PR, and the
broad README performance table remains unchanged.

## Optional finer integrity: design only

A separate opt-in packed version could use 64 independently decodable 64 KiB
Merkle leaves per 4 MiB frame. A 4 KiB read would authenticate 64 KiB, reducing
hash amplification from 1024 to 16 (64 times less). Six sibling SHA-256 hashes
give a 192-byte proof. A leaf-hash table costs 2048 bytes/frame (0.049%); the
full tree costs about 4064 bytes/frame (0.097%), or about 1--2 MiB across the
512-frame corpus.

The authenticated descriptor must bind the root, position, length, leaf codec
and ranges. Hashing needs domain separation. Whole-frame compression must be
replaced with independent leaf compression before range reads can avoid
whole-frame decompression. A new format/version must reject old readers and
remain opt-in; wire-005 is not reinterpreted. Corrupt proof/leaf, truncated
range, boundary-spanning, cache-hit and downgrade tests, plus matched random,
sequential and mixed fio guards, belong to that separate PR. No partial
format implementation is included here.
