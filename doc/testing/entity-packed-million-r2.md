# Million-file packed workspace campaign: two complete pairs and a recovered cell

On 2026-11-01, BrewFS packed + TiKV upper created and used four writable
workspaces in **110.200 s versus JuiceFS + TiKV's 4154.063 s** in round 1,
and **110.203 s versus 4257.212 s** in round 2. Avoiding repeated metadata
clones accounts for most of this lifecycle advantage. JuiceFS completed the
measured random-read, COW and full namespace-check stages faster.

All six operational cells and 24 workspaces eventually completed their byte
checks, strong drains and actual worker exits. The campaign's **full
observability and uninterrupted-six-cell acceptance failed**: maintenance
caused a 274.745 s object-host metric gap, reset the final client exporter
catalog, and interrupted the last JuiceFS cell. Rounds 1 and 2 supply two
uninterrupted operation pairs over the same base. JuiceFS round 3 is a
separately marked recovery observation, excluded from clean paired aggregates.

The measured BrewFS source is unchanged commit
`918deb90e046889d94eb8ff633822e275792ce28`, with observability enabled. This
report records an experiment, including its failures; it accepts no new runtime
optimization. The earlier [100k cohort and timed-out 1M attempt](entity-packed-fanout.md)
remain separate observations.

## Lifecycle and complete accounting

Each lifecycle covers all four sequential fresh workspaces: mount/bootstrap,
native clone where applicable, half-prewarm, random reads, COW with fsync and
byte checks, strong upload drain, ordinary teardown and the actual worker's
exit. The additional full correctness pass is outside that lifecycle clock
and appears separately below. Seconds are rounded to six decimals; the
[machine-readable results](entity-packed-million-r2/results.json) retain precision.

| Cell, in actual execution order | Four-workspace lifecycle s | Additional correctness s | Sum s | Timing eligibility |
| --- | ---: | ---: | ---: | --- |
| BrewFS r1 | 110.200256 | 2009.172427 | 2119.372683 | Complete operation pair 1 |
| JuiceFS r1 | 4154.062787 | 67.480192 | 4221.542979 | Complete operation pair 1 |
| JuiceFS r2 | 4257.212039 | 68.522605 | 4325.734644 | Complete operation pair 2 |
| BrewFS r2 | 110.203320 | 2006.609357 | 2116.812677 | Complete operation pair 2 |
| BrewFS r3 | 110.201538 | 2004.125042 | 2114.326580 | Complete standalone cell |
| JuiceFS r3 | 4090.323980 | 68.380121 | 4158.704101 | Interrupted; recovered original components |

The round 1 and 2 JuiceFS/BrewFS lifecycle ratios are 37.70 and 38.63.
Including each cell's correctness pass gives 1.99 and 2.04. These are ratios
of specified stages, not of the entire campaign.

| Accounting for the two complete operation pairs, seconds | BrewFS | JuiceFS |
| --- | ---: | ---: |
| One base onboarding, including teardown | 3082.653884 | 2940.457614 |
| Eight workspace lifecycles | 220.403575 | 8411.274826 |
| Onboarding + eight lifecycles | 3303.057459 | 11351.732440 |
| Additional correctness, both cells | 4015.781784 | 136.002797 |
| Onboarding + lifecycles + correctness | 7318.839244 | 11487.735237 |

Source generation including sync took **65.127816 s** once for both arms.
JuiceFS's native import itself took **2939.589795 s**; the 2940.457614 s row
also includes its drain, unmount and exit. Neither onboarding was replayed.
The combined original campaign ran from 15:29:36 to 23:08:00 UTC:
**27,503.500494 s**, including both arms, the two interruptions, repair,
waiting, checkpoints and correctness. Infrastructure preparation began earlier;
collection, archive transfer and resource release continued afterwards. These
shared costs have no independently measured per-product allocation.

## Creation gain and slower stages

JuiceFS used the real `clone -p` command, sharing payload objects while copying
metadata. Each clone took 1027.357–1037.577 s in round 1 and
1055.398–1060.619 s in round 2. BrewFS's fresh upper/lower binding and mount
bootstrap took 2.738–3.039 s and 2.756–3.073 s per workspace respectively.
The two operations provide writable workspaces through different metadata
mechanisms; the results measure those configured product workflows.

| Active stage summed over four workspaces, seconds | BF r1 | JF r1 | BF r2 | JF r2 |
| --- | ---: | ---: | ---: | ---: |
| Native clone | Included in fresh bootstrap | 4132.681690 | Included in fresh bootstrap | 4234.313497 |
| Half-prewarm | 9.451570 | 2.198846 | 9.417365 | 2.455775 |
| Random reads after prewarm | 65.958992 | 16.869159 | 65.416397 | 18.172635 |
| COW, fsync and byte checks | 21.334864 | 1.555997 | 21.650829 | 1.543532 |

BrewFS random-read P95 was 52.134–60.945 ms per workspace in pair 1 and
53.709–59.478 ms in pair 2; JuiceFS's was 9.971–13.899 ms and
11.478–15.094 ms. BrewFS's 100-file COW stage took 5.102–5.614 s and
5.298–5.525 s per workspace; JuiceFS's took 0.371–0.428 s and
0.380–0.393 s. The [24 workspace rows](entity-packed-million-r2/workspaces.csv)
also retain prewarm, P50/P95/P99, pwrite/fsync P95 and third-round values.

Drain-through-exit tails, already included in lifecycle time, were
0.307–0.487 s per BrewFS workspace in r1 and 0.397–0.438 s in r2.
JuiceFS's shared mount tails were 0.358707 s and 0.353102 s. BrewFS r3 tails
were 0.287–0.488 s; recovered JuiceFS r3's shared tail was 0.369089 s.
BrewFS used graceful SIGTERM with remote-ready/zero-debt confirmation and
self-unmount; JuiceFS drained staging debt and used ordinary `umount`.
All actual workers exited zero, with no live descendants or assisted FUSE abort.

Full namespace checks also cost substantially more for BrewFS: about 2004–2009 s
per cell versus JuiceFS's 67–69 s, even though JuiceFS checked its base and all
four clones. The full raw request log contains **3,368,427 HTTP 206 GETs and
860,376,725,649 response bytes** across all phases. It also contains 60,346
HTTP 200 GETs / 247,149,767 bytes and 1,009,870 successful PUTs. This substantial
traffic includes packed metadata traversal, correctness, onboarding and reads;
it is not a pure payload-read amplification ratio. The log establishes no
object bandwidth or request-cost advantage. First-five-cell counter differences
are retained; the interrupted sixth cell's missing starting offset stays null.

## Workload and configuration

- One deterministic synthetic base: 1,000,000 files × 4096 bytes in 1024
  directories, generated once. Each arm reuses its published/imported base and
  retains 12 writable workspaces. Rounds are repeated operations, not independent
  deployments. Arm order is BF/JF, JF/BF, BF/JF.
- Per workspace: the same 1024 selected files, 512 prewarm reads, 4096 balanced
  initially-warm/unwarmed O_DIRECT 4 KiB reads, then 100 full-file O_DIRECT COW
  writes with fsync and before/after byte verification; eight application workers.
  Initially warm describes trace membership, not a measured cache-hit claim.
- Both arms: default synchronous `upload_before_commit`, raw/no compression,
  4 MiB chunks/blocks, zero clean payload-cache budget, eight runtime/upload
  workers. Packed V3 uses inline off and the default 256 KiB random-frame cap.
  FUSE max_background is 256. No CPU profiler ran during timed cells.
- BrewFS retains its default immutable metadata caches. JuiceFS explicitly uses
  zero attr/entry/dir-entry/open caches. Equal payload-cache budgets do not make
  internal metadata caching identical; this is not both products' default setup.
- BrewFS creates four fresh mounts per cell. JuiceFS uses one shared mount with
  four directory clones. Workspaces run sequentially, not concurrently across
  clients. Experimental `workspace-entity-v1` admission is explicit; default
  and legacy paths are unchanged.
- Correctness exhaustively checks base names each cell and all four JuiceFS
  clone namespaces, samples base content, verifies every selected read/COW,
  and verifies committed BrewFS upper object sizes and SHA-256. Base payload
  bytes are sampled, not exhaustively scanned.

Three same-AZ Aliyun on-demand `ecs.g9i.2xlarge` nodes each supply 8 vCPU and
32 GiB: one client, one RustFS object server, one PD + single TiKV replica.
Traffic uses private addresses in one VPC. Each node has a 40 GiB PL1 system
disk and 200 GiB AutoPL data disk with 11,800 baseline + 18,200 provisioned
IOPS = 30,000 IOPS, burst disabled. The configured instance baselines are
6 Gbit/s network, 4 Gbit/s aggregate storage bandwidth and 60,000 aggregate
IOPS. Those specifications alone do not establish absence of a bottleneck.
Named-file disk preflight, per-node host/process/cgroup/storage/FUSE samples,
PD/TiKV and client exporter records are preserved in the private archives.

JuiceFS reports `1.3.1+2025-12-02.e0032b2` (source
`e0032b2ae5e9603403ca955eed7d05426f6f2f8c`). PD/TiKV report **8.5.3**, despite
their `v8.5.0` image aliases. Exact image IDs, binaries and the unchanged
26-file harness are frozen in the reproduction package. One TiKV replica is
the measured topology. This campaign measures a private RustFS service with
observability enabled, separate from the earlier single-host campaign.

## Failures, maintenance and recovery

| UTC event | Effect and retained handling |
| --- | --- |
| Before bootstrap | ECS NVMe exposes the exact disk serial without the API's `d-` class prefix. A storage-identity adapter was applied and refrozen on all nodes before first format/bootstrap; it accepts only complete exact serial representations. The final deployed 26-file manifest and original-transfer hashes are both published. |
| Preparation | Packed producer inventory SQLite used system-disk `/tmp` because TMPDIR was unset; about 72 GB of system-disk writes were observed. Source/backend/cache data used the separate disk. This placement can affect onboarding; the live database was not moved and import was not rerun. |
| 15:53:54 object observer recovery | RustFS also occupied port 9001. nginx used private port 9002 with client-source-only redirect from the existing 9001 endpoint. The setup bucket/list observation gap preceded every payload PUT; all payload uploads passed the logger. RustFS and nginx stayed running afterwards. |
| 17:11:04 original checkpoint failure | Cross-filesystem rename of request logs raised EXDEV after both onboardings and before any workspace cell. A temporary bind workaround also failed and was removed. Three phase archives, 184,419,225 bytes / 430 members, were downloaded and verified. Processes/data and original failure records were retained. |
| 17:48:28 authorized continuation | A separate object-agent adapter replaced only collection rotation: same-directory rename, nginx reopen, wait for old inode closure, checked/fsynced cross-disk copy, source segment retained. Frozen sources, endpoints and runtime stayed unchanged. Counters/offsets continued; old manual snapshots remained outside the canonical request log. Two diagnostic health GETs remain. Six previously unstarted cells reused saved onboarding results. |
| 22:00:28 object-node maintenance | Unattended upgrade restarted the agent. Its old startup baseline failed against the normally rotated empty log. At 22:04:58 the same unchanged adapter resumed from last durable counters. Request logs/counters remained continuous; the actual archived host-sample gap is 274.744959295 s. No metrics were backfilled. RustFS/nginx were not restarted. |
| 22:02:08 metadata-node maintenance | The agent restarted and recovered itself; final host max gap is 5.001687893 s, TiKV exporter max gap 5.018265297 s and PD 5.012132720 s. Maintenance logs are retained. |
| 22:53:30 client maintenance | Upgrade restarted the controller. Its once-only guard rejected replay. Five full cells and the first three workspaces of JF r3 were durable; the existing final clone and shared mount survived independently. The client exporter catalog reset. |
| 23:03:28 one-time attachment | The recovery program checked the exact original clone/mount identity and frozen source hashes, waited for the existing clone, then performed only the last workspace's unstarted reads/COW and original drain/teardown/full verification. No import, clone or completed workspace was repeated. The dead writer's gzip is preserved as `.interrupted`. |
| 23:08:00 operations complete | Six cells / 24 workspaces, byte/namespace checks, remote-ready drains and real worker exits finished. Observers closed normally and three final archives were generated and verified. |
| Final strict validation | The unchanged verifier exited with `KeyError: 'brewfs-mount'` in the final client catalog. `juicefs-sync` was also absent from that catalog; historical raw records retain 1283 and 1174 successful scrapes respectively. Independent audit additionally confirms the object-host coverage violation and interrupted performance sample. The success receipt was never fabricated. |

JF r3 explicitly records `recovered_interrupted_cell=true` and
`uninterrupted_performance_sample_valid=false`. Its private original
`perf_counter` origin and object starting offset died with the controller.
The 4090.323980 s lifecycle uses its original durable stage start through
actual exit, including interruption/attachment waits; the final clone's join
timer also spans recovery. It cannot supply a third uninterrupted performance
pair. The original failures, maintenance journals, interrupted gzip, recovery
source and exact component attachment proof remain with the evidence.

## Evidence and resource closure

Three immutable local final archives total **985,322,378 bytes**. Independent
verification checked outer hashes and every member's size/hash, with no active
or deferred segments. A single trace hash spans all 24 workspaces:
`aa48f81e8154489b2c8eba9e4fea206d2ce0cc4178de29b5d193859d06e1a783`.
Raw records contain **98,304 checked random reads, 12,288 prewarms and 2,400
COW/fsync operations** in 24 files of each kind. Fifteen timed managed-mount
exit receipts cover 12 BrewFS mounts and three shared JuiceFS mounts; additional
correctness/import/command component receipts remain in the archives.

| Archive | Bytes | Manifested members | SHA-256 |
| --- | ---: | ---: | --- |
| client-final.tar.gz | 69,168,005 | 711 | `1f08b795a24376e5ce208146d88ae50fac2634560812d9549c8107eee2c4ce7e` |
| objects-final.tar.gz | 87,415,595 | 131 | `72864bdefc4a5cbf69da4b13ed22b954a5c05a8170cd90bc72727d36c4c71589` |
| metadata-final.tar.gz | 828,738,778 | 843 | `9768229a469a9fc7d23ca2be4162b09282b6bc06a1f406e654348212c06aeb65` |

Full archives remain private because they contain generated credentials,
deployment identifiers and service logs. The public
[JSON](entity-packed-million-r2/results.json), [cell CSV](entity-packed-million-r2/cells.csv),
[workspace CSV](entity-packed-million-r2/workspaces.csv), and
[source hashes](entity-packed-million-r2/source-digests.json) remove those
identifiers while retaining observations and failed gates. Integrity/correctness
verification establishes safe archival closure; strict acceptance remains false.
The source package supplies image-lock/provenance JSON from the original frozen
transfer because the final archive's file selection omits those two metadata
files; both hashes match its final 26-file source manifest.

Immediately after that audit, all three owned instances, their three retained
data disks and three system disks were explicitly deleted and queried absent.
The SG deletion query found another task's instance sharing it; that SG and
foreign instance were preserved along with shared VPC/vSwitch/keypair. All run
managers exited and the finite local power lease was cleared. No report/PR/CI
wait prolonged this run's paid resources.

Authenticated configuration quotes total **CNY 13.23474/hour** for three nodes,
including fixed provisioned IOPS. CNY 15/hour was a planning reserve, not a cap.
From creation to actual VM deletion and explicit retained-disk deletion receipts,
the estimate is **CNY 110.706793** (client 36.885619; objects 36.902969;
metadata 36.918205). This includes preparation, failures, repairs, waiting,
correctness, observation and archival collection. Public traffic and billing
adjustments are additional. Billing was queried for both November 1 and 2 in
the provider's daily billing calendar. At about 23:40 UTC, visible partial
charges totaled **CNY 91.739243**, covering roughly the first 6–7 hours of
individual billing items, short of the full 8.37-hour lifetime. A complete
reconciled bill is unavailable at closure. The
foreign instance and shared resources are outside this run's cost estimate.

## Reproduction and validation

The [source and offline audit instructions](../../tools/perf/entity-packed-million-r2/README.md)
preserve the measured contract, deterministic workload, frozen harness, original
strict verifier and isolated recovery sources. Historical EXDEV/restart defects
are retained with their source hashes. Replaying the archived evidence requires
no cloud instance and performs no workload or resource mutation.

Before cloud measurement, unchanged Rust source passed formatting, shell syntax
and three report-driver tests, workspace check/build, runtime feature checks,
workspace lib/bin tests and strict all-target Clippy. Real 1024-file and 4096-file
smokes exercised both clients, native import/clone, strong drain, ordinary exit,
raw samples and complete archive transfer. Seven observation tests and a real
cross-filesystem repair test passed. These smokes are validation records, not
extra million-file comparison samples. No build overlapped a timed cloud cell.
Publication validation repeated the complete local gate: 1302 tests passed,
264 ignored, zero failed; strict all-target Clippy passed. The public offline
audit checked the complete original archives, source identities and JSON/CSV
projection. Python compilation and the real cross-filesystem repair test passed.
