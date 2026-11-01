# Packed immutable-base fanout: completed 100k cohort and incomplete 1M campaign

Measured on 2026-11-01 using BrewFS `9832ddd` and JuiceFS 1.3.1 + TiKV.
**The planned 14-cell campaign did not complete.** Its complete 100k-file
cohort favors BrewFS for creating and using retained writable workspaces;
the 1M-file extension stopped during JuiceFS baseline import at the fixed
3600-second timeout. No 1M workspace comparison or runtime optimization is
accepted by this report.

## Completed 100k-file cohort

One published immutable base is reused for three paired rounds. Each round
creates four new sequential writable workspaces, retaining earlier ones:
12 workspaces per implementation. Preparation is measured once, not once
per round. These are operational rounds over one base, not three independent
fresh deployments.

| Product workflow, seconds | BrewFS packed + TiKV upper | JuiceFS + TiKV |
| --- | ---: | ---: |
| Base packing / native import, n=1 | 145.516 | 358.580 |
| Sum of all three four-workspace lifecycles | 187.577 | 1037.151 |
| Preparation + all 12 workspace lifecycles | **333.093** | **1395.730** |
| Common synthetic source generation | 8.465 | 8.465 |
| Including common source generation | **341.558** | **1404.195** |

The JuiceFS/BrewFS product workflow time ratio is 4.19 (76.1% less time for
BrewFS), or 4.11 including common source generation. This is a result for the
specified configuration. It is not the wall-clock duration of the entire
benchmark session: infrastructure setup, correctness verification, checkpoint
transfer/waits and the failed 1M extension are outside those product clocks.

| Paired round, seconds for four new workspaces | BrewFS | JuiceFS |
| --- | ---: | ---: |
| 1, BrewFS first | 61.975 | 339.837 |
| 2, JuiceFS first | 62.826 | 347.870 |
| 3, BrewFS first | 62.775 | 349.443 |

Every lifecycle includes mount/bootstrap, native clone where applicable,
prewarm, reads, COW with fsync, final upload drain, unmount and process exit.
All six cells report remote-ready; no assisted FUSE abort or teardown timeout
was used. Total drain/exit time across these cells is 0.326 s for BrewFS and
0.185 s for JuiceFS, already included above.

## Where it wins and where it loses

JuiceFS uses its actual `clone -p` command, which copies metadata while
sharing object data. It is not a byte-copy substitute. See the
[JuiceFS clone documentation](https://juicefs.com/docs/community/guide/clone/).
BrewFS binds each fresh entity upper to the same packed lower. The observed
workflow advantage is dominated by avoiding repeated native metadata clone
work. Initial packing also batches metadata and payload; its preparation
result does not isolate metadata processing.

| Stage median, seconds summed over four workspaces | BrewFS | JuiceFS |
| --- | ---: | ---: |
| Native clone command | Lower binding is included in bootstrap | 329.637 |
| Half-prewarm | 7.373 | 1.947 |
| Random reads after prewarm | **47.377** | **14.523** |
| COW, fsync and byte checks | **6.645** | **1.571** |
| Final drain and exit | 0.109 | 0.062 |

**JuiceFS is faster for the measured read and COW stages.** BrewFS has much
larger object traffic too: the first four-workspace cell records
13,139,836,582 GET response bytes versus 78,733,312 for JuiceFS. These counts
include metadata, prewarm and drain; they are not a pure payload amplification
measurement. No remote object-store bandwidth, request-cost or general
random-I/O advantage is established.

## Workload and configuration

- Synthetic base: 100,000 files of 4096 bytes across 1024 directories.
- Each workspace selects the same deterministic 1024 files, pre-reads 512,
  performs 4096 balanced random 4 KiB reads, then modifies 100 full files with
  fsync and read verification. Eight application workers are used.
- Source-name counts are checked exhaustively after timing. All selected
  reads/COW bytes and remote upper-object checksums are checked; base-content
  verification is sampled. This is not an exhaustive byte scan of the base.
- Both arms use synchronous upload, raw/no compression, 4 MiB chunk/block
  configuration, upload concurrency 8 and zero clean payload-cache budget.
  Packed geometry remains the default 256 KiB maximum random-frame size.
- BrewFS uses `workspace-entity-v1` with explicit experimental mount admission.
  No default, production admission policy, legacy path or runtime code changes.
- Internal metadata caching is different: BrewFS retains its default immutable
  metadata caches; JuiceFS attr/entry/dir-entry/open caches are explicitly zero
  in this protocol. Payload-cache parity does not imply identical metadata
  caching or a comparison of both products' defaults.
- BrewFS creates four fresh mounts. JuiceFS uses one shared mount per batch
  with four separate native directory clones. Workspaces are created/used
  sequentially; this does not test simultaneous multi-client fanout.

The host is one Aliyun spot `ecs.g7.2xlarge`, 8 vCPU / 32 GiB / 200 GiB ESSD,
Intel Xeon Platinum 8369B. RustFS S3-compatible storage, one PD and one TiKV
replica share that host. TiKV/PD report **8.5.3**, despite the imported image
aliases ending in `v8.5.0`; image fingerprints are frozen. JuiceFS reports
`1.3.1+2025-12-02.e0032b2`. This is not remote OSS/S3 or an HA deployment test.

## Incomplete 1M-file extension

Packed production completed for 1,000,000 files, but JuiceFS native baseline
sync reached the predeclared 3600-second subprocess limit. The harness stopped
and cleaned its services. **Zero 1M workspace cells ran**, including the
capacity pilot. There is no completed 1M import time, total or speedup. The
timeout is a harness limit in this environment, not proof that JuiceFS cannot
support this size. The original 14-cell contract and analysis plan remain in
the reproduction material; no sample or timeout was replaced.

## Evidence, validation and reproduction

[Sanitized results](entity-packed-fanout/results.json) preserve campaign
status, the original contract, all six samples, trace/COW digests, stage data,
versions and local validation. [CSV](entity-packed-fanout/samples.csv) contains
all operational samples. [Source digests](entity-packed-fanout/source-digests.json)
record measured and published LF helper hashes; two unused local/Windows
defaults were made portable and an extra EOF blank line was removed, with
the measured Linux execution path unchanged.

The full failed-run archive is preserved privately because it contains
generated test credentials and service logs. SHA-256:
`f32d4be6ddbbdb083fa6113d185d25a66388a59f9ef88dcc4d865f0b506e2b49`.
All 26 frozen harness files in that archive match the transferred package.
The only owned cloud VM and security group, all test volumes/containers and
the temporary local power request were released after archive verification.

WSL validation of unchanged main passed 1298 tests with 264 ignored, formatting,
shell checks, workspace check/build, runtime feature checks and strict
all-target Clippy. The native JuiceFS sync/clone recipe was checked separately
for correctness. No compilation overlapped cloud measurement.

The earlier transaction candidate remains unpublished. Its completed normal
diagnostic showed worse TiKV mixed-I/O read P95 in all five pairs. Its rejection
record is [retained here](entity-packed-fanout/rejected-transaction-experiment.json);
these fanout data do not accept that optimization.

See the [frozen harness and staging instructions](../../tools/perf/entity-packed-fanout/README.md)
to reproduce the original protocol, including its 1M timeout and checkpoint
collection. No additional cloud run is required to interpret this report.
