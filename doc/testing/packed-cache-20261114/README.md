# Packed object cache validation

The candidate prepares different Packed objects concurrently, deduplicates preparation for the same object, and retains disk entries according to byte budgets. Completed entries release their idle file descriptors. Entry owners retain their memory charges until the last reader finishes, and registry storage is reclaimed as entries retire. Each cache instance permits four HEAD operations and four whole-object downloads. Admission can use the existing range-read path when a download slot or budget is unavailable.

P0 and P1 were built from `fa630c33ed9691f66b09742ae695aecb0d666a0e` with the same Native mutation correctness changes and observation code. Those common changes read Native open permissions and the commit version from one target snapshot and apply bounded waits between Busy retries. P1 adds the Packed cache changes. Both binaries enable whole-object caching for metadata and data.

The C3 workload uses 4 KiB files, raw metadata and data encoding, inline data disabled, 64 MiB chunks, 4 MiB blocks, and seed 9173. Each size uses one published lower shared by both binaries. Each mount gets a fresh client cache; publication and qualification reads warm the object-storage backend. Qualification, the measured mount, and the reopening check use separate cache directories. The measured interval starts before the initial mount and ends after normal process exit, including download completion and teardown.

Each comparison consists of six pairs in `P0 P1 P1 P0` order repeated three times. Acceptance requires all six pairs to be faster, at least a 3% median reduction in paired elapsed time, no correctness or closure failures, and at most a 5% regression in the median elapsed time and p99 latency of each COW, hot-write, and mixed phase.

A 32-file functional and reopening comparison passed before the larger runs. Both measured sizes use eight workers. The 1,000-file workload has 2,048 cold and 2,048 hot read operations, 64 COW writes, 64 hot writes, and 256 mixed operations. The 10,000-file workload has 8,192 cold and 8,192 hot read operations, 256 COW writes, 256 hot writes, and 1,024 mixed operations. Each workload also includes the prewarming phases recorded in its comparison JSON.

| Files | Faster pairs | Median paired elapsed-time reduction | COW elapsed ratio | Hot-write elapsed ratio | Mixed elapsed ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 6/6 | 43.89% | 0.943 | 0.992 | 1.002 |
| 10,000 | 6/6 | 65.40% | 1.006 | 0.993 | 1.015 |

For 1,000 files, the COW, hot-write, and mixed p99 ratios are 0.959, 0.990, and 0.976. All twelve runs completed content and reopening checks, with zero Packed backend failures and zero whole-object cache download tail bytes. Normal-exit drain time was 0.132–0.134 seconds for P0 and 0.130–0.132 seconds for P1.

For 10,000 files, the COW, hot-write, and mixed p99 ratios are 0.935, 1.028, and 1.001. Lifecycle time was 55.98–56.80 seconds for P0 and 19.41–19.73 seconds for P1. All twelve runs completed content and reopening checks, with zero Packed backend failures and zero whole-object cache download tail bytes. Normal-exit drain time was 0.157–0.205 seconds for P0 and 0.202–0.206 seconds for P1. Both sizes satisfy the stated elapsed-time and write-phase acceptance criteria.

For 1,000 files, whole-object cache lookup hit rates were 29.2–30.0% for P0 and 27.3–28.3% for P1; decoded payload retention remained about 79.8% for both. The candidate used more range fallbacks while preserving the four-download limit. Packed runtime backend reads were 13.07–13.25 MB for P0 and 12.29–12.30 MB for P1, against 19,660,800 application-read bytes per run. These backend counts cover Packed metadata and payload. Application reads also include cache hits and upper-layer reads; startup, SDK retries, and upper-layer transport are outside the Packed counters.

At 10,000 files, whole-object cache lookup hit rates were 45.3–47.1% for P0 and 85.3–85.5% for P1; decoded payload retention was about 27–28% for both. Packed runtime backend reads were 328.84–340.84 MB for P0 and 55.85–56.22 MB for P1, against 82,837,504 application-read bytes per run. The backend-to-application byte ratio decreased from 3.970–4.115 to 0.674–0.679. The combined candidate changes object preparation, entry capacity, and file ownership; these measurements describe their combined effect.

The 1,000-file pairs 1–4 used sequential service preparation and closure. Pairs 5–6 and all 10,000-file pairs performed independent host preparation, immutable-object checks, and closure concurrently. All these actions finish outside the measured mount interval. Both members of each pair use the same service scheduling, binaries, data, cache budgets, and workload settings.

The client memory limit is 32 GiB with swap disabled. Disk-cache limits total 64 GiB: 32 GiB read cache, 16 GiB write cache, and 16 GiB Packed cache. Read memory is 8 GiB and write memory is 2 GiB. The metadata cache is 1 GiB, row cache is 64 MiB, and retained payload is 8 MiB. The exact environment and cache settings are recorded in `mount-settings.json`.

| Packed setting | Default | Value used here |
| --- | --- | --- |
| `BREWFS_PACKED_CACHE` | `on` | `on` |
| `BREWFS_PACKED_CACHE_BYTES` | 268435456 | 17179869184 |
| `BREWFS_PACKED_METADATA_CACHE_BYTES` | 268435456 | 1073741824 |
| `BREWFS_PACKED_RETAINED_PAYLOAD_BYTES` | 8388608 | 8388608 |

Apply these settings to each applicable Workspace mount process. Initialization and publication commands use their own environments. Initial, verification, reopened, child, and migration-target mounts each need explicit cache roots and budgets. With concurrent mounts, account for each cache instance's download concurrency and memory and disk limits at host level. A persistent Packed directory has an exclusive lock; another concurrent user of that directory uses a temporary cache. Independent directories provide independent caches. Reusing a directory after the previous owner exits permits disk-cache reuse for an explicitly warm-cache experiment.

Both binaries passed complete server-local CI: formatting, the workflow shell checks, workspace checks and builds, runtime variants, workspace tests, Clippy, and release builds. P1 passed all 18 focused concurrency tests and six cache budget tests. Its runtime-feature tests passed 2,963 tests with zero failures; default workspace tests passed 1,364 with zero failures. The existing ignored counts were 487 and 266. Source verification directly compared all 2,028 frozen candidate files with the submitted source, with no content differences before adding this report.

The [1,000-file comparison](1000-comparison.json) and [10,000-file comparison](10000-comparison.json) preserve every pair, workload phase, counter, timing, and original result path. Automated comparison files retain their pre-review acceptance fields. The separate [small-sample decision](1000-acceptance.json) qualified the larger experiment, and the [final decision](10000-acceptance.json) records acceptance after the completed review. CI inputs and results are summarized in [ci-summary.json](ci-summary.json), and the tested budgets are in [mount-settings.json](mount-settings.json).
