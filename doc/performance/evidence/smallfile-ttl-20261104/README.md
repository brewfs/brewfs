# Small-file TTL measurement evidence

See [the measurement report](../../smallfile-ttl-attribution-2026-11-04.md) for
the four-arm comparison, uncertainty, GETATTR attribution and recommendations.
The branch contains documentation and data only. All executables/scripts ran
outside the repository with the preserved main binary; no compilation occurred.

| File | Contents |
| --- | --- |
| [measurements.tsv](measurements.tsv) | All twelve raw run durations and FUSE integer counts, GETs, order and timestamps |
| [summary.json](summary.json) | Run values, medians, mean t intervals, paired comparisons and fixed environment contract |
| [counter-snapshots.json](counter-snapshots.json) | HTTP before/after FUSE, object GET and metadata RPC counters used by the analysis |
| [diagnostics.json](diagnostics.json) | Sampler-free per-stage opcode counts, GETATTR inode traces, syscall correlations and excluded diagnostic paths |
| [scanner-syscalls.txt](scanner-syscalls.txt) | Actual stat/open/fstat/ioctl/read/close sequence; diagnostic timings are not benchmark results |
| [kernel-snapshots.json](kernel-snapshots.json) | Global dentry/inode occupancy and host state, explicitly not cache hit/miss rates |
| [provenance.json](provenance.json) | Main identity, source/harness digests, original artifact file digests, kernel and execution order |
| [reproduce.md](reproduce.md) | Exact external measurement and analysis scripts, archived as Markdown |

Original evidence is retained at
`/mnt/brewfs-data/home/hxy/smallfile-warm/artifacts/ttl-20261104-*` and
`/mnt/brewfs-data/home/hxy/smallfile-warm/ttl-20261104/`.
The parent `compose.sh`, immutable fixture, accepted artifacts and main/candidate
binaries were not modified. New run IDs prevent overwriting older measurements.

The main binary SHA-256 is
`e2a308519ad1e0f5d2295e10cf3fb9eb64ce57009eb64b01c87489b51128f34f`,
size 409,876,496 B, source `f5706712302435206269a253bf861a1a648761d1`.
The parent build recipe is hashed in provenance; this is the same preserved
Tokio/observability/overlay build used by #416, not a newly optimized executable.

Formal runs use the original Compose scanner for A/B/C. D is its raw
open/read-until-EOF/close variant. All counters include the original runner's
small monitoring workload; exact file-level opcode structure comes from the
separate sampler-free diagnostics. All twelve runs passed full content checks
and have the same SHA-256 map digest in `summary.json`.

Native 0.27 ms/file is the task's historical reference, not a new control.
Confidence intervals are between-run **mean** t intervals (n=3, df=2), not
nonparametric median intervals or per-file latency percentiles.

Initial strace `-yy/-P` and decoded-IOCTL diagnostics added their own FUSE
pathname requests. Their paths and rejection reason remain in `diagnostics.json`;
they are excluded from all reported counts/timing. Final `raw=ioctl` tracing
matches the independent untraced per-stage counts exactly.

The collection script validates all twelve completion markers/tool passes,
source/binary/mount environment, attr/entry gauges, byte counts, checksums,
SHA-256 maps and diagnostic trace counts before generating these files.
No accepted optimization, Rust CI result or cache-coherence guarantee is claimed.
