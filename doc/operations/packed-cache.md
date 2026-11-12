# Packed object cache

Shared workspace mounts use the Packed object cache by default. The first actual
read of an immutable lower object starts one asynchronous full-object download.
Readers consume locally written prefixes as soon as their requested bytes exist;
subsequent ranges reuse the same download or completed local file. Metadata and
frame authentication remain in the existing read path.

Use `--packed-cache on|off` (`BREWFS_PACKED_CACHE`) to compare modes in the same
binary. `--packed-cache-bytes BYTES` (`BREWFS_PACKED_CACHE_BYTES`) controls the
mount's disk budget, default 268435456 bytes. YAML/JSON accepts `cache.packed_cache`
and `cache.packed_cache_bytes`. Both options apply to `volume.format: workspace`.
The ordinary payload and metadata cache budgets are independent and must remain
fixed across comparisons. Download buffers and registry entries consume the
existing Stored and Control admission budgets.

Completed objects live under `<cache_root>/packed/<manifest-digest>`. Incomplete
files are not reusable after remount. The cache reserves each object's full size
before downloading; objects exceeding the budget and requests without an available
download slot or admission permit use the existing range path. At most four whole
objects download concurrently. Completed, unpinned entries can be evicted; active
read streams pin their entries until EOF/drop. A concurrent mount that cannot lock
the persistent directory uses a temporary cache with the same disk budget.

After a download fails, readers holding that entry still receive its original
error. New reads use the range path while an old reader or worker pins the failed
entry, so its partial file remains charged to the disk budget. Once the final pin
is released, the next read removes the partial file and releases its reservation
before starting another fill.

Unmount joins active downloads before releasing the read transport. Drain logs
report hits, ready/inflight hits, misses, local bytes and downloaded bytes. These
are cache-boundary counters, not an application-level metadata hit rate.

The `.stats` runtime backend counters record admitted Full GET, HEAD and Range GET
calls when their backend factory runs, and response body bytes when consumed,
before any local file write. Opening errors, the first body error, or a malformed
full-object length count one failed operation; admission rejection and local file
errors do not. Local cache hits add no remote requests or bytes. These counters
exclude mount-time manifest/probe I/O and SDK-internal retries; the external S3
meter remains the wire-level reference. AX's strict warm-read gate requires the
Full GET, HEAD, Range GET and received-byte counters to exist: cold reads must
issue a Full GET or Range GET and receive bytes, while the warm interval must add
zero Full GETs, HEADs, Range GETs and received bytes. Existing logical-read and
failure checks still apply.

## Names and existing data

The current ordinary volume is `flat`, the shared workspace is `workspace`, the
Cargo feature is `workspace`, and the internal module is `workspace::packed`.
The fixture producer is `packed_snapshot_fixture`. The old versioned volume
names and old `BREWFS_PACKED_V3_*` settings are rejected. Persistent workspace key
names changed; regenerate experimental volumes and snapshots. No migration or
old-format compatibility is provided. Wire validation retains its actual numeric
protocol checks; these are not selectable storage engines.

Build one artifact for both cache modes:

```sh
cargo build --locked --release --features workspace,observability,rustfs-ec-block-size-hint \
  --bin brewfs --bin packed_snapshot_fixture
```

Retain default Cargo features. Record the binary SHA, input hash, every cache
budget, S3 request/byte counters, cold/warm read time and drain time. A small
same-host RustFS qualification does not establish distributed C3/C4 performance.
