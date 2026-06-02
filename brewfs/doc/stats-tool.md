# BrewFS Stats — Real-time Performance Monitor

## Overview

`brewfs-stats` is a terminal-based real-time performance monitoring tool for BrewFS,
inspired by JuiceFS's `juicefs stats` command. It reads a virtual `.stats` file exposed
at the mount root and displays per-second throughput, latency, and operation counts.

## Architecture

```
┌─────────────────────────────────────────┐
│  BrewFS FUSE Daemon                   │
│                                         │
│  ┌─────────────┐  ┌─────────────────┐  │
│  │ FsStats     │  │  FUSE Handlers  │  │
│  │ (AtomicU64) │◀─│  OpTimer RAII   │  │
│  └──────┬──────┘  └─────────────────┘  │
│         │                               │
│  ┌──────▼──────────────────────┐        │
│  │ .stats virtual file         │        │
│  │ inode: 0x7FFF_FFFF_0000_0003│        │
│  │ Prometheus text format      │        │
│  └─────────────────────────────┘        │
└─────────────────────────────────────────┘
         │ cat /mnt/brewfs/.stats
         ▼
┌─────────────────────────────────────────┐
│  brewfs-stats CLI                     │
│  - Reads .stats every N seconds         │
│  - Computes deltas (ops/s, bytes/s)     │
│  - Colored terminal output              │
└─────────────────────────────────────────┘
```

## Usage

```bash
# Basic usage (1s refresh)
brewfs-stats /mnt/brewfs

# Custom interval
brewfs-stats /mnt/brewfs -i 2

# Read raw metrics directly
cat /mnt/brewfs/.stats
```

## Output Sections

| Section  | Columns               | Description                              |
|----------|-----------------------|------------------------------------------|
| **FUSE** | ops, read, write, r_lat, w_lat | FUSE layer throughput & latency |
| **META** | ops, txn, lat         | Metadata operations & transactions       |
| **OBJECT**| get, get/s, put, put/s, del | S3 object storage traffic       |
| **CACHE**| hit, miss, dirty      | Block cache hit rate & dirty buffer size  |

## Metrics Format

The `.stats` virtual file outputs Prometheus-compatible text:

```
brewfs_uptime_seconds 3600
brewfs_fuse_read_ops_total 123456
brewfs_fuse_read_bytes_total 1073741824
brewfs_fuse_read_lat_us_total 5000000
brewfs_fuse_write_ops_total 45678
brewfs_fuse_write_bytes_total 536870912
brewfs_fuse_write_lat_us_total 8000000
brewfs_fuse_lookup_ops_total 789012
brewfs_fuse_lookup_lat_us_total 2000000
...
```

## Instrumented Operations

Currently instrumented FUSE handlers:
- **read**: ops count, bytes transferred, latency
- **write**: ops count, bytes transferred, latency
- **lookup**: ops count, latency

Future: getattr, open, create, unlink, readdir, flush, plus meta and S3 layers.

## Building

```bash
cargo build -p brewfs-stats
# Binary at target/debug/brewfs-stats (or target/release/ with --release)
```

## Design Notes

- Uses `AtomicU64` with `Relaxed` ordering for zero-contention counter updates
- `OpTimer` RAII guard automatically records on drop — no manual bookkeeping
- Virtual file uses a high inode number (`0x7FFF_FFFF_0000_0003`) to avoid conflicts
- The `.stats` file is read-only (mode 0444) and only accessible at the mount root
- No additional dependencies in the main brewfs crate; CLI tool uses `crossterm` for colors
