# Explicit FUSE cache TTL

`--fuse-cache-ttl-ms` (YAML `fuse.cache_ttl_ms`) defaults to zero. It replaces the
forced-zero TTL only for metadata layers advertising writable POSIX ACLs. The
default column below uses neither the CLI flag nor the YAML field. Its TTL and
permission semantics are unchanged from PR #332.

The FUSE ABI serialises one `attr_valid` per reply, so a per-field split is
impossible. See `vendor/asyncfuse/src/raw/reply.rs`: `ReplyEntry` serializes its
TTL into both `entry_valid` and `attr_valid`, while `ReplyAttr` serializes one
attribute lifetime. A positive TTL consequently caches permissions as well as
size, timestamps and other inode attributes.

The four originally identified sites in `src/fuse/mod.rs` are:

| Reply | TTL source |
|---|---|
| Real `.stats` file LOOKUP (`ReplyEntry`, originally line 762) | `permission_cache_ttl()` |
| Ordinary LOOKUP (`ReplyEntry`, originally line 829) | `permission_cache_ttl()` |
| GETATTR (`ReplyAttr`, originally line 1205) | `permission_cache_ttl()` |
| Empty SETATTR (`ReplyAttr`, originally line 1244) | `permission_cache_ttl()` |

Other existing callers, including READDIRPLUS, non-size SETATTR and namespace
mutation replies, use the same policy. New regular-file CREATE attributes,
size-changing SETATTR and virtual `.stats` retain their separate zero TTLs.
The legacy TTL on unsupported/read-only ACL backends and the experimental
entity adapter are unchanged.

## Paired bench2 results

Measured on 2026-10-26 with the unchanged `/home/hxy/bench2.py`. Each format
used the same volume and `/mnt/brewfs-ttl-knob`, remounted between the absent
and 1000 ms settings. Every run passed `mountpoint -q` before measurement,
verified the `brewfs` FUSE source and a write/fsync/readback probe, and completed
all ten workloads, corpus checks and graceful unmount. The workload also
asserted the mount before every timed stage and at completion.

| Stat throughput (ops/s) | Knob absent (default unchanged) | `--fuse-cache-ttl-ms 1000` |
|---|---:|---:|
| Native, 1 thread | 152.7 | 459.7 |
| Native, 8 threads | 806.9 | 2,636.3 |
| Overlay, 1 thread | 386.1 | 1,162.9 |
| Overlay, 8 threads | 1,597.7 | 5,723.8 |

For every number in the absent column, zero attribute/entry TTL prevents a
complete stat from being served by those kernel caches. For every number in
the 1000 ms column, valid cached entries/attributes can serve kernel hits;
the benchmark does not count individual hits. Bench2 walks 5,000 files, with
directory-listing corpus checks outside timing, rather than repeatedly
accessing the small warm set used in the protocol fixture below.

The paired ratios are **3.01x / 3.27x native** and **3.01x / 3.58x overlay**.
These are single local observations, not medians or confidence intervals.

Historical reference medians supplied with the task are native **937.1 /
2,910.7 ops/s** and overlay **1,836.6 / 4,903.1 ops/s** for one/eight-thread stat.
Those writable-ACL, zero-TTL measurements required FUSE requests; a kernel-only
attribute/dentry hit could not serve the complete stat call. They were medians
of three observations on a separate ECS instance using an earlier binary
(`d13365c` with uncommitted changes). They are historical references, not
controlled before measurements for this revision. The fresh default results
are substantially lower, so the paired ratios must not be read as speedups
against those historical values.

Other measured workloads are retained here to show the limits of the result:

| Workload | Native absent | Native 1000 ms | Overlay absent | Overlay 1000 ms |
|---|---:|---:|---:|---:|
| Sequential write (MiB/s) | 163.1 | 151.6 | 195.2 | 202.5 |
| Sequential read, kernel caches dropped (MiB/s) | 57.4 | 56.5 | 81.8 | 79.0 |
| Sequential read, warm (MiB/s) | 181.2 | 209.2 | 77.5 | 77.9 |
| Create, 1 thread (ops/s) | 69.5 | 90.2 | 122.4 | 148.5 |
| Unlink, 1 thread (ops/s) | 66.8 | 90.7 | 188.6 | 242.9 |
| Create, 8 threads (ops/s) | 143.6 | 152.5 | 277.7 | 285.0 |
| Unlink, 8 threads (ops/s) | 150.9 | 173.9 | 427.7 | 472.7 |
| Write 4 KiB files (files/s) | 23.7 | 29.6 | 38.5 | 40.3 |

In particular, native sequential write was 7.1% lower in its 1000 ms run.
This single pair does not establish a general I/O non-regression result.
Sequential writes do not explicitly fsync; cold reads drop kernel caches,
not all client caches. No timed operation or teardown failed.

The local host has eight Intel Xeon Platinum vCPUs, 30 GiB usable RAM and
Linux 6.8.0-139-generic. Redis and RustFS ran in dedicated local containers.
Both formats used 16 FUSE workers, 512 background requests, `RUST_LOG=info`,
and identical cache defaults. Overlay used the authenticated empty packed
lower from the original comparison. No Cargo/rustc process was active at any
run's start; host swappiness was restored to zero before measurement.

The identical release executable used no default features, enabled
`fuse-io-uring-runtime,workspace-overlay`, disabled debug info, and set
BrewFS release codegen units to 256. Its source is the code committed in
`db8c548`; its embedded version says `2791752-dirty` because compilation began
before that commit. The measurements do not include the later asyncfuse
changes merged into `main`. Implementation PR #349 was merged by the
coordinator while local validation was still running; this report records
the completed validation of that measured revision.
SHA-256 identifies the measured artifacts:

```text
brewfs:    7be9b98873689bb2907f900f40cc8cfaa7ca7736a9871ae6d12f5850c0864e0b
bench2.py: 04f45c998246b5f17bb68241c4ebaacbcf68f7120a8ef907286e0d06436d49f5
```

Exact argv, source hashes, mount identities, TSVs, full logs and
`validated-bench-summary.json` are preserved under
`/home/hxy/attr/evidence/ttl-knob` on the measurement host. Each invocation was
`python3 /home/hxy/bench2.py <format>-<absent|1000ms> /mnt/brewfs-ttl-knob <out.tsv>`.

## Permission trade-off and invalidation

With 1,000 ms the warm stat loop drops from **10,000 FUSE LOOKUP + 20,000 GETATTR
requests to 0**, and the cost is a permission-staleness window of up to **0.876 s**
(measured: 86 stale access checks after an external `chmod 000`, denial delayed
0.876 s versus 0.0006 s at TTL 0). This is an explicit operator choice and the
default preserves strong consistency.

This prior measurement was a standalone FUSE protocol fixture on Linux
6.8.0-139-generic, with UID/GID 1000. Exact denial delays were 0.876128 s and
0.000568 s. At 1000 ms the kernel also allowed child stat after external ancestor
`chmod 000`; an acknowledged synchronous INVAL_INODE with offset -1, length 0
ended stale access/traversal. This demonstrates the trade-off, not a BrewFS
multi-mount coherence guarantee or a universal upper bound of 0.876 s.

The operator gives up immediate observation of external chmod/chown, ACL
changes, namespace changes and lease revocation on cache hits. Kernel access
and traversal can complete without checking the daemon's current authority.

Existing local INVAL_INODE calls for chmod/chown, POSIX ACL and xattr set/remove,
truncate, fallocate and copy remain reachable and unchanged. They queue a
notification for one mounted VFS; they do not acknowledge application by the
kernel or broadcast to other mounts. Entry invalidation is unavailable in the
notification path at the tested base (`b21654e`); it requires the separate
asyncfuse notification PR [#346](https://github.com/brewfs/brewfs/pull/346),
which has since merged upstream. `vendor/asyncfuse` is unchanged by this PR,
and the new API is not wired into namespace callbacks here.
INVAL_ENTRY is not added for create,
mkdir, mknod, symlink, link, unlink, rmdir or rename: a notification can wait on
locks held until that request's reply, and the current asyncfuse notification
sender shares a response queue. There is no safe post-reply hook. Ordinary
FUSE replies continue to update the initiating kernel. External metadata
edits, remote mutations and lease loss have no mount invalidation broadcast.
See [CONFIG.md](../../CONFIG.md) for the supported configuration and limitation.

## Validation

The three focused default-feature tests passed, covering absent and explicit
zero, CLI-over-YAML precedence, numeric bounds, and the unchanged legacy
backend policy. The CLI regression was first observed failing on the old
parser. The dedicated `fuse_cache_ttl` integration target also passed with
`--no-default-features --features fuse-io-uring-runtime,workspace-overlay` in
release mode. It exercises actual writable-ACL LOOKUP, GETATTR and SETATTR
replies for absent/0/7/1000 ms, shared VFS clones, real/virtual `.stats`, and
the creation/truncation zero-TTL exceptions.

The requested xfstests ran in the exact order below with both the CLI knob and
YAML field absent. No case was excluded or modified:

| Case | Native, knob absent | Overlay, knob absent |
|---|---|---|
| `generic/087` | PASS (2 s) | PASS (1 s) |
| `generic/097` | PASS (2 s) | PASS (2 s) |
| `generic/014` | PASS (319 s) | PASS (251 s) |
| `generic/100` | PASS (521 s) | PASS (228 s) |

Both successful containers exited zero without OOM. The initial overlay
attempt stopped before any case because the image declares `/mnt/brewfs` as
a Docker volume; the packed mount guard rejects a target that is already a
mount. The retry used the ordinary directory `/mnt/ttl-xfstests`, with the
same binary, volume, tests and configuration otherwise. Failed setup evidence
and successful per-case logs are preserved alongside the benchmark artifacts.
The test containers and dedicated Redis/RustFS volumes were removed afterward.

The requested workspace Cargo gates passed on the measured implementation:

```text
cargo build -j 2 --workspace
cargo test -j 2 --workspace --lib
cargo clippy -j 2 --workspace --lib -- -D warnings
```

The library suite reported **1,261 passed, zero failed, 262 existing ignored**.
`cargo fmt --all --check` and `cargo check -j 2 --workspace` also passed.
The no-default-feature `fuse-tokio-runtime` and `fuse-io-uring-runtime`
`cargo check -j 2 -p brewfs` runs passed as well.
The local command set matches the requested library gates and the CI workflow
at the tested base; it is not a claim about later `--all-targets` CI changes.

Every Cargo build/test/check/Clippy invocation exports
`CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0`,
uses `-j 2`, and first checks for Cargo/rustc processes. Builds also run in an
18 GiB RAM cgroup. Earlier attempts to compile the entire overlay unit-test
harness encountered compiler memory pressure; one cgroup OOM had peak RSS
17,692,776 KiB (16.9 GiB). The reply test was moved intact to its dedicated
integration target; no existing test was removed or weakened. The final
successful release/test build peaked at 15,524,168 KiB (14.8 GiB). Temporary
swappiness used for that build was restored to zero before measurements.
