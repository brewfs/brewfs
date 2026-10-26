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

Measurements are pending validation; no candidate throughput is claimed yet.

Historical reference medians supplied with the task are native **937.1 /
2,910.7 ops/s** and overlay **1,836.6 / 4,903.1 ops/s** for one/eight-thread stat.
Those writable-ACL, zero-TTL measurements required FUSE requests; a kernel-only
attribute/dentry hit could not serve the complete stat call. They are separate
from this revision's paired measurements, not an additional trial in either
column.

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
kernel or broadcast to other mounts. Entry invalidation is unavailable until
the separate asyncfuse notification PR lands; `vendor/asyncfuse` is unchanged.
INVAL_ENTRY is not added for create,
mkdir, mknod, symlink, link, unlink, rmdir or rename: a notification can wait on
locks held until that request's reply, and the current asyncfuse notification
sender shares a response queue. There is no safe post-reply hook. Ordinary
FUSE replies continue to update the initiating kernel. External metadata
edits, remote mutations and lease loss have no mount invalidation broadcast.
See [CONFIG.md](../../CONFIG.md) for the supported configuration and limitation.

## Validation

Validation is in progress. The CLI regression was first observed failing on the
old parser, then passed with the implementation. Focused tests cover absent
and explicit zero, CLI-over-YAML precedence, numeric bounds, and the unchanged
legacy backend policy. The writable-ACL test exercises the actual replies and
the creation/truncation/virtual-stats exceptions through a shared VFS clone.
