# FUSE cache TTL

The full configuration reference is [Configuration](doc/operations/configuration.md).

`brewfs mount --fuse-cache-ttl-ms <MILLIS>` explicitly opts a mount with writable
POSIX ACL support into kernel attribute and entry caching. The default is **0**:
omitting the option preserves today's strong permission consistency and the
permission semantics of PR #332. No volume format enables this override
automatically.

The equivalent YAML setting is:

```yaml
fuse:
  cache_ttl_ms: 0
```

CLI overrides YAML, including an explicit CLI `0` overriding YAML `1000`.
Values are unsigned 64-bit milliseconds. The legacy `BREWFS_CACHE_TTL_MS` policy
for backends without writable POSIX ACL support is unchanged. Newly created
regular-file attributes, size-changing SETATTR replies, and the virtual `.stats`
file retain their existing zero-TTL policies.

With 1,000 ms the warm stat loop drops from **10,000 FUSE LOOKUP + 20,000 GETATTR
requests to 0**, and the cost is a permission-staleness window of up to **0.876 s**
(measured: 86 stale access checks after an external `chmod 000`, denial delayed
0.876 s versus 0.0006 s at TTL 0). This is an explicit operator choice and the
default preserves strong consistency.

Those request counts and delays were measured in a standalone Linux FUSE
fixture. They illustrate the trade-off; 0.876 s is an observation, not a promised
upper bound. A positive TTL permits kernel access checks and directory traversal
to use old mode/UID/GID/ACL state until expiry or invalidation. Operators give up
immediate visibility of external permission revocation and metadata/namespace
changes, including lease loss or ownership takeover. The kernel can satisfy a
cache hit without consulting the daemon's current permissions or lease.

The ABI limitation is: **the FUSE ABI serialises one `attr_valid` per reply, so a
per-field split is impossible**. Caching size/timestamps also caches the
permission-bearing attributes. `ReplyEntry` uses the same TTL for `entry_valid`
and `attr_valid`; `ReplyAttr` has one attribute TTL.

Existing local `FUSE_NOTIFY_INVAL_INODE` calls remain in place for chmod/chown,
POSIX ACL and xattr set/remove, and the existing truncate, fallocate and copy
paths. These notifications belong to one mounted VFS. The asyncfuse API queues
them without acknowledging that the kernel applied them; they are not a
cross-mount permission-revocation barrier.

Entry invalidation is unavailable until the separate asyncfuse notification PR
lands; this change does not modify `vendor/asyncfuse`. `FUSE_NOTIFY_INVAL_ENTRY`
cannot safely be added to the current create, mkdir,
mknod, symlink, link, unlink, rmdir or rename callbacks: Linux may wait for
directory locks held by the triggering request, while the notification shares
the response queue needed to finish that request. There is no safe post-reply
notification hook here. The initiating kernel updates its namespace through
the ordinary FUSE replies. See the [libfuse notification API's deadlock
restrictions](https://github.com/libfuse/libfuse/blob/master/include/fuse_lowlevel.h#L1810).
Mutations from other mounts, direct metadata edits,
and workspace lease revocation do not broadcast inode or entry invalidations
to this mount. Normal write/SETATTR/FALLOCATE replies retain their existing
kernel-update behavior; this option adds no new page-cache invalidation or
data-consistency guarantee.

For measured throughput and acceptance results, see
[FUSE cache TTL measurements](doc/testing/fuse-cache-ttl.md).
