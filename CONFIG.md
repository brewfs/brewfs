# FUSE cache TTL

The full configuration reference is [Configuration](doc/operations/configuration.md).

`brewfs mount --fuse-cache-ttl-ms <MILLIS>` opts workspace LOOKUP and GETATTR
replies into bounded kernel metadata caching. The default remains **0**;
omitting the option preserves the existing reply TTLs and permission behavior.
CLI overrides YAML, including an explicit CLI `0` overriding YAML `1000`:

```yaml
fuse:
  cache_ttl_ms: 1000
```

The workspace metadata layer checks the remote workspace, head/base generation,
writer lease, and (when present) the independent packed binding. It records one
deadline **before** this check. Every cacheable reply receives only the time
remaining until that deadline. Hits do not extend it. On expiry, the next read
rechecks remote authority and clears older daemon metadata rows if the generation
changed. Failed checks return errors rather than extending the old cache.

This is bounded freshness, not immediate cross-mount invalidation. For example,
1000 permits old metadata/permissions for the remaining portion of a one-second
checked interval. The TTL is reduced by the time spent validating and reading.
As with other FUSE TTLs, the kernel starts its relative timer when it consumes
the reply: a stalled reply queue or paused daemon adds delivery delay. There is
no hard real-time wall-clock guarantee across such a stall. A new remote writer
lease fences the old mount; the old mount returns an error after expiry and must
be remounted. It does not silently adopt another writer's view.

Only workspace layers implementing the validation contract can grant positive
writable-ACL TTLs. Other writable-ACL implementations return zero. Mutation
replies, READDIRPLUS, and virtual `.stats` retain zero TTL on writable workspaces.
Backends without writable POSIX ACL support retain their legacy
`BREWFS_CACHE_TTL_MS` policy. Values are unsigned 64-bit milliseconds; a deadline
that cannot be represented disables caching.
The requested value is the maximum checked interval: `3600000`, for example,
allows up to one hour of metadata and permission staleness plus reply delivery
delay. There is no implicit one-second cap; use `1000` for the one-second policy.

The FUSE ABI has one `attr_valid` for the entire attribute snapshot. Caching
size/timestamps also caches permissions. `ReplyEntry` uses the same lifetime
for attributes and the dentry. This change does not split the ABI or change INIT
maxima.

PR #346 provides acknowledged inode and entry invalidations. Existing local
inode invalidations remain, but there is **no cross-mount writer broadcast**.
The refresh/deadline mechanism does not depend on one. No synchronous entry
notification is sent from a namespace callback: it can wait on a directory lock
held by the request whose reply shares the notification queue.

Entry invalidation is unavailable in the notification path at this change's
tested base (`b21654e`); it requires the separate asyncfuse notification PR
[#346](https://github.com/brewfs/brewfs/pull/346). That PR has since merged
upstream. This change does not modify `vendor/asyncfuse` or wire the new API into
namespace callbacks. `FUSE_NOTIFY_INVAL_ENTRY` cannot safely be added to the
tested create, mkdir,
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

Raw database/object edits that bypass catalog transactions and generation
increments are outside the supported protocol. Data/page-cache coherence,
shared dirty writeback across clients using copied lease credentials, and
immediate permission revocation need additional protocols. The option remains
opt-in. See the [writer inventory and validation report](doc/testing/workspace-metadata-cache.md)
and the [historical TTL-only experiment](doc/testing/fuse-cache-ttl.md).
