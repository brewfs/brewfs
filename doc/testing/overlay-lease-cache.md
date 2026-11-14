# Lease-bound upper row retention

Private Workspace mounts now retain cached upper metadata while the existing
exclusive lease proof remains valid. The shared default budget is still
64 MiB (including values retained by readers), with at most 65,536 entries
per cache. The default no longer discards unchanged rows at 60 seconds.

Local commits invalidate affected keys before and after the remote commit.
Negative extent/directory results carry workspace, layer versions, query range
and mutation generation. View replacement clears both caches. An uncertain
or cancelled commit disables cached authority; an observed lease lapse cannot
resurrect rows when a later heartbeat arrives. Neither cached permissions nor
directory views authorize a write: mutations retain remote lease/version CAS
and final metadata publication. This does not add an immediate remote
revocation notification channel; the existing lease heartbeat and deadlines
still bound cached read authority.

Configuration:

```bash
BREWFS_WORKSPACE_ROW_CACHE_POLICY=lease # default
BREWFS_WORKSPACE_ROW_CACHE_BYTES=67108864
# Optional previous insertion-age policy, retaining all lease checks:
BREWFS_WORKSPACE_ROW_CACHE_POLICY=ttl
BREWFS_WORKSPACE_ROW_CACHE_TTL_MS=60000
```

Zero bytes or zero TTL still disables the row caches. Without a bound lease,
rows always use TTL expiration. The policy changes retention, not consistency.

At directory rewind, an unchanged Packed Workspace source can be reused after
a fresh remote layer/lease check. This avoids rebuilding the lower directory,
probing upper directory pages again, and allocating another directory view.
Any layer/namespace change rebuilds the view, including attribute-only changes
because the handle carries dot attributes. Failed validation propagates to
READDIR/READDIRPLUS instead of silently using an old handle.

OPENDIR and the first READDIR still each check the remote lease. They cannot
share an unchecked remote proof: the lease may be revoked between requests.
The optimization deliberately does not claim to remove those two checks.

`tools/perf/check_overlay_hot.sh` runs KV, lease, cache, namespace and publication
regressions without executing SQLite. The matching Redis/RustFS scripts and
three-pair evidence are maintained in the
[experiment repository](https://github.com/Ivanbeethoven/ax-multi-agent-experiments/tree/codex/a355-lease-row-cache-evidence-20261114/experiments/overlay-lease-cache-a355-20261114).
