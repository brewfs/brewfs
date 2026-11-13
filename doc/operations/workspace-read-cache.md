# Workspace Overlay read cache

Workspace mounts with a Packed lower enable these caches by default:

| Cache | Default limit | Control |
|---|---|---|
| Upper metadata rows, including empty extent/directory results | 64 MiB shared by upper and namespace caches; at most 65,536 entries per cache; 60-second TTL | `BREWFS_WORKSPACE_ROW_CACHE_BYTES`, `BREWFS_WORKSPACE_ROW_CACHE_TTL_MS`; `BREWFS_WORKSPACE_EMPTY_QUERY_CACHE=0` disables only empty-result caching |
| Authenticated lower payload frames | 8 MiB per mount, filled on demand | `BREWFS_PACKED_RETAINED_PAYLOAD_BYTES`; `0` disables retention; nonzero capacity must be 513 bytes–64 MiB |

The 8 MiB default matches the local hot-lower validation and limits additional
memory per workspace. The charge includes decoded frame bytes and 512 bytes of
entry overhead; frames larger than the limit are read without being retained.
Old frames are evicted as the cache fills. This is separate from the existing
VFS chunk and Packed metadata cache budgets, which are unchanged.

No environment variables are required to enable these defaults. To give lower
frames 32 MiB on a larger workspace:

```bash
export BREWFS_PACKED_RETAINED_PAYLOAD_BYTES=33554432
```

Every newly fetched frame entering retention is authenticated, including after
eviction or lease invalidation. Retaining readers enforce this even if
`BREWFS_PACKED_VERIFY_MEMO=1`; that switch remains effective for readers without
payload retention. Local hits reuse the already authenticated immutable bytes.

Upper cache entries require the current workspace lease and matching layer
identity/version. Mutations invalidate empty results; lease loss and view
changes stop old entries from being served. TTL limits residency, not how long
stale data may be served. Positive upper data and deletion markers still take
part in Overlay resolution. No write acknowledgement or fsync semantics change.

Inspect `.stats` for `brewfs_workspace_upper_extent_queries_total`,
`brewfs_workspace_upper_extent_empty_cache_hits_total`,
`brewfs_workspace_upper_directory_queries_total`, and
`brewfs_workspace_upper_directory_empty_cache_hits_total`. Lower retention
reports `brewfs_packed_v3_payload_retention_capacity_bytes`, `resident_charge_bytes`,
`hits_total`, `misses_total`, and `evictions_total` under the same prefix.
RustFS request counts and Redis commandstats remain separate evidence.
