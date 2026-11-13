# Entity Redis prefix index (#230)

At `a8459dc`, every entity prefix observation executes a Lua loop over Redis
`SCAN MATCH ... COUNT 128` until cursor zero. The 2,048-call guard bounds the
whole database traversal, not the selected prefix. Directory enumeration,
directory emptiness checks, and recovery all use that path.

The backend now maintains a per-volume, score-zero sorted set at
`{brewfs-entity:<volume UUID>}/key-index`. Every transactional put/delete and
allocator-generated row updates it atomically. Prefix observations use one
`ZRANGEBYLEX ... LIMIT 0 <limit + 1>` operation, then validate/read only matching
rows. Overflow still fails closed, and existing parent/inode predicates and
complete-observation semantics remain in place. This addresses unrelated
database traversal; the existing matching-row limits and repeated per-file
cleanup scans are separate concerns (#187/#188 and #197).

## Upgrade

Stop older entity writers before opening a volume with this version. Its first
open backfills existing rows using ordinary cursor-based `SCAN` calls outside
Lua, with at most 128 keys in each atomic index update. A crash before completion
causes the next open to retry. An empty sorted-set member marks completion;
subsequent opens do not scan. The index itself is reserved and is not returned
as an entity row. First open still does O(database size) work, spread across
Redis turns. Do not mix old and new writers or modify entity rows out of band.

## Validation

On ax-srv, cargo/rustc/rustfmt are unavailable. The regression and implementation
were reviewed statically; no Rust tests, Redis fixture, or latency benchmark
were run. No performance acceptance measurements or README tables are updated.

`redis_entity_prefix_scan_does_not_traverse_unrelated_keys` seeds a legacy row,
opens the backend, and uses a scoped ACL user forbidden to call `SCAN`. It checks
complete ten-entry listings, empty prefixes, overflow, and transactional deletion
with 1,000 then 100,000 unrelated keys. On the old implementation the first
listing necessarily invokes the forbidden command. Allocator conformance tests
also check generated inode/dentry/xattr and pending-slice visibility; corruption
tests require index type errors to precede writes.

Before acceptance on brewfs, run the full Rust CI gate from `AGENTS.md` and
`.github/workflows/ci.yml`, including formatting, check/build, runtime feature
checks, clippy, and the mandatory workspace lib/bin tests. Enable
`workspace` to compile and run these tests as well:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo test --workspace --lib --bins
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo test --features workspace workspace::entity --lib
# Use an owned, isolated Redis 7 instance with ACL administration enabled.
BREWFS_TEST_REDIS_URL=redis://127.0.0.1:6379/ \
  CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo test --features workspace \
  workspace::entity::backend::tests::redis_ --lib \
  -- --ignored --test-threads=1
# Includes the real Redis namespace/recovery fixtures and RustFS data fixtures.
ENTITY_DATA_ARTIFACT_ROOT=/absolute/artifact/path \
  bash tools/testing/run_entity_data_tests.sh
```

Repeat a matched benchmark at the base and candidate commits: warm the same
ten-entry namespace directory, measure repeated `namespace.readdir` at 1,000 and
100,000 unrelated database keys, and record Redis commandstats plus a second
client's GET/lease-renewal p50/p95/p99/max latency. Measure initial backfill
separately from steady-state listing. Keep directory contents, Redis settings,
request counts, concurrency, and warmup identical; retain the artifacts and run
the compose acceptance gates required by `AGENTS.md` before accepting numbers.
