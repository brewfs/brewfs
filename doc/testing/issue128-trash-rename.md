# Atomic trash metadata for namespace deletion

Issue #128 concerns an overwritten file whose inode is retained for garbage
collection but whose original path is absent from the control-plane trash
record. PR #318's prepared-marker/promotion design cannot recover a crash after
the namespace commit and before promotion.

## Design

Use design A: write `system.brewfs.trash` in the backend operation that removes
the last name. Resolve the destination's full path from the backend namespace
inside that operation. The backend selects the destination inode and link
count; client lookup hints and path caches have no authority here.

SQLite and PostgreSQL have an existing rename transaction. The trash upsert
belongs in that transaction. Directory ancestry is read under the existing
namespace lock, with invalid or cyclic ancestry rejected instead of guessing a
root-relative path.

Redis scripts have isolation but **no rollback on runtime errors**. Namespace
scripts must prepare all writes, validate types/permissions, and encode all
values before executing any write. Reading or serializing a later source
hard-link update must not fail after a destination has already been deleted.
The record and tombstone then share one script execution and Redis persistence
boundary. This relies on the same Redis persistence guarantees as the namespace
itself, not a second client request or a recovery promotion.

Redis 7+ is required for `redis.acl_check_cmd`. Repository compose environments
already use Redis 7.2. The startup capability probe rejects older servers before
initializing metadata, and the scripts also reject a connection redirected to
an older server. Skipping that check is unsafe: a Redis 6.0 test with `ZADD`
denied demonstrated earlier writes surviving the script's eventual ACL error.

Unlink's old post-commit xattr write and restore's old post-commit xattr removal
can overwrite or erase a newer rename record on the same inode. Their record
creation/removal must also move into their backend transactions. Redis also
needs a restore operation: before this change it inherits the trait's
`NotImplemented` default. Restoring must validate the tombstone and destination
absence and consume the trash record atomically.

Only the final link creates a record. A live hard-linked target keeps its
existing xattrs. The next last-link deletion replaces any old record with that
deletion's path. A failed/no-replace/same-inode rename creates no record.
Retrying a committed rename with the now-missing source cannot alter its
displaced inode's metadata. There is no prepared state, operation token,
promotion, or compensating cleanup.

## Alternatives and backend scope

Design B requires an additional record identity, conditional cleanup, and
recovery protocol. Those are unnecessary when the namespace backend can own
the record directly.

Design C has no portable durable historical reverse index to use. SQL removes
the destination `content_meta` row and clears its parent on deletion. Etcd
clears its reverse row's parent/name. Redis retains some inline information,
but `get_names` filters deleted inodes, and ancestors can subsequently move.
The client's reverse path cache is not durable.

Etcd's `EtcdTxn` and TiKV's `write_txn_serialized` could commit a trash record
with their namespace changes, but these stores currently advertise no xattrs
and inherit unimplemented xattr and restore methods. This patch covers the
backends with control-plane trash metadata: Redis and SQL. Adding a trash API
to the other backends is separate functionality, not a non-atomic fallback.

`workspace_overlay/entity/{data_tests,backend_tests}.rs` exercise a different
entity backend API. The shared conformance tests here exercise `MetaStore` and
the actual control-plane handler, with backend-specific fault injection.

## Validation

Commands, exit codes, before/after counts, and self-review findings are recorded
below after execution. Commands use a separate target directory, two build
jobs, 16 dev codegen units, no incremental compilation, a `free -g` guard, and
`timeout -k 10 2700`.

### ACL failure audit (2026-10-25)

The `-hset: Ok(())` failure is not evidence of a denied write being ignored or
of a rollback failure. `RENAME_LUA` calls `stage_trash`, supplied by
`namespace.lua` in the same script, which stages exactly:

```lua
stage('HSET', xattr_prefix .. ino, xattr_name, cjson.encode({
    original_path='/' .. table.concat(parts, '/'), deleted_at=deleted_at
}))
```

The key is `x<destination inode>` and the field is `system.brewfs.trash`.
`stage` checks permissions before `commit_staged` runs any write. Both Rust
rename entry points propagate script errors with `.await.map_err(redis_err)?`;
the trait's `.map(|_| ())` preserves `Err`.

The authentication control experiment reproduced a test-harness trap:
redis-rs 0.26 parses `redis://issue128-auth-probe:@127.0.0.1:16390/0` with
`password=None`, and RESP2 skips `AUTH` in that case. The actual identity is
`default`, Lua's HSET permission check returns true, and direct HSET succeeds.
Setting `ConnectionInfo.redis.password=Some(String::new())` explicitly makes
the identity `issue128-auth-probe`, the check false, and direct HSET fail with
`NOPERM`. This is reproducible on Redis 7.4.2 without a script runtime error.

The working tree already had explicit authentication and a `WHOAMI` assertion
when this audit began. The initial audit run passed (1 passed, 0 failed).
The strengthened `rename_trash_denied_write_is_atomic` additionally verifies
that the actual HSET/ZADD command and target key are denied on the same
connection, that rename returns `namespace command denied: HSET/ZADD`, and
that the complete database DUMP snapshot is unchanged. It also checks that an
authorized retry succeeds and creates the expected `/dst` trash record.

The audit also found success-response JSON encoding after `commit_staged` in
rename, unlink, and restore. These three responses now encode before commit;
after commit the scripts only return the already encoded string. All reads,
type checks, ACL checks, and encodings in these operations precede the first
write.

Evidence is preserved in `../issue128-evidence/` beside the checkout:

- `after-trash-all.log`: original failure at `tests.rs:302`, `-hset: Ok(())`;
  exit 101, 23 passed and 1 failed. The older `after-focused.log` predates the
  ACL test and must not be used as a pass for this regression.
- `acl-auth-probe.rs` and `acl-auth-probe.log`: authentication control
  experiment, including actual identities and HSET outcomes.
- `acl-before-current.log`: unmodified working-tree test, exit 0.
- `acl-after.log`: strengthened regression test with explicit denial and
  unchanged-snapshot evidence.
- `acl-after-final.log`: regression test rerun after moving success-response
  encoding before commit, exit 0:

  ```text
  test meta::stores::redis::tests::rename_trash_denied_write_is_atomic ... ok
  test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1511 filtered out; finished in 0.03s
  ```
- `acl-trash-all.log`: `cargo test -p brewfs --lib trash -- --include-ignored
  --nocapture --test-threads=1`, exit 0, 25 passed, 0 failed, 0 ignored. This
  includes Redis 7.4.2, the Redis 6 capability rejection, SQLite, PostgreSQL,
  and the control-plane trash tests.
- `acl-fmt-isolated.log`: `cargo fmt --all --check` (no manifest-path option),
  exit 0. In the nested checkout the same command fails in Cargo metadata
  discovery because the parent repository claims the excluded vendor crate.
  It succeeds from an isolated copy whose 751 Rust source files were verified
  byte-for-byte identical; `acl-fmt-isolated.cwd` records its directory.
- `acl-script-gates.log`: all four required shell syntax checks and all three
  perf-report script tests, exit 0.
- `acl-rust-gates.sh`: completed with exit 0. Workspace check/build, both
  BrewFS FUSE runtime checks, workspace lib/bin tests, clippy, and diff check
  each returned exit 0. The workspace library test result was:

  ```text
  test result: ok. 1250 passed; 0 failed; 262 ignored; 0 measured; 0 filtered out; finished in 98.72s
  ```

The conditional `rfuse3` workspace runtime checks did not run because `rfuse3`
is not a workspace member. The all-features/overlay suites, all-targets
warnings-as-errors clippy, standalone vendor transport jobs, CSI/operator jobs,
and Docker/FUSE/performance suites were not run in this audit: they are outside
the local minimum gate and trash-specific validation executed here. This does
not claim a pass of the entire GitHub Actions workflow or performance suite.
