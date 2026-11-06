# TiKV active transaction drop

Branch: `fix/tikv-active-transaction-drop`

## Audit

The audit used:

```text
rg -n 'Transaction|begin_with_options|begin_read|begin_write|commit\(|rollback\(' src/
```

The vendored client starts ordinary optimistic and pessimistic transactions in
`Active`. `Drop` only reports that state; `read_only()` starts in `ReadOnly`,
which is terminal for the SDK drop check and owns no locks. The table lists
every production TiKV creation point in `src/` and the owner of its terminal
transition.

| File:line | Creation | Normal close | Cancellation/early-return risk before this change | Result |
| --- | --- | --- | --- | --- |
| `src/meta/stores/tikv/mod.rs:243` | optimistic `begin_read` | `run_txn` rolls back on read success, error, and retry | `run_txn` could be cancelled while the task or rollback future was pending | `TxnGuard` in `run_txn` schedules rollback from `Drop` |
| `src/meta/stores/tikv/mod.rs:251` | pessimistic `begin_write` | `run_txn` commits success and rolls back all pre-commit errors/retries | FUSE/request cancellation or a cancelled commit could leave pessimistic locks | `TxnGuard` in `run_txn` |
| `src/native_base/write/tikv.rs:59` | pessimistic control transaction | explicit rollback for checks/writes/deadline; commit on success | cancellation during any RPC or commit | `TxnGuard` plus existing explicit rollback |
| `src/native_base/write/tikv.rs:179` | optimistic point read | rollback on read error and success | cancellation during `get`/rollback | `TxnGuard` plus existing rollback |
| `src/native_base/write/tikv.rs:210` | optimistic prefix scan | rollback on scan error and success | cancellation during scan/rollback | `TxnGuard` plus existing rollback |
| `src/workspace_overlay/entity/tikv.rs:291` | optimistic `read_only` snapshot | no mutation or lock; SDK status is `ReadOnly` | no active-drop warning; an error only drops a read snapshot | kept explicit `read_only` + `Warn` to make future misuse visible |
| `src/workspace_overlay/entity/tikv.rs:370` | pessimistic entity commit | rollback on pre-commit conflict/error; commit on success | cancellation inside locking, validation, or commit | `TxnGuard` plus existing explicit rollback |
| `src/workspace_overlay/stores/tikv.rs:434` | optimistic native locator read | rollback after `get` | cancellation before rollback | `TxnGuard` plus existing rollback |
| `src/workspace_overlay/stores/tikv.rs:487-517` | pessimistic bounded authentication helper | caller rolls back between attempts and before return | helper returns an active transaction; caller cancellation could drop it before the next statement | caller wraps result in `TxnGuard` at `:987` and replacement at `:1073` |
| `src/workspace_overlay/stores/tikv.rs:520-547` | optimistic `read_only` bounded read | no locks; SDK status is `ReadOnly` | no active-drop warning; cancellation drops a read snapshot | retained as read-only; no `Ignore` suppression |
| `src/workspace_overlay/stores/tikv.rs:692` | optimistic full scan | rollback on retry/success | `?` from scan/validation or cancellation could bypass rollback | `TxnGuard` plus existing rollback |
| `src/workspace_overlay/stores/tikv.rs:805` | optimistic point read | rollback after `get` | cancellation during get/rollback | `TxnGuard` plus existing rollback |
| `src/workspace_overlay/stores/tikv.rs:903` | optimistic batch read | rollback after batch get | cancellation during timestamp/read/rollback | `TxnGuard` plus existing rollback |
| `src/workspace_overlay/stores/tikv.rs:1210` | pessimistic CAS | rollback on every pre-submit branch; commit is terminal/unknown | cancellation during lock, clock check, or commit | `TxnGuard` plus existing explicit rollback |
| `src/workspace_overlay/packed_v3/wire005/native_placement.rs:91` | optimistic import/publish transaction | commit on success; old `?` paths had no synchronous rollback | early return for immutable/change checks, or cancellation | `TiKvTxnGuard` returned by `begin` |
| `src/workspace_overlay/entity/tikv_tests.rs:272` | ignored test-only pessimistic blocker | explicit rollback after the lock-wait assertion | test abort/panic could abandon it; test is isolated and now closes it before teardown | test-only direct client use; no production path |
| `src/workspace_overlay/entity/tikv_tests.rs:305` | ignored test-only pessimistic probe (each retry) | rollback on probe error and on the returned transaction before assertion | test abort/panic could abandon it | test-only direct client use; every branch closes it |
| `src/workspace_overlay/entity/tikv_tests.rs:417` | ignored test-only allocator probe (each retry) | explicit rollback after every `get_for_update` | test abort/panic could abandon it | test-only direct client use; every branch closes it |
| `src/workspace_overlay/stores/manager_backend_tests.rs:390,401` | ignored test-only pessimistic setup/cleanup transactions | commit after the insert and cleanup mutations | `unwrap` panic could abandon one | test-only direct client use; no production path |

The three ignored entity lock-order tests and manager bootstrap tests also use
direct `begin_pessimistic`; each test explicitly rolls back or commits its
transaction before assertion/shutdown. No other `Transaction` creation exists
in production `src/` after the audit.

## Fix

`src/tikv_transaction.rs` contains `TxnGuard<T, R>`. A normal successful
commit/rollback calls `finish()`. If the owning future is cancelled, `Drop`
moves the transaction into a Tokio task and awaits one best-effort rollback;
rollback failures are logged with the operation label. The guard leaves
commit-error transactions armed, because the client permits rollback from
`StartedCommit` and a failed commit must not leak a pessimistic lock.

The vendor was not changed. The client implementation confirms that the
warning is emitted locally by `Transaction::Drop`; read-only transactions start
in `ReadOnly`, while pessimistic lock rows are buffered as lock mutations and
are released by rollback. This is application ownership/cancellation behavior,
not a TiKV server GC semantic.

## Tests and local 5k validation

`tikv_transaction::tests::cancelled_guard_rolls_back_without_active_drop_warning`
creates a locked fake transaction inside a spawned future, aborts that future,
then asserts the rollback removed the lock and the fake active-drop counter is
zero. `five_thousand_imports_survive_four_workers_and_failures` runs 5,000
unique imports with four Tokio workers and deterministic pseudo-random first
attempt cancellation; it asserts all files imported, all locks drained, and
zero active-drop warnings.

Run without a release profile:

```bash
CARGO_TARGET_DIR=/home/hxy/txn-target \
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
cargo test --lib --no-default-features --features fuse-tokio-runtime \
  tikv_transaction::tests -- --nocapture
```

Observed locally on the unoptimized test profile:

```text
running 2 tests
test tikv_transaction::tests::cancelled_guard_rolls_back_without_active_drop_warning ... ok
test tikv_transaction::tests::five_thousand_imports_survive_four_workers_and_failures ... ok
test result: ok. 2 passed; 0 failed; finished in 0.03s
```

The real-cluster follow-up (after the C3 instances are available) is:

```bash
export BREWFS_BIN=/path/to/brewfs
export BREWFS_META_TIKV_PD_ENDPOINTS='172.24.86.193:2379,pd-2.example:2379'
export BREWFS_META_TIKV_NAMESPACE='c3-1m-active-drop'
export BREWFS_DATA_BACKEND=s3
export BREWFS_META_BACKEND=tikv
export BREWFS_RUSTFS_ENDPOINT='http://rustfs.example:9000'
export BREWFS_RUSTFS_BUCKET='brewfs-c3-1m'
export AWS_ACCESS_KEY_ID='...'
export AWS_SECRET_ACCESS_KEY='...'
"$BREWFS_BIN" \
  --data-backend s3 --meta-backend tikv \
  --meta-tikv-pd-endpoints "$BREWFS_META_TIKV_PD_ENDPOINTS" \
  --meta-tikv-namespace "$BREWFS_META_TIKV_NAMESPACE" \
  --s3-endpoint "$BREWFS_RUSTFS_ENDPOINT" \
  --s3-bucket "$BREWFS_RUSTFS_BUCKET" \
  --s3-force-path-style true \
  --fuse-workers 4 \
  /mnt/brewfs >/tmp/brewfs-c3-1m.log 2>&1 &
# Run the repository's reproducible small-file driver with four workers:
python3 tools/perf/smallfiles_rw.py \
  --root /mnt/brewfs --subdir c3-1m --mode write \
  --files 1000000 --file-bytes 4096 --workers 4 \
  --json-output /tmp/brewfs-c3-1m.json --label tikv-rustfs-1m
grep -c 'Dropping an active transaction' /tmp/brewfs-c3-1m.log
```

The command uses a scratch TiKV namespace and RustFS bucket. Replace the binary,
PD, RustFS, and credentials with the C3 values, then also capture TiKV lock
metrics after the run.
