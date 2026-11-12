# Issue #222: stop the native mount heartbeat when its owner is dropped

## Root cause and minimal fix

`WorkspaceMountSession` owns a `LeaseHeartbeat`. Before this fix, dropping the
Tokio JoinHandle detached the task, and dropping the owner's cancellation
token did not cancel the clone held by the task. The task kept its metadata
store and validity proof alive and renewed the exclusive writer lease forever.

`LeaseHeartbeat` now owns the validity proof and implements Drop: revoke the
local proof, cancel the token and abort the task. Explicit stop still joins
the cancelled task before backend release. Drop does not synchronously release
backend authority. Any already-sent renewal may complete and expire under the
existing TTL and lease fencing; a late local `extend` cannot undo revocation.
Packed sessions continue to use their existing separate heartbeat owner.

## Before/after proof

The baseline ran on unchanged production at `origin/main` revision
`a8459dc92142bf94224e4f4af903f592cef5cb13`. Its saved patch changes only tests.
The current regression file is byte-for-byte identical to the baseline test.
The SQLite fixture acquires a native lease with a 300 ms TTL and a 50 ms
heartbeat interval, drops the session, keeps the runtime alive for three TTLs,
then tries to acquire a replacement writer. An explicit-release control
distinguishes the Drop path from ordinary release.

Before, exit 101:

```text
dropped_mount_session_stops_renewing_the_writer_lease ... FAILED
dropped mount kept renewing its exclusive writer lease: Some(Busy)
explicit_mount_release_allows_an_immediate_replacement ... ok
test result: FAILED. 1 passed; 1 failed; 0 ignored
```

After, three executions, each exit 0:

```text
dropped_mount_session_stops_renewing_the_writer_lease ... ok
explicit_mount_release_allows_an_immediate_replacement ... ok
test result: ok. 2 passed; 0 failed; 0 ignored
```

Each test run took 0.95 seconds. The fresh build and first run took 1,073.30
seconds; the repeat commands took 1.77 and 1.87 seconds. All after-runs used
`CARGO_BUILD_JOBS=2`, both dev and test codegen units 16, incremental/debug
information disabled, and a shared host build lock. The scope peaked at
14,756,077,568 bytes, below its 16 GiB memory limit, with zero OOM events.

```bash
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_CODEGEN_UNITS=16 \
CARGO_PROFILE_TEST_CODEGEN_UNITS=16 CARGO_INCREMENTAL=0 \
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
cargo test -p brewfs --features workspace-overlay \
  --test workspace_session_drop -- --test-threads=1 --nocapture
```

## Validation scope

The regression and explicit-release control above passed three times.
Formatting and diff whitespace are checked before publication. Existing
dependency warnings are retained in the complete logs.

All scheduled local gates passed:

| Gate | Result |
| --- | --- |
| Focused lifecycle harness | 5 passed / 0 failed / 0 ignored |
| Overlay resolver integration | 14 passed / 0 failed / 0 ignored |
| Full default workspace libraries/binaries | 1,196 passed / 0 failed / 225 ignored |
| Workspace check and build | both passed |
| Overlay workspace check | passed |
| Strict overlay and default all-target Clippy | both passed with `-D warnings` |
| Tokio and io-uring FUSE checks | both passed |
| Four shell syntax checks, three shell scripts, diff check | all passed |
| `cargo fmt --all --check` | passed |

The focused lifecycle run took 1.03 seconds after a 538.32 second build. The
full workspace test run took 140.15 seconds after compilation. The 16 GiB
cgroup reached its memory limit transiently (`max` events 632) but recorded
zero OOM kills; this resource output is preserved in the complete logs. All
commands used two Cargo jobs, dev/test codegen units 16, incremental/debug
information disabled, and ran serially under the shared host lock.

The full monolithic all-features overlay suite and mounted real-backend suites
were not run. The overlay coverage above is the selected lifecycle and resolver
coverage, not a full-overlay or mounted-backend claim.

The separate lifecycle harness selects all five tests in the lifecycle module.
Restoring only its excluded test declarations reproduces all 532 candidate
source files byte-for-byte; production bodies are unchanged. It covers lease
isolation, real renewal/loss, permanent revocation, the acquisition head race
and fork/discard. No harness exclusions are included in the PR. This selected
coverage is not a full-overlay suite claim.

`commands.md` lists completed commands, environments and exact result counts.
`validation.tar.gz` preserves full logs, source patches, the harness generator
and its source-equivalence report. After all commands exited, the private
target was removed only after a `/proc` users audit; `target-cleanup.json`
records 8,375,136,256 allocated bytes removed and disk moving from 85% to 82%.
No gate remains pending in this local scope.
