# Issue 180: ordering rollback and final identity re-check

This revision starts from PR #322 head `ca22fe9ff598a6bd717875aa811c646fad8ea368`
and integrates main `f6d85830022b013b3282f0fc71fca5d36468e593`. It fixes the
rejected-publish ordering defect and adds defence in depth at unmount. It does
not establish an atomic unmount. The [second-attempt evidence](README.md) is
preserved; none of the disproven syscall experiments was repeated.

Publish preflight rejects an existing foreign target without starting BrewFS
or writing state. For a new target, stage preparation remains in memory until
the publish recovery intent is written. The intent is necessary to preserve
#323's recovery across restart. Ownership/mode verification precedes committing
the publish. Verification errors trigger source-verified rollback of the bind,
the shared mount/process started by this attempt, and the recovery intent;
an existing stage and its prior state are preserved. If cleanup itself fails,
the operation returns retryable `Aborted` and retains pending recovery state,
including the ability to clean up an owned bind whose read-only remount failed.
It does not discard recovery information or unmount an unverifiable replacement.

Unmount inspection obtains `statx(...).stx_mnt_id` with `STATX_MNT_ID` and joins
it to `/proc/self/mountinfo`. The final check compares the mount ID and full
filesystem identity/mode against the retained validation observation. A changed,
absent, or uninspectable mount returns `WouldBlock`, mapped to CSI `Aborted`,
without calling the unmount operation. Mount observations are released only
after the comparison so normal `EBUSY` semantics remain intact.

The code comment at the unmount site states:

```text
Defence in depth: statx(STATX_MNT_ID) + /proc/self/mountinfo re-check
shrinks the TOCTOU window to the syscall boundary; it does NOT
eliminate it. A replacement after this check can still be unmounted.
Disproven on Linux 6.8: umount2("/proc/self/fd/N", 0) follows a
foreign overmount; quarantine + move_mount carries that overmount
along and the next unmount removes it. Do not retry either design.
Evidence: https://github.com/brewfs/brewfs/issues/180#issuecomment-6385927780
PR #322: operator/brewfs-csi/tests/issue180/README.md.
```

## Local validation

All commands use `timeout -k 10 2700`. Cargo runs use this environment and the
task's dedicated target directory (removed with the one task worktree after
completion):

```bash
export CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_TARGET_DIR=/home/luxian/fix-work/issue180-third-target
timeout -k 10 2700 free -g
# Before each Cargo command: if available memory is below 6 GiB, sleep 60s
# and check again. Observed available memory stayed above the threshold.
```

No swap was added or reconfigured. No CI result is used as validation.

The ordering regression was added alone to the original PR head and failed
before implementation. Exact baseline command (inside the timeout-wrapped
memory-guard/environment shell):

```bash
cargo test --manifest-path operator/brewfs-csi/Cargo.toml --locked --lib \
  ownership_failed_verification_rolls_back -- \
  --exact tests::ownership_failed_verification_rolls_back --nocapture
```

```text
failed ownership verification left persisted state
test tests::ownership_failed_verification_rolls_back ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 20 filtered out; finished in 0.00s
```

Exit **101**. The final regression additionally covers an ownership-inspection
error, prior staged state, and stopping the process created by the failed
attempt. Its exact final command and output are:

```bash
timeout -k 10 2700 cargo test --manifest-path operator/brewfs-csi/Cargo.toml \
  --locked --lib tests::ownership_failed_verification_rolls_back -- --exact --nocapture
```

```text
running 1 test
test tests::ownership_failed_verification_rolls_back ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.00s
```

Exit **0**. The identity regression feeds two observations of the same
filesystem/root/mode with different mount IDs into the production re-check
boundary. It asserts that the unmount callback is not invoked and that the
error is retryable. It also checks absent/uninspectable observations and an
unchanged observation that does invoke the callback. It does not race mounts.

```bash
timeout -k 10 2700 cargo test --manifest-path operator/brewfs-csi/Cargo.toml \
  --locked --lib tests::ownership_unmount_recheck_refuses_mismatched_identity -- --exact --nocapture
```

```text
running 1 test
test tests::ownership_unmount_recheck_refuses_mismatched_identity ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.00s
```

Exit **0**. All 22 final gates below exited **0**; none timed out or failed to
compile. The [machine-readable record](third-attempt-gates.json) also preserves
the environment and exact test-summary output.

| Suite | Passed | Failed | Ignored | Exit |
| --- | ---: | ---: | ---: | ---: |
| CSI library | 35 | 0 | 1 | 0 |
| CSI binary | 7 | 0 | 0 | 0 |
| Remount rollback integration | 1 | 0 | 1 | 0 |
| Stage timeout integration | 1 | 0 | 1 | 0 |
| CSI doc tests | 0 | 0 | 0 | 0 |
| Root workspace libraries/binaries | 1244 | 0 | 254 | 0 |

The CSI library ignore is the pre-existing privileged mount test, not run in
this attempt. The two integration ignores are subprocess entry points invoked
by their passing parent tests. CSI totals are 44 passed, 0 failed, 3 reported
ignored. The `rfuse3` feature matrix is inapplicable: workspace metadata lists
only `brewfs` and `brewfs-stats`. Workspace Clippy passed with existing vendor
warnings; CSI Clippy passed with `-D warnings`.

| Exact command | Exit |
| --- | ---: |
| `timeout -k 10 2700 cargo fmt --all --check` | 0 |
| `timeout -k 10 2700 cargo fmt --manifest-path operator/brewfs-csi/Cargo.toml --all --check` | 0 |
| `timeout -k 10 2700 cargo check --manifest-path operator/brewfs-csi/Cargo.toml --locked` | 0 |
| `timeout -k 10 2700 cargo build --manifest-path operator/brewfs-csi/Cargo.toml --locked` | 0 |
| `timeout -k 10 2700 cargo test --manifest-path operator/brewfs-csi/Cargo.toml --locked --lib tests::ownership_failed_verification_rolls_back -- --exact --nocapture` | 0 |
| `timeout -k 10 2700 cargo test --manifest-path operator/brewfs-csi/Cargo.toml --locked --lib tests::ownership_unmount_recheck_refuses_mismatched_identity -- --exact --nocapture` | 0 |
| `timeout -k 10 2700 cargo test --manifest-path operator/brewfs-csi/Cargo.toml --locked -- --nocapture` | 0 |
| `timeout -k 10 2700 cargo clippy --manifest-path operator/brewfs-csi/Cargo.toml --locked --all-targets -- -D warnings` | 0 |
| `timeout -k 10 2700 git diff --check` | 0 |
| `timeout -k 10 2700 bash -n docker/compose-xfstests/run_perf_in_container.sh` | 0 |
| `timeout -k 10 2700 bash -n docker/compose-xfstests/run_redis_perf.sh` | 0 |
| `timeout -k 10 2700 bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh` | 0 |
| `timeout -k 10 2700 bash -n docker/compose-xfstests/run_juicefs_perf.sh` | 0 |
| `timeout -k 10 2700 bash docker/compose-xfstests/test_perf_report_delta.sh` | 0 |
| `timeout -k 10 2700 bash docker/compose-xfstests/test_juicefs_direct_matrix.sh` | 0 |
| `timeout -k 10 2700 bash docker/compose-xfstests/test_juicefs_perf_report.sh` | 0 |
| `timeout -k 10 2700 cargo check --workspace` | 0 |
| `timeout -k 10 2700 cargo build --workspace` | 0 |
| `timeout -k 10 2700 cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` | 0 |
| `timeout -k 10 2700 cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime` | 0 |
| `timeout -k 10 2700 cargo test --workspace --lib --bins` | 0 |
| `timeout -k 10 2700 cargo clippy --workspace` | 0 |

## Self-review

The production diff was reviewed against both the previous PR and integrated
main. PID start markers and pidfd signaling from #320 are preserved; the
preflight idempotent-publish path now explicitly authenticates the live process,
covered by `ownership_existing_publish_preserves_pid_fencing`. The #323 recovery
tests remain enabled. Its command-injection integration test now runs through
the manager that owns rollback; it tests remount failure, cleanup failure, and
recovery after a simulated restart. Mountinfo tests replace the old `findmnt`
matcher coverage. Pending recovery only relaxes the requested mode for cleanup;
it still requires the recorded shared filesystem/root identity.

Intermediate strict Clippy failed (exit 101) on inherited PID-code lints;
the equivalent `and_then`/`nth` and platform-gated fallback fix those lints.
The first full CSI test attempt failed to compile (exit 101) because #323's
integration test still used the replaced `bind_matches` API. That test was
adapted as described above. Neither failure was treated as a pass.

The tests prove rollback and refusal when the identity re-check detects a
change. They do not prove safety against replacement after that final check.
No quarantine, `MOVE_MOUNT_BENEATH`, propagation change, lazy detach, or new
unmount architecture is introduced. Issue #180's residual syscall window
remains open.
