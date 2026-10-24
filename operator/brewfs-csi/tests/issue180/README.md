# Issue 180: rejected fd-unmount redesign

This is the historical second-attempt report. The narrowly scoped follow-up
and its validation are recorded in [third-attempt-validation.md](third-attempt-validation.md).
The failed designs and their evidence below are preserved unchanged.

PR #322 remains a draft. This directory preserves diagnostic reproductions;
it is not evidence that the ownership race is fixed. No cluster was used.
Tests ran on Linux `6.8.0-139-generic`, x86_64, on 2026-10-24.

## Result

The proposed quarantine implementation was committed in `5677e4e` and then
withdrawn in `31a1243`. The production tree is back to draft head `840f527`.
The issue remains open. The original blockers remain at:

- `operator/brewfs-csi/src/lib.rs:421`: pathname unmount after the last check.
- `operator/brewfs-csi/src/lib.rs:600`: stage/state persistence before the
  new bind's ownership verification at line 606.

The direct target-fd design failed: `umount2("/proc/self/fd/N", 0)` followed a
foreign overmount and detached it. Keeping the validated fd open did not
force the syscall to operate on that mount.

The quarantine design also failed. `move_mount(fd, "", ..., EMPTY_PATH)`
moved the validated mount **and its foreign overmount** into quarantine.
The following ordinary unmount then detached the foreign mount there. The
source mount fd itself had the expected mount ID throughout.

The first candidate regression appeared to pass only because its replacement
hook had been moved after `move_mount`. That omitted the race between the
last validation and the first mount-tree mutation. Restoring the hook before
`move_mount` failed. That earlier pass must not be used as merge evidence.

Additional compatibility checks:

- Moving a mount whose parent is shared returns `EINVAL` (22). CSI uses
  bidirectional mount propagation; silently making that parent private would
  change the deployment's propagation behavior.
- `MOVE_MOUNT_BENEATH` rejects an overmounted source atomically in this probe,
  but requires Linux 6.5 and a mounted destination. It does not by itself solve
  the shared-parent restriction or make the subsequent unmount atomic.
- A path to a quarantine directory is still a pathname. Closing mount fds to
  preserve normal `EBUSY` behavior reintroduces a check/use boundary there.

These results disprove the attempted designs. They do not claim that every
possible Linux mount-management architecture is impossible. A replacement
must prove the invariant across both validation-to-move and move-to-unmount,
preserve shared propagation and busy-mount semantics, and retain the later
main-branch PID fencing and failed-publish recovery changes (#320/#323).

## Syscall reproduction

The [probe](mount_api_probe.py) requires root and a private mount namespace,
checks those preconditions, uses a unique temporary directory, and cleans up
only its own fixture mounts. It checks mount IDs, not just source names.
The hard-coded `open_tree`/`move_mount` syscall numbers are guarded for x86_64.

From the repository root:

```bash
timeout -k 10 2700 sudo -n unshare --mount --fork --propagation private \
  python3 operator/brewfs-csi/tests/issue180/mount_api_probe.py
```

Observed output (temporary directory names shortened here):

```text
proc_fd: umount2=0 foreign_present=False
move_then_unmount: move_mount=0 errno=0 owned_at=<fixture>/quarantine foreign_at=<fixture>/quarantine
move_then_unmount: umount2=0 foreign_present=False
move_shared_parent: move_mount=-1 errno=22 owned_at=<fixture>/target foreign_at=None
beneath_overmount: move_mount=-1 errno=22 owned_at=<fixture>/target foreign_at=<fixture>/target
Unsafe candidate cases: 2/2; shared-parent and BENEATH restrictions confirmed.
```

Exit code: **1**, not a pass. Both attempted unmount designs violated the
foreign-mount invariant; the two additional kernel restrictions were observed.

## Rust regressions

The [regression patch](self-review-regressions.patch) is the original abandoned
adversarial patch applied to `840f527`. It is deliberately stored as a patch
rather than weakening or enabling known-failing tests in the normal suite.
The test-only hook belongs immediately before the first destructive syscall,
after all userspace validation. Do not move it past a mount-tree mutation.

Commands used, from the repository root (the patch was applied from
`/tmp/180-self-review-regressions.patch`; the committed copy only removes
trailing spaces on three empty context lines and passes `git apply --check`):

```bash
timeout -k 10 2700 git apply operator/brewfs-csi/tests/issue180/self-review-regressions.patch
timeout -k 10 2700 bash -c 'pgrep -af "rustc|cargo" | head; free -g'
export CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-180-target
export CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_INCREMENTAL=0
timeout -k 10 2700 cargo test --manifest-path operator/brewfs-csi/Cargo.toml \
  --locked --lib ownership_self_review_failed_verification_rolls_back -- --nocapture
timeout -k 10 2700 sudo -n unshare --mount --fork --propagation private \
  env CSI_PRIVATE_MOUNT_TEST=1 \
  /mnt/brewfs-data/src/brewfs-fix-180-target/debug/deps/brewfs_csi-9a1e1208d595a7ac \
  --ignored --nocapture
```

Use the test-binary path printed by Cargo if its hash differs. The host had
26–27 GiB available and no other Cargo build at these checks.

| Test/run | Passed | Failed | Ignored | Exit |
| --- | ---: | ---: | ---: | ---: |
| Baseline `ownership_self_review_failed_verification_rolls_back` | 0 | 1 | 0 | 101 |
| Baseline private-namespace ignored tests | 1 | 2 | 0 | 101 |
| Rejected candidate library tests | 20 | 0 | 3 | 0 |
| Candidate `ownership_self_review_last_syscall_window_preserves_foreign_mount`, hook before move | 0 | 1 | 0 | 101 |
| Reverted draft library and binary tests, regression patch removed | 26 | 0 | 1 | 0 |

Baseline private-namespace results:

```text
ownership_self_review_last_syscall_window_preserves_foreign_mount ... FAILED
  left: "owned"
 right: "foreign"
ownership_self_review_aliases_are_rejected ... FAILED
  aliased targets were accepted and unmounted: ["symlink"]
ownership_kernel_replacements_are_not_unmounted ... ok
```

The rollback regression failed with `failed ownership verification left
persisted state`. The rejected candidate fixed that focused rollback case,
but its unmount design remained unsafe, so the whole candidate was reverted.
The incomplete port onto newer main also failed three existing publish
recovery tests and was discarded. No claim is made that this draft safely
integrates main's newer CSI changes.

Local raw logs are `/tmp/180-redesign-baseline-rollback.log`,
`/tmp/180-redesign-baseline-kernel.log`,
`/tmp/180-redesign-candidate-unit.log`,
`/tmp/180-redesign-pre-move-race.log`, and
`/tmp/180-redesign-mount-api-probe.log`. The probe and patch above make the
failures reproducible without those host-local files.

The reverted draft's ordinary focused tests and formatting were also checked:

```bash
timeout -k 10 2700 cargo fmt --all --check
timeout -k 10 2700 cargo fmt --manifest-path operator/brewfs-csi/Cargo.toml --all --check
# Same CARGO_* environment as above, after removing the regression patch:
timeout -k 10 2700 cargo test --manifest-path operator/brewfs-csi/Cargo.toml \
  --locked --lib --bins -- --nocapture
timeout -k 10 2700 git diff --check
```

All four commands exited 0. The test run passed 19 library tests and 7 binary
tests, with one privileged library test ignored. These checks confirm that
the unsafe candidate was withdrawn without breaking the old focused suite;
they do not override the adversarial failures. No GitHub Actions results were
used, and neither readiness nor merge was requested for this failed redesign.
