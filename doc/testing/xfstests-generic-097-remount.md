`generic/097` remount investigation, 2026-10-25

Baseline: upstream `f0676c5df134f004bbc9f222a520c951a855d64a`, Docker
29.1.3, Linux 6.18.54.1-microsoft-standard-WSL2, real `/dev/fuse`, Redis
workspace catalog and the workspace-overlay release binary. The packed fixture
must be allowed through `.dockerignore` for the workspace image to build.

The failure is a race in the xfstests mount helper. Kernel unmount can return
while the old BrewFS daemon is still draining and releasing its workspace
authority. The helper previously waited only for the FUSE mount to disappear.
Its replacement daemon could therefore race the previous owner's metadata
updates and exit before mounting. The reproduced daemon error is:

```text
Error: workspace is busy
```

The unmodified baseline passed one initial `generic/097` run and 30 repeated
runs on this host. A plain `brewfs mount` → `umount` → `brewfs mount`, with
50 ms added in each direction to Redis, reproduced the failure without xfstests:

```text
umount returned with first daemon still alive
immediate-remount failed status=1
Error: workspace is busy
first daemon exited status=0; starting third mount
after-daemon-exit mounted
```

The third mount read the original payload, `trusted.colour="marone"` and
`user.colour="beige"`. No kernel mount remained after cleanup. This rules out
a persistent stale lease or lost xattrs in that reproduction.

To reproduce the full xfstests output reliably, the diagnostic image used the
same Redis delay and paused the old daemon for five seconds immediately after
the real `umount` returned. It then resumed that process. The original helper
failed with the reported shape:

```text
unmount the FS and see if EAs are persistent
brewfs mount process exited before /mnt/brewfs became a mountpoint (status=1)

get trusted EA colour: marone
getfattr: TEST_DIR/foo: No such file or directory
```

The fix holds a file lock beside the generated mount configuration. The daemon
inherits the descriptor, so the next helper waits for process teardown before
starting another mount. `BREWFS_PRE_MOUNT_WAIT_SECS` bounds this wait, with a
60-second default matching the startup wait. The former 10-second default
expired between repeated cases with the deliberately delayed backend. The
lock file stays on the same inode, and process exit releases its ownership.
No filesystem lease, fencing, xattr or FUSE teardown semantics change.
Direct CLI orchestration likewise needs to wait for the original daemon to
exit; this change supplies that synchronization in the test mount helper.

`docker/compose-xfstests/test_xfstests_mount_helper.sh` exercises the generated
helper with real subprocesses and a delayed lease release. It fails before the
fix and passes afterwards, checks both daemon logging branches, verifies that
a timeout launches no replacement daemon, and checks successful mounting after
an injected startup failure. CI runs this regression without Docker or FUSE.

The normal real-suite acceptance command passed all three requested cases:

```text
Ran: generic/001 generic/002 generic/097
Passed all 3 tests
```

The final diagnostic image also passed three consecutive `generic/097`
iterations with the same Redis delay and five-second teardown pause. There
were no mount retries or timeouts in that run. The real daemon was observed
holding the inherited descriptor for `/run/brewfs/config.yaml.mount.lock`.

Local artifacts, including the diagnostic image sources and minimal repro,
are preserved under `/home/luxian/xf097/evidence` and its parent directory:

- Failing full case: `teardown-before/workspace-overlay-20261025-141427-11246/xfstests-a/run-1792908870-5504/`, including `results/generic/097.out.bad` and `brewfs.log`.
- Plain CLI reproduction: `minimal-latency-50.log` and `minimal-20261025-140606/logs/`.
- Passing final normal suite: `final/workspace-overlay-20261025-142300-15724/xfstests-a/run-1792909382-11896/`.
- Passing final delayed suite: `teardown-final/workspace-overlay-20261025-142147-3712/xfstests-a/run-1792909311-15613/`.
- Shell regression: `regression-before.log` and `regression-after.log`.

The default workspace test gate passed (1,250 passed, 0 failed, 262 ignored),
as did workspace check/build, all four FUSE runtime feature checks, both Clippy
configurations, the patched FUSE transport tests/checks, formatting and the
script gates. The all-features command failed on unchanged baseline Rust code:
`EtcdEntryInfo: rkyv::Archive` is not implemented at
`src/meta/stores/etcd/mod.rs:1958`. An additional workspace-overlay unit-test
build exceeded this host's memory and was killed by the kernel OOM handler.
Those two results are preserved; neither is represented as a passing gate.
CSI checks/tests/Clippy and operator checks/tests/Clippy/CRD comparison passed.
