# Workspace truncate timestamps (#262)

The reported generic/313 failure used workspace with SQLite metadata.
The workspace atomic SETATTR updated size, ctime and data version but left
mtime unchanged when FUSE supplied only a size. Both truncate(2) and
ftruncate(2) reach this path.

Size-bearing SETATTR now sets mtime to the operation timestamp unless an
explicit mtime was supplied. SET_MTIME_NOW remains authoritative. Requests
without a size retain their existing behavior.

`tests/workspace_truncate_timestamps.rs` exercises the public metadata API
with a real SQLite workspace. It verifies extension, shrinking, unchanged
size, persistence, unchanged atime, explicit timestamps, chmod and
SET_MTIME_NOW. Historical timestamps make advancement assertions independent
of sleeps or clock resolution.

Before the production change, the new integration suite reported one pass
and three failures at the mtime advancement assertions. After the change,
all four tests passed in each of three runs:

```sh
CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  CARGO_PROFILE_TEST_DEBUG=0 cargo test -p brewfs --features workspace \
  --test workspace_truncate_timestamps -- --test-threads=1 --nocapture
```

Mounted verification used the compose xfstests container's generic/313 runner,
fresh SQLite workspace metadata and local object storage for every run.
The baseline reported:

```text
mtime not updated after truncate up
mtime not updated after ftruncate
```

The candidate container mounted the newly built executable from this
worktree. All three candidate runs exited zero and reported:

```text
Ran: generic/313
Passed all 1 tests
```

Full local CI results are recorded separately. The raw before/after evidence
on the verification host is retained under `/home/hxy/pr-verification/` in
`issue262-mounted-red/` and `issue262-mounted-green-{1,2,3}/`, with unit-test
output in `/home/hxy/issue262-{red,green-1,green-2,green-3}.log`.
