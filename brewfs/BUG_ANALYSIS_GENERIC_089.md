# generic/089 Bug Analysis

## Summary

`xfstests generic/089` failed on BrewFS because concurrent `t_mtab` processes could complete the mtab update loop but fail when appending their final status line to `mtab_output`.

The visible symptom was an output mismatch: each phase printed only one `completed 50 iterations` line instead of three.

## Failure Symptom

Expected output:

```text
QA output created by 089
completed 50 iterations
completed 50 iterations
completed 50 iterations
completed 10000 iterations
directory entries:
t_mtab
...
```

Actual output:

```text
QA output created by 089
completed 50 iterations
completed 10000 iterations
directory entries:
t_mtab
...
```

The missing lines came from the three concurrent commands in `generic/089`:

```bash
$here/src/t_mtab 50 $mtab_output &
$here/src/t_mtab 50 $mtab_output &
$here/src/t_mtab 50 $mtab_output &
wait
cat $mtab_output
```

## Investigation

Initial logs showed that the `t_mtab` lock loop itself completed. There were 150 successful lock acquisitions for three `t_mtab 50` processes, so the missing output was not caused by the mtab update loop stopping early.

A focused `strace` showed the real failure:

```text
openat(AT_FDCWD, "/mnt/brewfs/trace_out", O_WRONLY|O_CREAT|O_APPEND, 0666) = 3
lseek(3, 0, SEEK_END)                   = 24
fstat(3, {st_mode=S_IFREG|0644, st_size=24, ...}) = 0
write(3, "completed 50 iterations\n", 24) = -1 EACCES (Permission denied)
exit_group(0)                           = ?
```

So the second and third `t_mtab` processes successfully opened the existing output file with `O_APPEND`, but their final `write(2)` failed with `EACCES`.

A smaller shell repro confirmed this was not specific to `t_mtab`:

```bash
echo first > /mnt/brewfs/out
echo second >> /mnt/brewfs/out
```

Before the fix, the append to an existing file failed. After the fix, it produced:

```text
first
second
```

## Root Cause

BrewFS was interacting incorrectly with Linux FUSE writeback cache.

When Linux performs an append write to an existing file through FUSE writeback cache, it may first issue an internal read on the same file handle to fill a partial page before writing the page back.

For an `O_WRONLY | O_APPEND` file handle, BrewFS VFS correctly considered user reads invalid and returned `PermissionDenied`. However, in this case the read was an internal kernel page-cache fill, not a user-level `read(2)`. Returning `EACCES` caused the subsequent user write to fail before it reached the normal FUSE write path.

After fixing that, another writeback-cache issue appeared:

For cached writes, Linux may send a payload that already contains page-cache prefix data. For example:

```text
write #1: offset=0, size=24
write #2: offset=0, size=48
write #3: offset=0, size=72
```

These are page-cache writeback payloads at the supplied offset. They must not be treated as fresh `O_APPEND` writes. Applying append semantics again would duplicate existing prefixes.

## Fix

### 1. Allow kernel page-cache fill reads on write-only handles

In `src/fuse/mod.rs`, when FUSE read is called with a file handle and the VFS returns `PermissionDenied`, BrewFS now falls back to a temporary inode-based read handle:

```rust
match self.read(fh, offset, size as usize).await {
    Ok(data) => data,
    Err(VfsError::PermissionDenied { .. }) => {
        // With writeback cache, the kernel can issue a read on an
        // O_WRONLY fh to fill a partial page before writing it back.
        ...
    }
    Err(err) => return Err(err.into()),
}
```

This keeps normal user `O_WRONLY` read behavior intact while allowing the kernel's writeback-cache path to complete.

### 2. Handle `FUSE_WRITE_CACHE` writes by inode and offset

In `src/fuse/mod.rs`, cached writes are now handled before fh-based writes:

```rust
if write_flags & FUSE_WRITE_CACHE != 0 {
    // Cached writes already contain the page data at the supplied
    // offset; applying O_APPEND again would duplicate the prefix.
    self.write_ino(ino as i64, offset, data).await
} else if fh != 0 {
    self.write(fh, offset, data).await
} else {
    self.write_ino(ino as i64, offset, data).await
}
```

This ensures writeback-cache payloads are written exactly at the offset supplied by the kernel.

### 3. Serialize VFS append writes per inode

In `src/vfs/fs/mod.rs`, append writes now use an inode-level append mutex. This prevents multiple append handles from reading the same fresh file size and writing over each other.

### 4. Add a concurrent append regression test

The append unit test now opens multiple append handles and writes concurrently, then verifies all expected lines are present.

## Validation

The following checks passed:

```text
cargo fmt --check
cargo check -p rfuse3 -p brewfs --tests
cargo test -p brewfs vfs::fs::tests::io_tests::test_fs_append_handles_use_fresh_size_concurrently
```

Full xfstests validation:

```text
generic/089  376s
Passed all 1 tests
```

Passing artifact:

```text
brewfs/docker/compose-xfstests/artifacts/run-1777550125-27631
```

## Impact

This bug affected append writes to existing files when Linux FUSE writeback cache needed to perform a partial-page fill before writeback. It was exposed by `generic/089` because the test runs multiple `t_mtab` processes that append completion lines to a shared output file after a high-concurrency metadata update workload.

The fix is relevant beyond `generic/089`: any small append to an existing file through an `O_WRONLY | O_APPEND` handle could hit the same writeback-cache behavior.
