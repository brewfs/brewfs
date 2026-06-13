# generic/013 S3 Hang Analysis

## Background

`generic/013` previously appeared to hang when running xfstests with the S3 data backend:

```bash
bash docker/compose-xfstests/run_redis_xfstests.sh --s3 --cases "generic/013"
```

At first, the failure looked related to RustFS and small object uploads because the S3 backend was using a streaming AWS SDK body for small vectored writes:

```rust
ByteStream::from_body_0_4(Body::wrap_stream(stream))
```

with `content_length(total_size as i64)`.

## Independent RustFS Check

An ignored diagnostic unit test was added:

```bash
BREWFS_S3_ENDPOINT=http://127.0.0.1:9000 \
BREWFS_S3_BUCKET=brewfs-data \
BREWFS_S3_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
AWS_EC2_METADATA_DISABLED=true \
cargo test -p brewfs --lib rustfs_small_object_streaming_body_compat -- --ignored --nocapture
```

The test directly exercises the S3 adapter against a live RustFS instance. It does not use BrewFS VFS, FUSE, xfstests, Redis, or the writer pipeline.

Result:

```text
test cadapter::s3::tests::rustfs_small_object_streaming_body_compat ... ok
```

This means a simple small-object streaming PUT against RustFS does not reproduce the hang by itself.

## Revised Conclusion

The hang should not be described as a standalone RustFS small-object bug.

The stronger explanation is a BrewFS writer progress bug: a slice can become frozen while the commit thread waits for upload completion, but no upload task is guaranteed to make progress for that slice.

Old hang logs repeatedly stopped here:

```text
brewfs::vfs::io::writer::commit_chunk.wait_upload
in FileWriter.commit_chunk
```

That points at the writer state machine, not at metadata commit or FUSE lookup.

## Likely Root Cause

The relevant pipeline is:

1. `FileWriter::write_at` appends data to a writable slice.
2. `flush()` or auto-flush freezes the slice.
3. A background upload task uploads the slice blocks.
4. `commit_chunk` waits until the front slice is `Uploaded`.
5. Once uploaded, metadata is committed and the slice is popped.

The hang happens when step 4 keeps waiting for upload completion and no upload task makes the frozen slice progress.

The most relevant fix is the commit-thread re-kick logic:

```rust
if runtime.frozen {
    if handle.can_continue_upload() {
        Self::spawn_flush_slice(shared.clone(), slice.clone());
    }
    continue;
}
```

This makes `commit_chunk` self-heal when it observes a frozen slice that still has uploadable data but no active uploader.

## Why The S3 Change Still Helps

Small vectored S3 writes were changed to concatenate into a `Vec<u8>` and use the simpler direct PUT path.

That is still a reasonable defensive simplification:

- small objects do not benefit much from streaming
- direct PUT avoids a more complex AWS SDK body path
- it reduces the surface area involved during flush/upload

But based on the independent RustFS test, this should be considered a mitigation and simplification, not proof that RustFS was the root cause.

## Related Amplifier

The old handle write path flushed synchronously after every write. That forces many small writes through:

```text
write -> freeze -> upload -> commit -> wait
```

For `generic/013 --s3`, this greatly increases the chance of hitting any writer state-machine progress bug.

Removing per-write synchronous flush reduces this pressure. Explicit `flush`, `fsync`, `release`, and truncate paths still need to preserve correctness by flushing when required.

## Current Working Interpretation

The best current explanation is:

- RustFS is reachable and handles direct small-object streaming PUT in isolation.
- The observed xfstests hang is caused by BrewFS writer coordination around frozen slices and upload progress.
- The commit-thread upload re-kick is the most root-cause-aligned fix.
- The small-object contiguous PUT change is a conservative simplification that avoids an unnecessary streaming path.

## Validation Status

Confirmed:

```text
generic/013 --s3 passed
```

Confirmed separately:

```text
rustfs_small_object_streaming_body_compat passed
```

## Follow-up Ideas

Add a focused writer-level regression test that creates a frozen slice with uploadable data and verifies `commit_chunk` re-kicks upload progress instead of waiting forever.

Keep the independent RustFS diagnostic test ignored so it can be run manually when changing S3 adapter upload behavior.
