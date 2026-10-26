# Cache invalidation notifications

`Notify::try_invalid_entry(parent, &OsStr).await` and
`Notify::try_invalid_inode(inode, offset, len).await` return `std::io::Result<()>`
after the complete notification has been written to `/dev/fuse`. Linux processes
these invalidations synchronously in that write; the result is the kernel's
acceptance or errno, rather than an acknowledgement of enqueueing. `ENOENT` is
preserved when the kernel cannot find the parent, entry, or inode. A stopped
writer returns `BrokenPipe`; a short write returns `WriteZero`.

The existing consuming `invalid_entry(parent, OsString)` and
`invalid_inode(inode, offset, len)` async methods retain their `()` result and
enqueue-only completion. They share the encoder with the new methods, logging
enqueue and device-write failures at `warn`. Legacy calls retain their original
response queue, writer, and ordering with request replies. Existing inode payload
bytes and offset/length semantics are unchanged.

Fallible notifications have a dedicated queue and cloned connection, so a kernel
invalidation waiting on a filesystem operation does not block that operation's
reply writer. There is no ordering guarantee between **fallible** notifications
and request replies. Send invalidations outside related request handlers and
without holding locks they need. Canceling a fallible future after enqueue does
not retract its notification. Success does not prevent a concurrent lookup from
repopulating the cache. The fallible notification queue closes when session
dispatch ends.

Entry names accept arbitrary Unix bytes, including non-UTF-8. Empty names, NUL,
slash, and names longer than `NAME_MAX` return `InvalidInput` before enqueueing.

## Wire format and original notification path

The baseline already contained an INVAL_ENTRY arm, but it omitted the trailing
name byte and declared only the fixed payload length. The ordinary reply writer
repaired the length; the readonly reply tracker rejected it before repair. The
missing trailing byte caused Linux to reject it with `EINVAL`, which the old
mutable reply writer retried indefinitely.

Exact locations at baseline commit
`b21654e13b1ae1716e7e6d842f414eb4fe09bacd` (paths relative to this directory):

| Component | Original location |
| --- | --- |
| Public notifier and sender | `src/notify.rs:25`, `src/notify.rs:26` |
| Enqueue | `src/notify.rs:212` |
| Discarded inode / entry enqueue errors | `src/notify.rs:222`, `src/notify.rs:229` |
| Existing entry encoder | `src/notify.rs:86` |
| Notification codes / entry ABI payload | `src/raw/abi.rs:460`, `src/raw/abi.rs:1029` |
| Shared response queue fields / creation | `src/raw/session/mod.rs:673`, `src/raw/session/mod.rs:736` |
| Reply writer / awaited device write | `src/raw/session/mod.rs:1232`, `src/raw/session/mod.rs:1317` |
| ENOENT treated as a forgotten reply | `src/raw/session/mod.rs:1339` |
| Other mutable write errors retried indefinitely | `src/raw/session/mod.rs:1353` |
| Linux Tokio `writev` syscalls | `src/raw/connection/tokio.rs:703`, `src/raw/connection/tokio.rs:716` |
| io_uring write / completion | `src/raw/connection/io_uring_conn.rs:480`, `src/raw/connection/io_uring_conn.rs:509` |

The corrected packet has `unique=0`, notification code 3 in `error`, a fixed
`{ parent: u64, namelen: u32, flags: u32 = 0 }` payload, name bytes, and a trailing
NUL. `len` includes every byte; `namelen` excludes the trailing NUL. This uses the
existing ABI's `_padding` field as zero flags.

On tested Linux `6.8.0-36-generic` x86_64, **both the byte count and the extra
trailing byte are required**. Linux v6.8
[`fs/fuse/dev.c`](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dev.c#L1467)
checks `size == sizeof(outarg) + namelen + 1` at line 1492, copies that many name
bytes, then overwrites `buf[namelen]` with zero at line 1501. Lookup uses a
length-counted `qstr`; a bare `namelen`-byte payload is insufficient. We send an
actual NUL for compatibility.

## Reproducing the mounted probe

From the repository root, with `/dev/fuse` and mount permission/fusermount3:

```bash
export CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0
cargo +1.98.1 run -j 4 --manifest-path vendor/asyncfuse/Cargo.toml \
  --example notify_invalidation -- --mountpoint /tmp/asyncfuse-notify
cargo +1.98.1 run -j 4 --manifest-path vendor/asyncfuse/Cargo.toml \
  --features io-uring-runtime --example notify_invalidation -- \
  --mountpoint /tmp/asyncfuse-notify --workers 2
```

The mountpoint must not exist. The probe supplies two files, lists the directory,
populates a one-hour entry cache, confirms five stats issue no new LOOKUP, awaits
invalidation, then requires exactly one new LOOKUP. It also checks inode
attribute invalidation, real kernel errors, and errors after unmount. It unmounts
on verification failure. READDIRPLUS uses a zero entry TTL so the initial stat
must populate the entry through LOOKUP. A missing mount capability is an error,
not a skipped success.

## Observed kernel results (2026-10-26)

Kernel: `Linux 6.8.0-36-generic #36-Ubuntu SMP PREEMPT_DYNAMIC`, x86_64.
Compiler: `rustc 1.98.1 (48a229cea 2026-09-01)`. All builds used four jobs and
`CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0`.

| Runtime | Request workers | Result |
| --- | --- | --- |
| Tokio | 0 (default dispatch) | PASS |
| Tokio | 2 | PASS |
| io_uring | 0 (default dispatch) | PASS |
| io_uring | 2 | PASS |

All four runs produced the same counts and errors and unmounted cleanly. Full
output from the io_uring worker run:

```text
mounted /data/asyncfuse/probe-uring-workers; workers=2
directory populated; entry/attribute TTL=3600s
INVAL_ENTRY "cached.txt": LOOKUP populated=1, five cached stats=1, after acknowledged invalidation=2; PASS
INVAL_INODE "cached.txt": GETATTR before=0, after=1; PASS
INVAL_ENTRY "cached-\xFF": LOOKUP populated=1, five cached stats=1, after acknowledged invalidation=2; PASS
INVAL_INODE "cached-\xFF": GETATTR before=0, after=1; PASS
INVAL_ENTRY unknown parent: observed kernel errno 2 (No such file or directory (os error 2))
INVAL_INODE unknown inode: observed kernel errno 2 (No such file or directory (os error 2))
INVAL_ENTRY after unmount: BrokenPipe; PASS
INVAL_INODE after unmount: BrokenPipe; PASS
unmounted; kernel invalidation probe PASS
```

A negative control in an isolated copy removed only `data.push(0)` from the
entry encoder and reduced the declared length by one to match the actual packet.
The same probe mounted and populated the cache, then the first invalidation
returned kernel `EINVAL` (exit status 1), followed by clean unmount:

```text
mounted /data/asyncfuse/probe-no-nul-control; workers=0
directory populated; entry/attribute TTL=3600s
Error: Os { code: 22, kind: InvalidInput, message: "Invalid argument" }
```

This distinguishes a valid NUL-terminated packet from an otherwise correctly
length-counted packet without the extra byte. The production code was never
modified for this control.

## Other validation

- Vendored Tokio unit suite: 56 passed. io_uring suite: 95 passed, 15 existing
  ignored tests. Tests cover wire bytes, delayed write acknowledgement, original
  errno, disconnected/canceled callers, invalid names, and legacy completion and
  queue ordering.
- The async-io library builds; its mounted behavior is not tested.
- Workspace check/build, both BrewFS runtime feature checks, strict library
  Clippy (`--workspace --lib -- -D warnings`), ordinary workspace Clippy, Rust
  formatting, required shell checks, and whitespace checks pass. Builds use
  Rust 1.98.1, four jobs, and debug info disabled.
- The complete final workspace library test executable passed with
  `--test-threads=4`: **1,258 passed, 262 ignored, 0 failed** (73.09 seconds).
  Reproduction: `cargo test --workspace --lib -j 4 -- --test-threads=4`.
  At this host's default concurrency, two unchanged writer tests failed:
  `test_auto_flush_defers_cached_partial_tail_after_full_block_dispatch` and
  `test_upload_before_commit_does_not_publish_uploaded_prefix_by_default`.
  Both use wall-clock delays, passed in isolation, and passed within the complete
  four-thread run; no tests were omitted. An earlier default-concurrency full run
  also passed. The failed parallel run is retained as a timing-related limitation.
- `cargo test --workspace --lib --bins` is blocked by the existing binary-test
  error at `src/cli_runtime.rs:19`: `expected.parse().unwrap()` has an ambiguous
  type (E0283/E0284). This source is unchanged; CI already references issue #342
  for the binary-test target.

## Scope and remaining limitations

- BrewFS create/mkdir/mknod/symlink/link/unlink/rmdir/rename call sites are not
  changed. Callers must use the notifier; this patch does not automatically send
  invalidations for filesystem mutations.
- The old methods still do not return failures; use the new fallible methods for
  acknowledged completion. Their writer's existing retry behavior is preserved.
  Poll wakeup, STORE, RETRIEVE, and DELETE have no new fallible public API.
  DELETE's existing missing name terminator is not fixed.
- Only Linux is verified at the kernel boundary. FreeBSD and macOS behavior is
  unverified. Entry invalidation requires kernel protocol support (introduced
  with FUSE 7.12); unsupported kernel errors are returned.
- The probe verifies positive directory entries and inode attributes, not
  negative dentries, mmap, inode page-cache data invalidation, or concurrent
  namespace mutation ordering. Existing inode-cache limitations remain.
