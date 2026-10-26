# BrewFS local transport patch

Base: crates.io `asyncfuse` 0.1.12, registry checksum
`73944bd789372ebf1f10a25ee59296bbb437b31dbe234dcd959e98a1f3238fac`.
The original MIT license and crate metadata are preserved. Registry/cache files
are not modified; BrewFS uses the checked-in path dependency.

Scope: FUSE getxattr/listxattr reply framing. Worker replies must send the payload
once, not append it to the serialized header and then send it again as a second
segment. Size probes return success; insufficient-buffer errors use negative
ERANGE with header-only error replies. A shared encoder and binary/empty/size/
short-buffer tests cover both worker and serial paths.

The unpatched real-FUSE fixture returns a 6-byte value twice (12 bytes), with
SHA256 `d9d8df9f75dd531ded3dd82734c7fc8a9e0fce6ff2186f24a15981829ee3c3d9`.
Evidence: `docker/compose-xfstests/artifacts/packed-local-20261002T153825Z-1427699/`.
That xattr patch changes no unrelated API, notification ordering, mmap or cache
behavior.

Cache invalidation notifications: repair INVAL_ENTRY framing, add fallible
`try_invalid_entry` / `try_invalid_inode` methods that await the kernel write,
and preserve the legacy enqueue-only API, reply queue ordering, and warning logs.
Fallible notifications use their own queue/connection so they cannot hold up
request replies. See [NOTIFICATIONS.md](NOTIFICATIONS.md) for the original path,
API contract, kernel probe evidence, reproduction commands, and remaining
limitations.
