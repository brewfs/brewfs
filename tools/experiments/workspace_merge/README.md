# Workspace merge

`workspace merge CHILD TARGET --operation UUID` merges two unmounted `workspace`
catalog views that share an immutable native base and an object bucket. Redis
and TiKV use the same catalog implementation. The library entry point is
`WorkspaceStore::merge_native(MergeRequest)`.

```text
brewfs workspace --meta-backend redis --meta-url redis://127.0.0.1:6379 \
  --workspace-namespace experiment merge CHILD TARGET --operation UUID
```

Persist the operation UUID before submitting. A successful retry returns the
stored JSON result, including after child deletion. A new operation UUID denotes
a new submission. The output contains `committed`, `conflicts`, `scanned_records`,
`applied_records`, and the committed target head and sequence. Conflicts produce
JSON on stdout and a nonzero process exit status.

`scanned_records` counts rows returned by the two private delta scans; base
point lookups are outside this counter. `applied_records` counts copied delta
rows, excluding derived indexes, ownership rows, and the receipt.

The merge reads the two private delta tables and consults base inode rows for
directory attributes and ancestor relationships. It copies directory entries,
whiteouts, inode attributes, extended attributes, ACLs, and ordered data/hole
extents into the target head. Existing target entries outside this change set
remain present. Both branches changing a file report a conflict. Directory
replacement or movement overlapping a descendant change also reports a conflict.
Hard links keep their inode identity and link counts; symlinks keep their target
bytes. Referenced object data is shared in the common bucket.

The final catalog transaction checks both workspace records, both heads, the
base, mount state, and operation receipt. It installs the target rows, ownership
and collection guards, head sequence, and receipt atomically. A rejected
precondition publishes none of this packet. After an interrupted call, use the
same operation UUID to resolve whether it committed. Mount target after the
command succeeds; active mounts are rejected. Different object buckets and
Packed bindings are rejected.

The ordinary mount's collector retains each slice's maximum referenced range
in the KV catalog before removing a layer. This monotonic bound survives mount
restarts and preserves the deletion range when later references cover fewer
bytes. Object deletion still requires the existing catalog reservation.

`merge_posix.py BASE CHILD TARGET` is the ordinary filesystem comparison tool.
It enumerates all trees, compares necessary regular-file content byte by byte,
and checks conflicts before applying changes. Run it on quiesced mounts. It
preserves sparse regions where `SEEK_DATA` and `SEEK_HOLE` are supported, modes,
and symlinks. It rejects hard links and special files. An I/O error during its
apply phase can leave partial changes; it does not provide a multi-file atomic
filesystem transaction.

Focused tests:

```text
cargo test --features workspace --lib native_merge_tests
python3 -m unittest discover -s tools/experiments/workspace_merge -p test_merge_posix.py -v
```

`fuse_check.py` consumes the isolated Redis test namespaces emitted by the
ignored `native_merge_seed_fuse` test. Supply `BREWFS_TEST_REDIS_URL` and
`BREWFS_MERGE_FIXTURES` for that test. The FUSE check requires a fresh `--root`,
the built `--binary`, `--meta-url`, `--fixtures`, and AX's `--ax-runner` path.
It creates separate mount processes and cache directories and retains logs and
JSON results under its root. It uses actual file content and attributes for
verification.

The Linux Redis/local-fs run on 2026-11-15 passed all three scenarios in
[redis-fuse.json](results/redis-fuse.json): an unchanged target, disjoint changes,
and conflicting changes. Successful scenarios used AX's adapter, repeated the
operation before and after child metadata collection, and read the merged files
through a fresh mount process and cache. Checks cover renamed and deleted files,
hard links, symlinks, permissions, nested directories, and sparse-file contents.
The conflict scenario retained the target's files. The focused Rust suite passed
9 tests, AX passed 6 tests, and the POSIX comparison tool passed 5 tests.
Formatting, workspace compilation, and Clippy with the `workspace` feature also
passed. These results cover functional correctness with Redis and local-fs.
