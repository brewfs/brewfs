Issue #335 makes a sparse 8192-byte file unreadable after truncating it to
8191, 8190, ..., 4095 bytes in one writable head, without a snapshot or seal.
The 4097 adjacent one-byte holes all survive the containment-only compactor
in `f26af31c3fb06e98c0d2009264fd6aab11ecb1d5`. Readers count them before
filtering by the requested range, even though they are beyond the new EOF.

The regression `adjacent_4097_shrinks_remain_readable_after_reopen` executes
that exact sequence through the metadata SETATTR path, then creates a fresh
store and reads the file. On a separate checkout of that main revision, with
only this test added, the result was:

```text
4097 adjacent shrinks: 4097 persisted extent rows
uncached read after 4097 shrinks: Err(InvalidReadPlan("extent delta scan exceeds bounded metadata budget"))
test result: FAILED. 0 passed; 1 failed
```

The baseline test took 192.89 seconds on the shared host. Its backend test
clock is fixed so that a slow scan cannot turn this admission regression into
a lease-expiry test. An initial run with a real 120-second fixture lease
expired before completing the sequence; that run is not the budget evidence.
Production lease checks and reader budgets are unchanged by the fixture.

The first fixed run passed all ten focused compaction tests in 6.24 seconds.
After adding the paged-reader interleaving regression, the final suite passes
all eleven tests:

```text
4097 adjacent shrinks: 1 persisted extent rows
test adjacent_4097_shrinks_remain_readable_after_reopen ... ok
test result: ok. 11 passed; 0 failed
```

The fresh store admits the read and produces one zero segment of length 4095.
The final focused suite took 5.57 seconds, including 74,088 exhaustive three-record
Hole/Data overlap combinations, a 256-operation mixed history, intervening
writes, GC ownership after reopen, CAS conflict/failure, oversized-history
recovery, and rejection beyond the recovery bound. The row reduction does not
depend on a mount cache.

Writable extent mutations now remove a row only when the union of newer
coverage masks its entire range. Adjacent or overlapping holes merge at the
older sequence. A merge is refused when lowering the newer hole would expose
an intervening retained Data row. This preserves data slice identity and
offset, zero coverage, and gaps that must continue to fall through to lower
layers. It does not promote an older mask across a newer write.

Compaction and its admission check run inside the original head CAS, before
the native extent-generation fence. Admission counts the resulting rows,
including incoming rows and all lower-layer rows in the chunk before any
range or EOF filtering. The limits remain 4096 for ordinary reads across
their layer chain and 1024 for packed reads across their two native layers.
The packed reader and mutation admission share the same limit constant.

Ordinary paged extent reads bracket their scan with the existing per-layer
extent generations. A concurrent merge can move a mask behind a page cursor;
the reader must discard those mixed pages and retry. Generation reads use
bounded point requests, and at most three complete scans are attempted. The
page-boundary regression replays a real compaction commit between pages and
checks both the unsafe unfenced result and the correct fenced result.

Recovery on the next extent mutation can scan up to 8192 existing rows and
32 MiB of metadata, in pages of 32 records. At most 4096 incoming rows are
retained. Compaction reserves metadata capacity before retaining scan results
and keeps the permits through the final CAS. A history beyond that recovery
bound, corrupt metadata, or a result that cannot fit the reader limit fails
before publication. Sealed layers are counted but never rewritten.

Fully masked Data rows move their ownership to
`delta/extent-owner/{layer}/{slice}` in the same CAS as their removal. Each
record retains the maximum original slice end and can only grow. These
records participate in the GC census, slice-deletion reservations, the
per-layer extent-generation fence, and both metadata finalizers. Ownership
therefore remains available after reopen, including when multiple old rows
refer to different offsets of the same slice. These ownership records are
GC metadata and do not enter the byte-read history. GC's existing admission
bounds still apply.

The focused Rust regression can be run with:

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/home/hxy/fix335/target \
  timeout -k 10 2700 nice -n 10 cargo test -p brewfs --lib \
  --features workspace-overlay adjacent_4097_shrinks_remain_readable_after_reopen \
  -- --nocapture
```

The standalone syscall reproducer creates and removes its own temporary
directory on an existing workspace mount. The mount's chunk size must be at
least 8192 bytes:

```bash
python3 tests/scripts/repro_adjacent_shrinks.py /mnt/brewfs
```

It performs the same 4097 `ftruncate` calls, closes and reopens the file, and
requires an uncached read to return exactly 4095 zero bytes.

The first full overlay library run reported 2521 passed, five failed, and 451
ignored. Exactly those five tests were then run with identical
`--features workspace-overlay --lib` flags and `--test-threads=1 --nocapture`
on a completely clean `f26af31c` checkout and the branch. After updating the
two assertions affected by the intended storage layout, the comparison was:

```text
before-main: test result: FAILED. 2 passed; 3 failed; 0 ignored; 0 measured; 2961 filtered out; finished in 0.17s
branch:      test result: FAILED. 2 passed; 3 failed; 0 ignored; 0 measured; 2973 filtered out; finished in 0.17s
```

These three failures occur on both checkouts and are pre-existing, tracked by
[issue #337](https://github.com/brewfs/brewfs/issues/337). Their tests and
behavior are outside this fix:

- `g13a_kv_released_lease_is_not_an_expired_recovery_root`
- `kv_v3_open_topology_rejects_binding_change_between_routing_and_final_read`
- `migration_marker_fences_entity_and_hot_native_packets_without_writes`

`group_commit_handoff_preserves_batch_limit_and_order_with_cancelled_waiters`
passes on both. Its branch assertion requires one active final overwrite and
GC ownership of all 129 queued slices, including cancelled waiters, while
retaining the batch-count and byte-winner checks.
`native_metadata_legacy_deleting_identity_is_explicit_and_survives_resume`
also passes on both; the branch expects deletion of the additional ownership
family. These assertions describe the new layout without dropping their
ordering or complete-cleanup checks.

The initial full-library run coincided with another task's perf-owner
benchmarks. Both of the latter tests pass in the isolated five-test run on
this branch. The initial log also contains storage-layout assertion
mismatches, corrected as described above; concurrent load alone does not
explain those assertion changes.

At the user's direction, the local `run-ci.py` driver classifies only the
three named #337 failures as expected-red. It retains the raw Cargo exit
code, full log, log SHA256, and exact failed names in the evidence. A build
error, timeout, resource abort, incomplete failure summary, or additional
failed test remains fatal. Workspace and all-feature test commands include
`--no-fail-fast` so known library failures do not skip subsequent targets.
No tests are marked ignored or skipped by this policy.

Validation stopped at the user's explicit time box. Completed checks include
formatting, script checks, workspace check/build on the earlier candidate,
strict overlay Clippy on the final source, and the final eleven focused
tests. The final full-library rerun and candidate FUSE binary build were
stopped; the remaining CI matrix was not run. During the interrupted library
run, `source_root_pins_real_attributes_and_detects_namespace_changes_and_replacement`
reported a failure. It passed once and then fifty consecutive times in
isolation on both preserved baseline and final candidate test binaries. Its
full-run failure remains recorded, is not classified as expected-red, and
was not investigated further after the time box.

The requested `mainverify` image never appeared. With the user's approval,
the FUSE baseline reused `brewfs-workspace-xfstests:owner-baseline` with a
read-only bind of the clean f26af31c executable; the running container's
binary hash was verified as
`1e8d345978641924000c5fbe053db24325ac8b59be9bda10985bf4b5a7a95c1b`.
No image was built. Baseline `generic/014`, `generic/087`, and `generic/097`
all passed (633, 2, and 2 seconds respectively). The exact syscall reproducer
ran separately as temporary `generic/999` and failed at the reopened
`os.pread(fd, 4095, 0)` with `OSError: [Errno 5] Input/output error`.
The first attempt had skipped that temporary test because it was missing
from xfstests' installed group list; the separate run registered it correctly.
Candidate FUSE tests were not run. The user explicitly removed end-to-end
completion as a merge blocker and requested publication without more builds.
