# Compaction correctness: issues #219 and #220

## Root causes and fixes

For #219, heavy compaction uploaded a replacement slice, then deleted its
objects on a generic metadata publication error. A connection reset can lose
the reply after the replacement has committed, so that cleanup deleted live
data. Preserve uploaded objects and any remaining pending record on uncertain
publication errors; retain cleanup for known retry/conflict results and return
the original error.

For #220, readers resolve overlaps in metadata publication order, while both
compaction modes treated larger slice IDs as newer. IDs allocated before
upload can be published out of order. Light compaction now walks the metadata
vector backward to select surviving slices and preserves their relative
order. Heavy compaction merges in forward publication order. Version checks
and delayed deletion remain in place.

## Executed regression evidence

The tests ran against unchanged production code at `origin/main` revision
`a8459dc92142bf94224e4f4af903f592cef5cb13`, before the fixes. A SQLite wrapper
commits the real replacement and then injects ConnectionReset; the two order
cases publish a high-ID old write before a low-ID later overwrite.

```text
heavy_compaction_keeps_published_objects_after_a_lost_commit_reply ... FAILED
lost reply cleanup deleted an already-published object:
Err(incomplete block read for slice 1 block 0: requested 4096 bytes at offset 0, received 0)
heavy_compaction_preserves_publication_order_with_nonmonotonic_ids ... FAILED
heavy compaction resurrected the older, higher-ID data
light_compaction_preserves_publication_order_with_nonmonotonic_ids ... FAILED
light compaction resurrected the older, higher-ID data
test result: FAILED. 8 passed; 3 failed
```

The identical compactor integration suite passed three times after the fixes:

```text
test result: ok. 11 passed; 0 failed; 0 ignored
```

All four commands used:

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
CARGO_PROFILE_TEST_DEBUG=0 cargo test -p brewfs --features workspace-overlay \
  --test compactor_test -- --test-threads=1 --nocapture
```

Before exited 101; each after-run exited 0. The historical before-log filename
`219-220-242-before.log` executes the three regressions for #219/#220 plus eight
existing compactor tests. This change does not address #242.

## Completed validation and remaining scope

All 22 scheduled candidate commands passed, with zero command failures:

| Validation | Result |
| --- | --- |
| Compactor integration suite, three runs | 11 passed / 0 failed / 0 ignored each |
| Focused slice unit suite | 19 passed / 0 failed / 0 ignored |
| Full default workspace libraries and binaries | 1,196 passed / 0 failed / 225 ignored |
| Overlay resolver integration suite | 14 passed / 0 failed / 0 ignored |
| Formatting | passed |
| Default and overlay workspace checks | both passed |
| Default and overlay strict all-target Clippy | both passed with `-D warnings` |
| Workspace build | passed |
| No-default-feature Tokio and io-uring checks | both passed |
| Four shell syntax checks and three shell test scripts | all passed |
| Diff whitespace check | passed |

Every Cargo build/test used two jobs, disabled incremental compilation and
debug information, and ran serially in this task's own target under a 16 GiB
memory limit. No OOM or unrelated failing command occurred. Existing vendored
dependency warnings and the future-compatibility warning remain in the logs.
Ignored tests did not run.

The full monolithic overlay/all-features unit suite and mounted real-backend
suites were not run on this branch. The overlay coverage above is explicitly
the compactor and resolver integration suites, not a full-overlay claim.
Keep the PR draft while that broader validation remains outstanding.

`commands.md` records exact completed commands, environments, exit codes and
test summaries. `validation.tar.gz` preserves complete before/after output and
source patches, including dependency warnings. This follow-up evidence commit
does not modify the tested Rust sources. `target-cleanup.json` records the
inactive private target cleanup; logs, patches and the source worktree remain.
