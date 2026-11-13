# Issue #424: skip unconfigured TiKV tests

The TiKV fixtures now print an explicit `skip:` reason and return before
connecting when their endpoint environment is missing. Configured fixtures
retain their existing assertions, iteration counts, namespaces, and cleanup.
An unavailable configured server still fails the test; connection errors are
not treated as skips.

`src/test_support.rs::tikv_endpoints` reads
`BREWFS_TEST_TIKV_PD_ENDPOINTS`, removes empty comma-separated entries, and
returns `None` for missing, non-Unicode, or empty configuration. The module is
compiled only for library tests. Manager and entity fixtures carry the parsed
endpoints through their helpers. The CLI config writer is called only after
its TiKV entry point validates endpoints.

The original 71-item inventory uses `#[ignore]` source lines containing
uppercase `TIKV`. It includes older tests using `BREWFS_TIKV_PD_ENDPOINTS`,
`BREWFS_META_TIKV_PD_ENDPOINTS`, and `BREWFS_TIKV_TEST_PD_ENDPOINT`; their variable
names and parsing conventions are preserved. One inventory row is a macro that
expands into 19 tests. Shared fixtures also cover additional `real_tikv_*` and
entity tests whose ignore reasons spell `TiKV`. Consequently the number of
runtime tests and the number of edited initialization blocks differ from 71.
The TLS fixture checks its dedicated TLS variables independently.

Rust's stable test harness counts an early return as `ok`/passed, not as
dynamically ignored. The `--nocapture` acceptance logs expose each `skip:` line;
those passes do not claim that a real TiKV server was exercised.

## TDD evidence

The one helper regression test uses fresh child processes for unset, empty,
comma-only, and configured endpoint lists, without mutating the parent test
process environment. It checks actual environment reads and parsing.

First, the helper used the old `std::env::var(...).unwrap()` behavior. Running
the test reproduced `VarError::NotPresent` with exit code 101. After replacing
that behavior with `Option`, the same test passed all four child cases.

```sh
rustc --edition=2024 --test src/test_support.rs -o /tmp/brewfs-424-helper-red
env -u BREWFS_TEST_TIKV_PD_ENDPOINTS /tmp/brewfs-424-helper-red --nocapture
# Apply the Option implementation, then rerun the same test:
rustc --edition=2024 --test src/test_support.rs -o /tmp/brewfs-424-helper-green
env -u BREWFS_TEST_TIKV_PD_ENDPOINTS /tmp/brewfs-424-helper-green --nocapture
```

Logs: [red](evidence/issue-424/helper-red.log),
[green](evidence/issue-424/helper-green.log).

## Local gates

All acceptance builds and tests use dev/test profiles, with
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`.
The first debug test compilation with three build jobs hit the cargo shim's
24 GiB memory cgroup limit; the kernel reported an OOM kill before any test
ran. The retry uses `CARGO_BUILD_JOBS=1` and
`--config profile.test.package.brewfs.codegen-units=512` to limit compiler
memory without changing test assertions. The failed build and kernel evidence
are preserved in
[no-tikv-first-build-oom.log](evidence/issue-424/no-tikv-first-build-oom.log).
The large debug test build scopes were temporarily allowed up to 28 GiB within
the 32 GB host budget. The first successful build peaked at 23.7 GiB; scope
settings and resource evidence are in
[debug-build-memory.log](evidence/issue-424/debug-build-memory.log).
An attempt after the base update also hit the host OOM limit while an unrelated
compiler was running. Its failure is preserved in
[no-tikv-real_tikv-global-oom.log](evidence/issue-424/no-tikv-real_tikv-global-oom.log).
Final acceptance runs avoid simultaneous active compilers; interrupted and
OOM logs are diagnostic evidence, not successful gates.
A further retry failed because a frozen compiler still retained about 8 GiB
while the test compiler needed about 22 GiB. That failed run is recorded in
[no-tikv-real_tikv-frozen-memory-oom.log](evidence/issue-424/no-tikv-real_tikv-frozen-memory-oom.log).
Large test builds must wait for competing compilers to exit and release their
memory; freezing a compiler does not release its resident pages.
No release tests are run. Acceptance is based on local gates, independently of
GitHub Actions.

Earlier strict Clippy failures involved the existing etcd/rkyv reader and HDFS
test lints. Both fixes landed upstream before this change was merged, so neither
is part of the final TiKV diff. Their diagnostic logs remain available as
[the initial compile failure](evidence/issue-424/clippy-all-features-before-fix.log)
and [the HDFS lint failures](evidence/issue-424/clippy-all-features-hdfs-before-fix.log).
The final change consists of 42 Rust files, including the new helper and its
one regression test. Source audit confirmed that all 2,200 existing assertion
bodies, 386 loop headers, and 476 test/ignore attributes are unchanged.

The implementation was squash-merged in [PR #429](https://github.com/brewfs/brewfs/pull/429)
as `cbf3c17677ddb28a3a33f9d0858ace6c692e5aa5`. Its source tree is identical to
the checked branch commit `2728dbef0e045b3163c75f0a5d8739d9c819d97f`.
The final local gate logs below validate that source tree. The expanded
127-case fixture run predates the base update; those fixture sources did not
change in the update. Earlier target and Clippy logs are retained with
`before-main-update` filenames.

The required `real_tikv` selection produces 81 explicit skip messages and
`test result: ok. 81 passed; 0 failed` with PD endpoints unset. The helper also
passes under Cargo (`1 passed; 0 failed`). The strict all-targets, all-features
Clippy command exits 0. Full logs:

- [Explicit real_tikv skips](evidence/issue-424/no-tikv-visible-skips.log)
- [Requested real_tikv selection without output capture changes](evidence/issue-424/no-tikv-real_tikv.log)
- [Full TiKV selection with frozen-base-metadata enabled: 127 skips, zero failures](evidence/issue-424/no-tikv-complete-fixtures.log)
- [Helper under Cargo](evidence/issue-424/helper-cargo.log)
- [Strict Clippy](evidence/issue-424/clippy-all-features.log)
- [Format, shell checks, and diff whitespace](evidence/issue-424/format-shell-diff.log)
- [Source audit](evidence/issue-424/source-audit.log)
- [Changed Rust file manifest](evidence/issue-424/changed-rust-files.txt)
- [Self-review](evidence/issue-424/self-review.log)
- [Coverage of all 71 inventory entries](evidence/issue-424/inventory-coverage.log)

The final sequential matrix completed with exit code 0 for every command:

- `cargo fmt --all -- --check`: pass, 32.15 s ([fmt-final](evidence/issue-424/fmt-final.log)).
- Strict `cargo clippy --all-targets --all-features -- -D warnings`: pass, 332.55 s ([clippy-all-features](evidence/issue-424/clippy-all-features.log)).
- Requested no-environment selection: 81 passed, 0 failed ([no-tikv-real_tikv](evidence/issue-424/no-tikv-real_tikv.log)).
- Visible skip selection: 81 passed, 0 failed, with explicit `skip:` output ([no-tikv-visible-skips](evidence/issue-424/no-tikv-visible-skips.log)).
- Helper regression: 1 passed, 0 failed ([helper-cargo](evidence/issue-424/helper-cargo.log)).
- `cargo check --workspace`: pass ([check-workspace](evidence/issue-424/check-workspace.log)).
- `cargo check -p brewfs` with tokio and io-uring runtimes: both pass ([tokio](evidence/issue-424/check-fuse-tokio.log), [io-uring](evidence/issue-424/check-fuse-io-uring.log)).
- `cargo test --workspace --lib --bins`: 1,326 passed, 0 failed, 264 ignored, plus the two binary targets ([test-workspace](evidence/issue-424/test-workspace.log)).
- `cargo clippy --workspace --all-targets -- -D warnings`: pass ([clippy-workspace](evidence/issue-424/clippy-workspace.log)).

The default-feature integration smoke selection also passed both tests with
explicit skips ([no-tikv-integration](evidence/issue-424/no-tikv-integration.log)).
The expanded frozen-base-metadata fixture selection passed 127 tests with
explicit skips ([no-tikv-complete-fixtures](evidence/issue-424/no-tikv-complete-fixtures.log)).

Real TiKV regression tests were not run in this task because no isolated TiKV
test cluster was configured. The 8/8 xattr result reported on issue #424 is
prior evidence, not a new execution against this change.

## Original 71-item inventory

Source locations below refer to baseline `9a048e1`. Every row is covered by an
explicit missing-configuration guard, either directly or in its shared fixture.

| # | Baseline source | Test or macro | Guard |
| --- | --- | --- | --- |
| 1 | `src/manager/backend_tests.rs:885` | `real_tikv_manager_lifecycle_recovery_and_ownership` | tikv_endpoints + 用例入口 guard |
| 2 | `src/manager/native_backend_tests.rs:429` | `real_tikv_manager_native_p1_nonempty_fork_runtime` | tikv_endpoints + 用例入口 guard |
| 3 | `src/meta/stores/tikv/tests.rs:337` | `tikv_factory_initializes_root` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 4 | `src/meta/stores/tikv/tests.rs:356` | `directory_pages_follow_namespace_mutations` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 5 | `src/meta/stores/tikv/tests.rs:365` | `tikv_transactional_namespace_schema` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 6 | `src/meta/stores/tikv/tests.rs:417` | `tikv_rejects_non_empty_rmdir` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 7 | `src/meta/stores/tikv/tests.rs:440` | `tikv_chunk_discovery_visits_later_keys_across_clones_and_wraps` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 8 | `src/meta/stores/tikv/tests.rs:479` | `tikv_transactional_file_data_schema` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 9 | `src/meta/stores/tikv/tests.rs:541` | `tikv_serializes_hot_inode_and_parent_transactions` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 10 | `src/meta/stores/tikv/tests.rs:595` | `tikv_compaction_gc_workflow` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 11 | `src/meta/stores/tikv/tests.rs:663` | `tikv_allocates_generic_counters_transactionally` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 12 | `src/meta/stores/tikv/tests.rs:675` | `tikv_rename_overwrites_file_and_tombstones_destination` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 13 | `src/meta/stores/tikv/tests.rs:698` | `tikv_rename_exchange_swaps_entries` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 14 | `src/meta/stores/tikv/tests.rs:733` | `tikv_rename_transactions_prevent_concurrent_parent_cycle` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 15 | `src/meta/stores/tikv/tests.rs:783` | `tikv_link_and_symlink_extensions` | integration_config → Option + 用例入口 guard；保留 BREWFS_META_TIKV_PD_ENDPOINTS fallback |
| 16 | `src/native_base/write/tests_tikv.rs:19` | `$name` | BREWFS_TIKV_PD_ENDPOINTS 入口 guard（宏覆盖 19 个展开用例） |
| 17 | `src/native_base/write/tests_tikv.rs:60` | `independent_stores_racing_same_head_commit_once` | BREWFS_TIKV_PD_ENDPOINTS 入口 guard |
| 18 | `src/native_base/write/tikv.rs:252` | `real_tikv_native_deadline_and_owner_are_checked_with_the_write` | tikv_endpoints + 用例入口 guard |
| 19 | `src/workspace/entity/backend_tests.rs:9` | `tikv_entity_backend_and_allocator_conformance` | tikv_endpoints + 用例入口 guard |
| 20 | `src/workspace/entity/tikv_mount_tests.rs:95` | `tikv_entity_both_lowers_keep_cow_and_writeback_contract` | tikv_endpoints + 用例入口 guard |
| 21 | `src/workspace/entity/tikv_tests.rs:263` | `tikv_entity_waiting_mutation_retains_its_ordered_lock_prefix` | tikv_endpoints + 用例入口 guard |
| 22 | `src/workspace/entity/tikv_tests.rs:361` | `tikv_entity_allocation_retains_counter_until_object_reservation` | tikv_endpoints + 用例入口 guard |
| 23 | `src/workspace/stores/binding_tests.rs:1871` | `public_real_tikv_binding_contract` | tikv_endpoints + 用例入口 guard |
| 24 | `src/workspace/stores/binding_tests/real_open_tests.rs:217` | `public_real_tikv_snapshot_open_contract` | tikv_endpoints + 用例入口 guard |
| 25 | `src/workspace/stores/binding_tests/real_publication_tests.rs:143` | `public_real_tikv_g12_same_head_publication_retries_exactly` | 共享 real_tikv_publication_contract guard |
| 26 | `src/workspace/stores/binding_tests/real_publication_tests.rs:149` | `public_real_tikv_g12_inode_expansion_reserves_unissued_range_and_retries` | 共享 real_tikv_publication_contract guard |
| 27 | `src/workspace/stores/binding_tests/real_publication_tests.rs:155` | `public_real_tikv_g12_inode_expansion_rejects_issued_ids` | 共享 real_tikv_publication_contract guard |
| 28 | `src/workspace/stores/clock_cas_tests.rs:68` | `real_tikv_clock_cas_rejects_expired_deadline_without_writes` | tikv_endpoints + 用例入口 guard |
| 29 | `src/workspace/stores/g13_real_backend_tests.rs:898` | `g13_real_tikv_destructive_revalidation_contract` | tikv_endpoints + 用例入口 guard |
| 30 | `src/workspace/stores/kv_store.rs:8768` | `tikv_backend_passes_distributed_catalog_contract` | tikv_endpoints + 用例入口 guard |
| 31 | `src/workspace/stores/kv_store/packed_admin/packed_initial_snapshot_tests.rs:676` | `real_tikv_initial_root_birth_has_complete_native_reverse_authority` | tikv_endpoints + 用例入口 guard |
| 32 | `src/workspace/stores/kv_store/packed_admin/packed_initial_snapshot_tests.rs:708` | `real_tikv_initial_composer_snapshot_reopen_carrier_fork_and_actual_root_read` | tikv_endpoints + 用例入口 guard |
| 33 | `src/workspace/stores/kv_store/packed_admin_tests.rs:182` | `real_tikv_packed_clean_source_released_without_original_drain_receipt` | 共享 tikv guard |
| 34 | `src/workspace/stores/kv_store/packed_admin_tests.rs:194` | `real_tikv_packed_clean_source_expired_without_original_drain_receipt` | 共享 tikv guard |
| 35 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/carrier_basis_tests.rs:364` | `real_tikv_carrier_basis_final_cas_false_has_no_partial_write_or_second_final` | 共享 complete_driver/tests.rs::tikv guard |
| 36 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/carrier_basis_tests.rs:376` | `real_tikv_carrier_basis_counterfactual_native_revision_rejected_before_final` | 共享 complete_driver/tests.rs::tikv guard |
| 37 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/fork_alias_tests.rs:1384` | `real_tikv_native_carrier_fork_alias_siblings_pins_journals_and_retirement` | 共享 complete_driver/tests.rs::tikv guard |
| 38 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/history_retirement_tests.rs:389` | `real_tikv_authenticated_history_grace_reader_drain_membership_and_delete` | 共享 complete_driver/tests.rs::tikv guard |
| 39 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/native_lease_reaper_tests.rs:448` | `real_tikv_authenticated_native_lease_grace_releases_only_lease_hold` | 共享 lease_tikv guard |
| 40 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/native_lease_reaper_tests.rs:460` | `real_tikv_authenticated_native_lease_prepare_before_pnb_retains_hold` | 共享 lease_tikv guard |
| 41 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/prepare_tests.rs:545` | `real_tikv_prepare_original_lease_fresh_store_first_pnb_factory` | 共享 tikv guard |
| 42 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/prepare_tests.rs:555` | `real_tikv_prepare_expired_lease_unique_owner_first_pnb_factory` | 共享 tikv guard |
| 43 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/process_restart_tests.rs:1451` | `real_tikv_process_death_prepare_then_independent_recovery` | 共享 tikv_case guard |
| 44 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/process_restart_tests.rs:1461` | `real_tikv_process_death_quiesced_then_independent_recovery` | 共享 tikv_case guard |
| 45 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/process_restart_tests.rs:1471` | `real_tikv_process_death_pnb_then_independent_recovery` | 共享 tikv_case guard |
| 46 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/process_restart_tests.rs:1482` | `real_tikv_process_death_prepare_with_persistent_upper_payload` | 共享 tikv_case guard |
| 47 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/recovery_tests.rs:1182` | `real_tikv_recovery_expired_lease_new_store_typed_capture_hash_factory` | 共享 tikv_recovery guard |
| 48 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/recovery_tests.rs:1192` | `real_tikv_recovery_lost_claim_reply_exact_same_incarnation` | 共享 tikv_recovery guard |
| 49 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/recovery_tests.rs:1202` | `real_tikv_recovery_cancel_claim_waiter_keeps_held_actual_cas_owner` | 共享 tikv_recovery guard |
| 50 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/recovery_tests.rs:1212` | `real_tikv_recovery_lost_final_reply_confirms_new_lease_open_claim_successors` | 共享 tikv_recovery guard |
| 51 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/recovery_tests.rs:1222` | `real_tikv_recovery_cancel_final_waiter_keeps_fresh_source_and_graph` | 共享 tikv_recovery guard |
| 52 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/recovery_tests.rs:1232` | `real_tikv_recovery_held_final_rejects_actual_open_owner_expiry` | 共享 tikv_recovery guard |
| 53 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/tests.rs:1083` | `real_tikv_native_factory_atomic_carrier_head_pwb_ppj_registry` | 共享 tikv guard |
| 54 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/tests.rs:1093` | `real_tikv_native_factory_lost_reply_exact_confirmation_without_reissue` | 共享 tikv guard |
| 55 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/tests.rs:1103` | `real_tikv_native_factory_cancelled_waiter_keeps_owned_final_driver` | 共享 tikv guard |
| 56 | `src/workspace/stores/kv_store/packed_journal/native_publication/complete_driver/tests.rs:1113` | `real_tikv_native_factory_unknown_before_cannot_resubmit_packet` | 共享 tikv guard |
| 57 | `src/workspace/stores/kv_store/packed_journal/registry/initial_bootstrap_tests.rs:979` | `real_tikv_initial_packed_bootstrap_source_staging_unknown_reopen_and_quarantine` | tikv_endpoints + 用例入口 guard |
| 58 | `src/workspace/stores/kv_store/packed_journal/registry/initial_history_real_tests.rs:611` | `real_tikv_authenticated_initial_history_final_delete_and_native_gc` | tikv_endpoints + 用例入口 guard |
| 59 | `src/workspace/stores/kv_store/packed_journal_tests.rs:1301` | `real_tikv_packed_journal_reopen_staging_and_abort` | tikv_endpoints + 用例入口 guard |
| 60 | `src/workspace/stores/kv_store/packed_reader_pins/packed_reader_pin_tests.rs:1316` | `real_tikv_packed_reader_pin_reopen_and_crashed_reader_expiry` | tikv_endpoints + 用例入口 guard |
| 61 | `src/workspace/stores/manager_backend_tests.rs:190` | `real_tikv_manager_ownership_and_empty_root_contract` | tikv_endpoints + 用例入口 guard |
| 62 | `src/workspace/stores/manager_backend_tests.rs:355` | `real_tikv_manager_bootstrap_rechecks_native_locator_atomically` | tikv_endpoints + 用例入口 guard |
| 63 | `src/workspace/stores/tikv.rs:1713` | `real_tikv_bounded_xattr_reads_preserve_64k_value_and_shutdown_owners` | tikv_endpoints + 用例入口 guard |
| 64 | `src/workspace/stores/tikv.rs:1893` | `real_tikv_bounded_values_use_hard_cap_for_corrupt_oversized_point_and_scan` | tikv_endpoints + 用例入口 guard |
| 65 | `src/workspace/stores/tikv.rs:2033` | `real_tikv_bounded_keyset_pages_complete_large_prefix` | tikv_endpoints + 用例入口 guard |
| 66 | `src/workspace/stores/tikv.rs:2051` | `real_tikv_clock_window_fences_reap_and_advances_generation_atomically` | tikv_endpoints + 用例入口 guard |
| 67 | `src/workspace/stores/tikv_commit_failure_tests.rs:561` | `real_tikv_commit_failure_does_not_replay_as_mismatch` | tikv_endpoints + 用例入口 guard |
| 68 | `src/workspace/stores/tikv_manager_bootstrap_tests.rs:60` | `real_tikv_manager_bootstrap_observes_writer_committed_after_its_start` | tikv_endpoints + 用例入口 guard |
| 69 | `src/workspace/stores/tikv_secondary_join_tests.rs:5` | `real_tikv_secondary_join_multikey_cas_allows_immediate_bounded_point_and_scan` | tikv_endpoints + 用例入口 guard |
| 70 | `tests/compaction_orphan_claim_test.rs:210` | `tikv_compaction_gc_claim_fences_publication` | 保留原环境变量名，入口 guard |
| 71 | `tests/getlk_owner_regression.rs:204` | `tikv_getlk_ignores_own_lock` | 保留原环境变量名，入口 guard |
