# Issue #426: packed directory enumeration

Work branch: `fix/426-packed-readdir-regression`, based on `9a048e1`.
Local evidence root: `/mnt/brewfs-data/home/hxy/issue-426-20261105/`.
Reviewable extracts: [`evidence/issue-426-20261105/`](evidence/issue-426-20261105/).

## Attribution from the existing audit

The historical audit is not rerun to establish the regression. PR #409 is
`7afce5c9e55a32cbb612a9c7093a681b5f03d7d2`, whose parent is
`e7135c541a6a3ae3824205200123a55a9afad3d5`. Its fixture and production namespace
builder close sequential inline groups at 224 KiB: 56 rather than 256 files
per group for this 4 KiB corpus, producing 147 rather than 32 groups.

The packed-v3 subtree, fixture producer, object adapter subtree, VFS readdir
implementation/handles and packed-enumeration adapter have identical Git
objects at #409 and audited main `d1160dc`. The intervening commits concern
workspace extent compaction, TTL configuration/docs and test repairs. The TTL
change keeps the same zero setting for this packed-metadata-v3 harness. Thus
there is no intervening packed enumeration/producer change to explain the
group-count and group-read deltas. Object identities and the intervening log
are retained in `attribution-source.json` beside the archived counter extraction.

The enumeration path is `PackedDirectoryPageSource::read_page` →
`AuthenticatedV3Snapshot::readdir_page_with_metadata` → `V3DirectoryMetadata`
→ `V3GroupRef::read_metadata` → `GroupMeta::decode_restart`. Workspace mounts
reach the same lower through `WorkspaceDirectory`, either directly for an
empty upper or in 32-entry merge pages. `packed_list_xattr_bytes_owned` is a
separate xattr operation and is not called by readdir.

Existing `primary-409-e-r*-*/diagnostics/stats-readdir-{before,after}.txt`
establish the mechanism without a new benchmark. The following are counter
deltas over the pure-readdir phase, in pair order:

| Quantity | Parent #409 | Audited main |
| --- | --- | --- |
| GroupMetadata GETs | 94 / 94 / 94 | 147 / 147 / 147 |
| GroupMetadata bytes | 23,889,012 each | 34,196,023 each |
| Validated group fetch spans, ms | 831.319 / 841.056 / 823.818 | 1,518.783 / 1,392.542 / 1,462.674 |
| Backend body spans, ms | 604.796 / 608.387 / 598.221 | 1,179.734 / 1,081.339 / 1,149.759 |
| Group authentication, ms | 188.372 / 193.027 / 188.992 | 288.290 / 266.625 / 264.096 |
| Full GroupMeta decode, ms | 36.699 / 37.735 / 35.449 | 48.166 / 42.893 / 47.048 |
| readdir wall, ms | 929.292 / 945.280 / 917.988 | 1,625.519 / 1,482.779 / 1,601.370 |

The increased serial authenticated group-fetch spans explain most of the wall
increase. Repeated compressed data-frame decoding is not the dominant cause:
these sequential groups carry inline bytes in GroupMeta, and main decodes each
group once. The pre-#409 parent does refetch some of its 32 groups (94 GETs).
The output page budget also charges inline contents that readdir never returns.

This is a layout/enumeration interaction introduced by #409, rather than a
claim that every historical main/parent delta is uniquely attributable to that
PR. Other intervening changes remain part of the original A/B comparisons.
The extraction is saved as `archived-attribution.json` in the evidence root.

## Deterministic reproduction and implementation

`readdir_8192_inline_files_does_not_decode_file_contents` builds 8,192 inline
4 KiB files in 147 groups, enumerates them through 32-entry pages, and checks
every name and inode. A task-local counter at `GroupMeta::decode_restart`
asserts zero full record decodes during pure enumeration. The original code
failed with **147 != 0** (exit 101, 47.51 s test runtime); the binary, source
patch and log are retained in `gates/tdd-red-3*` and `gates/tdd-red-2.patch`.
Earlier compilation/tool setup failures are not the behavioral red result.
The test then reads files across group boundaries and checks their bytes,
ensuring contents remain available on demand.

The candidate projects authenticated GM07 metadata into a distinct directory
type containing only name, inode, kind and mode. It validates namespace and
record bounds while skipping inline bytes and extent records. Full placement
and content decoding remains on lookup/read paths. The wire format and #409
producer layout are unchanged. GM07 interleaves names with inline contents,
so enumeration still fetches and authenticates the entire metadata range;
it does not fetch or decode data frames.
Compressed metadata still requires bounded metadata decompression to expose
its names; file payload/placement records are not materialized. The audited
corpus uses Raw metadata, retained unchanged in the acceptance comparison.

Directory pages fetch at most eight requested groups concurrently (reduced
according to available mount budgets), retain a
bounded names cache and one charged per-handle group, and count only directory
records against the output budget. Exact group/container/generation identities,
epoch fencing and mount memory admission apply to hits and misses. A real read
can populate the existing full GroupMeta cache; later enumeration projects that
cached value without an extra GET. Non-root opendir checks the authenticated
inode index without decoding the parent's file contents.

## Acceptance record

Initial green: the four metadata-cache tests passed (45.23 s), including the
8,192-file regression and demand-read checks. The packed-v3 module passed
381 tests with four existing ignored tests (95.80 s). The final complete
overlay module gate passed **1,265 tests, zero failures, 181 existing ignored
tests** in 184.10 s, including authentication, close/fence, non-root opendir,
wide-page rewind and constrained-budget tests.
The required `cargo test --workspace --lib --bins` gate also passed: the main
library ran 1,325 tests with zero failures and 264 existing ignored tests
(148.23 s); the remaining targets passed two tests and an empty test suite.

The required all-features Clippy command also exposed a pre-existing compile
error: etcd `readdir_page` used the rkyv-bound helper for JSON-only
`EtcdEntryInfo`. That call dates to `7982d1b`, and the same failure was already
recorded in `doc/testing/xfstests-generic-097-remount.md`. The one-line repair
uses `etcd_get_json_serde_only`, as the other EtcdEntryInfo readers do; it does
not change the packed path or serialization format.
Once compilation succeeded, strict Clippy reported eight pre-existing HDFS
test lints (unnecessary mutable arguments and fields reassigned after default
construction). Those test expressions now use shared arguments and direct
initializers, with the same values and assertions.

Matched performance acceptance will use `primary` for three E pairs against
`e7135c5`, and `preservation` for three A–D pairs against the archived main
`d1160dc`. The latter is the direct guard on the benefits retained by this fix.
The harness copy changes only absolute paths; workloads, options, pairing and
statistics remain the archived versions. `oldmain` is an explicitly named
baseline arm using the original binary and fixture producer.
The branch base `9a048e1` differs from `d1160dc` only by the #425 audit
documentation; `src`, `Cargo.toml` and `Cargo.lock` are identical between them.

Self-review checked raw names and restart boundaries, ordered cookies after
rewind, exact cache identities and epoch fencing, per-handle ownership with
the cache disabled, and admission under concurrent memory use. A review finding
reduced directory batch parallelism according to available budget pools; a
40-group test reserves 240 MiB of the 256 MiB metadata pool for another user
and confirms enumeration still succeeds. The review also confirms that full
GroupMeta values are only projected after a real read has populated that cache.

An intermediate full overlay run passed 1,264 tests and failed only a new
test's assertion about the error returned after reader closure. It expected
`ReadViewChanged`, whereas the established API returns
`LimitExceeded("index reader is closed")`. The reader already rejected the
operation correctly; the assertion was corrected and the complete gate rerun.
The failed log remains archived as `gates/test-overlay-final-before-close-assertion.log`.

All local gates passed before the candidate source commit:

- `cargo fmt --all -- --check` and `git diff --check`.
- The seven required shell syntax/report gates from `AGENTS.md`.
- `cargo check --workspace` and `cargo build --workspace`.
- `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime`
  and the corresponding `fuse-io-uring-runtime` check.
- `cargo test --no-default-features --features fuse-tokio-runtime,workspace-overlay --lib workspace_overlay:: -- --test-threads=4`.
- `cargo test --workspace --lib --bins`.
- `cargo clippy --workspace --all-targets -- -D warnings`.
- `cargo clippy --all-targets --all-features -- -D warnings`.

Rust commands use toolchain 1.99.0, `CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0` and
`RUSTFLAGS=-C debuginfo=0`, under `MemoryMax=24G MemorySwapMax=40G`.
All test binaries use the debug profile. The workspace has no `rfuse3` member.
Exact commands, test summaries, log hashes and validated source hashes are
recorded in `evidence/issue-426-20261105/local-gates.json`. GitHub Actions was
not used.

Performance measurements remain pending. Historical numbers above are
attribution evidence, not candidate results; no performance acceptance or
merge is claimed yet.
