# Packed 005 cold attributes / hardlink validation (2026-10-03)

## Status and scope

This is correctness work, not a new accepted performance result or full spec
completion. It follows the PM07/GC05, direct VFS provider and mounted-read work in
`packed-v3-completion-correctness-2026-10-02.md`. HEAD remains `a429b0e`; the current
completion code is uncommitted. Default fixtures remain legacy 004. No cloud
resources, new JuiceFS campaign or README comparison row were created.

## Direct unified VFS checkpoint

005 now uses `WorkspaceReadPlanProvider::prepare_unified_read` and a prepared
`UnifiedReadPlan` + authenticated source fetcher. The VFS calls the existing
`execute_unified_into`; it does not fall back to `get_slices`/synthetic slice ids.
004/native providers retain their existing compatibility path. A regression
rejects any legacy-plan call and checks holes, inline bytes, generation failure
and successful-delivery-only counters. This does not yet complete the workspace
mutable-generation retry/binding path or mount-wide retained-byte budget.

Real FUSE evidence:
`docker/compose-xfstests/artifacts/packed-local-20261002T140937Z-1386633/`.
1,000 files / 102,400,000 payload bytes / checksum 124,948 / errors=0; backend
failures=0; 11,357 runtime range calls and 5,451,000 actual consumed bytes.
Cleanup exit 0. These are compressible zstd/debug-local diagnostics, not paired
performance acceptance. The complete gate before cold/hardlink additions is
`packed-v3-completion-20261002-unified-gate/`: workspace lib 1,015 passed/225
ignored; brewfs bin 1,099 passed/225 ignored; packed 99 passed; reader 12 passed;
overlay 256 passed/2 ignored. Required fmt/scripts/check/build/runtime/clippy steps
passed. It cannot validate later code by inheritance.

## CA05 publication and read paths

- Independent bounded `BRFCA005`/CA05 object with inode identity, raw symlink
  target, canonical ordered xattrs and ACL rules. Object refs are reached from
  the authenticated PM07 cold-index root; substituted objects, wrong inode,
  invalid target/name/count/value lengths, duplicate keys or invalid rwx rules
  fail closed.
- Producer checks symlink kind/target size against the inode before upload and
  validates symlink cold closure before manifest creation. Repeated identical
  cold attributes are idempotent; conflicting attributes for a shared inode
  poison publication.
- Cold objects load only for explicit `readlink/getxattr/listxattr/get_acl`
  requests, not for ordinary file reads. A byte-safe MetaLayer/FUSE readlink
  path preserves non-UTF8 targets; existing string API explicitly rejects a
  non-UTF8 target rather than lossy conversion.
- ACL rule round-trip and readonly adapter lookup are tested. This **does not**
  implement Linux POSIX-ACL xattr mode synchronization/inheritance: those xattrs
  remain explicitly unsupported by the existing FUSE policy. Full POSIX ACL
  application/inventory remains an independent incomplete spec item.

CA05 TDD logs: `packed-v3-completion-20261002-cold/red.log` (one behavioral
failure) and `green.log` (2 pass). Published adapter regression verifies raw
readlink, binary xattr, listxattr, ACL retrieval, wrong inode rejection and EROFS.

## FUSE dependency framing defect and fix

The initial real mount returned the 6-byte fixture xattr twice (12 bytes). Its
hash exactly equals SHA256(value || value). Worker `asyncfuse` 0.1.12 appended
payload to its header buffer and sent the same payload as a second segment.
The inspected cached 0.1.14 handler has the same defect, so a version bump alone
would not fix it. A worker size probe also returned positive ERANGE incorrectly.

The exact published 0.1.12 source is vendored under `vendor/asyncfuse/`, with its
MIT license and upstream metadata preserved. A checked-in path dependency makes
the fix reproducible; no Cargo registry/cache source was edited. Only worker/
serial xattr framing and the shared encoder were changed. Data is sent once;
size probes succeed; short buffers produce header-only **negative** ERANGE.
Binary/empty payload, size probe and short-buffer tests reproduce three failures
then pass. See `vendor/asyncfuse/PATCHES.md` and
`packed-v3-completion-20261002-cold/transport-{red-framing,green}.log`.

Readonly mutation errors now propagate EROFS. The diagnostic cold file uses
0666 to ensure the request reaches readonly handling instead of failing DAC
first. The ordinary corpus's permissions are unchanged. An earlier mount
failed this diagnostic with EACCES; it remains preserved, not renamed a pass.

Successful cold FUSE evidence:
`docker/compose-xfstests/artifacts/packed-local-20261002T161413Z-1492381/`.
Raw readlink bytes, get/list xattr and EROFS with unchanged value pass; the
subsequent 100-file scan validates 10,240,000 bytes, checksum 5,050, errors=0;
cleanup exit 0. Prior failed framing/DAC artifacts remain preserved.

## Hardlinks and reverse names

- Producer's disk spool records explicit inode identity, shared hot attributes,
  canonical logical data runs and SHA-256 content. It never deduplicates distinct
  inodes because their content happens to match. Conflicting content/hot fields,
  repeated directory inodes, excess links or missing visible links reject the
  snapshot. Sparse run boundaries are part of the current identity contract;
  different physical frame splits/codec/inline admission are not.
- Exactly one deterministic master locator (parent DirKey/raw name order) is
  published in the inode index. Each dentry retains its own independently
  authenticated placement. Across groups/containers, links may repeat immutable
  placement but must read the same logical content. Per-inode cold refs are shared.
- Authenticated reverse keys are `(inode_be, parent_inode_be, raw_name)`. Readers
  validate key/value and hot attributes against the canonical inode. Bounded
  reverse-page cursors support large link sets; compatibility `get_names` and
  `get_paths` return all links within 4,096 names / 256 KiB output limits and
  explicitly fail rather than silently truncate larger sets. Ancestor depth is
  bounded and missing ancestors/cycles fail closed.
- Tests cover cross-group links, content/attribute mismatch, paged reverse
  continuation, both paths, inline-vs-frame admission, distinct inodes with
  identical content and incomplete nlink/symlink cold closure. An initial
  inline/frame test accidentally used overlapping group name fences and was
  correctly rejected; its fixed disjoint corpus passes without relaxing routing.

Hardlink TDD: `packed-v3-completion-20261003-hardlinks/red.log` fails the old
duplicate-inode handling; `green.log` and `reverse-tests.log` pass. The complete
producer regression `producer-all-fixed.log` reports 6 passing tests.

Successful combined actual FUSE evidence:
`docker/compose-xfstests/artifacts/packed-local-20261003T021043Z-1555949/`.
Root `.hardlink-a` and `d000/.hardlink-b` have the same inode and nlink=2,
contents equal; cold readlink/xattr/listxattr/EROFS also pass. The ordinary
100-file scan validates 10,240,000 bytes, checksum 5,050, errors=0. Cleanup exit 0.
These hidden diagnostics are intentionally separate from the performance corpus.

## Latest validity gate and remaining task

Latest full gate logs/status:
`docker/compose-xfstests/artifacts/packed-v3-completion-20261003-cold-hardlink-gate/`.
The gate completed successfully. fmt, required scripts/reports, scanner/TTL/
local-runner tests, workspace check/build, both FUSE runtime checks with/without
overlay, default and overlay clippy, and diff check pass. Workspace lib:
1,015 passed/225 ignored; brewfs bin: 1,099 passed/225 ignored; packed:
105 passed; reader: 12 passed; overlay: 262 passed/2 ignored. Vendored library:
13 passed, with tokio/io-uring/async-io runtime checks all passing. The same
vendored transport tests/runtime checks and new runner checks are added to CI.
Existing warnings are not an all-feature/operator/`-D warnings` CI pass.

Still required for the entire spec/task: production source inventory and real
SEEK_DATA/SEEK_HOLE capture, external large-file placements, source/root POSIX
metadata and consistency policy, complete memory/traffic accounting and shared
budgets, mutable workspace packed binding/generation retry/seal/head-CAS/recovery/
GC, same-executor ablations and bounded matched release/cloud performance gates.
Cold/hardlink fixture tests are not substitutes for those missing integrations.
Keep user files, credentials, `.claude/` and referenced artifacts intact.
