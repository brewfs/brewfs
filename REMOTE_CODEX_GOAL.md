# Remote Codex Goal: BrewFS Packed Metadata Read-Only Optimization

Work in `/home/hxy/brewfs` on branch `codex/packed-metadata-aliyun-20260930`.
The checkout was migrated from the validated local branch; do not reset or
discard existing work.  The personal GitHub remote is `origin` and the public
repository is `upstream`.

## Current user scope (2026-10-07)

Complete the required SPEC implementation and small correctness gates before
planning performance experiments. Packed metadata supports packed only;
snapshot is its encoding identifier. Public modules, APIs, metrics and budget
configuration use V3/v3, and old 004/v1/v2 compatibility is not required.
Prioritize Redis/TiKV metadata publication, recovery, reader retention and GC.
The local SQLite scratch inventory is disposable graph-validation storage.
These instructions supersede older goal text that requested compatibility.

## Objective

Make the v3 read-only packed-metadata path measurably faster than the current
JuiceFS reference for the intended workload: large namespaces of 100 KiB to
1 MiB files, sequential and random scans, cold local page/object caches, and
OSS metadata/data fetched into the local filesystem before file reads.

Start from the existing v3 implementation and its design spec.  Preserve the
three integrated ideas: readonly-packed metadata, dynamic data blocks, and
overlay workspace.  Prefer small, evidence-backed changes over broad rewrites.

## Investigation priorities

1. Verify the complete read path from FUSE lookup/readdir/open/read through the
   catalog, pageable indexes, group container, frame decoder, and object-store
   adapter.  Remove duplicate lookups, unnecessary allocations, and serial
   awaits, but retain digest, CRC, length, and boundary validation.
2. Implement a real byte-budgeted metadata cache and bounded/page-aware
   prefetch.  A single huge directory must never require loading all entries.
3. Use streaming or bounded range decoding where it reduces memory and latency.
   Do not hide work in teardown or close, and do not use a shared data block for
   unrelated files merely to improve a benchmark.
4. For files below 256 KiB, evaluate the existing MinIO-style inline
   metadata+data/object layout. The current user scope is v3 only: reject old
   versions explicitly; historical fixture compatibility is not required.

## Experiment rules

- Read `AGENTS.md`, the v3 spec, and current performance documents first.
- Keep cache budgets and cold-read state explicit.  Do not report a warm-cache
  number as a cold result.
- Use the compose/performance runners and the existing comparison tooling.
  Record fio bandwidth, request counts, object amplification, and end-to-end
  active-plus-drain bandwidth.  Compare matched JuiceFS settings when a
  JuiceFS baseline is needed; do not rerun a full large JuiceFS campaign just
  for noise.
- Cloud experiments must be small and bounded (begin with 10k files, then
  scale only when the result justifies it).  Remove temporary OSS objects,
  Redis/TiKV data, mounts, containers, and servers after each run.
- Before accepting any code or performance claim, run the repository CI gate:
  `cargo fmt --all --check`, script syntax checks, `cargo check --workspace`,
  `cargo build --workspace`, and
  `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins`.
  Add focused regression tests for each behavioral change.
- Do not weaken correctness checks or add benchmark-only special cases.  Do
  not commit credentials, tokens, or local machine paths.

## Delivery

For each accepted change, record the hypothesis, artifact paths, cold-cache
setup, and matched baseline in `doc/performance/`.  Keep rejected candidates
out of the code and document why they were rejected.  Commit coherent changes
to the personal branch and push them to `origin`; report the final commit IDs,
tests, and any remaining performance gap.
