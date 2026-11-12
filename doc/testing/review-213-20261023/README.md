# Entity fragment reads: issue #213

An overwrite can leave the same immutable object visible as a prefix and a
suffix. `EntityData::read_attempt` deduplicated descriptors but called
`read_object` per span, downloading and authenticating the whole object each
time. A 1 MiB write followed by a 4 KiB middle overwrite therefore required
three GETs and 2,101,248 transferred bytes for a 1 MiB read.

The fix groups bounded scatter recipes by slice ID, authenticates each distinct
object once, and copies its requested fragments before releasing the buffer.
Only one full object buffer is retained at a time. Existing ownership checks,
read-admission lifetime, fragmentation limits, and both transport validation
fences remain in place.

## Before and after

The public-API regression verifies all output bytes before checking GET and
byte counts. It failed on production code at `origin/main` revision
`a8459dc92142bf94224e4f4af903f592cef5cb13`, before the production edit:

```text
fragmented read: 3 GETs, 2101248 bytes
assertion `left == right` failed: the surviving prefix and suffix refetched the same object
  left: 3
 right: 2
test result: FAILED. 0 passed; 1 failed
```

The same regression passed three times after the fix:

```text
fragmented read: 2 GETs, 1052672 bytes
test result: ok. 1 passed; 0 failed
```

All four runs used:

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
CARGO_PROFILE_TEST_DEBUG=0 cargo test -p brewfs --features workspace-overlay \
  --test entity_fragment_reads -- --test-threads=1 --nocapture
```

The original before-log label is `213-237-before`; it executes only the #213
test. The before/after test sources differ only by rustfmt removing a trailing
blank line. Before: exit 101; each after-run: exit 0.

## Gate status

Passed on this candidate:

- `cargo fmt --all --check`.
- `cargo check --workspace --features workspace-overlay`.
- `cargo clippy --workspace --all-targets --features workspace-overlay -- -D warnings`.
- Four perf-runner shell syntax checks and three report/matrix shell tests.
- `git diff --check`.

**Incomplete:** the broader entity unit suite, full overlay workspace unit
suite, overlay resolver integration suite, default workspace check/build/test,
default Clippy, FUSE feature checks, and real-backend entity suite. Earlier
full library test compilations hit the 24 GiB scope memory cap. A subsequent
compiler-option retry failed before tests. The corrected serial-codegen retry
stayed below the cap but was interrupted when another task started a build;
the resolver retry was similarly interrupted. None is counted as a test pass
or as a regression failure. Remaining gates must be completed before merge.

The scenario proves reduced duplicate transport and hashing; no mounted
throughput or overall performance acceptance is claimed. No performance table
is changed.

`commands.md` lists exact recorded commands, environments, exit codes and test
summaries. `validation.tar.gz` preserves complete before/after output, gate
attempts, corresponding source patches and resource diagnostics. Dependency
warnings and infrastructure failures are retained.
