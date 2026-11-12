# Packed v3 exact decoded-frame cache local A/B (2026-10-01)

## Status and scope

This is a bounded local FUSE/LocalFS experiment for the explicit
`warm-frame-cache` profile. It is not an OSS result and does not replace the
strict-cold JuiceFS comparison. The accepted code keeps the decoded frame cache
disabled by default (`BREWFS_PACKED_DECODED_FRAME_CACHE_BYTES=0`).

The hypothesis is specific to the current packed layout: adjacent 100 KiB files
can share one independently authenticated roughly 200 KiB frame. Strict cold
reads may fetch that frame again when the second file arrives after the first
coordinator batch. An exact byte-budgeted decoded-frame cache can reuse that
already authenticated frame without the 4 MiB alignment overscan of the window
cache.

## Matched workload

All runs used the same shape and scanner:

- 10,000 independent 100 KiB files;
- two directory levels, fanout 10, 100 files per leaf;
- random-small-file packed layout;
- 16 FUSE workers and 16 scanner workers;
- zero persistent payload memory/SSD cache;
- frame-window cache and data prefetch disabled;
- metadata prefetch `auto`, metadata budget 256 MiB;
- `tools/perf/smallfiles_scan.py --mode full`, reading every file to EOF and
  validating every payload byte;
- checksum `1273096`, 1,024,000,000 logical/payload bytes, zero errors in every
  run.

The scanner itself is covered by unit tests for tree/stat/full modes, payload
corruption, and size mismatch, plus a smoke using the real
`packed_v3_snapshot_fixture --raw-only` output.

## Repeated A/B

The execution order was strict → decoded → decoded → strict, reusing the same
fixture but creating a fresh mount for every row.

| profile | run | files/s | MiB/s | p50 ms | p95 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| strict, cache 0 | A | 920.72 | 89.91 | 15.96 | 30.13 |
| decoded cache 32 MiB | A | 1123.24 | 109.69 | 13.61 | 23.54 |
| decoded cache 32 MiB | B | 1107.02 | 108.11 | 13.86 | 23.53 |
| strict, cache 0 | B | 939.25 | 91.72 | 15.75 | 29.00 |
| strict mean | — | 929.99 | 90.82 | 15.86 | 29.57 |
| decoded mean | — | 1115.13 | 108.90 | 13.73 | 23.54 |

Mean effect:

- files/s: **+19.91%**;
- logical MiB/s: **+19.91%**;
- p50 latency: **-13.40%**;
- p95 latency: **-20.40%**.

Request-graph samples were stable across the paired runs. Strict issued about
2,996–3,041 physical data ranges and decoded about 7,409–7,452 frames. The 32
MiB cache issued about 1,899–1,903 physical data ranges and decoded about
5,279–5,296 frames. It reported roughly 1,743–1,756 exact frame hits. One earlier
run showed a bounded resident size of 33,382,400 bytes (162 entries) and 4,818
capacity evictions.

The implementation also corrects `packed_logical_bytes`: it is now counted at
the user read boundary and reports the full 1,024,000,000 bytes rather than a
post-dedup coordinator approximation.

Artifacts:

- `docker/compose-xfstests/artifacts/local-packed-v3-decoded-ab-20261001-014011/`
- preliminary candidate:
  `docker/compose-xfstests/artifacts/local-packed-v3-decoded32m-10k-20261001-013704/`

## Rejected candidates

### 64 MiB aligned 4 MiB window

The window reduced physical range GETs from about 3,032 to 522, but increased
physical bytes from about 1.51 GiB to 3.00 GiB. Throughput fell from 918.32 to
434.18 files/s and p95 rose from 29.37 to 96.16 ms. The window remains opt-in;
it is rejected as the default for this random 100 KiB workload.

Artifact:
`docker/compose-xfstests/artifacts/local-packed-v3-window64m-10k-20260930-235851/`.

### 1 ms coordinator delay

Changing the 250 µs collection delay back to 1 ms produced one noisy +1.69%
throughput sample but worsened the request graph: data ranges rose from 3,032 to
3,525, decoded frames from 7,381 to 7,928, and p95 regressed by about 9.8%.
The source was restored to 250 µs.

Artifact:
`docker/compose-xfstests/artifacts/local-packed-v3-coalesce1ms-10k-20261001-002037/`.

### Eager versus adaptive metadata warm-up

Adaptive `auto` eliminated 11,676 demand descriptor-range requests by warming
72 small frame-directory ranges, but LocalFS full-read throughput was unchanged
(917.68 versus 918.32 files/s). Keep `auto` as the full-read default because the
request graph is better for remote object stores; do not claim a LocalFS
throughput win.

## Acceptance boundary

The exact cache is accepted as an opt-in, bounded `warm-frame-cache` capability,
not as a strict-cold improvement. A matched JuiceFS comparison must give the
reference side the same explicit data-cache budget and must continue to report
strict cold separately. Cloud/OSS acceptance remains pending; no server was
created for these local runs, and every temporary FUSE mount, process, cache and
fixture directory was removed by the test traps.
