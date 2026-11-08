#!/bin/bash
set -e
/srv/ax-data/artifacts/bin/full-fuse-replay \
  /srv/ax-data/traces/extracted/pytest-full-02/capture/events.jsonl \
  /srv/ax-data/traces/extracted/pytest-full-02/capture/blobs \
  /mnt/juicefs-c2 \
  /srv/ax-data/c2/strict-pytest.json > /srv/ax-data/c2/strict-pytest.out 2>&1
python3 /srv/ax-data/src/bench/trace_checks.py tree \
  --root /mnt/juicefs-c2 \
  --expected /srv/ax-data/traces/extracted/pytest-full-02/final-tree.json \
  --output /srv/ax-data/c2/strict-pytest-final.json >> /srv/ax-data/c2/strict-pytest.out 2>&1
echo STRICT_PYTEST_DONE >> /srv/ax-data/c2/strict-pytest.out
