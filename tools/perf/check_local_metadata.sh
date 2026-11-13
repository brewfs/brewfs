#!/usr/bin/env bash
# Local-only Redis experiment; per-operation evidence includes the final drain.
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
out=${1:?usage: check_local_metadata.sh NEW_ABSOLUTE_OUTPUT_DIRECTORY}
[[ $out == /* && ! -e $out ]] || { echo 'Output must be new and absolute' >&2; exit 2; }
mkdir -p -- "$out"
cd -- "$root"
exec > >(tee "$out/run.log") 2>&1
name=brewfs-local-metadata-$$
container=
cleanup() { if [[ -n $container ]]; then docker rm -f "$container" >/dev/null; fi; }
trap cleanup EXIT
docker image inspect redis:7.2-alpine --format '{{.Id}}' > "$out/redis-image.txt"
container=$(docker run --pull=never --detach --name "$name" --publish 127.0.0.1::6379 redis:7.2-alpine redis-server --save '' --appendonly no)
printf '%s\n' "$container" > "$out/container.txt"
port=$(docker port "$container" 6379/tcp)
export BREWFS_LOCAL_SYNC_REDIS_URL="redis://$port/"
for attempt in $(seq 1 50); do
    if docker exec "$container" redis-cli ping | rg -q PONG; then break; fi
    sleep 0.1
done
git rev-parse HEAD > "$out/source-commit.txt"
git diff --binary HEAD -- src doc tools Cargo.toml > "$out/source.patch"
rustc -Vv > "$out/rustc.txt"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
features=fuse-io-uring-runtime,workspace,native-packed-base,frozen-base-metadata,rustfs-ec-block-size-hint,observability
cargo test --locked --lib --no-default-features --features "$features" local_metadata -- --nocapture
cargo test --locked --lib --no-default-features --features "$features" local_metadata_real_redis_pairs -- --ignored --nocapture --test-threads=1 | tee "$out/redis-pairs.log"
cargo test --locked --lib --no-default-features --features "$features" local_metadata_untimed_redis_pairs -- --ignored --nocapture --test-threads=1 | tee "$out/untimed-pairs.log"
python3 - "$out" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
rows = [json.loads(line.split('LOCAL_SYNC_RESULT ', 1)[1])
        for line in (p / 'redis-pairs.log').read_text().splitlines() if 'LOCAL_SYNC_RESULT ' in line]
assert len(rows) == 9
(p / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
untimed = [json.loads(line.split('UNTIMED_READ_RESULT ', 1)[1])
           for line in (p / 'untimed-pairs.log').read_text().splitlines() if 'UNTIMED_READ_RESULT ' in line]
assert len(untimed) == 6
(p / 'untimed-results.json').write_text(json.dumps(untimed, indent=2) + '\n')
PY
printf '%s\n' passed > "$out/status.txt"
