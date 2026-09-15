#!/usr/bin/env bash
# Probe what verification backends this WSL distro can actually run.
for c in redis-server redis-cli minio mc docker podman fusermount3 fusermount; do
    if command -v "$c" >/dev/null 2>&1; then
        echo "$c: $(command -v $c)"
    else
        echo "$c: MISSING"
    fi
done
echo "---"
echo "kernel: $(uname -r)"
echo "fuse dev: $(ls -l /dev/fuse 2>&1)"
echo "---"
# docker desktop distro state (from inside Ubuntu, docker CLI may talk to it if installed)
if command -v docker >/dev/null 2>&1; then
    docker info --format '{{.ServerVersion}}' 2>&1 | head -2
fi
echo "---"
# Can we actually mount fuse? Try fusermount3 -v just to see presence
fusermount3 --version 2>&1 || fusermount --version 2>&1
echo "---"
# what test backends does the repo's test suite know how to use?
grep -rhoE 'REDIS_[A-Z_]+' /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5/tests 2>/dev/null | sort -u | head
echo "---"
ls /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5/docker/compose-xfstests/ 2>/dev/null
