#!/bin/bash
set -u
pkill -f c2-django-perf.sh 2>/dev/null || true
pkill -f full-fuse-replay 2>/dev/null || true
sleep 2
echo "  replay: $(pgrep -fc full-fuse-replay || echo 0)  script: $(pgrep -fc c2-django-perf.sh || echo 0)"
umount /mnt/bfs-c2perf 2>/dev/null; echo "  umount rc=$?"
echo "  mount=$(mountpoint -q /mnt/bfs-c2perf && echo y || echo n)"
