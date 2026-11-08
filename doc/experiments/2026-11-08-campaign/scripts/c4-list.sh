#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
echo "=== 1. workspace list ==="
timeout 90 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>&1 | head -40 | cut -c1-160 | sed 's/^/  /'
echo
echo "=== 2. mount 与 workspace 相关的参数 ==="
$B mount --help 2>&1 | grep -B1 -A3 -iE 'workspace|volume-format|volume_format' | head -40 | cut -c1-140 | sed 's/^/  /'
EOS
