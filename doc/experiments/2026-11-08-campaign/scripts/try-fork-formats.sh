#!/bin/bash
# 尝试不同 volume-format 挂 fork，看哪种能看到 base 数据
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1
set -u
cd /srv/ax-data/src || exit 1
B=target/release/brewfs
OUT=/srv/ax-data/sealtest
META="sqlite://$OUT/meta.db"
FID=$(python3 - <<'PY'
import re
raw=open('/srv/ax-data/sealtest/fork.json',errors='replace').read()
m=re.findall(r'"workspace_id":\s*"([0-9a-f-]{36})"', raw)
print(m[0] if m else "")
PY
)
WID=$(python3 - <<'PY'
import json,glob,os
# 主 workspace：list 里 head_epoch 0 且无 base_revision 的那个（从 meta.db 读更稳）
import subprocess
out=subprocess.run(["sqlite3","/srv/ax-data/sealtest/meta.db",
  "select workspace_id from workspace_overlay_workspaces where base_revision is null limit 1;"],
  capture_output=True)
print(out.stdout.decode().strip() or "")
PY
)
echo "  FID=$FID  WID=$WID"
for fmt in workspace-v1 workspace-native-v2; do
  for tgt in "$FID" "$WID"; do
    [ -z "$tgt" ] && continue
    pkill -f "mount.*$OUT/mnt" 2>/dev/null; umount -l $OUT/mnt 2>/dev/null; sleep 1
    timeout 40 $B mount --meta-backend sqlx --meta-url "$META" --data-backend local-fs --data-dir $OUT/data \
      --volume-format "$fmt" --workspace "$tgt" $OUT/mnt > $OUT/m-$fmt-$(echo $tgt | cut -c1-8).log 2>&1 &
    for i in $(seq 1 25); do sleep 1; mountpoint -q $OUT/mnt && break; done
    N=$(ls $OUT/mnt/sealbase 2>/dev/null | wc -l)
    echo "  fmt=$fmt ws=${tgt:0:8} mount=$(mountpoint -q $OUT/mnt && echo y || echo n) files=$N"
    umount $OUT/mnt 2>/dev/null
    sleep 1
  done
done
EOS
