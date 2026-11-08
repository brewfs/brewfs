#!/bin/bash
# 精简版：BrewFS cp -a 两档 + BrewFS 100k pilot ×3（workspace 用 cp -a 建，几十秒）
set -u
K=/root/.ssh/ax-c1-20261106
cat > /tmp/c2.py <<'PY'
import os, subprocess, time, json
SMALL = "/srv/ax-data/artifacts/bin/smallfiles"
BSRC = "/mnt/brewfs-c3/base/small"
DST = "/mnt/brewfs-c3/base/copytest"
OUT = "/srv/ax-data/c2b2"
os.makedirs(OUT, exist_ok=True)
tsv = os.path.join(OUT, "fork.tsv")
if not os.path.exists(tsv):
    open(tsv, "w").write("system\tmethod\tdirs\tfiles\tseconds\tfiles_per_s\n")
def say(m):
    print(m, flush=True); open(os.path.join(OUT, "run.log"), "a").write(m + "\n")
def nfiles(p):
    return sum(len(fs) for _, _, fs in os.walk(p))

for ndirs in (11, 102):
    subprocess.run(["rm", "-rf", DST], capture_output=True)
    os.makedirs(DST, exist_ok=True)
    t0 = time.time()
    for d in sorted(os.listdir(BSRC))[:ndirs]:
        subprocess.run(["cp", "-a", os.path.join(BSRC, d), os.path.join(DST, d)], capture_output=True)
    s = time.time() - t0
    n = nfiles(DST)
    say(f"BREW cp-a dirs={ndirs} files={n} copy={s:.1f}s ({n/max(s,1e-9):.0f} f/s)")
    open(tsv, "a").write(f"brewfs\tcp-a\t{ndirs}\t{n}\t{s:.1f}\t{n/max(s,1e-9):.0f}\n")
    subprocess.run(["rm", "-rf", DST], capture_output=True)

# BrewFS 100k pilot ×3
WS = "/mnt/brewfs-c3/base/ws100k"
ptsv = os.path.join(OUT, "pilot100k.tsv")
if not os.path.exists(ptsv):
    open(ptsv, "w").write("rep\tbuild_s\twall_s\treads\twrites\n")
for rep in (1, 2, 3):
    subprocess.run(["rm", "-rf", WS], capture_output=True)
    os.makedirs(WS, exist_ok=True)
    t0 = time.time()
    for d in sorted(os.listdir(BSRC))[:102]:
        subprocess.run(["cp", "-a", os.path.join(BSRC, d), os.path.join(WS, d)], capture_output=True)
    build_s = time.time() - t0
    subprocess.run(["sync"], capture_output=True)
    open("/proc/sys/vm/drop_caches", "w").write("3")
    with open(os.path.join(OUT, f"pilot-r{rep}.out"), "w") as f:
        subprocess.run([SMALL, "run", "--root", WS, "--output", os.path.join(OUT, f"p{rep}/run"),
                        "--files", "100000", "--hot-files", "512", "--workers", "8",
                        "--read-ops", "8192", "--cow-files", "256", "--write-ops", "256",
                        "--mixed-ops", "1024", "--seed", "9173", "--workspace", "1"],
                       stdout=f, stderr=subprocess.STDOUT)
    p = os.path.join(OUT, f"p{rep}/run/result.json")
    if os.path.exists(p):
        j = json.load(open(p))
        say(f"BREW pilot100k rep{rep}: build={build_s:.0f}s wall={j.get('wall_seconds',0):.4f}s reads={j.get('verified_reads')} writes={j.get('writes')}")
        open(ptsv, "a").write(f"{rep}\t{build_s:.0f}\t{j.get('wall_seconds',0):.4f}\t{j.get('verified_reads')}\t{j.get('writes')}\n")
    else:
        say(f"BREW pilot100k rep{rep}: result.json 缺失")
    subprocess.run(["rm", "-rf", WS], capture_output=True)
say("=== C2 SUPPLEMENT DONE ===")
PY
scp -q -o StrictHostKeyChecking=no -i $K /tmp/c2.py root@10.20.0.11:/root/c2.py
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -9 -f ab.py 2>/dev/null || true
sleep 2
setsid nohup python3 /root/c2.py > /srv/ax-data/c2b2-console.log 2>&1 &
sleep 120
echo "  进程: $(pgrep -fc c2.py || echo 0)"
cat /srv/ax-data/c2b2/run.log 2>/dev/null | sed 's/^/    /'
EOS
