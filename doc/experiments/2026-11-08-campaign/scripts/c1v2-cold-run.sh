#!/bin/bash
# C1-v2 冷读专项：重启 RustFS 后**重挂载**被测系统，再读（避免 mount 连接失效导致 EIO）
set -u
K=/root/.ssh/ax-c1-20261106
cat > /root/c1v2-cold.py <<'PY'
import json, os, subprocess, time, sys

OUT = "/srv/ax-data/c1v2"
TSV = os.path.join(OUT, "c1v2.tsv")
SYS = {
    "juicefs": dict(mnt="/mnt/jfs-c1v2"),
    "brewfs":  dict(mnt="/mnt/bfs-c1v2"),
}
SIZES = ["1M", "4M", "64M"]
CELL = "4G"
BASE = ["--direct=1","--ioengine=libaio","--percentile_list=50:95:99"]
KEY = "/root/.ssh/ax-c1-20261106"

def say(m):
    print(m, flush=True); open(os.path.join(OUT,"run.log"),"a").write(m+"\n")

def obj_ssh(cmd, timeout=120):
    return subprocess.run(["ssh","-o","StrictHostKeyChecking=no","-i",KEY,"root@10.20.2.163",cmd],
                          capture_output=True, timeout=timeout)

def obj_free():
    r = obj_ssh("df -BG --output=avail /srv/ax-data | tail -1", 30)
    t = (r.stdout or b"").decode().strip().rstrip("G")
    try: return int(t)
    except Exception: return -1

def restart_rustfs():
    obj_ssh("pkill -x rustfs; sleep 2", 60)
    obj_ssh("setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 "
            "--console-address 0.0.0.0:9001 /srv/ax-data/rustfs "
            "</dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 & echo started", 60)
    for i in range(30):
        time.sleep(1)
        r = obj_ssh("curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:9000/health || true", 30)
        if (r.stdout or b"").decode().strip().endswith("200"):
            return f"200@{i+1}s"
    return "FAILED"

def drop_caches():
    try: open("/proc/sys/vm/drop_caches","w").write("3")
    except Exception: pass

def remount(sysname):
    if sysname == "juicefs":
        subprocess.run(["bash","-lc",
            "pkill -f 'juicefs mount.*jfs-c1v2' 2>/dev/null; umount -l /mnt/jfs-c1v2 2>/dev/null; "
            "sleep 1; /srv/ax-data/artifacts/bin/juicefs mount -d --cache-size 102400 "
            "--cache-dir /srv/ax-data/jcache tikv://10.20.2.162:2379/c1fio2 /mnt/jfs-c1v2 "
            ">/srv/ax-data/logs/juice-mount.log 2>&1"], timeout=180)
    else:
        subprocess.run(["bash","-lc",
            "pkill -f 'brewfs-patched mount.*bfs-c1v2' 2>/dev/null; umount -l /mnt/bfs-c1v2 2>/dev/null; "
            "sleep 1; AWS_ACCESS_KEY_ID=$(cat /etc/ax-c1/rustfs-access) "
            "AWS_SECRET_ACCESS_KEY=$(cat /etc/ax-c1/rustfs-secret) AWS_DEFAULT_REGION=us-east-1 "
            "setsid nohup /srv/ax-data/artifacts/bin/brewfs-patched mount --privileged "
            "--config /etc/ax-c1/brewfs-c1v2.yaml /mnt/bfs-c1v2 "
            ">/srv/ax-data/logs/brew-mount.log 2>&1 & "], timeout=180)
    for i in range(40):
        time.sleep(1)
        r = subprocess.run(["mountpoint","-q",SYS[sysname]["mnt"]])
        if r.returncode == 0:
            return True
    return False

def pcts(direction):
    p = ((direction or {}).get("clat_ns") or {}).get("percentile") or {}
    def g(k):
        v = p.get(k)
        return round(v/1e6, 3) if isinstance(v,(int,float)) else None
    return g("50.000000"), g("95.000000"), g("99.000000")

def run_fio(d, name, fname, args):
    jf = os.path.join(OUT, f"{name}.json")
    cmd = (["fio","--name="+name,"--directory="+d,"--filename="+fname,
            "--output-format=json","--output="+jf] + BASE + args)
    t0 = time.time(); r = subprocess.run(cmd, capture_output=True, timeout=3600); wall = time.time()-t0
    if r.returncode != 0 or not os.path.exists(jf):
        return None, wall, (r.stderr or b"")[:160].decode("utf-8","ignore")
    job = json.load(open(jf))["jobs"][0]
    w, rd = job.get("write",{}) or {}, job.get("read",{}) or {}
    src = w if w.get("io_bytes",0) >= rd.get("io_bytes",0) else rd
    p50,p95,p99 = pcts(src)
    return {"bw_mib": (w.get("bw_bytes",0)+rd.get("bw_bytes",0))/2**20,
            "read_bw": rd.get("bw_bytes",0)/2**20, "iops": w.get("iops",0)+rd.get("iops",0),
            "p50":p50,"p95":p95,"p99":p99,
            "read_mib": rd.get("io_bytes",0)/2**20, "write_mib": w.get("io_bytes",0)/2**20,
            "errors": (rd.get("total_errs",0) or 0)+(w.get("total_errs",0) or 0)}, wall, None

say("=== C1-v2 coldread start ===")
for bs in SIZES:
    for rep in (1,2,3):
        for sysname in ("juicefs","brewfs"):
            if obj_free() < 80: say("ABORT disk"); sys.exit(2)
            d = SYS[sysname]["mnt"]
            fname = f"c1v2-cold-{sysname}-{bs}.dat"
            res, wall, err = run_fio(d, f"prep2-{sysname}-{bs}-r{rep}", fname,
                                     ["--rw=write","--bs=%s"%bs,"--size=%s"%CELL,"--end_fsync=1"])
            if res is None: say(f"prep2 {sysname} {bs} r{rep} FAILED {err}"); continue
            code = restart_rustfs(); drop_caches()
            ok = remount(sysname)
            if not ok: say(f"remount failed {sysname}"); continue
            res, wall, err = run_fio(d, f"{sysname}-coldread-{bs}-r{rep}", fname,
                                     ["--rw=read","--bs=%s"%bs,"--size=%s"%CELL])
            if res is None: say(f"coldread {sysname} {bs} r{rep} FAILED {err} rustfs={code}"); continue
            say(f"{sysname}-coldread-{bs}-r{rep}: bw={res['bw_mib']:.1f} iops={res['iops']:.0f} "
                f"p50={res['p50']} p95={res['p95']} p99={res['p99']} wall={wall:.1f}s errs={res['errors']} rustfs={code}")
            open(TSV,"a").write(f"{sysname}\tcoldread\t{bs}\t{rep}\tcold\t{res['bw_mib']:.2f}\t{res['read_bw']:.2f}\t"
                f"{res['iops']:.0f}\t{res['p50']}\t{res['p95']}\t{res['p99']}\t{wall:.2f}\t"
                f"{res['read_mib']:.1f}\t{res['write_mib']:.1f}\t{res['errors']}\n")
say("=== C1-v2 coldread done ===")
PY
scp -q -o StrictHostKeyChecking=no -i $K /root/c1v2-cold.py root@10.20.2.161:/root/c1v2-cold.py
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
python3 -c "import ast;ast.parse(open('/root/c1v2-cold.py').read())" && echo "  py ok"
pkill -f c1v2.py 2>/dev/null || true
pkill -f c1v2-cold.py 2>/dev/null || true
sleep 2
setsid nohup python3 /root/c1v2-cold.py > /srv/ax-data/c1v2-cold-console.log 2>&1 &
sleep 90
echo "  进程: $(pgrep -fc c1v2-cold.py || echo 0)"
tail -4 /srv/ax-data/c1v2/run.log 2>/dev/null | cut -c1-170 | sed 's/^/  /'
EOS
