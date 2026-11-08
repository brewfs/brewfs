#!/bin/bash
# C1-v2：对称缓存（两侧默认）+ 4 GiB cell + create/overwrite 分开 + randrw 分方向 + 饱和组
set -u
cat > /root/c1v2.py <<'PY'
import json, os, subprocess, time, sys, statistics

OUT = "/srv/ax-data/c1v2"
os.makedirs(OUT, exist_ok=True)
TSV = os.path.join(OUT, "c1v2.tsv")
open(TSV, "w").write("system\top\tbs\trep\tmode\tbw_mib_s\tread_bw\tiops\tp50_ms\tp95_ms\tp99_ms\twall_s\tread_mib\twrite_mib\terrors\n")

SYS = {"juicefs": "/mnt/jfs-c1v2", "brewfs": "/mnt/bfs-c1v2"}
SIZES = ["1M", "4M", "64M"]
CELL = "4G"
BASE = ["--direct=1","--ioengine=libaio","--percentile_list=50:95:99"]

def say(m):
    print(m, flush=True)
    open(os.path.join(OUT, "run.log"), "a").write(m + "\n")

def obj_free_gb():
    r = subprocess.run(["ssh","-o","StrictHostKeyChecking=no","-i","/root/.ssh/ax-c1-20261106",
                        "root@10.20.2.163","df -BG --output=avail /srv/ax-data | tail -1"],
                       capture_output=True, timeout=60)
    txt = (r.stdout or b"").decode().strip().rstrip("G")
    try: return int(txt)
    except Exception: return -1

def drop_caches():
    try: open("/proc/sys/vm/drop_caches","w").write("3")
    except Exception: pass

def restart_rustfs():
    cmd = ("pkill -x rustfs; sleep 2; "
           "AK=$(cat /etc/ax-c1/rustfs-access 2>/dev/null); "
           "setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 "
           "--console-address 0.0.0.0:9001 /srv/ax-data/rustfs "
           "</dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 & sleep 6; "
           "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9000/health")
    r = subprocess.run(["ssh","-o","StrictHostKeyChecking=no","-i","/root/.ssh/ax-c1-20261106",
                        "root@10.20.2.163", cmd], capture_output=True, timeout=120)
    return (r.stdout or b"").decode().strip()[-3:]

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
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, timeout=3600)
    wall = time.time()-t0
    if r.returncode != 0 or not os.path.exists(jf):
        return None, wall, (r.stderr or b"")[:200].decode("utf-8","ignore")
    job = json.load(open(jf))["jobs"][0]
    w, rd = job.get("write",{}) or {}, job.get("read",{}) or {}
    src = w if w.get("io_bytes",0) >= rd.get("io_bytes",0) else rd
    p50,p95,p99 = pcts(src)
    return {"bw_mib": (w.get("bw_bytes",0)+rd.get("bw_bytes",0))/2**20,
            "read_bw": rd.get("bw_bytes",0)/2**20,
            "iops": w.get("iops",0)+rd.get("iops",0),
            "p50":p50,"p95":p95,"p99":p99,
            "read_mib": rd.get("io_bytes",0)/2**20, "write_mib": w.get("io_bytes",0)/2**20,
            "errors": (rd.get("total_errs",0) or 0)+(w.get("total_errs",0) or 0)}, wall, None

def rec(sysname, op, bs, rep, mode, res, wall):
    say(f"{sysname}-{op}-{bs}-r{rep}({mode}): bw={res['bw_mib']:.1f} rdBW={res['read_bw']:.1f} "
        f"iops={res['iops']:.0f} p50={res['p50']} p95={res['p95']} p99={res['p99']} wall={wall:.1f}s errs={res['errors']}")
    open(TSV,"a").write(f"{sysname}\t{op}\t{bs}\t{rep}\t{mode}\t{res['bw_mib']:.2f}\t{res['read_bw']:.2f}\t"
        f"{res['iops']:.0f}\t{res['p50']}\t{res['p95']}\t{res['p99']}\t{wall:.2f}\t"
        f"{res['read_mib']:.1f}\t{res['write_mib']:.1f}\t{res['errors']}\n")

say("=== C1-v2 start ===")
try:
    for bs in SIZES:
        for op in ("write","randwrite","randrw"):
            for rep in (1,2,3):
                for sysname, d in SYS.items():
                    free = obj_free_gb()
                    if free < 80:
                        say(f"ABORT obj disk {free}G < 80G"); sys.exit(2)
                    if rep == 1:
                        fname = f"c1v2-{sysname}-{op}-{bs}-r{rep}.dat"; mode = "create"
                    else:
                        fname = f"c1v2-{sysname}-{op}-{bs}-r1.dat"; mode = "overwrite"
                    args = {"write": ["--rw=write","--bs=%s"%bs,"--size=%s"%CELL,"--end_fsync=1"],
                            "randwrite": ["--rw=randwrite","--bs=%s"%bs,"--size=%s"%CELL,"--end_fsync=1"],
                            "randrw": ["--rw=randrw","--rwmixread=70","--bs=%s"%bs,"--size=%s"%CELL]}[op]
                    name = f"{sysname}-{op}-{bs}-r{rep}"
                    res, wall, err = run_fio(d, name, fname, args)
                    if res is None: say(f"{name}: FAILED {err}"); continue
                    rec(sysname, op, bs, rep, mode, res, wall)
        # 冷读（每 bs 3 轮，两系统）
        for rep in (1,2,3):
            for sysname, d in SYS.items():
                if obj_free_gb() < 80: say("ABORT disk"); sys.exit(2)
                fname = f"c1v2-{sysname}-cold-{bs}.dat"
                res, wall, err = run_fio(d, f"prep-{sysname}-{bs}-r{rep}", fname,
                                         ["--rw=write","--bs=%s"%bs,"--size=%s"%CELL,"--end_fsync=1"])
                if res is None: say(f"prep fail {err}"); continue
                code = restart_rustfs(); drop_caches()
                res, wall, err = run_fio(d, f"{sysname}-coldread-{bs}-r{rep}", fname,
                                         ["--rw=read","--bs=%s"%bs,"--size=%s"%CELL])
                if res is None: say(f"coldread fail {err} health={code}"); continue
                rec(sysname, "coldread", bs, rep, "cold", res, wall)
        # 饱和组（numjobs=8 iodepth=32，write，每系统 1 次）
        for sysname, d in SYS.items():
            if obj_free_gb() < 80: say("ABORT disk"); sys.exit(2)
            fname = f"c1v2-{sysname}-sat-{bs}.dat"
            res, wall, err = run_fio(d, f"{sysname}-sat-{bs}", fname,
                                     ["--rw=write","--bs=%s"%bs,"--size=%s"%CELL,
                                      "--numjobs=8","--iodepth=32","--end_fsync=1"])
            if res is None: say(f"sat fail {err}"); continue
            rec(sysname, "write_sat", bs, 1, "sat", res, wall)
    say("=== C1-v2 done ===")
finally:
    for d in SYS.values():
        for f in os.listdir(d):
            if f.endswith(".dat"):
                try: os.remove(os.path.join(d,f))
                except Exception: pass
PY
scp -q -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 /root/c1v2.py root@10.20.2.161:/root/c1v2.py
ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
python3 -c "import ast;ast.parse(open('/root/c1v2.py').read())" && echo "  py ok"
pkill -f c1v2.py 2>/dev/null || true
setsid nohup python3 /root/c1v2.py > /srv/ax-data/c1v2-console.log 2>&1 &
sleep 60
echo "  进程: $(pgrep -fc c1v2.py || echo 0)"
tail -5 /srv/ax-data/c1v2/run.log 2>/dev/null | cut -c1-170 | sed 's/^/  /'
EOS
