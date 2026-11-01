#!/usr/bin/env python3
"""Paired packed-frame memo experiment using the frozen perf-full protocol.

Example (build and stop all compilers before `measure`):
  python3 tools/perf/packed_verify_memo.py setup
  sudo -n python3 tools/perf/packed_verify_memo.py prepare
  sudo -n python3 tools/perf/packed_verify_memo.py measure
  sudo -n python3 tools/perf/packed_verify_memo.py profiles
  python3 tools/perf/packed_verify_memo.py summarize

The historical campaign is read-only input, never a comparison arm. Both arms
use one pinned candidate executable, new mounts, two 20s warmups per mount and
one 20s measured round. The predeclared order is off/on, on/off, off/on.
"""

import argparse
import hashlib
import importlib.util
import json
import math
import mmap
import os
import re
from pathlib import Path
import shutil
import statistics
import subprocess
import time

import yaml

BASE = Path("/home/hxy/perf-full-20261101")
ROOT = Path("/home/hxy/perf-packed-verify-20261101")
STATE = Path("/home/hxy/perf-packed-verify-state-20261101")
REPO = Path(__file__).resolve().parents[2]
ORDER = ((0, 1), (1, 0), (0, 1))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def sha(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def setup():
    for folder in ("harness", "environment", "bin", "results", "mounts"):
        (ROOT / folder).mkdir(parents=True, exist_ok=True)
    for folder in ("redis", "objects", "cache", "mounts"):
        (STATE / folder).mkdir(parents=True, exist_ok=True)
    # The pinned RustFS image runs under its own UID, as in perf-full setup.
    (STATE / "objects").chmod(0o777)
    # Reuse the immutable seeded corpus without duplicating 2 GiB on disk.
    if not (STATE / "source").exists():
        shutil.copytree(Path("/home/hxy/perf-full-state-20261101/source"),
                        STATE / "source", copy_function=os.link)
    shutil.copy2(BASE / "environment/corpus-sha256.json", ROOT / "environment/corpus-sha256.json")
    originals = {}
    for name in ("run.py", "worker.py", "profile.py"):
        original = BASE / "harness" / name
        originals[name] = sha(original)
        source = original.read_text().replace(str(BASE), str(ROOT)).replace(
            "/home/hxy/perf-full-state-20261101", str(STATE))
        source = source.replace("http://127.0.0.1:29000", "http://127.0.0.1:29300")
        source = source.replace("brewfs-perf-full-20261101", "brewfs-perf-packed-verify-20261101")
        # The frozen worker uses this label solely to prohibit metadata writes.
        source = source.replace("if mode=='packed-v3': argv.append('--readonly')",
                                "if mode!='flat-v1': argv.append('--readonly')")
        (ROOT / "harness" / name).write_text(source)
    save(ROOT / "environment/original-harness-sha256.json", originals)
    compose = yaml.safe_load((BASE / "harness/compose.yml").read_text())
    compose["name"] = "brewfs-perf-packed-verify-20261101"
    compose["services"]["redis"]["ports"] = ["127.0.0.1:29379:6379"]
    compose["services"]["redis"]["volumes"] = [f"{STATE}/redis:/data"]
    compose["services"]["rustfs"]["ports"] = ["127.0.0.1:29300:9000", "127.0.0.1:29301:9001"]
    compose["services"]["rustfs"]["volumes"] = [f"{STATE}/objects:/data"]
    (ROOT / "harness/compose.yml").write_text(yaml.safe_dump(compose, sort_keys=False))
    protocol = json.loads((BASE / "protocol.json").read_text())
    protocol.update(task_started_utc="2026-11-01T13:28:38Z",
                    deadline_utc="2026-11-01T16:28:38Z",
                    git_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
                    comparison="one candidate binary, BREWFS_PACKED_VERIFY_MEMO=0/1",
                    pair_order=ORDER, warmups_per_fresh_mount=2,
                    statistic="per-arm median; paired on-minus-off mean and Student t 95% CI (df=2)",
                    historical_protocol_sha256=sha(BASE / "protocol.json"))
    save(ROOT / "protocol.json", protocol)
    # Reuse matching system-library debug symbols; candidate binary remains
    # available at its recorded absolute path under the new symbol root.
    if not (ROOT / "symbols").exists():
        shutil.copytree(BASE / "symbols", ROOT / "symbols", copy_function=os.link, symlinks=True)
    shutil.copy2(BASE / "environment/kallsyms.txt", ROOT / "environment/kallsyms.txt")


def harness():
    spec = importlib.util.spec_from_file_location("packed_frozen_run", ROOT / "harness/run.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def prepare():
    run = harness()
    run.build_packed("random-small-file")
    run.build_packed("sequential-small-file")


def check_quiet():
    pause_file = ROOT / "validation/paused-for-measurement.json"
    paused = {}
    if pause_file.exists() and not (ROOT / "validation/measurement-ci-resumed.json").exists():
        paused = {str(row["pid"]): row for row in json.loads(pause_file.read_text())}
    active = []
    verified = []
    for path in Path("/proc").glob("[0-9]*/comm"):
        try:
            if path.read_text().strip() in ("cargo", "rustc", "cc1", "cc1plus", "rust-lld"):
                expected = paused.get(path.parent.name)
                if expected:
                    fields = (path.parent / "stat").read_text().rsplit(")", 1)[1].split()
                    ticks = [int(fields[11]), int(fields[12])]
                    if (fields[0] == "T" and fields[19] == expected["starttime"]
                            and ticks == expected["cpu_ticks"]):
                        verified.append(int(path.parent.name))
                        continue
                active.append(str(path.parent))
        except OSError:
            pass
    if active:
        raise RuntimeError(f"stop compilation before measurement: {active}")
    if len(verified) != len(paused):
        raise RuntimeError("paused compiler identity changed during measurement")
    if paused:
        with (ROOT / "environment/compiler-pause-audit.jsonl").open("a") as output:
            output.write(json.dumps({"unix": time.time(), "verified_stopped_pids": verified,
                                     "cpu_ticks_unchanged": True}) + "\n")


def payload_work_counts(directory):
    def counts(path):
        result = {"authentication_wall": 0, "decode_wall": 0}
        for line in path.read_text().splitlines():
            if (line.startswith("brewfs_object_read_work_nanoseconds_count{")
                    and 'kind="external_payload"' in line):
                for work in result:
                    if f'work="{work}"' in line:
                        result[work] += int(line.rsplit(" ", 1)[1])
        return result
    before = counts(directory / "before.stats")
    after = counts(directory / "after-workload.stats")
    return {key: after[key] - before[key] for key in before}


def verify_disabled_profile(directory):
    """Check both sampled leaves and call chains before treating off as A."""
    options = ["--symfs", str(ROOT / "symbols"), "--kallsyms",
               str(ROOT / "environment/kallsyms.txt"), "--no-inline"]
    report = subprocess.check_output(
        ["perf", "report", *options, "--stdio", "--no-children", "-g", "none",
         "--percent-limit", "0", "-i", str(directory / "cpu.data")], text=True)
    (directory / "off-equivalence-perf-report.txt").write_text(report)
    sha_lines = [line for line in report.splitlines()
                 if re.search(r"sha256|sha2::", line, re.IGNORECASE)]
    leaf_matches = [line for line in report.splitlines() if "verify_memo" in line]
    chain_matches = []
    stack_lines = 0
    with (directory / "off-equivalence-perf-script.log").open("w") as errors:
        process = subprocess.Popen(
            ["perf", "script", *options, "-i", str(directory / "cpu.data")],
            stdout=subprocess.PIPE, stderr=errors, text=True)
        for line in process.stdout:
            stack_lines += 1
            if "verify_memo" in line:
                chain_matches.append(line.rstrip())
        process.stdout.close()
        if process.wait():
            raise RuntimeError(f"failed to inspect off call chains: {directory}")
    counts = payload_work_counts(directory)
    result = {"sha_leaf_lines": sha_lines, "verify_memo_leaf_matches": leaf_matches,
              "verify_memo_chain_matches": chain_matches, "script_lines": stack_lines,
              "payload_work_counts": counts,
              "passed": bool(sha_lines and stack_lines) and not leaf_matches and not chain_matches
                        and counts["authentication_wall"] == counts["decode_wall"] > 0,
              "scope": "sampled CPU paths; source const specialization proves disabled memo operations are removed"}
    save(directory / "off-equivalence.json", result)
    if not result["passed"]:
        raise RuntimeError(f"off baseline equivalence failed: {directory}")


def measure(workloads):
    check_quiet()
    run = harness()
    binary = ROOT / "bin/brewfs-observability"
    fingerprint = sha(binary)
    save(ROOT / "environment/measured-binary.json", {"path": str(binary), "sha256": fingerprint})
    symbol = ROOT / "symbols" / str(binary).lstrip("/")
    symbol.parent.mkdir(parents=True, exist_ok=True)
    if not symbol.exists():
        os.link(binary, symbol)
    for workload in workloads:
        profile = "random-small-file" if workload == "randread" else "sequential-small-file"
        manifest = (ROOT / "preparation" / f"packed-{profile}" / "manifest.txt").read_text().strip()
        for pair, arms in enumerate(ORDER, 1):
            for enabled in arms:
                arm = "memo-on" if enabled else "memo-off"
                result = ROOT / "results" / arm / workload / f"measure-{pair}"
                if result.exists():
                    raise RuntimeError(f"refusing to overwrite or select rounds: {result}")
                check_quiet()
                if sha(binary) != fingerprint:
                    raise RuntimeError("candidate binary changed between arms")
                run.ENV["BREWFS_PACKED_VERIFY_MEMO"] = str(enabled)
                label = f"{workload}-pair{pair}-{arm}"
                with run.mounted("packed-v3", label, manifest, binary=binary) as (mount, pid, _):
                    run.verify_packed(mount, label, small_files=workload in ("smallfiles", "metadata"))
                    for warmup in (1, 2):
                        run.run_round(arm, workload, mount, pid, f"warmup-pair{pair}", warmup)
                    run.run_round(arm, workload, mount, pid, "measure", pair)
                    # Verify the same offsets again after memo entries exist.
                    run.verify_packed(mount, label + "-after", small_files=workload in ("smallfiles", "metadata"))
                if not enabled:
                    verify_disabled_profile(result)
                check_quiet()
                save(ROOT / "progress.json", {"last_completed": label, "utc": time.time()})
        for arm in ("memo-off", "memo-on"):
            save(ROOT / "results" / arm / workload / "complete.json", {"complete": True})


def paired(values):
    differences = [on - off for off, on in values]
    mean = statistics.mean(differences)
    half = 4.302652729911275 * statistics.stdev(differences) / math.sqrt(3)
    result = {"off_median": statistics.median(v[0] for v in values),
              "on_median": statistics.median(v[1] for v in values),
              "paired_differences": differences, "paired_mean_difference": mean,
              "paired_mean_95ci": [mean - half, mean + half],
              "significant_95": mean - half > 0 or mean + half < 0}
    if all(off != 0 for off, _ in values):
        percentages = [100 * (on / off - 1) for off, on in values]
        percent_mean = statistics.mean(percentages)
        percent_half = 4.302652729911275 * statistics.stdev(percentages) / math.sqrt(3)
        result.update(paired_percent_differences=percentages,
                      paired_mean_percent_difference=percent_mean,
                      paired_percent_95ci=[percent_mean - percent_half, percent_mean + percent_half])
    return result


def verify_content():
    """Correctness only: SHA-256 every corpus file, twice per mount.

    This may run alongside compilation, but must finish before perf sampling.
    """
    run = harness()
    expected = json.loads((ROOT / "environment/corpus-sha256.json").read_text())
    binary = ROOT / "bin/brewfs-observability"
    fingerprint = sha(binary)
    save(ROOT / "environment/correctness-binary.json", {"path": str(binary), "sha256": fingerprint})
    for profile in ("random-small-file", "sequential-small-file"):
        manifest = (ROOT / "preparation" / f"packed-{profile}" / "manifest.txt").read_text().strip()
        for enabled in (0, 1):
            if sha(binary) != fingerprint:
                raise RuntimeError("candidate binary changed during content verification")
            run.ENV["BREWFS_PACKED_VERIFY_MEMO"] = str(enabled)
            label = f"full-sha256-{profile}-memo{enabled}"
            passes = [ROOT / "correctness" / label / f"pass-{repeat}.json" for repeat in (1, 2)]
            teardown = ROOT / "mounts" / label / "teardown.json"
            if all(path.exists() and json.loads(path.read_text()) == expected for path in passes) and teardown.exists():
                closed = json.loads(teardown.read_text())
                if closed["daemon_returncode"] == 0 and closed["unmount_status"] == 0 and not closed["mounted"]:
                    run.log(f"Retain completed whole-corpus verification: {label}")
                    continue
            with run.mounted("packed-v3", label, manifest, binary=binary) as (mount, _, _):
                for repeat in (1, 2):
                    verified = {}
                    with mmap.mmap(-1, 4 << 20) as buffer:
                        for name, reference in sorted(expected.items()):
                            digest = hashlib.sha256()
                            fd = os.open(mount / name, os.O_RDONLY | os.O_DIRECT)
                            offset = 0
                            try:
                                while offset < reference["bytes"]:
                                    length = min(len(buffer), reference["bytes"] - offset)
                                    view = memoryview(buffer)[:length]
                                    try:
                                        count = os.preadv(fd, [view], offset)
                                        if count != length:
                                            raise RuntimeError(f"short verification read: {name}@{offset}")
                                        digest.update(view)
                                    finally:
                                        view.release()
                                    offset += count
                            finally:
                                os.close(fd)
                            actual = digest.hexdigest()
                            if actual != reference["sha256"]:
                                raise RuntimeError(f"whole-file SHA-256 mismatch: {label}/{name}")
                            verified[name] = {"bytes": offset, "sha256": actual}
                    run.save(ROOT / "correctness" / label / f"pass-{repeat}.json", verified)
                    run.drain(mount, ROOT / "correctness" / label / f"drain-{repeat}")
                    run.log(f"SHA-256 PASS {label}/{repeat}: {len(verified)} files")


def summarize():
    output = {}
    for complete in sorted((ROOT / "results/memo-off").glob("*/complete.json")):
        workload = complete.parent.name
        rows = []
        for pair in (1, 2, 3):
            for arm in ("memo-off", "memo-on"):
                directory = ROOT / "results" / arm / workload / f"measure-{pair}"
                result = json.loads((directory / "summary.json").read_text())
                profile = json.loads((directory / "profile.json").read_text())
                sha_period = sum(x["period"] for x in profile["all_symbols"]
                                 if "sha256" in x["symbol"].lower() or "sha2::" in x["symbol"].lower())
                ring_period = sum(x["period"] for x in profile["all_symbols"]
                                  if "ring_core" in x["symbol"].lower() or "ring::" in x["symbol"].lower())
                sha_avx_period = sum(x["period"] for x in profile["all_symbols"]
                                     if "sha256_block_data_order_avx" in x["symbol"].lower())
                blake3_period = sum(x["period"] for x in profile["all_symbols"]
                                    if "blake3" in x["symbol"].lower())
                rss = []
                for line in (directory / "sampler.jsonl").read_text().splitlines():
                    sample = json.loads(line)
                    if "proc_stat" in sample:
                        fields = sample["proc_stat"].rsplit(")", 1)[1].split()
                        rss.append(int(fields[21]) * os.sysconf("SC_PAGE_SIZE") / 1048576)
                row = {"pair": pair, "arm": arm, "sha_self_pct": 100 * sha_period / profile["period"],
                       "sha_avx_self_pct": 100 * sha_avx_period / profile["period"],
                       "ring_self_pct": 100 * ring_period / profile["period"],
                       "blake3_self_pct": 100 * blake3_period / profile["period"],
                       "rss_mib_median": statistics.median(rss), "rss_mib_max": max(rss),
                       "wall_seconds": result["wall_seconds"], "drain_seconds": result["drain"]["seconds"],
                       "lost_samples": profile["lost_samples"], "unknown_self_pct": profile["unknown_self_pct"]}
                row["payload_work_counts"] = payload_work_counts(directory)
                if "fio" in result:
                    row.update({key: result["fio"][key] for key in
                                ("bw_mib_s", "wall_bw_mib_s", "active_plus_drain_bw_mib_s",
                                 "wall_plus_drain_bw_mib_s")})
                    row["read_clat_p99_ms"] = result["fio"]["read_clat"]["percentile"]["99.000000"] / 1e6
                    row["sha_cpu_seconds_per_gib"] = sha_period / 1e9 / (result["fio"]["io_bytes"] / (1 << 30))
                    row["get_amplification"] = result["objects_active"]["get"]["bytes"] / result["fio"]["io_bytes"]
                rows.append(row)
        comparisons = {}
        for key in ("sha_self_pct", "sha_avx_self_pct", "ring_self_pct", "blake3_self_pct", "rss_mib_median", "wall_seconds",
                    "bw_mib_s", "wall_bw_mib_s", "active_plus_drain_bw_mib_s",
                    "wall_plus_drain_bw_mib_s", "read_clat_p99_ms", "sha_cpu_seconds_per_gib",
                    "get_amplification"):
            if key in rows[0]:
                comparisons[key] = paired([(rows[n][key], rows[n + 1][key]) for n in (0, 2, 4)])
        output[workload] = {"rows": rows, "comparisons": comparisons}
    save(ROOT / "paired-summary.json", output)
    print(json.dumps(output, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("setup", "prepare", "measure", "profiles", "summarize", "verify-content"))
    parser.add_argument("--workloads", nargs="+", default=["seqread", "randread"])
    args = parser.parse_args()
    if args.stage == "setup":
        setup()
    elif args.stage == "prepare":
        prepare()
    elif args.stage == "measure":
        measure(args.workloads)
    elif args.stage == "profiles":
        # Offline symbolization is independent of any sampled workload.
        subprocess.run(["python3", str(ROOT / "harness/profile.py")], check=True)
    elif args.stage == "verify-content":
        verify_content()
    else:
        summarize()


if __name__ == "__main__":
    main()
