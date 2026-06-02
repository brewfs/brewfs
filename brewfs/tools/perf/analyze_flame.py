#!/usr/bin/env python3
"""BrewFS flame graph analysis helpers."""
import argparse
import re
import subprocess
import sys
from collections import Counter, defaultdict


def cmd_hotspots(folded_path: str) -> None:
    """Print top on-CPU hotspots from a folded stack file."""
    func_counts: Counter[str] = Counter()
    with open(folded_path) as f:
        for line in f:
            parts = line.strip().rsplit(" ", 1)
            if len(parts) != 2:
                continue
            stack, count_str = parts
            try:
                count = int(count_str)
            except ValueError:
                continue
            for frame in stack.split(";"):
                # Strip module paths and offsets: "func+0x123 (/path)" -> "func"
                short = frame.rsplit("(", 1)[0].rsplit("+", 1)[0].strip()
                if short and not short.startswith("["):
                    func_counts[short] += count

    print("\n  Top 15 on-CPU functions:")
    for func, cnt in func_counts.most_common(15):
        print(f"    {cnt:>12d}  {func[:110]}")

    # Also show leaf functions (deepest frame in each stack)
    leaf_counts: Counter[str] = Counter()
    with open(folded_path) as f:
        for line in f:
            parts = line.strip().rsplit(" ", 1)
            if len(parts) != 2:
                continue
            stack, count_str = parts
            try:
                count = int(count_str)
            except ValueError:
                continue
            frames = stack.split(";")
            if frames:
                leaf = frames[-1].rsplit("(", 1)[0].rsplit("+", 1)[0].strip()
                if leaf and not leaf.startswith("["):
                    leaf_counts[leaf] += count

    print("\n  Top 10 leaf functions (actual CPU work):")
    for func, cnt in leaf_counts.most_common(10):
        print(f"    {cnt:>12d}  {func[:110]}")


def cmd_offcpu(perf_data: str, folded_out: str) -> None:
    """Convert sched:sched_switch perf data to off-CPU folded stacks."""
    proc = subprocess.Popen(
        ["perf", "script", "-i", perf_data],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )

    offcpu: dict[str, int] = defaultdict(int)
    current_stack: list[str] = []
    event_count = 0
    brewfs_header_count = 0

    # Regex for sched_switch event header
    header_re = re.compile(
        r"(\S+)\s+\d+\s+\[\d+\]\s+([\d.]+):\s+"
        r"sched:sched_switch:\s+(\S+):(\d+)\s+\[\d+\]\s+\S+\s+==>"
    )

    for line in proc.stdout:
        line = line.rstrip("\n")
        if line.startswith("\t"):
            m = re.match(r"\s+([0-9a-f]+)\s+(.+)", line)
            if m:
                func = m.group(2)
                if "([kernel.kallsyms])" not in func and not func.startswith("["):
                    current_stack.append(func)
        else:
            header_m = header_re.match(line)
            if header_m:
                if current_stack and any(
                    "target/release/brewfs" in f or "brewfs" in f
                    for f in current_stack
                ):
                    # Clean frames: strip offset and path
                    frames = [
                        f.rsplit("(", 1)[0].rsplit("+", 1)[0].strip()
                        for f in current_stack
                    ]
                    frames = [f for f in frames if f and not f.startswith("[")]
                    if frames:
                        folded = ";".join(reversed(frames))
                        offcpu[folded] += 1
                    brewfs_header_count += 1
                current_stack = []
                event_count += 1

    proc.wait()

    with open(folded_out, "w") as f:
        for stack, cnt in sorted(offcpu.items(), key=lambda x: -x[1])[:3000]:
            f.write(f"{stack} {cnt}\n")

    print(f"  sched_switch events: {event_count:,}")
    print(f"  brewfs off-CPU stacks: {len(offcpu):,}")

    # Top blocking points
    if offcpu:
        print("\n  Top 10 off-CPU blocking points (by count):")
        for stack, cnt in sorted(offcpu.items(), key=lambda x: -x[1])[:10]:
            leaf = stack.split(";")[-1] if ";" in stack else stack
            print(f"    {cnt:>8d}  {leaf[:120]}")


def cmd_crypto(folded_path: str) -> None:
    """Analyze crypto overhead in on-CPU samples."""
    total = 0
    crypto = 0
    with open(folded_path) as f:
        for line in f:
            parts = line.strip().rsplit(" ", 1)
            if len(parts) != 2:
                continue
            stack, count_str = parts
            try:
                cnt = int(count_str)
            except ValueError:
                continue
            total += cnt
            if any(
                x in stack
                for x in ("sha256", "md5::", "crc_fast", "hmac", "sha2::", "digest::")
            ):
                crypto += cnt

    if total > 0:
        pct = crypto * 100 / total
        print(f"  total samples: {total:,}")
        print(f"  crypto samples: {crypto:,} ({pct:.1f}%)")
        if pct > 10:
            print(
                "  WARNING: crypto overhead >10%! "
                "Consider disabling SigV4 payload signing for non-AWS backends."
            )
            print(
                "  In S3Backend config, set payload_checksum_enabled=false "
                "or use UNSIGNED-PAYLOAD."
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="BrewFS flame graph analysis")
    parser.add_argument("--hotspots", metavar="FOLDED", help="Print hotspot analysis")
    parser.add_argument("--offcpu", nargs=2, metavar=("PERF_DATA", "FOLDED_OUT"),
                        help="Convert sched_switch data to off-CPU folded")
    parser.add_argument("--crypto", metavar="FOLDED", help="Crypto overhead analysis")
    args = parser.parse_args()

    if args.hotspots:
        cmd_hotspots(args.hotspots)
    elif args.offcpu:
        cmd_offcpu(args.offcpu[0], args.offcpu[1])
    elif args.crypto:
        cmd_crypto(args.crypto)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
