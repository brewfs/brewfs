#!/usr/bin/env python3
"""Generate the README's normalized BrewFS/JuiceFS performance chart."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "doc" / "assets" / "performance-vs-juicefs.svg"

CORE_RESULTS = [
    ("Large write", 1.17),
    ("Large read", 2.29),
    ("Sequential read", 1.57),
    ("Random read", 2.45),
    ("File create", 2.62),
    ("Directory read", 1.62),
]

MIXED_RESULTS = [
    ("Read", 12.09),
    ("Write", 11.86),
]


def text(x: int, y: int, value: str, size: int, fill: str, weight: int = 400) -> str:
    return (
        f'<text x="{x}" y="{y}" font-family="Inter,Segoe UI,Arial,sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}">{value}</text>'
    )


def rect(x: int, y: int, width: float, height: int, fill: str, radius: int = 8) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{width:.1f}" height="{height}" '
        f'rx="{radius}" fill="{fill}" />'
    )


def generate() -> str:
    svg = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="760" viewBox="0 0 1280 760">',
        '<rect width="1280" height="760" rx="20" fill="#0b1220" />',
        text(58, 68, "Selected benchmark wins vs JuiceFS", 32, "#f8fafc", 700),
        text(58, 102, "BrewFS relative performance · JuiceFS = 1.0× · higher is better", 17, "#94a3b8"),
        rect(48, 132, 752, 568, "#111827", 14),
        rect(824, 132, 408, 568, "#111827", 14),
        text(78, 178, "CORE THROUGHPUT + METADATA", 15, "#5eead4", 700),
        text(854, 178, "MIXED RANDOM I/O", 15, "#bef264", 700),
        text(78, 205, "Same Redis + RustFS benchmark", 14, "#64748b"),
        text(854, 205, "70% read / 30% write", 14, "#64748b"),
    ]

    bar_x = 275
    bar_width = 470
    baseline_x = bar_x + bar_width / 3.0
    svg.append(
        f'<line x1="{baseline_x:.1f}" y1="225" x2="{baseline_x:.1f}" y2="650" '
        'stroke="#fb7185" stroke-width="2" stroke-dasharray="5 7" />'
    )
    svg.append(text(int(baseline_x) - 28, 674, "1.0×", 13, "#fb7185", 700))

    for index, (label, value) in enumerate(CORE_RESULTS):
        y = 238 + index * 68
        svg.append(text(78, y + 25, label, 17, "#e2e8f0", 600))
        svg.append(rect(bar_x, y, bar_width, 34, "#1f2937", 7))
        value_width = bar_width * value / 3.0
        svg.append(rect(bar_x, y, value_width, 34, "#2dd4bf", 7))
        svg.append(text(min(int(bar_x + value_width + 12), 742), y + 24, f"{value:.2f}×", 16, "#f8fafc", 700))

    mixed_x = 854
    mixed_width = 330
    mixed_baseline_x = mixed_x + mixed_width / 13.0
    svg.append(
        f'<line x1="{mixed_baseline_x:.1f}" y1="238" x2="{mixed_baseline_x:.1f}" y2="510" '
        'stroke="#fb7185" stroke-width="2" stroke-dasharray="5 7" />'
    )
    svg.append(text(854, 540, "JuiceFS baseline", 13, "#fb7185", 600))

    for index, (label, value) in enumerate(MIXED_RESULTS):
        y = 255 + index * 145
        svg.append(text(mixed_x, y, label, 18, "#e2e8f0", 600))
        svg.append(rect(mixed_x, y + 18, mixed_width, 52, "#1f2937", 9))
        value_width = mixed_width * value / 13.0
        svg.append(rect(mixed_x, y + 18, value_width, 52, "#a3e635", 9))
        svg.append(text(mixed_x, y + 102, f"{value:.2f}×", 28, "#f8fafc", 700))

    svg.extend(
        [
            text(854, 600, "Local snapshot · July 2026", 14, "#94a3b8", 600),
            text(854, 627, "Buffered fio, compression off", 14, "#64748b"),
            text(854, 654, "See README for full results", 14, "#64748b"),
            '</svg>',
        ]
    )
    return "\n".join(svg) + "\n"


if __name__ == "__main__":
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(generate(), encoding="utf-8")
    print(OUTPUT)
