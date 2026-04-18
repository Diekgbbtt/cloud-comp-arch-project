#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = ROOT / "results" / "part2" / "b" / "results_part2b_summary.csv"
OUTPUT_PNG = ROOT / "results" / "part3" / "strategy_comparison.png"

COLORS = {
    "streamcluster": "#5B4BDB",
    "freqmine": "#157A6E",
    "canneal": "#E3642A",
    "vips": "#8BC34A",
    "blackscholes": "#4A90E2",
    "radix": "#D98C10",
    "barnes": "#E46AA5",
    "memcached": "#BDBDBD",
}


def load_durations(path: Path) -> dict[tuple[str, int], float]:
    durations: dict[tuple[str, int], float] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            durations[(row["benchmark"], int(row["threads"]))] = float(row["real_sec"])
    return durations


def make_strategy_definitions(durations: dict[tuple[str, int], float]):
    baseline = {
        "title": "Baseline: big 3 on Node A",
        "rows": [
            ("A cores 0-3", [("streamcluster", 4, 0)]),
            ("A cores 4-7", [("freqmine", 4, 0), ("canneal", 4, durations[("freqmine", 4)])]),
            ("B cores 1-2", [("vips", 2, 0)]),
            ("B core 3", [("blackscholes", 1, 0), ("radix", 1, durations[("blackscholes", 1)])]),
        ],
    }

    strategy_b = {
        "title": "Strategy B: canneal to Node B",
        "rows": [
            ("A cores 0-3", [("streamcluster", 4, 0)]),
            (
                "A cores 4-7",
                [
                    ("freqmine", 4, 0),
                    ("barnes", 4, durations[("freqmine", 4)]),
                    ("vips", 4, durations[("freqmine", 4)] + durations[("barnes", 4)]),
                ],
            ),
            ("B cores 1-2", [("canneal", 2, 0)]),
            ("B core 3", [("blackscholes", 1, 0), ("radix", 1, durations[("blackscholes", 1)])]),
        ],
    }

    anti_pattern = {
        "title": "Anti-pattern: freqmine to Node B",
        "rows": [
            ("A cores 0-3", [("streamcluster", 4, 0)]),
            (
                "A cores 4-7",
                [
                    ("canneal", 4, 0),
                    ("barnes", 4, durations[("canneal", 4)]),
                    ("vips", 4, durations[("canneal", 4)] + durations[("barnes", 4)]),
                ],
            ),
            ("B cores 1-2", [("freqmine", 2, 0)]),
            ("B core 3", [("blackscholes", 1, 0), ("radix", 1, durations[("blackscholes", 1)])]),
        ],
    }

    strategies = [baseline, strategy_b, anti_pattern]
    for strategy in strategies:
        strategy["makespan"] = max(
            start + durations[(bench, threads)]
            for _, segments in strategy["rows"]
            for bench, threads, start in segments
        )
        strategy["rows"] = strategy["rows"] + [("B core 0", [("memcached", 1, 0)])]
    return strategies


def add_bar(ax, y, start, duration, color, label):
    ax.barh(y, duration, left=start, height=0.65, color=color, edgecolor="none", zorder=3)
    text_x = start + duration / 2
    if duration >= 18:
        ax.text(text_x, y, label, ha="center", va="center", color="white", fontsize=10, zorder=4)
    else:
        ax.text(start + duration + 2, y, label, ha="left", va="center", color="white", fontsize=9, zorder=4)


def plot():
    durations = load_durations(INPUT_CSV)
    strategies = make_strategy_definitions(durations)

    OUTPUT_PNG.parent.mkdir(parents=True, exist_ok=True)

    xmax = max(strategy["makespan"] for strategy in strategies)
    xmax = max(200, int((xmax + 19) // 20) * 20)

    plt.style.use("dark_background")
    fig, axes = plt.subplots(len(strategies), 1, figsize=(12, 10), constrained_layout=True)
    fig.patch.set_facecolor("#1F1F1F")

    for ax, strategy in zip(axes, strategies):
        ax.set_facecolor("#1F1F1F")
        row_labels = [row for row, _ in strategy["rows"]]
        y_positions = list(range(len(row_labels)))[::-1]

        for y, (row_label, segments) in zip(y_positions, strategy["rows"]):
            ax.barh(y, xmax, left=0, height=0.65, color="#2B2B2B", edgecolor="none", zorder=1)
            for bench, threads, start in segments:
                duration = strategy["makespan"] if bench == "memcached" else durations[(bench, threads)]
                label = f"memcached ({strategy['makespan']:.0f}s)" if bench == "memcached" else f"{bench} {threads}T ({duration:.0f}s)"
                add_bar(ax, y, start, duration, COLORS[bench], label)

        title = f"{strategy['title']} ({strategy['makespan']:.0f}s)"
        if strategy["title"].startswith("Baseline"):
            title = strategy["title"]
        ax.set_title(title, loc="left", fontsize=16, weight="bold", pad=10)
        ax.axvline(strategy["makespan"], color="#9E9E9E", linestyle="--", linewidth=1.2, alpha=0.9)
        ax.set_xlim(0, xmax)
        ax.set_xticks(range(0, xmax + 1, 50))
        ax.set_xticklabels([f"{tick}s" for tick in range(0, xmax + 1, 50)], color="#CFCFCF")
        ax.set_yticks(y_positions)
        ax.set_yticklabels(row_labels, color="#E0E0E0", fontsize=11)
        ax.tick_params(axis="both", length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)

        badge_color = "#2E7D32" if "Strategy B" in strategy["title"] else "#CFCFCF"
        badge_text_color = "white" if "Strategy B" in strategy["title"] else "#111111"
        ax.text(
            0.0,
            -0.25,
            f"Makespan: {strategy['makespan']:.0f}s",
            transform=ax.transAxes,
            fontsize=13,
            color=badge_text_color,
            bbox=dict(boxstyle="round,pad=0.35,rounding_size=0.8", facecolor=badge_color, edgecolor="none"),
        )

    fig.suptitle("Scheduling Strategies From Part 2b Runtimes", fontsize=18, weight="bold", y=1.02)
    fig.savefig(OUTPUT_PNG, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"Wrote {OUTPUT_PNG}")


if __name__ == "__main__":
    plot()
