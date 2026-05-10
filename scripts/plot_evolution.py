#!/usr/bin/env python3
"""Plot evolution of `makespan_sec` and SLO violations from program JSONs.

Usage:
    python scripts/plot_evolution.py
    python scripts/plot_evolution.py --programs-dir openevolve/runs/19/checkpoints/checkpoint_16/programs

Produces two files in the current working directory:
    - makespan_evolution.png
    - slo_evolution.png

SLO mapping: plotted value = 0 if `slo_term == 1` else 1
"""
from __future__ import annotations

import argparse
import glob
import json
import os
from datetime import datetime
from typing import List, Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def parse_timestamp(obj: dict, path: str) -> float:
    # Prefer top-level `timestamp` (epoch seconds)
    if "timestamp" in obj:
        try:
            return float(obj["timestamp"])
        except Exception:
            pass

    # Try parsing artifacts_json.start_time_utc if present
    art = obj.get("artifacts_json")
    if isinstance(art, str):
        try:
            aj = json.loads(art)
            st = aj.get("start_time_utc")
            if st:
                # ISO format -> datetime
                dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
                return dt.timestamp()
        except Exception:
            pass

    # fallback to file mtime
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


def extract_metrics(path: str) -> Optional[tuple]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        return None

    ts = parse_timestamp(obj, path)
    metrics = obj.get("metrics", {}) if isinstance(obj.get("metrics"), dict) else {}
    makespan = metrics.get("makespan_sec")
    slo_term = metrics.get("slo_term")

    # Try also top-level keys if nested metrics missing
    if makespan is None:
        makespan = obj.get("makespan_sec")
    if slo_term is None:
        slo_term = obj.get("slo_term")

    if makespan is None and slo_term is None:
        return None

    return ts, float(makespan) if makespan is not None else None, float(slo_term) if slo_term is not None else None


def main(programs_dir: str, out_prefix: str = None) -> None:
    if not programs_dir:
        print("No programs directory provided.")
        return

    files: List[str] = sorted(glob.glob(os.path.join(programs_dir, "*.json")))
    if not files:
        print(f"No JSON files found in: {programs_dir}")
        return

    rows = []
    for p in files:
        md = extract_metrics(p)
        if md is None:
            continue
        rows.append((p, *md))

    if not rows:
        print("No metrics found in matched files.")
        return

    # sort by timestamp
    rows.sort(key=lambda r: r[1])

    # Build readable timestamps and values
    times_dt = [datetime.fromtimestamp(r[1]) for r in rows]
    times_str = [dt.isoformat(sep=" ") for dt in times_dt]
    makespans = [r[2] for r in rows]
    slo_terms = [r[3] for r in rows]

    # Map slo_term to binary violation: 0 if slo_term == 1 else 1
    slo_binary = [0 if (st is not None and st == 1.0) else 1 for st in slo_terms]

    # Outline collected timestamps and source files
    print("Collected program timestamps:")
    for idx, (path, ts, _, st) in enumerate(rows):
        print(f"{idx:02d}: file={os.path.basename(path)} epoch={ts:.0f} iso={times_str[idx]} makespan={makespans[idx]} slo_term={st}")

    # Use an index-based x-axis to avoid forcing a monotonic datetime scale
    x = list(range(len(rows)))
    # Choose tick sampling to avoid overcrowding
    max_ticks = 12
    step = max(1, len(x) // max_ticks)
    tick_positions = x[::step]
    tick_labels = [times_str[i] for i in tick_positions]

    # Makespan plot (index-based x-axis, timestamps as labels)
    plt.figure(figsize=(10, 4))
    plt.plot(x, makespans, marker="o", linestyle="-", color="tab:blue")
    plt.title("Makespan over sequence (timestamps labeled)")
    plt.ylabel("makespan_sec")
    plt.xlabel("program index (timestamp shown on ticks)")
    plt.xticks(tick_positions, tick_labels, rotation=30, ha="right")
    out_makespan = (out_prefix + "_makespan.png") if out_prefix else "makespan_evolution.png"
    plt.tight_layout()
    plt.savefig(out_makespan, dpi=150)
    print(f"Saved makespan plot: {out_makespan}")
    plt.close()

    # SLO violation plot (binary, index-based x-axis)
    plt.figure(figsize=(10, 2.5))
    plt.step(x, slo_binary, where="post", color="tab:red")
    plt.scatter(x, slo_binary, color="tab:red")
    plt.ylim(-0.1, 1.1)
    plt.yticks([0, 1], ["no violation (slo_term==1)", "violation"])
    plt.title("SLO violation over sequence (timestamps labeled)")
    plt.xlabel("program index (timestamp shown on ticks)")
    plt.xticks(tick_positions, tick_labels, rotation=30, ha="right")
    out_slo = (out_prefix + "_slo.png") if out_prefix else "slo_evolution.png"
    plt.tight_layout()
    plt.savefig(out_slo, dpi=150)
    print(f"Saved SLO plot: {out_slo}")
    plt.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Plot makespan_sec and SLO evolution from program JSONs.")
    ap.add_argument("--programs-dir", "-d", default="openevolve/runs/19/checkpoints/checkpoint_16/programs",
                    help="directory containing program JSON files (non-recursive).")
    ap.add_argument("--out-prefix", "-o", default=None, help="optional prefix for output files")
    args = ap.parse_args()
    main(args.programs_dir, args.out_prefix)
