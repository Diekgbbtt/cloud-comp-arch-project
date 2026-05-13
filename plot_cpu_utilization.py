#!/usr/bin/env python3
"""Plot CPU core utilization from CSV files.

Candlestick-style aggregation: for each time bucket we plot the min/max range as a
vertical line and the mean as a point, with a mean trend line connecting buckets.

Uses only built-in libraries + matplotlib.
"""

import csv
from datetime import datetime
from pathlib import Path
import sys


def aggregate_candlestick(values, group_size=4):
    """
    Aggregate values into per-bucket summary used for "candles": low/high range and mean.
    
    Args:
        values: List of float values
        group_size: Number of values to aggregate per candlestick
        
    Returns:
        List of dicts with 'low', 'high', 'mean' keys
    """
    candlesticks = []
    for i in range(0, len(values), group_size):
        group = values[i:i+group_size]
        if group:
            candlesticks.append(
                {
                    "low": min(group),
                    "high": max(group),
                    "mean": sum(group) / len(group),
                    "index": i // group_size,
                }
            )
    return candlesticks


def _bucket_x_seconds(n_samples: int, group_size: int, bucket_s: float) -> list[float]:
    n_buckets = (n_samples + group_size - 1) // group_size
    return [float(i) * float(bucket_s) for i in range(n_buckets)]


def plot_cpu_utilization(csv_file, output_file=None):
    """
    Plot CPU core utilization from a CSV file using candlestick aggregation with mean trends.
    
    Args:
        csv_file: Path to the CSV file containing cpu_util data
        output_file: Path to save the plot (optional)
    """
    try:
        import matplotlib.pyplot as plt
        from matplotlib.ticker import MultipleLocator
    except ModuleNotFoundError as e:
        raise SystemExit(
            "Missing dependency: matplotlib. Install it with `/usr/bin/python3 -m pip install matplotlib --user`."
        ) from e

    # Read the CSV file
    data = {'datetime': [], 'cpu_cols': {}}
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse timestamp
            dt = datetime.fromisoformat(row['iso_timestamp'].replace('Z', '+00:00'))
            data['datetime'].append(dt)
            
            # Extract CPU columns
            for key, value in row.items():
                if key.startswith('cpu_'):
                    if key not in data['cpu_cols']:
                        data['cpu_cols'][key] = []
                    try:
                        data['cpu_cols'][key].append(float(value))
                    except ValueError:
                        data['cpu_cols'][key].append(0)
    
    if not data['cpu_cols']:
        print(f"Error: No CPU columns found in {csv_file}")
        return
    
    # Sort CPU columns
    cpu_cols = sorted(data['cpu_cols'].keys())
    
    group_size = 8
    bucket_s = 8.0

    # X-axis in seconds: 0, 8, 16, ... for 8s buckets.
    x = _bucket_x_seconds(len(data["datetime"]), group_size=group_size, bucket_s=bucket_s)

    # Aggregate into per-4-sample "candles" (low/high) and mean.
    aggregated_data: dict[str, list[dict[str, float]]] = {}
    for cpu_col in cpu_cols:
        aggregated_data[cpu_col] = aggregate_candlestick(data["cpu_cols"][cpu_col], group_size=group_size)

    # 4 stacked subplots (one per core): candlesticks (min/max) + mean line.
    n = len(cpu_cols)
    fig, axes = plt.subplots(
        nrows=n,
        ncols=1,
        figsize=(12, 7.5),
        sharex=True,
        sharey=True,
    )
    if n == 1:
        axes = [axes]

    colors = plt.cm.tab10(range(n))
    for i, cpu_col in enumerate(cpu_cols):
        ax = axes[i]
        candles = aggregated_data[cpu_col]
        xs = x[: len(candles)]
        lows = [c["low"] for c in candles]
        highs = [c["high"] for c in candles]
        means = [c["mean"] for c in candles]

        ax.vlines(xs, lows, highs, color=colors[i], alpha=0.45, linewidth=2.0)
        ax.plot(xs, means, color=colors[i], linewidth=1.2, marker="o", markersize=2.8)

        ax.set_ylabel(cpu_col)

        ax.grid(True, which="major", linestyle=":", alpha=0.25)
        ax.grid(True, which="minor", linestyle=":", alpha=0.12)

    # Titles and labels
    title = Path(csv_file).name
    subtitle = "candles: min/max over 8 samples, line/points: mean"
    fig.suptitle(f"{title} ({subtitle})")
    axes[-1].set_xlabel("Time [s] (8s buckets)")
    fig.supylabel("CPU utilization [%]")

    # 10% interval scale.
    axes[0].set_ylim(bottom=0)
    axes[0].set_yticks(range(0, 101, 10))

    # X-axis ticks: major every 40s, minor every 8s.
    major_s = bucket_s * 5.0
    axes[-1].xaxis.set_major_locator(MultipleLocator(major_s))
    axes[-1].xaxis.set_minor_locator(MultipleLocator(bucket_s))
    axes[-1].tick_params(axis="x", labelsize=7)
    if x:
        axes[-1].set_xlim(left=0.0, right=x[-1])

    # Tight layout while keeping suptitle.
    fig.tight_layout(rect=(0, 0, 1, 0.96))

    if output_file:
        fig.savefig(output_file, dpi=150)
        plt.close(fig)
        print(f"✓ Plot saved to: {output_file}")
    else:
        plt.show()


def plot_all_cpu_files(directory, pattern='cpu_util_*.csv'):
    """
    Plot all CPU utilization files in a directory.
    
    Args:
        directory: Directory containing CPU utilization CSV files
        pattern: File pattern to match
    """
    dir_path = Path(directory)
    csv_files = sorted(dir_path.glob(pattern))
    
    if not csv_files:
        print(f"No files matching {pattern} found in {directory}")
        return
    
    for csv_file in csv_files:
        print(f"\nProcessing {csv_file.name}...")
        output_file = csv_file.parent / f"{csv_file.stem}_candlestick.png"
        plot_cpu_utilization(str(csv_file), str(output_file))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        # If a file is provided as argument, plot that specific file
        csv_file = sys.argv[1]
        if not Path(csv_file).exists():
            print(f"Error: File {csv_file} not found")
            sys.exit(1)
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        plot_cpu_utilization(csv_file, output_file)
    else:
        # Default: plot all cpu_util_*.csv files in part4/results/4_3
        results_dir = Path("/Users/diekgbbtt/cloud-comp-arch-project/part4/results/4_3")
        if results_dir.exists():
            plot_all_cpu_files(str(results_dir))
        else:
            print("Please provide a CSV file path as argument:")
            print(f"  python3 {sys.argv[0]} <csv_file> [output_file]")
