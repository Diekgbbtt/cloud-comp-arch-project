from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import statistics
from collections import defaultdict

try:
    # When executed as a module: `python3 -m automation.plot_mcperf_qps_p95_timeseries ...`
    from automation.mcperf import McperfSample, parse_mcperf_file
except ModuleNotFoundError:
    # When executed as a script: `python3 automation/plot_mcperf_qps_p95_timeseries.py ...`
    from mcperf import McperfSample, parse_mcperf_file

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import MultipleLocator
except ModuleNotFoundError as e:
    raise SystemExit(
        "Missing dependency: matplotlib. Install it with `python3 -m pip install matplotlib`."
    ) from e


def _cummax(values: list[float]) -> list[float]:
    out: list[float] = []
    current: float | None = None
    for v in values:
        if current is None or v > current:
            current = v
        out.append(current)
    return out


def _median_positive(values: list[float]) -> float | None:
    positives = [v for v in values if v > 0]
    if not positives:
        return None
    return statistics.median(positives)


def _guess_interval_ms(samples: list[McperfSample]) -> int:
    # Prefer ts_start deltas; fallback to ts_end-ts_start.
    starts = [s.ts_start_ms for s in samples if s.ts_start_ms is not None]
    if len(starts) >= 2:
        deltas = [b - a for a, b in zip(starts, starts[1:])]
        med = _median_positive([float(d) for d in deltas])
        if med is not None:
            return int(round(med))

    widths = [
        (s.ts_end_ms - s.ts_start_ms)
        for s in samples
        if s.ts_start_ms is not None and s.ts_end_ms is not None
    ]
    med_w = _median_positive([float(w) for w in widths])
    if med_w is not None:
        return int(round(med_w))

    # Last resort.
    return 15_000


def _is_epoch_ms(ts_ms: int) -> bool:
    # 2001-09-09 in ms is ~1e12.
    return ts_ms >= 1_000_000_000_000


def _build_x_axis(samples: list[McperfSample], interval_ms: int) -> tuple[list[object], str]:
    ts0 = samples[0].ts_start_ms
    if ts0 is None:
        # Should not happen because parser synthesizes timestamps, but keep safe.
        return ([0.0 + i * (interval_ms / 1000.0) for i in range(len(samples))], "Time [s]")

    if _is_epoch_ms(ts0):
        # Plot real wall-clock times; use local tz for readability.
        x = []
        for s in samples:
            ts = s.ts_start_ms if s.ts_start_ms is not None else ts0
            x.append(datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone())
        return x, "Timestamp"

    # Otherwise treat as relative ms.
    x0 = float(ts0)
    x = []
    for s in samples:
        ts = float(s.ts_start_ms if s.ts_start_ms is not None else ts0)
        x.append((ts - x0) / 1000.0)
    return x, "Time [s]"


def _build_offsets_s(samples: list[McperfSample]) -> list[float]:
    """Offsets from the first sample ts_start, in seconds."""
    ts0 = samples[0].ts_start_ms
    if ts0 is None:
        return [float(i) for i in range(len(samples))]
    base = float(ts0)
    out: list[float] = []
    for s in samples:
        ts = float(s.ts_start_ms if s.ts_start_ms is not None else ts0)
        out.append((ts - base) / 1000.0)
    return out


def _aggregate_candles(
    x_raw: list[object],
    offsets_s: list[float],
    p95_us: list[float],
    qps: list[float],
    bucket_s: int,
) -> tuple[list[object], list[float], list[float], list[float], list[float], list[float], list[float]]:
    """Aggregate into fixed-width time buckets.

    Returns:
      x_bucket, p95_min, p95_max, p95_med, qps_min, qps_max, qps_med
    """
    if bucket_s <= 0:
        raise ValueError("bucket_s must be > 0")
    if not x_raw:
        return [], [], [], [], [], [], []

    bucket_to_indices: dict[int, list[int]] = defaultdict(list)
    for i, t in enumerate(offsets_s):
        b = int(t // bucket_s)
        bucket_to_indices[b].append(i)

    buckets = sorted(bucket_to_indices.keys())
    x_bucket: list[object] = []
    p95_min: list[float] = []
    p95_max: list[float] = []
    p95_med: list[float] = []
    qps_min: list[float] = []
    qps_max: list[float] = []
    qps_med: list[float] = []

    x0 = x_raw[0]
    for b in buckets:
        idxs = bucket_to_indices[b]
        pv = [p95_us[i] for i in idxs]
        qv = [qps[i] for i in idxs]
        p95_min.append(min(pv))
        p95_max.append(max(pv))
        p95_med.append(statistics.median(pv))
        qps_min.append(min(qv))
        qps_max.append(max(qv))
        qps_med.append(statistics.median(qv))

        if isinstance(x0, datetime):
            x_bucket.append(x0 + timedelta(seconds=b * bucket_s))
        else:
            x_bucket.append(float(b * bucket_s))

    return x_bucket, p95_min, p95_max, p95_med, qps_min, qps_max, qps_med


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Plot mcperf time series with two y-axes: p95 latency and QPS. "
            "Works with logs that have per-row ts_start/ts_end or only file-level Timestamp start/end."
        )
    )
    ap.add_argument("--input", "-i", required=True, help="Path to an mcperf output .txt file")
    ap.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output image path (default: <input>_qps_p95.png)",
    )
    ap.add_argument(
        "--title",
        default=None,
        help="Optional plot title (default: input filename)",
    )
    ap.add_argument(
        "--use-target-qps",
        action="store_true",
        help="Explicitly plot target QPS instead of measured QPS (default: auto if present).",
    )
    ap.add_argument(
        "--monotonic",
        action="store_true",
        help="Enforce monotonic increase (cumulative max) for both series (off by default).",
    )
    ap.add_argument(
        "--no-monotonic",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    ap.add_argument(
        "--no-vlines",
        action="store_true",
        help="Do not draw vertical lines at each measurement timestamp.",
    )
    ap.add_argument(
        "--candles-seconds",
        type=int,
        default=0,
        help=(
            "Aggregate into candles of this many seconds (e.g. 60). "
            "Plots min/max ranges per bucket and a median marker, improving readability. "
            "Default: 0 (disabled)."
        ),
    )
    ap.add_argument(
        "--anchor-ms",
        type=int,
        default=None,
        help=(
            "Override the wall-clock anchor (epoch ms) used for x-axis alignment. "
            "Default: mcperf 'Timestamp start' floored to whole seconds."
        ),
    )
    args = ap.parse_args()

    in_path = Path(args.input)
    if args.output is None:
        out_path = in_path.with_suffix("")
        out_path = out_path.parent / f"{out_path.name}_qps_p95.png"
    else:
        out_path = Path(args.output)

    series = parse_mcperf_file(in_path)
    if not series.samples:
        raise SystemExit(f"No mcperf samples parsed from {in_path}")

    samples = series.samples
    interval_ms = _guess_interval_ms(samples)
    interval_s = interval_ms / 1000.0

    # Build x-axis.
    x_raw, x_label = _build_x_axis(samples, interval_ms)
    # Preserve original sample offsets while rebasing the origin.
    if x_raw and isinstance(x_raw[0], datetime):
        real_anchor_ms = series.overall_ts_start_ms or samples[0].ts_start_ms

        if real_anchor_ms is None:
            real_anchor_ms = 0

        if args.anchor_ms is not None:
            base_anchor_ms = args.anchor_ms
        else:
            # floor only the DISPLAY origin
            base_anchor_ms = (int(real_anchor_ms) // 1000) * 1000

        base_anchor_dt = datetime.fromtimestamp(
            base_anchor_ms / 1000.0,
            tz=timezone.utc
        ).astimezone()

        # preserve true offsets from original timestamps
        x_raw = []
        for s in samples:
            ts_ms = s.ts_start_ms if s.ts_start_ms is not None else real_anchor_ms
            delta_ms = ts_ms - real_anchor_ms
            x_raw.append(base_anchor_dt + timedelta(milliseconds=delta_ms))

        x_label = "Time (HH:MM:SS)"
    offsets_s = _build_offsets_s(samples)
    p95_us = [s.p95_us for s in samples]
    has_target = any(s.target_qps is not None for s in samples)
    use_target = args.use_target_qps or has_target
    if use_target:
        qps = [s.target_qps if s.target_qps is not None else s.qps for s in samples]
        qps_label = "Target QPS"
    else:
        qps = [s.qps for s in samples]
        qps_label = "Measured QPS"

    if args.monotonic:
        p95_us = _cummax(p95_us)
        qps = _cummax(qps)

    if args.candles_seconds and args.candles_seconds > 0:
        (
            x,
            p95_min,
            p95_max,
            p95_med,
            qps_min,
            qps_max,
            qps_med,
        ) = _aggregate_candles(x_raw, offsets_s, p95_us, qps, args.candles_seconds)
    else:
        # Prepend a baseline (0,0) point so both series start at 0.
        if x_raw:
            x0 = x_raw[0]
            if isinstance(x0, datetime):
                x = [x0 - timedelta(seconds=interval_s)] + list(x_raw)
            else:
                # Place baseline one measurement interval before the first sample.
                x = [float(x0) - interval_s] + list(x_raw)
            p95_us = [0.0] + p95_us
            qps = [0.0] + qps
        else:
            x = []

    fig, ax_lat = plt.subplots(figsize=(12, 4.5))
    ax_qps = ax_lat.twinx()

    if args.candles_seconds and args.candles_seconds > 0:
        # Candle (range) visualization: min/max as vlines + median as markers.
        ax_lat.vlines(x, p95_min, p95_max, color="#1f77b4", alpha=0.45, linewidth=2.0)
        ax_qps.vlines(x, qps_min, qps_max, color="#ff7f0e", alpha=0.45, linewidth=2.0)
        lat_line = ax_lat.plot(
            x,
            p95_med,
            color="#1f77b4",
            linewidth=1.2,
            marker="o",
            markersize=3,
            label="p95 latency (median)",
        )
        qps_line = ax_qps.plot(
            x,
            qps_med,
            color="#ff7f0e",
            linewidth=1.2,
            marker="o",
            markersize=3,
            label=f"{qps_label} (median)",
        )
    else:
        lat_line = ax_lat.plot(
            x,
            p95_us,
            color="#1f77b4",
            linewidth=1.8,
            marker="o",
            markersize=3,
            label="p95 latency (monotonic)" if args.monotonic else "p95 latency",
        )
        qps_line = ax_qps.plot(
            x,
            qps,
            color="#ff7f0e",
            linewidth=1.8,
            marker="o",
            markersize=3,
            label=f"{qps_label} (monotonic)" if args.monotonic else qps_label,
        )

    ax_lat.set_xlabel(x_label)
    ax_lat.set_ylabel("p95 latency [µs]")
    ax_qps.set_ylabel("QPS")
    ax_lat.set_ylim(bottom=0)
    ax_qps.set_ylim(bottom=0)

    title = args.title if args.title is not None else in_path.name
    subtitle = f"measurement interval ≈ {interval_s:.0f}s"
    ax_lat.set_title(f"{title} ({subtitle})")

    # Point out measurement timestamps (or candle bucket boundaries).
    if not args.no_vlines and len(x) > 1:
        alpha = 0.06 if not (args.candles_seconds and args.candles_seconds > 0) else 0.12
        for xv in x[1:]:
            ax_lat.axvline(xv, color="black", alpha=alpha, linewidth=0.8, zorder=0)

    # Timeline ticks.
    if x and isinstance(x[0], datetime):
        tick_s = int(max(1, round(interval_s)))
        if args.candles_seconds and args.candles_seconds > 0:
            tick_s = args.candles_seconds
        if args.candles_seconds and args.candles_seconds > 0:
            ax_lat.xaxis.set_major_locator(mdates.SecondLocator(interval=tick_s))
        else:
            # Label every 4 measurements to avoid overlapping labels.
            ax_lat.xaxis.set_major_locator(mdates.SecondLocator(interval=tick_s * 4))
            ax_lat.xaxis.set_minor_locator(mdates.SecondLocator(interval=tick_s))
        ax_lat.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        fig.autofmt_xdate(rotation=45, ha="right")
        ax_lat.tick_params(axis="x", labelsize=7)
        ax_lat.grid(True, which="major", linestyle=":", alpha=0.25)
        if not (args.candles_seconds and args.candles_seconds > 0):
            ax_lat.grid(True, which="minor", linestyle=":", alpha=0.12)
    else:
        tick_s = interval_s
        if args.candles_seconds and args.candles_seconds > 0:
            tick_s = float(args.candles_seconds)
        if args.candles_seconds and args.candles_seconds > 0:
            ax_lat.xaxis.set_major_locator(MultipleLocator(tick_s))
        else:
            ax_lat.xaxis.set_major_locator(MultipleLocator(tick_s * 4.0))
            ax_lat.xaxis.set_minor_locator(MultipleLocator(tick_s))
        ax_lat.tick_params(axis="x", labelsize=7)
        ax_lat.grid(True, which="major", linestyle=":", alpha=0.25)
        if not (args.candles_seconds and args.candles_seconds > 0):
            ax_lat.grid(True, which="minor", linestyle=":", alpha=0.12)

    lines = lat_line + qps_line
    labels = [l.get_label() for l in lines]
    ax_lat.legend(lines, labels, loc="upper left")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
