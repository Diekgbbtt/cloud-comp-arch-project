"""Plot per-core utilization over time for Part 4 scheduler runs.

Reads the relational core allocation CSVs produced by
`automation/parse_part4_core_timeline.py` (full snapshot representation), and
renders a Gantt-style chart: one horizontal bar per core, one color per job.

Pauses are already modeled in the CSVs via memcached takeover of paused cores.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter


DEFAULT_COLORS = {
    "barnes": "#AACCCA",
    "blackscholes": "#CCA000",
    "canneal": "#CCCCAA",
    "freqmine": "#0CCA00",
    "radix": "#00CCA0",
    "streamcluster": "#CCACCA",
    "vips": "#CC0A00",
    "memcached": "#888888",
    "idle": "#FFFFFF",
}


@dataclass(frozen=True)
class Segment:
    core: int
    start_s: float
    duration_s: float
    job: str


def _read_core_allocation_csv(path: Path) -> tuple[list[datetime], list[dict[int, str]]]:
    with path.open(newline="") as f:
        r = csv.DictReader(f)
        timestamps: list[datetime] = []
        snapshots: list[dict[int, str]] = []
        for row in r:
            ts = datetime.fromisoformat(row["timestamp"])
            snap: dict[int, str] = {}
            for k, v in row.items():
                if not k.startswith("core"):
                    continue
                core = int(k.replace("core", ""))
                snap[core] = (v or "idle").strip() or "idle"
            timestamps.append(ts)
            snapshots.append(snap)
    return timestamps, snapshots


def _parse_mcperf_start_ms(path: Path) -> int:
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("Timestamp start:"):
                return int(line.split(":", 1)[1].strip())
    raise ValueError(f"Could not find 'Timestamp start:' in {path}")


def _infer_run_id_from_csv_name(path: Path) -> str | None:
    m = re.search(r"run(\d+)", path.stem)
    return m.group(1) if m else None


def _parse_jobs_scheduler_anchor(path: Path) -> tuple[datetime, int]:
    scheduler_iso: datetime | None = None
    scheduler_start_ms: int | None = None

    with path.open() as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            parts = line.split()
            if len(parts) < 3:
                continue

            # Format examples:
            # 2026-... start scheduler
            # 2026-... custom scheduler_start 1778526682077
            ts_str = parts[0]
            kind = parts[1]

            if kind == "start" and len(parts) >= 3 and parts[2] == "scheduler":
                try:
                    scheduler_iso = datetime.fromisoformat(ts_str)
                except ValueError:
                    pass

            if kind == "custom" and len(parts) >= 4 and parts[2] == "scheduler_start":
                try:
                    scheduler_start_ms = int(parts[3])
                except ValueError:
                    pass

            if scheduler_iso is not None and scheduler_start_ms is not None:
                return scheduler_iso, scheduler_start_ms

    raise ValueError(f"Could not find scheduler ISO + epoch-ms anchor in {path}")


def _parse_jobs_scheduler_end_ms(path: Path) -> int | None:
    with path.open() as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 4 and parts[1] == "custom" and parts[2] == "scheduler_end":
                try:
                    return int(parts[3])
                except ValueError:
                    return None
    return None


def _parse_jobs_latest_job_end_ms(path: Path, *, iso_anchor: tuple[datetime, int]) -> int | None:
    anchor_iso, anchor_epoch_ms = iso_anchor
    latest_ms: int | None = None

    with path.open() as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            if parts[1] != "end":
                continue

            job = parts[2]
            if job in {"scheduler", "memcached"}:
                continue

            try:
                iso_ts = datetime.fromisoformat(parts[0])
            except ValueError:
                continue

            ev_ms = _iso_dt_to_epoch_ms(iso_ts, anchor_iso=anchor_iso, anchor_epoch_ms=anchor_epoch_ms)
            if latest_ms is None or ev_ms > latest_ms:
                latest_ms = ev_ms

    return latest_ms


def _format_abs_gmt(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%H:%M:%S")


def _iso_dt_to_epoch_ms(ts: datetime, *, anchor_iso: datetime, anchor_epoch_ms: int) -> int:
    return anchor_epoch_ms + int((ts - anchor_iso).total_seconds() * 1000)


def _build_segments(
    timestamps: list[datetime],
    snapshots: list[dict[int, str]],
    *,
    anchor_start_ms: int | None = None,
    iso_anchor: tuple[datetime, int] | None = None,
) -> tuple[list[Segment], list[int]]:
    if len(timestamps) < 2:
        return [], []

    cores = sorted({c for snap in snapshots for c in snap.keys()})
    def to_epoch_ms(ts: datetime) -> int:
        if iso_anchor is not None:
            anchor_iso, anchor_epoch_ms = iso_anchor
            return _iso_dt_to_epoch_ms(ts, anchor_iso=anchor_iso, anchor_epoch_ms=anchor_epoch_ms)
        return int(ts.timestamp() * 1000)

    t0_ms = to_epoch_ms(timestamps[0])
    anchor_ms = anchor_start_ms if anchor_start_ms is not None else t0_ms

    segments: list[Segment] = []
    for i in range(len(timestamps) - 1):
        ts = timestamps[i]
        next_ts = timestamps[i + 1]
        ts_ms = to_epoch_ms(ts)
        next_ts_ms = to_epoch_ms(next_ts)
        start_s = (ts_ms - anchor_ms) / 1000.0
        dur_s = (next_ts_ms - ts_ms) / 1000.0
        if dur_s <= 0:
            continue

        snap = snapshots[i]
        for core in cores:
            job = snap.get(core, "idle")
            segments.append(Segment(core=core, start_s=start_s, duration_s=dur_s, job=job))

    # Merge adjacent segments with same job per core
    merged: list[Segment] = []
    for core in cores:
        core_segs = [s for s in segments if s.core == core]
        cur: Segment | None = None
        for s in core_segs:
            if cur is None:
                cur = s
                continue
            if s.job == cur.job and abs((cur.start_s + cur.duration_s) - s.start_s) < 1e-9:
                cur = Segment(core=cur.core, start_s=cur.start_s, duration_s=cur.duration_s + s.duration_s, job=cur.job)
            else:
                merged.append(cur)
                cur = s
        if cur is not None:
            merged.append(cur)

    return merged, cores


def plot_core_utilization(
    segments: list[Segment],
    cores: list[int],
    out_path: Path,
    title: str | None = None,
    colors: dict[str, str] | None = None,
    show_idle: bool = False,
    axis_time_labels: tuple[float, str, float, str] | None = None,
    hide_zero_tick: bool = False,
) -> None:
    colors = colors or dict(DEFAULT_COLORS)

    fig_h = max(3.0, 0.55 * len(cores) + 1.0)
    fig, ax = plt.subplots(figsize=(12, fig_h))

    y_index = {core: i for i, core in enumerate(cores)}

    jobs_in_plot: set[str] = set()
    for s in segments:
        if (not show_idle) and s.job == "idle":
            continue
        jobs_in_plot.add(s.job)
        y = y_index[s.core]
        face = colors.get(s.job, "#CCCCCC")
        hatch = "//" if s.job == "memcached" else None
        ax.broken_barh(
            [(s.start_s, s.duration_s)],
            (y - 0.4, 0.8),
            facecolors=face,
            edgecolor="black",
            linewidth=0.6,
            hatch=hatch,
        )

    ax.set_yticks(range(len(cores)))
    ax.set_yticklabels([f"core {c}" for c in cores])
    ax.invert_yaxis()
    ax.set_xlabel("time [s]")
    if title:
        ax.set_title(title)
    ax.grid(axis="x", linestyle=":", alpha=0.5)

    if hide_zero_tick:
        def _fmt_tick(x: float, _pos: int) -> str:
            if abs(x) < 1e-9:
                return ""
            if abs(x - round(x)) < 1e-9:
                return str(int(round(x)))
            return f"{x:g}"

        ax.xaxis.set_major_formatter(FuncFormatter(_fmt_tick))

    if axis_time_labels is not None:
        start_x, start_txt, end_x, end_txt = axis_time_labels
        # Place the labels along the x-axis, at the measurement points.
        # y is in axes coords (0 at axis line); negative moves below the axis.
        tx = ax.get_xaxis_transform()
        ax.text(
            start_x,
            -0.10,
            start_txt,
            transform=tx,
            ha="left",
            va="top",
            rotation=45,
            fontsize=9,
            clip_on=False,
        )
        ax.text(
            end_x,
            -0.10,
            end_txt,
            transform=tx,
            ha="right",
            va="top",
            rotation=45,
            fontsize=9,
            clip_on=False,
        )

    legend_handles: list[Patch] = []
    for job in sorted(jobs_in_plot):
        face = colors.get(job, "#CCCCCC")
        hatch = "//" if job == "memcached" else None
        legend_handles.append(Patch(facecolor=face, edgecolor="black", hatch=hatch, label=job))

    if legend_handles:
        ax.legend(handles=legend_handles, loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=9)

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description="Plot Part 4 per-core utilization over time from core allocation CSVs.")
    p.add_argument("inputs", nargs="+", type=Path, help="core_allocation_run*.csv files (full snapshot)")
    p.add_argument("--out-dir", type=Path, default=Path("."), help="Directory for output PNGs")
    p.add_argument("--show-idle", action="store_true", help="Render idle segments too")
    p.add_argument(
        "--mcperf-dir",
        type=Path,
        default=None,
        help="Directory containing mcperf_<run>.txt (defaults to each input's parent)",
    )
    p.add_argument(
        "--anchor",
        choices=["scheduler", "mcperf", "first"],
        default="mcperf",
        help="Anchor x=0 to scheduler_start (jobs log), mcperf Timestamp start, or first CSV event",
    )
    args = p.parse_args()

    for in_path in args.inputs:
        timestamps, snapshots = _read_core_allocation_csv(in_path)
        run_id = _infer_run_id_from_csv_name(in_path)
        mcperf_dir = args.mcperf_dir if args.mcperf_dir is not None else in_path.parent
        anchor_ms: int | None = None
        iso_anchor: tuple[datetime, int] | None = None
        mcperf_start_ms: int | None = None
        scheduler_end_ms: int | None = None
        latest_job_end_ms: int | None = None

        if run_id is not None:
            jobs_path = in_path.parent / f"jobs_{run_id}.txt"
            if jobs_path.exists():
                iso_anchor = _parse_jobs_scheduler_anchor(jobs_path)
                scheduler_end_ms = _parse_jobs_scheduler_end_ms(jobs_path)
                if iso_anchor is not None:
                    latest_job_end_ms = _parse_jobs_latest_job_end_ms(jobs_path, iso_anchor=iso_anchor)

            if args.anchor == "scheduler" and iso_anchor is not None:
                anchor_ms = iso_anchor[1]
            elif args.anchor == "mcperf":
                mcperf_path = mcperf_dir / f"mcperf_{run_id}.txt"
                if mcperf_path.exists():
                    mcperf_start_ms = _parse_mcperf_start_ms(mcperf_path)
                    anchor_ms = mcperf_start_ms
            elif args.anchor == "first":
                anchor_ms = None

            if mcperf_start_ms is None:
                mcperf_path = mcperf_dir / f"mcperf_{run_id}.txt"
                if mcperf_path.exists():
                    mcperf_start_ms = _parse_mcperf_start_ms(mcperf_path)

        segments, cores = _build_segments(
            timestamps,
            snapshots,
            anchor_start_ms=anchor_ms,
            iso_anchor=iso_anchor,
        )

        # Compute start/end labels (absolute times in GMT) and place them at their x positions.
        def to_epoch_ms(ts: datetime) -> int:
            if iso_anchor is not None:
                anchor_iso, anchor_epoch_ms = iso_anchor
                return _iso_dt_to_epoch_ms(ts, anchor_iso=anchor_iso, anchor_epoch_ms=anchor_epoch_ms)
            return int(ts.timestamp() * 1000)

        if timestamps:
            anchor_used_ms = anchor_ms
            if anchor_used_ms is None:
                anchor_used_ms = to_epoch_ms(timestamps[0])

            start_label_ms = mcperf_start_ms
            # Latest job end time: mcperf_start + makespan, where makespan is measured from scheduler_start.
            end_label_ms: int | None = None
            if start_label_ms is not None and iso_anchor is not None and latest_job_end_ms is not None:
                scheduler_start_ms = iso_anchor[1]
                makespan_ms = latest_job_end_ms - scheduler_start_ms
                end_label_ms = start_label_ms + makespan_ms
            if end_label_ms is None:
                end_label_ms = scheduler_end_ms
            if end_label_ms is None:
                end_label_ms = to_epoch_ms(timestamps[-1])

            axis_time_labels: tuple[float, str, float, str] | None = None
            if start_label_ms is not None and end_label_ms is not None:
                # Replace the 0 tick label with the start time.
                start_x = 0.0
                # End label is placed at the makespan (seconds) while numeric ticks (e.g. 800) remain.
                end_x = (end_label_ms - anchor_used_ms) / 1000.0
                axis_time_labels = (start_x, _format_abs_gmt(start_label_ms), end_x, _format_abs_gmt(end_label_ms))
        else:
            axis_time_labels = None

        out_name = in_path.stem + "_utilization.png"
        out_path = args.out_dir / out_name
        # User-facing caption: show absolute start/end timestamps.
        # Start is from mcperf logs (epoch-ms). End is last job (scheduler_end epoch-ms when present).
        start_label = str(mcperf_start_ms) if mcperf_start_ms is not None else "unknown"
        end_label = str(scheduler_end_ms) if scheduler_end_ms is not None else "unknown"
        plot_core_utilization(
            segments,
            cores,
            out_path,
            title=in_path.stem,
            show_idle=args.show_idle,
            axis_time_labels=axis_time_labels,
            hide_zero_tick=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
