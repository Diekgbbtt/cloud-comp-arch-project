from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Event:
    ts: datetime
    kind: str
    job: str
    cores: set[int] | None


_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)\s+"
    r"(?P<kind>\w+)\s+(?P<job>\S+)(?:\s+(?P<args>.*))?$"
)
_CORES_RE = re.compile(r"\[(?P<cores>[0-9,\s]*)\]")


def _parse_event(line: str) -> Event | None:
    m = _LINE_RE.match(line.strip())
    if not m:
        return None

    ts = datetime.fromisoformat(m.group("ts"))
    kind = m.group("kind")
    job = m.group("job")
    args = m.group("args") or ""

    cores: set[int] | None = None
    if kind in {"start", "update_cores"}:
        cm = _CORES_RE.search(args)
        if cm:
            raw = cm.group("cores").strip()
            if raw:
                cores = {int(x.strip()) for x in raw.split(",") if x.strip()}
            else:
                cores = set()

    return Event(ts=ts, kind=kind, job=job, cores=cores)


def build_core_timeline(lines: list[str]) -> tuple[list[dict[str, str]], list[int]]:
    """Return (rows, cores_sorted).

    Each row is a dict with keys: timestamp, core0, core1, ...
    Snapshot is taken after each allocation-changing event.
    """

    events: list[Event] = []
    max_core = -1
    max_ts: datetime | None = None
    for line in lines:
        ev = _parse_event(line)
        if not ev:
            continue
        if max_ts is None or ev.ts > max_ts:
            max_ts = ev.ts
        if ev.cores:
            max_core = max(max_core, max(ev.cores))
        events.append(ev)

    if max_core < 0:
        max_core = 0
    cores_sorted = list(range(max_core + 1))

    core_to_job: dict[int, str] = {c: "idle" for c in cores_sorted}
    memcached_cores: set[int] = set()
    memcached_started = False

    job_running_cores: dict[str, set[int]] = {}
    job_paused_cores: dict[str, set[int]] = {}

    def snapshot(ts: datetime) -> dict[str, str]:
        row = {"timestamp": ts.isoformat()}
        for c in cores_sorted:
            row[f"core{c}"] = core_to_job.get(c, "idle")
        return row

    def release_cores(cores: set[int]) -> None:
        for c in cores:
            if memcached_started and c in memcached_cores:
                core_to_job[c] = "memcached"
            else:
                core_to_job[c] = "idle"

    rows: list[dict[str, str]] = []
    for ev in sorted(events, key=lambda e: e.ts):
        # Ignore non-allocation events
        if ev.kind == "custom":
            continue
        if ev.job == "scheduler":
            # Scheduler is not pinned to a specific core set in logs.
            # Still keep its timestamp for the final snapshot (handled below).
            continue

        changed = False

        if ev.kind == "start" and ev.cores is not None:
            cores = set(ev.cores)
            if ev.job == "memcached":
                memcached_started = True
                memcached_cores = cores
                for c in cores:
                    if core_to_job.get(c, "idle") in {"idle", "memcached"}:
                        core_to_job[c] = "memcached"
                changed = True
            else:
                # Start job on its assigned cores.
                # Remove these cores from whoever currently owns them.
                for c in cores:
                    prev = core_to_job.get(c, "idle")
                    if prev != ev.job:
                        core_to_job[c] = ev.job
                job_running_cores[ev.job] = cores
                job_paused_cores.pop(ev.job, None)
                changed = True

        elif ev.kind == "update_cores" and ev.cores is not None:
            new_cores = set(ev.cores)
            if ev.job == "memcached":
                old_cores = set(memcached_cores)
                memcached_started = True
                memcached_cores = new_cores

                # Remove memcached from cores it no longer owns (if it was shown there)
                for c in old_cores - new_cores:
                    if core_to_job.get(c) == "memcached":
                        core_to_job[c] = "idle"

                # Apply memcached on its current cores, but don't preempt running jobs.
                for c in new_cores:
                    if core_to_job.get(c, "idle") in {"idle", "memcached"}:
                        core_to_job[c] = "memcached"
                changed = True
            else:
                old_cores = set(job_running_cores.get(ev.job, set()))
                removed = old_cores - new_cores
                added = new_cores - old_cores

                for c in removed:
                    if core_to_job.get(c) == ev.job:
                        if memcached_started and c in memcached_cores:
                            core_to_job[c] = "memcached"
                        else:
                            core_to_job[c] = "idle"

                for c in added:
                    core_to_job[c] = ev.job

                job_running_cores[ev.job] = new_cores
                changed = True

        elif ev.kind == "pause":
            # Paused job gives its cores to memcached, and remembers them.
            cores = set(job_running_cores.get(ev.job, set()))
            if cores:
                job_paused_cores[ev.job] = set(cores)
                job_running_cores[ev.job] = set()
                for c in cores:
                    if memcached_started:
                        core_to_job[c] = "memcached"
                    else:
                        core_to_job[c] = "idle"
                changed = True

        elif ev.kind == "unpause":
            # Unpaused job reclaims the same cores.
            cores = set(job_paused_cores.get(ev.job, set()))
            if cores:
                for c in cores:
                    core_to_job[c] = ev.job
                job_running_cores[ev.job] = set(cores)
                job_paused_cores.pop(ev.job, None)
                changed = True

        elif ev.kind == "end":
            # Release any running or paused cores.
            running = set(job_running_cores.pop(ev.job, set()))
            paused = set(job_paused_cores.pop(ev.job, set()))
            cores = running | paused
            if cores:
                release_cores(cores)
                changed = True

        if changed:
            rows.append(snapshot(ev.ts))

    # Add a final snapshot at the end of the run so the last interval has duration.
    if max_ts is not None:
        if not rows or rows[-1]["timestamp"] != max_ts.isoformat():
            rows.append(snapshot(max_ts))

    return rows, cores_sorted


def write_csv(rows: list[dict[str, str]], cores_sorted: list[int], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["timestamp"] + [f"core{c}" for c in cores_sorted]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def to_delta_rows(rows: list[dict[str, str]], cores_sorted: list[int]) -> list[dict[str, str]]:
    """Convert full snapshot rows into delta rows.

    Delta rows keep the timestamp; each core cell is filled only
    if the core's allocation changed compared to the previous row.
    """

    if not rows:
        return []

    core_cols = [f"core{c}" for c in cores_sorted]
    out: list[dict[str, str]] = []
    prev = None
    for row in rows:
        d: dict[str, str] = {"timestamp": row["timestamp"]}
        if prev is None:
            for col in core_cols:
                d[col] = row.get(col, "idle")
        else:
            for col in core_cols:
                cur = row.get(col, "idle")
                old = prev.get(col, "idle")
                d[col] = cur if cur != old else ""
        if prev is None or any(d[col] for col in core_cols):
            out.append(d)
        prev = row
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Parse part4 scheduler logs into per-core allocation timelines.")
    p.add_argument("inputs", nargs="+", type=Path, help="One or more jobs_*.txt log files")
    p.add_argument("--out-dir", type=Path, required=True, help="Output directory for CSV timelines")
    p.add_argument("--prefix", type=str, default="core_timeline", help="Output file prefix")
    p.add_argument(
        "--representation",
        choices=["full", "delta", "both"],
        default="both",
        help="Write full snapshot rows, delta rows, or both",
    )
    args = p.parse_args()

    for i, in_path in enumerate(args.inputs, start=1):
        lines = in_path.read_text().splitlines()
        rows, cores_sorted = build_core_timeline(lines)
        if args.representation in {"full", "both"}:
            out_path = args.out_dir / f"{args.prefix}_run{i}.csv"
            write_csv(rows, cores_sorted, out_path)
        if args.representation in {"delta", "both"}:
            out_path = args.out_dir / f"{args.prefix}_run{i}_delta.csv"
            write_csv(to_delta_rows(rows, cores_sorted), cores_sorted, out_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
