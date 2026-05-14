#!/usr/bin/env python3
"""Extract a memcache-latency sub-frame aligned to a run's job makespan.

Workflow (per run):
- Read pods JSON dump.
- Find `parsec-streamcluster` startTime and `parsec-radix` completion time.
- Compute desired window: [start-buffer, end+buffer].
- Align to closest available memcache report interval boundaries while
  *preserving coverage*:
  - aligned_start = latest interval start <= desired_start (else earliest start)
  - aligned_end   = earliest interval end  >= desired_end   (else latest end)
- Write the selected memcache latency lines for intervals inside the aligned
  window.

This script is intentionally dependency-free.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


def parse_rfc3339(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def dt_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def pod_completion_time(pod: dict) -> Optional[datetime]:
    status = pod.get("status") or {}

    best: Optional[datetime] = None

    # 1) conditions with reason PodCompleted
    for c in (status.get("conditions") or []):
        if c.get("reason") == "PodCompleted":
            t = parse_rfc3339(c.get("lastTransitionTime"))
            if t and (best is None or t > best):
                best = t

    # 2) containerStatuses[*].state.terminated.finishedAt
    for cs in (status.get("containerStatuses") or []):
        term = ((cs.get("state") or {}).get("terminated") or {})
        t = parse_rfc3339(term.get("finishedAt"))
        if t and (best is None or t > best):
            best = t

    # 3) fallback: max lastTransitionTime
    if best is None:
        for c in (status.get("conditions") or []):
            t = parse_rfc3339(c.get("lastTransitionTime"))
            if t and (best is None or t > best):
                best = t

    return best


@dataclass(frozen=True)
class Interval:
    start_ms: int
    end_ms: int
    line: str


def load_latency_intervals(path: Path) -> list[Interval]:
    intervals: list[Interval] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            start_ms = int(parts[-2])
            end_ms = int(parts[-1])
        except ValueError:
            continue
        intervals.append(Interval(start_ms=start_ms, end_ms=end_ms, line=line))
    return intervals


def floor_start(starts: Iterable[int], target: int) -> int:
    eligible = [s for s in starts if s <= target]
    return max(eligible) if eligible else min(starts)


def ceil_end(ends: Iterable[int], target: int) -> int:
    eligible = [e for e in ends if e >= target]
    return min(eligible) if eligible else max(ends)


def find_pod_by_job(items: list[dict], job_name: str) -> Optional[dict]:
    for pod in items:
        if pod.get("kind") != "Pod":
            continue
        labels = (pod.get("metadata") or {}).get("labels") or {}
        if labels.get("job-name") == job_name:
            return pod
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pods-json", required=True, type=Path)
    parser.add_argument("--latencies", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--buffer-seconds", type=int, default=40)
    args = parser.parse_args()

    data = json.loads(args.pods_json.read_text(encoding="utf-8"))
    items = data.get("items") or []

    stream = find_pod_by_job(items, "parsec-streamcluster")
    radix = find_pod_by_job(items, "parsec-radix")
    if not stream or not radix:
        raise SystemExit("Missing parsec-streamcluster or parsec-radix pod")

    stream_start = parse_rfc3339((stream.get("status") or {}).get("startTime"))
    radix_end = pod_completion_time(radix)

    if not stream_start:
        raise SystemExit("Missing streamcluster status.startTime")
    if not radix_end:
        raise SystemExit("Cannot determine radix completion time")

    desired_start = dt_to_ms(stream_start) - args.buffer_seconds * 1000
    desired_end = dt_to_ms(radix_end) + args.buffer_seconds * 1000

    intervals = load_latency_intervals(args.latencies)
    if not intervals:
        raise SystemExit("No intervals parsed from latency report")

    starts = [i.start_ms for i in intervals]
    ends = [i.end_ms for i in intervals]

    aligned_start = floor_start(starts, desired_start)
    aligned_end = ceil_end(ends, desired_end)

    selected = [
        i.line
        for i in intervals
        if i.start_ms >= aligned_start and i.end_ms <= aligned_end
    ]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(selected) + ("\n" if selected else ""), encoding="utf-8")

    pre_buffer_s = (dt_to_ms(stream_start) - aligned_start) / 1000
    post_buffer_s = (aligned_end - dt_to_ms(radix_end)) / 1000

    print("desired window:", ms_to_dt(desired_start).isoformat(), ms_to_dt(desired_end).isoformat())
    print("aligned window:", ms_to_dt(aligned_start).isoformat(), ms_to_dt(aligned_end).isoformat())
    print("pre buffer seconds:", pre_buffer_s)
    print("post buffer seconds:", post_buffer_s)
    print("lines:", len(selected))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
