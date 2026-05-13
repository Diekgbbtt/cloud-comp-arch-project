from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class McperfSample:
    p95_us: float
    qps: float
    target_qps: float | None
    ts_start_ms: int | None
    ts_end_ms: int | None


@dataclass(frozen=True)
class McperfSeries:
    samples: list[McperfSample]
    overall_ts_start_ms: int | None = None
    overall_ts_end_ms: int | None = None


def _try_parse_int(s: str) -> int | None:
    try:
        return int(s)
    except Exception:
        return None


def _try_parse_float(s: str) -> float | None:
    try:
        return float(s)
    except Exception:
        return None


def _normalize_header_cols(header_tokens: list[str]) -> dict[str, int]:
    # Normalize to lowercase and strip a leading '#'.
    cols: list[str] = []
    for tok in header_tokens:
        tok = tok.strip()
        if tok.startswith("#"):
            tok = tok[1:]
        cols.append(tok.lower())
    return {c: i for i, c in enumerate(cols)}


def _parse_sample_with_header(tokens: list[str], col_idx: dict[str, int]) -> McperfSample | None:
    if not tokens or tokens[0] != "read":
        return None

    def get(col: str) -> str | None:
        i = col_idx.get(col)
        if i is None or i >= len(tokens):
            return None
        return tokens[i]

    p95_s = get("p95")
    qps_s = get("qps")
    target_s = get("target")
    ts_start_s = get("ts_start")
    ts_end_s = get("ts_end")

    p95_us = _try_parse_float(p95_s) if p95_s is not None else None
    qps = _try_parse_float(qps_s) if qps_s is not None else None
    target_qps = _try_parse_float(target_s) if target_s is not None else None

    if p95_us is None or qps is None:
        return None

    return McperfSample(
        p95_us=p95_us,
        qps=qps,
        target_qps=target_qps,
        ts_start_ms=_try_parse_int(ts_start_s) if ts_start_s is not None else None,
        ts_end_ms=_try_parse_int(ts_end_s) if ts_end_s is not None else None,
    )


def _parse_sample_headerless(tokens: list[str]) -> McperfSample | None:
    # Supported headerless layouts:
    # - With timestamps (common in part3):
    #   read <avg..p9999> <QPS> <target> <ts_start> <ts_end>
    # - Without timestamps (common in part4 mcperf_*.txt):
    #   read <avg..p9999> <QPS> <target>
    if not tokens or tokens[0] != "read":
        return None

    # Count includes the leading 'read'
    if len(tokens) == 20:
        # read + 19 values
        p95_us = _try_parse_float(tokens[12])
        qps = _try_parse_float(tokens[16])
        target_qps = _try_parse_float(tokens[17])
        ts_start_ms = _try_parse_int(tokens[18])
        ts_end_ms = _try_parse_int(tokens[19])
    elif len(tokens) == 18:
        # read + 17 values
        p95_us = _try_parse_float(tokens[12])
        qps = _try_parse_float(tokens[16])
        target_qps = _try_parse_float(tokens[17])
        ts_start_ms = None
        ts_end_ms = None
    else:
        # Unknown layout.
        return None

    if p95_us is None or qps is None:
        return None
    return McperfSample(
        p95_us=p95_us,
        qps=qps,
        target_qps=target_qps,
        ts_start_ms=ts_start_ms,
        ts_end_ms=ts_end_ms,
    )


def parse_mcperf_file(path: str | Path) -> McperfSeries:
    """Parse an mcperf output file.

    Supports:
    - Per-row timestamps via 'ts_start'/'ts_end' columns
    - No per-row timestamps, but file-level 'Timestamp start/end'
    - Headerless files where each row starts with 'read'

    Returns a series of samples; if per-row timestamps are missing, they are
    synthesized using the file-level timestamps when available (or a 1s step
    fallback).
    """

    path = Path(path)
    overall_start: int | None = None
    overall_end: int | None = None
    col_idx: dict[str, int] | None = None
    samples: list[McperfSample] = []

    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line:
            continue

        if line.lower().startswith("timestamp start:"):
            # Example: Timestamp start: 1778526681376
            overall_start = _try_parse_int(line.split(":", 1)[1].strip())
            continue
        if line.lower().startswith("timestamp end:"):
            overall_end = _try_parse_int(line.split(":", 1)[1].strip())
            continue

        if line.lower().startswith("#type"):
            col_idx = _normalize_header_cols(line.split())
            continue

        tokens = line.split()
        if not tokens or tokens[0] != "read":
            continue

        if col_idx is not None:
            sample = _parse_sample_with_header(tokens, col_idx)
        else:
            sample = _parse_sample_headerless(tokens)
        if sample is not None:
            samples.append(sample)

    samples = _synthesize_missing_timestamps(samples, overall_start, overall_end)
    return McperfSeries(samples=samples, overall_ts_start_ms=overall_start, overall_ts_end_ms=overall_end)


def _synthesize_missing_timestamps(
    samples: list[McperfSample],
    overall_start_ms: int | None,
    overall_end_ms: int | None,
) -> list[McperfSample]:
    if not samples:
        return samples
    if all(s.ts_start_ms is not None and s.ts_end_ms is not None for s in samples):
        return samples

    n = len(samples)
    if overall_start_ms is not None and overall_end_ms is not None and overall_end_ms > overall_start_ms:
        total = overall_end_ms - overall_start_ms
        # mcperf rows are (roughly) evenly-spaced summary windows.
        step = total / n
        out: list[McperfSample] = []
        for i, s in enumerate(samples):
            if s.ts_start_ms is not None and s.ts_end_ms is not None:
                out.append(s)
                continue
            ts_s = int(round(overall_start_ms + i * step))
            ts_e = int(round(overall_start_ms + (i + 1) * step))
            out.append(
                McperfSample(
                    p95_us=s.p95_us,
                    qps=s.qps,
                    target_qps=s.target_qps,
                    ts_start_ms=ts_s,
                    ts_end_ms=ts_e,
                )
            )
        return out

    # Fallback: monotonic 1s windows starting at 0.
    out = []
    for i, s in enumerate(samples):
        if s.ts_start_ms is not None and s.ts_end_ms is not None:
            out.append(s)
            continue
        out.append(
            McperfSample(
                p95_us=s.p95_us,
                qps=s.qps,
                target_qps=s.target_qps,
                ts_start_ms=i * 1000,
                ts_end_ms=(i + 1) * 1000,
            )
        )
    return out


def iter_p95_qps(samples: Iterable[McperfSample]) -> tuple[list[float], list[float]]:
    p95 = [s.p95_us for s in samples]
    qps = [s.qps for s in samples]
    return p95, qps


def iter_time_s(samples: Iterable[McperfSample]) -> list[float]:
    s_list = list(samples)
    if not s_list:
        return []
    t0 = s_list[0].ts_start_ms or 0
    out: list[float] = []
    for s in s_list:
        ts_s = s.ts_start_ms if s.ts_start_ms is not None else t0
        ts_e = s.ts_end_ms if s.ts_end_ms is not None else ts_s
        out.append(((ts_s + ts_e) / 2 - t0) / 1000.0)
    return out


def load_memcache_p95_intervals(path: str | Path) -> list[tuple[int, int, float]]:
    """Return (ts_start_ms, ts_end_ms, p95_us) tuples for plotting bars."""
    series = parse_mcperf_file(path)
    out: list[tuple[int, int, float]] = []
    for s in series.samples:
        if s.ts_start_ms is None or s.ts_end_ms is None:
            continue
        out.append((s.ts_start_ms, s.ts_end_ms, s.p95_us))
    return out
