#!/usr/bin/env python3
"""
Parse raw PARSEC/SPLASH2x benchmark output files into a CSV.

Usage:
    python3 automation/parse_parsec_results.py part2a_barnes.txt [part2a_blackscholes.txt ...]
    python3 automation/parse_parsec_results.py part2a_*.txt

Output: results are written to parsec_results.csv (or specify with -o).
"""

import argparse
import csv
import re
import sys
from pathlib import Path


def parse_time(time_str):
    """Convert time strings like '3m33.887s' or '0.456s' to seconds."""
    match = re.match(r"(?:(\d+)m)?(\d+(?:\.\d+)?)s", time_str)
    if not match:
        return None
    minutes = int(match.group(1)) if match.group(1) else 0
    seconds = float(match.group(2))
    return round(minutes * 60 + seconds, 3)


def parse_parsec_output(filepath):
    """Extract metrics from a single PARSEC output file."""
    path = Path(filepath)
    text = path.read_text()

    result = {"file": Path(filepath).name}

    # Benchmark name: "Running benchmark splash2x.barnes [1]"
    m = re.search(r"Running benchmark (\S+)\s+\[(\d+)\]", text)
    if m:
        suite_bench = m.group(1)
        parts = suite_bench.split(".", 1)
        result["suite"] = parts[0] if len(parts) == 2 else ""
        result["benchmark"] = parts[1] if len(parts) == 2 else parts[0]

    # Input size: either unpacked explicitly or mentioned in "No archive" logs.
    m = re.search(r"(?:Unpacking benchmark input|No archive for input) '(\w+)'", text)
    if m:
        result["input_size"] = m.group(1)

    # Thread count from the run command line. Different PARSEC apps place the
    # thread count in different positions, so handle the common shapes we see
    # in this project.
    m = re.search(r"^\[PARSEC\] Running 'time ([^']+)'", text, re.MULTILINE)
    command = m.group(1) if m else ""
    if command:
        # SPLASH2x wrappers like: run.sh 4 native
        m = re.search(r"\b(\d+)\s+(?:native|simlarge|simmedium|simsmall|simdev|test)\s*$", command)
        if m:
            result["threads"] = int(m.group(1))
        elif result.get("benchmark") in {"blackscholes", "canneal"}:
            # These apps pass thread count as the first integer argument.
            m = re.search(r"\S+\s+(\d+)(?:\s|$)", command)
            if m:
                result["threads"] = int(m.group(1))
        elif result.get("benchmark") == "streamcluster":
            # streamcluster puts threads at the end of the command.
            m = re.search(r"\s(\d+)\s*$", command)
            if m:
                result["threads"] = int(m.group(1))

    if "threads" not in result:
        # fallback: look for "-n X" in the run command or the internal "NPROC" field
        m = re.search(r"-n\s+(\d+)", command)
        if m:
            result["threads"] = int(m.group(1))
        else:
            m = re.search(r"NPROC\s+(\d+)", text)
            if m:
                result["threads"] = int(m.group(1))

    # Part 2b filenames are of the form benchmark_run<threads>.txt, and some
    # PARSEC apps do not echo thread count in a parseable way in the logs.
    if "threads" not in result and "results/part2/b/raw" in str(path):
        m = re.match(r".+_run(\d+)\.txt$", path.name)
        if m:
            result["threads"] = int(m.group(1))

    # real/user/sys from the time command
    for label in ("real", "user", "sys"):
        m = re.search(rf"^{label}\s+([\dm.]+s)\s*$", text, re.MULTILINE)
        if m:
            result[f"{label}_sec"] = parse_time(m.group(1))

    # Compute CPU utilization: user / real
    if result.get("real_sec") and result.get("user_sec") and result["real_sec"] > 0:
        result["cpu_util"] = round(result["user_sec"] / result["real_sec"], 3)

    # KEY = VALUE pairs (internal timings like COMPUTETIME, FORCECALCTIME, etc.)
    # Match lines like: "COMPUTETIME   =    211562750"
    internal = {}
    for m in re.finditer(r"^(\w+TIME\w*)\s*=\s*(\d+)\s*$", text, re.MULTILINE):
        internal[m.group(1)] = int(m.group(2))
    if internal:
        result["internal_timings"] = internal

    return result


def main():
    parser = argparse.ArgumentParser(description="Parse PARSEC benchmark output files to CSV")
    parser.add_argument("files", nargs="+", help="Raw output text files to parse")
    parser.add_argument("-o", "--output", default="parsec_results.csv", help="Output CSV path (default: parsec_results.csv)")
    args = parser.parse_args()

    records = []
    all_internal_keys = set()

    for f in args.files:
        try:
            rec = parse_parsec_output(f)
            if "internal_timings" in rec:
                all_internal_keys.update(rec["internal_timings"].keys())
            records.append(rec)
        except Exception as e:
            print(f"Warning: failed to parse {f}: {e}", file=sys.stderr)

    # Sort internal keys for stable column order
    internal_cols = sorted(all_internal_keys)

    # CSV columns: fixed columns first, then any internal timing columns
    fixed_cols = ["file", "benchmark", "suite", "input_size", "threads",
                  "real_sec", "user_sec", "sys_sec", "cpu_util"]
    all_cols = fixed_cols + internal_cols

    with open(args.output, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            # Flatten internal timings into top-level keys
            internals = rec.pop("internal_timings", {})
            rec.update(internals)
            writer.writerow(rec)

    print(f"Wrote {len(records)} record(s) to {args.output}")


if __name__ == "__main__":
    main()
