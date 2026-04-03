#!/usr/bin/env python3
"""
Part 2a automation: Run PARSEC benchmarks isolated and with ibench interference.

Orchestrates the full benchmark lifecycle:
  1. (Optional) Start interference pod, wait for it to be ready
  2. Start PARSEC workload job, wait for completion
  3. Collect logs, parse results
  4. Tear down workload job and interference pod
  5. Append results to CSV

Usage:
    # Run all workloads × all interference combinations
    python3 automation/run_part2a.py

    # Run only barnes, isolated and with cpu interference
    python3 automation/run_part2a.py --workloads barnes --interferences none cpu

    # Dry run to see what would execute
    python3 automation/run_part2a.py --dry-run

    # Multiple repetitions
    python3 automation/run_part2a.py --runs 3
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add parent dir to path so we can import sibling modules
sys.path.insert(0, str(Path(__file__).resolve().parent))
from kube_utils import (
    kubectl_create,
    kubectl_wait_job,
    kubectl_wait_pod,
    kubectl_logs_job,
    kubectl_delete_job,
    kubectl_delete_pod,
)
from parse_parsec_results import parse_parsec_output

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent

WORKLOADS = ["barnes", "blackscholes", "canneal", "freqmine",
             "radix", "streamcluster", "vips"]

INTERFERENCES = ["none", "cpu", "l1d", "l1i", "l2", "llc", "membw"]

PARSEC_YAML_DIR = PROJECT_ROOT / "parsec-benchmarks" / "part2a"
INTERFERENCE_YAML_DIR = PROJECT_ROOT / "interference"
RESULTS_DIR = PROJECT_ROOT / "results" / "part2" / "a"
RAW_DIR = RESULTS_DIR / "raw"

# Map interference name → YAML filename
INTERFERENCE_YAMLS = {
    "cpu": "ibench-cpu.yaml",
    "l1d": "ibench-l1d.yaml",
    "l1i": "ibench-l1i.yaml",
    "l2": "ibench-l2.yaml",
    "llc": "ibench-llc.yaml",
    "membw": "ibench-membw.yaml",
}

# Map interference name → pod name in the cluster
INTERFERENCE_POD_NAMES = {
    "cpu": "ibench-cpu",
    "l1d": "ibench-l1d",
    "l1i": "ibench-l1i",
    "l2": "ibench-l2",
    "llc": "ibench-llc",
    "membw": "ibench-membw",
}

# For part2a, all interference pods must target the parsec node
NODE_SELECTOR = "parsec"

# Job timeout in seconds (some benchmarks can take a while)
JOB_TIMEOUT = 900

# CSV output path
CSV_OUTPUT = RESULTS_DIR / "results_part2a.csv"
CSV_COLUMNS = [
    "benchmark", "suite", "input_size", "threads",
    "interference", "run", "real_sec", "user_sec", "sys_sec", "cpu_util",
]


# ──────────────────────────────────────────────────────────────────────
# Module 1: Interference Management
# ──────────────────────────────────────────────────────────────────────

def start_interference(interference_type):
    """Start an ibench interference pod and wait for it to be ready.

    Returns:
        True if the interference pod is running, False on failure.
    """
    if interference_type == "none":
        return True

    yaml_file = INTERFERENCE_YAML_DIR / INTERFERENCE_YAMLS[interference_type]
    pod_name = INTERFERENCE_POD_NAMES[interference_type]

    print(f"\n>>> Starting interference: {interference_type}", file=sys.stderr)

    if not kubectl_create(str(yaml_file), node_selector_override=NODE_SELECTOR):
        print(f"  ERROR: Failed to create interference pod {pod_name}",
              file=sys.stderr)
        return False

    if not kubectl_wait_pod(pod_name, timeout=120):
        print(f"  ERROR: Interference pod {pod_name} did not become ready",
              file=sys.stderr)
        stop_interference(interference_type)
        return False

    # Small delay to let interference stabilize
    time.sleep(5)
    print(f"  Interference {interference_type} is running.", file=sys.stderr)
    return True


def stop_interference(interference_type):
    """Stop and delete an interference pod.

    Returns:
        True if cleanup succeeded, False otherwise.
    """
    if interference_type == "none":
        return True

    pod_name = INTERFERENCE_POD_NAMES[interference_type]
    print(f"  Stopping interference: {interference_type}", file=sys.stderr)
    return kubectl_delete_pod(pod_name)


# ──────────────────────────────────────────────────────────────────────
# Module 2: Workload Management
# ──────────────────────────────────────────────────────────────────────

def start_workload(workload_name):
    """Start a PARSEC workload job.

    Returns:
        True if the job was created successfully, False otherwise.
    """
    yaml_file = PARSEC_YAML_DIR / f"parsec-{workload_name}.yaml"
    job_name = f"parsec-{workload_name}"

    print(f"\n>>> Starting workload: {workload_name}", file=sys.stderr)

    if not kubectl_create(str(yaml_file)):
        print(f"  ERROR: Failed to create job {job_name}", file=sys.stderr)
        return False

    return True


def wait_workload(workload_name, timeout=JOB_TIMEOUT):
    """Wait for a PARSEC workload job to complete.

    Returns:
        Status string: "Complete", "Failed", or "Timeout".
    """
    job_name = f"parsec-{workload_name}"
    print(f"  Waiting for job {job_name} to complete...", file=sys.stderr)
    status = kubectl_wait_job(job_name, timeout=timeout)
    print(f"  Job {job_name} status: {status}", file=sys.stderr)
    return status


def get_workload_logs(workload_name):
    """Get logs from a completed PARSEC workload job.

    Returns:
        Raw log string.
    """
    job_name = f"parsec-{workload_name}"
    return kubectl_logs_job(job_name)


def cleanup_workload(workload_name):
    """Delete a PARSEC workload job and its pods.

    Returns:
        True if cleanup succeeded.
    """
    job_name = f"parsec-{workload_name}"
    print(f"  Cleaning up job: {job_name}", file=sys.stderr)
    return kubectl_delete_job(job_name)


# ──────────────────────────────────────────────────────────────────────
# Module 3: Results Parsing & CSV Output
# ──────────────────────────────────────────────────────────────────────

def save_raw_log(workload, interference, run_number, raw_log):
    """Save raw benchmark output to a file for debugging/reprocessing.

    Returns:
        Path to the saved file.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{workload}_{interference}_run{run_number}.txt"
    filepath = RAW_DIR / filename
    filepath.write_text(raw_log)
    print(f"  Raw log saved to: {filepath}", file=sys.stderr)
    return filepath


def parse_result(raw_log_path, workload, interference, run_number):
    """Parse a raw PARSEC log file and return a result dict with metadata.

    Returns:
        Dict with CSV_COLUMNS keys, or None on parse failure.
    """
    try:
        parsed = parse_parsec_output(str(raw_log_path))
    except Exception as e:
        print(f"  ERROR: Failed to parse {raw_log_path}: {e}", file=sys.stderr)
        return None

    return {
        "benchmark": parsed.get("benchmark", workload),
        "suite": parsed.get("suite", ""),
        "input_size": parsed.get("input_size", ""),
        "threads": parsed.get("threads", ""),
        "interference": interference,
        "run": run_number,
        "real_sec": parsed.get("real_sec", ""),
        "user_sec": parsed.get("user_sec", ""),
        "sys_sec": parsed.get("sys_sec", ""),
        "cpu_util": parsed.get("cpu_util", ""),
    }


def append_to_csv(result):
    """Append a single result row to the CSV file. Creates header if needed."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = CSV_OUTPUT.exists() and CSV_OUTPUT.stat().st_size > 0

    with open(CSV_OUTPUT, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(result)

    print(f"  Result appended to: {CSV_OUTPUT}", file=sys.stderr)


# ──────────────────────────────────────────────────────────────────────
# Module 4: Teardown (consolidated cleanup)
# ──────────────────────────────────────────────────────────────────────

def teardown(workload_name, interference_type):
    """Clean up all resources from an experiment run."""
    cleanup_workload(workload_name)
    stop_interference(interference_type)


# ──────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────

def run_experiment(workload, interference, run_number, job_timeout=JOB_TIMEOUT):
    """Run a single experiment: one workload with one interference type.

    Returns:
        Result dict on success, None on failure.
    """
    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  EXPERIMENT: {workload} | interference={interference} | "
          f"run={run_number}", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    result = None

    try:
        # Step 1: Start interference (if any)
        if not start_interference(interference):
            print(f"  SKIP: interference {interference} failed to start",
                  file=sys.stderr)
            return None

        # Step 2: Start workload
        if not start_workload(workload):
            stop_interference(interference)
            return None

        # Step 3: Wait for workload completion
        status = wait_workload(workload, timeout=job_timeout)
        if status != "Complete":
            print(f"  SKIP: workload {workload} ended with status={status}",
                  file=sys.stderr)
            teardown(workload, interference)
            return None

        # Step 4: Collect logs
        raw_log = get_workload_logs(workload)
        if not raw_log:
            print(f"  SKIP: no logs for {workload}", file=sys.stderr)
            teardown(workload, interference)
            return None

        # Step 5: Save raw log and parse
        raw_path = save_raw_log(workload, interference, run_number, raw_log)
        result = parse_result(raw_path, workload, interference, run_number)

        if result:
            append_to_csv(result)
            print(f"\n  RESULT: {workload} | {interference} | "
                  f"real={result.get('real_sec', '?')}s", file=sys.stderr)

    finally:
        # Step 6: Always clean up
        teardown(workload, interference)

    return result


def run_all(workloads, interferences, num_runs, dry_run=False,
            job_timeout=JOB_TIMEOUT):
    """Run all experiments in the specified matrix.

    Args:
        workloads: List of workload names.
        interferences: List of interference types.
        num_runs: Number of repetitions per combination.
        dry_run: If True, just print what would run.
        job_timeout: Timeout in seconds for each job.

    Returns:
        List of result dicts.
    """
    total = len(workloads) * len(interferences) * num_runs
    print(f"\n{'#'*60}", file=sys.stderr)
    print(f"  Part 2a Automation", file=sys.stderr)
    print(f"  Workloads:     {', '.join(workloads)}", file=sys.stderr)
    print(f"  Interferences: {', '.join(interferences)}", file=sys.stderr)
    print(f"  Runs per combo: {num_runs}", file=sys.stderr)
    print(f"  Total experiments: {total}", file=sys.stderr)
    print(f"  CSV output: {CSV_OUTPUT}", file=sys.stderr)
    print(f"{'#'*60}\n", file=sys.stderr)

    if dry_run:
        for workload in workloads:
            for interference in interferences:
                for run in range(1, num_runs + 1):
                    print(f"  [DRY RUN] {workload} | {interference} | "
                          f"run={run}")
        print(f"\n  Would run {total} experiments.")
        return []

    results = []
    completed = 0
    failed = 0
    start_time = time.time()

    for workload in workloads:
        for interference in interferences:
            for run in range(1, num_runs + 1):
                result = run_experiment(workload, interference, run,
                                       job_timeout=job_timeout)
                if result:
                    results.append(result)
                    completed += 1
                else:
                    failed += 1

                elapsed = time.time() - start_time
                done = completed + failed
                print(f"\n  Progress: {done}/{total} "
                      f"(ok={completed}, fail={failed}, "
                      f"elapsed={elapsed:.0f}s)\n", file=sys.stderr)

    print(f"\n{'#'*60}", file=sys.stderr)
    print(f"  DONE: {completed} succeeded, {failed} failed "
          f"out of {total}", file=sys.stderr)
    print(f"  Results: {CSV_OUTPUT}", file=sys.stderr)
    print(f"  Raw logs: {RAW_DIR}", file=sys.stderr)
    print(f"{'#'*60}\n", file=sys.stderr)

    return results


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run Part 2a PARSEC benchmarks with optional interference.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--workloads", nargs="+", default=WORKLOADS,
        choices=WORKLOADS, metavar="W",
        help=f"Workloads to run (default: all). Choices: {', '.join(WORKLOADS)}",
    )
    parser.add_argument(
        "--interferences", nargs="+", default=INTERFERENCES,
        choices=INTERFERENCES, metavar="I",
        help=f"Interference types (default: all). Choices: {', '.join(INTERFERENCES)}",
    )
    parser.add_argument(
        "--runs", type=int, default=1,
        help="Number of repetitions per combination (default: 1)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would run without executing",
    )
    parser.add_argument(
        "--timeout", type=int, default=JOB_TIMEOUT,
        help=f"Job timeout in seconds (default: {JOB_TIMEOUT})",
    )

    args = parser.parse_args()

    results = run_all(
        workloads=args.workloads,
        interferences=args.interferences,
        num_runs=args.runs,
        dry_run=args.dry_run,
        job_timeout=args.timeout,
    )

    if results:
        print(f"\nFinal CSV at: {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
