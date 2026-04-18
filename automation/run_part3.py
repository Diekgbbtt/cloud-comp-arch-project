#!/usr/bin/env python3
"""
Part 3 automation: Deploy and orchestrate the scheduling strategy.

Schedule (Strategy B — canneal to Node B):
  Node A (8 cores):
    Slot 1 (cores 0-3): streamcluster @4T           → runs full duration
    Slot 2 (cores 4-7): freqmine @4T → barnes @4T → vips @4T  (sequential)
  Node B (4 cores):
    Core 0:             memcached (Guaranteed, long-running)
    Cores 1-2:          canneal @2T  (Guaranteed)
    Core 3:             blackscholes @1T → radix @1T (sequential, BestEffort)

Launch order:
  t=0:   memcached, streamcluster, freqmine, canneal, blackscholes
  wait:  blackscholes completes → launch radix
  wait:  freqmine completes    → launch barnes
  wait:  barnes completes      → launch vips
  wait:  all jobs complete     → collect results

Usage:
    python3 run_part3.py
    python3 run_part3.py --dry-run
    python3 run_part3.py --runs 3
    python3 run_part3.py --yaml-dir ./part3-yamls
"""

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kube_utils import (
    kubectl_create,
    kubectl_wait_job,
    kubectl_wait_pod,
    kubectl_logs_job,
    kubectl_delete_job,
    kubectl_delete_pod,
)
from scheduler_logger import SchedulerLogger, Job

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

YAML_DIR = Path(__file__).resolve().parent

# Wave 0: all launched at t=0
# (yaml_file, job_name, Job enum, cores, threads)
WAVE_0 = [
    ("part3-parsec-streamcluster.yaml", "parsec-streamcluster", Job.STREAMCLUSTER, [0,1,2,3], 4),
    ("part3-parsec-freqmine.yaml",      "parsec-freqmine",      Job.FREQMINE,      [4,5,6,7], 4),
    ("part3-parsec-canneal.yaml",       "parsec-canneal",       Job.CANNEAL,        [1,2],     2),
    ("part3-parsec-blackscholes.yaml",  "parsec-blackscholes",  Job.BLACKSCHOLES,   [3],       1),
]

# Sequential chains: each entry is (trigger_job, next_yaml, next_job, enum, cores, threads)
# Chain 1 (Node B core 3): blackscholes → radix
# Chain 2 (Node A slot 2): freqmine → barnes → vips
CHAINS = {
    "parsec-blackscholes": ("part3-parsec-radix.yaml",  "parsec-radix",  Job.RADIX,  [3],       1),
    "parsec-freqmine":     ("part3-parsec-barnes.yaml", "parsec-barnes", Job.BARNES, [4,5,6,7], 4),
    "parsec-barnes":       ("part3-parsec-vips.yaml",   "parsec-vips",   Job.VIPS,   [4,5,6,7], 4),
}

ALL_JOBS = [
    "parsec-streamcluster", "parsec-freqmine", "parsec-canneal",
    "parsec-blackscholes", "parsec-radix", "parsec-barnes", "parsec-vips",
]

JOB_TIMEOUT = 600


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def ts():
    """Short timestamp for log lines."""
    return datetime.now().strftime("%H:%M:%S")


def parse_real_time(log_text):
    """Extract wall-clock seconds from PARSEC 'real Xm Y.ZZZs' output."""
    m = re.search(r"real\s+(\d+)m([\d.]+)s", log_text)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    return None


def cleanup_all():
    """Delete all batch jobs and the memcached pod."""
    print(f"[{ts()}] Cleaning up all resources...", file=sys.stderr)
    for job_name in ALL_JOBS:
        kubectl_delete_job(job_name)
    time.sleep(5)


# ──────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────

def run_schedule(yaml_dir, run_number, results_dir):
    """Execute one complete run of the Part 3 schedule.

    Returns dict with makespan and per-job timings, or None on failure.
    """
    logger = SchedulerLogger()
    raw_dir = results_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  PART 3 — RUN {run_number}  (log: {logger.get_file_name()})", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    # memcached considered alredy runing due to dependencies of newly configured of mcpperf on agents
    # TODO this couldbe automated as well

    """
    run with this cmd to get the output piped to cmdline curently
    ssh -i ~/.ssh/id_rsa ubuntu@34.14.87.124 \                                                                                                                                               
    "cd memcache-perf-dynamic && ./mcperf -s 100.96.1.4 \                                                                                                                            
     -a 10.0.16.8 -a 10.0.16.6 \                                                                                                                                                           
     --noload -T 6 -C 4 -D 4 -Q 1000 -c 4 -t 10 \                                                                                                                                          
     --scan 30000:30500:5"
    """
    
    # ── 2. Launch wave 0 ─────────────────────────────────────────────
    t0 = time.time()
    print(f"\n[{ts()}] === WAVE 0 ===", file=sys.stderr)
    for yaml_file, job_name, job_enum, cores, threads in WAVE_0:
        print(f"[{ts()}]   → {job_name} (cores={cores}, {threads}T)", file=sys.stderr)
        if not kubectl_create(str(yaml_dir / yaml_file)):
            print(f"  FATAL: could not launch {job_name}", file=sys.stderr)
            cleanup_all()
            logger.end()
            return None
        logger.job_start(job_enum, cores, threads)
        time.sleep(1)

    # ── 3. Monitor: poll for completions, trigger chains ─────────────
    print(f"\n[{ts()}] === Monitoring jobs ===", file=sys.stderr)

    completed = set()
    launched = {entry[1] for entry in WAVE_0}  # job names already launched
    real_times = {}

    while len(completed) < len(ALL_JOBS):
        for job_name in list(launched - completed):
            status = kubectl_wait_job(job_name, timeout=5) ## TODO is such a low timeout a good design choice, would be better equal to the longest expected job

            if status == "Complete":
                elapsed = time.time() - t0
                bench = job_name.replace("parsec-", "")
                logger.job_end(Job[bench.upper()])
                completed.add(job_name)
                print(f"[{ts()}] ✓ {job_name} complete (wall {elapsed:.0f}s)", file=sys.stderr)

                # Collect logs immediately
                logs = kubectl_logs_job(job_name)
                if logs:
                    fpath = raw_dir / f"{bench}_run{run_number}.txt"
                    fpath.write_text(logs)
                    rt = parse_real_time(logs)
                    if rt is not None:
                        real_times[bench] = rt

                # Trigger next in chain if applicable
                if job_name in CHAINS:
                    nxt_yaml, nxt_job, nxt_enum, nxt_cores, nxt_threads = CHAINS[job_name]
                    print(f"[{ts()}]   → chain: launching {nxt_job}", file=sys.stderr)
                    kubectl_create(str(yaml_dir / nxt_yaml))
                    logger.job_start(nxt_enum, nxt_cores, nxt_threads)
                    launched.add(nxt_job)
                    time.sleep(1)

            elif status == "Failed":
                bench = job_name.replace("parsec-", "")
                print(f"[{ts()}] ✗ {job_name} FAILED", file=sys.stderr)
                logger.job_end(Job[bench.upper()])
                completed.add(job_name)

        if len(completed) < len(ALL_JOBS):
            time.sleep(3)

    makespan = time.time() - t0

    # ── 4. Summary ───────────────────────────────────────────────────
    print(f"\n{'─'*60}", file=sys.stderr)
    print(f"  RUN {run_number} RESULTS", file=sys.stderr)
    print(f"{'─'*60}", file=sys.stderr)
    for job_name in ALL_JOBS:
        bench = job_name.replace("parsec-", "")
        rt = real_times.get(bench, "—")
        if isinstance(rt, float):
            rt = f"{rt:.1f}"
        print(f"  {bench:20s}  real = {rt:>8s}s", file=sys.stderr)
    print(f"  {'MAKESPAN':20s}       = {makespan:>7.1f}s", file=sys.stderr)
    print(f"{'─'*60}\n", file=sys.stderr)

    # ── 5. Cleanup ───────────────────────────────────────────────────
    logger.job_end(Job.MEMCACHED)
    logger.end()
    cleanup_all()

    return {
        "run": run_number,
        "makespan": makespan,
        "real_times": real_times,
        "log_file": logger.get_file_name(),
    }


# ──────────────────────────────────────────────────────────────────────
# Multi-run wrapper
# ──────────────────────────────────────────────────────────────────────

def run_all(yaml_dir, num_runs, results_dir, dry_run=False):
    """Run the Part 3 schedule num_runs times."""
    print(f"\n{'#'*60}", file=sys.stderr)
    print(f"  Part 3 Scheduling Automation (Strategy B)", file=sys.stderr)
    print(f"  Runs: {num_runs}   YAMLs: {yaml_dir}", file=sys.stderr)
    print(f"{'#'*60}\n", file=sys.stderr)

    if dry_run:
        print("  Schedule per run:", file=sys.stderr)
        print("    t=0: memcached(B,c0) streamcluster(A,c0-3,4T) "
              "freqmine(A,c4-7,4T) canneal(B,c1-2,2T) "
              "blackscholes(B,c3,1T)", file=sys.stderr)
        print("    blackscholes done → radix(B,c3,1T)", file=sys.stderr)
        print("    freqmine done     → barnes(A,c4-7,4T)", file=sys.stderr)
        print("    barnes done       → vips(A,c4-7,4T)", file=sys.stderr)
        print(f"    × {num_runs} runs", file=sys.stderr)
        return []

    all_results = []
    for run in range(1, num_runs + 1):
        cleanup_all()
        time.sleep(5)

        result = run_schedule(yaml_dir, run, results_dir)
        if result:
            all_results.append(result)

###### TODO weak implementation : should be programmed to be run as ascript that execute the bench once and therefore provide analysis of just one run

        if run < num_runs:
            print(f"  Pause before next run (15s)...", file=sys.stderr)
            time.sleep(15)

    # ── Summary across runs ──
    if all_results:
        makespans = [r["makespan"] for r in all_results]
        ## maths here are calculated on wrong numbers
        avg = sum(makespans) / len(makespans)
        std = (sum((m - avg) ** 2 for m in makespans) / len(makespans)) ** 0.5

        print(f"\n{'#'*60}", file=sys.stderr)
        print(f"  FINAL ({len(all_results)} runs)", file=sys.stderr)
        print(f"  Makespan: mean={avg:.1f}s  std={std:.1f}s", file=sys.stderr)
        for r in all_results:
            print(f"    Run {r['run']}: {r['makespan']:.1f}s", file=sys.stderr)
        print(f"{'#'*60}\n", file=sys.stderr)

    return all_results


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Part 3: Orchestrate batch scheduling strategy.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--yaml-dir", type=Path, default=YAML_DIR,
                        help="Directory with Part 3 YAML files")
    parser.add_argument("--runs", type=int, default=3,
                        help="Number of runs (default: 3)")
    parser.add_argument("--results-dir", type=Path, default=Path("results/part3"),
                        help="Output directory")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print schedule without executing")

    args = parser.parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    run_all(args.yaml_dir, args.runs, args.results_dir, args.dry_run)


if __name__ == "__main__":
    main()
