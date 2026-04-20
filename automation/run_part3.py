#!/usr/bin/env python3
"""
Part 3 automation: deploy and orchestrate batch scheduling.

Memcached is intentionally out of scope for this script. Start it manually
before running the automation.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kube_utils import kubectl_logs_job, kubectl_logs_pod, run_kubectl
from scheduler_logger import Job, SchedulerLogger


REPO_ROOT = Path(__file__).resolve().parents[1]
YAML_DIR = REPO_ROOT / "parsec-benchmarks" / "part3"

# Wave 0: all launched at t=0
# (yaml_file, job_name, Job enum, cores, threads)
WAVE_0 = [
    ("part3-parsec-streamcluster.yaml", "parsec-streamcluster", Job.STREAMCLUSTER, [0, 1, 2, 3], 4),
    ("part3-parsec-freqmine.yaml", "parsec-freqmine", Job.FREQMINE, [4, 5, 6, 7], 4),
    ("part3-parsec-canneal.yaml", "parsec-canneal", Job.CANNEAL, [1, 2], 2),
    ("part3-parsec-blackscholes.yaml", "parsec-blackscholes", Job.BLACKSCHOLES, [3], 1),
]

# Sequential chains: (trigger_job, next_yaml, next_job, enum, cores, threads)
CHAINS = {
    "parsec-blackscholes": ("part3-parsec-radix.yaml", "parsec-radix", Job.RADIX, [3], 1),
    "parsec-freqmine": ("part3-parsec-barnes.yaml", "parsec-barnes", Job.BARNES, [4, 5, 6, 7], 4),
    "parsec-barnes": ("part3-parsec-vips.yaml", "parsec-vips", Job.VIPS, [4, 5, 6, 7], 4),
}

ALL_JOBS = [
    "parsec-streamcluster",
    "parsec-freqmine",
    "parsec-canneal",
    "parsec-blackscholes",
    "parsec-radix",
    "parsec-barnes",
    "parsec-vips",
]

PHASE_ORDER = (
    "job_created",
    "pod_scheduled",
    "container_started",
    "benchmark_start",
    "benchmark_end",
    "pod_end",
)

MARKER_START_RE = re.compile(r"CCA_BENCHMARK_START_TS=(\d+(?:\.\d+)?)")
MARKER_END_RE = re.compile(r"CCA_BENCHMARK_END_TS=(\d+(?:\.\d+)?)\s+EXIT_CODE=(\d+)")
IMAGE_RE = re.compile(r"^\s*(?:-\s*)?image:\s*(\S+)\s*$", re.MULTILINE)
NODE_SELECTOR_RE = re.compile(r'cca-project-nodetype:\s*"([^"]+)"')
ARGS_RE = re.compile(r'args:\s*\["-c",\s*"((?:[^"\\]|\\.)*)"\]')


@dataclass
class JobRun:
    yaml_file: str
    base_name: str
    actual_name: str
    job_enum: Job
    cores: list[int]
    threads: int
    pod_name: str | None = None
    status: str = "Pending"
    timestamps: dict[str, float | None] = field(
        default_factory=lambda: {phase: None for phase in PHASE_ORDER}
    )
    metrics: dict[str, float | None] = field(
        default_factory=lambda: {
            "startup_overhead": None,
            "runtime": None,
            "job_wall": None,
            "parsec_real": None,
            "exit_code": None,
        }
    )
    observed_phases: set[str] = field(default_factory=set)


def ts() -> str:
    """Short timestamp for stderr log lines."""
    return datetime.now().strftime("%H:%M:%S")


def now_epoch() -> float:
    return time.time()


def parse_real_time(log_text: str) -> float | None:
    """Extract wall-clock seconds from PARSEC 'real Xm Y.ZZZs' output."""
    match = re.search(r"real\s+(\d+)m([\d.]+)s", log_text)
    if not match:
        return None
    return int(match.group(1)) * 60 + float(match.group(2))


def parse_benchmark_markers(log_text: str) -> dict[str, float | int | None]:
    """Extract explicit benchmark timestamps emitted by the wrapped command."""
    start_match = MARKER_START_RE.search(log_text)
    end_match = MARKER_END_RE.search(log_text)
    return {
        "benchmark_start": float(start_match.group(1)) if start_match else None,
        "benchmark_end": float(end_match.group(1)) if end_match else None,
        "exit_code": int(end_match.group(2)) if end_match else None,
    }


def iso_to_epoch(value: str | None) -> float | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).timestamp()


def yaml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def instrument_shell_command(command: str) -> str:
    wrapped = (
        "set +e; "
        "echo CCA_BENCHMARK_START_TS=$(date +%s.%N); "
        f"{command}; "
        "status=$?; "
        "echo CCA_BENCHMARK_END_TS=$(date +%s.%N) EXIT_CODE=$status; "
        "exit $status"
    )
    return wrapped


def render_job_manifest(manifest: str, base_job_name: str, actual_job_name: str) -> str:
    """Render a per-run manifest with a unique job name and wrapped command."""
    rendered = manifest.replace(base_job_name, actual_job_name)
    rendered = rendered.replace("imagePullPolicy: Always", "imagePullPolicy: IfNotPresent")

    match = ARGS_RE.search(rendered)
    if not match:
        raise ValueError(f"Could not find inline shell args for {base_job_name}")

    original_command = bytes(match.group(1), "utf-8").decode("unicode_escape")
    wrapped_command = instrument_shell_command(original_command)
    replacement = f'args: ["-c", "{yaml_escape(wrapped_command)}"]'
    return ARGS_RE.sub(replacement, rendered, count=1)


def kubectl_create_manifest(manifest: str) -> bool:
    rc, out, err = run_kubectl(["create", "-f", "-"], stdin_data=manifest)
    if rc != 0:
        print(f"  [kubectl] create failed: {err.strip()}", file=sys.stderr)
        return False
    print(f"  [kubectl] {out.strip()}", file=sys.stderr)
    return True


def kubectl_delete_resource(kind: str, name: str) -> bool:
    rc, _, _ = run_kubectl(["delete", f"{kind}/{name}", "--ignore-not-found=true"])
    return rc == 0


def get_job_status(job_name: str) -> str:
    rc, out, err = run_kubectl(["get", f"job/{job_name}", "-o", "json"])
    if rc != 0:
        print(f"  [kubectl] get failed for job/{job_name}: {err.strip()}", file=sys.stderr)
        return "Missing"

    payload = json.loads(out)
    status = payload.get("status", {})
    if status.get("succeeded", 0) > 0:
        return "Complete"
    if status.get("failed", 0) > 0:
        return "Failed"
    return "Running"


def get_job_pod_snapshot(job_name: str) -> dict | None:
    rc, out, err = run_kubectl(["get", "pods", "-l", f"job-name={job_name}", "-o", "json"])
    if rc != 0:
        print(f"  [kubectl] get pods failed for job/{job_name}: {err.strip()}", file=sys.stderr)
        return None

    payload = json.loads(out)
    items = payload.get("items", [])
    if not items:
        return None
    items.sort(key=lambda item: item["metadata"]["name"])
    return items[0]


def record_phase(
    job_run: JobRun,
    phase: str,
    timestamp: float | None,
    logger: SchedulerLogger,
    extra: str = "",
) -> None:
    if timestamp is None or job_run.timestamps.get(phase) is not None:
        return

    job_run.timestamps[phase] = timestamp
    if phase not in job_run.observed_phases:
        job_run.observed_phases.add(phase)
        detail = f"phase={phase} epoch={timestamp:.6f}"
        if extra:
            detail = f"{detail} {extra}"
        logger.custom_event(job_run.job_enum, detail)
        print(f"[{ts()}]   · {job_run.base_name}: {phase} @ {timestamp:.6f}", file=sys.stderr)


def update_metrics(job_run: JobRun) -> None:
    start = job_run.timestamps["benchmark_start"]
    end = job_run.timestamps["benchmark_end"]
    created = job_run.timestamps["job_created"]
    pod_end = job_run.timestamps["pod_end"]

    if created is not None and start is not None:
        job_run.metrics["startup_overhead"] = start - created
    if start is not None and end is not None:
        job_run.metrics["runtime"] = end - start
    if created is not None and pod_end is not None:
        job_run.metrics["job_wall"] = pod_end - created


def update_job_observations(job_run: JobRun, logger: SchedulerLogger) -> None:
    pod = get_job_pod_snapshot(job_run.actual_name)
    if pod:
        pod_name = pod["metadata"]["name"]
        if job_run.pod_name is None:
            job_run.pod_name = pod_name
            logger.custom_event(job_run.job_enum, f"pod_name={pod_name}")
            print(f"[{ts()}]   · {job_run.base_name}: pod={pod_name}", file=sys.stderr)

        conditions = pod.get("status", {}).get("conditions", [])
        scheduled_at = next(
            (
                iso_to_epoch(cond.get("lastTransitionTime"))
                for cond in conditions
                if cond.get("type") == "PodScheduled" and cond.get("status") == "True"
            ),
            None,
        )
        record_phase(job_run, "pod_scheduled", scheduled_at, logger)

        statuses = pod.get("status", {}).get("containerStatuses", [])
        if statuses:
            state = statuses[0].get("state", {})
            running = state.get("running")
            terminated = state.get("terminated")
            container_started = None
            pod_end = None

            if running:
                container_started = iso_to_epoch(running.get("startedAt"))
            elif terminated:
                container_started = iso_to_epoch(terminated.get("startedAt"))
                pod_end = iso_to_epoch(terminated.get("finishedAt"))

            record_phase(job_run, "container_started", container_started, logger)
            record_phase(job_run, "pod_end", pod_end, logger)

        if job_run.pod_name:
            logs = kubectl_logs_pod(job_run.pod_name)
            if logs:
                markers = parse_benchmark_markers(logs)
                record_phase(job_run, "benchmark_start", markers["benchmark_start"], logger)
                record_phase(job_run, "benchmark_end", markers["benchmark_end"], logger)
                if markers["exit_code"] is not None:
                    job_run.metrics["exit_code"] = markers["exit_code"]

    update_metrics(job_run)


def format_metric(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.3f}"


def build_actual_job_name(base_name: str, session_tag: str, run_number: int) -> str:
    return f"{base_name}-{session_tag}-r{run_number}"


def get_manifest_paths(yaml_dir: Path) -> list[Path]:
    names = {entry[0] for entry in WAVE_0}
    names.update(item[0] for item in CHAINS.values())
    return [yaml_dir / name for name in sorted(names)]


def collect_images_and_worker_labels(yaml_dir: Path) -> tuple[list[str], list[str]]:
    images: set[str] = set()
    worker_labels: set[str] = set()

    for path in get_manifest_paths(yaml_dir):
        text = path.read_text()
        images.update(IMAGE_RE.findall(text))
        worker_labels.update(NODE_SELECTOR_RE.findall(text))

    return sorted(images), sorted(worker_labels)


def build_prefetch_daemonset(ds_name: str, images: list[str], worker_labels: list[str]) -> str:
    values = "\n".join([f"                  - {label}" for label in worker_labels])
    init_blocks = []
    for idx, image in enumerate(images):
        init_blocks.append(
            "\n".join(
                [
                    f"      - name: pull-{idx}",
                    f"        image: {image}",
                    '        command: ["/bin/sh", "-c", "true"]',
                ]
            )
        )

    init_yaml = "\n".join(init_blocks)
    return f"""apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {ds_name}
spec:
  selector:
    matchLabels:
      app: {ds_name}
  template:
    metadata:
      labels:
        app: {ds_name}
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: cca-project-nodetype
                operator: In
                values:
{values}
      initContainers:
{init_yaml}
      containers:
      - name: hold
        image: busybox:1.36
        command: ["/bin/sh", "-c", "sleep 3600"]
        resources:
          requests:
            cpu: "5m"
            memory: "8Mi"
          limits:
            cpu: "10m"
            memory: "16Mi"
"""


def wait_for_daemonset_ready(name: str, timeout: int = 300) -> bool:
    rc, out, err = run_kubectl(
        ["rollout", "status", f"daemonset/{name}", f"--timeout={timeout}s"],
        timeout=timeout + 30,
    )
    if rc != 0:
        print(f"  [kubectl] rollout failed for daemonset/{name}: {err.strip()}", file=sys.stderr)
        return False
    print(f"  [kubectl] {out.strip()}", file=sys.stderr)
    return True


def verify_prefetch_daemonset(name: str, expected_images: list[str]) -> bool:
    rc, out, err = run_kubectl(["get", "pods", "-l", f"app={name}", "-o", "json"])
    if rc != 0:
        print(f"  [kubectl] prefetch pod lookup failed: {err.strip()}", file=sys.stderr)
        return False

    payload = json.loads(out)
    pods = payload.get("items", [])
    if not pods:
        print("  [kubectl] prefetch verification found no pods", file=sys.stderr)
        return False

    remaining = set(expected_images)
    for pod in pods:
        for status in pod.get("status", {}).get("initContainerStatuses", []):
            image = status.get("image")
            image_id = status.get("imageID")
            if image in remaining and image_id:
                remaining.discard(image)

    if remaining:
        print(f"  [kubectl] prefetch verification missing image IDs for: {sorted(remaining)}", file=sys.stderr)
        return False

    print(f"[{ts()}] Prefetch verification succeeded for {len(expected_images)} images", file=sys.stderr)
    return True


def pre_pull_images(yaml_dir: Path, session_tag: str, dry_run: bool = False) -> None:
    images, worker_labels = collect_images_and_worker_labels(yaml_dir)
    if dry_run:
        print(f"  Warm-up images: {images}", file=sys.stderr)
        print(f"  Warm-up worker labels: {worker_labels}", file=sys.stderr)
        return

    ds_name = f"part3-prefetch-{session_tag}"
    print(f"[{ts()}] Pre-pulling benchmark images on worker nodes...", file=sys.stderr)
    manifest = build_prefetch_daemonset(ds_name, images, worker_labels)
    if not kubectl_create_manifest(manifest):
        raise RuntimeError("Failed to create image prefetch DaemonSet")

    try:
        if not wait_for_daemonset_ready(ds_name):
            raise RuntimeError("Image prefetch DaemonSet did not become ready")
        if not verify_prefetch_daemonset(ds_name, images):
            raise RuntimeError("Image prefetch verification failed")
    finally:
        kubectl_delete_resource("daemonset", ds_name)


def write_lifecycle_report(job_runs: dict[str, JobRun], destination: Path) -> None:
    payload = {}
    for base_name, job_run in sorted(job_runs.items()):
        payload[base_name] = {
            "actual_name": job_run.actual_name,
            "pod_name": job_run.pod_name,
            "status": job_run.status,
            "timestamps": job_run.timestamps,
            "metrics": job_run.metrics,
            "cores": job_run.cores,
            "threads": job_run.threads,
        }
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True))


def create_job_run(
    yaml_dir: Path,
    yaml_file: str,
    base_name: str,
    job_enum: Job,
    cores: list[int],
    threads: int,
    run_number: int,
    session_tag: str,
    logger: SchedulerLogger,
) -> JobRun:
    actual_name = build_actual_job_name(base_name, session_tag, run_number)
    manifest = (yaml_dir / yaml_file).read_text()
    rendered = render_job_manifest(manifest, base_job_name=base_name, actual_job_name=actual_name)

    print(f"[{ts()}]   → {base_name} as {actual_name} (cores={cores}, {threads}T)", file=sys.stderr)
    if not kubectl_create_manifest(rendered):
        raise RuntimeError(f"Could not launch {base_name}")

    job_run = JobRun(
        yaml_file=yaml_file,
        base_name=base_name,
        actual_name=actual_name,
        job_enum=job_enum,
        cores=cores,
        threads=threads,
    )
    record_phase(job_run, "job_created", now_epoch(), logger, extra=f"actual_name={actual_name}")
    logger.job_start(job_enum, cores, threads)
    return job_run


def collect_job_logs(job_run: JobRun, raw_dir: Path, run_number: int, logger: SchedulerLogger) -> None:
    logs = kubectl_logs_job(job_run.actual_name)
    if not logs:
        return

    bench = job_run.base_name.replace("parsec-", "")
    (raw_dir / f"{bench}_run{run_number}.txt").write_text(logs)

    markers = parse_benchmark_markers(logs)
    record_phase(job_run, "benchmark_start", markers["benchmark_start"], logger)
    record_phase(job_run, "benchmark_end", markers["benchmark_end"], logger)
    if markers["exit_code"] is not None:
        job_run.metrics["exit_code"] = markers["exit_code"]

    real = parse_real_time(logs)
    if real is not None:
        job_run.metrics["parsec_real"] = real

    update_metrics(job_run)


def compute_makespan(job_runs: dict[str, JobRun]) -> float | None:
    created = [job.timestamps["job_created"] for job in job_runs.values() if job.timestamps["job_created"] is not None]
    finished = [job.timestamps["pod_end"] for job in job_runs.values() if job.timestamps["pod_end"] is not None]
    if not created or not finished:
        return None
    return max(finished) - min(created)


def print_run_summary(job_runs: dict[str, JobRun], makespan: float | None) -> None:
    print(f"\n{'─' * 80}", file=sys.stderr)
    print("  RUN SUMMARY", file=sys.stderr)
    print(f"{'─' * 80}", file=sys.stderr)
    header = (
        f"  {'job':20s} {'status':10s} {'startup':>10s} "
        f"{'runtime':>10s} {'job_wall':>10s} {'parsec_real':>10s}"
    )
    print(header, file=sys.stderr)
    for base_name in ALL_JOBS:
        job_run = job_runs.get(base_name)
        if not job_run:
            continue
        print(
            f"  {base_name.replace('parsec-', ''):20s} "
            f"{job_run.status:10s} "
            f"{format_metric(job_run.metrics['startup_overhead']):>10s} "
            f"{format_metric(job_run.metrics['runtime']):>10s} "
            f"{format_metric(job_run.metrics['job_wall']):>10s} "
            f"{format_metric(job_run.metrics['parsec_real']):>10s}",
            file=sys.stderr,
        )
    print(f"  {'MAKESPAN':20s} {'':10s} {'':10s} {'':10s} {format_metric(makespan):>10s}", file=sys.stderr)
    print(f"{'─' * 80}\n", file=sys.stderr)


def run_schedule(yaml_dir: Path, run_number: int, results_dir: Path, session_tag: str) -> dict | None:
    """Execute one complete Part 3 run."""
    logger = SchedulerLogger()
    raw_dir = results_dir / "raw"
    lifecycle_dir = results_dir / "lifecycle"
    raw_dir.mkdir(parents=True, exist_ok=True)
    lifecycle_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'=' * 72}", file=sys.stderr)
    print(f"  PART 3 — RUN {run_number}  (log: {logger.get_file_name()})", file=sys.stderr)
    print("  memcached: expected to be running already", file=sys.stderr)
    print(f"{'=' * 72}", file=sys.stderr)

    launched: dict[str, JobRun] = {}
    completed: set[str] = set()

    try:
        print(f"\n[{ts()}] === WAVE 0 ===", file=sys.stderr)
        for yaml_file, base_name, job_enum, cores, threads in WAVE_0:
            launched[base_name] = create_job_run(
                yaml_dir,
                yaml_file,
                base_name,
                job_enum,
                cores,
                threads,
                run_number,
                session_tag,
                logger,
            )
            time.sleep(1)

        print(f"\n[{ts()}] === Monitoring jobs ===", file=sys.stderr)
        while len(completed) < len(ALL_JOBS):
            for base_name, job_run in list(launched.items()):
                if base_name in completed:
                    continue

                update_job_observations(job_run, logger)
                status = get_job_status(job_run.actual_name)
                if status not in {"Complete", "Failed"}:
                    continue

                job_run.status = status
                update_job_observations(job_run, logger)
                collect_job_logs(job_run, raw_dir, run_number, logger)
                update_job_observations(job_run, logger)
                logger.job_end(job_run.job_enum)
                completed.add(base_name)

                print(f"[{ts()}] {'✓' if status == 'Complete' else '✗'} {base_name} {status.lower()}", file=sys.stderr)

                if status == "Complete" and base_name in CHAINS:
                    next_yaml, next_base, next_enum, next_cores, next_threads = CHAINS[base_name]
                    launched[next_base] = create_job_run(
                        yaml_dir,
                        next_yaml,
                        next_base,
                        next_enum,
                        next_cores,
                        next_threads,
                        run_number,
                        session_tag,
                        logger,
                    )
                    time.sleep(1)

            if len(completed) < len(ALL_JOBS):
                time.sleep(3)

        makespan = compute_makespan(launched)
        print_run_summary(launched, makespan)
        write_lifecycle_report(launched, lifecycle_dir / f"run{run_number}.json")
        logger.end()

        return {
            "run": run_number,
            "makespan": makespan,
            "jobs": launched,
            "log_file": logger.get_file_name(),
        }
    except Exception:
        logger.end()
        raise


def run_all(
    yaml_dir: Path,
    num_runs: int,
    results_dir: Path,
    dry_run: bool = False,
    skip_warmup: bool = False,
) -> list[dict]:
    """Run the Part 3 schedule num_runs times."""
    session_tag = datetime.now().strftime("%m%d%H%M%S")

    print(f"\n{'#' * 72}", file=sys.stderr)
    print("  Part 3 Scheduling Automation", file=sys.stderr)
    print(f"  Runs: {num_runs}   YAMLs: {yaml_dir}", file=sys.stderr)
    print("  memcached: manual, pre-started", file=sys.stderr)
    print("  cleanup: disabled, job pods remain after each run", file=sys.stderr)
    print(f"  warm-up: {'disabled' if skip_warmup else 'enabled'}", file=sys.stderr)
    print(f"{'#' * 72}\n", file=sys.stderr)

    if dry_run:
        print("  Schedule per run:", file=sys.stderr)
        print("    t=0: streamcluster(A,c0-3,4T) freqmine(A,c4-7,4T)", file=sys.stderr)
        print("         canneal(B,c1-2,2T) blackscholes(B,c3,1T)", file=sys.stderr)
        print("    blackscholes done → radix(B,c3,1T)", file=sys.stderr)
        print("    freqmine done     → barnes(A,c4-7,4T)", file=sys.stderr)
        print("    barnes done       → vips(A,c4-7,4T)", file=sys.stderr)
        if skip_warmup:
            print("  Warm-up skipped", file=sys.stderr)
        else:
            pre_pull_images(yaml_dir, session_tag, dry_run=True)
        print(f"    × {num_runs} runs", file=sys.stderr)
        return []

    if skip_warmup:
        print(f"[{ts()}] Skipping benchmark image warm-up", file=sys.stderr)
    else:
        pre_pull_images(yaml_dir, session_tag)

    all_results = []
    for run in range(1, num_runs + 1):
        result = run_schedule(yaml_dir, run, results_dir, session_tag)
        if result:
            all_results.append(result)

        if run < num_runs:
            print(f"[{ts()}] Pause before next run (15s)...", file=sys.stderr)
            time.sleep(15)

    successful = [r["makespan"] for r in all_results if r["makespan"] is not None]
    if successful:
        avg = sum(successful) / len(successful)
        std = (sum((m - avg) ** 2 for m in successful) / len(successful)) ** 0.5
        print(f"\n{'#' * 72}", file=sys.stderr)
        print(f"  FINAL ({len(all_results)} runs)", file=sys.stderr)
        print(f"  Makespan: mean={avg:.3f}s  std={std:.3f}s", file=sys.stderr)
        for result in all_results:
            print(f"    Run {result['run']}: {format_metric(result['makespan'])}s", file=sys.stderr)
        print(f"{'#' * 72}\n", file=sys.stderr)

    return all_results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Part 3: orchestrate batch scheduling strategy.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--yaml-dir",
        type=Path,
        default=YAML_DIR,
        help="Directory with Part 3 YAML files",
    )
    parser.add_argument("--runs", type=int, default=3, help="Number of runs (default: 3)")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results/part3"),
        help="Output directory",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print schedule without executing")
    parser.add_argument(
        "--skip-warmup",
        action="store_true",
        help="Skip benchmark image pre-pull before measured runs",
    )

    args = parser.parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    run_all(
        args.yaml_dir,
        args.runs,
        args.results_dir,
        dry_run=args.dry_run,
        skip_warmup=args.skip_warmup,
    )


if __name__ == "__main__":
    main()
