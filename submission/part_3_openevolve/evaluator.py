from __future__ import annotations

import ast
import json
import math
import os
import re
import signal
import shlex
import subprocess
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from openevolve.evaluation_result import EvaluationResult
import sys

REPO_ROOT = (Path.home() / "cloud-comp-arch-project")

# ---------------------------------------------------------------------------
# Splice configuration
# ---------------------------------------------------------------------------

# The full automation script that contains the empty EVOLVE block.
# This file is NEVER modified — it is read as a template on every evaluation.
_AUTOMATION_TEMPLATE_PATH: Path = REPO_ROOT / "automation" / "part3_automation.py"

# Marker strings that delimit the evolvable region inside the template.
# Both must appear exactly once in the template.
_EVOLVE_START_MARKER: str = "# EVOLVE-BLOCK-START"
_EVOLVE_END_MARKER: str = "# EVOLVE-BLOCK-END"

# Base PARSEC job names used throughout this project.
JOB_BASE_NAMES: list[str] = [
    "parsec-streamcluster",
    "parsec-freqmine",
    "parsec-canneal",
    "parsec-blackscholes",
    "parsec-radix",
    "parsec-barnes",
    "parsec-vips",
]


# Makespan of the hand-crafted policy measured in controlled conditions.
# Used only for final report comparison; NOT used as the scoring denominator.
TARGET_MAKESPAN_SEC: float = 247.67


class EvaluationError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Evolve-block splice helpers
# ---------------------------------------------------------------------------

def _strip_markdown_fences_from_source(source: str) -> str:
    """Remove leading/trailing markdown code fences if the LLM added them.

    Only strips when the source starts with a fence — does not touch fences
    that appear in the middle of legitimate code.
    Handles both plain ``` and language-tagged ```python variants.
    """
    stripped = source.strip()
    if not stripped.startswith("```"):
        return source
    lines = stripped.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines)


def _validate_python_syntax(source: str, label: str) -> None:
    """Raise EvaluationError with a precise message if *source* does not parse.

    Parameters
    ----------
    source:
        Python source text to validate.
    label:
        Human-readable name used in the error message (e.g. "evolved block",
        "spliced script").
    """
    try:
        ast.parse(source)
    except SyntaxError as exc:
        raise EvaluationError(
            f"SYNTAX_ERROR in {label} — "
            f"line {exc.lineno}: {exc.msg}\n"
            f"Context: {exc.text!r}"
        ) from exc


def _extract_evolve_block_from_file(program_path: str) -> str:
    """Read the evolved file and return the content between the EVOLVE markers.

    The evolved file is structured as:
        # EVOLVE-BLOCK-START
        <content>
        # EVOLVE-BLOCK-END

    This function returns only <content> — the markers themselves are excluded
    because the template already contains them and they must not be duplicated.

    Raises EvaluationError if:
    - the file cannot be read
    - either marker is missing or appears more than once
    - the END marker precedes the START marker
    - the extracted content is empty after stripping whitespace
    """
    try:
        raw = Path(program_path).read_text(encoding="utf-8")
    except OSError as exc:
        raise EvaluationError(
            f"Could not read evolved program at {program_path}: {exc}"
        ) from exc

    # Strip markdown fences wrapping the whole file (common LLM artefact).
    source = _strip_markdown_fences_from_source(raw)

    # Validate marker presence and uniqueness.
    start_count = source.count(_EVOLVE_START_MARKER)
    end_count = source.count(_EVOLVE_END_MARKER)

    if start_count == 0:
        raise EvaluationError(
            f"EVOLVE-BLOCK-START marker not found in evolved file: {program_path}"
        )
    if start_count > 1:
        raise EvaluationError(
            f"EVOLVE-BLOCK-START marker appears {start_count} times in evolved file "
            f"(expected exactly 1): {program_path}"
        )
    if end_count == 0:
        raise EvaluationError(
            f"EVOLVE-BLOCK-END marker not found in evolved file: {program_path}"
        )
    if end_count > 1:
        raise EvaluationError(
            f"EVOLVE-BLOCK-END marker appears {end_count} times in evolved file "
            f"(expected exactly 1): {program_path}"
        )

    start_pos = source.index(_EVOLVE_START_MARKER)
    end_pos = source.index(_EVOLVE_END_MARKER)

    if end_pos <= start_pos:
        raise EvaluationError(
            f"EVOLVE-BLOCK-END marker precedes EVOLVE-BLOCK-START in evolved file: "
            f"{program_path}"
        )

    # Extract content strictly between the two marker lines (exclusive).
    after_start = source[start_pos + len(_EVOLVE_START_MARKER):]
    content = after_start[: after_start.index(_EVOLVE_END_MARKER)]

    if not content.strip():
        raise EvaluationError(
            f"Evolved block is empty (no content between markers): {program_path}"
        )

    return content


def _splice_block_into_template(block_content: str, template_source: str) -> str:
    """Insert *block_content* between the EVOLVE markers in *template_source*.

    The markers in the template are preserved as-is; only the region between
    them is replaced.  A single newline separates each section.

    Raises EvaluationError if the template markers are malformed.
    """
    # Validate template markers.
    start_count = template_source.count(_EVOLVE_START_MARKER)
    end_count = template_source.count(_EVOLVE_END_MARKER)

    if start_count != 1:
        raise EvaluationError(
            f"Template must contain exactly one EVOLVE-BLOCK-START marker "
            f"(found {start_count}): {_AUTOMATION_TEMPLATE_PATH}"
        )
    if end_count != 1:
        raise EvaluationError(
            f"Template must contain exactly one EVOLVE-BLOCK-END marker "
            f"(found {end_count}): {_AUTOMATION_TEMPLATE_PATH}"
        )

    start_pos = template_source.index(_EVOLVE_START_MARKER)
    end_pos = template_source.index(_EVOLVE_END_MARKER)

    if end_pos <= start_pos:
        raise EvaluationError(
            f"EVOLVE-BLOCK-END precedes EVOLVE-BLOCK-START in template: "
            f"{_AUTOMATION_TEMPLATE_PATH}"
        )

    # Build replacement: START marker / block content / END marker.
    # block_content may carry leading/trailing newlines from extraction;
    # strip and re-add consistently so the output is clean regardless of
    # how the LLM formatted the block.
    replacement = (
        _EVOLVE_START_MARKER
        + "\n"
        + block_content.strip("\n")
        + "\n"
        + _EVOLVE_END_MARKER
    )

    # Replace the entire region from START marker through END marker (inclusive).
    pattern = re.compile(
        re.escape(_EVOLVE_START_MARKER)
        + r".*?"
        + re.escape(_EVOLVE_END_MARKER),
        re.DOTALL,
    )
    spliced, n_subs = pattern.subn(replacement, template_source, count=1)

    if n_subs != 1:
        # Should never happen given the count checks above.
        raise EvaluationError(
            f"Unexpected substitution count during EVOLVE block splice "
            f"(got {n_subs}, expected 1)"
        )

    return spliced


def _prepare_spliced_script(
    *,
    program_path: str,
    output_dir: Path,
    artifacts: dict[str, str | bytes],
) -> Path:
    """Full splice pipeline: read → extract → validate block → splice → validate full → write.

    Returns the Path to the ready-to-execute spliced script written inside
    *output_dir*.  Populates *artifacts* with diagnostic keys at every stage
    so the LLM receives actionable feedback when something fails.

    Raises EvaluationError with a precise message on any failure.
    """
    # Step 1: extract the evolved block content from the evolved file.
    block_content = _extract_evolve_block_from_file(program_path)
    artifacts["evolved_block_char_count"] = str(len(block_content))

    # Step 2: validate the extracted block in isolation before touching the template.
    _validate_python_syntax(block_content, label="evolved block")

    # Step 3: read the immutable automation template.
    if not _AUTOMATION_TEMPLATE_PATH.exists():
        raise EvaluationError(
            f"Automation template not found: {_AUTOMATION_TEMPLATE_PATH}"
        )
    try:
        template_source = _AUTOMATION_TEMPLATE_PATH.read_text(encoding="utf-8")
    except OSError as exc:
        raise EvaluationError(
            f"Could not read automation template {_AUTOMATION_TEMPLATE_PATH}: {exc}"
        ) from exc

    # Step 4: splice the block into the template.
    spliced_source = _splice_block_into_template(block_content, template_source)

    # Step 5: validate the full spliced file — catches any interaction between
    # the block and the surrounding scaffold (e.g. indentation boundary issues).
    _validate_python_syntax(spliced_source, label="spliced automation script")

    # Step 6: write to a stable, inspectable path inside the run output dir.
    dest = output_dir / "spliced_automation.py"
    dest.write_text(spliced_source, encoding="utf-8")
    artifacts["spliced_script_path"] = str(dest)

    return dest


# ---------------------------------------------------------------------------
# Evaluator metadata persistence
# ---------------------------------------------------------------------------

@dataclass
class IterationRecord:
    """Metrics snapshot for a single completed evaluation."""
    makespan_sec: float
    combined_score: float
    perf_term: float
    slo_term: float
    p95_worst_ms: float
    slo_violation_fraction: float
    evaluation_wall_sec: float


@dataclass
class EvaluatorMetadata:
    """Persistent state written to ``output_dir/evaluator_metadata.json``.

    Fields
    ------
    baseline_makespan_sec
        Makespan of the first (unedited) baseline run executed inside
        OpenEvolve.  ``None`` until that run completes.
    best_measured_makespan_sec
        Lowest makespan observed across all completed runs.  ``None`` until
        the first run completes.
    target_makespan_sec
        Makespan of the hand-crafted policy from controlled experiments.
        Stored for reference / report comparison only.
    iterations
        Ordered list of per-iteration metric snapshots.
    """
    baseline_makespan_sec: Optional[float]
    best_measured_makespan_sec: Optional[float]
    target_makespan_sec: float
    iterations: list[dict[str, Any]]


def _load_metadata(path: Path) -> EvaluatorMetadata:
    """Load metadata from *path*, or return a fresh instance if absent."""
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return EvaluatorMetadata(
                baseline_makespan_sec=raw.get("baseline_makespan_sec"),
                best_measured_makespan_sec=raw.get("best_measured_makespan_sec"),
                target_makespan_sec=raw.get("target_makespan_sec", TARGET_MAKESPAN_SEC),
                iterations=raw.get("iterations", []),
            )
        except Exception:
            pass  # Corrupt file — start fresh rather than crashing.
    return EvaluatorMetadata(
        baseline_makespan_sec=None,
        best_measured_makespan_sec=None,
        target_makespan_sec=TARGET_MAKESPAN_SEC,
        iterations=[],
    )


def _save_metadata(path: Path, meta: EvaluatorMetadata) -> None:
    """Atomically write *meta* to *path*."""
    payload = {
        "baseline_makespan_sec": meta.baseline_makespan_sec,
        "best_measured_makespan_sec": meta.best_measured_makespan_sec,
        "target_makespan_sec": meta.target_makespan_sec,
        "iterations": meta.iterations,
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _update_metadata(
    *,
    path: Path,
    record: IterationRecord,
) -> EvaluatorMetadata:
    """Load, mutate, save and return the updated metadata.

    On the first call (baseline run) both ``baseline_makespan_sec`` and
    ``best_measured_makespan_sec`` are initialised from *record*.
    On subsequent calls only ``best_measured_makespan_sec`` is updated when
    *record* improves on the current best.
    """
    meta = _load_metadata(path)

    # First completed run — initialise baseline and best.
    if meta.baseline_makespan_sec is None:
        meta.baseline_makespan_sec = record.makespan_sec
        meta.best_measured_makespan_sec = record.makespan_sec
    elif (
        meta.best_measured_makespan_sec is None
        or record.makespan_sec < meta.best_measured_makespan_sec
    ):
        meta.best_measured_makespan_sec = record.makespan_sec

    # Append iteration snapshot (all fields, makespan_sec always present).
    meta.iterations.append({
        "makespan_sec": record.makespan_sec,
        "combined_score": record.combined_score,
        "perf_term": record.perf_term,
        "slo_term": record.slo_term,
        "p95_worst_ms": record.p95_worst_ms,
        "slo_violation_fraction": record.slo_violation_fraction,
        "evaluation_wall_sec": record.evaluation_wall_sec,
    })

    _save_metadata(path, meta)
    return meta


@dataclass
class SSHProc:
    """A local ssh client subprocess (not the remote PID)."""
    proc: subprocess.Popen[str]
    label: str
    stdout_path: str | None = None
    stderr_path: str | None = None


@dataclass
class RemotePid:
    host: str
    label: str
    pid: int


def evaluate(program_path: str) -> EvaluationResult:
    """Evaluate an evolved scheduler snippet end-to-end.

    *program_path* contains only the EVOLVE block (with its markers).  This
    function splices it into the full automation template before execution so
    that the LLM never sees or touches the surrounding scaffold.

    Phases (must be preserved):
    - Splice evolved block into part3_automation.py template -> spliced_automation.py
    - Deploy memcached pod and wait Ready, then fetch Pod IP
    - Start mcperf agents on both agent nodes
    - Warm cache on measure node (load-only) and block until done
    - Start steady ~30k QPS load on measure node concurrently with scheduler
    - Execute the spliced scheduler script via subprocess (never import)
    - Wait for all 7 jobs to complete (terminal state)
    - Collect results: makespan from pod termination timestamps + mcperf p95 series
    - Teardown (best-effort, robust)
    """
    start_ts = time.time()
    run_id = _next_run_id()
    output_dir = REPO_ROOT / "openevolve" / "runs" / f"{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    mainfests_dir = REPO_ROOT / "automation" / "results" / f"part3" / f"diego_tentative2"

    metrics: dict[str, float] = {
        "combined_score": 0.0,
    }
    artifacts: dict[str, str | bytes] = {
        "program_path": str(program_path),
        "output_dir": str(output_dir),
        "start_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    ssh_background: list[SSHProc] = []
    remote_pids: list[RemotePid] = []
    effective_configs: dict[str, Any] | None = None
    memcached_pod_name = os.environ.get("MEMCACHED_POD_NAME", "some-memcached")
    namespace = os.environ.get("KUBE_NAMESPACE", "default")
    kube_context = os.environ.get("KUBE_CONTEXT")
    memcached_manifest = Path(
        os.environ.get("MEMCACHED_MANIFEST", str(REPO_ROOT / "part3-memcached.yaml"))
    )

    # Path to the persistent metadata file for this evolution run.
    # Caller controls output_dir via the launch command; this file accumulates
    # across all iterations of the same run.
    metadata_path = output_dir / "evaluator_metadata.json"

    # mcperf settings
    ssh_key_file = Path(os.environ.get("SSH_KEY_FILE", str(Path.home() / ".ssh" / "id_rsa")))
    ssh_user = os.environ.get("SSH_USER", "ubuntu")
    agent_a_threads = int(os.environ.get("MCPERF_AGENT_A_THREADS", "2"))
    agent_b_threads = int(os.environ.get("MCPERF_AGENT_B_THREADS", "4"))
    measure_threads = int(os.environ.get("MCPERF_MEASURE_THREADS", "6"))
    measure_conn = int(os.environ.get("MCPERF_MEASURE_CONNS", "4"))
    measure_depth = int(os.environ.get("MCPERF_MEASURE_DEPTH", "4"))
    measure_qps = int(os.environ.get("MCPERF_TARGET_QPS", "30000"))
    measure_scan_end = int(os.environ.get("MCPERF_SCAN_END_QPS", str(measure_qps + 500)))
    measure_scan_step = int(os.environ.get("MCPERF_SCAN_STEP_QPS", "5"))
    measure_interval_s = int(os.environ.get("MCPERF_INTERVAL_S", "10"))
    warmup_timeout_s = int(os.environ.get("MCPERF_WARMUP_TIMEOUT_S", "120"))
    job_timeout_s = int(os.environ.get("JOBS_TIMEOUT_S", "900"))

    # Scheduler invocation settings
    # Some scheduler programs block until all jobs finish, so default this to JOBS_TIMEOUT_S.
    scheduler_timeout_s = int(os.environ.get("SCHEDULER_TIMEOUT_S", str(job_timeout_s)))
    # jobs_dir_override = output_dir / "manifests"
    #  jobs_dir_override.mkdir(parents=True, exist_ok=True)
    # baseline manifests kept at original directory of best manual policy manifests_dir
    try:
        k8s = _k8s_client(kube_context=kube_context)
        core_api = k8s["core_api"]
        batch_api = k8s["batch_api"]
        api_client = k8s["api_client"]

        # Resolve client node IPs via Kubernetes API (no kubectl).
        # mcperf agent-mode traffic must target the nodes' private InternalIP addresses.
        # SSH to the nodes must use public ExternalIP addresses (fallback: InternalIP).
        agent_a_priv_ip, agent_a_pub_ip = _get_node_ip_by_nodetype(core_api, "client-agent-a")
        agent_b_priv_ip, agent_b_pub_ip = _get_node_ip_by_nodetype(core_api, "client-agent-b")
        _measure_priv_ip, measure_pub_ip = _get_node_ip_by_nodetype(core_api, "client-measure")

        # Keep the existing artifact keys, but store the public IPs.
        artifacts["agent_a_ip"] = agent_a_pub_ip
        artifacts["agent_b_ip"] = agent_b_pub_ip
        artifacts["measure_ip"] = measure_pub_ip

        # Pre-clean: delete leftover parsec jobs & memcached pod, and kill mcperf on clients.
        _preclean_k8s(batch_api=batch_api, core_api=core_api, namespace=namespace)
        _preclean_mcperf(
            agent_a_ip=agent_a_pub_ip,
            agent_b_ip=agent_b_pub_ip,
            measure_ip=measure_pub_ip,
            ssh_user=ssh_user,
            ssh_key_file=ssh_key_file,
            artifacts=artifacts,
        )

        # Deploy memcached pod and wait until Ready.
        _delete_pod_if_exists(core_api, namespace=namespace, name=memcached_pod_name)
        _create_from_yaml(api_client, memcached_manifest, namespace=namespace)
        _wait_for_pod_ready(core_api, namespace=namespace, name=memcached_pod_name, timeout_s=180)
        memcached_ip = _get_pod_ip(core_api, namespace=namespace, name=memcached_pod_name)
        if not memcached_ip:
            raise EvaluationError("memcached pod has no IP")
        artifacts["memcached_pod_ip"] = memcached_ip

        # Start mcperf agents (remote background, capture remote pids).
        remote_pids.append(
            _start_mcperf_agent(
                host=agent_a_pub_ip,
                label="agent-a",
                threads=agent_a_threads,
                ssh_user=ssh_user,
                ssh_key_file=ssh_key_file,
            )
        )
        remote_pids.append(
            _start_mcperf_agent(
                host=agent_b_pub_ip,
                label="agent-b",
                threads=agent_b_threads,
                ssh_user=ssh_user,
                ssh_key_file=ssh_key_file,
            )
        )

        # Warm-up phase (blocking) to avoid garbage p95.
        warm = _run_measure_warmup(
            host=measure_pub_ip,
            memcached_ip=memcached_ip,
            ssh_user=ssh_user,
            ssh_key_file=ssh_key_file,
            timeout_s=warmup_timeout_s,
        )
        artifacts["mcperf_warmup_stdout"] = warm["stdout"]
        artifacts["mcperf_warmup_stderr"] = warm["stderr"]
        artifacts["mcperf_warmup_returncode"] = float(warm["returncode"])
        if warm["returncode"] != 0:
            raise EvaluationError("mcperf warmup failed")

        # Start steady load concurrently with scheduler, keep a handle to stdout.
        load_proc = _start_measure_load(
            host=measure_pub_ip,
            memcached_ip=memcached_ip,
            agent_a_ip=agent_a_priv_ip,
            agent_b_ip=agent_b_priv_ip,
            ssh_user=ssh_user,
            ssh_key_file=ssh_key_file,
            threads=measure_threads,
            conn=measure_conn,
            depth=measure_depth,
            target_qps=measure_qps,
            scan_end_qps=measure_scan_end,
            scan_step=measure_scan_step,
            interval_s=measure_interval_s,
            label=f"measure-load-run{run_id}",
            output_dir=output_dir,
        )
        ssh_background.append(load_proc)

        if load_proc.stdout_path:
            artifacts["mcperf_load_stdout_path"] = load_proc.stdout_path
        if load_proc.stderr_path:
            artifacts["mcperf_load_stderr_path"] = load_proc.stderr_path

        # Splice the evolved block into the automation template, validate both
        # the block and the resulting full script, and write to output_dir.
        # This replaces the former strip_out_markdown_chars + direct program_path execution.
        spliced_script_path = _prepare_spliced_script(
            program_path=program_path,
            output_dir=output_dir,
            artifacts=artifacts,
        )

        # Run the spliced scheduler via subprocess (never import).
        # Prefer passing explicit args; fall back to bare invocation if the
        # script does not recognise these flags.
        scheduler_cmd = [
            f"{str(Path.home())}/cloud-comp-arch-project/automation/.venv/bin/python3.13",
            str(spliced_script_path),
            "--namespace",
            namespace,
            "--jobs-dir",
            str(mainfests_dir),
            "--output-dir",
            str(output_dir),
        ]
        artifacts["scheduler_cmd"] = " ".join(scheduler_cmd)
        sched = subprocess.run(
            scheduler_cmd,
            capture_output=True,
            text=True,
            timeout=scheduler_timeout_s,
        )
        if sched.returncode != 0 and ("unrecognized arguments" in (sched.stderr or "").lower()):
            scheduler_cmd = ["python3", str(spliced_script_path)]
            artifacts["scheduler_cmd_fallback"] = " ".join(scheduler_cmd)
            sched = subprocess.run(
                scheduler_cmd,
                capture_output=True,
                text=True,
                timeout=scheduler_timeout_s,
            )

        artifacts["scheduler_stdout"] = sched.stdout
        artifacts["scheduler_stderr"] = sched.stderr
        artifacts["scheduler_returncode"] = float(sched.returncode)
        if sched.returncode != 0:
            raise EvaluationError("scheduler subprocess failed")

        # Scheduler returned 0, so all jobs are already complete. Build the
        # base_name -> actual Job name map with a single API call.
        job_map = _build_job_map(
            batch_api=batch_api,
            base_names=JOB_BASE_NAMES,
            namespace=namespace,
        )
        artifacts["job_name_map_json"] = json.dumps(job_map, indent=2, sort_keys=True)

        # Collect pods (API -> dict) for time/makespan computation.
        pods_payload = _collect_job_pods_payload(
            api_client=api_client,
            core_api=core_api,
            namespace=namespace,
            job_names=list(job_map.values()),
        )
        pods_json_path = output_dir / f"pods_dump.json"
        pods_json_path.write_text(json.dumps(pods_payload, indent=2, sort_keys=True))
        artifacts["pods_json_path"] = str(pods_json_path)

        job_timeline, makespan_sec = _compute_job_timeline_and_makespan(pods_payload, job_map)
        metrics["makespan_sec"] = float(makespan_sec)
        artifacts["job_timeline_json"] = json.dumps(job_timeline, indent=2, sort_keys=True)

        # Gather effective job configurations from Job objects.
        effective_configs = _persist_effective_job_configs(
            batch_api=batch_api,
            namespace=namespace,
            job_names=list(job_map.values()),
            job_base_names=JOB_BASE_NAMES,
            artifacts=artifacts,
        )

        # Compute structural MAP-Elites features from the policy graph written
        # by the scheduler at startup.
        _update_structural_metrics(
            output_dir=output_dir,
            effective_configs=effective_configs,
            metrics=metrics,
        )

        # Stop load generator (best-effort), then parse its stdout from the local file.
        _, _, load_rc = _stop_and_collect_ssh(load_proc.proc, timeout_s=30)
        # metrics["mcperf_load_returncode"] = float(load_rc)

        load_stdout_text = ""
        if load_proc.stdout_path:
            try:
                load_stdout_text = _read_text_utf8(Path(load_proc.stdout_path))
            except Exception as exc:
                artifacts["mcperf_load_stdout_read_error"] = str(exc)
        else:
            artifacts["mcperf_load_stdout_read_error"] = "missing stdout_path"

        p95_series_ms = _parse_mcperf_p95_series_ms(load_stdout_text)
        if p95_series_ms:
            p95_worst_ms = max(p95_series_ms)
            violation_fraction = sum(1 for x in p95_series_ms if x > 1.0) / float(len(p95_series_ms))
        else:
            p95_worst_ms = float("inf")
            violation_fraction = 1.0
        artifacts["mcperf_p95_series_ms_json"] = json.dumps(p95_series_ms)
        metrics["p95_worst_ms"] = float(p95_worst_ms)
        metrics["slo_violation_fraction"] = float(violation_fraction)

        slo = _slo_term(p95_series_ms=p95_series_ms, slo_ms=1.0)
        wall_sec = float(time.time() - start_ts)

        # Load current metadata to check whether a baseline exists.
        meta_snapshot = _load_metadata(metadata_path)
        is_baseline_run = meta_snapshot.baseline_makespan_sec is None

        if is_baseline_run:
            # First run: the unedited program establishes the environmental
            # baseline.  Score is 0.5 = sigmoid(0) — neutral by definition.
            perf = 0.5
        else:
            perf = _perf_speedup_term(
                measured_makespan_sec=makespan_sec,
                baseline_makespan_sec=meta_snapshot.baseline_makespan_sec,
            )

        combined = float(perf * slo)
        metrics["perf_term"] = float(perf)
        metrics["slo_term"] = float(slo)
        metrics["combined_score"] = combined
        metrics["evaluation_wall_sec"] = wall_sec
        metrics["is_baseline_run"] = float(is_baseline_run)
        metrics["target_makespan_sec"] = TARGET_MAKESPAN_SEC

        # Persist iteration record and update rolling best.
        record = IterationRecord(
            makespan_sec=float(makespan_sec),
            combined_score=combined,
            perf_term=float(perf),
            slo_term=float(slo),
            p95_worst_ms=float(p95_worst_ms),
            slo_violation_fraction=float(violation_fraction),
            evaluation_wall_sec=wall_sec,
        )
        updated_meta = _update_metadata(path=metadata_path, record=record)
        metrics["baseline_makespan_sec"] = float(updated_meta.baseline_makespan_sec)
        metrics["best_measured_makespan_sec"] = float(updated_meta.best_measured_makespan_sec)

        return EvaluationResult(metrics=metrics, artifacts=artifacts)

    except subprocess.TimeoutExpired as exc:
        artifacts["error_type"] = "TimeoutExpired"
        artifacts["error_message"] = str(exc)
        metrics["traceback"] = traceback.format_exc()
        metrics["combined_score"] = 0.0
        metrics["evaluation_wall_sec"] = float(time.time() - start_ts)
        return EvaluationResult(metrics=metrics, artifacts=artifacts)
    except Exception as exc:
        if "batch_api" in locals() and "job_map" in locals():
            try:
                effective_configs = _persist_effective_job_configs(
                    batch_api=batch_api,
                    namespace=namespace,
                    job_names=list(job_map.values()),
                    job_base_names=JOB_BASE_NAMES,
                    artifacts=artifacts,
                )
            except Exception as cfg_exc:
                artifacts["job_effective_configs_error"] = str(cfg_exc)

        _update_structural_metrics(
            output_dir=output_dir,
            effective_configs=effective_configs,
            metrics=metrics,
        )
        metrics["error_type"] = type(exc).__name__
        metrics["error_message"] = str(exc)
        metrics["traceback"] = traceback.format_exc()
        metrics["combined_score"] = 0.0
        metrics["evaluation_wall_sec"] = float(time.time() - start_ts)
        metrics["scheduler_stdout"] = sched.stdout
        metrics["scheduler_stderr"] = sched.stderr
        return EvaluationResult(metrics=metrics, artifacts=artifacts)
    finally:
        # Teardown must be best-effort and robust.
        try:
            for sshp in ssh_background:
                _stop_and_collect_ssh(sshp.proc, timeout_s=5)
        except Exception:
            pass

        try:
            for rp in remote_pids:
                _kill_remote_pid_and_verify(
                    host=rp.host,
                    pid=rp.pid,
                    ssh_user=ssh_user,
                    ssh_key_file=ssh_key_file,
                )
        except Exception:
            pass

        try:
            # Also kill any stray mcperf processes, just in case.
            _preclean_mcperf(
                agent_a_ip=artifacts.get("agent_a_ip", ""),
                agent_b_ip=artifacts.get("agent_b_ip", ""),
                measure_ip=artifacts.get("measure_ip", ""),
                ssh_user=ssh_user,
                ssh_key_file=ssh_key_file,
                artifacts=None,
            )
        except Exception:
            pass

        try:
            k8s = _k8s_client(kube_context=kube_context)
            _preclean_k8s(batch_api=k8s["batch_api"], core_api=k8s["core_api"], namespace=namespace)
            _delete_pod_if_exists(k8s["core_api"], namespace=namespace, name=memcached_pod_name)
        except Exception:
            pass


def _k8s_client(*, kube_context: str | None) -> dict[str, Any]:
    try:
        from kubernetes import client, config
        from kubernetes.utils import create_from_yaml
    except Exception as exc:  # pragma: no cover
        raise EvaluationError(
            "Missing Kubernetes dependencies. Install 'kubernetes' and 'PyYAML'."
        ) from exc

    try:
        config.load_kube_config(context=kube_context)
    except Exception:
        # Best effort: allow in-cluster too.
        config.load_incluster_config()

    api_client = client.ApiClient()
    return {
        "client": client,
        "create_from_yaml": create_from_yaml,
        "api_client": api_client,
        "core_api": client.CoreV1Api(api_client),
        "batch_api": client.BatchV1Api(api_client),
    }


def _create_from_yaml(api_client: Any, yaml_path: Path, *, namespace: str) -> None:
    from kubernetes.utils import create_from_yaml

    if not yaml_path.exists():
        raise EvaluationError(f"memcached manifest not found: {yaml_path}")
    create_from_yaml(api_client, str(yaml_path), namespace=namespace, verbose=False)


def _get_node_ip_by_nodetype(core_api: Any, nodetype: str) -> tuple[str, str]:
    # Prefer explicit labels when present.
    nodes = []
    try:
        nodes = core_api.list_node(label_selector=f"cca-project-nodetype={nodetype}").items
    except Exception:
        nodes = []

    # Fallback: node names in this project often look like client-agent-a-k6mb.
    if not nodes:
        try:
            all_nodes = core_api.list_node().items
            nodes = [
                n
                for n in all_nodes
                if (getattr(n.metadata, "name", "") == nodetype)
                or getattr(n.metadata, "name", "").startswith(nodetype + "-")
            ]
        except Exception:
            nodes = []

    if not nodes:
        raise EvaluationError(
            f"Could not find node for '{nodetype}'. Tried label cca-project-nodetype={nodetype} and name prefix '{nodetype}-'"
        )

    # Deterministic pick.
    nodes = sorted(nodes, key=lambda n: getattr(n.metadata, "name", ""))
    node = nodes[0]
    addresses = {a.type: a.address for a in (node.status.addresses or [])}
    private_ip = addresses.get("InternalIP") or ""
    public_ip = addresses.get("ExternalIP") or ""
    if not private_ip and not public_ip:
        raise EvaluationError(f"Node '{nodetype}' has no InternalIP/ExternalIP in status.addresses")

    # Some clusters omit ExternalIP. For SSH, allow falling back to InternalIP.
    if not public_ip:
        public_ip = private_ip

    # mcperf agent-mode wants a private address; if missing, fall back to public.
    if not private_ip:
        private_ip = public_ip

    return private_ip, public_ip


def _delete_pod_if_exists(core_api: Any, *, namespace: str, name: str) -> None:
    from kubernetes.client import ApiException

    try:
        core_api.delete_namespaced_pod(name=name, namespace=namespace, grace_period_seconds=0)
    except ApiException as exc:
        if exc.status != 404:
            raise
    # Give the API a moment to remove it.
    for _ in range(60):
        try:
            core_api.read_namespaced_pod(name=name, namespace=namespace)
        except ApiException as exc:
            if exc.status == 404:
                return
        time.sleep(1)


def _wait_for_pod_ready(core_api: Any, *, namespace: str, name: str, timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        pod = core_api.read_namespaced_pod(name=name, namespace=namespace)
        conds = pod.status.conditions or []
        ready = any(c.type == "Ready" and c.status == "True" for c in conds)
        phase = pod.status.phase
        if ready and phase == "Running":
            return
        time.sleep(2)
    raise EvaluationError(f"Timeout waiting for pod Ready: {namespace}/{name}")


def _get_pod_ip(core_api: Any, *, namespace: str, name: str) -> str:
    pod = core_api.read_namespaced_pod(name=name, namespace=namespace)
    return pod.status.pod_ip or ""


def _preclean_k8s(*, batch_api: Any, core_api: Any, namespace: str) -> None:
    """Delete existing PARSEC jobs/pods and block until they are fully gone.

    Foreground deletion is asynchronous — without waiting, the scheduler can
    race with the ongoing deletion: it gets a 409 on create, silently tracks
    the terminating job, sees a stale Complete event, and returns 0 while the
    cluster is still tearing down.  Subsequent pod queries then find nothing.
    """
    from kubernetes.client import ApiException

    try:
        # Initiate deletion of all matching jobs.
        deleted_job_names: set[str] = set()
        jobs = batch_api.list_namespaced_job(namespace=namespace).items
        for job in jobs:
            name = job.metadata.name if job.metadata else ""
            if not name:
                continue
            if any(name == b or name.startswith(b + "-") for b in JOB_BASE_NAMES):
                try:
                    batch_api.delete_namespaced_job(
                        name=name,
                        namespace=namespace,
                        propagation_policy="Foreground",
                        grace_period_seconds=0,
                    )
                    deleted_job_names.add(name)
                except ApiException as exc:
                    if exc.status != 404:
                        raise

        # Explicitly delete any stray parsec pods not owned by a Job.
        pods = core_api.list_namespaced_pod(namespace=namespace).items
        for pod in pods:
            pname = pod.metadata.name if pod.metadata else ""
            if not pname:
                continue
            if any(pname.startswith(b) for b in JOB_BASE_NAMES):
                try:
                    core_api.delete_namespaced_pod(
                        name=pname,
                        namespace=namespace,
                        grace_period_seconds=0,
                    )
                except ApiException as exc:
                    if exc.status != 404:
                        raise

        # Block until every deleted job has disappeared from the API.
        if deleted_job_names:
            deadline = time.time() + 120
            while time.time() < deadline:
                remaining = {
                    j.metadata.name
                    for j in batch_api.list_namespaced_job(namespace=namespace).items
                    if j.metadata and j.metadata.name
                }
                if not (deleted_job_names & remaining):
                    break
                time.sleep(2)

        # Block until all parsec pods are gone (Foreground deletion removes pods first).
        deadline = time.time() + 60
        while time.time() < deadline:
            pods = core_api.list_namespaced_pod(namespace=namespace).items
            still_present = [
                p for p in pods
                if p.metadata and p.metadata.name
                and any(p.metadata.name.startswith(b) for b in JOB_BASE_NAMES)
            ]
            if not still_present:
                break
            time.sleep(2)

    except Exception:
        # Always best-effort.
        return


def _ssh_base_args(*, ssh_key_file: Path) -> list[str]:
    args = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
    ]
    if ssh_key_file:
        args += ["-i", str(ssh_key_file)]
    return args


def _ssh_run(
    *,
    host: str,
    ssh_user: str,
    ssh_key_file: Path,
    remote_cmd: str,
    timeout_s: int | None,
) -> subprocess.CompletedProcess[str]:
    cmd = _ssh_base_args(ssh_key_file=ssh_key_file) + [f"{ssh_user}@{host}", remote_cmd]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)


def _ssh_popen(
    *,
    host: str,
    ssh_user: str,
    ssh_key_file: Path,
    remote_cmd: str,
    stdout_path: Path | None = None,
    stderr_path: Path | None = None,
) -> subprocess.Popen[str]:
    cmd_argv = _ssh_base_args(ssh_key_file=ssh_key_file) + [f"{ssh_user}@{host}", remote_cmd]
    if stdout_path is not None or stderr_path is not None:
        # Stream output directly to local files via shell redirection (more reliable than PIPE).
        cmd_str = shlex.join(cmd_argv)
        if stdout_path is not None:
            cmd_str += f" > {shlex.quote(str(stdout_path))}"
        if stderr_path is not None:
            cmd_str += f" 2> {shlex.quote(str(stderr_path))}"
        return subprocess.Popen(cmd_str, shell=True, text=True, executable="/bin/bash")
    return subprocess.Popen(cmd_argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _safe_filename_component(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "out"
    return "".join((c if (c.isalnum() or c in ("-", "_")) else "_") for c in s)


def _preclean_mcperf(
    *,
    agent_a_ip: str,
    agent_b_ip: str,
    measure_ip: str,
    ssh_user: str,
    ssh_key_file: Path,
    artifacts: dict[str, str | bytes] | None,
) -> None:
    # Kill common binary names; do not fail evaluation if cleanup fails.
    remote = (
        "set -euo pipefail; "
        "(pgrep -f '(mcperf|mcp_perf)' >/dev/null 2>&1 && pkill -9 -f '(mcperf|mcp_perf)' || true); "
        "sleep 0.2; "
        "pgrep -f '(mcperf|mcp_perf)' >/dev/null 2>&1 && echo 'WARN: mcperf still running' || true"
    )
    for label, host in (("agent-a", agent_a_ip), ("agent-b", agent_b_ip), ("measure", measure_ip)):
        if not host:
            continue
        try:
            cp = _ssh_run(
                host=host,
                ssh_user=ssh_user,
                ssh_key_file=ssh_key_file,
                remote_cmd=remote,
                timeout_s=30,
            )
            if artifacts is not None:
                artifacts[f"preclean_mcperf_{label}_stdout"] = cp.stdout
                artifacts[f"preclean_mcperf_{label}_stderr"] = cp.stderr
        except Exception:
            continue


def _remote_mcperf_dir() -> str:
    # Support both repositories used in this project.
    return "cd $HOME/memcache-perf-dynamic"


def _start_mcperf_agent(
    *,
    host: str,
    label: str,
    threads: int,
    ssh_user: str,
    ssh_key_file: Path,
) -> RemotePid:
    remote_cmd = (
        "set -euo pipefail; "
        f"{_remote_mcperf_dir()}; "
        f"nohup ./mcperf -T {threads} -A > mcperf-{label}.log 2>&1 < /dev/null & echo $!"
    )
    cp = _ssh_run(
        host=host,
        ssh_user=ssh_user,
        ssh_key_file=ssh_key_file,
        remote_cmd=remote_cmd,
        timeout_s=30,
    )
    if cp.returncode != 0:
        raise EvaluationError(
            f"Failed to start mcperf agent on {label} ({host}): rc={cp.returncode} stderr={cp.stderr.strip()}"
        )
    pid_str = cp.stdout.strip().splitlines()[-1].strip() if cp.stdout.strip() else ""
    try:
        pid = int(pid_str)
    except Exception as exc:
        raise EvaluationError(f"Could not parse remote PID for {label}: stdout={cp.stdout!r}") from exc
    return RemotePid(host=host, label=label, pid=pid)


def _run_measure_warmup(
    *,
    host: str,
    memcached_ip: str,
    ssh_user: str,
    ssh_key_file: Path,
    timeout_s: int,
) -> dict[str, Any]:
    remote_cmd = (
        "set -euo pipefail; "
        f"{_remote_mcperf_dir()}; "
        f"./mcperf -s {memcached_ip} --loadonly"
    )
    cp = _ssh_run(
        host=host,
        ssh_user=ssh_user,
        ssh_key_file=ssh_key_file,
        remote_cmd=remote_cmd,
        timeout_s=timeout_s,
    )
    return {"returncode": cp.returncode, "stdout": cp.stdout, "stderr": cp.stderr}


def _start_measure_load(
    *,
    host: str,
    memcached_ip: str,
    agent_a_ip: str,
    agent_b_ip: str,
    ssh_user: str,
    ssh_key_file: Path,
    threads: int,
    conn: int,
    depth: int,
    target_qps: int,
    scan_end_qps: int,
    scan_step: int,
    interval_s: int,
    label: str,
    output_dir: Path,
) -> SSHProc:
    remote_cmd = (
        "set -euo pipefail; "
        f"{_remote_mcperf_dir()}; "
        f"./mcperf -s {memcached_ip} -a {agent_a_ip} -a {agent_b_ip} --noload -T {threads} -C {conn} -D {depth} -Q 1000 -c 4 -t {interval_s} --scan {target_qps}:{scan_end_qps}:{scan_step}"
    )

    safe_label = _safe_filename_component(label)
    stdout_path = output_dir / f"mcperf-{safe_label}.txt"
    stderr_path = output_dir / f"mcperf-{safe_label}.stderr.txt"
    proc = _ssh_popen(
        host=host,
        ssh_user=ssh_user,
        ssh_key_file=ssh_key_file,
        remote_cmd=remote_cmd,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    return SSHProc(proc=proc, label=label, stdout_path=str(stdout_path), stderr_path=str(stderr_path))


def _read_text_utf8(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _stop_and_collect_ssh(proc: subprocess.Popen[str], *, timeout_s: int) -> tuple[str, str, int]:
    """Stop a local ssh process and collect its output."""
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGINT)
        except Exception:
            pass
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except Exception:
            pass
        stdout, stderr = proc.communicate(timeout=timeout_s)
    return stdout or "", stderr or "", int(proc.returncode or 0)


def _kill_remote_pid_and_verify(
    *,
    host: str,
    pid: int,
    ssh_user: str,
    ssh_key_file: Path,
) -> None:
    remote_cmd = (
        "set -euo pipefail; "
        f"pid={pid}; "
        "if kill -0 $pid >/dev/null 2>&1; then kill -9 $pid || true; fi; "
        "for i in $(seq 1 20); do "
        "  if kill -0 $pid >/dev/null 2>&1; then sleep 0.25; else exit 0; fi; "
        "done; "
        "echo 'ERROR: pid still alive' >&2; ps -p $pid -o pid=,cmd= >&2 || true; exit 1"
    )
    cp = _ssh_run(
        host=host,
        ssh_user=ssh_user,
        ssh_key_file=ssh_key_file,
        remote_cmd=remote_cmd,
        timeout_s=30,
    )
    if cp.returncode != 0:
        raise EvaluationError(
            f"Failed to kill remote pid {pid} on {host}: rc={cp.returncode} stderr={cp.stderr.strip()}"
        )


def _build_job_map(
    *,
    batch_api: Any,
    base_names: list[str],
    namespace: str,
) -> dict[str, str]:
    """Return base_name -> actual Job name from a single API call.

    The scheduler subprocess returning 0 already guarantees all jobs completed
    successfully, so no retry loop is needed. Raises immediately if a base name
    is absent from the cluster or its latest job reports failure.
    """
    jobs = batch_api.list_namespaced_job(namespace=namespace).items
    by_base: dict[str, list[Any]] = {b: [] for b in base_names}
    for job in jobs:
        name = job.metadata.name if job.metadata else ""
        if name in by_base:
            by_base[name].append(job)

    job_map: dict[str, str] = {}
    for base, candidates in by_base.items():
        if not candidates:
            # Jobs may have already been removed from the cluster (Foreground deletion
            # from preclean, manifest TTL, etc.). The scheduler returning 0 already
            # guarantees all jobs completed successfully. The automation script always
            # submits jobs under their exact base names, so the actual name == base.
            job_map[base] = base
            continue

        def _ts(job: Any) -> float:
            try:
                ct = job.metadata.creation_timestamp
                return float(ct.timestamp()) if ct else 0.0
            except Exception:
                return 0.0

        candidates.sort(key=_ts)
        latest = candidates[-1]
        st = latest.status
        conds = st.conditions or []
        is_failed = any(c.type == "Failed" and c.status == "True" for c in conds) or (
            (getattr(st, "failed", 0) or 0) > 0 and (getattr(st, "active", 0) or 0) == 0
        )
        if is_failed:
            raise EvaluationError(f"Job reported failure: {latest.metadata.name}")
        job_map[base] = latest.metadata.name

    return job_map


def _collect_job_pods_payload(
    *,
    api_client: Any,
    core_api: Any,
    namespace: str,
    job_names: list[str],
) -> dict[str, Any]:
    """Collect a compact pod payload for later makespan extraction.

    The Kubernetes Python client's `sanitize_for_serialization()` uses camelCase
    keys and includes very large, repetitive fields (e.g., `managedFields`).
    To keep artifacts readable and stable, we retain only the small subset we
    need for timeline reconstruction.
    """

    items: list[dict[str, Any]] = []
    for job_name in job_names:
        pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=f"job-name={job_name}").items
        for pod in pods:
            raw = api_client.sanitize_for_serialization(pod)
            meta = raw.get("metadata", {}) or {}
            spec = raw.get("spec", {}) or {}
            status = raw.get("status", {}) or {}
            items.append(
                {
                    "metadata": {
                        "name": meta.get("name"),
                        "namespace": meta.get("namespace"),
                        "labels": meta.get("labels", {}),
                        "creationTimestamp": meta.get("creationTimestamp"),
                    },
                    "spec": {
                        "nodeName": spec.get("nodeName"),
                        "nodeSelector": spec.get("nodeSelector", {}),
                        "schedulerName": spec.get("schedulerName"),
                    },
                    "status": {
                        "phase": status.get("phase"),
                        "startTime": status.get("startTime"),
                        "containerStatuses": status.get("containerStatuses", []),
                    },
                }
            )

    return {"apiVersion": "v1", "kind": "List", "items": items}


def _parse_rfc3339_utc(ts: str) -> datetime:
    """Parse Kubernetes RFC3339 timestamps to a UTC datetime.

    In practice this can be either Z-suffixed ("...Z") or ISO8601 with an
    explicit offset ("...+00:00"), sometimes with fractional seconds.
    """
    ts = (ts or "").strip()
    if not ts:
        raise ValueError("empty timestamp")
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _compute_job_timeline_and_makespan(
    pods_payload: dict[str, Any],
    job_map: dict[str, str],
) -> tuple[dict[str, Any], float]:
    """Compute per-job timeline and makespan (seconds)."""
    # base -> best terminated container record (latest finishedAt)
    best: dict[str, dict[str, Any]] = {}

    def _get_any(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
        for k in keys:
            if k in d:
                return d[k]
        return default

    for item in pods_payload.get("items", []):
        try:
            status = item.get("status", {}) or {}
            spec = item.get("spec", {}) or {}
            meta = item.get("metadata", {}) or {}

            # Use the pod's job-name label to identify which base job this pod
            # belongs to — avoids any container-name prefix matching.
            base = (meta.get("labels") or {}).get("job-name", "")
            if not base or base not in job_map:
                continue

            cstats = _get_any(status, "containerStatuses", "container_statuses", default=[]) or []
            for cstat in cstats:
                cname = (cstat or {}).get("name")
                if not cname or cname == "memcached":
                    continue

                state = (cstat or {}).get("state", {}) or {}
                terminated = _get_any(state, "terminated", default=None)
                if not terminated:
                    continue

                started_at = _get_any(terminated, "startedAt", "started_at", default=None)
                finished_at = _get_any(terminated, "finishedAt", "finished_at", default=None)
                if not started_at or not finished_at:
                    continue

                exit_code_val = _get_any(terminated, "exitCode", "exit_code", default=0)
                candidate = {
                    "pod": meta.get("name", ""),
                    "container": cname,
                    "node_name": _get_any(spec, "nodeName", "node_name", default="") or "",
                    "node_selector": _get_any(spec, "nodeSelector", "node_selector", default={}) or {},
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "exit_code": int(exit_code_val or 0),
                }
                if base not in best:
                    best[base] = candidate
                else:
                    prev_end = best[base].get("finished_at")
                    try:
                        if prev_end and _parse_rfc3339_utc(finished_at) > _parse_rfc3339_utc(prev_end):
                            best[base] = candidate
                    except Exception:
                        best[base] = candidate
                break
        except Exception:
            continue

    if len(best) != len(job_map):
        missing = sorted(set(job_map.keys()) - set(best.keys()))
        raise EvaluationError(f"Missing terminated pod timestamps for jobs: {missing}")

    starts = []
    ends = []
    timeline: dict[str, Any] = {}
    for base, rec in best.items():
        s = _parse_rfc3339_utc(rec["started_at"]).timestamp()
        e = _parse_rfc3339_utc(rec["finished_at"]).timestamp()
        starts.append(s)
        ends.append(e)
        nodetype = rec.get("node_selector", {}).get("cca-project-nodetype", "")
        if not nodetype:
            nodename = rec.get("node_name", "")
            if "node-a" in nodename:
                nodetype = "node-a-8core"
            elif "node-b" in nodename:
                nodetype = "node-b-4core"
        timeline[base] = {
            "start_epoch_s": s,
            "end_epoch_s": e,
            "duration_s": e - s,
            "node": nodetype,
            "pod": rec.get("pod"),
            "exit_code": rec.get("exit_code"),
        }
        if int(rec.get("exit_code") or 0) != 0:
            raise EvaluationError(f"Non-zero exit code for {base}: {rec.get('exit_code')}")

    makespan = max(ends) - min(starts)
    return timeline, float(makespan)


def _collect_effective_job_configs(
    *,
    batch_api: Any,
    namespace: str,
    job_names: list[str],
    job_base_names: list[str],
) -> dict[str, Any]:
    configs: dict[str, Any] = {}
    for jn in job_names:
        job = batch_api.read_namespaced_job(name=jn, namespace=namespace)
        podspec = job.spec.template.spec
        container = podspec.containers[0]
        base = _infer_base_job_name(job_name=jn, base_names=job_base_names)
        configs[base] = {
            "job_name": jn,
            "node_selector": dict(podspec.node_selector or {}),
            "resources": {
                "requests": (container.resources.requests or {}) if container.resources else {},
                "limits": (container.resources.limits or {}) if container.resources else {},
            },
            "command": list(container.command or []),
            "args": list(container.args or []),
        }
    return configs


def _persist_effective_job_configs(
    *,
    batch_api: Any,
    namespace: str,
    job_names: list[str],
    job_base_names: list[str],
    artifacts: dict[str, str | bytes],
) -> dict[str, Any]:
    """Collect effective job configs and persist them into artifacts."""
    effective_configs = _collect_effective_job_configs(
        batch_api=batch_api,
        namespace=namespace,
        job_names=job_names,
        job_base_names=job_base_names,
    )
    artifacts["job_effective_configs_json"] = json.dumps(
        effective_configs, indent=2, sort_keys=True
    )
    return effective_configs


def _update_structural_metrics(
    *,
    output_dir: Path,
    effective_configs: dict[str, Any] | None,
    metrics: dict[str, float],
) -> None:
    """Compute structural metrics and merge them into metrics dict."""
    policy_graph_path = output_dir / "policy_graph.json"
    structural = _compute_structural_metrics(
        policy_graph_path=policy_graph_path,
        effective_configs=effective_configs,
    )
    metrics.update({k: float(v) for k, v in structural.items()})


def _infer_base_job_name(*, job_name: str, base_names: list[str]) -> str:
    for base in base_names:
        if job_name == base or job_name.startswith(base + "-"):
            return base
    return job_name


def _parse_mcperf_p95_series_ms(stdout: str) -> list[float]:
    """Parse p95 series from mcperf-like output.

    The project examples contain a header where p95 is column index 12 (0-based).
    Values are in microseconds; we convert to milliseconds.
    """
    series_ms: list[float] = []
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if not parts or parts[0] != "read":
            continue
        if len(parts) <= 12:
            continue
        try:
            p95_us = float(parts[12])
            series_ms.append(p95_us / 1000.0)
        except Exception:
            continue
    return series_ms


# ---------------------------------------------------------------------------
# Structural MAP-Elites feature computation
# ---------------------------------------------------------------------------

# Fixed bit positions for the node-allocation scalar (index = position in
# JOB_BASE_NAMES, bit=1 means node-a-8core).  Baseline policy encodes to:
#   streamcluster(1) freqmine(0) canneal(1) blackscholes(1) radix(1) barnes(0) vips(0)
#   = 0b1011100 = 92
_NODE_SCALAR_BIT: dict[str, int] = {
    "parsec-streamcluster": 6,
    "parsec-freqmine":      5,
    "parsec-canneal":       4,
    "parsec-blackscholes":  3,
    "parsec-radix":         2,
    "parsec-barnes":        1,
    "parsec-vips":          0,
}

# Baseline thread sums per node (used for parallelism-delta features).
_BASELINE_THREADS_NODE_A: float = 15.0   # streamcluster(7)+canneal(4)+blackscholes(3)+radix(1)
_BASELINE_THREADS_NODE_B: float = 12.0   # freqmine(4)+barnes(4)+vips(4)


def _parse_thread_count(args_list: list[str]) -> int | None:
	"""Extract the -n <N> thread count from a container args list.

	The args list is typically ["-c", "taskset -c 1-7 ./run ... -n 7"].
	Returns None if the pattern is not found.
	"""
	import re
	full_cmd = " ".join(args_list)
	m = re.search(r"\s-n\s+(\d+)", full_cmd)
	return int(m.group(1)) if m else None


def _compute_structural_metrics(
	*,
	policy_graph_path: Path,
	effective_configs: dict[str, Any],
) -> dict[str, float]:
    """Compute MAP-Elites features and auxiliary structural metrics.

    Reads ``policy_graph.json`` written by the scheduler at startup for the
    dependency and node-assignment data.  Falls back to safe defaults on any
    parse error so that a graph-write failure never blocks scoring.

    Returns a flat dict of float metrics ready to merge into the main metrics
    dict.  Keys:
    node_allocation_scalar   — 7-bit int, Hamming-distance-aware (MAP-Elites dim 1)
    dep_edge_count           — total dependency edges 0-21   (MAP-Elites dim 2)
    parallelism_delta_node_a — thread-sum delta vs baseline on node-a
    parallelism_delta_node_b — thread-sum delta vs baseline on node-b
    """
    result: dict[str, float] = {
        "node_allocation_scalar": 92.0,   # baseline default
        "dep_edge_count": 7.0,            # baseline default
        "parallelism_delta_node_a": 0.0,
        "parallelism_delta_node_b": 0.0,
    }

    try:
        raw = json.loads(policy_graph_path.read_text(encoding="utf-8"))
    except Exception:
        return result

    # --- Feature 1: node_allocation_scalar ---
    assignment: dict[str, str] = raw.get("node_assignment", {})
    scalar = 0
    for job, bit in _NODE_SCALAR_BIT.items():
        if assignment.get(job, "") == "node-a-8core":
            scalar |= (1 << bit)
            result["node_allocation_scalar"] = float(scalar)

    # --- Feature 2: dep_edge_count ---
    deps: dict[str, list[str]] = raw.get("dependencies", {})
    result["dep_edge_count"] = float(sum(len(v) for v in deps.values()))

    # --- Auxiliary: parallelism deltas ---
    threads_a = 0.0
    threads_b = 0.0
    if effective_configs is not None:
        for base_name, cfg in effective_configs.items():
            args_list: list[str] = cfg.get("args", [])
            n = _parse_thread_count(args_list)
            if n is None:
                continue
            node = assignment.get(base_name, "")
            if node == "node-a-8core":
                threads_a += n
            elif node == "node-b-4core":
                threads_b += n
        threads_b = threads_b - _BASELINE_THREADS_NODE_B
        threads_a = threads_a - _BASELINE_THREADS_NODE_A

    result["parallelism_delta_node_a"] = threads_a
    result["parallelism_delta_node_b"] = threads_b

    return result


def _perf_speedup_term(*, measured_makespan_sec: float, baseline_makespan_sec: float, k: float = 4.0) -> float:
    if measured_makespan_sec <= 0 or baseline_makespan_sec <= 0:
        return 0.0
    log_speedup = math.log(baseline_makespan_sec / measured_makespan_sec)
    return 1.0 / (1.0 + math.exp(-k * log_speedup))


def _slo_term(*, p95_series_ms: list[float], slo_ms: float = 1.0) -> float:
    if not p95_series_ms:
        return 0.0
    worst = max(p95_series_ms)
    violations = sum(1 for x in p95_series_ms if x > slo_ms)
    violation_fraction = violations / float(len(p95_series_ms))
    if worst <= slo_ms:
        return 1.0
    # Violator regime: always < 1.0; cap at 0.05.
    persistence_term = 1.0 - violation_fraction
    return 0.05 * persistence_term


def _next_run_id() -> int:
    """
    The caller sets RUN_ID manually before each OpenEvolve invocation.
    counter_path is retained in the signature for backward compatibility
    but is no longer used.
    """
    raw = os.environ.get("RUN_ID")
    if raw is None:
        raise RuntimeError("RUN_ID environment variable is not set. Export it before running.")
    return int(raw)

if __name__ == "__main__":
    sys.exit(evaluate(sys.argv[1]))
