#!/usr/bin/env python3
"""
Reusable Kubernetes helper functions for the CCA project automation.
Wraps kubectl commands with proper error handling and status detection.
"""

import subprocess
import sys
import time
import re


def run_kubectl(args, stdin_data=None, capture=True, timeout=300):
    """Run a kubectl command, return (returncode, stdout, stderr)."""
    cmd = ["kubectl"] + args
    print(f"  [kubectl] {' '.join(cmd)}", file=sys.stderr)
    try:
        result = subprocess.run(
            cmd,
            input=stdin_data,
            capture_output=capture,
            text=True,
            timeout=timeout,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        print(f"  [kubectl] TIMEOUT after {timeout}s", file=sys.stderr)
        return -1, "", "timeout"


def kubectl_create(yaml_path, node_selector_override=None):
    """Create a resource from a YAML file. Optionally override the nodeSelector value.

    Args:
        yaml_path: Path to the YAML manifest.
        node_selector_override: If set, replace any cca-project-nodetype value
                                with this string (e.g. "parsec").

    Returns:
        True if creation succeeded, False otherwise.
    """
    if node_selector_override:
        with open(yaml_path) as f:
            content = f.read()
        content = re.sub(
            r'(cca-project-nodetype:\s*)"[^"]+"',
            rf'\1"{node_selector_override}"',
            content,
        )
        rc, out, err = run_kubectl(["create", "-f", "-"], stdin_data=content)
    else:
        rc, out, err = run_kubectl(["create", "-f", yaml_path])

    if rc != 0:
        print(f"  [kubectl] create failed: {err.strip()}", file=sys.stderr)
        return False
    print(f"  [kubectl] {out.strip()}", file=sys.stderr)
    return True


def kubectl_wait_job(job_name, timeout=600):
    """Wait for a Kubernetes Job to complete.

    Returns:
        "Complete" if the job succeeded,
        "Failed" if the job failed,
        "Timeout" if it timed out.
    """
    rc, out, err = run_kubectl(
        ["wait", f"--for=condition=complete", f"job/{job_name}",
         f"--timeout={timeout}s"],
        timeout=timeout + 30,
    )
    if rc == 0:
        return "Complete"

    # Check if the job failed (pods in error state)
    rc2, out2, _ = run_kubectl(
        ["get", f"job/{job_name}", "-o",
         "jsonpath={.status.failed}"],
    )
    if rc2 == 0 and out2.strip() and int(out2.strip()) > 0:
        return "Failed"

    return "Timeout"


def kubectl_wait_pod(pod_name, timeout=120):
    """Wait for a Pod to be in Running phase.

    Returns:
        True if the pod is running, False otherwise.
    """
    rc, out, err = run_kubectl(
        ["wait", f"--for=condition=Ready", f"pod/{pod_name}",
         f"--timeout={timeout}s"],
        timeout=timeout + 30,
    )
    if rc == 0:
        return True
    print(f"  [kubectl] pod {pod_name} not ready: {err.strip()}", file=sys.stderr)
    return False


def kubectl_logs_job(job_name):
    """Get logs from a completed Job's pod.

    Returns:
        Log string, or empty string on failure.
    """
    rc, out, err = run_kubectl(["logs", f"job/{job_name}"])
    if rc != 0:
        print(f"  [kubectl] logs failed for job/{job_name}: {err.strip()}",
              file=sys.stderr)
        return ""
    return out


def kubectl_logs_pod(pod_name):
    """Get logs from a Pod.

    Returns:
        Log string, or empty string on failure.
    """
    rc, out, err = run_kubectl(["logs", pod_name])
    if rc != 0:
        print(f"  [kubectl] logs failed for pod/{pod_name}: {err.strip()}",
              file=sys.stderr)
        return ""
    return out


def kubectl_delete_job(job_name):
    """Delete a Job and its pods."""
    rc, out, err = run_kubectl(
        ["delete", f"job/{job_name}", "--ignore-not-found=true"]
    )
    return rc == 0


def kubectl_delete_pod(pod_name):
    """Delete a Pod."""
    rc, out, err = run_kubectl(
        ["delete", f"pod/{pod_name}", "--ignore-not-found=true"]
    )
    return rc == 0


def kubectl_resource_exists(resource_type, name):
    """Check if a resource exists."""
    rc, _, _ = run_kubectl(["get", f"{resource_type}/{name}"], capture=True)
    return rc == 0
