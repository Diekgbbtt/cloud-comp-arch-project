"""Schedule Part 3 PARSEC Kubernetes jobs with dependency-aware policies.

This script schedules a fixed set of Kubernetes Job manifests from a folder,
supports parallel and sequential execution via an editable dependency map,
and logs when each job is scheduled and when it finishes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from kubernetes import client, config, watch
from kubernetes.client import ApiException
from kubernetes.utils import FailToCreateError, create_from_yaml
from yaml import SafeLoader, load_all


REPO_ROOT = Path(__file__).resolve().parents[1]

JOB_NAMES = [
	"parsec-streamcluster",
	"parsec-freqmine",
	"parsec-canneal",
	"parsec-blackscholes",
	"parsec-radix",
	"parsec-barnes",
	"parsec-vips",
]


# EVOLVE-BLOCK-START
# ======== // SECTION A - KUBERNETES JOB CONFIGURATION \ =======
def edit_job_configuration(yaml_objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	"""Configure Kubernetes Job objects based on job type and constraints.

	Mutates the passed YAML objects in-place (and also returns them) by setting:
	- template_spec["nodeSelector"]["cca-project-nodetype"]: "node-b-4core"/"node-a-8core"
	- the cluster working node a job is allocated on: either node-b-4core(n2d-highcpu-4) or node-a-8core(e2-standard-8)
	- containers[0]["resources"]["requests"]["cpu"]/["memory"] and ["resources"]["limits"]["cpu"]/["memory"]
	- the job minimum available vCPUs and memory space to be allocated and the maximum they can get
	- containers[0]["args"]: list of launch cmd chars, template: ["-c", "taskset -c <vCPUs_range> ./run -a run -S parsec -p blackscholes -i native -n <software_threads_count>"]
	- CPU pinning through process affinity with tasksets("-c", "taskset -c <vCPUs_range>), in which vCPUs_range is a 0-indexed list with syntax: <lower-boundary>-<upper-boundary>, e.g. 2-3 or 0-4 or just collpases to the vCPU number if bound to jsut one core, e.g. 1
	- Software threads count: -n <software_threads_count>, e.g. -n 3 will spawn 3 software threads
	HARD CONSTRAINT: On node-a-8core, batch job taskset ranges must never include core 0. 
	Valid ranges: 1-7, or any subset thereof (e.g. 1-4, 2-7, 5-7).

	### ANY OTHER PROPERTY SHOULD NOT BE TOUCHED
	"""
	job_name = yaml_objects[0].setdefault("metadata", {}).setdefault("name", "")

	template_spec = (
		yaml_objects[0].setdefault("spec", {})
		.setdefault("template", {})
		.setdefault("spec", {})
	)

	containers: List[Dict[str, Any]] = template_spec.setdefault("containers", [])
	if not containers:
		containers.append({})
	container = containers[0]

	# Ensure minimum structure exists - edited below
	template_spec.setdefault("nodeSelector", {})
	container.setdefault("args", [])
	resources = container.setdefault("resources", {})
	requests = resources.setdefault("requests", {})
	limits = resources.setdefault("limits", {})

	# Job-specific configuration taken from the per-job manifests in
	# automation/results/part3/diego_tentative2/.
	job_configurations = {
		"parsec-blackscholes": {
			"nodeSelector": {"cca-project-nodetype": "node-a-8core"},
			"requests": {"cpu": "4"},
			"limits": {"cpu": "5"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 1-4 ./run -a run -S parsec -p blackscholes -i native -n 4",
			],
		},
		"parsec-radix": {
			"nodeSelector": {"cca-project-nodetype": "node-a-8core"},
			"requests": {"cpu": "1"},
			"limits": {"cpu": "2"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 1 ./run -a run -S splash2x -p radix -i native -n 1",
			],
		},
		"parsec-canneal": {
			"nodeSelector": {"cca-project-nodetype": "node-b-4core"},
			"requests": {"cpu": "2"},
			"limits": {"cpu": "3"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 0-2 ./run -a run -S parsec -p canneal -i native -n 3",
			],
		},
		"parsec-streamcluster": {
			"nodeSelector": {"cca-project-nodetype": "node-a-8core"},
			"requests": {"cpu": "4"},
			"limits": {"cpu": "5"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 5-7 ./run -a run -S parsec -p streamcluster -i native -n 3",
			],
		},
		"parsec-freqmine": {
			"nodeSelector": {"cca-project-nodetype": "node-b-4core"},
			"requests": {"cpu": "3"},
			"limits": {"cpu": "4"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 0-3 ./run -a run -S parsec -p freqmine -i native -n 4",
			],
		},
		"parsec-barnes": {
			"nodeSelector": {"cca-project-nodetype": "node-b-4core"},
			"requests": {"cpu": "2"},
			"limits": {"cpu": "3"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 0-2 ./run -a run -S splash2x -p barnes -i native -n 3",
			],
		},
		"parsec-vips": {
			"nodeSelector": {"cca-project-nodetype": "node-b-4core"},
			"requests": {"cpu": "2"},
			"limits": {"cpu": "3"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 0-2 ./run -a run -S parsec -p vips -i native -n 3",
			],
		},
	}

	job_config = job_configurations.get(job_name)
	if job_config is not None:
		template_spec["nodeSelector"] = job_config["nodeSelector"]
		requests.update(job_config["requests"])
		limits.update(job_config["limits"])
		container["command"] = job_config["command"]
		container["args"] = job_config["args"]
	return yaml_objects



# ======== // SECTION B - CONCURRENY STRUCTURE \ =========
JOB_DEPENDENCIES: dict[str, list[str]] = {
	"parsec-streamcluster": [],
	"parsec-canneal": [],
	"parsec-freqmine": [],
	"parsec-blackscholes": ["parsec-streamcluster", "parsec-canneal"],
	"parsec-barnes": ["parsec-canneal"],
	"parsec-vips": ["parsec-barnes"],
	"parsec-radix": ["parsec-blackscholes", "parsec-freqmine", "parsec-streamcluster"],
}
# EVOLVE-BLOCK-END


def _write_effective_job_yaml_objects(
	*,
	output_dir: Path | None,
	job_name: str,
	yaml_objects: list[dict[str, Any]],
) -> None:
	"""Persist the *post-edit* manifest documents for later inspection (best-effort)."""
	if output_dir is None:
		return
	try:
		effective_dir = output_dir / "effective_job_yaml"
		effective_dir.mkdir(parents=True, exist_ok=True)
		path = effective_dir / f"{job_name}.json"
		path.write_text(json.dumps(yaml_objects, indent=2, sort_keys=True))
	except Exception:
		# Never fail scheduling just because artifact writing failed.
		return


def _resolve_node_assignment() -> dict[str, str]:
	"""Return a mapping job_name -> nodetype by dry-running edit_job_configuration.

	Each job's YAML is constructed with only the metadata.name field populated —
	the minimum needed for edit_job_configuration to hit the correct branch.
	The nodeSelector written by that branch is then read back.
	"""
	assignment: dict[str, str] = {}
	for job_name in JOB_NAMES:
		# Minimal stub — only metadata.name is needed for the if/elif dispatch.
		stub: list[dict[str, Any]] = [{"metadata": {"name": job_name}, "spec": {}}]
		result = edit_job_configuration(stub)
		node = (
			result[0]
			.get("spec", {})
			.get("template", {})
			.get("spec", {})
			.get("nodeSelector", {})
			.get("cca-project-nodetype", "unknown")
		)
		assignment[job_name] = node
	return assignment


def _write_policy_graph(*, output_dir: Path | None) -> None:
	"""Write policy_graph.json to output_dir (best-effort).

	Schema
	------
	{
	  "dependencies": { job_name: [prereq, ...], ... },
	  "node_assignment": { job_name: "node-a-8core" | "node-b-4core", ... },
	  "node_a_jobs": [job_name, ...],
	  "node_b_jobs": [job_name, ...]
	}

	The evaluator reads this file to compute structural MAP-Elites features
	without needing to re-parse source code or Kubernetes objects.
	"""
	if output_dir is None:
		return
	try:
		assignment = _resolve_node_assignment()
		node_a = sorted(j for j, n in assignment.items() if n == "node-a-8core")
		node_b = sorted(j for j, n in assignment.items() if n == "node-b-4core")
		payload = {
			"dependencies": {j: list(deps) for j, deps in JOB_DEPENDENCIES.items()},
			"node_assignment": assignment,
			"node_a_jobs": node_a,
			"node_b_jobs": node_b,
		}
		path = output_dir / "policy_graph.json"
		path.write_text(json.dumps(payload, indent=2, sort_keys=True))
	except Exception:
		# Never fail scheduling because graph writing failed.
		return


def main() -> int:
	args = parse_args()
	configure_logging()
	load_kube_config(args.kube_context)

	start = time.time()
	try:
		return run_scheduler(args)
	except ApiException as exc:
		logging.exception("Kubernetes API error: status=%s reason=%s", exc.status, exc.reason)
		return 1
	except Exception:
		logging.exception("Unexpected scheduler error")
		return 1
	finally:
		elapsed = time.time() - start


def run_scheduler(args: argparse.Namespace) -> int:
	# validate_policy(JOB_NAMES, JOB_DEPENDENCIES)
	manifest_paths = build_manifest_paths(args.jobs_dir)

	# Write the resolved policy graph before any jobs are submitted so the
	# evaluator can compute structural features even if the run fails early.
	_write_policy_graph(output_dir=args.output_dir)

	api_client = client.ApiClient()
	batch_api = client.BatchV1Api()

	# Establish baseline resource version for watch stream.
	initial = batch_api.list_namespaced_job(namespace=args.namespace)
	resource_version = initial.metadata.resource_version
	if not resource_version:
		raise RuntimeError("Could not obtain initial resourceVersion from Kubernetes API")

	unscheduled = set(JOB_NAMES)
	completed: set[str] = set()
	failed: set[str] = set()

	while len(completed | failed) < len(JOB_NAMES):
		schedule_ready_jobs(
			api_client=api_client,
			batch_api=batch_api,
			namespace=args.namespace,
			manifest_paths=manifest_paths,
			output_dir=args.output_dir,
			dependencies=JOB_DEPENDENCIES,
			unscheduled=unscheduled,
			completed=completed,
			failed=failed,
		)

		if failed and not args.continue_on_failure:
			logging.error("Stopping on first failure: %s", sorted(failed))
			return 1

		running = (set(JOB_NAMES) - unscheduled) - (completed | failed)
		if not running and unscheduled:
			blocked = sorted(unscheduled)
			logging.error(
				"No runnable jobs left. Blocked jobs=%s (likely unmet deps due to failures)",
				blocked,
			)
			return 2

		if len(completed | failed) == len(JOB_NAMES):
			break

		resource_version = watch_for_job_updates(
			batch_api=batch_api,
			namespace=args.namespace,
			tracked_jobs=set(JOB_NAMES) - unscheduled,
			completed=completed,
			failed=failed,
			resource_version=resource_version,
			watch_timeout_seconds=args.watch_timeout_seconds,
		)

	if failed:
		logging.error(
			"Scheduling completed with failures. succeeded=%s failed=%s",
			sorted(completed),
			sorted(failed),
		)
		return 1


	return 0


def schedule_ready_jobs(
	api_client: client.ApiClient,
	batch_api: client.BatchV1Api,
	namespace: str,
	manifest_paths: dict[str, Path],
	output_dir: Path | None,
	dependencies: dict[str, list[str]],
	unscheduled: set[str],
	completed: set[str],
	failed: set[str],
) -> list[str]:
	ready = [
		job for job in JOB_NAMES if job in unscheduled and set(dependencies[job]).issubset(completed)
	]

	for job_name in ready:
		create_or_track_existing_job(
			api_client=api_client,
			batch_api=batch_api,
			namespace=namespace,
			job_name=job_name,
			manifest_path=manifest_paths[job_name],
			output_dir=output_dir,
		)
		unscheduled.remove(job_name)

	return ready


def create_yaml_object(manifest_path: str) -> list[dict]:
	try:
		with open(os.path.abspath(manifest_path)) as f:
			yml_document_all = load_all(f, Loader=SafeLoader)
			return list(yml_document_all)
	except Exception as e:
		raise FileNotFoundError("non existing configuration file")

def create_or_track_existing_job(
	api_client: client.ApiClient,
	batch_api: client.BatchV1Api,
	namespace: str,
	job_name: str,
	manifest_path: Path,
	output_dir: Path | None,
) -> None:
	try:
		yaml_objects = create_yaml_object(str(manifest_path))
		yaml_objects_new = edit_job_configuration(yaml_objects)
		_write_effective_job_yaml_objects(
			output_dir=output_dir,
			job_name=job_name,
			yaml_objects=yaml_objects_new,
		)
		
		create_from_yaml(
			api_client,
			yaml_objects=yaml_objects_new,
			namespace=namespace,
			verbose=False,
		)
	except ApiException as exc:
		if exc.status == 409:
			logging.warning(
				"Job %s/%s already exists; tracking existing object", namespace, job_name
			)
			return
		raise
	except FailToCreateError as exc:
		# create_from_yaml may bundle API exceptions for one or more documents.
		non_conflicts = [e for e in exc.api_exceptions if getattr(e, "status", None) != 409]
		if not non_conflicts:
			logging.warning(
				"Job %s/%s already exists; tracking existing object", namespace, job_name
			)
			return
		raise non_conflicts[0]


def watch_for_job_updates(
	batch_api: client.BatchV1Api,
	namespace: str,
	tracked_jobs: set[str],
	completed: set[str],
	failed: set[str],
	resource_version: str,
	watch_timeout_seconds: int,
) -> str:
	watcher = watch.Watch()
	try:
		for event in watcher.stream(
			batch_api.list_namespaced_job,
			namespace=namespace,
			resource_version=resource_version,
			timeout_seconds=watch_timeout_seconds,
		):
			obj: client.V1Job = event["object"]
			if obj.metadata and obj.metadata.resource_version:
				resource_version = obj.metadata.resource_version

			job_name = obj.metadata.name if obj.metadata else None
			if not job_name or job_name not in tracked_jobs:
				continue

			if job_name in completed or job_name in failed:
				continue

			state = parse_job_status(obj.status)
			if state == "succeeded":
				completed.add(job_name)

				break
			elif state == "failed":
				failed.add(job_name)

				break
	finally:
		watcher.stop()

	return resource_version


def parse_job_status(status: client.V1JobStatus | None) -> str | None:
	"""
	Determine if a Job status indicates a terminal state (succeeded or failed).
	"""
	if status is None:
		return None

	for condition in status.conditions or []:
		if condition.type == "Complete" and condition.status == "True":
			return "succeeded"
		if condition.type == "Failed" and condition.status == "True":
			return "failed"

	if status.succeeded and status.succeeded > 0:
		return "succeeded"
	if status.failed and status.failed > 0 and status.active in (None, 0):
		return "failed"
	return None


def load_kube_config(kube_context: str | None) -> None:
	try:
		config.load_kube_config(context=kube_context)

	except Exception:
		config.load_incluster_config()

def build_manifest_paths(jobs_dir: Path) -> dict[str, Path]:
	manifest_paths: dict[str, Path] = {}
	for job_name in JOB_NAMES:
		manifest_path = jobs_dir / f"part3-{job_name}.yaml"
		if not manifest_path.exists():
			raise FileNotFoundError(f"Missing manifest file: {manifest_path}")
		manifest_paths[job_name] = manifest_path
	return manifest_paths


def configure_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s %(levelname)s %(message)s",
	)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Schedule Part 3 PARSEC jobs in Kubernetes with dependencies.",
	)
	parser.add_argument(
		"--jobs-dir",
		type=Path,
		default=REPO_ROOT / "automation" / "results" / "part3" / "diego_tentative2",
		help="Directory containing parsec-*.yaml job manifests.",
	)
	parser.add_argument(
		"--output-dir",
		type=Path,
		default=REPO_ROOT / "automation" / "results" / "part3" / "diego_tentative2" / "muted_conf_persisted",
		help="Optional directory for writing effective (post-edit) job manifests.",
	)
	parser.add_argument(
		"--namespace",
		default="default",
		help="Kubernetes namespace for the jobs.",
	)
	parser.add_argument(
		"--kube-context",
		default=None,
		help="Optional kubeconfig context name.",
	)
	parser.add_argument(
		"--continue-on-failure",
		action="store_true",
		help="Do not stop scheduling when a job fails.",
	)
	parser.add_argument(
		"--watch-timeout-seconds",
		type=int,
		default=300,
		help="Timeout for each Kubernetes watch cycle.",
	)
	return parser.parse_args()

if __name__ == "__main__":
	sys.exit(main())
