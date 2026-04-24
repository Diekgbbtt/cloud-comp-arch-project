#!/usr/bin/env python3
"""Schedule Part 3 PARSEC Kubernetes jobs with dependency-aware policies.

This script schedules a fixed set of Kubernetes Job manifests from a folder,
supports parallel and sequential execution via an editable dependency map,
and logs when each job is scheduled and when it finishes.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from kubernetes import client, config, watch
from kubernetes.client import ApiException
from kubernetes.utils import FailToCreateError, create_from_yaml


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

# Scheduling policy (easy to edit): map job -> prerequisite jobs.
# A job is eligible to be scheduled when all prerequisites are complete.
# This policy encodes:
# - streamcluster
# - freqmine -> barnes -> vips
# - canneal
# - blackscholes -> radix
JOB_DEPENDENCIES: dict[str, list[str]] = {
	"parsec-streamcluster": [],
	"parsec-freqmine": [],
	"parsec-canneal": ["parsec-streamcluster"],
	"parsec-blackscholes": ["parsec-streamcluster"],
	"parsec-barnes": ["parsec-freqmine"],
	"parsec-vips": ["parsec-barnes", "parsec-freqmine"],
	"parsec-radix": ["parsec-streamcluster"],
}


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
		logging.info("Scheduler finished in %.2f seconds", elapsed)


def run_scheduler(args: argparse.Namespace) -> int:
	validate_policy(JOB_NAMES, JOB_DEPENDENCIES)
	manifest_paths = build_manifest_paths(args.jobs_dir)

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

	logging.info("All jobs finished successfully: %s", sorted(completed))
	return 0


def schedule_ready_jobs(
	api_client: client.ApiClient,
	batch_api: client.BatchV1Api,
	namespace: str,
	manifest_paths: dict[str, Path],
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
		)
		unscheduled.remove(job_name)
		logging.info("SCHEDULED job=%s deps=%s", job_name, dependencies[job_name])

	return ready


def create_or_track_existing_job(
	api_client: client.ApiClient,
	batch_api: client.BatchV1Api,
	namespace: str,
	job_name: str,
	manifest_path: Path,
) -> None:
	try:
		create_from_yaml(
			api_client,
			yaml_file=str(manifest_path),
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
				logging.info("FINISHED job=%s status=Succeeded", job_name)
				break
			elif state == "failed":
				failed.add(job_name)
				logging.info("FINISHED job=%s status=Failed", job_name)
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


def delete_job_if_present(batch_api: client.BatchV1Api, namespace: str, name: str) -> None:
	try:
		batch_api.delete_namespaced_job(
			name=name,
			namespace=namespace,
			propagation_policy="Foreground",
		)
		logging.info("Deleted existing job %s/%s", namespace, name)
	except ApiException as exc:
		if exc.status != 404:
			raise


def load_kube_config(kube_context: str | None) -> None:
	try:
		config.load_kube_config(context=kube_context)
		logging.info("Loaded kubeconfig context=%s", kube_context or "current")
	except Exception:
		config.load_incluster_config()
		logging.info("Loaded in-cluster Kubernetes config")


def validate_policy(job_names: list[str], dependencies: dict[str, list[str]]) -> None:
	known = set(job_names)
	if set(dependencies.keys()) != known:
		missing = sorted(known - set(dependencies.keys()))
		extra = sorted(set(dependencies.keys()) - known)
		raise ValueError(
			f"Invalid dependency keys. missing={missing}, extra={extra}"
		)

	for job, prereqs in dependencies.items():
		unknown = sorted(set(prereqs) - known)
		if unknown:
			raise ValueError(f"Unknown prerequisites for {job}: {unknown}")

	# Kahn's algorithm to reject cycles in scheduling policy.
	indegree = {job: 0 for job in job_names}
	outgoing: dict[str, list[str]] = {job: [] for job in job_names}
	for job, prereqs in dependencies.items():
		indegree[job] = len(prereqs)
		for prereq in prereqs:
			outgoing[prereq].append(job)

	queue = [job for job in job_names if indegree[job] == 0]
	seen = 0
	while queue:
		current = queue.pop()
		seen += 1
		for nxt in outgoing[current]:
			indegree[nxt] -= 1
			if indegree[nxt] == 0:
				queue.append(nxt)

	if seen != len(job_names):
		raise ValueError("Dependency policy contains a cycle")


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
		default=REPO_ROOT / "automation" / "results" / "part3" / "diego_tentative3",
		help="Directory containing parsec-*.yaml job manifests.",
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
