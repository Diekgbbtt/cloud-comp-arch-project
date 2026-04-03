#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CONFIG_FILE="${PART1_ENV_FILE:-${SCRIPT_DIR}/part1.env}"
if [[ -f "${CONFIG_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CONFIG_FILE}"
fi

CLUSTER_NAME="${CLUSTER_NAME:-part1.k8s.local}"
PART1_CONFIG_FILE="${PART1_CONFIG_FILE:-part1.yaml}"
KOPS_STATE_STORE="${KOPS_STATE_STORE:-gs://cca-eth-2026-group-059-dgobbetti}"
KOPS_ADMIN_TTL="${KOPS_ADMIN_TTL:-87600h}"
KOPS_WAIT_DURATION="${KOPS_WAIT_DURATION:-10m}"
BOOTSTRAP_SCRIPT="${BOOTSTRAP_SCRIPT:-automation/bootstrap/bootstrap_part1_services.sh}"
MEMCACHED_MANIFEST="${MEMCACHED_MANIFEST:-memcache-t1-cpuset.yaml}"
MEMCACHED_SERVICE_NAME="${MEMCACHED_SERVICE_NAME:-some-memcached-11211}"

DRY_RUN="false"
SKIP_PREREQ_CHECK="false"
SKIP_BOOTSTRAP="false"
SKIP_MEMCACHED_DEPLOY="false"
for arg in "$@"; do
  case "${arg}" in
    --dry-run)
      DRY_RUN="true"
      ;;
    --skip-prereq-check)
      SKIP_PREREQ_CHECK="true"
      ;;
    --skip-bootstrap)
      SKIP_BOOTSTRAP="true"
      ;;
    --skip-memcached-deploy)
      SKIP_MEMCACHED_DEPLOY="true"
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done

log() {
  printf '[part1-create] %s\n' "$*"
}

run_cmd() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    log "DRY RUN: $*"
  else
    eval "$@"
  fi
}

require_cmd() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 || {
    echo "Missing required command: ${cmd}" >&2
    exit 1
  }
}

if [[ "${SKIP_PREREQ_CHECK}" != "true" ]]; then
  require_cmd kops
  require_cmd kubectl
  require_cmd gcloud
fi

export KOPS_STATE_STORE

main() {
  log "Using KOPS_STATE_STORE=${KOPS_STATE_STORE}"

  local bootstrap_args=()
  if [[ "${DRY_RUN}" == "true" ]]; then
    bootstrap_args+=("--dry-run")
  fi
  if [[ "${SKIP_PREREQ_CHECK}" == "true" ]]; then
    bootstrap_args+=("--skip-prereq-check")
  fi

  run_cmd "kops create -f '${REPO_ROOT}/${PART1_CONFIG_FILE}'"
  run_cmd "kops update cluster '${CLUSTER_NAME}' --yes --admin='${KOPS_ADMIN_TTL}'"
  run_cmd "kops validate cluster '${CLUSTER_NAME}' --wait '${KOPS_WAIT_DURATION}'"

  if [[ "${SKIP_MEMCACHED_DEPLOY}" != "true" ]]; then
    run_cmd "kubectl apply -f '${REPO_ROOT}/${MEMCACHED_MANIFEST}'"
    run_cmd "kubectl expose pod some-memcached --name '${MEMCACHED_SERVICE_NAME}' --type LoadBalancer --port 11211 --protocol TCP || true"
  fi

  if [[ "${SKIP_BOOTSTRAP}" != "true" ]]; then
    run_cmd "'${REPO_ROOT}/${BOOTSTRAP_SCRIPT}' ${bootstrap_args[*]}"
  fi

  log "Part 1 cluster creation workflow completed"
}

main "$@"
