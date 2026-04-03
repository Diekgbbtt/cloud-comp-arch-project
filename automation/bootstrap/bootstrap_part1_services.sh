#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CONFIG_FILE="${PART1_ENV_FILE:-${REPO_ROOT}/automation/infrastructure/part1.env}"
if [[ -f "${CONFIG_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CONFIG_FILE}"
fi

CLUSTER_NAME="${CLUSTER_NAME:-part1.k8s.local}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/id_ed25519}"
GCE_ZONE="${GCE_ZONE:-europe-west1-b}"
MEMCACHED_SERVICE_NAME="${MEMCACHED_SERVICE_NAME:-some-memcached-11211}"
START_MCPERF_AGENT="${START_MCPERF_AGENT:-true}"
MCPERF_AGENT_THREADS="${MCPERF_AGENT_THREADS:-16}"
BOOTSTRAP_INSTALL_MCPERF_ON_MEASURE="${BOOTSTRAP_INSTALL_MCPERF_ON_MEASURE:-true}"

DRY_RUN="false"
SKIP_PREREQ_CHECK="false"
for arg in "$@"; do
  case "${arg}" in
    --dry-run)
      DRY_RUN="true"
      ;;
    --skip-prereq-check)
      SKIP_PREREQ_CHECK="true"
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done

log() {
  printf '[part1-bootstrap] %s\n' "$*"
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
  require_cmd kubectl
  require_cmd gcloud
fi

get_node_name_by_type() {
  local node_type="$1"
  kubectl get nodes -l "cca-project-nodetype=${node_type}" -o jsonpath='{.items[0].metadata.name}'
}

ssh_exec() {
  local host="$1"
  local remote_cmd="$2"

  run_cmd "gcloud compute ssh --ssh-key-file '${SSH_KEY_FILE}' '${SSH_USER}@${host}' --zone '${GCE_ZONE}' --command \"${remote_cmd}\""
}

install_mcperf_deps() {
  local host="$1"
  ssh_exec "${host}" "set -euo pipefail; \
sudo apt-get update; \
sudo apt-get install -y libevent-dev libzmq3-dev git make g++; \
sudo sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources || true; \
sudo apt-get update; \
sudo apt-get build-dep -y memcached; \
if [[ ! -d \$HOME/memcache-perf ]]; then git clone https://github.com/shaygalon/memcache-perf.git \$HOME/memcache-perf; fi; \
cd \$HOME/memcache-perf; \
git fetch --all; \
git checkout 0afbe9b; \
make"
}

start_mcperf_agent() {
  local host="$1"
  ssh_exec "${host}" "set -euo pipefail; \
cd \$HOME/memcache-perf; \
nohup ./mcperf -T ${MCPERF_AGENT_THREADS} -A > mcperf-agent.log 2>&1 < /dev/null &"
}

wait_for_memcached_service_ip() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    log "DRY RUN: skip waiting for ${MEMCACHED_SERVICE_NAME} external IP"
    return 0
  fi

  log "Waiting for ${MEMCACHED_SERVICE_NAME} external IP..."
  for _ in $(seq 1 60); do
    local ip
    ip="$(kubectl get svc "${MEMCACHED_SERVICE_NAME}" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
    if [[ -n "${ip}" ]]; then
      log "Memcached service external IP: ${ip}"
      return 0
    fi
    sleep 5
  done

  echo "Timed out waiting for memcached service IP" >&2
  exit 1
}

main() {
  log "Bootstrapping Part 1 services for ${CLUSTER_NAME}"

  local client_agent_node=""
  local client_measure_node=""

  if [[ "${DRY_RUN}" == "true" ]]; then
    client_agent_node="client-agent"
    client_measure_node="client-measure"
  else
    client_agent_node="$(get_node_name_by_type client-agent)"
    client_measure_node="$(get_node_name_by_type client-measure)"
  fi

  if [[ -z "${client_agent_node}" ]]; then
    echo "Could not find client-agent node. Check node labels and cluster status." >&2
    exit 1
  fi

  log "Detected client-agent node: ${client_agent_node}"
  if [[ -n "${client_measure_node}" ]]; then
    log "Detected client-measure node: ${client_measure_node}"
  fi

  install_mcperf_deps "${client_agent_node}"

  if [[ "${BOOTSTRAP_INSTALL_MCPERF_ON_MEASURE}" == "true" && -n "${client_measure_node}" ]]; then
    install_mcperf_deps "${client_measure_node}"
  fi

  if [[ "${START_MCPERF_AGENT}" == "true" ]]; then
    start_mcperf_agent "${client_agent_node}"
  fi

  wait_for_memcached_service_ip
  log "Part 1 bootstrap completed"
}

main "$@"
