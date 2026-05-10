from __future__ import annotations

import os
import time
import sys
import threading
import subprocess
import re
import docker
import psutil
from scheduler_logger import SchedulerLogger

JOBS = {
    "barnes": {
        "image": "anakli/cca:splash2x_barnes",
        "run": "./run -a run -S splash2x -p barnes -i native -n 3",
    },
    "blackscholes": {
        "image": "anakli/cca:parsec_blackscholes",
        "run": "./run -a run -S parsec -p blackscholes -i native -n 3",
    },
    "canneal": {
        "image": "anakli/cca:parsec_canneal",
        "run": "./run -a run -S parsec -p canneal -i native -n 3",
    },
    "freqmine": {
        "image": "anakli/cca:parsec_freqmine",
        "run": "./run -a run -S parsec -p freqmine -i native -n 3",
    },
    "radix": {
        "image": "anakli/cca:splash2x_radix",
        "run": "./run -a run -S splash2x -p radix -i native -n 2",
    },
    "streamcluster": {
        "image": "anakli/cca:parsec_streamcluster",
        "run": "./run -a run -S parsec -p streamcluster -i native -n 3",
    },
    "vips": {
        "image": "anakli/cca:parsec_vips",
        "run": "./run -a run -S parsec -p vips -i native -n 3",
    },
}

SCHEDULE = ["barnes", "blackscholes", "canneal", "freqmine", "radix", "streamcluster", "vips"]

# Global state
current_memcached_cores = 3
active_containers = []
active_containers_lock = threading.Lock()

unscheduled = set(JOBS.keys())
completed: set[str] = set()
failed: set[str] = set()
logger = SchedulerLogger()


def main() -> int:
    memcached_pid = start_memcached()

    # Start the monitor thread
    resource_thread = threading.Thread(target=manage_resources, args=(memcached_pid,), daemon=True)
    resource_thread.start()

    run_jobs()

    logger.end()


def manage_resources(memcached_pid: int) -> None:
    global current_memcached_cores

    memcached_process = psutil.Process(memcached_pid)
    memcached_process.cpu_percent(interval=None)

    while len(completed | failed) < len(JOBS):
        time.sleep(0.5)
        util = memcached_process.cpu_percent(interval=None)
        new_cores = current_memcached_cores

        # 1. Evaluate State Machine
        if current_memcached_cores == 1:
            if util > 90:
                new_cores = 2
                pause_cores = [1]
        elif current_memcached_cores == 2:
            if util > 175:
                new_cores = 3
                pause_cores = [2]
            elif util < 140:
                new_cores = 1
                resume_cores = [1]
        elif current_memcached_cores == 3:
            if util < 170:
                new_cores = 1
                resume_cores = [1, 2]
            elif util < 215:
                new_cores = 2
                resume_cores = [2]

        # 2. Apply Changes if State Switched
        if new_cores != current_memcached_cores:
            # Determine mapping
            if new_cores == 1:
                mem_affinity = [0]
                job_cpuset = "1-3"
                job_affinity = [1, 2, 3]

            elif new_cores == 2:
                mem_affinity = [0, 1]
                job_cpuset = "2-3"
                job_affinity = [2, 3]
            elif new_cores == 3:
                mem_affinity = [0, 1, 2]
                job_cpuset = "3"
                job_affinity = [3]

            # Update Memcached Affinity natively
            subprocess.run(["sudo", "taskset", "-a", "-cp", ",".join([str(i) for i in mem_affinity]), str(memcached_pid)], capture_output=True, check=True)
            logger.update_cores("memcached", mem_affinity)

            # Update active Docker containers
            with active_containers_lock:
                for container in active_containers:
                    try:
                        container.update(cpuset_cpus=job_cpuset)
                        logger.update_cores(container.name, job_affinity)
                    except Exception as e:
                        logger.logger.error(f"Failed to update container {container.name} affinity: {e}")
                        pass  # Handle edge case where container dies exactly during update

            current_memcached_cores = new_cores


def run_jobs():
    client = docker.from_env()

    for job_name in SCHEDULE:
        with active_containers_lock:
            # Figure out what cpuset to start with based on current state
            job_cpuset = "1-3"
            job_affinity = [1, 2, 3]
            if current_memcached_cores == 2:
                job_cpuset = "2-3"
                job_affinity = [2, 3]
            elif current_memcached_cores == 3:
                job_cpuset = "3"
                job_affinity = [3]

            container = client.containers.run(
                image=JOBS[job_name]["image"],
                command=JOBS[job_name]["run"],
                name=job_name,
                remove=True,
                detach=True,
                cpuset_cpus=job_cpuset,
            )
            logger.job_start(job_name, job_affinity, 3)
            active_containers.append(container)
            unscheduled.remove(job_name)

        # Wait for container to finish
        result = container.wait()
        exit_code = result.get("StatusCode", -1)

        with active_containers_lock:
            active_containers.remove(container)

        if exit_code == 0:
            completed.add(job_name)
            logger.job_end(job_name)
        else:
            failed.add(job_name)


def start_memcached() -> int:
    # Restart memcached service
    # subprocess.run(["sudo", "systemctl", "restart", "memcached"], check=True)
    # Get status output
    result = subprocess.run(
        ["sudo", "systemctl", "status", "memcached"], capture_output=True, text=True, check=True
    )
    status_output = result.stdout
    match = re.search(r"Main PID: (\d+)", status_output)
    if not match:
        raise RuntimeError("Could not find memcached PID in status output")
    memcached_pid = int(match.group(1))
    subprocess.run(["sudo", "taskset", "-a", "-cp", ",".join([str(i) for i in [0, 1, 2]]), str(memcached_pid)], capture_output=True, check=True)
    logger.job_start("memcached", [0, 1, 2], 3)
    return memcached_pid


if __name__ == "__main__":
    sys.exit(main())
