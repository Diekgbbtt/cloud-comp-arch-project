from __future__ import annotations

import time
import sys
import threading
import subprocess
import re
import docker
import psutil
from dataclasses import dataclass
from collections import deque
from queue import Queue
from scheduler_logger import SchedulerLogger

JOBS = {
    "barnes": {
        "image": "anakli/cca:splash2x_barnes",
        "run": "./run -a run -S splash2x -p barnes -i native -n ",
        "threads": 1,
    },
    "blackscholes": {
        "image": "anakli/cca:parsec_blackscholes",
        "run": "./run -a run -S parsec -p blackscholes -i native -n ",
        "threads": 1,
    },
    "canneal": {
        "image": "anakli/cca:parsec_canneal",
        "run": "./run -a run -S parsec -p canneal -i native -n ",
        "threads": 1,
    },
    "freqmine": {
        "image": "anakli/cca:parsec_freqmine",
        "run": "./run -a run -S parsec -p freqmine -i native -n ",
        "threads": 3,
    },
    "radix": {
        "image": "anakli/cca:splash2x_radix",
        "run": "./run -a run -S splash2x -p radix -i native -n ",
        "threads": 1,
    },
    "streamcluster": {
        "image": "anakli/cca:parsec_streamcluster",
        "run": "./run -a run -S parsec -p streamcluster -i native -n ",
        "threads": 1,
    },
    "vips": {
        "image": "anakli/cca:parsec_vips",
        "run": "./run -a run -S parsec -p vips -i native -n ",
        "threads": 1,
    },
}

SCHEDULE = ["streamcluster", "canneal", "blackscholes", "vips", "barnes", "radix", "freqmine"]


@dataclass
class JobState:
    name: str
    container: docker.models.containers.Container
    primary_core: int
    cpuset: set[int]
    threads: int
    paused: bool = False


# Global state
current_memcached_cores = 3
active_jobs: dict[str, JobState] = {}
active_jobs_lock = threading.Lock()
completed: set[str] = set()
failed: set[str] = set()
event_queue = Queue()
job_queue = deque(SCHEDULE)
logger = SchedulerLogger()
client = docker.from_env()


def main() -> int:
    start_time = time.time()
    logger.custom_event("scheduler_start", str(int(start_time * 1000)))
    memcached_pid = start_memcached()

    # Start the monitor thread
    resource_thread = threading.Thread(target=manage_resources, args=(memcached_pid,), daemon=True)
    resource_thread.start()

    run_jobs()
    end_time = time.time()
    logger.custom_event("scheduler_end", str(int(end_time * 1000)))
    logger.logger.info(f"Total scheduler runtime: {end_time - start_time:.2f} seconds")
    logger.end()


def manage_resources(memcached_pid: int) -> None:
    global current_memcached_cores

    memcached_process = psutil.Process(memcached_pid)
    memcached_process.cpu_percent(interval=None)

    while len(completed | failed) < len(JOBS):
        time.sleep(1)
        util = memcached_process.cpu_percent(interval=None)
        new_cores = current_memcached_cores
        pause_cores: list[int] = []
        resume_cores: list[int] = []

        # 1. Evaluate State Machine
        if current_memcached_cores == 1:
            if util > 85:
                new_cores = 2
                pause_cores = [1]
        elif current_memcached_cores == 2:
            if util > 170:
                new_cores = 3
                pause_cores = [2]
            elif util < 125:
                new_cores = 1
                resume_cores = [1]
        elif current_memcached_cores == 3:
            if util < 150:
                new_cores = 1
                resume_cores = [1, 2]
            elif util < 200:
                new_cores = 2
                resume_cores = [2]

        # 2. Apply Changes if State Switched
        if new_cores != current_memcached_cores:
            # Determine mapping
            if new_cores == 1:
                mem_affinity = [0]
            elif new_cores == 2:
                mem_affinity = [0, 1]
            elif new_cores == 3:
                mem_affinity = [0, 1, 2]

            # Update Memcached Affinity natively
            subprocess.run(
                [
                    "sudo",
                    "taskset",
                    "-a",
                    "-cp",
                    ",".join([str(i) for i in mem_affinity]),
                    str(memcached_pid),
                ],
                check=True,
                capture_output=True,
            )
            logger.update_cores("memcached", mem_affinity)

            # Update active Docker containers
            with active_jobs_lock:
                # Pause/remove cores if needed
                for core in pause_cores:
                    for job in active_jobs.values():
                        if core in job.cpuset:
                            if job.cpuset == {core}:
                                # Pause core
                                try:
                                    job.container.pause()
                                    job.paused = True
                                    logger.job_pause(job.name)
                                except Exception as e:
                                    logger.logger.error(
                                        f"failed to pause container {job.name}: {e}"
                                    )
                            else:
                                # Remove core from job
                                job.cpuset.remove(core)
                                try:
                                    job.container.update(cpuset_cpus=cpuset_str(job.cpuset))
                                    logger.update_cores(job.name, job.cpuset)
                                except Exception as e:
                                    logger.logger.error(
                                        f"failed to update container {job.name}: {e}"
                                    )
                # Unpause/add cores if needed
                for core in resume_cores:
                    found_paused_job = False
                    for job in active_jobs.values():
                        if job.cpuset == {core} and job.paused:
                            # Unpause core
                            try:
                                found_paused_job = True
                                job.container.unpause()
                                job.paused = False
                                logger.job_unpause(job.name)
                            except Exception as e:
                                logger.logger.error(f"failed to unpause container {job.name}: {e}")
                    if not found_paused_job:
                        event_queue.put(("core_available", core))

                current_memcached_cores = new_cores

            time.sleep(0.5)  # Small delay to allow system to stabilize after changes
            memcached_process.cpu_percent(interval=None) # Reset CPU percent measurement after state change


def run_jobs():
    schedule_available_jobs()

    while len(completed | failed) < len(JOBS):
        event_type, payload = event_queue.get()
        if event_type == "job_finished":
            job_name, exit_code = payload
            with active_jobs_lock:
                active_jobs.pop(job_name)
                if exit_code == 0:
                    completed.add(job_name)
                    logger.job_end(job_name)
                else:
                    failed.add(job_name)
                    logger.logger.error(f"JOB {job_name} FAILED WITH EXIT CODE {exit_code}")
            schedule_available_jobs()
        if event_type == "core_available":
            schedule_available_jobs()


def schedule_available_jobs():
    with active_jobs_lock:
        # Get set of free cores
        free_cores = allowed_cores_for_jobs()
        for active_job in active_jobs.values():
            for core in active_job.cpuset:
                free_cores.discard(core)
        if not free_cores:
            logger.logger.info("no free cores available to schedule new jobs")
            return
        for core in free_cores:
            if not job_queue:
                assign_free_core(core)
                return

            # Schedule next job on this core
            job_name = job_queue.popleft()
            container = client.containers.run(
                image=JOBS[job_name]["image"],
                command=JOBS[job_name]["run"] + str(JOBS[job_name]["threads"]),
                name=job_name,
                remove=True,
                detach=True,
                cpuset_cpus=str(core),
            )
            active_jobs[job_name] = JobState(
                name=job_name, container=container, primary_core=core, cpuset={core}, threads=JOBS[job_name]["threads"]
            )
            logger.job_start(job_name, [core], JOBS[job_name]["threads"])
            wait_thread = threading.Thread(
                target=wait_for_container, args=(job_name, container), daemon=True
            )
            wait_thread.start()


def assign_free_core(core):
    if not active_jobs:
        logger.logger.info(f"core {core} is idle and available for scheduling")
        return  # All jobs completed
    # Get job with smallest (busiest) core
    active_jobs_list = [job for job in active_jobs.values() if job.threads > 1]
    target = min(active_jobs_list, key=lambda job: job.primary_core)
    if target.paused:
        # If target is paused, assign core as primary and unpause
        try:
            target.primary_core = core
            target.cpuset = {core}
            target.container.update(cpuset_cpus=str(core))
            logger.update_cores(target.name, {core})
            target.container.unpause()
            target.paused = False
            logger.job_unpause(target.name)
        except Exception as e:
            logger.logger.error(f"failed to unpause container {target.name}: {e}")
    else:
        # If target is running, just add core to its cpuset
        try:
            target.cpuset.add(core)
            target.container.update(cpuset_cpus=cpuset_str(target.cpuset))
            logger.update_cores(target.name, target.cpuset)
        except Exception as e:
            logger.logger.error(f"failed to update container {target.name}: {e}")


def wait_for_container(job_name: str, container) -> None:
    result = container.wait()
    exit_code = result.get("StatusCode", -1)
    event_queue.put(("job_finished", (job_name, exit_code)))


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
    subprocess.run(
        ["sudo", "taskset", "-a", "-cp", ",".join([str(i) for i in [0, 1, 2]]), str(memcached_pid)],
        check=True,
        capture_output=True,
    )
    logger.job_start("memcached", [0, 1, 2], 3)
    return memcached_pid


def allowed_cores_for_jobs():
    if current_memcached_cores == 1:
        return {1, 2, 3}
    elif current_memcached_cores == 2:
        return {2, 3}
    else:
        return {3}


def cpuset_str(cores: set[int]) -> str:
    return ",".join(str(core) for core in sorted(cores))


if __name__ == "__main__":
    sys.exit(main())
