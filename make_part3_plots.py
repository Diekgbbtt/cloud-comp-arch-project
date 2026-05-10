"""Generate part-3 plots (plot A: memcache p95 + node-a-8core job annotations;
plot B: per-core Gantt across both machines) for specified runs."""
import json
import sys
from datetime import datetime, timezone
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

BASE = sys.argv[1] if len(sys.argv) > 1 else "automation/results/part3/diego_tentative2"
RUNS = [int(x) for x in sys.argv[2:]] if len(sys.argv) > 2 else [1, 2, 4]
FMT = "%Y-%m-%dT%H:%M:%SZ"

COLORS = {
    "barnes":        "#AACCCA",
    "blackscholes":  "#CCA000",
    "canneal":       "#CCCCAA",
    "freqmine":      "#0CCA00",
    "radix":         "#00CCA0",
    "streamcluster": "#CCACCA",
    "vips":          "#CC0A00",
}

# node/cores per job (from yamls)
JOB_NODE = {
    "blackscholes":  ("node-a-8core", list(range(1, 6))),   # taskset -c 1-5
    "canneal":       ("node-b-4core", list(range(0, 3))),   # taskset -c 0-2
    "radix":         ("node-a-8core", [1]),                 # taskset -c 1
    "streamcluster": ("node-a-8core", list(range(1, 6))),   # taskset -c 1-5
    "barnes":        ("node-b-4core", list(range(0, 3))),   # taskset -c 0-2
    "freqmine":      ("node-b-4core", list(range(0, 4))),   # taskset -c 0-3
    "vips":          ("node-b-4core", list(range(0, 3))),   # taskset -c 0-2
}

NODEA_JOBS = ["streamcluster", "radix", "blackscholes"]


def load_run(r):
    with open(f"{BASE}/results_{r}.json") as f:
        j = json.load(f)
    jobs = {}
    for it in j["items"]:
        n = it["status"]["containerStatuses"][0]["name"]
        t = it["status"]["containerStatuses"][0]["state"].get("terminated")
        if not t:
            continue
        s = datetime.strptime(t["startedAt"], FMT).replace(tzinfo=timezone.utc)
        e = datetime.strptime(t["finishedAt"], FMT).replace(tzinfo=timezone.utc)
        short = n.replace("parsec-", "")
        jobs[short] = (int(s.timestamp() * 1000), int(e.timestamp() * 1000))
    t0 = min(s for s, _ in jobs.values())
    lats = []
    with open(f"{BASE}/results_{r}_memcache_latencies.txt") as f:
        for line in f:
            p = line.split()
            if not p or p[0] != "read":
                continue
            try:
                p95 = float(p[12])
                ts_s = int(p[-2])
                ts_e = int(p[-1])
            except Exception:
                continue
            lats.append((ts_s, ts_e, p95))
    return t0, jobs, lats


def plot_a(r, t0, jobs, lats, out):
    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(12, 5.5), sharex=True,
        gridspec_kw={"height_ratios": [1.4, 2.6]},
    )

    # Top: Gantt for the 4 node-a-8core jobs (memcache co-located)
    row_labels = []
    for i, j in enumerate(NODEA_JOBS):
        if j not in jobs:
            continue
        s, e = jobs[j]
        x = (s - t0) / 1000.0
        w = (e - s) / 1000.0
        cores = JOB_NODE[j][1]
        ax_top.broken_barh([(x, w)], (i - 0.4, 0.8),
                           facecolors=COLORS[j], edgecolor="black")
        ax_top.text(x + w / 2, i, f"cores {cores[0]}-{cores[-1]}"
                    if len(cores) > 1 else f"core {cores[0]}",
                    ha="center", va="center", fontsize=8)
        row_labels.append(j)
    ax_top.set_yticks(range(len(NODEA_JOBS)))
    ax_top.set_yticklabels(NODEA_JOBS)
    ax_top.set_ylim(-0.6, len(NODEA_JOBS) - 0.4)
    ax_top.invert_yaxis()
    ax_top.grid(axis="x", linestyle=":", alpha=0.5)

    # Bottom: memcache p95 bars (width = ts_end-ts_start, height = p95)
    for ts_s, ts_e, p95 in lats:
        x = (ts_s - t0) / 1000.0
        w = (ts_e - ts_s) / 1000.0
        ax_bot.bar(x, p95, width=w, align="edge",
                   color="#4477AA", edgecolor="black", linewidth=0.4)
    ax_bot.axhline(1000, color="red", linestyle="--", linewidth=1,
                   label="SLO 1 ms")
    # vertical guides at each job start/end
    for j in NODEA_JOBS:
        if j not in jobs:
            continue
        s, e = jobs[j]
        ax_bot.axvline((s - t0) / 1000.0, color=COLORS[j],
                       linestyle="-", alpha=0.6, linewidth=1)
        ax_bot.axvline((e - t0) / 1000.0, color=COLORS[j],
                       linestyle="--", alpha=0.6, linewidth=1)
    ax_bot.set_xlabel("Time since first container start [s]")
    ax_bot.set_ylabel("memcached p95 latency [µs]")
    ax_bot.legend(loc="upper right", fontsize=8)
    ax_bot.grid(axis="y", linestyle=":", alpha=0.5)

    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def plot_b(r, t0, jobs, out):
    # rows: node-a cores 0..7 (0 = memcached), node-b cores 0..3
    rows = ([("node-a-8core", c) for c in range(8)]
            + [("node-b-4core", c) for c in range(4)])
    row_index = {rc: i for i, rc in enumerate(rows)}
    labels = [f"{n.split('-')[1]} c{c}" for n, c in rows]

    fig, ax = plt.subplots(figsize=(12, 5.5))

    # memcached occupies node-a core 0 throughout the run
    run_end = max(e for _, e in jobs.values())
    ax.broken_barh([(0, (run_end - t0) / 1000.0)],
                   (row_index[("node-a-8core", 0)] - 0.4, 0.8),
                   facecolors="#888888", edgecolor="black",
                   hatch="//", label="memcached")

    # Build a map of core -> list of (job_name, start_ms, end_ms)
    core_jobs = {}
    for job, (s, e) in jobs.items():
        node, cores = JOB_NODE[job]
        for c in cores:
            rc = (node, c)
            if rc not in core_jobs:
                core_jobs[rc] = []
            core_jobs[rc].append((job, s, e))

    # For each core with jobs, draw stacked bars for overlapping jobs
    for (node, core), job_list in core_jobs.items():
        i = row_index[(node, core)]
        
        # Build timeline: collect all start/end events
        events = []
        for job, s, e in job_list:
            events.append((s, 'start', job))
            events.append((e, 'end', job))
        events.sort()
        
        # Process events to find time intervals with overlaps
        active_jobs = []
        prev_time = None
        intervals = []  # list of (start_time, end_time, active_jobs_list)
        
        for time, event_type, job in events:
            if prev_time is not None and time > prev_time and active_jobs:
                intervals.append((prev_time, time, list(active_jobs)))
            
            if event_type == 'start':
                active_jobs.append(job)
            else:  # end
                if job in active_jobs:
                    active_jobs.remove(job)
            
            prev_time = time
        
        # Draw stacked bars for each interval
        for start_ms, end_ms, active_list in intervals:
            x = (start_ms - t0) / 1000.0
            w = (end_ms - start_ms) / 1000.0
            
            # Sort jobs for consistent ordering
            active_list.sort()
            num_jobs = len(active_list)
            height = 0.8 / num_jobs
            
            for idx, job in enumerate(active_list):
                y_base = i - 0.4 + idx * height
                ax.broken_barh([(x, w)], (y_base, height),
                               facecolors=COLORS[job], edgecolor="black", linewidth=0.5)

    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Time since first container start [s]")
    ax.grid(axis="x", linestyle=":", alpha=0.5)
    # separator between nodes
    ax.axhline(7.5, color="black", linewidth=0.8)

    legend = [Patch(facecolor=COLORS[j], edgecolor="black", label=j)
              for j in COLORS]
    legend.append(Patch(facecolor="#888888", edgecolor="black",
                        hatch="//", label="memcached"))
    ax.legend(handles=legend, loc="center left",
              bbox_to_anchor=(1.01, 0.5), fontsize=8)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


print(f"Plotting from {BASE}, runs: {RUNS}\n")
for r in RUNS:
    t0, jobs, lats = load_run(r)
    plot_a(r, t0, jobs, lats, f"part3_run{r}_memcache_p95.png")
    plot_b(r, t0, jobs, f"part3_run{r}_core_schedule.png")
    print(f"run {r}: {len(lats)} latency samples, t0={t0}")
print("done")
