# Cloud Computing Architecture Project

This repository contains starter code for the Cloud Computing Architecture course project at ETH Zurich. Students will explore how to schedule latency-sensitive and batch applications in a cloud cluster. Please follow the instructions in the project handout.

## Technologies

- **Infrastructure**: Google Cloud Engine (GCE), kops (Kubernetes Operations), Kubernetes 1.31.5
- **Latency-sensitive app**: `memcached` (image `anakli/memcached:t1`), pinned to core 0 via `taskset -c 0`, single-threaded (`-t 1`)
- **Load generator**: `mcperf` (memcache-perf) -- two VMs: a **client-measure** node and a **client-agent** node (agent generates load, measure collects latency stats)
- **Interference workloads**: `ibench` micro-benchmarks (cpu, l1d, l1i, l2, llc, membw) and **PARSEC** benchmark suite (blackscholes, canneal, freqmine, vips, barnes, radix, streamcluster)
- **Region**: `europe-west1-b`

## Part 1: Memcached Sensitivity to Micro-Architectural Interference

### Cluster Topology

| Node | Machine Type | Role |
|------|-------------|------|
| Master | `n2-standard-2` | K8s control plane |
| memcache-server | `e2-highcpu-2` (2 vCPU) | Runs memcached pod |
| client-measure | `n2-standard-2` | Runs mcperf measurement |
| client-agent | `e2-standard-8` | Runs mcperf load generation |

### What Is Benchmarked

- Memcached runs **alone on a dedicated node** (`e2-highcpu-2`, 2 vCPUs).
- QPS is swept from 5000 to 55000 in steps of 5000 using mcperf (`--scan 5000:55000:5000`).
- First: **baseline** (memcached alone, no interference).
- Then: **one ibench interference at a time** co-located on the same node (same `cca-project-nodetype: memcached` nodeSelector). Each ibench is also pinned to core 0 (`taskset -c 0`), so it directly competes with memcached for the same core's resources.
- 6 interference types: **CPU, L1d cache, L1i cache, L2 cache, LLC (last-level cache), memory bandwidth**.

### Goal

Measure how each micro-architectural resource (compute, cache levels, memory BW) impacts memcached tail latency (p95/p99) across load levels.

## Part 2a: PARSEC Benchmarks Alone (Profiling)

### Cluster Topology

| Node | Machine Type | Role |
|------|-------------|------|
| Master | `e2-standard-2` | K8s control plane |
| parsec-server | `e2-standard-2` (2 vCPU) | Runs PARSEC jobs |

### What Is Benchmarked

- Each of 7 PARSEC benchmarks run **in isolation** on a small machine.
- Input size: **`simlarge`** (moderate).
- Threads: **`-n 1`** (single-threaded).
- No memcached, no interference -- just profiling the PARSEC workloads themselves.
- 7 benchmarks: blackscholes, canneal, freqmine, vips, barnes, radix, streamcluster.

### Goal

Characterize each PARSEC benchmark's resource profile (CPU-bound vs memory-bound vs cache-sensitive) on a small machine with a lighter workload.

## Part 2b: PARSEC Benchmarks as Interference to Memcached

### Cluster Topology

| Node | Machine Type | Role |
|------|-------------|------|
| Master | `e2-standard-2` | K8s control plane |
| parsec-server | `e2-standard-8` (8 vCPU) | Runs PARSEC jobs |

### What Is Benchmarked

- Each PARSEC benchmark runs on a **larger machine** with a **`native`** (full-size) input.
- Still single-threaded (`-n 1`).
- These are co-located with memcached to observe how real application workloads (not synthetic ibench) interfere with memcached latency.

### Goal

Use the resource profiles from Part 2a to explain why certain PARSEC benchmarks degrade memcached performance more than others when co-located.

## PARSEC Benchmark Suite

The project uses benchmarks from two sub-suites within PARSEC:

| Benchmark | Suite | Description |
|---|---|---|
| blackscholes | parsec | Option pricing via Black-Scholes PDE (compute-bound, regular FP math) |
| canneal | parsec | Simulated annealing for chip design (memory-bound, irregular access) |
| freqmine | parsec | Frequent itemset mining (mixed compute/memory) |
| vips | parsec | Image processing pipeline (memory-streaming, high bandwidth) |
| barnes | splash2x | N-body simulation via Barnes-Hut algorithm (compute + pointer-chasing) |
| radix | splash2x | Integer radix sort (memory-streaming, high bandwidth) |
| streamcluster | parsec | Online clustering of streams (mixed compute/memory) |

### Input Sizes: `simlarge` vs `native`

PARSEC defines a hierarchy of input sizes that control the dataset each benchmark operates on. The two used in this project are:

- **`simlarge`**: A reduced dataset designed for simulation and profiling. Runs faster, produces a lighter resource footprint, and is suitable for characterizing the *type* of resource pressure a benchmark exerts without needing long execution times. Used in **Part 2a** for standalone profiling on a small machine (`e2-standard-2`).
- **`native`**: The full, production-scale dataset that represents a realistic workload. Runs significantly longer and stresses hardware resources at full intensity. Used in **Part 2b** where benchmarks act as interference to memcached on a larger machine (`e2-standard-8`).

The distinction matters because the resource profile of a benchmark can shift with input size -- a workload that fits in cache at `simlarge` may spill to main memory at `native`, changing its interference characteristics.

### Input Sizes Per Benchmark in This Project

| Benchmark | Part 2a (profiling) | Part 2b (interference) |
|---|---|---|
| blackscholes | simlarge | native |
| canneal | simlarge | native |
| freqmine | simlarge | native |
| streamcluster | simlarge | native |
| barnes | native | native |
| radix | native | native |
| vips | native | native |

Note: `barnes`, `radix`, and `vips` use `native` in both parts. The splash2x benchmarks (barnes, radix) and vips already use `native` for Part 2a profiling.

## Comparison: Part 1 vs Part 2a vs Part 2b

| | Part 1 | Part 2a | Part 2b |
|---|---|---|---|
| **Purpose** | Sensitivity analysis of memcached to micro-arch resources | Workload profiling of PARSEC benchmarks in isolation | Real-world interference: PARSEC co-located with memcached |
| **Memcached?** | Yes (the subject) | No | Yes (the subject) |
| **Interference** | ibench (synthetic): cpu, l1d, l1i, l2, llc, membw | None -- profiling only | PARSEC (real apps): blackscholes, canneal, freqmine, vips, etc. |
| **Interference nature** | Synthetic, targets ONE resource each | N/A | Realistic, mixed resource profiles |
| **Server machine** | `e2-highcpu-2` (2 vCPU) | `e2-standard-2` (2 vCPU) | `e2-standard-8` (8 vCPU) |
| **Input size** | N/A (ibench) | `simlarge` | `native` (full) |
| **Metric** | memcached latency vs QPS | PARSEC execution time + resource use | memcached latency vs QPS |
| **Core pinning** | Both memcached and ibench on core 0 | No pinning | Co-scheduled by K8s |
| **Cluster nodes** | 4 (master, memcache-server, 2 clients) | 2 (master, parsec) | 2 (master, parsec) |

### Logical Flow

1. **Part 1** answers: *"Which hardware resources does memcached depend on most?"* -- by stressing one resource at a time with ibench and measuring latency degradation.
2. **Part 2a** answers: *"What resource profile does each PARSEC benchmark have?"* -- by running them alone and observing their execution characteristics.
3. **Part 2b** answers: *"Can we predict/explain memcached interference from real workloads?"* -- by co-locating PARSEC benchmarks with memcached, then using Part 1's sensitivity map + Part 2a's resource profiles to explain the observed latency degradation.

In short: Part 1 identifies memcached's **vulnerabilities**, Part 2a profiles the **attackers**, and Part 2b puts them together to validate the model.
