



#################

ANY TIME YOU MAKE A CHANGE CHECK IF IT CONSTRADICTS OR IT IS MISSING IN THE project walkthrough.md and update it accordingly

################


################

## PART 3 CORE TASK

Your goal is to design a scheduling policy that will minimize the time it takes for all seven batch
workloads to complete (their makespan), while guaranteeing a tail latency service level objective
(SLO) for the long-running memcached service. It might be helpful to take into account the characteristics of the batch applications you noted in Part 2 of the project(e.g. speedup across cores, total
runtime, etc.). For this part of the project, the memcached service will receive requests from the
client at a steady rate, and you will measure the request tail latency. Your scheduling policy should
minimize the makespan of all batch applications, without violating a strict service level objective for memcached of 1 ms 95th percentile latency at 30K QPS. You also must ensure that
all seven batch applications complete successfully, as jobs may abort due to errors (e.g. out of
memory). Use the native dataset size for all batch applications. At every point in time, you
must use as many resources of your cluster as possible.


ADD ONE REFERENCE DOCUMENT PER BATCH APPLICAIOTN ILLUSTRATING THEIR EXECUTION PROFILES