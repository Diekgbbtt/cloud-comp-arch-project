import docker

client = docker.from_env()

benchmark = "blackscholes"
num_threads = 2
container = client.containers.run(
    f"anakli/cca:parsec_{benchmark}",
    f"./run -a run -S parsec -p {benchmark} -i native -n {num_threads}",
    cpuset_cpus="0",
    name=benchmark,
    remove=True,
    detach=True,
)
