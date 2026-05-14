# EVOLVE-BLOCK-START
# ======== // SECTION A - KUBERNETES JOB CONFIGURATION \\ =======
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
			"requests": {"cpu": "6"},
			"limits": {"cpu": "7"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 1-7 ./run -a run -S parsec -p blackscholes -i native -n 7",
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
			"nodeSelector": {"cca-project-nodetype": "node-a-8core"},
			"requests": {"cpu": "6"},
			"limits": {"cpu": "7"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 1-7 ./run -a run -S parsec -p canneal -i native -n 7",
			],
		},
		"parsec-streamcluster": {
			"nodeSelector": {"cca-project-nodetype": "node-b-4core"},
			"requests": {"cpu": "3"},
			"limits": {"cpu": "4"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 0-3 ./run -a run -S parsec -p streamcluster -i native -n 4",
			],
		},
		"parsec-freqmine": {
			"nodeSelector": {"cca-project-nodetype": "node-a-8core"},
			"requests": {"cpu": "6"},
			"limits": {"cpu": "7"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 1-7 ./run -a run -S parsec -p freqmine -i native -n 7",
			],
		},
		"parsec-barnes": {
			"nodeSelector": {"cca-project-nodetype": "node-b-4core"},
			"requests": {"cpu": "3"},
			"limits": {"cpu": "4"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 0-3 ./run -a run -S splash2x -p barnes -i native -n 4",
			],
		},
		"parsec-vips": {
			"nodeSelector": {"cca-project-nodetype": "node-b-4core"},
			"requests": {"cpu": "3"},
			"limits": {"cpu": "4"},
			"command": ["/bin/sh"],
			"args": [
				"-c",
				"taskset -c 0-3 ./run -a run -S parsec -p vips -i native -n 4",
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



# ======== // SECTION B - CONCURRENY STRUCTURE \\ =========
JOB_DEPENDENCIES: dict[str, list[str]] = {
	"parsec-streamcluster": [],
	"parsec-canneal": [],
	"parsec-freqmine": ["parsec-canneal"],
	"parsec-blackscholes": ["parsec-freqmine", "parsec-canneal"],
	"parsec-barnes": ["parsec-streamcluster"],
	"parsec-vips": ["parsec-barnes", "parsec-streamcluster"],
	"parsec-radix": ["parsec-blackscholes", "parsec-freqmine", "parsec-canneal"],
}
# EVOLVE-BLOCK-END