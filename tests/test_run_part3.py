import tempfile
from pathlib import Path
import unittest

from automation import run_part3


class RunPart3Tests(unittest.TestCase):
    def test_parse_benchmark_markers_extracts_start_end_and_exit_code(self):
        logs = """
Launching parsec.vips...
CCA_BENCHMARK_START_TS=1713630301.100000000
benchmark output
CCA_BENCHMARK_END_TS=1713630329.450000000 EXIT_CODE=0
"""
        markers = run_part3.parse_benchmark_markers(logs)

        self.assertEqual(markers["benchmark_start"], 1713630301.1)
        self.assertEqual(markers["benchmark_end"], 1713630329.45)
        self.assertEqual(markers["exit_code"], 0)

    def test_render_job_manifest_applies_run_suffix_and_instruments_command(self):
        manifest = """apiVersion: batch/v1
kind: Job
metadata:
  name: parsec-vips
  labels:
    name: parsec-vips
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:parsec_vips
        name: parsec-vips
        imagePullPolicy: Always
        command: ["/bin/sh"]
        args: ["-c", "./run -a run -S parsec -p vips -i native -n 4"]
"""
        rendered = run_part3.render_job_manifest(
            manifest,
            base_job_name="parsec-vips",
            actual_job_name="parsec-vips-run2",
        )

        self.assertIn("name: parsec-vips-run2", rendered)
        self.assertIn("CCA_BENCHMARK_START_TS=", rendered)
        self.assertIn("CCA_BENCHMARK_END_TS=", rendered)
        self.assertIn("./run -a run -S parsec -p vips -i native -n 4", rendered)

    def test_collect_images_and_worker_labels_reads_part3_manifests(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            files = {
                "part3-parsec-streamcluster.yaml": """apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:parsec_streamcluster
      nodeSelector:
        cca-project-nodetype: "node-a-8core"
""",
                "part3-parsec-freqmine.yaml": """apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:parsec_freqmine
      nodeSelector:
        cca-project-nodetype: "node-a-8core"
""",
                "part3-parsec-canneal.yaml": """apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:parsec_canneal
      nodeSelector:
        cca-project-nodetype: "node-b-4core"
""",
                "part3-parsec-blackscholes.yaml": """apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:parsec_blackscholes
      nodeSelector:
        cca-project-nodetype: "node-b-4core"
""",
                "part3-parsec-radix.yaml": """apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:splash2x_radix
      nodeSelector:
        cca-project-nodetype: "node-b-4core"
""",
                "part3-parsec-barnes.yaml": """apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:splash2x_barnes
      nodeSelector:
        cca-project-nodetype: "node-a-8core"
""",
                "part3-parsec-vips.yaml": """apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
      - image: anakli/cca:parsec_vips
      nodeSelector:
        cca-project-nodetype: "node-a-8core"
""",
            }
            for name, content in files.items():
                (root / name).write_text(content)

            images, labels = run_part3.collect_images_and_worker_labels(root)

            self.assertEqual(
                images,
                sorted(
                    [
                        "anakli/cca:parsec_blackscholes",
                        "anakli/cca:parsec_canneal",
                        "anakli/cca:parsec_freqmine",
                        "anakli/cca:parsec_streamcluster",
                        "anakli/cca:parsec_vips",
                        "anakli/cca:splash2x_barnes",
                        "anakli/cca:splash2x_radix",
                    ]
                ),
            )
            self.assertEqual(labels, ["node-a-8core", "node-b-4core"])


if __name__ == "__main__":
    unittest.main()
