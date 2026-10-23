"""Executable regression witnesses for the benchmark gate (stdlib only)."""

import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import unittest


TOOL = Path(__file__).resolve().parents[1] / "tools/bench-compare"
METHOD = "individual_wall_time_linear_interpolation_v1"


class BenchCompareTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.results = self.root / "criterion"
        self.baseline = self.root / "baseline.json"
        self.profile = "linux" if platform.system() == "Linux" else "laptop"
        self.write_sample("door", 100_000)
        self.write_sample("ladder", 200_000)
        self.manifest(["door", "ladder"])
        result = self.run_tool("--write", "--machine", "fixture-machine")
        self.assertEqual(result.returncode, 0, result.stderr)

    def write_sample(self, name, p99, p50=50_000):
        folder = self.results / name / "new"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "estimates.json").write_text(json.dumps({
            "p50": {"point_estimate": p50}, "p99": {"point_estimate": p99},
            "latency_method": METHOD, "latency_samples": 100,
        }), encoding="utf-8")

    def manifest(self, names, finished=True):
        (self.results / "door-run.json").write_text(json.dumps({
            "benchmarks": names, "finished": finished,
        }), encoding="utf-8")

    def run_tool(self, *args):
        return subprocess.run([
            sys.executable, str(TOOL), str(self.results), "--baseline", str(self.baseline),
            "--profile", self.profile, *args,
        ], capture_output=True, text=True, check=False)

    def test_identical_baseline_passes(self):
        result = self.run_tool()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PASS: 2 benchmarks, 0 regressions", result.stdout)

    def test_p99_over_five_percent_fails(self):
        self.write_sample("door", 105_001)
        result = self.run_tool()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("1 regressions", result.stdout)

    def test_exactly_five_percent_passes(self):
        self.write_sample("door", 105_000)
        self.assertEqual(self.run_tool().returncode, 0)

    def test_tolerance_is_overridable(self):
        self.write_sample("door", 109_000)
        self.assertEqual(self.run_tool("--tolerance", "10").returncode, 0)

    def test_p50_is_reported_but_does_not_gate(self):
        self.write_sample("door", 100_000, p50=99_000)
        self.assertEqual(self.run_tool().returncode, 0)

    def test_missing_benchmark_cannot_pass_using_a_stale_directory(self):
        self.manifest(["door"])
        result = self.run_tool()
        self.assertEqual(result.returncode, 2)
        self.assertIn("missing=['ladder']", result.stderr)

    def test_new_benchmark_needs_a_baseline(self):
        self.write_sample("new", 100_000)
        self.manifest(["door", "ladder", "new"])
        self.assertEqual(self.run_tool().returncode, 2)

    def test_incomplete_run_is_rejected(self):
        self.manifest(["door", "ladder"], finished=False)
        result = self.run_tool()
        self.assertEqual(result.returncode, 2)
        self.assertIn("did not finish", result.stderr)

    def test_nonfinite_estimate_is_rejected(self):
        self.write_sample("door", float("nan"))
        self.assertEqual(self.run_tool().returncode, 2)

    def test_pending_linux_numbers_are_never_fabricated(self):
        data = json.loads(self.baseline.read_text(encoding="utf-8"))
        data["profiles"][self.profile] = {"status": "pending"}
        self.baseline.write_text(json.dumps(data), encoding="utf-8")
        result = self.run_tool()
        self.assertEqual(result.returncode, 2)
        self.assertIn("baseline pending", result.stderr)

    def test_different_platform_is_rejected(self):
        data = json.loads(self.baseline.read_text(encoding="utf-8"))
        data["profiles"][self.profile]["system"] = "Other"
        self.baseline.write_text(json.dumps(data), encoding="utf-8")
        self.assertEqual(self.run_tool().returncode, 2)

    def test_corrupt_baseline_shape_is_rejected(self):
        self.baseline.write_text('"not a baseline"', encoding="utf-8")
        self.assertEqual(self.run_tool().returncode, 2)

    def test_filter_compares_only_the_requested_witness(self):
        self.manifest(["door"])
        self.write_sample("door", 110_000)
        result = self.run_tool("--filter", "door")
        self.assertEqual(result.returncode, 1)
        self.assertIn("FAIL: 1 benchmarks", result.stdout)


if __name__ == "__main__":
    unittest.main()
