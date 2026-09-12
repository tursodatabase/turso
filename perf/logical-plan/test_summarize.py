import json
from pathlib import Path
import tempfile
import unittest

from summarize import instruction_run, native_run, summarize


class MeasurementTests(unittest.TestCase):
    def test_native_parser_keeps_nested_workloads_and_medians(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "native.txt"
            path.write_text("prepare_benchmark\n"
                            "├─ alpha    1 µs │ 9 µs │ 5 µs │ 4 µs\n"
                            "╰─ joins\n"
                            "   ├─ 2     2 ms │ 8 ms │ 3 ms │ 4 ms\n"
                            "   ╰─ 20    1 s  │ 9 s  │ 5 s  │ 4 s\n")
            self.assertEqual(native_run(path), {
                "alpha": 5_000, "joins/2": 3_000_000, "joins/20": 5_000_000_000,
            })

    def test_test_mode_output_is_not_a_native_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "native.txt"
            path.write_text("prepare_benchmark\n╰─ alpha\n")
            with self.assertRaisesRegex(ValueError, "native runs require --bench"):
                native_run(path)

    def test_instruction_parser_uses_only_the_prepare_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            write_instructions(directory, 1, 123)
            self.assertEqual(instruction_run(directory, 1), {"alpha": 123})
            (directory / "callgrind-1.txt").write_text("prepare_benchmark\n├─ alpha\n╰─ beta\n")
            with self.assertRaisesRegex(ValueError, "Cannot pair"):
                instruction_run(directory, 1)

    def test_uncertainty_uses_the_fixed_baseline_range_and_mad(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            for repeat, median in enumerate([10, 12, 9, 11, 10, 8, 10], 1):
                (directory / f"native-{repeat}.txt").write_text(
                    f"prepare_benchmark\n╰─ alpha  1 ns │ 15 ns │ {median} ns │ 9 ns\n")
            for repeat in range(1, 4):
                write_instructions(directory, repeat, 100)
            result = summarize(directory)["alpha"]
            self.assertEqual(result["native_median_ns"], 10)
            self.assertEqual(result["native_uncertainty_ns"], 4)
            self.assertEqual(result["instructions"], [100, 100, 100])


def write_instructions(directory, repeat, count):
    (directory / f"callgrind-{repeat}.txt").write_text("prepare_benchmark\n╰─ alpha\n")
    (directory / f"callgrind-{repeat}.json").write_text(json.dumps([
        {"file": "metadata.gz", "raw": ["desc: Trigger: Client Request", "totals: 999"]},
        {"file": "prepare.gz", "raw": [
            "desc: Trigger: --dump-after=prepare_benchmark::measure_prepare",
            "summary: 18446744073709551615", f"totals: {count}"]},
        {"file": "termination.gz", "raw": ["desc: Trigger: Program termination", "totals: 999"]},
    ]))


if __name__ == "__main__":
    unittest.main()
