import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("plot_fts", Path(__file__).with_name("plot-fts.py"))
plot_fts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plot_fts)

memory_spec = importlib.util.spec_from_file_location("fts_memory", Path(__file__).parents[1] / "scripts/memory.py")
fts_memory = importlib.util.module_from_spec(memory_spec)
memory_spec.loader.exec_module(fts_memory)


class PlotTests(unittest.TestCase):
    def read(self, rows, other=None, percentile="p50"):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index, batch in enumerate([rows] if other is None else [rows, other]):
                path = Path(directory) / f"synthetic-{index}.csv"
                with path.open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=batch[0].keys())
                    writer.writeheader()
                    writer.writerows(batch)
                paths.append(path)
            return plot_fts.read_runs(paths, percentile) if other is None else plot_fts.read_sweep(paths, percentile)

    def rows(self):
        return [dict(engine="turso", mode="wal", state="warm", documents="103",
                     connections="1", queries="4", benchmark="throughput", debug_assertions="true",
                     requested_queries="0", min_seconds="0.001", p50_ms="", p95_ms="", p99_ms="",
                     run="1", query=query, seconds="0.008") for query in plot_fts.QUERIES]

    def test_throughput_is_completed_queries_per_second(self):
        rows = self.rows() + [dict(row, run="2", queries="8", seconds="0.016") for row in self.rows()]
        _, samples = self.read(rows)
        self.assertEqual(samples[("turso", "wal")]["rare"], [500.0, 500.0])

    def test_search_uses_individual_query_percentiles_not_wall_time_average(self):
        rows = [dict(row, benchmark="search", requested_queries="4", min_seconds="0",
                     p50_ms="1", p95_ms="5", p99_ms="6") for row in self.rows()]
        _, samples = self.read(rows)
        self.assertEqual(samples[("turso", "wal")]["rare"], [1.0])
        _, samples = self.read(rows, percentile="p95")
        self.assertEqual(samples[("turso", "wal")]["rare"], [5.0])
        with self.assertRaises(ValueError):
            self.read(rows + self.rows())

    def test_sweep_keeps_connections_separate_and_requires_equal_duration(self):
        four = [dict(row, connections="4", seconds="0.004") for row in self.rows()]
        _, points = self.read(four, self.rows())
        self.assertEqual(list(points), [1, 4])
        self.assertEqual(points[4][("turso", "wal")]["rare"], [1000.0])
        self.assertEqual(points[1][("turso", "wal")]["rare"], [500.0])
        for change in ({"min_seconds": "0.002"}, {"connections": "1"}, {"engine": "sqlite"}):
            with self.assertRaises(ValueError):
                self.read(self.rows(), [dict(row, **change) for row in four])

    def test_latency_sweep_uses_p99_individual_samples_at_each_connection_count(self):
        one = [dict(row, benchmark="search", requested_queries="4", min_seconds="0",
                    p50_ms="1", p95_ms="5", p99_ms="6") for row in self.rows()]
        four = [dict(row, connections="4", p50_ms="2", p95_ms="8", p99_ms="11") for row in one]
        configuration, points = self.read(one, four, percentile="p99")
        self.assertEqual(configuration["percentile"], "p99")
        self.assertEqual(points[1][("turso", "wal")]["rare"], [6.0])
        self.assertEqual(points[4][("turso", "wal")]["rare"], [11.0])
        with self.assertRaises(ValueError):
            self.read(one, [dict(row, requested_queries="8", queries="8") for row in four])

    def test_keeps_engine_modes_and_repetitions_separate(self):
        rows = []
        for run, seconds in [("1", "0.004"), ("2", "0.020")]:
            for index, (engine, mode) in enumerate(plot_fts.SERIES, 1):
                rows.extend(dict(row, run=run, engine=engine, mode=mode,
                                 seconds=str(float(seconds) * index)) for row in self.rows())
        _, samples = self.read(rows)
        self.assertEqual(samples[("sqlite", "wal")]["rare"], [1000.0, 200.0])
        self.assertEqual(samples[("turso", "wal")]["rare"], [500.0, 100.0])
        self.assertEqual(samples[("turso", "mvcc")]["rare"], [4 / 0.012, 4 / 0.060])
        with self.assertRaises(ValueError):
            self.read(rows[:-6])

    def test_rejects_sqlite_mvcc(self):
        with self.assertRaises(ValueError):
            self.read([dict(row, engine="sqlite", mode="mvcc") for row in self.rows()])

    def test_rejects_profiled_timings(self):
        with self.assertRaisesRegex(ValueError, "profiled timings"):
            self.read([dict(row, profiled="true") for row in self.rows()])
        _, samples = self.read([dict(row, profiled="false") for row in self.rows()])
        self.assertEqual(samples[("turso", "wal")]["rare"], [500.0])

    def test_rejects_incomplete_duplicate_and_mixed_runs(self):
        rows = self.rows()
        with self.assertRaises(ValueError):
            self.read(rows[:-1])
        with self.assertRaises(ValueError):
            self.read(rows + [rows[0]])
        rows[-1]["documents"] = "104"
        with self.assertRaises(ValueError):
            self.read(rows)

    def test_rejects_invalid_measurements(self):
        for seconds in ("nan", "inf", "0", "-1", "0.0009"):
            rows = self.rows()
            rows[0]["seconds"] = seconds
            with self.assertRaises(ValueError):
                self.read(rows)

    def test_speedup_uses_ratio_of_medians_and_correct_direction(self):
        points = {1: {("sqlite", "wal"): {"rare": [4, 8]}, ("turso", "wal"): {"rare": [2, 6]}},
                  4: {("sqlite", "wal"): {"rare": [3]}, ("turso", "wal"): {"rare": [9]}}}
        medians, lows, highs = plot_fts.sweep_values(points, ("turso", "wal"), "rare", "search", True)
        self.assertEqual(list(medians), [1.5, 1 / 3])
        self.assertEqual(list(lows), [1, 1 / 3])
        self.assertEqual(list(highs), [3, 1 / 3])
        medians, _, _ = plot_fts.sweep_values(points, ("turso", "wal"), "rare", "throughput", True)
        self.assertEqual(list(medians), [2 / 3, 3])
        for values in plot_fts.sweep_values(points, ("sqlite", "wal"), "rare", "search", True):
            self.assertEqual(list(values), [1, 1])

    def test_large_values_use_thousands_separators(self):
        for value, expected in [(12345, "12,345"), (999.6, "1,000"), (999.4, "999"), (0.01234, "0.0123")]:
            self.assertEqual(plot_fts.format_value(value), expected)

    def test_memory_plots_peak_bytes_as_mib_without_timing(self):
        one = [dict(row, benchmark="memory", requested_queries="4", min_seconds="0",
                    peak_heap_bytes="1572864") for row in self.rows()]
        for row in one:
            del row["seconds"]
        four = [dict(row, connections="4", queries="16", peak_heap_bytes="5242880") for row in one]
        _, points = self.read(one, four)
        self.assertEqual(points[1][("turso", "wal")]["rare"], [1.5])
        self.assertEqual(points[4][("turso", "wal")]["rare"], [5])
        with self.assertRaises(ValueError):
            self.read([dict(row, engine="sqlite") for row in one])
        with self.assertRaises(ValueError):
            self.read([dict(row, queries="4") for row in four])

    def test_memory_summary_preserves_peak_not_total_or_per_query(self):
        report = dict(query="common", documents=203, rows_per_query=203, mode="wal", state="warm",
                      queries=6, connections=3, transactions=0, peak_live_query_bytes=8192,
                      retained_query_bytes=4096, total_allocated_bytes=48000, total_allocations=700)
        row = fts_memory.measurement(report, 2, 1, "bench-profile")
        self.assertEqual(row["peak_heap_bytes"], 8192)
        self.assertEqual(row["queries"], 6)
        self.assertEqual(row["requested_queries"], 2)
        for change in (dict(queries=2), dict(rows_per_query=202), dict(retained_query_bytes=8193),
                       dict(total_allocated_bytes=8191)):
            with self.assertRaises(ValueError):
                fts_memory.measurement(report | change, 2, 1, "bench-profile")


if __name__ == "__main__":
    unittest.main()
