"""Tests for the optimizer tuning driver.

Run with `python3 -m unittest discover -s perf/optimizer-tuning`.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cost_vector
import runner
import tune


class CostVectorChecks(unittest.TestCase):
    def test_defaults_pass(self):
        checked = cost_vector.validate(cost_vector.defaults())
        self.assertEqual(set(checked), set(cost_vector.FIELD_NAMES))

    def test_unknown_key_is_rejected(self):
        vector = dict(cost_vector.defaults(), cpu_cost_per_rowe=1.0)
        with self.assertRaises(cost_vector.VectorError):
            cost_vector.validate(vector)

    def test_missing_key_is_rejected(self):
        vector = dict(cost_vector.defaults())
        del vector["index_bonus"]
        with self.assertRaises(cost_vector.VectorError):
            cost_vector.validate(vector)

    def test_non_finite_is_rejected(self):
        for bad in (float("inf"), float("nan"), float("-inf")):
            with self.assertRaises(cost_vector.VectorError):
                cost_vector.validate(dict(cost_vector.defaults(), cpu_cost_per_row=bad))

    def test_selectivity_domain(self):
        for bad in (0.0, -0.1, 1.5):
            with self.assertRaises(cost_vector.VectorError):
                cost_vector.validate(dict(cost_vector.defaults(), sel_range=bad))

    def test_indexed_selectivity_must_not_exceed_unindexed(self):
        vector = dict(cost_vector.defaults(), sel_eq_indexed=0.5, sel_eq_unindexed=0.1)
        with self.assertRaises(cost_vector.VectorError):
            cost_vector.validate(vector)

    def test_rows_per_table_page_of_one_is_rejected(self):
        # estimate_btree_depth divides by the natural logarithm of this value.
        for bad in (1.0, 0.5):
            with self.assertRaises(cost_vector.VectorError):
                cost_vector.validate(
                    dict(cost_vector.defaults(), rows_per_table_page=bad)
                )

    def test_cache_reuse_factor_domain(self):
        for bad in (-0.01, 1.0, 1.5):
            with self.assertRaises(cost_vector.VectorError):
                cost_vector.validate(
                    dict(cost_vector.defaults(), cache_reuse_factor=bad)
                )
        cost_vector.validate(dict(cost_vector.defaults(), cache_reuse_factor=0.0))

    def test_index_bonus_has_an_upper_bound(self):
        with self.assertRaises(cost_vector.VectorError):
            cost_vector.validate(dict(cost_vector.defaults(), index_bonus=1e6))
        cost_vector.validate(dict(cost_vector.defaults(), index_bonus=0.0))

    def test_samples_are_always_valid(self):
        import random
        rng = random.Random(12345)
        for _ in range(500):
            cost_vector.validate(cost_vector.sample_cost_only(rng))
            cost_vector.validate(cost_vector.sample_heuristics(rng))
            cost_vector.validate(
                cost_vector.perturb(rng, cost_vector.defaults(),
                                    cost_vector.COST_ONLY_FIELDS)
            )

    def test_matches_detects_a_changed_field(self):
        left = cost_vector.defaults()
        self.assertTrue(cost_vector.matches(left, dict(left)))
        self.assertFalse(
            cost_vector.matches(left, dict(left, cpu_cost_per_row=0.0031))
        )


class EngineDefaults(unittest.TestCase):
    def test_the_table_matches_the_engine(self):
        """The table in cost_vector.py must equal `CostModelParams::new()`.

        A search normalizes every candidate against the compiled defaults, so a
        table that drifted from the engine would score every candidate against
        the wrong reference.
        """
        engine = cost_vector.engine_defaults()
        self.assertEqual(sorted(engine), sorted(cost_vector.DEFAULTS))
        for name, value in sorted(engine.items()):
            self.assertAlmostEqual(
                value, cost_vector.DEFAULTS[name], places=12,
                msg=f"{name} differs from the engine's compiled default",
            )


class BytecodeNormalization(unittest.TestCase):
    def test_schema_cookie_is_removed(self):
        first = [["0", "Init", "0", "10", "0", "", "0"],
                 ["1", "Transaction", "0", "1", "120", "", "0"]]
        second = [["0", "Init", "0", "10", "0", "", "0"],
                  ["1", "Transaction", "0", "1", "122", "", "0"]]
        self.assertNotEqual(first, second)
        self.assertEqual(
            runner.normalized_bytecode(first), runner.normalized_bytecode(second)
        )

    def test_other_operands_still_count(self):
        first = [["1", "SeekGE", "0", "9", "3", "", "0"]]
        second = [["1", "SeekGE", "0", "9", "4", "", "0"]]
        self.assertNotEqual(
            runner.normalized_bytecode(first), runner.normalized_bytecode(second)
        )


class WorkloadManifest(unittest.TestCase):
    def setUp(self):
        here = os.path.dirname(os.path.abspath(__file__))
        self.workload = runner.Workload(
            os.path.join(here, "..", "tpc-h", "queries")
        )

    def test_every_file_is_runnable_because_none_is_skipped(self):
        self.assertEqual(len(self.workload.queries), 22)
        self.assertEqual(len(self.workload.names), 22)

    def test_queries_are_in_numeric_order(self):
        self.assertEqual(
            self.workload.names, [str(n) for n in range(1, 23)]
        )

    def test_the_query_that_creates_a_view_needs_a_writable_database(self):
        writable = [row["query"] for row in self.workload.queries if row["writable"]]
        self.assertEqual(writable, ["15"])

    def test_sqlite_skip_does_not_remove_a_query_from_turso(self):
        with_skip = [
            row["query"] for row in self.workload.queries
            if any(d.startswith("-- SQLITE_SKIP") for d in row["directives"])
        ]
        self.assertEqual(with_skip, ["17", "20", "22"])
        for name in with_skip:
            self.assertIn(name, self.workload.names)


class RowComparison(unittest.TestCase):
    @staticmethod
    def integer_row(*values):
        return [{"i": v} for v in values]

    def test_equal_rows_match(self):
        rows = [self.integer_row(1, 2), self.integer_row(3, 4)]
        verdict = tune.compare_rows(rows, list(rows))
        self.assertTrue(verdict["same_multiset"])
        self.assertTrue(verdict["same_order"])

    def test_multiplicity_counts(self):
        left = [self.integer_row(1), self.integer_row(1)]
        right = [self.integer_row(1)]
        self.assertFalse(tune.compare_rows(left, right)["same_multiset"])

    def test_null_is_not_an_empty_string(self):
        self.assertFalse(
            tune.compare_rows([[None]], [[{"t": ""}]])["same_multiset"]
        )

    def test_integer_is_not_a_float(self):
        self.assertFalse(
            tune.compare_rows([[{"i": 1}]], [[{"f": 1.0}]])["same_multiset"]
        )

    def test_a_reordered_result_is_reported_as_the_same_rows(self):
        left = [self.integer_row(1), self.integer_row(2)]
        right = [self.integer_row(2), self.integer_row(1)]
        verdict = tune.compare_rows(left, right)
        self.assertTrue(verdict["same_multiset"])
        self.assertFalse(verdict["same_order"])

    def test_float_sums_that_differ_in_the_last_bits_match(self):
        left = [[{"f": 2538563.0450000018}]]
        right = [[{"f": 2538563.0449999995}]]
        self.assertTrue(tune.compare_rows(left, right)["same_multiset"])

    def test_a_real_float_difference_does_not_match(self):
        left = [[{"f": 2538563.04}]]
        right = [[{"f": 2538564.04}]]
        self.assertFalse(tune.compare_rows(left, right)["same_multiset"])


class Objective(unittest.TestCase):
    def test_defaults_score_one(self):
        totals = {"nostats": 56.6, "analyzed": 57.2}
        self.assertAlmostEqual(tune.objective(totals, totals), 1.0)

    def test_both_regimes_weigh_the_same(self):
        baseline = {"nostats": 100.0, "analyzed": 100.0}
        half_of_one = {"nostats": 50.0, "analyzed": 100.0}
        half_of_other = {"nostats": 100.0, "analyzed": 50.0}
        self.assertAlmostEqual(
            tune.objective(half_of_one, baseline),
            tune.objective(half_of_other, baseline),
        )
        self.assertAlmostEqual(tune.objective(half_of_one, baseline), 0.75)


if __name__ == "__main__":
    unittest.main()
