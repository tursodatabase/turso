#!/usr/bin/env python3
"""Tune `CostModelParams` against TPC-H and ClickBench with a learned model.

The tuner runs a sequential model-based search. A random forest learns how a
parameter set maps to workload runtime, an expected-improvement rule picks the
next parameter sets to try, and the measurements feed the forest again.

Two facts make the search cheap:

1. Cost parameters change runtime only when they change the bytecode a query
   compiles to. The tuner hashes the bytecode of every query, so it measures
   each distinct plan one time and reads the rest out of a cache.
2. A parameter set that compiles every query to bytecode already in the cache
   costs one EXPLAIN pass and no measurement at all.

Commands:
    screen    Report which parameters can change a plan at all.
    search    Run the model-based search.
    shortlist Re-read every parameter set the search tried, and pick one.
    grid      Measure every combination of a few round values per parameter.
    shrink    Move a parameter set back toward the defaults, plans unchanged.
    polish    Round a parameter set while every plan stays the same.
    evaluate  Measure one parameter file and print per-query times.
"""

import argparse
import itertools
import json
import math
import os
import sys
import time

import numpy as np
from sklearn.ensemble import RandomForestRegressor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bench
import space

NOISE_FLOOR_MS = 25.0
TIMEOUT_PENALTY = 8.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "screen",
            "search",
            "shortlist",
            "grid",
            "shrink",
            "polish",
            "evaluate",
        ],
    )
    parser.add_argument("--binary", default=bench.DEFAULT_BINARY)
    parser.add_argument("--suites", default="tpch,clickbench")
    parser.add_argument("--cache", default="/tmp/turso-tune-cache.json")
    parser.add_argument("--params-file", default="/tmp/turso-tune-params.json")
    parser.add_argument("--params", default=None)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--seed", type=int, default=20260917)
    parser.add_argument("--initial", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=30)
    parser.add_argument("--batch", type=int, default=4)
    parser.add_argument("--candidates", type=int, default=4000)
    parser.add_argument("--trees", type=int, default=200)
    parser.add_argument("--local-sigma", type=float, default=0.08)
    parser.add_argument("--active-params", default=None)
    parser.add_argument("--screen-bases", type=int, default=4)
    parser.add_argument("--screen-steps", type=int, default=7)
    parser.add_argument("--screen-out", default="/tmp/turso-screen.json")
    parser.add_argument("--history-out", default="/tmp/turso-history.json")
    parser.add_argument("--digits", type=int, default=3)
    parser.add_argument("--snap", type=float, default=0.1)
    parser.add_argument("--shrink-steps", type=int, default=14)
    parser.add_argument("--history", default="/tmp/turso-history.json")
    parser.add_argument("--shortlist-size", type=int, default=20)
    parser.add_argument("--max-regression", type=float, default=1.25)
    parser.add_argument("--tolerance", type=float, default=0.02)
    parser.add_argument("--values", default=None)
    parser.add_argument("--out", default="/tmp/turso-best-params.json")
    args = parser.parse_args()

    queries = bench.load_workload(tuple(args.suites.split(",")))
    evaluator = Evaluator(
        args.binary, queries, args.cache, args.repeats, args.timeout, args.params_file
    )
    commands = {
        "screen": cmd_screen,
        "search": cmd_search,
        "shortlist": cmd_shortlist,
        "grid": cmd_grid,
        "shrink": cmd_shrink,
        "polish": cmd_polish,
        "evaluate": cmd_evaluate,
    }
    commands[args.command](args, queries, evaluator)


def cmd_screen(args, queries, evaluator):
    """Report which parameters can change a plan, and which never do.

    Each parameter is swept over its range from the defaults and from a few
    random parameter sets, because a parameter can matter only in company.
    """
    rng = np.random.default_rng(args.seed)
    base_points = [space.to_unit(space.DEFAULTS)]
    base_points.extend(rng.random((args.screen_bases - 1, len(space.NAMES))))
    report = {}
    for index, name in enumerate(space.NAMES):
        per_base = []
        changed_queries = set()
        for base in base_points:
            signatures = []
            for position in np.linspace(0.02, 0.98, args.screen_steps):
                point = list(base)
                point[index] = position
                signatures.append(evaluator.signature(space.from_unit(point)))
            per_base.append(len({tuple(sorted(s.items())) for s in signatures}))
            for key in signatures[0]:
                if len({s[key] for s in signatures}) > 1:
                    changed_queries.add(key)
        report[name] = {
            "max_plans_per_base": max(per_base),
            "queries": sorted(changed_queries),
        }
        print(
            f"{name:34s} up to {max(per_base)} plans per sweep, "
            f"{len(changed_queries)} queries move",
            flush=True,
        )
    json.dump(report, open(args.screen_out, "w"), indent=2)
    print(f"\nwrote {args.screen_out}")


def cmd_search(args, queries, evaluator):
    baseline_times, _ = evaluator.times(space.DEFAULTS)
    live = {k: v for k, v in baseline_times.items() if v is not None}
    print(f"baseline: {len(live)} queries, {sum(live.values())/1000:.1f} s total", flush=True)
    evaluator.save()

    active = list(space.NAMES)
    if args.active_params:
        active = json.load(open(args.active_params))
        print(f"searching {len(active)} of {len(space.NAMES)} parameters", flush=True)
    active_index = [space.NAMES.index(name) for name in active]
    default_point = np.array(space.to_unit(space.DEFAULTS))

    def full_point(partial):
        point = default_point.copy()
        point[active_index] = partial
        return point

    history_x, history_y = [], []
    seen_signatures = {}

    def record(partial_point):
        params = space.from_unit(full_point(partial_point))
        signature = evaluator.signature(params)
        signature_key = json.dumps(sorted(signature.items()))
        if signature_key in seen_signatures:
            evaluator.cached_evaluations += 1
            score = seen_signatures[signature_key]
        else:
            times = {
                q.key: evaluator.query_time(q, signature[q.key], params) for q in queries
            }
            score = objective(times, baseline_times)
            seen_signatures[signature_key] = score
            evaluator.signature_cost[signature_key] = score
            evaluator.save()
        history_x.append(list(partial_point))
        history_y.append(score)
        return score

    started = time.time()
    baseline_score = record(default_point[active_index])
    print(f"default parameters score {baseline_score:.4f}", flush=True)

    for number, point in enumerate(sobol_points(args.initial, len(active_index), args.seed), 1):
        score = record(point)
        print(
            f"[init {number:3d}/{args.initial}] score {score:.4f} "
            f"best {min(history_y):.4f} plans measured {evaluator.measured_plans} "
            f"({time.time()-started:.0f}s)",
            flush=True,
        )

    rng = np.random.default_rng(args.seed + 1)
    for round_number in range(1, args.rounds + 1):
        forest = RandomForestRegressor(
            n_estimators=args.trees,
            min_samples_leaf=1,
            max_features=0.7,
            bootstrap=True,
            random_state=args.seed + round_number,
            n_jobs=-1,
        )
        forest.fit(np.array(history_x), np.array(history_y))

        best_so_far = min(history_y)
        best_point = np.array(history_x[int(np.argmin(history_y))])
        explore = rng.random((args.candidates, len(active_index)))
        refine = np.clip(
            best_point + rng.normal(0.0, args.local_sigma, (args.candidates, len(active_index))),
            0.0,
            1.0,
        )
        candidates = np.vstack([explore, refine])
        mean, sigma = forest_predict(forest, candidates)
        picks = np.argsort(-expected_improvement(mean, sigma, best_so_far))[: args.batch]
        for pick in picks:
            record(candidates[pick])
        print(
            f"[round {round_number:3d}/{args.rounds}] best {min(history_y):.4f} "
            f"plans measured {evaluator.measured_plans} "
            f"free evaluations {evaluator.cached_evaluations} "
            f"({time.time()-started:.0f}s)",
            flush=True,
        )
        json.dump(
            {
                "active": active,
                "history_x": history_x,
                "history_y": history_y,
                "baseline_score": baseline_score,
            },
            open(args.history_out, "w"),
        )

    best_params = space.from_unit(full_point(np.array(history_x[int(np.argmin(history_y))])))
    json.dump(best_params, open(args.out, "w"), indent=2, sort_keys=True)
    print(f"\nbest score {min(history_y):.4f} (default {baseline_score:.4f})")
    print(f"wrote {args.out}")


def cmd_shortlist(args, queries, evaluator):
    """Score every parameter set the search tried, and pick one by two rules.

    The search minimizes one number, so it accepts a large loss on one query
    when other queries gain more. That trade is not always the one to ship. The
    plan cache holds a time for every plan already measured, so each parameter
    set in the history can be scored again for the cost of one EXPLAIN pass,
    with no new measurement.

    Three rules pick the winner, in order. Drop every parameter set where one
    query is more than `--max-regression` times slower than it is with the
    current defaults. Of what is left, keep the sets within `--tolerance` of
    the lowest total runtime. Of those, take the one that moves the parameters
    least, measured as the distance from the defaults in the unit cube.

    The last rule matters as much as the first two. Runtime only tells the
    search which side of a plan flip a value is on, so several very different
    parameter sets reach the same runtime, and the search has no reason to
    prefer the moderate one. This rule states that preference.
    """
    baseline_times, _ = evaluator.times(space.DEFAULTS)
    default_point = np.array(space.to_unit(space.DEFAULTS))
    rows = []
    seen = set()
    for path in args.history.split(","):
        history = json.load(open(path))
        active_index = [space.NAMES.index(name) for name in history["active"]]
        for partial in history["history_x"]:
            point = default_point.copy()
            point[active_index] = partial
            params = space.from_unit(point)
            key = json.dumps(params, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            rows.append(score_from_cache(evaluator, queries, params, baseline_times))
    rows = [row for row in rows if row is not None]
    rows.sort(key=lambda row: row["total_ms"])

    print(f"{'total':>10s} {'score':>7s} {'worst':>7s}  parameters")
    for row in rows[: args.shortlist_size]:
        marker = " " if row["worst_ratio"] <= args.max_regression else "x"
        print(
            f"{row['total_ms']/1000:9.1f}s {row['score']:7.4f} "
            f"{row['worst_ratio']:6.2f}x {marker} {row['worst_query']}"
        )
    allowed = [row for row in rows if row["worst_ratio"] <= args.max_regression]
    if not allowed:
        sys.exit(f"no parameter set keeps every query under {args.max_regression}x")
    limit = allowed[0]["total_ms"] * (1.0 + args.tolerance)
    as_good = [row for row in allowed if row["total_ms"] <= limit]
    for row in as_good:
        row["distance"] = float(
            np.linalg.norm(
                np.array(space.to_unit(row["params"])) - np.array(space.to_unit(space.DEFAULTS))
            )
        )
    picked = min(as_good, key=lambda row: row["distance"])
    json.dump(picked["params"], open(args.out, "w"), indent=2, sort_keys=True)
    print(
        f"\n{len(as_good)} of {len(allowed)} sets are within {args.tolerance:.0%} of "
        f"{allowed[0]['total_ms']/1000:.1f} s and change no query more than "
        f"{args.max_regression:.2f}x"
    )
    print(
        f"picked total {picked['total_ms']/1000:.1f} s, worst query "
        f"{picked['worst_query']} at {picked['worst_ratio']:.2f}x, "
        f"distance from the defaults {picked['distance']:.3f}"
    )
    print(f"wrote {args.out}")


def score_from_cache(evaluator, queries, params, baseline_times):
    """Return the runtime of one parameter set, out of already-measured plans."""
    signature = evaluator.signature(params)
    times = {}
    for query in queries:
        cache_key = f"{query.key}|{signature[query.key]}"
        if cache_key not in evaluator.plan_times:
            return None
        times[query.key] = evaluator.plan_times[cache_key]
    worst_ratio, worst_query = 0.0, None
    total = 0.0
    for key, base in baseline_times.items():
        if base is None:
            continue
        measured = times.get(key)
        ratio = TIMEOUT_PENALTY if measured is None else (measured + NOISE_FLOOR_MS) / (
            base + NOISE_FLOOR_MS
        )
        total += base * TIMEOUT_PENALTY if measured is None else measured
        if ratio > worst_ratio:
            worst_ratio, worst_query = ratio, key
    return {
        "params": params,
        "score": objective(times, baseline_times),
        "total_ms": total,
        "worst_ratio": worst_ratio,
        "worst_query": worst_query,
    }


def cmd_grid(args, queries, evaluator):
    """Measure every combination of a handful of round values per parameter.

    The searches agree on which way a parameter should move but not on how far,
    and each one lands on a value with eight digits that no measurement can
    support. This step takes the directions they agree on, offers a few round
    values along each, and measures every combination. What it picks is a set
    of round numbers, each of which a reader can weigh against what the
    parameter is supposed to mean.

    `--values` names a JSON file, `{"parameter": [value, ...]}`. Every
    parameter it leaves out keeps its default.
    """
    choices = json.load(open(args.values))
    baseline_times, _ = evaluator.times(space.DEFAULTS)
    names = list(choices)
    rows = []
    for number, combination in enumerate(itertools.product(*(choices[n] for n in names)), 1):
        params = space.repair({**space.DEFAULTS, **dict(zip(names, combination))})
        signature = evaluator.signature(params)
        times = {
            q.key: evaluator.query_time(q, signature[q.key], params) for q in queries
        }
        evaluator.save()
        total = sum(
            baseline_times[key] * TIMEOUT_PENALTY if value is None else value
            for key, value in times.items()
            if baseline_times.get(key) is not None
        )
        worst_ratio, worst_query = 0.0, None
        for key, base in baseline_times.items():
            if base is None:
                continue
            measured = times.get(key)
            ratio = TIMEOUT_PENALTY if measured is None else (measured + NOISE_FLOOR_MS) / (
                base + NOISE_FLOOR_MS
            )
            if ratio > worst_ratio:
                worst_ratio, worst_query = ratio, key
        rows.append(
            {
                "params": params,
                "total_ms": total,
                "score": objective(times, baseline_times),
                "worst_ratio": worst_ratio,
                "worst_query": worst_query,
                "changed": sum(
                    1 for n in names if params[n] != space.DEFAULTS[n]
                ),
            }
        )
        print(
            f"[{number:4d}] total {total/1000:6.1f}s worst {worst_ratio:5.2f}x "
            f"{' '.join(f'{n}={params[n]:g}' for n in names)}",
            flush=True,
        )

    rows.sort(key=lambda row: (row["total_ms"], row["changed"]))
    allowed = [row for row in rows if row["worst_ratio"] <= args.max_regression]
    if not allowed:
        sys.exit(f"no combination keeps every query under {args.max_regression}x")
    limit = allowed[0]["total_ms"] * (1.0 + args.tolerance)
    as_good = [row for row in allowed if row["total_ms"] <= limit]
    picked = min(as_good, key=lambda row: row["changed"])
    print(
        f"\nbaseline {sum(v for v in baseline_times.values() if v is not None)/1000:.1f} s; "
        f"{len(as_good)} combinations within {args.tolerance:.0%} of "
        f"{allowed[0]['total_ms']/1000:.1f} s"
    )
    print(
        f"picked total {picked['total_ms']/1000:.1f} s, {picked['changed']} parameters "
        f"changed, worst query {picked['worst_query']} at {picked['worst_ratio']:.2f}x"
    )
    json.dump(picked["params"], open(args.out, "w"), indent=2, sort_keys=True)
    print(f"wrote {args.out}")


def cmd_shrink(args, queries, evaluator):
    """Move every parameter back toward its default, plans unchanged.

    The search reads runtime, and runtime only tells it which side of a plan
    flip a value is on. It has no reason to stop once it is past the flip, so
    it often reports a value at the edge of its range. Such a value is tuned to
    16 queries and says nothing true about a database in general.

    Two parameter sets that compile every query to the same bytecode run at the
    same speed. So each parameter moves back toward its default as far as the
    plans allow, largest change first. What is left is the smallest change to
    the shipped constants that buys the whole measured gain.
    """
    searched = space.repair({**space.DEFAULTS, **json.load(open(args.params))})
    target = evaluator.signature(searched)
    default_point = np.array(space.to_unit(space.DEFAULTS))
    searched_point = np.array(space.to_unit(searched))

    order = np.argsort(-np.abs(searched_point - default_point))
    point = searched_point.copy()
    for index in order:
        if abs(searched_point[index] - default_point[index]) < 1e-9:
            continue
        low, high = 0.0, 1.0
        for _ in range(args.shrink_steps):
            middle = (low + high) / 2
            trial = point.copy()
            trial[index] = default_point[index] + middle * (
                searched_point[index] - default_point[index]
            )
            if evaluator.signature(space.from_unit(trial)) == target:
                high = middle
            else:
                low = middle
        point[index] = default_point[index] + high * (
            searched_point[index] - default_point[index]
        )
        name = space.NAMES[index]
        kept = space.from_unit(point)[name]
        print(f"{name:34s} {searched[name]:14.6g} -> {kept:14.6g} ({high:.0%} of the move)")

    shrunk = space.from_unit(point)
    if evaluator.signature(shrunk) != target:
        sys.exit("the shrunk parameter set does not give the same plans")
    json.dump(shrunk, open(args.out, "w"), indent=2, sort_keys=True)
    print(f"\nwrote {args.out}")


def cmd_polish(args, queries, evaluator):
    """Cut a searched parameter set down to few digits, plan by plan.

    The search reports eight digits, which claims a precision the measurements
    do not have. The score only moves when a plan flips, so a rounded value is
    as good as the searched one as long as every query still compiles to the
    same bytecode. A parameter that does change a plan keeps more digits.
    """
    searched = space.repair({**space.DEFAULTS, **json.load(open(args.params))})
    target = evaluator.signature(searched)
    polished = dict(searched)
    for name in space.NAMES:
        if searched[name] == space.DEFAULTS[name]:
            continue
        for digits in range(args.digits, 9):
            candidate = dict(polished)
            candidate[name] = round_significant(searched[name], digits)
            if evaluator.signature(space.repair(candidate)) == target:
                polished = candidate
                print(f"{name:34s} {searched[name]} -> {polished[name]} ({digits} digits)")
                break
        else:
            print(f"{name:34s} {searched[name]} (no shorter value keeps the plans)")
    if evaluator.signature(space.repair(polished)) != target:
        sys.exit("the rounded parameter set does not give the same plans")

    # A parameter that ends within --snap of its default is winning a cost tie,
    # not stating a fact about databases. Put it back and report what moves.
    snapped = dict(polished)
    for name in space.NAMES:
        default = space.DEFAULTS[name]
        if polished[name] != default and abs(polished[name] - default) <= args.snap * abs(default):
            print(f"{name:34s} {polished[name]} -> {default} (within {args.snap:.0%} of the default)")
            snapped[name] = default
    snapped = space.repair(snapped)
    moved = [
        key for key, value in evaluator.signature(snapped).items() if value != target[key]
    ]
    if moved:
        print(f"\nputting those back changes the plan of: {', '.join(moved)}")

    json.dump(snapped, open(args.out, "w"), indent=2, sort_keys=True)
    print(f"\nwrote {args.out}")


def round_significant(value, digits):
    if value == 0.0:
        return 0.0
    exponent = math.floor(math.log10(abs(value)))
    return round(value, -(exponent - digits + 1))


def cmd_evaluate(args, queries, evaluator):
    params = space.DEFAULTS if args.params is None else json.load(open(args.params))
    params_path = bench.write_params(
        space.repair({**space.DEFAULTS, **params}), args.params_file
    )
    times = bench.measure(
        evaluator.binary, queries, params_path, repeats=args.repeats, timeout=args.timeout
    )
    for query in queries:
        value = times[query.key]
        print(f"{query.key:18s} {'FAIL' if value is None else f'{value:9.1f} ms'}")
    total = sum(value for value in times.values() if value is not None)
    print(f"total {total/1000:.2f} s")
    json.dump(times, open(args.out, "w"), indent=2)


class Evaluator:
    """Measure parameter sets, and remember every plan already measured."""

    def __init__(self, binary, queries, cache_path, repeats, timeout, params_path):
        self.binary = binary
        self.queries = queries
        self.cache_path = cache_path
        self.repeats = repeats
        self.timeout = timeout
        self.params_path = params_path
        self.plan_times = {}
        self.signature_cost = {}
        self.measured_plans = 0
        self.cached_evaluations = 0
        if cache_path and os.path.exists(cache_path):
            saved = json.load(open(cache_path))
            self.plan_times = saved.get("plan_times", {})
            self.signature_cost = saved.get("signature_cost", {})

    def times(self, params):
        signature = self.signature(params)
        return {
            query.key: self.query_time(query, signature[query.key], params)
            for query in self.queries
        }, signature

    def signature(self, params):
        bench.write_params(params, self.params_path)
        return bench.bytecode_signature(self.binary, self.queries, self.params_path)

    def query_time(self, query, plan_hash, params):
        """Return the runtime of one query, measuring it only when unseen."""
        cache_key = f"{query.key}|{plan_hash}"
        if cache_key in self.plan_times:
            return self.plan_times[cache_key]
        bench.write_params(params, self.params_path)
        measured = None
        if bench.run_once(self.binary, query, self.params_path, self.timeout) is not None:
            samples = []
            for _ in range(self.repeats):
                sample = bench.run_once(self.binary, query, self.params_path, self.timeout)
                if sample is None:
                    samples = []
                    break
                samples.append(sample)
            measured = min(samples) if samples else None
        self.plan_times[cache_key] = measured
        self.measured_plans += 1
        return measured

    def save(self):
        """Write the cache through a temporary file.

        The cache holds hours of measurement. Writing it in place loses all of
        it when the process stops during the write, so write a new file and
        rename it over the old one, which cannot leave a half-written cache.
        """
        if not self.cache_path:
            return
        temporary = f"{self.cache_path}.new"
        with open(temporary, "w") as handle:
            json.dump(
                {"plan_times": self.plan_times, "signature_cost": self.signature_cost},
                handle,
            )
        os.replace(temporary, self.cache_path)


def objective(times, baseline):
    """Return the geometric mean of the per-query runtime ratios.

    A geometric mean keeps one slow query from deciding the answer on its own.
    A plain sum of runtimes would let the slowest query outvote all the others.
    The addition of a fixed 25 ms stops a small wobble on a fast query from
    reading as a large regression.
    """
    logs = []
    for key, base in baseline.items():
        if base is None:
            continue
        measured = times.get(key)
        if measured is None:
            ratio = TIMEOUT_PENALTY
        else:
            ratio = (measured + NOISE_FLOOR_MS) / (base + NOISE_FLOOR_MS)
        logs.append(math.log(ratio))
    return math.exp(sum(logs) / len(logs))


def sobol_points(count, dimensions, seed):
    """Return `count` Sobol points, drawn in a batch of a power of two.

    A Sobol sequence spreads evenly only over such a batch, so draw the next
    power of two and keep the first `count` points.
    """
    from scipy.stats import qmc

    sampler = qmc.Sobol(d=dimensions, scramble=True, seed=seed)
    batch = 1 << math.ceil(math.log2(max(count, 1)))
    return sampler.random(batch)[:count]


def forest_predict(forest, points):
    """Return the mean prediction of the trees and how much they disagree."""
    per_tree = np.stack([tree.predict(points) for tree in forest.estimators_])
    return per_tree.mean(axis=0), per_tree.std(axis=0)


def expected_improvement(mean, sigma, best):
    sigma = np.maximum(sigma, 1e-9)
    z = (best - mean) / sigma
    cdf = 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))
    pdf = np.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)
    return (best - mean) * cdf + sigma * pdf


if __name__ == "__main__":
    main()
