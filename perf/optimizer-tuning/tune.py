#!/usr/bin/env python3
"""Tune Turso's optimizer cost parameters against the bundled TPC-H workload.

The driver measures execution time, not estimated cost. It looks for one global
parameter vector that lowers the total runtime of the whole query set in both
statistics regimes at the same time.

Commands:

    baseline   Measure the compiled defaults and freeze the workload manifest.
    probe      Move one parameter at a time, and groups of them, to find which
               ones change a plan at all.
    search     Draw candidate vectors in log space and score them.
    validate   Re-measure the defaults and the finalists in fresh paired rounds.
    report     Print what the run recorded.

Every measurement runs in its own process with TURSO_OPTIMIZER_PARAMS already
set, because the engine caches its parameter file once per process.
"""

import argparse
import json
import math
import os
import random
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cost_vector
import runner

REGIMES = ("nostats", "analyzed")
SEARCH_WARMUPS = 1
SEARCH_REPETITIONS = 3
MIN_TIMEOUT_SECONDS = 30
TIMEOUT_BASELINE_MULTIPLE = 8


class Experiment:
    def __init__(self, out_dir):
        self.out_dir = os.path.abspath(out_dir)
        os.makedirs(self.out_dir, exist_ok=True)
        self.state_path = os.path.join(self.out_dir, "state.json")
        self.measurements_path = os.path.join(self.out_dir, "measurements.jsonl")
        self.candidates_path = os.path.join(self.out_dir, "candidates.jsonl")
        self.plans_path = os.path.join(self.out_dir, "plans.jsonl")
        self.state = {}
        if os.path.exists(self.state_path):
            self.state = json.load(open(self.state_path, encoding="utf-8"))
        self.measurements = {}
        self.seen_fingerprints = set()
        self._load_measurements()

    def save_state(self):
        temporary = self.state_path + ".partial"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(self.state, handle, indent=2, sort_keys=True)
        os.replace(temporary, self.state_path)

    def _load_measurements(self):
        if not os.path.exists(self.measurements_path):
            return
        for line in open(self.measurements_path, encoding="utf-8"):
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            self.measurements[self._key(record)] = record
        if os.path.exists(self.plans_path):
            for line in open(self.plans_path, encoding="utf-8"):
                if line.strip():
                    self.seen_fingerprints.add(json.loads(line)["fingerprint"])

    @staticmethod
    def _key(record):
        return (
            record["regime"],
            record["query"],
            record["fingerprint"],
            record["database_id"],
            record["build_id"],
            record["context"],
        )

    def append(self, path, record):
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def record_measurement(self, record):
        self.measurements[self._key(record)] = record
        self.append(self.measurements_path, record)

    def record_plan(self, record):
        if record["fingerprint"] in self.seen_fingerprints:
            return
        self.seen_fingerprints.add(record["fingerprint"])
        self.append(self.plans_path, record)


def median_seconds(samples):
    return statistics.median(s["elapsed_ns"] / 1e9 for s in samples)


def build_engine(experiment, args):
    databases = {
        "nostats": os.path.join(args.data_dir, "tpch-nostats.db"),
        "nostats_rw": os.path.join(args.data_dir, "tpch-nostats-rw.db"),
        "analyzed": os.path.join(args.data_dir, "tpch-analyzed.db"),
        "analyzed_rw": os.path.join(args.data_dir, "tpch-analyzed-rw.db"),
    }
    return runner.Engine(
        args.binary,
        databases,
        os.path.join(experiment.out_dir, "vectors"),
        os.path.join(experiment.out_dir, "logs"),
    )


def database_identity(engine):
    """Identify the data each regime runs on, by the first part of each file."""
    identity = {}
    for regime in REGIMES:
        path = engine.databases[regime]
        identity[regime] = {
            "path": path,
            "bytes": os.path.getsize(path),
            "sha256_head_64mib": runner.sha256_of_file(path, limit=64 << 20),
        }
    return identity


def collect_plans(experiment, engine, workload, vector_id, vector_path, query_names):
    """Get the plan of every named query in both regimes, in fresh processes."""
    fingerprints = {}
    for regime in REGIMES:
        records = engine.plans(regime, query_names, vector_path, workload)
        for name in query_names:
            if name not in records:
                raise RuntimeError(f"no plan for query {name} in regime {regime}")
            fingerprint, parts = runner.plan_fingerprint(records[name])
            fingerprints[(regime, name)] = fingerprint
            experiment.record_plan(
                {
                    "fingerprint": fingerprint,
                    "regime": regime,
                    "query": name,
                    "first_seen_vector": vector_id,
                    "plan_details": runner.structural_plan(records[name]),
                    "parts": parts,
                }
            )
    return fingerprints


def timeout_for(experiment, regime, query_name):
    baseline = experiment.state.get("baseline", {}).get(regime, {}).get(query_name)
    if not baseline:
        return MIN_TIMEOUT_SECONDS * 4
    return max(
        MIN_TIMEOUT_SECONDS,
        int(math.ceil(TIMEOUT_BASELINE_MULTIPLE * baseline["median_seconds"])),
    )


def measure_vector(experiment, engine, workload, vector_id, vector, query_names,
                   context, warmups, repetitions, force=False):
    """Score one vector. Reuse a measurement when the program is the same."""
    vector_path = engine.vector_path(vector_id, vector)
    effective = engine.effective_vector(vector_path)
    if not cost_vector.matches(effective, vector):
        raise RuntimeError(
            f"engine loaded a different vector for {vector_id}; "
            "the parameter file was rejected and defaults were used"
        )
    fingerprints = collect_plans(
        experiment, engine, workload, vector_id, vector_path, query_names
    )

    results = {}
    reused = 0
    for regime in REGIMES:
        results[regime] = {}
        for name in query_names:
            fingerprint = fingerprints[(regime, name)]
            key = (
                regime, name, fingerprint,
                experiment.state["database_id"][regime]["sha256_head_64mib"],
                experiment.state["build_id"], context,
            )
            if not force and key in experiment.measurements:
                results[regime][name] = experiment.measurements[key]
                reused += 1
                continue
            timeout = timeout_for(experiment, regime, name)
            outcome = engine.measure(
                regime, name, vector_path, workload, warmups, repetitions, timeout
            )
            record = {
                "regime": regime,
                "query": name,
                "fingerprint": fingerprint,
                "database_id": experiment.state["database_id"][regime][
                    "sha256_head_64mib"
                ],
                "build_id": experiment.state["build_id"],
                "context": context,
                "vector_id": vector_id,
                "warmups": warmups,
                "repetitions": repetitions,
                "timeout_seconds": timeout,
                "status": outcome["status"],
                "wall_seconds": outcome["wall_seconds"],
                "stderr_path": outcome["stderr_path"],
                "samples": outcome["samples"],
                "measured_at": time.time(),
            }
            if outcome["status"] == "ok":
                record["median_seconds"] = median_seconds(outcome["samples"])
                record["result_rows"] = outcome["samples"][0]["result_rows"]
                record["counters"] = {
                    k: outcome["samples"][0][k]
                    for k in ("rows_read", "vm_steps", "instructions",
                              "fullscan_steps", "index_steps", "btree_seeks",
                              "btree_table_seeks", "btree_index_seeks",
                              "btree_next", "sort_operations",
                              "hash_probe_calls", "hash_spill_bytes")
                    if k in outcome["samples"][0]
                }
            else:
                # A censored observation. The real time is at least the limit,
                # so the total below is a lower bound and the candidate is
                # marked so it can never become a finalist without a re-run.
                record["median_seconds"] = float(timeout)
                record["censored"] = True
            experiment.record_measurement(record)
            results[regime][name] = record
    return results, fingerprints, reused


def totals_of(results, query_names):
    totals, censored = {}, False
    for regime in REGIMES:
        total = 0.0
        for name in query_names:
            record = results[regime][name]
            total += record["median_seconds"]
            censored = censored or record.get("censored", False)
        totals[regime] = total
    return totals, censored


def objective(totals, baseline_totals):
    return sum(
        0.5 * totals[regime] / baseline_totals[regime] for regime in REGIMES
    )


def command_baseline(args):
    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = workload.names

    experiment.state.setdefault("created_at", time.time())
    experiment.state["workload"] = workload.manifest()
    experiment.state["build_id"] = engine.binary_sha256
    experiment.state["binary"] = engine.binary
    experiment.state["database_id"] = database_identity(engine)
    experiment.state["regimes"] = list(REGIMES)
    experiment.state["search_protocol"] = {
        "warmups": SEARCH_WARMUPS,
        "repetitions": SEARCH_REPETITIONS,
        "timeout_rule": f"max({MIN_TIMEOUT_SECONDS}s, "
                        f"{TIMEOUT_BASELINE_MULTIPLE}x baseline median)",
        "io_backend": "PlatformIO (UnixIO syscalls), warm operating-system cache",
    }
    experiment.save_state()

    defaults = cost_vector.defaults()
    results, _, _ = measure_vector(
        experiment, engine, workload, "defaults", defaults, names,
        context="search", warmups=SEARCH_WARMUPS, repetitions=SEARCH_REPETITIONS,
        force=args.force,
    )
    baseline = {
        regime: {
            name: {
                "median_seconds": results[regime][name]["median_seconds"],
                "result_rows": results[regime][name].get("result_rows"),
                "fingerprint": results[regime][name]["fingerprint"],
            }
            for name in names
        }
        for regime in REGIMES
    }
    totals, _ = totals_of(results, names)
    experiment.state["baseline"] = baseline
    experiment.state["baseline_totals"] = totals
    experiment.save_state()

    print(json.dumps({"baseline_totals": totals}, indent=2))
    for regime in REGIMES:
        for name in names:
            print(f"  {regime:9} Q{name:<3} "
                  f"{baseline[regime][name]['median_seconds']:8.3f}s")


def command_probe(args):
    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = workload.names
    defaults = cost_vector.defaults()
    baseline_totals = experiment.state["baseline_totals"]

    fields = list(cost_vector.COST_ONLY_FIELDS)
    if args.group in ("heuristics", "all"):
        fields += list(cost_vector.HEURISTIC_FIELDS)

    default_path = engine.vector_path("defaults", defaults)
    default_fingerprints = collect_plans(
        experiment, engine, workload, "defaults", default_path, names
    )

    probes = []
    for field in fields:
        for factor in (0.1, 10.0):
            vector = dict(defaults)
            value = defaults[field] * factor
            if field == "cache_reuse_factor":
                value = min(value, cost_vector.MAX_CACHE_REUSE_FACTOR)
            if field == "index_bonus":
                value = min(value, cost_vector.MAX_INDEX_BONUS)
            if field in cost_vector.SELECTIVITY_FIELDS or \
                    field == "closed_range_selectivity_factor":
                value = min(value, 1.0)
            if field == "rows_per_table_page":
                value = max(value, 2.0)
            vector[field] = value
            if vector[field] == defaults[field]:
                continue
            if vector["sel_eq_indexed"] > vector["sel_eq_unindexed"]:
                vector["sel_eq_indexed"] = vector["sel_eq_unindexed"]
            probes.append((f"probe-{field}-x{factor}", vector))

    probes.append(("probe-group-all-cpu-x10", {
        **defaults,
        **{f: defaults[f] * 10 for f in cost_vector.POSITIVE_WEIGHTS},
    }))
    probes.append(("probe-group-all-cpu-x0.1", {
        **defaults,
        **{f: defaults[f] * 0.1 for f in cost_vector.POSITIVE_WEIGHTS},
    }))

    print(f"{len(probes)} probes; plan-only sensitivity first")
    changed = []
    for vector_id, vector in probes:
        path = engine.vector_path(vector_id, vector)
        effective = engine.effective_vector(path)
        if not cost_vector.matches(effective, vector):
            raise RuntimeError(f"{vector_id}: engine did not load the vector")
        fingerprints = collect_plans(
            experiment, engine, workload, vector_id, path, names
        )
        differing = sorted(
            {f"{r}/Q{n}" for (r, n), f in fingerprints.items()
             if f != default_fingerprints[(r, n)]},
            key=lambda s: (s.split("/")[0], int(s.split("Q")[1])),
        )
        print(f"  {vector_id:44} plans changed: {len(differing):2}  {differing}")
        if differing:
            changed.append(vector_id)
        experiment.append(
            os.path.join(experiment.out_dir, "probes.jsonl"),
            {"vector_id": vector_id, "vector": vector,
             "changed_plans": differing, "changed_count": len(differing)},
        )

    print(f"\n{len(changed)} of {len(probes)} probes changed at least one plan")
    if not args.measure:
        return
    print("measuring the probes that changed a plan")
    for vector_id, vector in probes:
        if vector_id not in changed:
            continue
        results, _, reused = measure_vector(
            experiment, engine, workload, vector_id, vector, names,
            context="search", warmups=SEARCH_WARMUPS,
            repetitions=SEARCH_REPETITIONS,
        )
        totals, censored = totals_of(results, names)
        score = objective(totals, baseline_totals)
        row = {"vector_id": vector_id, "vector": vector, "totals": totals,
               "objective": score, "censored": censored, "reused": reused,
               "kind": "probe"}
        experiment.append(experiment.candidates_path, row)
        print(f"  {vector_id:44} J={score:.4f} "
              f"nostats={totals['nostats']:.1f}s analyzed={totals['analyzed']:.1f}s"
              f"{' CENSORED' if censored else ''}")


def command_search(args):
    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = workload.names
    baseline_totals = experiment.state["baseline_totals"]
    rng = random.Random(args.seed)

    done = set()
    if os.path.exists(experiment.candidates_path):
        for line in open(experiment.candidates_path, encoding="utf-8"):
            if line.strip():
                done.add(json.loads(line)["vector_id"])

    base = cost_vector.defaults()
    if args.base:
        base = cost_vector.read(args.base)

    started = time.time()
    for index in range(args.candidates):
        vector_id = f"{args.tag}-s{args.seed}-{index:03d}"
        if vector_id in done:
            continue
        if args.group == "cost":
            vector = cost_vector.sample_cost_only(rng, base, span=args.span)
        elif args.group == "heuristics":
            vector = cost_vector.sample_heuristics(rng, base, span=args.span)
        elif args.group == "local":
            fields = {
                "cost": cost_vector.COST_ONLY_FIELDS,
                "heuristics": cost_vector.HEURISTIC_FIELDS,
                "all": cost_vector.COST_ONLY_FIELDS + cost_vector.HEURISTIC_FIELDS,
            }[args.fields]
            vector = cost_vector.perturb(rng, base, fields, span=args.span)
        else:
            vector = cost_vector.sample_heuristics(
                rng, cost_vector.sample_cost_only(rng, base, span=args.span),
                span=args.span,
            )
        results, fingerprints, reused = measure_vector(
            experiment, engine, workload, vector_id, vector, names,
            context="search", warmups=SEARCH_WARMUPS,
            repetitions=SEARCH_REPETITIONS,
        )
        totals, censored = totals_of(results, names)
        score = objective(totals, baseline_totals)
        row = {"vector_id": vector_id, "vector": vector, "totals": totals,
               "objective": score, "censored": censored, "reused": reused,
               "kind": args.group, "seed": args.seed,
               "fingerprints": {f"{regime}/{query}": fingerprint
                                for (regime, query), fingerprint
                                in fingerprints.items()}}
        experiment.append(experiment.candidates_path, row)
        elapsed = time.time() - started
        print(f"[{index + 1}/{args.candidates} {elapsed / 60:6.1f}min] "
              f"{vector_id} J={score:.4f} "
              f"nostats={totals['nostats']:7.1f}s "
              f"analyzed={totals['analyzed']:7.1f}s reused={reused}/44"
              f"{' CENSORED' if censored else ''}", flush=True)
        if args.budget_minutes and elapsed > args.budget_minutes * 60:
            print("budget reached")
            break


def load_named_vectors(paths):
    """Read the vectors to compare, with the compiled defaults always first."""
    configurations = [("defaults", cost_vector.defaults())]
    for path in paths or []:
        name = os.path.splitext(os.path.basename(path))[0]
        configurations.append((name, cost_vector.read(path)))
    return configurations


def command_validate(args):
    """Re-measure every configuration in fresh paired rounds.

    Each round runs the whole query set for every configuration. The order of
    the configurations turns around on every other round, so a slow machine at
    one moment cannot favour one side.
    """
    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = workload.names
    baseline_totals = experiment.state["baseline_totals"]
    configurations = load_named_vectors(args.vectors)
    validation_path = os.path.join(experiment.out_dir, "validation.jsonl")

    done = set()
    if os.path.exists(validation_path):
        for line in open(validation_path, encoding="utf-8"):
            if line.strip():
                row = json.loads(line)
                done.add((row["config"], row["round"]))

    for round_index in range(args.rounds):
        order = configurations if round_index % 2 == 0 else list(reversed(configurations))
        for name, vector in order:
            if (name, round_index) in done:
                continue
            context = f"validate-{name}-r{round_index}"
            results, fingerprints, _ = measure_vector(
                experiment, engine, workload, f"validate-{name}", vector, names,
                context=context, warmups=args.warmups,
                repetitions=args.repetitions, force=True,
            )
            totals, censored = totals_of(results, names)
            row = {
                "config": name,
                "round": round_index,
                "totals": totals,
                "objective": objective(totals, baseline_totals),
                "censored": censored,
                "per_query": {
                    regime: {
                        q: {
                            "median_seconds": results[regime][q]["median_seconds"],
                            "status": results[regime][q]["status"],
                            "result_rows": results[regime][q].get("result_rows"),
                            "fingerprint": results[regime][q]["fingerprint"],
                            "samples": [
                                sample["elapsed_ns"] / 1e9
                                for sample in results[regime][q]["samples"]
                            ],
                            "counters": results[regime][q].get("counters", {}),
                        }
                        for q in names
                    }
                    for regime in REGIMES
                },
                "measured_at": time.time(),
            }
            experiment.append(validation_path, row)
            print(f"round {round_index} {name:24} "
                  f"nostats={totals['nostats']:7.2f}s "
                  f"analyzed={totals['analyzed']:7.2f}s "
                  f"J={row['objective']:.4f}"
                  f"{' CENSORED' if censored else ''}", flush=True)


ROW_RELATIVE_TOLERANCE = 1e-9
ROW_ABSOLUTE_TOLERANCE = 1e-6


def row_shape(row):
    """The part of a result row that must match exactly.

    Every float becomes a placeholder, because two plans can add the same values
    in a different order and end with a slightly different sum. Everything else
    keeps its own type tag, so a NULL never equals an empty string and an
    integer never equals a float that prints the same.
    """
    shape = []
    for value in row:
        if value is None:
            shape.append(("null",))
        elif "i" in value:
            shape.append(("int", value["i"]))
        elif "f" in value:
            shape.append(("float",))
        elif "t" in value:
            shape.append(("text", value["t"]))
        else:
            shape.append(("blob", value["b"]))
    return tuple(shape)


def row_floats(row):
    return tuple(v["f"] for v in row if v is not None and "f" in v)


def floats_match(left, right):
    return all(
        abs(a - b) <= max(ROW_ABSOLUTE_TOLERANCE,
                          ROW_RELATIVE_TOLERANCE * max(abs(a), abs(b)))
        for a, b in zip(left, right)
    )


def compare_rows(expected, actual):
    """Compare two result sets with multiplicity, and report order separately.

    Rows are grouped by the part that must match exactly. Inside a group the
    float tuples are sorted and compared one by one with a fixed tolerance, so
    the comparison keeps multiplicity without depending on the row order.
    """
    from collections import defaultdict

    def grouped(rows):
        groups = defaultdict(list)
        for row in rows:
            groups[row_shape(row)].append(row_floats(row))
        for key in groups:
            groups[key].sort()
        return groups

    expected_groups, actual_groups = grouped(expected), grouped(actual)
    same = set(expected_groups) == set(actual_groups)
    if same:
        for key, left in expected_groups.items():
            right = actual_groups[key]
            if len(left) != len(right) or not all(
                floats_match(a, b) for a, b in zip(left, right)
            ):
                same = False
                break

    missing = sorted(set(expected_groups) - set(actual_groups))[:5]
    extra = sorted(set(actual_groups) - set(expected_groups))[:5]
    expected_order = [row_shape(r) for r in expected]
    actual_order = [row_shape(r) for r in actual]
    return {
        "row_count_expected": len(expected),
        "row_count_actual": len(actual),
        "same_multiset": same,
        "same_order": expected_order == actual_order,
        "missing": [[list(part) for part in key] for key in missing],
        "extra": [[list(part) for part in key] for key in extra],
    }


def command_correctness(args):
    """Check every plan a candidate introduces, without timing it.

    Only a query whose program differs from the default program needs checking,
    because an identical program cannot return a different answer.
    """
    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = workload.names
    configurations = load_named_vectors(args.vectors)
    out_path = os.path.join(experiment.out_dir, "correctness.jsonl")

    default_path = engine.vector_path("defaults", cost_vector.defaults())
    default_fingerprints = collect_plans(
        experiment, engine, workload, "defaults", default_path, names
    )
    expected = {}
    failures = 0
    for name, vector in configurations[1:]:
        path = engine.vector_path(f"correctness-{name}", vector)
        effective = engine.effective_vector(path)
        if not cost_vector.matches(effective, vector):
            raise RuntimeError(f"{name}: engine did not load the vector")
        fingerprints = collect_plans(
            experiment, engine, workload, f"correctness-{name}", path, names
        )
        for regime in REGIMES:
            for query in names:
                if fingerprints[(regime, query)] == default_fingerprints[(regime, query)]:
                    continue
                if (regime, query) not in expected:
                    expected[(regime, query)] = engine.rows(
                        regime, query, default_path, workload, args.timeout_seconds
                    )
                actual = engine.rows(
                    regime, query, path, workload, args.timeout_seconds
                )
                verdict = compare_rows(expected[(regime, query)], actual)
                verdict.update({"config": name, "regime": regime, "query": query,
                                "fingerprint": fingerprints[(regime, query)]})
                experiment.append(out_path, verdict)
                mark = "ok" if verdict["same_multiset"] else "MISMATCH"
                if not verdict["same_multiset"]:
                    failures += 1
                order_note = "" if verdict["same_order"] else " (order differs)"
                print(f"  {name:20} {regime:9} Q{query:<3} {mark}"
                      f" rows={verdict['row_count_actual']}{order_note}", flush=True)
    print(f"{failures} mismatches")
    return 1 if failures else 0


def command_evaluate(args):
    """Score named vectors with the search protocol, for hybrids and ablations."""
    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = workload.names
    baseline_totals = experiment.state["baseline_totals"]

    done = set()
    if os.path.exists(experiment.candidates_path):
        for line in open(experiment.candidates_path, encoding="utf-8"):
            if line.strip():
                done.add(json.loads(line)["vector_id"])

    for path in args.vectors:
        vector_id = os.path.splitext(os.path.basename(path))[0]
        if vector_id in done and not args.force:
            print(f"{vector_id} already scored")
            continue
        vector = cost_vector.read(path)
        results, fingerprints, reused = measure_vector(
            experiment, engine, workload, vector_id, vector, names,
            context="search", warmups=SEARCH_WARMUPS,
            repetitions=SEARCH_REPETITIONS,
        )
        totals, censored = totals_of(results, names)
        score = objective(totals, baseline_totals)
        experiment.append(experiment.candidates_path, {
            "vector_id": vector_id, "vector": vector, "totals": totals,
            "objective": score, "censored": censored, "reused": reused,
            "kind": args.kind,
            "fingerprints": {f"{regime}/{query}": fingerprint
                             for (regime, query), fingerprint in fingerprints.items()},
        })
        print(f"{vector_id:38} J={score:.4f} "
              f"nostats={totals['nostats']:7.2f}s analyzed={totals['analyzed']:7.2f}s "
              f"reused={reused}/44{' CENSORED' if censored else ''}", flush=True)


def program_time_map(experiment):
    """Map (regime, query, program) to the time that program took.

    A censored entry keeps its timeout value and is marked, so a prediction that
    depends on it can never become a finalist without a real measurement.
    """
    times = {}
    for record in experiment.measurements.values():
        if record["context"] != "search":
            continue
        times[(record["regime"], record["query"], record["fingerprint"])] = (
            record["median_seconds"], record.get("censored", False)
        )
    return times


def command_sweep(args):
    """Draw many vectors, collect their plans only, and predict the objective.

    Collecting a plan does not execute the query, so this pass can use every
    core. It never measures a runtime, so the parallelism cannot disturb a
    timing. A vector whose programs were all measured before gets a predicted
    objective for free; a vector that introduces a new program is reported
    separately, because it is the one worth measuring next.

    The query file that creates a view is left out of the parallel pass: it
    needs a writable database, and several processes must not write to one
    file. Its two known programs differ by less than 0.06 s, so the prediction
    uses its default time and the real evaluation measures it.
    """
    import concurrent.futures

    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = [n for n in workload.names if n not in runner.WRITABLE_QUERIES]
    skipped = [n for n in workload.names if n in runner.WRITABLE_QUERIES]
    baseline = experiment.state["baseline"]
    baseline_totals = experiment.state["baseline_totals"]
    times = program_time_map(experiment)
    rng = random.Random(args.seed)

    base = cost_vector.defaults()
    if args.base:
        base = cost_vector.read(args.base)

    vectors = []
    for index in range(args.candidates):
        vector_id = f"{args.tag}-s{args.seed}-{index:04d}"
        if args.group == "cost":
            vector = cost_vector.sample_cost_only(rng, base, span=args.span)
        elif args.group == "heuristics":
            vector = cost_vector.sample_heuristics(rng, base, span=args.span)
        elif args.group == "local":
            vector = cost_vector.perturb(
                rng, base,
                cost_vector.COST_ONLY_FIELDS + cost_vector.HEURISTIC_FIELDS,
                span=args.span,
            )
        else:
            vector = cost_vector.sample_heuristics(
                rng, cost_vector.sample_cost_only(rng, base, span=args.span),
                span=args.span,
            )
        vectors.append((vector_id, vector))

    def plans_of(item):
        vector_id, vector = item
        path = engine.vector_path(vector_id, vector)
        effective = engine.effective_vector(path)
        if not cost_vector.matches(effective, vector):
            raise RuntimeError(f"{vector_id}: engine did not load the vector")
        found = {}
        for regime in REGIMES:
            records = engine.plans(regime, names, path, workload)
            for name in names:
                found[(regime, name)] = runner.plan_fingerprint(records[name])[0]
        return vector_id, vector, found

    sweep_path = os.path.join(experiment.out_dir, "sweep.jsonl")
    known, unknown = [], []
    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for position, (vector_id, vector, found) in enumerate(
            pool.map(plans_of, vectors)
        ):
            totals, censored, missing = {}, False, 0
            for regime in REGIMES:
                total = 0.0
                for name in workload.names:
                    if name in skipped:
                        total += baseline[regime][name]["median_seconds"]
                        continue
                    entry = times.get((regime, name, found[(regime, name)]))
                    if entry is None:
                        missing += 1
                        continue
                    total += entry[0]
                    censored = censored or entry[1]
                totals[regime] = total
            row = {
                "vector_id": vector_id, "vector": vector, "missing_programs": missing,
                "censored": censored,
                "predicted_totals": totals if not missing else None,
                "predicted_objective": (
                    objective(totals, baseline_totals) if not missing else None
                ),
                "programs": {f"{r}/{q}": f for (r, q), f in found.items()},
            }
            experiment.append(sweep_path, row)
            (unknown if missing else known).append(row)
            if (position + 1) % 100 == 0:
                print(f"  {position + 1}/{len(vectors)} "
                      f"({time.time() - started:.0f}s)", flush=True)

    known.sort(key=lambda r: r["predicted_objective"])
    print(f"\n{len(known)} vectors fully predicted, "
          f"{len(unknown)} introduce a program that was never measured")
    print("best predicted (needs a real measurement to count):")
    for row in known[: args.top]:
        print(f"  {row['predicted_objective']:.4f} {row['vector_id']:22} "
              f"nostats={row['predicted_totals']['nostats']:7.2f}s "
              f"analyzed={row['predicted_totals']['analyzed']:7.2f}s"
              f"{' CENSORED' if row['censored'] else ''}")

    if args.write_top:
        os.makedirs(args.write_top, exist_ok=True)
        written = 0
        chosen = [r for r in known if not r["censored"]][: args.top]
        seen_programs = set()
        for row in chosen:
            key = tuple(sorted(row["programs"].items()))
            if key in seen_programs:
                continue
            seen_programs.add(key)
            cost_vector.write(
                os.path.join(args.write_top, f"{row['vector_id']}.json"),
                row["vector"],
            )
            written += 1
        # A vector that introduces a new program is where an unmeasured gain
        # could hide, so keep a few of those as well.
        for row in unknown[: args.write_unknown]:
            cost_vector.write(
                os.path.join(args.write_top, f"{row['vector_id']}.json"),
                row["vector"],
            )
            written += 1
        print(f"wrote {written} vectors to {args.write_top}")


def programs_of(engine, workload, names, vector_id, vector):
    """Collect the program of every query in both regimes for one vector."""
    path = engine.vector_path(vector_id, vector)
    effective = engine.effective_vector(path)
    if not cost_vector.matches(effective, vector):
        raise RuntimeError(f"{vector_id}: engine did not load the vector")
    found = {}
    for regime in REGIMES:
        records = engine.plans(regime, names, path, workload)
        for name in names:
            found[(regime, name)] = runner.plan_fingerprint(records[name])[0]
    return found


def command_minimize(args):
    """Strip a winning vector down to the changes that carry it.

    A random search moves every field, including the ones that change nothing.
    This pass puts each field back to its default and keeps the change only
    when the engine still picks the same program for every query in both
    regimes. What survives is the smallest change that selects the same work,
    and it is what a production default change should carry.
    """
    experiment = Experiment(args.out_dir)
    engine = build_engine(experiment, args)
    workload = runner.Workload(args.query_dir)
    names = workload.names
    defaults = cost_vector.defaults()

    vector = cost_vector.read(args.vector)
    target = programs_of(engine, workload, names, "minimize-target", vector)
    print(f"target selects {len({*target.values()})} distinct programs "
          f"over {len(target)} query/regime pairs")

    def keeps_target(trial, tag):
        try:
            return programs_of(engine, workload, names, tag, trial) == target
        except (RuntimeError, ValueError):
            return False

    current = dict(vector)
    changed = [f for f in cost_vector.FIELD_NAMES
               if abs(current[f] - defaults[f]) > 1e-12]
    print(f"vector starts with {len(changed)} changed fields")

    improved = True
    step = 0
    while improved:
        improved = False
        for field in list(changed):
            trial = dict(current)
            trial[field] = defaults[field]
            if trial["sel_eq_indexed"] > trial["sel_eq_unindexed"]:
                continue
            step += 1
            if keeps_target(trial, f"minimize-drop-{step}"):
                current = trial
                changed.remove(field)
                improved = True
                print(f"  reverted {field} to its default")

    print(f"{len(changed)} fields still needed: {changed}")

    # Round what is left. A default that reads as a considered number is easier
    # to reason about than one that carries a random search's digits.
    for field in list(changed):
        value = current[field]
        for digits in (1, 2, 3):
            rounded = round_significant(value, digits)
            if rounded == value:
                continue
            trial = dict(current)
            trial[field] = rounded
            try:
                cost_vector.validate(trial)
            except cost_vector.VectorError:
                continue
            step += 1
            if keeps_target(trial, f"minimize-round-{step}"):
                print(f"  rounded {field} {value:.6g} -> {rounded:.6g}")
                current = trial
                break

    final = cost_vector.validate(current)
    if not keeps_target(final, "minimize-final"):
        raise RuntimeError("the minimized vector no longer selects the same programs")
    cost_vector.write(args.out, final)
    print(f"\nwrote {args.out}")
    for field in cost_vector.FIELD_NAMES:
        if abs(final[field] - defaults[field]) > 1e-12:
            print(f"  {field:34} {defaults[field]:>12.6g} -> {final[field]:<14.6g}"
                  f" (x{final[field] / defaults[field]:.3f})")


def round_significant(value, digits):
    if value == 0.0:
        return 0.0
    exponent = math.floor(math.log10(abs(value)))
    return round(value, -(exponent - digits + 1))


def command_report(args):
    experiment = Experiment(args.out_dir)
    rows = []
    if os.path.exists(experiment.candidates_path):
        for line in open(experiment.candidates_path, encoding="utf-8"):
            if line.strip():
                rows.append(json.loads(line))
    rows.sort(key=lambda r: r["objective"])
    print(f"baseline totals: {experiment.state.get('baseline_totals')}")
    print(f"{len(rows)} scored candidates, "
          f"{len(experiment.measurements)} cached measurements, "
          f"{len(experiment.seen_fingerprints)} distinct programs")
    for row in rows[: args.top]:
        print(f"  {row['objective']:.4f} {row['vector_id']:28} "
              f"nostats={row['totals']['nostats']:7.1f}s "
              f"analyzed={row['totals']['analyzed']:7.1f}s"
              f"{' CENSORED' if row.get('censored') else ''}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--binary", default="target/release/turso-join-benchmark")
    parser.add_argument("--query-dir", default="perf/tpc-h/queries")
    parser.add_argument("--data-dir", required=True)
    sub = parser.add_subparsers(dest="command", required=True)

    one = sub.add_parser("baseline")
    one.add_argument("--force", action="store_true")
    one.set_defaults(func=command_baseline)

    two = sub.add_parser("probe")
    two.add_argument("--group", default="cost",
                     choices=["cost", "heuristics", "all"])
    two.add_argument("--measure", action="store_true")
    two.set_defaults(func=command_probe)

    three = sub.add_parser("search")
    three.add_argument("--seed", type=int, default=1)
    three.add_argument("--candidates", type=int, default=64)
    three.add_argument("--group", default="cost",
                       choices=["cost", "heuristics", "local", "all"])
    three.add_argument("--span", type=float, default=10.0)
    three.add_argument("--base")
    three.add_argument("--tag", default="cost")
    three.add_argument("--fields", default="all",
                       choices=["cost", "heuristics", "all"])
    three.add_argument("--budget-minutes", type=float)
    three.set_defaults(func=command_search)

    five = sub.add_parser("validate")
    five.add_argument("--vectors", nargs="*", default=[])
    five.add_argument("--rounds", type=int, default=5)
    five.add_argument("--warmups", type=int, default=SEARCH_WARMUPS)
    five.add_argument("--repetitions", type=int, default=SEARCH_REPETITIONS)
    five.set_defaults(func=command_validate)

    six = sub.add_parser("correctness")
    six.add_argument("--vectors", nargs="*", default=[])
    six.add_argument("--timeout-seconds", type=int, default=600)
    six.set_defaults(func=command_correctness)

    seven = sub.add_parser("evaluate")
    seven.add_argument("--vectors", nargs="+", required=True)
    seven.add_argument("--kind", default="hybrid")
    seven.add_argument("--force", action="store_true")
    seven.set_defaults(func=command_evaluate)

    eight = sub.add_parser("sweep")
    eight.add_argument("--seed", type=int, default=11)
    eight.add_argument("--candidates", type=int, default=1000)
    eight.add_argument("--group", default="all",
                       choices=["cost", "heuristics", "local", "all"])
    eight.add_argument("--span", type=float, default=10.0)
    eight.add_argument("--base")
    eight.add_argument("--tag", default="sweep")
    eight.add_argument("--workers", type=int, default=4)
    eight.add_argument("--top", type=int, default=20)
    eight.add_argument("--write-top")
    eight.add_argument("--write-unknown", type=int, default=0)
    eight.set_defaults(func=command_sweep)

    nine = sub.add_parser("minimize")
    nine.add_argument("--vector", required=True)
    nine.add_argument("--out", required=True)
    nine.set_defaults(func=command_minimize)

    four = sub.add_parser("report")
    four.add_argument("--top", type=int, default=20)
    four.set_defaults(func=command_report)

    args = parser.parse_args()
    sys.exit(args.func(args) or 0)


if __name__ == "__main__":
    main()
