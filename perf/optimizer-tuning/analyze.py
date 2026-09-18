#!/usr/bin/env python3
"""Read a tuning run and print what it measured.

Commands:

    top          Rank the scored candidates.
    detail       Per-query ratios, work counters and plan changes of one vector.
    validation   Paired statistics over the validation rounds.
"""

import argparse
import json
import math
import os
import random
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cost_vector

REGIMES = ("nostats", "analyzed")


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def load(out_dir):
    with open(os.path.join(out_dir, "state.json"), encoding="utf-8") as handle:
        state = json.load(handle)
    return {
        "state": state,
        "candidates": read_jsonl(os.path.join(out_dir, "candidates.jsonl")),
        "measurements": read_jsonl(os.path.join(out_dir, "measurements.jsonl")),
        "plans": read_jsonl(os.path.join(out_dir, "plans.jsonl")),
        "validation": read_jsonl(os.path.join(out_dir, "validation.jsonl")),
    }


def command_top(args):
    run = load(args.out_dir)
    rows = [r for r in run["candidates"] if not r.get("censored")]
    rows.sort(key=lambda r: r["objective"])
    groups = {}
    for row in run["candidates"]:
        groups.setdefault(row.get("kind", "?"), []).append(row["objective"])
    print(f"baseline totals: "
          f"nostats={run['state']['baseline_totals']['nostats']:.2f}s "
          f"analyzed={run['state']['baseline_totals']['analyzed']:.2f}s")
    print(f"{len(run['candidates'])} candidates, "
          f"{len(run['measurements'])} measurements, "
          f"{len({p['fingerprint'] for p in run['plans']})} distinct programs")
    for kind, scores in sorted(groups.items()):
        print(f"  group {kind:12} n={len(scores):3} "
              f"best={min(scores):.4f} median={statistics.median(scores):.4f} "
              f"worst={max(scores):.4f}")
    print()
    for row in rows[: args.top]:
        print(f"  {row['objective']:.4f} {row['vector_id']:20} "
              f"nostats={row['totals']['nostats']:7.2f}s "
              f"analyzed={row['totals']['analyzed']:7.2f}s "
              f"({row.get('kind','?')})")


def measurement_index(run):
    index = {}
    for record in run["measurements"]:
        index[(record["regime"], record["query"], record["fingerprint"],
               record["context"])] = record
    return index


def command_detail(args):
    """Show which queries a vector replans, and how its measured times compare.

    A vector that reuses another vector's measurement is recorded under that
    other vector, so the per-query times are found through the program the
    vector selects, not through its name.
    """
    run = load(args.out_dir)
    state = run["state"]
    names = state["workload"]["runnable"]
    baseline = state["baseline"]
    plans = {p["fingerprint"]: p for p in run["plans"]}
    by_program = {}
    for record in run["measurements"]:
        if record["context"] == "search":
            by_program[(record["regime"], record["query"],
                        record["fingerprint"])] = record

    candidate = next(
        (r for r in run["candidates"] if r["vector_id"] == args.vector_id), None
    )
    if candidate is None:
        raise SystemExit(f"no candidate named {args.vector_id}")
    fingerprints = candidate.get("fingerprints")

    defaults = cost_vector.defaults()
    print(f"{args.vector_id}  J={candidate['objective']:.4f}  "
          f"nostats={candidate['totals']['nostats']:.2f}s  "
          f"analyzed={candidate['totals']['analyzed']:.2f}s")
    print("changed parameters:")
    for field in cost_vector.FIELD_NAMES:
        value = candidate["vector"][field]
        if abs(value - defaults[field]) > 1e-12:
            ratio = value / defaults[field] if defaults[field] else float("inf")
            print(f"  {field:34} {defaults[field]:>12.6g} -> {value:<14.6g}"
                  f" (x{ratio:.3f})")
    if not fingerprints:
        print("\n(this candidate predates per-query program recording; "
              "re-run the search to get the per-query view)")
        return

    ratios = []
    for regime in REGIMES:
        print(f"\n  {regime}: baseline {state['baseline_totals'][regime]:.2f}s"
              f" -> {candidate['totals'][regime]:.2f}s")
        print(f"  {'query':>6} {'base':>9} {'cand':>9} {'ratio':>7}  plan")
        for name in names:
            base = baseline[regime][name]
            fingerprint = fingerprints[f"{regime}/{name}"]
            record = by_program.get((regime, name, fingerprint))
            if record is None:
                continue
            ratio = record["median_seconds"] / base["median_seconds"]
            ratios.append(ratio)
            changed = fingerprint != base["fingerprint"]
            print(f"  Q{name:>5} {base['median_seconds']:8.3f}s "
                  f"{record['median_seconds']:8.3f}s {ratio:7.3f}  "
                  f"{'changed' if changed else ''}")
            if changed and args.plans and fingerprint in plans:
                print(f"      before: {plans[base['fingerprint']]['plan_details']}")
                print(f"      after : {plans[fingerprint]['plan_details']}")
    if ratios:
        geometric = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
        print(f"\n  geometric-mean runtime ratio: {geometric:.4f} "
              f"(speedup {1 / geometric:.4f}x)")


def paired_bootstrap(values, rounds=20000, seed=17):
    """Bootstrap the mean of paired per-round values, resampling rounds."""
    rng = random.Random(seed)
    if len(values) < 2:
        return (float("nan"), float("nan"))
    means = []
    for _ in range(rounds):
        sample = [values[rng.randrange(len(values))] for _ in values]
        means.append(sum(sample) / len(sample))
    means.sort()
    return (means[int(0.025 * len(means))], means[int(0.975 * len(means))])


def command_validation(args):
    run = load(args.out_dir)
    rows = run["validation"]
    if not rows:
        raise SystemExit("no validation rounds recorded")
    names = run["state"]["workload"]["runnable"]

    by_config = {}
    for row in rows:
        by_config.setdefault(row["config"], {})[row["round"]] = row
    rounds = sorted(set.intersection(*[set(v) for v in by_config.values()]))
    print(f"{len(rounds)} complete paired rounds: {rounds}")

    print(f"\n{'config':24} {'nostats mean':>14} {'analyzed mean':>14} "
          f"{'J mean':>8} {'J 95% CI':>20}")
    reference = by_config["defaults"]
    summary = {}
    for config, per_round in sorted(by_config.items()):
        per_regime = {}
        objectives = []
        for r in rounds:
            ratios = {}
            for regime in REGIMES:
                ratios[regime] = (
                    per_round[r]["totals"][regime]
                    / reference[r]["totals"][regime]
                )
                per_regime.setdefault(regime, []).append(
                    per_round[r]["totals"][regime]
                )
            objectives.append(0.5 * ratios["nostats"] + 0.5 * ratios["analyzed"])
        low, high = paired_bootstrap(objectives)
        summary[config] = {
            "totals": {k: statistics.mean(v) for k, v in per_regime.items()},
            "objective_mean": statistics.mean(objectives),
            "objective_values": objectives,
            "ci": (low, high),
        }
        print(f"{config:24} {summary[config]['totals']['nostats']:13.2f}s "
              f"{summary[config]['totals']['analyzed']:13.2f}s "
              f"{summary[config]['objective_mean']:8.4f} "
              f"[{low:.4f}, {high:.4f}]")

    for config in sorted(by_config):
        if config == "defaults":
            continue
        print(f"\n--- {config}: per-query paired medians over {len(rounds)} rounds ---")
        print(f"  {'query':>6} {'regime':>9} {'defaults':>10} {'candidate':>10} "
              f"{'ratio':>7} {'note':>12}")
        regressions, improvements = [], []
        all_ratios = []
        for regime in REGIMES:
            for name in names:
                base = [reference[r]["per_query"][regime][name]["median_seconds"]
                        for r in rounds]
                cand = [by_config[config][r]["per_query"][regime][name]
                        ["median_seconds"] for r in rounds]
                base_mean, cand_mean = statistics.mean(base), statistics.mean(cand)
                ratio = cand_mean / base_mean
                all_ratios.append(ratio)
                note = ""
                if ratio > 1.10:
                    note = "REGRESSION"
                    regressions.append((name, regime, ratio, base_mean, cand_mean))
                elif ratio < 0.90:
                    note = "faster"
                    improvements.append((name, regime, ratio, base_mean, cand_mean))
                changed = (by_config[config][rounds[0]]["per_query"][regime][name]
                           ["fingerprint"]
                           != reference[rounds[0]]["per_query"][regime][name]
                           ["fingerprint"])
                if changed:
                    note = (note + " plan") if note else "plan"
                print(f"  Q{name:>5} {regime:>9} {base_mean:9.3f}s "
                      f"{cand_mean:9.3f}s {ratio:7.3f} {note:>12}")
        geometric = math.exp(sum(math.log(r) for r in all_ratios) / len(all_ratios))
        print(f"  geometric-mean per-query ratio: {geometric:.4f} "
              f"(speedup {1 / geometric:.4f}x)")
        print(f"  regressions over 10%: {len(regressions)}")
        for name, regime, ratio, base_mean, cand_mean in sorted(
            regressions, key=lambda x: -x[2]
        ):
            print(f"    Q{name} {regime}: {base_mean:.3f}s -> {cand_mean:.3f}s "
                  f"(x{ratio:.3f})")
        print(f"  improvements over 10%: {len(improvements)}")
        for name, regime, ratio, base_mean, cand_mean in sorted(
            improvements, key=lambda x: x[2]
        ):
            print(f"    Q{name} {regime}: {base_mean:.3f}s -> {cand_mean:.3f}s "
                  f"(x{ratio:.3f})")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    sub = parser.add_subparsers(dest="command", required=True)

    top = sub.add_parser("top")
    top.add_argument("--top", type=int, default=15)
    top.set_defaults(func=command_top)

    detail = sub.add_parser("detail")
    detail.add_argument("vector_id")
    detail.add_argument("--plans", action="store_true")
    detail.set_defaults(func=command_detail)

    validation = sub.add_parser("validation")
    validation.set_defaults(func=command_validation)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
