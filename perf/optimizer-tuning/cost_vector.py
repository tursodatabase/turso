"""Cost-vector schema, validation and sampling for the optimizer tuning driver.

The engine's own loader falls back to compiled defaults when a parameter file is
missing, malformed or invalid. A tuning run that relied on that behaviour would
silently measure the defaults again, so every vector this module writes is
checked here first and the driver refuses to run an unchecked vector.

The checks below repeat the engine-side checks in
`core/translate/optimizer/cost_params.rs` and add the ones the engine does not
make. Two of the added checks matter for a search:

* `rows_per_table_page` must be more than 1. `estimate_btree_depth` divides by
  `rows_per_table_page.ln()`, which is 0 at 1.0 and negative below it.
* `index_bonus` is subtracted from every index cost, which is then clamped to
  0.001. A large bonus makes every index access cost the same clamped value and
  the optimizer can no longer tell index plans apart.
"""

import json
import math
import os
import re

POSITIVE_WEIGHTS = (
    "cpu_cost_per_row",
    "cpu_cost_per_where_step",
    "cpu_cost_per_seek",
    "sort_cpu_per_row",
    "hash_cpu_cost",
    "hash_insert_cost",
    "hash_lookup_cost",
)

COST_ONLY_FIELDS = POSITIVE_WEIGHTS + ("cache_reuse_factor", "index_bonus")

HEURISTIC_FIELDS = (
    "rows_per_table_fallback",
    "rows_per_table_page",
    "in_subquery_rows",
    "sel_eq_unindexed",
    "sel_eq_indexed",
    "sel_range",
    "sel_is_null",
    "sel_is_not_null",
    "sel_like",
    "sel_not_like",
    "sel_other",
    "hash_bytes_per_row",
    "closed_range_selectivity_factor",
)

SELECTIVITY_FIELDS = (
    "sel_eq_unindexed",
    "sel_eq_indexed",
    "sel_range",
    "sel_is_null",
    "sel_is_not_null",
    "sel_like",
    "sel_not_like",
    "sel_other",
)

# The engine's compiled defaults. `engine_defaults()` reads the same numbers
# out of `CostModelParams::new()` and a test compares the two, so this table
# cannot drift away from the engine without the test saying so.
DEFAULTS = {
    "rows_per_table_fallback": 1000000.0,
    "rows_per_table_page": 50.0,
    "sel_eq_unindexed": 0.04,
    "sel_eq_indexed": 0.001,
    "sel_range": 0.4,
    "sel_is_null": 0.1,
    "sel_is_not_null": 0.9,
    "sel_like": 0.2,
    "sel_not_like": 0.2,
    "sel_other": 0.9,
    "in_subquery_rows": 25.0,
    "cache_reuse_factor": 0.2,
    "cpu_cost_per_row": 0.003,
    "cpu_cost_per_where_step": 0.003,
    "cpu_cost_per_seek": 0.01,
    "index_bonus": 0.5,
    "sort_cpu_per_row": 0.002,
    "hash_cpu_cost": 0.001,
    "hash_insert_cost": 0.002,
    "hash_lookup_cost": 0.003,
    "hash_bytes_per_row": 100.0,
    "closed_range_selectivity_factor": 0.2,
}

FIELD_NAMES = tuple(DEFAULTS)

# The largest index_bonus the search may propose. Above this value the 0.001
# clamp in estimate_index_cost starts to flatten the cost of small index
# accesses, which removes information the optimizer needs.
MAX_INDEX_BONUS = 10.0
MAX_CACHE_REUSE_FACTOR = 0.95


COST_PARAMS_SOURCE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "core", "translate", "optimizer", "cost_params.rs",
)


def engine_defaults(path=COST_PARAMS_SOURCE):
    """Read the compiled defaults out of `CostModelParams::new()`.

    The table above must agree with the engine, or a search would measure
    candidates against the wrong reference. A test compares the two.
    """
    with open(path, encoding="utf-8") as handle:
        source = handle.read()
    start = source.index("pub const fn new() -> Self {")
    end = source.index("}", source.index("Self {", start))
    body = source[start:end]
    found = {}
    for name, value in re.findall(r"(\w+):\s*([0-9_.]+),", body):
        found[name] = float(value.replace("_", ""))
    return found


class VectorError(ValueError):
    """A cost vector that the driver refuses to run."""


def validate(vector):
    """Raise VectorError unless every field is present, finite and in domain."""
    if not isinstance(vector, dict):
        raise VectorError(f"vector must be an object, got {type(vector).__name__}")

    unknown = sorted(set(vector) - set(DEFAULTS))
    if unknown:
        raise VectorError(f"unknown keys: {', '.join(unknown)}")
    missing = sorted(set(DEFAULTS) - set(vector))
    if missing:
        raise VectorError(f"missing keys: {', '.join(missing)}")

    for name, value in sorted(vector.items()):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise VectorError(f"{name} must be a number, got {value!r}")
        if not math.isfinite(float(value)):
            raise VectorError(f"{name} must be finite, got {value!r}")

    for name in SELECTIVITY_FIELDS:
        value = float(vector[name])
        if value <= 0.0 or value > 1.0:
            raise VectorError(f"{name} must be in (0, 1], got {value}")

    if float(vector["sel_eq_indexed"]) > float(vector["sel_eq_unindexed"]):
        raise VectorError(
            "sel_eq_indexed ({}) must be <= sel_eq_unindexed ({})".format(
                vector["sel_eq_indexed"], vector["sel_eq_unindexed"]
            )
        )

    for name in ("rows_per_table_fallback", "in_subquery_rows", "hash_bytes_per_row"):
        if float(vector[name]) <= 0.0:
            raise VectorError(f"{name} must be positive, got {vector[name]}")

    # estimate_btree_depth divides by rows_per_table_page.ln().
    if float(vector["rows_per_table_page"]) <= 1.0:
        raise VectorError(
            "rows_per_table_page must be more than 1 because estimate_btree_depth "
            f"divides by its natural logarithm, got {vector['rows_per_table_page']}"
        )

    cache_reuse_factor = float(vector["cache_reuse_factor"])
    if cache_reuse_factor < 0.0 or cache_reuse_factor >= 1.0:
        raise VectorError(f"cache_reuse_factor must be in [0, 1), got {cache_reuse_factor}")

    for name in POSITIVE_WEIGHTS:
        if float(vector[name]) < 0.0:
            raise VectorError(f"{name} must be non-negative, got {vector[name]}")

    index_bonus = float(vector["index_bonus"])
    if index_bonus < 0.0 or index_bonus > MAX_INDEX_BONUS:
        raise VectorError(
            f"index_bonus must be in [0, {MAX_INDEX_BONUS}], got {index_bonus}"
        )

    factor = float(vector["closed_range_selectivity_factor"])
    if factor <= 0.0 or factor > 1.0:
        raise VectorError(
            f"closed_range_selectivity_factor must be in (0, 1], got {factor}"
        )

    return {name: float(vector[name]) for name in FIELD_NAMES}


def defaults():
    return dict(DEFAULTS)


def write(path, vector):
    """Write a fully expanded, checked vector. Partial files are never written."""
    checked = validate(vector)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(checked, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return checked


def read(path):
    with open(path, encoding="utf-8") as handle:
        return validate(json.load(handle))


def matches(left, right, tolerance=1e-12):
    """Compare two vectors field by field with a relative tolerance."""
    for name in FIELD_NAMES:
        a, b = float(left[name]), float(right[name])
        if abs(a - b) > tolerance * max(1.0, abs(a), abs(b)):
            return False
    return True


def sample_cost_only(rng, base=None, span=10.0):
    """Draw one cost-only vector. Positive weights move in log space."""
    vector = dict(base or DEFAULTS)
    for name in POSITIVE_WEIGHTS:
        anchor = float((base or DEFAULTS)[name])
        if anchor == 0.0:
            anchor = DEFAULTS[name]
        vector[name] = anchor * math.exp(rng.uniform(-math.log(span), math.log(span)))
    vector["cache_reuse_factor"] = rng.uniform(0.0, MAX_CACHE_REUSE_FACTOR)
    # index_bonus has a legal zero, so it is drawn on a linear scale that can
    # reach the boundary instead of a logarithmic one that never can.
    vector["index_bonus"] = rng.choice(
        [0.0, rng.uniform(0.0, 2.0), rng.uniform(0.0, MAX_INDEX_BONUS)]
    )
    return validate(vector)


def sample_heuristics(rng, base=None, span=10.0):
    """Draw one vector that also moves the estimation heuristics."""
    vector = dict(base or DEFAULTS)
    anchor = base or DEFAULTS
    for name in ("rows_per_table_fallback", "rows_per_table_page", "in_subquery_rows",
                 "hash_bytes_per_row"):
        value = float(anchor[name]) * math.exp(
            rng.uniform(-math.log(span), math.log(span))
        )
        vector[name] = max(value, 2.0) if name == "rows_per_table_page" else value
    for name in SELECTIVITY_FIELDS:
        value = float(anchor[name]) * math.exp(
            rng.uniform(-math.log(span), math.log(span))
        )
        vector[name] = min(value, 1.0)
    if vector["sel_eq_indexed"] > vector["sel_eq_unindexed"]:
        vector["sel_eq_indexed"] = vector["sel_eq_unindexed"]
    vector["closed_range_selectivity_factor"] = min(
        float(anchor["closed_range_selectivity_factor"])
        * math.exp(rng.uniform(-math.log(span), math.log(span))),
        1.0,
    )
    return validate(vector)


def perturb(rng, base, fields, span=1.5):
    """Draw a small multiplicative perturbation of `base` on the given fields."""
    vector = dict(base)
    for name in fields:
        if name == "cache_reuse_factor":
            value = float(base[name]) * math.exp(
                rng.uniform(-math.log(span), math.log(span))
            )
            vector[name] = min(max(value, 0.0), MAX_CACHE_REUSE_FACTOR)
        elif name == "index_bonus":
            value = float(base[name]) * math.exp(
                rng.uniform(-math.log(span), math.log(span))
            )
            vector[name] = min(max(value, 0.0), MAX_INDEX_BONUS)
        elif name in SELECTIVITY_FIELDS or name == "closed_range_selectivity_factor":
            value = float(base[name]) * math.exp(
                rng.uniform(-math.log(span), math.log(span))
            )
            vector[name] = min(value, 1.0)
        elif name == "rows_per_table_page":
            value = float(base[name]) * math.exp(
                rng.uniform(-math.log(span), math.log(span))
            )
            vector[name] = max(value, 2.0)
        else:
            vector[name] = float(base[name]) * math.exp(
                rng.uniform(-math.log(span), math.log(span))
            )
    if vector["sel_eq_indexed"] > vector["sel_eq_unindexed"]:
        vector["sel_eq_indexed"] = vector["sel_eq_unindexed"]
    return validate(vector)
