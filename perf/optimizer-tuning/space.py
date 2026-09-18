#!/usr/bin/env python3
"""The search space of `CostModelParams`, and how to move between units.

Each parameter gets a range and a scale. A scale of "log" means the tuner
searches the exponent, because these parameters act as multipliers and a step
from 0.001 to 0.002 matters as much as a step from 0.1 to 0.2.

`to_unit` maps a parameter set into the unit cube the model works in.
`from_unit` maps a point of that cube back into a parameter set.
"""

import math

# name: (default, low, high, scale)
SPACE = {
    "rows_per_table_fallback": (1_000_000.0, 1_000.0, 10_000_000.0, "log"),
    "rows_per_table_page": (50.0, 5.0, 500.0, "log"),
    "sel_eq_unindexed": (0.1, 0.005, 0.6, "log"),
    "sel_eq_indexed": (0.001, 0.00001, 0.3, "log"),
    "sel_range": (0.4, 0.05, 0.95, "linear"),
    "sel_is_null": (0.1, 0.01, 0.5, "log"),
    "sel_is_not_null": (0.9, 0.5, 1.0, "linear"),
    "sel_like": (0.2, 0.01, 0.8, "log"),
    "sel_not_like": (0.2, 0.01, 0.8, "log"),
    "sel_other": (0.9, 0.3, 1.0, "linear"),
    "in_subquery_rows": (25.0, 2.0, 1000.0, "log"),
    "cache_reuse_factor": (0.2, 0.0, 0.95, "linear"),
    "cpu_cost_per_row": (0.003, 0.0001, 0.1, "log"),
    "cpu_cost_per_where_step": (0.003, 0.0001, 0.1, "log"),
    "cpu_cost_per_seek": (0.01, 0.0001, 0.5, "log"),
    "ephemeral_index_build_cost": (0.43, 0.0001, 2.0, "log"),
    "index_bonus": (0.5, 0.0, 20.0, "linear"),
    "sort_cpu_per_row": (0.002, 0.0001, 0.1, "log"),
    "hash_cpu_cost": (0.001, 0.00001, 0.05, "log"),
    "hash_insert_cost": (0.002, 0.00001, 0.05, "log"),
    "hash_lookup_cost": (0.003, 0.00001, 0.05, "log"),
    "hash_bytes_per_row": (100.0, 10.0, 1000.0, "log"),
    "closed_range_selectivity_factor": (0.2, 0.02, 1.0, "log"),
}

NAMES = list(SPACE)
DEFAULTS = {name: SPACE[name][0] for name in NAMES}


def to_unit(params):
    point = []
    for name in NAMES:
        _, low, high, scale = SPACE[name]
        value = min(max(params[name], low), high)
        if scale == "log":
            position = (math.log(value) - math.log(low)) / (math.log(high) - math.log(low))
        else:
            position = (value - low) / (high - low)
        point.append(position)
    return point


def from_unit(point):
    params = {}
    for name, position in zip(NAMES, point):
        _, low, high, scale = SPACE[name]
        position = min(max(float(position), 0.0), 1.0)
        if scale == "log":
            value = math.exp(math.log(low) + position * (math.log(high) - math.log(low)))
        else:
            value = low + position * (high - low)
        params[name] = round(value, 8)
    return repair(params)


def repair(params):
    """Make a parameter set pass `CostModelParams::validate`."""
    params = dict(params)
    params["sel_eq_indexed"] = min(params["sel_eq_indexed"], params["sel_eq_unindexed"])
    params["cache_reuse_factor"] = min(max(params["cache_reuse_factor"], 0.0), 0.949)
    for name in NAMES:
        _, low, high, _ = SPACE[name]
        params[name] = min(max(params[name], low), high)
    return params
