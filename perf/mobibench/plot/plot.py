#!/usr/bin/env python3
"""
Grouped bar chart comparing SQLite and Turso transaction performance
using Mobibench (WAL + FULL synchronous).

Usage: uv run python plot.py [results.csv]
"""

import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scienceplots  # noqa: F401

csv_path = sys.argv[1] if len(sys.argv) > 1 else "results.csv"
df = pd.read_csv(csv_path)

operations = ["Insert", "Update", "Delete"]
systems = ["Turso", "SQLite"]

means = {}
stds = {}
for sys_name in systems:
    means[sys_name] = []
    stds[sys_name] = []
    for op in operations:
        subset = df[(df["system"] == sys_name) & (df["operation"] == op)]
        means[sys_name].append(subset["tps"].mean())
        stds[sys_name].append(subset["tps"].std())

plt.style.use(["science", "no-latex"])
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
})

fig, ax = plt.subplots(figsize=(5, 3.5))

x = np.arange(len(operations))
bar_width = 0.3

ax.bar(
    x - bar_width / 2, means["Turso"], bar_width,
    yerr=stds["Turso"], capsize=3,
    label="Turso", color="#2E7D32",
    edgecolor="black", linewidth=0.5,
)
ax.bar(
    x + bar_width / 2, means["SQLite"], bar_width,
    yerr=stds["SQLite"], capsize=3,
    label="SQLite", color="#1976D2",
    edgecolor="black", linewidth=0.5, hatch="///",
)

ax.set_xticks(x)
ax.set_xticklabels(operations)
ax.set_ylabel("Transactions / sec")
ax.legend(frameon=True)

ax.grid(True, axis="y", alpha=0.3, linestyle="-", linewidth=0.5)
ax.set_axisbelow(True)

fig.tight_layout()
fig.savefig("mobibench.pdf", dpi=300, bbox_inches="tight")
print("Saved mobibench.pdf")
