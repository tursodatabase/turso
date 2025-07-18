#!/usr/bin/env python3
"""Plot connection benchmark results from CSV data."""

import csv
import sys

import matplotlib.pyplot as plt


def main() -> None:
    """Plot benchmark results from CSV file."""
    if len(sys.argv) != 2:
        print("Usage: python3 plot.py <results.csv>")
        sys.exit(1)

    # Read the CSV file
    databases = []
    p50 = []
    p95 = []
    p99 = []

    with open(sys.argv[1]) as file:
        reader = csv.DictReader(file)
        for row in reader:
            databases.append(row["database"])
            p50.append(int(row["p50"]))
            p95.append(int(row["p95"]))
            p99.append(int(row["p99"]))

    x = range(len(databases))
    width = 0.25

    plt.figure(figsize=(12, 8))
    plt.bar([i - width for i in x], p50, width, label="p50", alpha=0.8)
    plt.bar([i for i in x], p95, width, label="p95", alpha=0.8)
    plt.bar([i + width for i in x], p99, width, label="p99", alpha=0.8)

    plt.xlabel("Database (Number of Tables)")
    plt.ylabel("Connection Time (nanoseconds)")
    plt.title("Database Connection Performance by Table Count")
    plt.xticks(x, databases)
    plt.legend()
    plt.yscale("log")  # Use log scale for better visualization
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("connection_benchmark.png", dpi=300, bbox_inches="tight")
    plt.show()

    print("Chart saved as connection_benchmark.png")


if __name__ == "__main__":
    main()
