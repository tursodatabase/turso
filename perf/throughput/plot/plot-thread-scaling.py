import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scienceplots  # noqa: F401

plt.style.use(["science"])

# Get CSV filenames from command line arguments
if len(sys.argv) < 2:
    print("Usage: python script.py <csv_filename> [<csv_filename> ...]")
    sys.exit(1)

csv_filenames = sys.argv[1:]

# Output filename
output_filename = "thread-scaling.pdf"

# Read data from all CSV files and concatenate
dfs = [pd.read_csv(filename) for filename in csv_filenames]
df = pd.concat(dfs, ignore_index=True)

# Filter for compute time = 0
df_filtered = df[df["compute"] == 0].sort_values("threads")

# Get unique systems and threads
systems = df_filtered["system"].unique()
threads = sorted(df_filtered["threads"].unique())

# Create figure and axis
fig, ax = plt.subplots(figsize=(10, 6))

# Set up bar positions
x_pos = np.arange(len(threads))
bar_width = 0.35

# Colors and hatching patterns matching TPC-H plot
system_colors = {"Turso": "#2E7D32", "SQLite": "#1976D2"}
system_hatches = {"Turso": "//", "SQLite": "///"}

# Plot bars for each system
for i, system in enumerate(systems):
    system_data = df_filtered[df_filtered["system"] == system].sort_values("threads")
    throughput = system_data["throughput"].tolist()

    offset = (i - len(systems)/2 + 0.5) * bar_width
    bars = ax.bar(x_pos + offset, throughput, bar_width,
                   label=system,
                   color=system_colors.get(system, "#888888"),
                   edgecolor="black", linewidth=0.5,
                   hatch=system_hatches.get(system, ""),
                   rasterized=True)

# Customize the plot
ax.set_xlabel("Number of Threads", fontsize=14, fontweight="bold")
ax.set_ylabel("Throughput (rows/sec)", fontsize=14, fontweight="bold")

# Set y-axis to start from 0 with dynamic upper limit
max_throughput = df_filtered["throughput"].max()
ax.set_ylim(0, max_throughput * 1.15)  # Add 15% tolerance for legend space

# Format y-axis labels
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x/1000)}k"))

# Set x-axis ticks to show thread values
ax.set_xticks(x_pos)
ax.set_xticklabels(threads)

# Add legend
ax.legend(loc="upper left", frameon=True, fontsize=12)

# Add grid for better readability
ax.grid(axis="y", alpha=0.3, linestyle="--")
ax.set_axisbelow(True)

# Adjust layout
plt.tight_layout()

# Save the figure
plt.savefig(output_filename, dpi=300, bbox_inches="tight")
print(f"Saved plot to {output_filename}")

# Display the plot
plt.show()
