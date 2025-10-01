import os
import sys

import matplotlib.pyplot as plt
import pandas as pd
import scienceplots  # noqa: F401

plt.style.use(["science"])
plt.rcParams.update({
  "text.usetex": True,
  "font.family": "serif",
  "font.serif": ["Times"],
})

# Get CSV filename from command line argument
if len(sys.argv) < 2:
    print("Usage: python script.py <csv_filename>")
    sys.exit(1)

csv_filename = sys.argv[1]

# Get basename without extension for output filename
basename = os.path.splitext(csv_filename)[0]
output_filename = f"{basename}-compute.png"

# Read data from CSV file
df = pd.read_csv(csv_filename)

# Create figure and axis
fig, ax = plt.subplots(figsize=(10, 6))

# Get unique systems and thread counts
systems = df["system"].unique()
thread_counts = sorted(df["threads"].unique())

# Get colors from the current color cycle
prop_cycle = plt.rcParams["axes.prop_cycle"]
colors_list = prop_cycle.by_key()["color"]

# Plot a line for each system-thread combination
markers = ["o", "s", "^", "D"]
linestyles = ["-", "--", "-.", ":"]

plot_idx = 0
for sys_idx, system in enumerate(systems):
    df_system = df[df["system"] == system]
    for thread_idx, threads in enumerate(thread_counts):
        df_thread = df_system[df_system["threads"] == threads].sort_values("compute")
        if len(df_thread) > 0:
            ax.plot(df_thread["compute"], df_thread["throughput"],
                    marker=markers[thread_idx % len(markers)],
                    color=colors_list[plot_idx % len(colors_list)],
                    linestyle=linestyles[sys_idx % len(linestyles)],
                    linewidth=2, markersize=8,
                    label=f'{system} ({threads} thread{"s" if threads > 1 else ""})')
            plot_idx += 1

# Customize the plot
ax.set_xlabel(r"Compute Time (microseconds)", fontsize=14, fontweight="bold")
ax.set_ylabel("Throughput (rows/second)", fontsize=14, fontweight="bold")

# Set y-axis to start from 0 with dynamic upper limit
max_throughput = df["throughput"].max()
ax.set_ylim(0, max_throughput * 1.15)  # Add 15% tolerance for legend space

# Format y-axis labels
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x/1000)}k"))

# Add legend
ax.legend(loc="lower left", frameon=True, fontsize=11)

# Add grid for better readability
ax.grid(axis="both", alpha=0.3, linestyle="--")
ax.set_axisbelow(True)

# Adjust layout
plt.tight_layout()

# Save the figure
plt.savefig(output_filename, dpi=300, bbox_inches="tight")
print(f"Saved plot to {output_filename}")

# Display the plot
plt.show()
