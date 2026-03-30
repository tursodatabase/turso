import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv("results.csv")

data = [df["Turso"].tolist(), df["SQLite"].tolist()]
columns = tuple(df["Query"].tolist())
rows = ["Turso", "SQLite"]

colors = ["#2E7D32", "#1976D2"]
n_rows = len(data)
n_cols = len(columns)

index = np.arange(n_cols)
bar_width = 0.35

fig, ax = plt.subplots(figsize=(max(10, n_cols * 0.9 + 2), 5))

cell_text = []
patterns = ["//", "///"]
for row in range(n_rows):
    offset = (row - 0.5) * bar_width
    bars = ax.bar(index + offset, data[row], bar_width,
                  label=rows[row], color=colors[row],
                  edgecolor="black", linewidth=0.5, hatch=patterns[row],
                  rasterized=True)
    cell_text.append(["%.2f" % x for x in data[row]])

ax.set_ylabel("Runtime [s]", fontsize=11)
ax.set_yscale("log")
ax.set_xticks([])
ax.set_xlim(-0.5, n_cols - 0.5)

ax.grid(True, axis="y", alpha=0.3, linestyle="-", linewidth=0.5)
ax.set_axisbelow(True)

ax.legend(loc="upper left", fontsize=10, frameon=True)

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_linewidth(0.8)
ax.spines["bottom"].set_linewidth(0.8)

ax.tick_params(axis="y", labelsize=9)

# Column widths span the full axes width; row labels sit to the left automatically
col_width = 1.0 / n_cols
the_table = ax.table(cellText=cell_text,
                     rowLabels=rows,
                     colLabels=columns,
                     cellLoc="center",
                     loc="bottom",
                     colWidths=[col_width] * n_cols)

the_table.auto_set_font_size(False)
the_table.set_fontsize(8)
the_table.scale(1.0, 1.4)

for (i, j), cell in the_table.get_celld().items():
    cell.set_linewidth(0.5)
    cell.set_edgecolor("black")

fig.subplots_adjust(left=0.08, right=0.97, top=0.97, bottom=0.18)

fig.savefig("tpch.pdf", dpi=300)
print("Saved tpch.pdf")
plt.show()
