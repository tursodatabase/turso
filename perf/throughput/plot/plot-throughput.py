#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Draw throughput and CPU utilization against connections from result files.

Usage: uv run plot-throughput.py plot/*-result.csv \
           -o throughput.png -o throughput.pdf -o throughput.tikz

Every result file is one run. Runs of the same engine and connection
count make one point: their mean, with an error bar of one standard
deviation across the runs. Transactions per second go on the left axis
as solid lines with filled markers; the CPU the whole process used during
the run, as a share of every hardware thread on the machine, goes on the
right axis, from 0 to 100%, as dashed lines with hollow markers, because
a throughput number must never be shown without what it cost.

`-o` can be given more than once, and each output's format follows its
extension: `.png`, `.pdf` and the other matplotlib formats draw the
figure; `.tikz` or `.tex` write a pgfplots picture for `\\input` into a
LaTeX document that loads pgfplots, TikZ's `calc` library and
`\\pgfplotsset{compat=1.18}`.
"""

import argparse
import csv
import os
from pathlib import Path

import numpy as np

# The Okabe-Ito palette: colour-blind safe, legible in greyscale, and what
# gnuplot draws with by default. `tikz_mark` is the pgfplots name of the
# matplotlib `marker`.
ENGINES = {
    "sqlite": {"name": "SQLite", "color": "#E69F00", "marker": "s", "tikz_mark": "square"},
    "turso": {"name": "Turso", "color": "#0072B2", "marker": "o", "tikz_mark": "o"},
}
FALLBACK_COLORS = ["#009E73", "#D55E00", "#CC79A7"]
FALLBACK_MARKERS = [("^", "triangle"), ("D", "diamond"), ("v", "triangle")]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs="+", type=Path)
    parser.add_argument("-o", "--output", action="append", type=Path, metavar="FILE",
                        help="output file; repeat for several formats "
                             "(default throughput.png)")
    parser.add_argument("--name", action="append", default=[], metavar="ENGINE=NAME",
                        help="legend name for an engine, e.g. turso=Limbo")
    args = parser.parse_args()
    names = dict(name.split("=", 1) for name in args.name)

    runs = {}
    for path in args.csv_files:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                key = (row["engine"], int(row["connections"]))
                runs.setdefault(key, []).append(Run(row))
    if not runs:
        raise SystemExit("no results found")

    figure = Figure(runs, names)
    for output in args.output or [Path("throughput.png")]:
        if output.suffix in (".tikz", ".tex"):
            output.write_text(figure.tikz())
        else:
            figure.matplotlib(output)
        print(f"wrote {output}")


class Run:
    def __init__(self, row):
        self.throughput = float(row["transactions_per_s"])
        busy = float(row["cpu_user_s"]) + float(row["cpu_sys_s"])
        # Result files from before the count was recorded fall back to
        # this machine's, which is only right when plotting where it ran.
        threads = int(row.get("hardware_threads") or os.cpu_count() or 1)
        self.cpu_percent = busy / float(row["seconds"]) / threads * 100.0


class Series:
    """One engine: a point per connection count for each measure, with its
    spread across runs."""

    def __init__(self, engine, index, runs, name=None):
        look = ENGINES.get(engine)
        self.label = name or (look["name"] if look else engine)
        if look:
            self.color, self.marker, self.tikz_mark = look["color"], look["marker"], look["tikz_mark"]
        else:
            self.color = FALLBACK_COLORS[index % len(FALLBACK_COLORS)]
            self.marker, self.tikz_mark = FALLBACK_MARKERS[index % len(FALLBACK_MARKERS)]
        self.tikz_color = "".join(ch for ch in engine if ch.isalpha())
        mine = sorted(((c, v) for (e, c), v in runs.items() if e == engine), key=lambda kv: kv[0])
        self.throughput = [(c, *mean_sd([r.throughput for r in v])) for c, v in mine]
        self.cpu = [(c, *mean_sd([r.cpu_percent for r in v])) for c, v in mine]


def mean_sd(values):
    return float(np.mean(values)), float(np.std(values)) if len(values) > 1 else 0.0


class Figure:
    def __init__(self, runs, names):
        self.connections = sorted({c for _, c in runs})
        self.series = [Series(e, i, runs, names.get(e))
                       for i, e in enumerate(sorted({e for e, _ in runs}))]
        self.ymax = max(m + sd for s in self.series for _, m, sd in s.throughput) * 1.15

    def matplotlib(self, output):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import scienceplots  # noqa: F401  (registers the styles)
        from matplotlib.lines import Line2D
        from matplotlib.ticker import FuncFormatter, NullLocator

        plt.style.use(["science", "no-latex"])

        fig, ax = plt.subplots(figsize=(3.8, 2.6), dpi=300)
        ax.set_xscale("log", base=2)
        ax.grid(True, which="major", linewidth=0.5, linestyle=(0, (2, 2)), color="0.7")
        ax.set_axisbelow(True)
        cpu_ax = ax.twinx()
        for s in self.series:
            ax.errorbar([c for c, _, _ in s.throughput], [m for _, m, _ in s.throughput],
                        yerr=[sd for _, _, sd in s.throughput], color=s.color, linewidth=1.3,
                        marker=s.marker, markersize=4.5, capsize=2.5, elinewidth=0.9,
                        zorder=3)
            cpu_ax.errorbar([c for c, _, _ in s.cpu], [m for _, m, _ in s.cpu],
                            yerr=[sd for _, _, sd in s.cpu], color=s.color, linewidth=1.1,
                            linestyle=(0, (4, 2)), marker=s.marker, markersize=4.5,
                            markerfacecolor="white", capsize=2.5, elinewidth=0.9, zorder=3)
        ax.set_xticks(self.connections)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.set_xlim(self.connections[0] / 1.5, self.connections[-1] * 1.5)
        ax.set_ylim(0, self.ymax)
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_count))
        ax.set_xlabel("Connections")
        ax.set_ylabel("Transactions/s")
        cpu_ax.set_ylim(0, 100)
        cpu_ax.set_yticks([0, 25, 50, 75, 100])
        cpu_ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}%"))
        cpu_ax.yaxis.set_minor_locator(NullLocator())
        cpu_ax.set_ylabel("CPU utilization")

        handles = [Line2D([], [], color=s.color, linewidth=1.3, marker=s.marker,
                          markersize=4.5, label=s.label) for s in self.series]
        handles += [
            Line2D([], [], color="0.35", linewidth=1.3, marker="o", markersize=4.5,
                   label="transactions/s"),
            Line2D([], [], color="0.35", linewidth=1.1, linestyle=(0, (4, 2)), marker="o",
                   markersize=4.5, markerfacecolor="white", label="CPU utilization"),
        ]
        fig.legend(handles=handles, loc="lower center", ncol=len(handles), frameon=False,
                   handlelength=2.2, columnspacing=1.4, bbox_to_anchor=(0.5, -0.24))
        fig.savefig(output, bbox_inches="tight")

    def tikz(self):
        out = ["% Generated by perf/throughput/plot/plot-throughput.py. Do not edit.",
               r"\begin{tikzpicture}"]
        for s in self.series:
            out.append(rf"\definecolor{{{s.tikz_color}}}{{HTML}}{{{s.color.lstrip('#')}}}")
        xticks = ",".join(str(c) for c in self.connections)
        common = (rf"scale only axis, width=0.78\linewidth, height=0.5\linewidth, "
                  rf"xmode=log, log basis x=2, xmin={self.connections[0] / 1.5:g}, "
                  rf"xmax={self.connections[-1] * 1.5:g}, xtick={{{xticks}}}, "
                  rf"log ticks with fixed point, xminorticks=false, "
                  rf"tick label style={{font=\scriptsize}}, label style={{font=\footnotesize}}, "
                  rf"axis line style={{line width=0.4pt}}, tick style={{line width=0.4pt, black}}, "
                  rf"error bars/y dir=both, error bars/y explicit, "
                  rf"error bars/error mark options={{line width=0.5pt, mark size=1.5pt, rotate=90}}")
        # Two axes on top of each other: throughput on the left, CPU on the
        # right. The legend is built on the first and placed under both.
        out.append(rf"""\begin{{axis}}[
  name=throughput, {common},
  ymin=0, ymax={self.ymax:.4g}, xlabel={{Connections}}, ylabel={{Transactions/s}},
  grid=major, grid style={{line width=0.3pt, dashed, draw=black!30}},
  axis y line*=left,
  legend columns=-1, legend to name=throughputlegend, legend cell align=left,
  legend style={{draw=none, font=\scriptsize, /tikz/every even column/.append style={{column sep=0.4cm}}}},
]""")
        for s in self.series:
            coords = " ".join(f"({c},{m:.4g}) +- (0,{sd:.4g})" for c, m, sd in s.throughput)
            out.append(rf"\addplot[{s.tikz_color}, line width=0.9pt, mark={s.tikz_mark}*, "
                       rf"mark size=1.6pt] coordinates {{{coords}}};")
            out.append(rf"\addlegendentry{{{s.label}}}")
        out.append(r"\addlegendimage{black!65, line width=0.9pt, mark=*, mark size=1.6pt}")
        out.append(r"\addlegendentry{transactions/s}")
        out.append(r"\addlegendimage{black!65, line width=0.8pt, dashed, mark=o, mark size=1.6pt}")
        out.append(r"\addlegendentry{CPU utilization}")
        out.append(r"\end{axis}")
        out.append(rf"""\begin{{axis}}[
  at={{(throughput.south west)}}, anchor=south west, {common},
  ymin=0, ymax=100, ytick={{0,25,50,75,100}}, yticklabel={{\pgfmathprintnumber{{\tick}}\%}},
  axis y line*=right, axis x line=none, ylabel={{CPU utilization}},
]""")
        for s in self.series:
            coords = " ".join(f"({c},{m:.4g}) +- (0,{sd:.4g})" for c, m, sd in s.cpu)
            out.append(rf"\addplot[{s.tikz_color}, line width=0.8pt, dashed, mark={s.tikz_mark}, "
                       rf"mark size=1.6pt, mark options={{solid}}] coordinates {{{coords}}};")
        out.append(r"\end{axis}")
        out.append(r"\node[anchor=north] at ($(throughput.south west)!0.5!(throughput.south east) + (0,-0.9cm)$) "
                   r"{\pgfplotslegendfromname{throughputlegend}};")
        out.append(r"\end{tikzpicture}")
        return "\n".join(out) + "\n"


def fmt_count(value, _pos=None):
    if value >= 1_000_000:
        return f"{value / 1e6:g}M"
    if value >= 1_000:
        return f"{value / 1e3:g}k"
    return f"{value:g}"


if __name__ == "__main__":
    main()
