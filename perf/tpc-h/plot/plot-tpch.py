#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Draw the runtime of every TPC-H query as grouped bars from a results CSV.

Usage: ./results2csv.sh ../results_<timestamp>.txt > results.csv
       uv run plot-tpch.py results.csv -o tpch.png -o tpch.pdf -o tpch.tikz

The CSV has one row per query and one column per engine, as
`results2csv.sh` writes it: `Query,Limbo,SQLite`. Every query gets a
group of bars, one per engine, with runtime in seconds up a log axis so
a query that takes a fifth of a second and one that takes a minute both
read. A bar that is missing because the engine did not run the query
(`NA` in the CSV) is marked `n/a` in its place, so a gap is never
mistaken for a fast run.

`-o` can be given more than once, and each output's format follows its
extension: `.png`, `.pdf` and the other matplotlib formats draw the
figure; `.tikz` or `.tex` write a pgfplots picture for `\\input` into a
LaTeX document that loads pgfplots and `\\pgfplotsset{compat=1.18}`.
"""

import argparse
import csv
from pathlib import Path

import numpy as np

# The Okabe-Ito palette: colour-blind safe, legible in greyscale, and what
# gnuplot draws with by default, so it looks like the systems papers a
# reader knows. Keyed by the CSV column name in lower case; `results2csv.sh`
# calls the engine Limbo, and it gets the same colour as Turso does in the
# latency and throughput plots.
ENGINES = {
    "sqlite": {"name": "SQLite", "color": "#E69F00"},
    "limbo": {"name": "Limbo", "color": "#0072B2"},
    "turso": {"name": "Turso", "color": "#0072B2"},
}
FALLBACK_COLORS = ["#009E73", "#D55E00", "#CC79A7"]

# The share of the space between one query and the next that its bars fill.
GROUP_WIDTH = 0.8


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", type=Path)
    parser.add_argument("-o", "--output", action="append", type=Path, metavar="FILE",
                        help="output file; repeat for several formats "
                             "(default tpch.png)")
    parser.add_argument("--name", action="append", default=[], metavar="ENGINE=NAME",
                        help="legend name for an engine, e.g. limbo=Turso")
    args = parser.parse_args()
    names = dict(name.split("=", 1) for name in args.name)

    with open(args.csv_file, newline="") as f:
        reader = csv.DictReader(f)
        columns = [c for c in reader.fieldnames if c != "Query"]
        rows = list(reader)
    if not rows or not columns:
        raise SystemExit("no results found")

    figure = Figure(rows, columns, names)
    for output in args.output or [Path("tpch.png")]:
        if output.suffix in (".tikz", ".tex"):
            output.write_text(figure.tikz())
        else:
            figure.matplotlib(output)
        print(f"wrote {output}")


class Series:
    """One engine: its runtime for every query, `None` where it did not run."""

    def __init__(self, column, index, rows, name=None):
        engine = column.lower()
        look = ENGINES.get(engine)
        self.label = name or (look["name"] if look else column)
        self.color = look["color"] if look else FALLBACK_COLORS[index % len(FALLBACK_COLORS)]
        self.tikz_color = "".join(ch for ch in engine if ch.isalpha())
        self.times = [parse_time(row[column]) for row in rows]


def parse_time(text):
    text = text.strip()
    if not text or text.upper() == "NA":
        return None
    return float(text)


class Figure:
    def __init__(self, rows, columns, names):
        self.queries = [row["Query"] for row in rows]
        self.series = [Series(c, i, rows, names.get(c.lower())) for i, c in enumerate(columns)]
        times = [t for s in self.series for t in s.times if t is not None]
        if not times:
            raise SystemExit("no query finished on any engine")
        # A decade of headroom under the fastest query and over the slowest,
        # so the `n/a` marks fit under the bars and the legend over them.
        self.ymin = 10 ** np.floor(np.log10(min(times)))
        self.ymax = 10 ** (np.ceil(np.log10(max(times))) + 0.5)
        self.bar_width = GROUP_WIDTH / len(self.series)

    def offset(self, index):
        """How far the bars of the engine at `index` sit from the query's centre."""
        return (index - (len(self.series) - 1) / 2) * self.bar_width

    def matplotlib(self, output):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import scienceplots  # noqa: F401  (registers the styles)
        from matplotlib.ticker import FuncFormatter, NullLocator

        plt.style.use(["science", "no-latex"])

        fig, ax = plt.subplots(figsize=(6.4, 2.4), dpi=300)
        ax.set_yscale("log")
        ax.grid(True, axis="y", which="major", linewidth=0.5, linestyle=(0, (2, 2)), color="0.7")
        ax.set_axisbelow(True)
        x = np.arange(len(self.queries))
        for i, s in enumerate(self.series):
            xs = x + self.offset(i)
            ax.bar([xi for xi, t in zip(xs, s.times) if t is not None],
                   [t for t in s.times if t is not None],
                   self.bar_width, color=s.color, linewidth=0, label=s.label, zorder=3)
            for xi, t in zip(xs, s.times):
                if t is None:
                    ax.text(xi, self.ymin * 1.2, "n/a", rotation=90, ha="center", va="bottom",
                            fontsize=5, color=s.color)
        ax.set_xticks(x)
        ax.set_xticklabels(self.queries, fontsize=6)
        ax.xaxis.set_minor_locator(NullLocator())
        ax.tick_params(axis="x", length=0)
        ax.set_xlim(-0.5 - (1 - GROUP_WIDTH), len(self.queries) - 0.5 + (1 - GROUP_WIDTH))
        ax.set_ylim(self.ymin, self.ymax)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.yaxis.set_minor_locator(NullLocator())
        ax.set_xlabel("TPC-H query")
        ax.set_ylabel("Runtime (s)")
        ax.legend(loc="upper left", ncol=len(self.series), frameon=False,
                  handlelength=1.0, handletextpad=0.4, columnspacing=1.8)
        fig.savefig(output, bbox_inches="tight")

    def tikz(self):
        out = ["% Generated by perf/tpc-h/plot/plot-tpch.py. Do not edit.",
               r"\begin{tikzpicture}"]
        for s in self.series:
            out.append(rf"\definecolor{{{s.tikz_color}}}{{HTML}}{{{s.color.lstrip('#')}}}")
        n = len(self.queries)
        xticks = ",".join(str(i) for i in range(n))
        xticklabels = ",".join(self.queries)
        # Queries sit at 0, 1, 2, ... and every engine's bars are shifted
        # off that by hand, so `bar width` and `bar shift` are in axis units
        # and the groups stay the same shape as the matplotlib figure.
        out.append(rf"""\begin{{axis}}[
  scale only axis, width=0.9\linewidth, height=0.36\linewidth,
  ybar, bar width={self.bar_width:.4g}, bar shift=0pt,
  ymode=log, log origin=infty, ymin={self.ymin:g}, ymax={self.ymax:.4g},
  log ticks with fixed point, yminorticks=false,
  xmin={-0.5 - (1 - GROUP_WIDTH):g}, xmax={n - 0.5 + (1 - GROUP_WIDTH):g},
  xtick={{{xticks}}}, xticklabels={{{xticklabels}}}, xminorticks=false,
  x tick style={{draw=none}},
  xlabel={{TPC-H query}}, ylabel={{Runtime (s)}},
  ymajorgrids, grid style={{line width=0.3pt, dashed, draw=black!30}},
  axis on top=false,
  tick label style={{font=\scriptsize}}, x tick label style={{font=\tiny}},
  label style={{font=\footnotesize}},
  axis line style={{line width=0.4pt}}, tick style={{line width=0.4pt, black}},
  legend pos=north west, legend columns=-1,
  legend style={{draw=none, font=\scriptsize, /tikz/every even column/.append style={{column sep=0.5cm}}}},
  legend image code/.code={{\fill[#1] (0cm,-0.1cm) rectangle (0.3cm,0.1cm);}},
]""")
        for i, s in enumerate(self.series):
            coords = " ".join(f"({x + self.offset(i):.4g},{t:.4g})"
                              for x, t in enumerate(s.times) if t is not None)
            out.append(rf"\addplot[fill={s.tikz_color}, draw=none] coordinates {{{coords}}};")
            out.append(rf"\addlegendentry{{{s.label}}}")
        for i, s in enumerate(self.series):
            for x, t in enumerate(s.times):
                if t is None:
                    out.append(rf"\node[rotate=90, anchor=west, inner sep=1pt, font=\tiny, "
                               rf"text={s.tikz_color}] at (axis cs:{x + self.offset(i):.4g},{self.ymin * 1.2:.4g}) {{n/a}};")
        out.append(r"\end{axis}")
        out.append(r"\end{tikzpicture}")
        return "\n".join(out) + "\n"


if __name__ == "__main__":
    main()
