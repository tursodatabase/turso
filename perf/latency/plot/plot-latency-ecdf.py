#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Plot transaction latency as an eCDF from turso-txn-latency CSV output.

Usage: uv run plot-latency-ecdf.py sqlite-c{1,8,16,32}.csv turso-c{1,8,16,32}.csv \
           -o latency-ecdf.png -o latency-ecdf.pdf -o latency-ecdf.tikz

Latency along the x axis on a log scale, the share of transactions at or
below it up the y axis. A marker sits on each curve at p50 and p90, and a
dashed vertical line labelled with the value marks its p99.9. A curve's
right-hand end is its slowest transaction.

Each connection count gets its own panel, side by side with shared axes,
so the engines are compared within a panel and the effect of concurrency
is read across panels. One file per engine and count, as `bench.sh`
writes them, plain or gzipped.

`-o` can be given more than once, and each output's format follows its
extension. `.png`, `.pdf` and the other matplotlib formats draw the
figure; `.tikz` or `.tex` write a pgfplots picture for `\\input` into a
LaTeX document that loads pgfplots with its `groupplots` library and
`\\pgfplotsset{compat=1.18}`. The picture sizes itself from
`\\linewidth`, so it fits whatever figure environment contains it.
"""

import argparse
import csv
import gzip
from pathlib import Path

import numpy as np

# The Okabe-Ito palette: colour-blind safe, legible in greyscale, and what
# gnuplot draws with by default, so it looks like the systems papers a
# reader knows. `tikz_mark` is the pgfplots name of the matplotlib `marker`.
ENGINES = {
    "sqlite": {"name": "SQLite", "color": "#E69F00", "marker": "s", "tikz_mark": "square*"},
    "turso": {"name": "Turso", "color": "#0072B2", "marker": "o", "tikz_mark": "*"},
}
FALLBACK_COLORS = ["#009E73", "#D55E00", "#CC79A7"]
FALLBACK_MARKERS = [("^", "triangle*"), ("D", "diamond*"), ("v", "triangle*")]

# Percentiles that get a marker on the curve.
PERCENTILES = [50, 90]
# Percentile that gets a vertical line.
TAIL_PERCENTILE = 99.9
TAIL_STYLE = (0, (4, 2))

COLUMN_LABELS = {
    "total_ns": "Transaction latency",
    "queue_ns": "Time waiting for a free connection",
    "begin_ns": "Time to start the transaction",
    "work_ns": "Time inserting rows",
    "commit_ns": "Time to commit",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs="+", type=Path)
    parser.add_argument("--column", default="total_ns", choices=sorted(COLUMN_LABELS))
    parser.add_argument("-o", "--output", action="append", type=Path, metavar="FILE",
                        help="output file; repeat for several formats "
                             "(default latency-ecdf.png)")
    parser.add_argument("--name", action="append", default=[], metavar="ENGINE=NAME",
                        help="legend name for an engine, e.g. turso=Limbo")
    args = parser.parse_args()
    names = dict(name.split("=", 1) for name in args.name)

    # Files with the same engine and connection count pool their samples:
    # several runs of one configuration make one curve.
    series = {}
    for path in args.csv_files:
        for key, samples in read_series(path, args.column).items():
            series.setdefault(key, []).append(samples)
    series = {k: np.concatenate(v) for k, v in series.items()}
    if not series:
        raise SystemExit("no samples found")

    figure = Figure(series, COLUMN_LABELS[args.column], names)
    for output in args.output or [Path("latency-ecdf.png")]:
        if output.suffix in (".tikz", ".tex"):
            output.write_text(figure.tikz())
        else:
            figure.matplotlib(output)
        print(f"wrote {output}")


def read_series(path, column):
    """Group one CSV file's samples by engine, mode and connection count."""
    series = {}
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", newline="") as f:
        reader = csv.DictReader(f)
        if column not in (reader.fieldnames or []):
            raise SystemExit(f"{path} has no {column} column; is it a samples file?")
        for row in reader:
            if row.get("warmup", "0") == "1":
                continue
            key = (row["engine"], row["mode"], int(row["connections"]))
            series.setdefault(key, []).append(float(row[column]) / 1e6)
    return {k: np.array(v) for k, v in series.items()}


class Figure:
    """The panels and curves of the figure, worked out once for both backends."""

    def __init__(self, series, column_label, names):
        self.column_label = column_label
        modes_per_engine = {}
        for engine, mode, _ in series:
            modes_per_engine.setdefault(engine, set()).add(mode)
        self.panels = []
        for connections in sorted({c for _, _, c in series}):
            panel = {k: v for k, v in series.items() if k[2] == connections}
            curves = []
            for index, ((engine, mode, _), samples) in enumerate(sorted(panel.items())):
                curves.append(Curve(engine, mode, index, samples, modes_per_engine[engine],
                                    names.get(engine)))
            self.panels.append((connections, curves))
        lo = min(float(np.min(v)) for v in series.values())
        hi = max(float(np.max(v)) for v in series.values())
        # Start at a decade, end just past the slowest transaction: rounding
        # the top up to a decade can leave most of a panel empty.
        self.xmin = 10 ** np.floor(np.log10(max(lo, 1e-3)))
        self.xmax = hi * 1.5
        # Panels go in one row.
        self.ncols = len(self.panels)
        self.nrows = 1

    def legend_entries(self):
        """One (label, curve) per engine and mode, in first-seen order."""
        entries = {}
        for _, curves in self.panels:
            for curve in curves:
                entries.setdefault(curve.label, curve)
        return list(entries.items())

    def matplotlib(self, output):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import scienceplots  # noqa: F401  (registers the styles)
        from matplotlib.lines import Line2D
        from matplotlib.ticker import LogFormatterSciNotation, LogLocator, NullLocator

        plt.style.use(["science", "no-latex"])

        ncols, nrows = self.ncols, self.nrows
        fig, axes = plt.subplots(nrows, ncols, figsize=(2.9 * ncols, 2.6 * nrows), dpi=300,
                                 sharex=True, sharey=True, squeeze=False)
        for ax, (connections, curves) in zip(axes.flat, self.panels):
            ax.set_xscale("log")
            ax.grid(True, which="major", linewidth=0.4, alpha=0.6)
            ax.set_axisbelow(True)
            for curve in curves:
                x, y = curve.points(2000)
                ax.plot(x, y, color=curve.color, linewidth=1.3, zorder=3)
                # A marker at p50 and p90, so the values can be read against
                # the grid and the series told apart even where lines overlap.
                ax.plot(curve.marks, PERCENTILES, linestyle="none", marker=curve.marker,
                        markersize=4, color=curve.color, markeredgewidth=1.0, zorder=4)
                # The tail, as a vertical line with the value written along
                # it in the engine's colour. Each engine gets its own half of
                # the height, so the labels stay apart when the lines fall on
                # the same spot.
                ax.axvline(curve.tail, color=curve.color, linewidth=0.9,
                           linestyle=TAIL_STYLE, zorder=2)
                y, va = (45, "top") if curve.index == 0 else (55, "bottom")
                ax.annotate(curve.tail_label, xy=(curve.tail, y), xytext=(-3, 0),
                            textcoords="offset points", rotation=90, ha="right", va=va,
                            color=curve.color, fontsize=6.5, zorder=5)
            ax.set_title(panel_title(connections), loc="left",
                         fontsize=plt.rcParams["font.size"])
        for ax in axes.flat[len(self.panels):]:
            ax.set_visible(False)

        ax0 = axes[0, 0]
        ax0.set_xlim(self.xmin, self.xmax)
        ax0.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
        ax0.xaxis.set_minor_locator(NullLocator())
        # Powers of ten: the tail can span six decades, and plain numbers
        # run into each other.
        ax0.xaxis.set_major_formatter(LogFormatterSciNotation(base=10))
        ax0.set_ylim(0, 103)
        ax0.set_yticks([0, 25, 50, 75, 100])
        for ax in axes[-1, :]:
            ax.set_xlabel(f"{self.column_label} (ms)")
        for ax in axes[:, 0]:
            ax.set_ylabel("Transactions (%)")

        # One legend for the figure, centred below the panels.
        handles = [Line2D([], [], color=curve.color, linewidth=1.3, marker=curve.marker,
                          markersize=4, label=label)
                   for label, curve in self.legend_entries()]
        # The legend sits just under the bottom row's x label, whose height
        # is a fixed share of a one-row figure and shrinks with the rows.
        fig.legend(handles=handles, loc="lower center", ncol=len(handles), frameon=False,
                   handlelength=2.4, columnspacing=2.0,
                   bbox_to_anchor=(0.5, -0.2 / nrows**2))

        fig.savefig(output, bbox_inches="tight")

    def tikz(self):
        out = ["% Generated by perf/latency/plot/plot-latency-ecdf.py. Do not edit.",
               r"\begin{tikzpicture}"]
        colors = {}
        for _, curves in self.panels:
            for curve in curves:
                colors[curve.tikz_color] = curve.color
        for name, color in colors.items():
            out.append(rf"\definecolor{{{name}}}{{HTML}}{{{color.lstrip('#')}}}")
        ncols, nrows = self.ncols, self.nrows
        # Panels in a column share the x axis and panels in a row the y
        # axis, so only the edge panels carry labels; `scale only axis`
        # keeps the panels the same size anyway. The legend hangs below
        # the middle of the last row. It belongs to the last panel, whose
        # axis is (ncols - 1) panels and gaps to the right of the first, so
        # the middle of the row is (1 - ncols / 2) axis widths plus half
        # the gaps to the left of that panel's origin. (pgfplots' `legend
        # to name` would do this, but its label clashes with cleveref.)
        legend_x = 1 - ncols / 2
        legend_xshift = -0.55 * (ncols - 1) / 2
        xticks = [10 ** k for k in range(int(np.log10(self.xmin)), int(np.ceil(np.log10(self.xmax))) + 1)]
        out.append(rf"""\begin{{groupplot}}[
  group style={{
    group name=latency, group size={ncols} by {nrows},
    horizontal sep=0.55cm, vertical sep=0.95cm,
    xlabels at=edge bottom, xticklabels at=edge bottom,
    ylabels at=edge left, yticklabels at=edge left,
  }},
  scale only axis, width={0.84 / ncols:.3f}\linewidth, height=0.24\linewidth,
  xmode=log, log basis x=10, xmin={self.xmin:g}, xmax={self.xmax:.4g},
  xtick={{{",".join(f"{t:g}" for t in xticks)}}},
  xminorticks=false,
  ymin=0, ymax=103, ytick={{0,25,50,75,100}},
  xlabel={{{self.column_label} (ms)}}, ylabel={{Transactions (\%)}},
  grid=major, grid style={{line width=0.3pt, draw=black!25}},
  axis line style={{line width=0.4pt}},
  tick style={{line width=0.4pt, black}},
  tick label style={{font=\scriptsize}}, label style={{font=\footnotesize}},
  every axis title/.style={{font=\footnotesize, at={{(0,1)}}, anchor=south west, yshift=1pt}},
  legend columns=-1,
  legend style={{
    at={{({legend_x:g},0)}}, anchor=north, xshift={legend_xshift:g}cm, yshift=-1.0cm,
    draw=none, font=\footnotesize, /tikz/every even column/.append style={{column sep=0.5cm}},
  }},
  legend image post style={{mark size=1.6pt}},
]""")
        legend = self.legend_entries()
        for panel_index, (connections, curves) in enumerate(self.panels):
            out.append(rf"\nextgroupplot[title={{{panel_title(connections)}}}]")
            if panel_index == len(self.panels) - 1:
                for label, curve in legend:
                    out.append(rf"\addlegendimage{{{curve.tikz_color}, line width=0.9pt, "
                               rf"mark={curve.tikz_mark}}}")
                    out.append(rf"\addlegendentry{{{label}}}")
            for curve in curves:
                x, y = curve.points(400)
                coords = " ".join(f"({xi:.4g},{yi:.4f})" for xi, yi in zip(x, y))
                out.append(rf"\addplot[{curve.tikz_color}, line width=0.9pt, forget plot] "
                           rf"coordinates {{{coords}}};")
                marks = " ".join(f"({m:.4g},{p})" for m, p in zip(curve.marks, PERCENTILES))
                out.append(rf"\addplot[{curve.tikz_color}, only marks, mark={curve.tikz_mark}, "
                           rf"mark size=1.6pt, forget plot] coordinates {{{marks}}};")
                # Same layout as the matplotlib labels: the text runs up the
                # left of the line, the first engine's hanging down from
                # mid-height and the second's rising from it. A rotated node
                # is placed by its unrotated anchor, so south east puts the
                # end of the text at the point and south west its start.
                y, anchor = (45, "south east") if curve.index == 0 else (55, "south west")
                out.append(rf"\draw[{curve.tikz_color}, dashed, line width=0.6pt] "
                           rf"(axis cs:{curve.tail:.4g},0) -- (axis cs:{curve.tail:.4g},103);")
                out.append(rf"\node[{curve.tikz_color}, rotate=90, anchor={anchor}, inner sep=2pt, "
                           rf"font=\tiny] at (axis cs:{curve.tail:.4g},{y}) {{{curve.tail_label}}};")
        out.append(r"\end{groupplot}")
        out.append(r"\end{tikzpicture}")
        return "\n".join(out) + "\n"


class Curve:
    """One engine's samples in one panel, with the numbers drawn on it."""

    def __init__(self, engine, mode, index, samples, modes, name=None):
        self.index = index
        self.samples = samples
        if engine in ENGINES:
            look = ENGINES[engine]
            name, self.color = name or look["name"], look["color"]
            self.marker, self.tikz_mark = look["marker"], look["tikz_mark"]
        else:
            name = name or engine
            self.color = FALLBACK_COLORS[index % len(FALLBACK_COLORS)]
            self.marker, self.tikz_mark = FALLBACK_MARKERS[index % len(FALLBACK_MARKERS)]
        self.label = f"{name} ({mode})" if len(modes) > 1 else name
        self.tikz_color = "".join(ch for ch in f"{engine}{mode}" if ch.isalpha())
        self.marks = np.percentile(samples, PERCENTILES)
        self.tail = float(np.percentile(samples, TAIL_PERCENTILE))
        self.tail_label = f"{fmt_ms(self.tail)} ms"

    def points(self, max_points):
        """Sorted samples and their cumulative percentage, thinned for plotting.

        Points are picked on an even grid and on a log grid running back from
        the slowest sample, so the last handful of transactions still show up
        individually.
        """
        x = np.sort(self.samples)
        n = x.size
        even = np.linspace(0, n - 1, min(max_points, n))
        tail = n - 1 - np.logspace(0, np.log10(n), min(max_points, n))
        idx = np.unique(np.clip(np.concatenate([even, tail]), 0, n - 1).astype(int))
        return x[idx], (idx + 1) / n * 100.0


def panel_title(connections):
    return "1 connection" if connections == 1 else f"{connections} connections"


def fmt_ms(value, _pos=None):
    if value >= 1:
        return f"{value:,.0f}"
    return f"{value:g}"


if __name__ == "__main__":
    main()
