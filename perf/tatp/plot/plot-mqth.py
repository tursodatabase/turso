#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Draw TATP Mean Qualified Throughput (MQTh) against database size, one
panel per connection count.

Usage: uv run plot-mqth.py plot/*-result.csv \
           -o mqth.png -o mqth.pdf -o mqth.tikz

Every result file is one run. Each panel is the figure the SQLite VLDB
paper (Gaffney et al., PVLDB 15, Figure 2) draws for TATP: the number of
subscribers on the x axis and a bar per engine. The panels share one
linear y axis, so reading across them shows what more connections do and
reading within one shows what a bigger database does. A bar is the mean
of the runs of the same engine, size and connection count, with an error
bar of one standard deviation, and is labelled with its value.

`-o` can be given more than once, and each output's format follows its
extension: `.png`, `.pdf` and the other matplotlib formats draw the
figure; `.tikz` or `.tex` write a pgfplots picture for `\\input` into a
LaTeX document that loads pgfplots with the `groupplots` library and
`\\pgfplotsset{compat=1.18}`.
"""

import argparse
import csv
from pathlib import Path

import numpy as np

# The Okabe-Ito palette, as in perf/throughput: colour-blind safe and
# legible in greyscale.
ENGINES = {
    "sqlite": {"name": "SQLite", "color": "#E69F00"},
    "turso": {"name": "Turso", "color": "#0072B2"},
}
FALLBACK_COLORS = ["#009E73", "#D55E00", "#CC79A7"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs="+", type=Path)
    parser.add_argument("-o", "--output", action="append", type=Path, metavar="FILE",
                        help="output file; repeat for several formats (default mqth.png)")
    parser.add_argument("--name", action="append", default=[], metavar="ENGINE=NAME",
                        help="legend name for an engine, e.g. turso=Limbo")
    args = parser.parse_args()
    names = dict(name.split("=", 1) for name in args.name)

    runs = {}
    for path in args.csv_files:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                key = (row["engine"], int(row["subscribers"]), int(row["connections"]))
                runs.setdefault(key, []).append(float(row["mqth"]))
    if not runs:
        raise SystemExit("no results found")

    figure = Figure(runs, names)
    for output in args.output or [Path("mqth.png")]:
        if output.suffix in (".tikz", ".tex"):
            output.write_text(figure.tikz())
        else:
            figure.matplotlib(output)
        print(f"wrote {output}")


class Figure:
    def __init__(self, runs, names):
        self.engines = sorted({e for e, _, _ in runs})
        self.sizes = sorted({s for _, s, _ in runs})
        self.connections = sorted({c for _, _, c in runs})
        # (engine, size, connections) -> (mean, standard deviation)
        self.bars = {key: (float(np.mean(v)), float(np.std(v)) if len(v) > 1 else 0.0)
                     for key, v in runs.items()}
        self.ymax = max(m + sd for m, sd in self.bars.values()) * 1.18
        self.looks = {}
        for i, engine in enumerate(self.engines):
            look = ENGINES.get(engine)
            self.looks[engine] = (
                names.get(engine) or (look["name"] if look else engine),
                look["color"] if look else FALLBACK_COLORS[i % len(FALLBACK_COLORS)],
            )

    def matplotlib(self, output):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import scienceplots  # noqa: F401  (registers the styles)
        from matplotlib.patches import Patch
        from matplotlib.ticker import FuncFormatter

        plt.style.use(["science", "no-latex"])

        panels = len(self.connections)
        fig, axes = plt.subplots(1, panels, figsize=(2.1 * panels + 0.4, 2.4), dpi=300,
                                 sharey=True, squeeze=False)
        width = 0.8 / len(self.engines)
        xs = np.arange(len(self.sizes))
        for ax, connections in zip(axes[0], self.connections):
            ax.grid(True, axis="y", linewidth=0.5, linestyle=(0, (2, 2)), color="0.7")
            ax.set_axisbelow(True)
            for i, engine in enumerate(self.engines):
                _, color = self.looks[engine]
                offset = (i - (len(self.engines) - 1) / 2) * width
                for x, size in zip(xs, self.sizes):
                    bar = self.bars.get((engine, size, connections))
                    if bar is None:
                        continue
                    mean, sd = bar
                    ax.bar(x + offset, mean, width * 0.92, color=color, zorder=2)
                    ax.errorbar(x + offset, mean, yerr=sd, color="0.15", elinewidth=0.6,
                                capsize=1.5, capthick=0.6, zorder=3)
                    ax.annotate(short(mean), (x + offset, mean + sd), xytext=(0, 1.5),
                                textcoords="offset points", ha="center", va="bottom",
                                fontsize=5, color="0.2")
            ax.set_xticks(xs, [size_label(s) for s in self.sizes])
            ax.tick_params(axis="x", which="both", length=0, labelsize=7)
            ax.tick_params(axis="y", labelsize=7)
            ax.set_xlim(-0.6, len(self.sizes) - 0.4)
            ax.set_title(connections_label(connections), fontsize=8)
            ax.set_xlabel("Subscribers", fontsize=8)
        axes[0, 0].set_ylim(0, self.ymax)
        axes[0, 0].yaxis.set_major_formatter(FuncFormatter(lambda v, _: short(v)))
        axes[0, 0].set_ylabel("MQTh (transactions/s)", fontsize=8)

        handles = [Patch(facecolor=color, label=name) for name, color in self.looks.values()]
        fig.legend(handles=handles, loc="lower center", ncol=len(handles), frameon=False,
                   fontsize=8, handlelength=1.0, handleheight=0.8, columnspacing=1.8,
                   bbox_to_anchor=(0.5, -0.06))
        fig.tight_layout(rect=(0, 0.06, 1, 1))
        fig.savefig(output, bbox_inches="tight")
        plt.close(fig)

    def tikz(self):
        out = ["% Generated by perf/tatp/plot/plot-mqth.py. Do not edit.",
               r"\begin{tikzpicture}"]
        for engine in self.engines:
            _, color = self.looks[engine]
            out.append(rf"\definecolor{{{tikz_color(engine)}}}{{HTML}}{{{color.lstrip('#')}}}")
        coords = ",".join(size_label(s) for s in self.sizes)
        bar_width = 0.8 / len(self.engines)
        out.append(rf"""\begin{{groupplot}}[
  group style={{group size={len(self.connections)} by 1, horizontal sep=0.35cm, y descriptions at=edge left}},
  width=0.36\linewidth, height=0.32\linewidth, scale only axis=false,
  ybar=0pt, bar width={bar_width:.2f}, symbolic x coords={{{coords}}}, xtick=data,
  enlarge x limits=0.25, ymin=0, ymax={self.ymax:.4g},
  xlabel={{Subscribers}}, scaled y ticks=false,
  yticklabel style={{/pgf/number format/1000 sep={{}}}},
  tick label style={{font=\scriptsize}}, label style={{font=\footnotesize}},
  title style={{font=\footnotesize}},
  xtick style={{draw=none}}, ymajorgrids, grid style={{line width=0.3pt, dashed, draw=black!30}},
  error bars/y dir=both, error bars/y explicit,
  error bars/error mark options={{line width=0.4pt, mark size=1pt, rotate=90}},
  error bars/error bar style={{line width=0.4pt, black!85}},
  nodes near coords, nodes near coords style={{font=\tiny, text=black!80, yshift=1pt}},
  point meta=explicit symbolic,
  legend columns=-1, legend to name=mqthlegend,
  legend style={{draw=none, font=\scriptsize, /tikz/every even column/.append style={{column sep=0.5cm}}}},
]""")
        for p, connections in enumerate(self.connections):
            ylabel = r", ylabel={MQTh (transactions/s)}" if p == 0 else ""
            out.append(rf"\nextgroupplot[title={{{connections_label(connections)}}}{ylabel}]")
            for engine in self.engines:
                name, _ = self.looks[engine]
                points = []
                for size in self.sizes:
                    bar = self.bars.get((engine, size, connections))
                    if bar is not None:
                        mean, sd = bar
                        points.append(f"({size_label(size)},{mean:.4g}) +- (0,{sd:.4g}) [{short(mean)}]")
                out.append(rf"\addplot[fill={tikz_color(engine)}, draw=none] coordinates "
                           rf"{{{' '.join(points)}}};")
                if p == 0:
                    out.append(rf"\addlegendentry{{{name}}}")
        out.append(r"\end{groupplot}")
        out.append(r"\node[anchor=north] at ($(group c1r1.south west)!0.5!"
                   rf"(group c{len(self.connections)}r1.south east) + (0,-0.9cm)$) "
                   r"{\pgfplotslegendfromname{mqthlegend}};")
        out.append(r"\end{tikzpicture}")
        return "\n".join(out) + "\n"


def tikz_color(engine):
    return "".join(ch for ch in engine if ch.isalpha())


def size_label(subscribers):
    if subscribers >= 1_000_000 and subscribers % 1_000_000 == 0:
        return f"{subscribers // 1_000_000}M"
    if subscribers >= 1_000 and subscribers % 1_000 == 0:
        return f"{subscribers // 1_000}K"
    return str(subscribers)


def connections_label(connections):
    return "1 connection" if connections == 1 else f"{connections} connections"


def short(value):
    """A value with two significant figures, in thousands when it is that big."""
    if value >= 1000:
        thousands = value / 1000
        return f"{thousands:.0f}k" if thousands >= 10 else f"{thousands:.1f}k"
    return f"{value:.2g}" if value < 10 else f"{value:.0f}"


if __name__ == "__main__":
    main()
