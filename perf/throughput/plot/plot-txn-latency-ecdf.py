#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt

STEM = re.compile(
    r"(sqlite|turso)-([^-]+(?:-[^-]+)?)-(\d+)w-b(\d+)-"
    r"([^-]+)-th(-?\d+|na|disabled)(?:-([^-]+))?(?:-([^-]+))?-r(\d+)"
)


def stem_meta(name: str) -> dict:
    m = STEM.match(name)
    if not m:
        return {"stem": name}
    engine, topology, workers, batch, ckpt, _threshold, _extra1, _extra2, repeat = m.groups()
    return {
        "engine": engine,
        "topology": topology,
        "workers": int(workers),
        "batch": int(batch),
        "checkpoint": "wal" if engine == "sqlite" else ckpt,
        "repeat": int(repeat),
    }


def label(meta: dict, stem: str) -> str:
    if meta.get("engine") == "sqlite":
        return "SQLite occupancy-1"
    if "checkpoint" in meta and "workers" in meta:
        return f"Turso {meta['checkpoint']}, {meta['workers']}w"
    return stem


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: plot-txn-latency-ecdf.py TIMELINE_DIR OUT.png")
    timeline_dir = Path(sys.argv[1])
    out = Path(sys.argv[2])
    files = sorted(timeline_dir.glob("*-ecdf.json"))
    if not files:
        print(f"no *-ecdf.json under {timeline_dir}", file=sys.stderr)
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = {
        "wal": "#B279A2",
        "truncate": "#4C78A8",
        "passive": "#54A24B",
    }
    plotted = 0
    seen_labels: set[str] = set()
    for path in files:
        meta = stem_meta(path.name.replace("-ecdf.json", ""))
        data = json.loads(path.read_text())
        xs = [x / 1000.0 for x in data.get("us") or []]
        ys = data.get("cdf") or []
        if not xs or len(xs) != len(ys):
            continue
        series = label(meta, path.stem)
        ckpt = meta.get("checkpoint")
        ax.plot(
            xs,
            ys,
            color=colors.get(ckpt, "#888888"),
            linewidth=2,
            label=series if series not in seen_labels else None,
        )
        seen_labels.add(series)
        plotted += 1

    if plotted == 0:
        print(f"no plottable ecdf series under {timeline_dir}", file=sys.stderr)
        return

    ax.set_xlabel("BEGIN→COMMIT service time (ms)")
    ax.set_ylabel("CDF")
    ax.set_ylim(0, 1)
    ax.set_xlim(left=0)
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(frameon=True)
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=160)
    fig.savefig(out.with_suffix(".pdf"))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
