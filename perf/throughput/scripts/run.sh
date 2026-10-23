#!/bin/sh
# Runs the whole benchmark with its defaults and draws the figures. Takes
# about two hours, and asks for sudo before every run, to trim the drive
# and drop the page cache.
set -eu

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}
export OUT

"$HERE/scripts/bench.sh"

command -v uv > /dev/null || {
  echo "uv is needed to draw the figures: https://docs.astral.sh/uv/" >&2
  exit 1
}
cd "$OUT"
uv run "$HERE/plot/plot-throughput.py" ./*-result.csv \
    -o throughput.png -o throughput.pdf -o throughput.tikz
