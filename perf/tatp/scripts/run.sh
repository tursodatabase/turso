#!/bin/sh
# Runs the whole benchmark with its defaults and draws the figures. Takes
# about two hours, and asks for sudo once, at the start, to trim the drive
# and drop the page cache before every run.
set -eu

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}
SIZES=${SIZES:-"10000 100000 1000000"}
export OUT SIZES

"$HERE/scripts/bench.sh"

command -v uv > /dev/null || {
  echo "uv is needed to draw the figures: https://docs.astral.sh/uv/" >&2
  exit 1
}
cd "$OUT"
uv run "$HERE/plot/plot-mqth.py" ./*-result.csv \
    -o mqth.png -o mqth.pdf -o mqth.tikz
# The response times are drawn for the middle size of the sweep.
set -- $SIZES
middle=$(eval echo "\${$(( ($# + 1) / 2 ))}")
uv run "$HERE/plot/plot-response-times.py" --subscribers "$middle" ./*-transactions.csv \
    -o response-times.png -o response-times.pdf
