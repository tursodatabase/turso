#!/bin/sh
# Runs the whole benchmark with its defaults and draws the figures. Takes
# about 40 minutes, and asks for sudo before every run, to trim the drive
# and drop the page cache.
set -eu

HERE="$(cd "$(dirname "$0")/.." && pwd)"
CONNS=${CONNS:-"1 2 4 8 16"}
OUT=${OUT:-"$HERE/plot"}
export CONNS OUT

"$HERE/scripts/bench.sh"

command -v uv > /dev/null || {
  echo "uv is needed to draw the figures: https://docs.astral.sh/uv/" >&2
  exit 1
}
cd "$OUT"
uv run "$HERE/plot/plot-tpcc.py" tpmc ./*-result.csv \
    -o tpmc.png -o tpmc.pdf -o tpmc.tikz
for conns in $CONNS; do
  uv run "$HERE/plot/plot-tpcc.py" timeline ./*-c"$conns"-r*-timeline.csv \
      -o "timeline-c$conns.png" -o "timeline-c$conns.pdf" -o "timeline-c$conns.tikz"
  uv run "$HERE/plot/plot-tpcc.py" response-time ./*-c"$conns"-r*-hist.csv \
      -o "response-time-c$conns.png" -o "response-time-c$conns.pdf" \
      -o "response-time-c$conns.tikz"
done
