#!/bin/sh
# Runs the whole benchmark with its defaults and draws the figure. Takes
# about half an hour and asks for sudo once per run, to drop the page cache.
set -eu

HERE="$(cd "$(dirname "$0")/.." && pwd)"
CONNECTIONS=${CONNECTIONS:-"1 8 16 32"}
OUT=${OUT:-"$HERE/plot"}
export CONNECTIONS OUT

"$HERE/scripts/bench.sh"

command -v uv > /dev/null || {
  echo "uv is needed to draw the figure: https://docs.astral.sh/uv/" >&2
  exit 1
}
cd "$OUT"
files=""
for connections in $CONNECTIONS; do
  files="$files $(ls sqlite-c$connections-r*.csv turso-c$connections-r*.csv | grep -v checkpoints)"
done
uv run "$HERE/plot/plot-latency-ecdf.py" $files \
    -o latency-ecdf.png -o latency-ecdf.pdf -o latency-ecdf.tikz
