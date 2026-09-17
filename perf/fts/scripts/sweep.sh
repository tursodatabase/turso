#!/usr/bin/env bash
set -euo pipefail

script_dir=$(dirname "$(realpath "$0")")
output=${1:?Usage: bash perf/fts/scripts/sweep.sh NEW_OUTPUT_DIRECTORY}
benchmark=${BENCHMARK:-throughput}
connections=${CONNECTIONS:-1 2 4 8 16 32}
seconds=${SECONDS_PER_RUN:-5}
queries=${QUERIES:-10000}
documents=${DOCUMENTS:-10000}
runs=${RUNS:-3}
case "$benchmark" in
    search) options=(--queries "$queries") ;;
    throughput) options=(--seconds "$seconds") ;;
    *) echo 'BENCHMARK must be search or throughput' >&2; exit 1 ;;
esac
for count in $connections; do
    if (( count <= 0 )); then
        echo 'Every connection count must be positive' >&2
        exit 1
    fi
done
mkdir "$output"
output=$(realpath "$output")
inputs=()
for count in $connections; do
    echo "Benchmarking $benchmark at $count connections"
    bash "$script_dir/run.sh" "$output/c$count" --benchmark "$benchmark" --documents "$documents" \
        --runs "$runs" --connections "$count" "${options[@]}"
    inputs+=("$output/c$count/results.csv")
done
if [[ "${FLAMEGRAPH:-0}" == 1 ]]; then
    exit 0
fi
if [[ "$benchmark" == search ]]; then
    for percentile in p50 p95 p99; do
        uv run "$script_dir/../plot/plot-fts.py" --sweep "${inputs[@]}" --percentile "$percentile" \
            -o "$output/fts-$percentile.png" -o "$output/fts-$percentile.pdf" -o "$output/fts-$percentile.svg"
        uv run "$script_dir/../plot/plot-fts.py" --sweep --relative "${inputs[@]}" --percentile "$percentile" \
            -o "$output/fts-$percentile-speedup.png" -o "$output/fts-$percentile-speedup.pdf" -o "$output/fts-$percentile-speedup.svg"
    done
else
    uv run "$script_dir/../plot/plot-fts.py" --sweep "${inputs[@]}" \
        -o "$output/fts-throughput.png" -o "$output/fts-throughput.pdf" -o "$output/fts-throughput.svg"
    uv run "$script_dir/../plot/plot-fts.py" --sweep --relative "${inputs[@]}" \
        -o "$output/fts-throughput-speedup.png" -o "$output/fts-throughput-speedup.pdf" -o "$output/fts-throughput-speedup.svg"
fi
