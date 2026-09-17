#!/usr/bin/env bash
set -euo pipefail

root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
output=${1:?Usage: bash perf/fts/scripts/run.sh NEW_OUTPUT_DIRECTORY [benchmark options]}
shift
mkdir "$output"
output=$(realpath "$output")
profile=${PROFILE:-bench-profile}
case "$profile" in
    dev|bench-profile) ;;
    *) echo 'PROFILE must be dev or bench-profile' >&2; exit 1 ;;
esac
cargo_options=()
results=results.csv
case "${FLAMEGRAPH:-0}" in
    0) ;;
    1)
        if ! command -v inferno-flamegraph >/dev/null; then
            echo 'Install the external renderer with: cargo install inferno --locked' >&2
            exit 1
        fi
        cargo_options=(--features flamegraph)
        set -- "$@" --flamegraph "$output/profiles"
        results=profiled-results.csv
        ;;
    *) echo 'FLAMEGRAPH must be 0 or 1' >&2; exit 1 ;;
esac
{
    date -u
    uname -a
    rustc -Vv
    git -C "$root" rev-parse HEAD
    git -C "$root" status --short
    printf 'profile=%s\narguments=' "$profile"
    printf '%q ' "$@"
    printf '\n'
    if command -v lscpu >/dev/null; then lscpu; fi
    if [[ "${FLAMEGRAPH:-0}" == 1 ]]; then
        printf 'flamegraph_renderer=%s\n' "$(command -v inferno-flamegraph)"
    fi
} > "$output/environment.txt"
cargo run --manifest-path "$root/Cargo.toml" --profile "$profile" -p fts-benchmark "${cargo_options[@]}" -- "$@" \
    > "$output/$results" 2> "$output/bench.log"
if [[ "${FLAMEGRAPH:-0}" == 0 ]]; then
    uv run "$root/perf/fts/plot/plot-fts.py" "$output/results.csv" \
        -o "$output/fts.png" -o "$output/fts.pdf" -o "$output/fts.svg"
else
    for folded in "$output"/profiles/*.folded; do
        inferno-flamegraph < "$folded" > "${folded%.folded}.svg"
    done
fi
