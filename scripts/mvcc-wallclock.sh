#!/bin/bash

set -euo pipefail
cd "$(dirname "$0")/.."

SCENARIO="${1:-point_read}"
ITERATIONS="${2:-}"
SAMPLES="${3:-9}"

case "$SCENARIO" in
    point_read|index_read) DEFAULT_ITERATIONS=20000 ;;
    scan_128) DEFAULT_ITERATIONS=2000 ;;
    point_update_rollback|insert_rollback|point_update_commit|insert_commit) DEFAULT_ITERATIONS=5000 ;;
    *) echo "unknown scenario: $SCENARIO" >&2; exit 2 ;;
esac
ITERATIONS="${ITERATIONS:-$DEFAULT_ITERATIONS}"

case "$ITERATIONS:$SAMPLES" in
    *[!0-9:]*|:*|*:) echo "iteration and sample counts must be positive integers" >&2; exit 2 ;;
esac
if (( ITERATIONS == 0 || SAMPLES == 0 )); then
    echo "iteration and sample counts must be positive integers" >&2
    exit 2
fi

cargo build --profile bench-profile -p turso_core --bench mvcc_icount >/dev/null
BENCH_BINARY=$(find target/bench-profile/deps -maxdepth 1 -type f -name 'mvcc_icount-*' ! -name '*.d' -print | head -1)
if [[ -z "$BENCH_BINARY" ]]; then
    echo "mvcc_icount benchmark binary not found" >&2
    exit 1
fi

ICOUNT_SCENARIO="$SCENARIO" ICOUNT_ITERS="$ITERATIONS" WALLCLOCK_SAMPLES=1 \
    "$BENCH_BINARY" >/dev/null

RESULT=$(ICOUNT_SCENARIO="$SCENARIO" ICOUNT_ITERS="$ITERATIONS" WALLCLOCK_SAMPLES="$SAMPLES" \
    "$BENCH_BINARY")
printf '%s\n' "$RESULT"
printf '%s\n' "$RESULT" | awk -F'ns_per_operation=' '
    /mvcc-wallclock:/ { values[count++] = $2 }
    END {
        for (i = 0; i < count; i++) {
            for (j = i + 1; j < count; j++) {
                if (values[j] < values[i]) {
                    tmp = values[i]
                    values[i] = values[j]
                    values[j] = tmp
                }
            }
        }
        median = values[int(count / 2)]
        printf "mvcc-wallclock-summary: median=%d ns/op min=%d max=%d samples=%d\n", \
            median, values[0], values[count - 1], count
    }
'
