#!/bin/bash
# SQLRight fuzzer for Turso - Quick run script.
# Usage: ./run.sh [--cores N] [--resume]
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LIMBO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CORES=1
RESUME=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cores) CORES="$2"; shift 2 ;;
        --resume) RESUME=true; shift ;;
        *) echo "Unknown option: $1"; echo "Usage: $0 [--cores N] [--resume]"; exit 1 ;;
    esac
done

BUILD_DIR="$SCRIPT_DIR/build"
AFL="$BUILD_DIR/afl-fuzz"
TURSODB="$LIMBO_ROOT/target/debug/tursodb"
SEEDS_DIR="$SCRIPT_DIR/seeds"
OUTPUT="/tmp/sqlright_test"

# Verify setup
if [ ! -x "$AFL" ]; then
    echo "Error: afl-fuzz not found at $AFL"
    echo "Run setup first: $SCRIPT_DIR/setup.sh"
    exit 1
fi

if [ ! -x "$TURSODB" ]; then
    echo "Error: tursodb not found at $TURSODB"
    echo "Build it first: cargo afl build --bin tursodb"
    exit 1
fi

export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1
export AFL_SKIP_CPUFREQ=1
export AFL_MAP_SIZE=2097152
export AFL_OLD_FORKSERVER=1

AFL_CMD="$AFL"
AFL_TARGET="-- $TURSODB -q -m list"

if [ "$RESUME" = true ] && [ -d "$OUTPUT/primary/queue" ]; then
    echo "Resuming from $OUTPUT..."
    INPUT_FLAG="-"
else
    RESUME=false

    # Extract seeds from sqltest corpus
    echo "Extracting seeds from sqltest corpus..."
    cargo run --manifest-path "$LIMBO_ROOT/Cargo.toml" --bin test-runner -- \
        extract-sql "$LIMBO_ROOT/testing/runner/tests/" \
        --output-dir "$SEEDS_DIR"
    echo "Seeds: $(ls "$SEEDS_DIR" | wc -l) files"

    rm -rf "$OUTPUT"
    INPUT_FLAG="$SEEDS_DIR"
fi

# Set up working directory with init_lib and pragma
WORK_DIR=$(mktemp -d /tmp/sqlright_work_XXXXXX)
trap "rm -rf $WORK_DIR" EXIT
cp -r "$BUILD_DIR/init_lib" "$WORK_DIR/"
cp "$BUILD_DIR/pragma" "$WORK_DIR/"
touch "$WORK_DIR/map_id_triggered.txt"
cd "$WORK_DIR"

PIDS=()
cleanup() {
    echo ""
    echo "Stopping all fuzzer instances..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null
    echo "Done."
}
trap "cleanup; rm -rf $WORK_DIR" INT TERM

# Launch primary instance
$AFL_CMD -M primary -i "$INPUT_FLAG" -o "$OUTPUT" -c 0 -O NOREC -m none $AFL_TARGET &
PIDS+=($!)
echo "  primary (PID $!) started"

if [ "$CORES" -gt 1 ]; then
    sleep 2
    for i in $(seq 2 "$CORES"); do
        $AFL_CMD -S "secondary_$i" -i "$INPUT_FLAG" -o "$OUTPUT" -c 0 -O NOREC -m none $AFL_TARGET &
        PIDS+=($!)
        echo "  secondary_$i (PID $!) started"
    done
fi

echo ""
echo "=== Fuzzing with $CORES core(s) ==="
echo "Output: $OUTPUT"
echo "Stats:  cat $OUTPUT/primary/fuzzer_stats"
echo "Stop:   Ctrl+C"
echo ""

wait "${PIDS[0]}" 2>/dev/null || true
cleanup
