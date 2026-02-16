#!/bin/bash
# Collect code coverage from a SQLRight fuzzing corpus.
# Builds a coverage-instrumented binary and replays the corpus through it.
# Usage: ./collect_coverage.sh [CORPUS_DIR] [REPORT_DIR]
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LIMBO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CORPUS_DIR="${1:-$(ls -dt "$SCRIPT_DIR"/results/run_*/primary/queue 2>/dev/null | head -1)}"
REPORT_DIR="${2:-$SCRIPT_DIR/coverage_report}"

COV_PROFILE_DIR=/tmp/turso_coverage_profiles
RUST_SYSROOT=$(rustc --print sysroot)
LLVM_PROFDATA="$RUST_SYSROOT/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata"
LLVM_COV="$RUST_SYSROOT/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-cov"

if [ ! -d "$CORPUS_DIR" ]; then
    echo "Error: corpus directory not found: $CORPUS_DIR"
    echo "Usage: $0 [CORPUS_DIR] [REPORT_DIR]"
    exit 1
fi

echo "=== Building coverage-instrumented tursodb ==="
cd "$LIMBO_ROOT"
RUSTFLAGS="-C instrument-coverage" cargo build --bin tursodb 2>&1 | tail -5
COV_BINARY="$LIMBO_ROOT/target/debug/tursodb"

echo "=== Replaying corpus through coverage binary ==="
rm -rf "$COV_PROFILE_DIR"
mkdir -p "$COV_PROFILE_DIR"

COUNT=0
TOTAL=$(find "$CORPUS_DIR" -maxdepth 1 -type f | wc -l)
echo "Replaying $TOTAL test cases..."

for testcase in "$CORPUS_DIR"/*; do
    [ -f "$testcase" ] || continue
    COUNT=$((COUNT + 1))

    LLVM_PROFILE_FILE="$COV_PROFILE_DIR/turso_${COUNT}.profraw" \
        timeout 5 "$COV_BINARY" -q -m list \
        --experimental-views --experimental-strict --experimental-triggers \
        --experimental-index-method --experimental-autovacuum --experimental-attach \
        < "$testcase" > /dev/null 2>&1 || true

    if [ $((COUNT % 100)) -eq 0 ]; then
        echo "  Processed $COUNT / $TOTAL test cases..."
    fi
done

echo "  Processed $COUNT test cases total"

echo "=== Merging profile data ==="
PROFDATA="$COV_PROFILE_DIR/merged.profdata"
find "$COV_PROFILE_DIR" -name "*.profraw" -print0 | \
    xargs -0 "$LLVM_PROFDATA" merge -sparse -o "$PROFDATA"

echo "=== Generating coverage report ==="
mkdir -p "$REPORT_DIR"

# Text summary
"$LLVM_COV" report \
    "$COV_BINARY" \
    --instr-profile="$PROFDATA" \
    --ignore-filename-regex='\.cargo|rustc|/usr/' \
    > "$REPORT_DIR/summary.txt" 2>/dev/null

# HTML report
"$LLVM_COV" show \
    "$COV_BINARY" \
    --instr-profile="$PROFDATA" \
    --format=html \
    --output-dir="$REPORT_DIR/html" \
    --ignore-filename-regex='\.cargo|rustc|/usr/' \
    2>/dev/null || echo "HTML report generation failed (non-critical)"

# LCOV export
"$LLVM_COV" export \
    "$COV_BINARY" \
    --instr-profile="$PROFDATA" \
    --format=lcov \
    --ignore-filename-regex='\.cargo|rustc|/usr/' \
    > "$REPORT_DIR/coverage.lcov" 2>/dev/null || echo "LCOV export failed (non-critical)"

echo ""
echo "=== Coverage Summary ==="
head -80 "$REPORT_DIR/summary.txt"
echo ""
echo "Full report: $REPORT_DIR/summary.txt"
echo "HTML report: $REPORT_DIR/html/index.html"
echo "LCOV data:   $REPORT_DIR/coverage.lcov"
