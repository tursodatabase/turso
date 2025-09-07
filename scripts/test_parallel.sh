#!/usr/bin/env bash
set -e

REPO_ROOT=$(git rev-parse --show-toplevel)
TESTING_DIR="$REPO_ROOT/testing"
CORES=$1
if [[ -z "$CORES" ]]; then
	CORES=$(nproc || sysctl -n hw.ncpu || echo 4)
fi
SQLITE_EXEC=${SQLITE_EXEC:-sqlite3}
RUST_LOG=${RUST_LOG:-info}

if ! command -v parallel &> /dev/null; then
    echo "GNU Parallel is required. Please install it."
    exit 1
fi

cleanup() {
    echo "Cleaning up temporary test directories..."
    find "$TESTING_DIR/tmp_db" -type d -name "[0-9]*" -exec rm -rf {} \; 2>/dev/null || true
}

trap cleanup EXIT
cleanup

mkdir -p "$TESTING_DIR/tmp_db"

TEST_FILES=($(find "$TESTING_DIR" -name "*.test" -not -path "*/tmp_db/*" -not -path "*/sqlite3/*"))

SKIP_FILES=("$TESTING_DIR/cmdlineshell.test", "$TESTING_DIR/all.test", "$TESTING_DIR/attach.test", "$TESTING_DIR/time.test")
FILTERED_FILES=()

for file in "${TEST_FILES[@]}"; do
    SKIP=false
    for skip_file in "${SKIP_FILES[@]}"; do
        if [[ "$file" == "$skip_file" ]]; then
            SKIP=true
            break
        fi
    done
    
    if [[ "$SKIP" == "false" ]]; then
        FILTERED_FILES+=("$file")
    fi
done

run_test() {
    local job_id=$1
    local test_file=$2
    local test_name=$(basename "$test_file")
    
    echo "=== Job $job_id: Starting test $test_name ==="
    
    "$REPO_ROOT/scripts/generate_db.sh" "$job_id" > /dev/null
    
    local tmp_dir="$TESTING_DIR/tmp_db/$job_id"
    
    export TEST_JOB_ID="$job_id"
    export SQLITE_EXEC="$SQLITE_EXEC"
    export RUST_LOG="$RUST_LOG"
    
    if tclsh "$test_file"; then
        echo "=== Job $job_id: Test $test_name passed ==="
        return 0
    else
        echo "=== Job $job_id: Test $test_name FAILED ==="
        return 1
    fi
}

export -f run_test
export REPO_ROOT TESTING_DIR SQLITE_EXEC RUST_LOG

echo "Running ${#FILTERED_FILES[@]} tests on $CORES cores..."

if parallel --will-cite -j "$CORES" run_test {#} {1} ::: "${FILTERED_FILES[@]}"; then
    echo "All tests passed!"
    exit 0
else
    echo "Some tests failed!"
    exit 1
fi
