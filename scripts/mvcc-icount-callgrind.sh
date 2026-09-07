#!/bin/bash
# Stable MVCC instruction counts on macOS via Callgrind in a Linux container.

set -euo pipefail
cd "$(dirname "$0")/.."

SCENARIO="${1:-point_read}"
SMALL_ITERS="${2:-20}"
LARGE_ITERS="${3:-220}"
IMAGE=turso-mvcc-icount
CARGO_VOLUME=turso-mvcc-icount-cargo-home
RUSTUP_VOLUME=turso-mvcc-icount-rustup-home
TARGET_VOLUME=turso-mvcc-icount-target
REPORT_DIR="target/mvcc-icount-report"

case "$SCENARIO" in
    point_read|index_read|scan_128|point_update_rollback|insert_rollback|point_update_commit) ;;
    *) echo "unknown scenario: $SCENARIO" >&2; exit 2 ;;
esac
case "$SMALL_ITERS:$LARGE_ITERS" in
    *[!0-9:]*|:*|*:) echo "iteration counts must be positive integers" >&2; exit 2 ;;
esac
if (( SMALL_ITERS == 0 || LARGE_ITERS <= SMALL_ITERS )); then
    echo "large_iters must be greater than non-zero small_iters" >&2
    exit 2
fi

mkdir -p "$REPORT_DIR"

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "mvcc-icount: building profiling image"
    docker build -f scripts/mvcc-icount.Dockerfile -t "$IMAGE" scripts/
fi

echo "mvcc-icount: fetching dependencies on host"
cargo fetch

echo "mvcc-icount: seeding cargo cache"
docker run --rm \
    -v "$HOME/.cargo":/host-cargo:ro \
    -v "$CARGO_VOLUME":/cargo-home \
    "$IMAGE" \
    rsync -a --delete \
        --exclude registry/src \
        --exclude git/checkouts \
        --exclude bin \
        --exclude .package-cache \
        /host-cargo/registry /host-cargo/git /cargo-home/

run_in_container() {
    docker run --rm \
        -v "$PWD":/work \
        -v "$CARGO_VOLUME":/cargo-home \
        -v "$RUSTUP_VOLUME":/rustup-home \
        -v "$TARGET_VOLUME":/target \
        -e CARGO_HOME=/cargo-home \
        -e RUSTUP_HOME=/rustup-home \
        -e CARGO_TARGET_DIR=/target \
        -e CARGO_NET_OFFLINE=true \
        -w /work \
        "$IMAGE" "$@"
}

echo "mvcc-icount: building fixed-iteration harness"
run_in_container bash -c '
    set -euo pipefail
    rustup show active-toolchain >/dev/null
    cargo build --profile bench-profile -p turso_core --bench mvcc_icount
    cp "$(ls -t /target/bench-profile/deps/mvcc_icount-* | grep -v '\''.d$'\'' | head -1)" /target/mvcc-icount-bin
'

run_callgrind() {
    local iterations="$1"
    local output="$2"
    run_in_container bash -c "
        set -euo pipefail
        ICOUNT_ITERS=$iterations ICOUNT_SCENARIO=$SCENARIO \
            valgrind --tool=callgrind --callgrind-out-file=/work/$output \
            /target/mvcc-icount-bin >/dev/null
    "
}

echo "mvcc-icount: Callgrind run 1/2 (iterations=$SMALL_ITERS)"
run_callgrind "$SMALL_ITERS" "$REPORT_DIR/callgrind.$SCENARIO.small.out"
echo "mvcc-icount: Callgrind run 2/2 (iterations=$LARGE_ITERS)"
run_callgrind "$LARGE_ITERS" "$REPORT_DIR/callgrind.$SCENARIO.large.out"

ir_of() {
    awk '/^summary:/ { print $2 }' "$1"
}

SMALL_REPORT="$REPORT_DIR/callgrind.$SCENARIO.small.out"
LARGE_REPORT="$REPORT_DIR/callgrind.$SCENARIO.large.out"
IR_SMALL=$(ir_of "$SMALL_REPORT")
IR_LARGE=$(ir_of "$LARGE_REPORT")
PER_OPERATION=$(( (IR_LARGE - IR_SMALL) / (LARGE_ITERS - SMALL_ITERS) ))

SMALL_ANNOTATION="$REPORT_DIR/annotate.$SCENARIO.small.txt"
LARGE_ANNOTATION="$REPORT_DIR/annotate.$SCENARIO.large.txt"
DIFF_ANNOTATION="$REPORT_DIR/diff.$SCENARIO.txt"
run_in_container bash -c "
    callgrind_annotate --threshold=100 --inclusive=no /work/$SMALL_REPORT \
        > /work/$SMALL_ANNOTATION
    callgrind_annotate --threshold=100 --inclusive=no /work/$LARGE_REPORT \
        > /work/$LARGE_ANNOTATION
"
scripts/callgrind-annotation-diff.py \
    "$SMALL_ANNOTATION" "$LARGE_ANNOTATION" \
    "$(( LARGE_ITERS - SMALL_ITERS ))" > "$DIFF_ANNOTATION"

echo
echo "mvcc-icount[$SCENARIO]: $PER_OPERATION instructions/operation"
echo "  Ir: $IR_SMALL @ $SMALL_ITERS, $IR_LARGE @ $LARGE_ITERS iterations"
echo "  per-function difference: $DIFF_ANNOTATION"
