#!/bin/sh

cargo build --release -p write-throughput

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"

ENGINES=${ENGINES:-"turso sqlite"}
THREADS=${THREADS:-"1 2 3 4"}
COMPUTE=${COMPUTE:-"0 100 500 1000"}
REPEATS=${REPEATS:-1}
BATCH_SIZE=${BATCH_SIZE:-100}
ITERATIONS=${ITERATIONS:-1000}
MODE=${MODE:-concurrent}

echo "system,mode,threads,batch_size,compute,throughput"

for repeat in $(seq 1 "$REPEATS"); do
  for engine in $ENGINES; do
    for threads in $THREADS; do
      for compute in $COMPUTE; do
        rm -f write_throughput_test.db*
        "$RELEASE_DIR/write-throughput" --engine ${engine} --threads ${threads} \
            --batch-size ${BATCH_SIZE} --compute ${compute} \
            -i ${ITERATIONS} --mode ${MODE}
      done
    done
  done
done
