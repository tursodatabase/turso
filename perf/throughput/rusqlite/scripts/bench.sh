#!/bin/sh

cargo build --release

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"

echo "system,threads,batch_size,compute,throughput"

for threads in 1 2 4 8; do
  for compute in 0 100 500 1000; do
      rm -f write_throughput_test.db*
      "$RELEASE_DIR/write-throughput-sqlite" --threads ${threads} --batch-size 100 --compute ${compute} -i 1000
  done
done
