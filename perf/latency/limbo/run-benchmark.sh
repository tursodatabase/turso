#!/bin/bash

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"

for i in $(seq 10 10 100)
do
  "$RELEASE_DIR/limbo-multitenancy" $i >> results.csv
done
