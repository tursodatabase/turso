#!/bin/bash

# Get list of parallel*.py files into an array
test_drivers=($(find . -name "parallel*.py"))

# Exit if no files found
[ ${#test_drivers[@]} -eq 0 ] && exit 1

while true; do
    # Pick a random file from the array
    file=${test_drivers[$RANDOM % ${#test_drivers[@]}]}
    python "$file" || exit $?
done
