#!/bin/bash

ARGS=("$@")

# Intercept the seed if it's passed
while [[ $# -gt 0 ]]; do
  case $1 in
    -s=*|--seed=*)
      seed="${1#*=}"
      shift
      ;;
    -s|--seed)
      seed="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
# Otherwise make one up
if [ -z "$seed" ]; then
  # Dump 8 bytes of /dev/random as decimal u64
  seed=$(od -An -N8 -tu8 /dev/random | tr -d ' ')
  ARGS+=("--seed" "${seed}")
  echo "Generated seed for Miri and simulator: ${seed}"
else
  echo "Intercepted simulator seed to pass to Miri: ${seed}"
fi

MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-disable-stacked-borrows -Zmiri-seed=${seed}" cargo +nightly miri run --bin limbo_sim -- "${ARGS[@]}"
