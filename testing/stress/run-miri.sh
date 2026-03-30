#!/bin/bash


MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-disable-stacked-borrows" cargo +nightly miri run -p turso_stress -- "$@"
