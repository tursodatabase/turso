#!/usr/bin/env bash

RUST_LOG=debug /bin/turso_stress --nr-threads 2 --nr-iterations 10000 --verbose
