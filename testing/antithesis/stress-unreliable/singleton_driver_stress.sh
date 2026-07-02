#!/usr/bin/env bash

LD_PRELOAD=/usr/lib/unreliable-libc.so /bin/turso_stress --nr-threads $((1 + RANDOM % 5)) --nr-iterations $((1000 + RANDOM % 9000)) --db-file /tmp/stress.db
