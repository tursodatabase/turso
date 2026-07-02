#!/usr/bin/env bash

/bin/turso_stress --nr-threads $((1 + RANDOM % 5)) --nr-iterations $((1000 + RANDOM % 9000)) --vfs memory_yield --db-file /tmp/stress.db
