#!/usr/bin/env bash

source "$(dirname "$0")/../random.sh"

sqlite3 /tmp/stress.db 'vacuum'

/bin/turso_stress --nr-threads "$(random_range 1 5)" --nr-iterations "$(random_range 1000 9999)" --vfs memory_yield --db-file /tmp/stress.db
