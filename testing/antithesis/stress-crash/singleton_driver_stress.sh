#!/usr/bin/env bash

# Kill the stress workload with SIGKILL at a random moment, then check that
# Turso can open the database, recover it, and pass integrity checks.

db_file=$(mktemp /tmp/stress-crash.XXXXXX.db)
tx_mode=$(python3 -c 'from antithesis.random import random_choice; print(random_choice(["sqlite", "concurrent"]))')
kill_after=$(python3 -c 'from antithesis.random import get_random; print(get_random() % 60 + 1)')

/bin/turso_stress --nr-threads 2 --nr-iterations 100000 --tx-mode "$tx_mode" --db-file "$db_file" &
stress_pid=$!
sleep "$kill_after"
kill -9 "$stress_pid"
wait "$stress_pid"
stress_status=$?

# 137 means the process died from our SIGKILL. 0 means it finished before
# the kill. Anything else is a failure inside the workload.
if [ "$stress_status" -ne 0 ] && [ "$stress_status" -ne 137 ]; then
    exit "$stress_status"
fi

exec python3 /opt/antithesis/test/v1/stress-crash/helper_recovery_check.py "$db_file"
