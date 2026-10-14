#!/usr/bin/env bash

# Integrity checks run in a separate step without the fault injecting libc, so
# an injected write or sync error cannot be mistaken for corruption.

db_file=$(mktemp /tmp/stress-unreliable.XXXXXX.db)

LD_PRELOAD=/usr/lib/unreliable-libc.so /bin/turso_stress --nr-threads 2 --nr-iterations 10000 --db-file "$db_file" --skip-integrity-check
stress_status=$?

python3 /opt/antithesis/test/v1/stress-unreliable/helper_integrity_check.py "$db_file"
check_status=$?

if [ "$stress_status" -ne 0 ]; then
    exit "$stress_status"
fi
exit "$check_status"
