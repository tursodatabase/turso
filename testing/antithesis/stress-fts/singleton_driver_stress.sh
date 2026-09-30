#!/usr/bin/env bash

exec /bin/turso_stress --fts --nr-threads 2 --nr-iterations 10000 "$@"
