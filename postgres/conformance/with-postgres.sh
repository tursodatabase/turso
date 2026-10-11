#!/usr/bin/env bash
# Runs a command with PG_DSN pointing at a PostgreSQL server.
#
# If PG_DSN is already set (for example to a CI service container), the
# command uses that server. Otherwise this starts a throwaway PostgreSQL
# container with Docker, runs the command, and removes the container.
#
#   ./with-postgres.sh sh -c 'echo "$PG_DSN"'
#
# The PostgreSQL version follows the grammar of the parser (pg_query 6.x is
# PostgreSQL 17), so the server checks the same language the frontend parses.
set -euo pipefail

POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:17}"

if [[ -n "${PG_DSN:-}" ]]; then
    exec "$@"
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "error: PG_DSN is not set and docker is not installed." >&2
    echo "Set PG_DSN to a PostgreSQL server whose user can create databases," >&2
    echo "for example PG_DSN=postgres://postgres@127.0.0.1:5432/postgres." >&2
    exit 2
fi

container=$(docker run --detach --rm \
    --env POSTGRES_HOST_AUTH_METHOD=trust \
    --publish 127.0.0.1::5432 \
    "$POSTGRES_IMAGE")
trap 'docker stop "$container" >/dev/null' EXIT

# The image runs an init server first and restarts; wait for the final one.
for _ in $(seq 1 120); do
    if docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
if ! docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then
    echo "error: PostgreSQL in container $container did not start" >&2
    exit 1
fi

port=$(docker port "$container" 5432/tcp | head -n 1 | sed 's/.*://')
export PG_DSN="postgres://postgres@127.0.0.1:${port}/postgres"
echo "Using $POSTGRES_IMAGE at $PG_DSN" >&2
"$@"
