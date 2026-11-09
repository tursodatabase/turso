#!/usr/bin/env bash
# Runs a command with PG_DSN set to the URL of a PostgreSQL server.
#
#   ./with-postgres.sh sh -c 'echo "$PG_DSN"'
#
# If PG_DSN is already set, the command uses that server. Otherwise this
# script starts a throwaway PostgreSQL container with Docker, runs the
# command, and removes the container.
#
# To test against another PostgreSQL version, set POSTGRES_VERSION to a tag
# of the postgres Docker image, for example POSTGRES_VERSION=18. To change the
# version for everyone, change postgres/version.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/postgres-container.sh"

if [[ -n "${PG_DSN:-}" ]]; then
    exec "$@"
fi

if ! docker info >/dev/null 2>&1; then
    echo "error: PG_DSN is not set and Docker is not running." >&2
    echo "Start Docker, or set PG_DSN to a PostgreSQL server, for example" >&2
    echo "PG_DSN=postgres://postgres@127.0.0.1:5432/postgres." >&2
    exit 2
fi

container=$(start_postgres_container --publish 127.0.0.1::5432)
trap 'docker rm --force --volumes "$container" >/dev/null' EXIT
wait_until_ready "$container"

port=$(docker port "$container" 5432/tcp | head -n 1 | sed 's/.*://')
export PG_DSN="postgres://postgres@127.0.0.1:${port}/postgres"
echo "Using $POSTGRES_IMAGE at $PG_DSN" >&2
"$@"
