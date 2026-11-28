#!/usr/bin/env bash
# Runs a command with PG_DSN set to the URL of a PostgreSQL server.
#
#   ./with-postgres.sh sh -c 'echo "$PG_DSN"'
#
# If PG_DSN is already set, the command uses that server. Otherwise this
# script starts a throwaway PostgreSQL container with Docker, runs the
# command, and removes the container.
#
# The default is PostgreSQL 18.7, a minor release of the major version in
# postgres/version. The tag is a minor release because the postgres:18 tag
# moves with each 18.x release, and 18.7 changed behavior that the tests
# expect (ON CONFLICT DO UPDATE checks the row policies before its WHERE).
# To try another version locally, set POSTGRES_VERSION to a tag of the
# postgres Docker image, for example POSTGRES_VERSION=17.
set -euo pipefail

VERSION_FILE="$(dirname "${BASH_SOURCE[0]}")/../version"
DEFAULT_POSTGRES_VERSION=18.7
if [[ "${DEFAULT_POSTGRES_VERSION%%.*}" != "$(cat "$VERSION_FILE")" ]]; then
    echo "error: DEFAULT_POSTGRES_VERSION in $0 is not a release of the major" >&2
    echo "version in postgres/version." >&2
    exit 2
fi
POSTGRES_VERSION="${POSTGRES_VERSION:-$DEFAULT_POSTGRES_VERSION}"

if [[ -n "${PG_DSN:-}" ]]; then
    exec "$@"
fi

if ! docker info >/dev/null 2>&1; then
    echo "error: PG_DSN is not set and Docker is not running." >&2
    echo "Start Docker, or set PG_DSN to a PostgreSQL server, for example" >&2
    echo "PG_DSN=postgres://postgres@127.0.0.1:5432/postgres." >&2
    exit 2
fi

# The mirror can lag behind Docker Hub, so a tag it does not have yet is
# pulled from Docker Hub.
if [[ -z "${POSTGRES_IMAGE:-}" ]]; then
    POSTGRES_IMAGE="mirror.gcr.io/library/postgres:$POSTGRES_VERSION"
    if ! docker pull --quiet "$POSTGRES_IMAGE" >/dev/null 2>&1; then
        POSTGRES_IMAGE="docker.io/library/postgres:$POSTGRES_VERSION"
        docker pull --quiet "$POSTGRES_IMAGE" >/dev/null
    fi
fi

container=$(docker run --detach \
    --env POSTGRES_HOST_AUTH_METHOD=trust \
    --env PGDATA=/pgdata/data \
    --tmpfs /pgdata \
    --publish 127.0.0.1::5432 \
    "$POSTGRES_IMAGE" \
    -c fsync=off \
    -c synchronous_commit=off \
    -c full_page_writes=off \
    -c wal_level=minimal \
    -c max_wal_senders=0 \
    -c autovacuum=off \
    -c jit=off \
    -c max_connections=500)
trap 'docker rm --force --volumes "$container" >/dev/null' EXIT

for _ in $(seq 1 300); do
    if docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done
if ! docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then
    echo "error: PostgreSQL in container $container did not start:" >&2
    docker logs "$container" >&2
    exit 1
fi

port=$(docker port "$container" 5432/tcp | head -n 1 | sed 's/.*://')
export PG_DSN="postgres://postgres@127.0.0.1:${port}/postgres"
echo "Using $POSTGRES_IMAGE at $PG_DSN" >&2
"$@"
