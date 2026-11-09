#!/usr/bin/env bash
# Keeps a disposable PostgreSQL container running on a fixed localhost port,
# so tools that cannot start Docker themselves can still connect to it.
#
#   ./pg-docker.sh up     start the container (or reuse it) and print PG_DSN
#   ./pg-docker.sh down   remove the container and its data
#
# with-postgres.sh starts a new container for every command. This script
# starts one container and leaves it running. A sandboxed coding agent cannot
# reach the Docker socket, but it can connect to the published port, so run
# `up` from your own shell and then give the agent the printed PG_DSN.
#
# Settings (environment variables):
#   POSTGRES_VERSION  tag of the postgres Docker image, default in postgres/version
#   PG_CONTAINER      container name, default turso-pg
#   PG_PORT           port on 127.0.0.1, default 54329
set -euo pipefail

VERSION_FILE="$(dirname "${BASH_SOURCE[0]}")/../version"
POSTGRES_VERSION="${POSTGRES_VERSION:-$(cat "$VERSION_FILE")}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-mirror.gcr.io/library/postgres:$POSTGRES_VERSION}"
PG_CONTAINER="${PG_CONTAINER:-turso-pg}"
PG_PORT="${PG_PORT:-54329}"

main() {
    case "${1:-}" in
        up) up ;;
        down) docker rm --force --volumes "$PG_CONTAINER" ;;
        *)
            echo "usage: $0 up|down" >&2
            exit 2
            ;;
    esac
}

up() {
    if ! docker info >/dev/null 2>&1; then
        echo "error: Docker is not running (or this shell cannot reach it)." >&2
        exit 2
    fi

    if docker container inspect "$PG_CONTAINER" >/dev/null 2>&1; then
        docker start "$PG_CONTAINER" >/dev/null
    else
        start_new_container
    fi

    wait_until_ready
    echo "PostgreSQL ($POSTGRES_IMAGE) is running in container $PG_CONTAINER."
    echo "  export PG_DSN=postgres://postgres@127.0.0.1:${PG_PORT}/postgres"
}

start_new_container() {
    # Same server settings as with-postgres.sh: fast and not durable, which is
    # fine because the data is thrown away.
    docker run --detach \
        --name "$PG_CONTAINER" \
        --env POSTGRES_HOST_AUTH_METHOD=trust \
        --env PGDATA=/pgdata/data \
        --tmpfs /pgdata \
        --publish "127.0.0.1:${PG_PORT}:5432" \
        "$POSTGRES_IMAGE" \
        -c fsync=off \
        -c synchronous_commit=off \
        -c full_page_writes=off \
        -c wal_level=minimal \
        -c max_wal_senders=0 \
        -c autovacuum=off \
        -c jit=off \
        -c max_connections=500 >/dev/null
}

wait_until_ready() {
    for _ in $(seq 1 300); do
        if docker exec "$PG_CONTAINER" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then
            return
        fi
        sleep 0.1
    done
    echo "error: PostgreSQL in container $PG_CONTAINER did not start:" >&2
    docker logs "$PG_CONTAINER" >&2
    exit 1
}

main "$@"
