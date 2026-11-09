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

source "$(dirname "${BASH_SOURCE[0]}")/postgres-container.sh"
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

    local image
    if image=$(docker container inspect --format '{{.Config.Image}}' "$PG_CONTAINER" 2>/dev/null); then
        if [[ "$image" != "$POSTGRES_IMAGE" ]]; then
            echo "error: container $PG_CONTAINER runs $image, not $POSTGRES_IMAGE." >&2
            echo "Remove it with '$0 down', then run '$0 up' again." >&2
            exit 2
        fi
        docker start "$PG_CONTAINER" >/dev/null
    else
        start_postgres_container --name "$PG_CONTAINER" --publish "127.0.0.1:${PG_PORT}:5432" >/dev/null
    fi

    wait_until_ready "$PG_CONTAINER"
    echo "PostgreSQL ($POSTGRES_IMAGE) is running in container $PG_CONTAINER."
    echo "  export PG_DSN=postgres://postgres@127.0.0.1:${PG_PORT}/postgres"
}

main "$@"
