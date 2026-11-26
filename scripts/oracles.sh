#!/usr/bin/env bash
# Sets up the reference engines that a change is compared against.
#
#   scripts/oracles.sh            sqlite3 and PostgreSQL
#   scripts/oracles.sh --sources  also clone the SQLite and PostgreSQL sources, to read
#   scripts/oracles.sh --stop     stop and remove the PostgreSQL container
#
# - sqlite3 is the version in sqlite/version. It goes to .sqlite3/ in this
#   checkout (scripts/install-sqlite3.sh). scripts/diff.sh uses the sqlite3 on PATH.
# - PostgreSQL is the version in postgres/version, in a Docker container that
#   listens on 127.0.0.1:54329. If the container is already running, the script reuses it.
# - Sources go to ${TURSO_ORACLE_CACHE:-~/.cache/turso-oracles}, at the same versions.
#
# It is safe to run again. It needs no sudo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER=turso-oracle-postgres
PORT=54329
CACHE="${TURSO_ORACLE_CACHE:-$HOME/.cache/turso-oracles}"
SQLITE_VERSION="$(cat "$ROOT/sqlite/version")"
PG_VERSION="$(cat "$ROOT/postgres/version")"
IMAGE="${POSTGRES_IMAGE:-mirror.gcr.io/library/postgres:$PG_VERSION}"

want_sources=0
case "${1:-}" in
    "") ;;
    --sources) want_sources=1 ;;
    --stop)
        docker rm --force "$CONTAINER" >/dev/null 2>&1 || true
        echo "stopped $CONTAINER"
        exit 0
        ;;
    *)
        echo "usage: $0 [--sources|--stop]" >&2
        exit 2
        ;;
esac

setup_sqlite() {
    local local_bin="$ROOT/.sqlite3/sqlite3"
    if [[ ! -x "$local_bin" ]]; then
        if command -v sqlite3 >/dev/null 2>&1 && [[ "$(sqlite3 --version | cut -d' ' -f1)" == "$SQLITE_VERSION" ]]; then
            echo "sqlite3 $SQLITE_VERSION is on PATH"
            return
        fi
        "$ROOT/scripts/install-sqlite3.sh"
    fi
    echo "sqlite3 $SQLITE_VERSION: $local_bin"
    echo "  put it first on PATH, because scripts/diff.sh uses the sqlite3 on PATH:"
    echo "  export PATH=\"$ROOT/.sqlite3:\$PATH\""
}

setup_postgres() {
    if ! docker info >/dev/null 2>&1; then
        echo "PostgreSQL: Docker is not running. Start it, or set PG_DSN to a PostgreSQL $PG_VERSION server." >&2
        return 1
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)" != true ]]; then
        docker rm --force "$CONTAINER" >/dev/null 2>&1 || true
        docker run --detach --name "$CONTAINER" \
            --env POSTGRES_HOST_AUTH_METHOD=trust \
            --publish "127.0.0.1:$PORT:5432" \
            "$IMAGE" -c fsync=off -c max_connections=200 >/dev/null
    fi
    local _
    for _ in $(seq 150); do # gives up after 30 s
        docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && break
        sleep 0.2
    done
    if ! docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
        echo "PostgreSQL did not start. See: docker logs $CONTAINER" >&2
        return 1
    fi
    echo "PostgreSQL $(docker exec "$CONTAINER" postgres --version | awk '{print $3}'): container $CONTAINER"
    echo "  export PG_DSN=postgres://postgres@127.0.0.1:$PORT/postgres"
    echo "  psql:   psql \"\$PG_DSN\"   (or: docker exec -it $CONTAINER psql -U postgres)"
    echo "  stop:   scripts/oracles.sh --stop"
}

clone_tag() { # clone_tag URL TAG DIR
    if [[ -d "$3/.git" ]]; then
        echo "source already at $3"
        return
    fi
    mkdir -p "$(dirname "$3")"
    git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$2" "$1" "$3.tmp"
    mv "$3.tmp" "$3"
    echo "source: $3"
}

setup_sources() {
    rm -rf "$CACHE"/*.tmp
    clone_tag https://github.com/sqlite/sqlite.git "version-$SQLITE_VERSION" "$CACHE/sqlite-$SQLITE_VERSION"
    # Clone the source at the running server's exact release, so the two match.
    local full tag
    full="$(docker exec "$CONTAINER" postgres --version 2>/dev/null | awk '{print $3}' || true)"
    if [[ -n "$full" ]]; then
        tag="REL_${full//./_}"
    else
        tag="REL_${PG_VERSION}_STABLE"
    fi
    clone_tag https://github.com/postgres/postgres.git "$tag" "$CACHE/postgres-${full:-$PG_VERSION}"
}

setup_sqlite
pg_ok=0
setup_postgres && pg_ok=1
if [[ $want_sources -eq 1 ]]; then
    setup_sources
fi
[[ $pg_ok -eq 1 ]]
