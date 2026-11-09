# Shared by with-postgres.sh, pg-docker.sh and in-postgres-image.sh: the
# PostgreSQL image and how to start it in Docker. Source this file; do not run
# it.

POSTGRES_VERSION="${POSTGRES_VERSION:-$(cat "$(dirname "${BASH_SOURCE[0]}")/../version")}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-mirror.gcr.io/library/postgres:$POSTGRES_VERSION}"

# Starts a PostgreSQL container in the background and prints its ID. The
# arguments go to `docker run`, for example --name or --publish. The server is
# fast and not durable, which is fine because the data is thrown away.
start_postgres_container() {
    docker run --detach \
        --env POSTGRES_HOST_AUTH_METHOD=trust \
        --env PGDATA=/pgdata/data \
        --tmpfs /pgdata \
        "$@" \
        "$POSTGRES_IMAGE" \
        -c fsync=off \
        -c synchronous_commit=off \
        -c full_page_writes=off \
        -c wal_level=minimal \
        -c max_wal_senders=0 \
        -c autovacuum=off \
        -c jit=off \
        -c max_connections=500
}

# Waits until PostgreSQL in the container accepts connections. If it does not
# start, prints the container log and exits.
wait_until_ready() {
    local container=$1
    for _ in $(seq 1 300); do
        if docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then
            return
        fi
        sleep 0.1
    done
    echo "error: PostgreSQL in container $container did not start:" >&2
    docker logs "$container" >&2
    exit 1
}
