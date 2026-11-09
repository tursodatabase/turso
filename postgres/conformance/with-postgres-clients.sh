#!/usr/bin/env bash
# Runs a command with psql and pg_dump from the PostgreSQL image of
# postgres/version first on PATH, so that the clients have the same version as
# the server of with-postgres.sh.
#
#   ./with-postgres-clients.sh pg_dump --version
#   ./with-postgres-clients.sh make fuzz-pgdump
#
# This script starts one container for the command and removes it at the end.
# The psql and pg_dump scripts in docker-clients/ run each call in this
# container with docker exec. The calls use the host network, so they can
# connect to servers on 127.0.0.1. They can read and write only the files below
# the directory where this script starts: the rest of the container is read
# only, so a write to another path fails. This works only when Docker runs on
# Linux.
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$dir/postgres-container.sh"

POSTGRES_CLIENT_CONTAINER=$(docker run --detach --network host \
    --user "$(id -u):$(id -g)" \
    --read-only \
    --volume "$PWD:$PWD" \
    --entrypoint sleep \
    "$POSTGRES_IMAGE" infinity)
trap 'docker rm --force "$POSTGRES_CLIENT_CONTAINER" >/dev/null' EXIT
export POSTGRES_CLIENT_CONTAINER
export PATH="$dir/docker-clients:$PATH"
echo "Using psql and pg_dump from $POSTGRES_IMAGE" >&2
"$@"
