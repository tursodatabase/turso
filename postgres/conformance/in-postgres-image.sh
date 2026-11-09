#!/usr/bin/env bash
# Runs a command in a container of the PostgreSQL image of postgres/version,
# so that the command uses the psql and pg_dump of the server version.
#
#   ./in-postgres-image.sh pg_dump --version
#
# With PGDUMP_IN_IMAGE=1, the Makefile starts the dump fuzzer with this script.
# The command must be a Linux program, such as a binary that cargo built on
# Linux. The container uses the host network, so the command can connect to
# servers on 127.0.0.1. The command runs as the current user, in the current
# directory, and it can read and write only the files below the repository
# root and in /tmp. The rest of the container is read only.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/postgres-container.sh"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# The command gets the PG* variables, such as PG_DSN, the RUST_* variables,
# TURSOPG_BINARY, and CI and GITHUB_ACTIONS, because Hegel changes its
# settings in CI.
env_args=()
for name in $(compgen -e); do
    case "$name" in
        PG* | RUST_* | TURSOPG_BINARY | CI | GITHUB_ACTIONS) env_args+=(--env "$name") ;;
    esac
done

# --init makes Ctrl-C stop the command: a process with PID 1 ignores SIGINT
# when it has no handler for it.
exec docker run --rm --init --network host \
    --user "$(id -u):$(id -g)" \
    --read-only --tmpfs /tmp \
    --volume "$root:$root" --workdir "$PWD" \
    "${env_args[@]}" \
    "$POSTGRES_IMAGE" "$@"
