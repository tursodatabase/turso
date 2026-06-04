#!/usr/bin/env sh
# Generates .devcontainer/.env so docker-compose can resolve MAIN_GIT_DIR.
#
# /workspace/.git inside the container is bind-mounted from the host. When the
# host workspace is a git worktree, .git is a pointer file containing
#   gitdir: <abs-host-path>/.git/worktrees/<name>
# That absolute host path must exist verbatim inside the container, so we
# bind-mount the main repo's .git dir 1:1.
set -eu

cd "$(dirname "$0")/.."

if [ -f .git ]; then
  gitdir=$(sed -n 's|^gitdir: ||p' .git)
  main_git_dir=${gitdir%/worktrees/*}
elif [ -d .git ]; then
  main_git_dir="$(pwd)/.git"
else
  echo "init-env.sh: no .git found in $(pwd)" >&2
  exit 1
fi

printf 'MAIN_GIT_DIR=%s\n' "$main_git_dir" > .devcontainer/.env
