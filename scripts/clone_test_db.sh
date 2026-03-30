#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Install sqlite3 locally if needed
"$PROJECT_ROOT/scripts/install-sqlite3.sh"
SQLITE3_BIN="$PROJECT_ROOT/.sqlite3/sqlite3"

rm -f testing/testing_clone.db*
"$SQLITE3_BIN" testing/testing/db '.clone testing/testing_clone.db' > /dev/null
