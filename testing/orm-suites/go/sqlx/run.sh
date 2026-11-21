#!/usr/bin/env bash
# Run sqlx's own tests against mattn/go-sqlite3 and against Turso.
source "$(dirname "$0")/../common.sh"

build_turso
SRC="$(fetch https://github.com/jmoiron/sqlx v1.4.0 sqlx)"

# The only upstream change: let SQLX_SQLITE_DRIVER pick the database/sql driver.
sed -i.orig 's|Connect("sqlite3", sqdsn)|Connect(os.Getenv("SQLX_SQLITE_DRIVER"), sqdsn)|' "$SRC/sqlx_test.go"
grep -q SQLX_SQLITE_DRIVER "$SRC/sqlx_test.go"
# sqlx picks the placeholder style by driver name; "turso" uses "?" like "sqlite3".
add_turso "$SRC" "$SRC" sqlx 'var _ = func() bool { BindDriver("turso", QUESTION); return true }()'

export SQLX_POSTGRES_DSN=skip SQLX_MYSQL_DSN=skip
rm -f "$CACHE"/sqlx-*.db*
SQLX_SQLITE_DRIVER=sqlite3 SQLX_SQLITE_DSN="$CACHE/sqlx-sqlite.db" run_tests sqlite sqlx "$SRC" .
SQLX_SQLITE_DRIVER=turso SQLX_SQLITE_DSN="$CACHE/sqlx-turso.db" run_tests turso sqlx "$SRC" .
