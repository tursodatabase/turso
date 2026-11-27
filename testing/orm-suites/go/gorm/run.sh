#!/usr/bin/env bash
# Run GORM's own tests/ suite against mattn/go-sqlite3 and against Turso.
source "$(dirname "$0")/../common.sh"

build_turso
SRC="$(fetch https://github.com/go-gorm/gorm v1.31.2 gorm)"

# The only upstream change: let GORM_SQLITE_DRIVER pick the database/sql
# driver name (empty = the dialector's default, "sqlite3").
sed -i.orig 's|sqlite.Open(filepath.Join(os.TempDir(), "gorm.db"))|sqlite.New(sqlite.Config{DriverName: os.Getenv("GORM_SQLITE_DRIVER"), DSN: filepath.Join(os.TempDir(), "gorm.db")})|' \
  "$SRC/tests/tests_test.go"
grep -q GORM_SQLITE_DRIVER "$SRC/tests/tests_test.go"
add_turso "$SRC/tests" "$SRC/tests" tests_test

export GORM_DIALECT=sqlite
GORM_SQLITE_DRIVER= run_tests sqlite gorm "$SRC/tests" ./...
GORM_SQLITE_DRIVER=turso run_tests turso gorm "$SRC/tests" ./...
