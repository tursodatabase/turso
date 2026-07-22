# Turso SQL conformance test Makefile
#
# The .sqltest corpora live here; the sqltest runner crate lives in
# testing/sqltest. All sqltest invocations run with this directory as
# the working directory so that relative @database paths in the corpora
# (e.g. database/integrity_*.db) resolve against sqlite/conformance/database.

# Paths
ROOT_DIR := $(shell cd ../.. && pwd)
SQLITE := $(ROOT_DIR)/.sqlite3/sqlite3
RUNNER_DIR := $(ROOT_DIR)/testing/sqltest

# Use release mode in CI, debug mode locally
ifdef CI
    CARGO_PROFILE := --release
    TARGET_DIR := release
else
    CARGO_PROFILE :=
    TARGET_DIR := debug
endif

TURSODB := $(ROOT_DIR)/target/$(TARGET_DIR)/tursodb
TEST_RUNNER := cargo run --manifest-path $(RUNNER_DIR)/Cargo.toml --bin sqltest $(CARGO_PROFILE)

# Test corpora (relative to this directory)
SQLITE_TESTS := sqlite-sqltests
TURSO_TESTS := turso-sqltests

.PHONY: all build build-tursodb build-runner check run test run-examples install-sqlite test-sqlite convert clean run-js build-js-bindings

# Build everything
all: build

# Build both tursodb and test runner
build: build-tursodb build-runner

# Build tursodb CLI
build-tursodb:
	cd $(ROOT_DIR) && cargo build $(CARGO_PROFILE) --package turso_cli --features test_helper

# Build the test runner
build-runner:
	cargo build --manifest-path $(RUNNER_DIR)/Cargo.toml $(CARGO_PROFILE)

# Check syntax of all test files
check: build-runner
	$(TEST_RUNNER) check $(SQLITE_TESTS)
	$(TEST_RUNNER) check $(TURSO_TESTS)

# MVCC mode flag (set MVCC=1 to enable)
ifdef MVCC
    MVCC_FLAG := --mvcc
else
    MVCC_FLAG :=
endif

# Backend selection (cli or rust)
BACKEND ?= cli

# Cross-check binary (set CROSS_CHECK_BINARY=path to enable)
ifdef CROSS_CHECK_BINARY
    CROSS_CHECK_FLAG := --cross-check-binary $(CROSS_CHECK_BINARY)
else
    CROSS_CHECK_FLAG :=
endif

# Run all tests with selected backend
run: build
	$(TEST_RUNNER) run $(SQLITE_TESTS) --timeout 600 --backend $(BACKEND) --binary $(TURSODB) $(MVCC_FLAG) $(CROSS_CHECK_FLAG)
	$(TEST_RUNNER) run $(TURSO_TESTS) --backend $(BACKEND) --binary $(TURSODB) $(MVCC_FLAG) $(CROSS_CHECK_FLAG)

# Run tests with CLI backend (spawns tursodb subprocess)
run-cli: build
	$(TEST_RUNNER) run $(SQLITE_TESTS) --timeout 600 --backend cli --binary $(TURSODB) $(MVCC_FLAG) $(CROSS_CHECK_FLAG)
	$(TEST_RUNNER) run $(TURSO_TESTS) --backend cli --binary $(TURSODB) $(MVCC_FLAG) $(CROSS_CHECK_FLAG)

# Run tests with Rust backend (native turso bindings)
run-rust: build-runner
	$(TEST_RUNNER) run $(SQLITE_TESTS) --backend rust $(MVCC_FLAG) $(ARGS)
	$(TEST_RUNNER) run $(TURSO_TESTS) --backend rust $(MVCC_FLAG) $(ARGS)

# Build JavaScript bindings (requires Node.js and yarn)
JS_DIR := $(ROOT_DIR)/sdks/javascript
build-js-bindings:
	cd $(JS_DIR) && yarn install && yarn workspace @tursodatabase/database-common build && yarn workspace @tursodatabase/database build:test-helper

# Run tests with JavaScript backend (Node.js + @tursodatabase/database)
run-js: build-runner build-js-bindings
	$(TEST_RUNNER) run $(SQLITE_TESTS) --backend js $(MVCC_FLAG)
	$(TEST_RUNNER) run $(TURSO_TESTS) --backend js $(MVCC_FLAG)

# Alias for run
test: run

# Run tests with filter
run-filter: build
	$(TEST_RUNNER) run $(SQLITE_TESTS) --binary $(TURSODB) --filter $(FILTER) $(MVCC_FLAG)

# Run a single test file: make run-one FILE=sqlite-sqltests/orderby/memory.sqltest
run-one: build
	$(TEST_RUNNER) run $(FILE) --binary $(TURSODB) $(MVCC_FLAG)

# Run only core tests
run-tests: build
	$(TEST_RUNNER) run $(SQLITE_TESTS) --binary $(TURSODB) $(MVCC_FLAG)

# Run only turso-specific tests
run-turso-tests: build
	$(TEST_RUNNER) run $(TURSO_TESTS) --binary $(TURSODB) $(MVCC_FLAG)

# Run example tests
run-examples: build
	$(TEST_RUNNER) run $(RUNNER_DIR)/examples --binary $(TURSODB) $(MVCC_FLAG)

# Install SQLite binary
install-sqlite:
	$(ROOT_DIR)/scripts/install-sqlite3.sh

# Run tests against SQLite
test-sqlite: build-runner install-sqlite
	$(TEST_RUNNER) run $(SQLITE_TESTS) --timeout 600 --backend cli --binary $(SQLITE)

# Convert Tcl .test files to .sqltest format
# Usage: make convert                    - convert all .test files in ../testing to ./converted
#        make convert INPUT=path/to/file - convert specific file(s)
#        make convert OUTPUT=./out       - convert to custom output directory
INPUT ?= $(ROOT_DIR)/testing/system
OUTPUT ?= ./converted
convert: build-runner
	$(TEST_RUNNER) convert -o $(OUTPUT) $(INPUT)

# Convert with verbose output
convert-verbose: build-runner
	$(TEST_RUNNER) convert -v -o $(OUTPUT) $(INPUT)

# Clean build artifacts
clean:
	cargo clean --manifest-path $(RUNNER_DIR)/Cargo.toml
