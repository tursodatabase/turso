# Turso Test Runner Makefile

# Paths
ROOT_DIR := $(shell cd .. && pwd)
SQLITE := $(ROOT_DIR)/.sqlite3/sqlite3

# Use release mode in CI, debug mode locally
ifdef CI
    CARGO_PROFILE := --release
    TARGET_DIR := release
else
    CARGO_PROFILE :=
    TARGET_DIR := debug
endif

TURSODB := $(ROOT_DIR)/target/$(TARGET_DIR)/tursodb
TEST_RUNNER := ./target/$(TARGET_DIR)/turso-test-runner

.PHONY: all build build-tursodb build-runner check run test run-examples install-sqlite test-sqlite convert clean

# Build everything
all: build

# Build both tursodb and test runner
build: build-tursodb build-runner

# Build tursodb CLI
build-tursodb:
	cd $(ROOT_DIR) && cargo build $(CARGO_PROFILE) --package turso_cli

# Build the test runner
build-runner:
	cargo build $(CARGO_PROFILE)

# Check syntax of all test files
check: build-runner
	$(TEST_RUNNER) check tests
	$(TEST_RUNNER) check turso-tests

# MVCC mode flag (set MVCC=1 to enable)
ifdef MVCC
    MVCC_FLAG := --mvcc
else
    MVCC_FLAG :=
endif

# Run all tests
run: build
	$(TEST_RUNNER) run tests --binary $(TURSODB) $(MVCC_FLAG)
	$(TEST_RUNNER) run turso-tests --binary $(TURSODB) $(MVCC_FLAG)

# Alias for run
test: run

# Run tests with filter
run-filter: build
	$(TEST_RUNNER) run tests --binary $(TURSODB) --filter $(FILTER) $(MVCC_FLAG)

# Run a single test file: make run-one FILE=tests/select/memory.sqltest
run-one: build
	$(TEST_RUNNER) run $(FILE) --binary $(TURSODB) $(MVCC_FLAG)

# Run only core tests
run-tests: build
	$(TEST_RUNNER) run tests --binary $(TURSODB) $(MVCC_FLAG)

# Run only turso-specific tests
run-turso-tests: build
	$(TEST_RUNNER) run turso-tests --binary $(TURSODB) $(MVCC_FLAG)

# Run example tests
run-examples: build
	$(TEST_RUNNER) run examples --binary $(TURSODB) $(MVCC_FLAG)

# Install SQLite binary
install-sqlite:
	$(ROOT_DIR)/scripts/install-sqlite3.sh

# Run tests against SQLite
test-sqlite: build-runner install-sqlite
	$(TEST_RUNNER) run tests --binary $(SQLITE)

# Convert Tcl .test files to .sqltest format
# Usage: make convert                    - convert all .test files in ../testing to ./converted
#        make convert INPUT=path/to/file - convert specific file(s)
#        make convert OUTPUT=./out       - convert to custom output directory
INPUT ?= $(ROOT_DIR)/testing
OUTPUT ?= ./converted
convert: build-runner
	$(TEST_RUNNER) convert -o $(OUTPUT) $(INPUT)

# Convert with verbose output
convert-verbose: build-runner
	$(TEST_RUNNER) convert -v -o $(OUTPUT) $(INPUT)

# Clean build artifacts
clean:
	cargo clean
