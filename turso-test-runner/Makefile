# Turso Test Runner Makefile

# Paths
ROOT_DIR := $(shell cd .. && pwd)
SQLITE := sqlite3

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

.PHONY: all build build-tursodb build-runner check run test run-examples test-sqlite clean

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

# Run all tests
run: build
	$(TEST_RUNNER) run tests --binary $(TURSODB)
	$(TEST_RUNNER) run turso-tests --binary $(TURSODB)

# Alias for run
test: run

# Run tests with filter
run-filter: build
	$(TEST_RUNNER) run tests --binary $(TURSODB) --filter $(FILTER)

# Run a single test file: make run-one FILE=tests/select/memory.sqltest
run-one: build
	$(TEST_RUNNER) run $(FILE) --binary $(TURSODB)

# Run only core tests
run-tests: build
	$(TEST_RUNNER) run tests --binary $(TURSODB)

# Run only turso-specific tests
run-turso-tests: build
	$(TEST_RUNNER) run turso-tests --binary $(TURSODB)

# Run example tests
run-examples: build
	$(TEST_RUNNER) run examples --binary $(TURSODB)

# Run tests against SQLite
test-sqlite: build-runner
	$(TEST_RUNNER) run tests --binary $(SQLITE)

# Clean build artifacts
clean:
	cargo clean
