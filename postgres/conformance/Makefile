# Turso PostgreSQL conformance test Makefile
#
# The .sqltest corpora live here; the sqltest runner crate lives in
# testing/sqltest. The pg backend spawns a tursopg server per database
# instance and drives it over the PostgreSQL wire protocol.

# Paths
ROOT_DIR := $(shell cd ../.. && pwd)
RUNNER_DIR := $(ROOT_DIR)/testing/sqltest

# Use release mode in CI, debug mode locally
ifdef CI
    CARGO_PROFILE := --release
    TARGET_DIR := release
else
    CARGO_PROFILE :=
    TARGET_DIR := debug
endif

# Honor a CARGO_TARGET_DIR override; default to the workspace target dir.
TARGET_ROOT := $(or $(CARGO_TARGET_DIR),$(ROOT_DIR)/target)
TURSOPG := $(TARGET_ROOT)/$(TARGET_DIR)/tursopg
SQLTEST := cargo run --manifest-path $(RUNNER_DIR)/Cargo.toml --bin sqltest $(CARGO_PROFILE)

# Test corpora (relative to this directory)
PG_TESTS := pg-sqltests

PG_DIFFERENTIAL := $(TARGET_ROOT)/$(TARGET_DIR)/pg-differential
DIFFERENTIAL_CASES ?= 300

.PHONY: all build build-tursopg build-runner check run test run-one run-upstream run-postgres differential clean

# Build everything
all: build

# Build both tursopg and the sqltest runner
build: build-tursopg build-runner

# Build the tursopg server binary
build-tursopg:
	cd $(ROOT_DIR) && cargo build $(CARGO_PROFILE) --package tursopg

# Build the sqltest runner
build-runner:
	cargo build --manifest-path $(RUNNER_DIR)/Cargo.toml $(CARGO_PROFILE)

# Check syntax of all test files
check: build-runner
	$(SQLTEST) check $(PG_TESTS)

# Run all tests against a spawned tursopg server
run: build
	$(SQLTEST) run $(PG_TESTS) --timeout 600 --backend pg --binary $(TURSOPG)

# Alias for run
test: run

# Run a single test file: make run-one FILE=pg-sqltests/select.sqltest
run-one: build
	$(SQLTEST) run $(FILE) --backend pg --binary $(TURSOPG)

# Run the corpus against real PostgreSQL to check that it expects what
# PostgreSQL does. Uses the server at PG_DSN, or a throwaway Docker container.
run-postgres: build-runner
	./with-postgres.sh sh -c '$(SQLTEST) run $(PG_TESTS) --backend pg --postgres-dsn "$$PG_DSN"'

# Run random statements with INSERT, UPDATE and DELETE inside WITH on tursopg
# and on real PostgreSQL, in every mode, and fail on the first difference.
# Uses the server at PG_DSN, or a throwaway Docker container.
differential: build-tursopg
	cd $(ROOT_DIR) && cargo build $(CARGO_PROFILE) --package turso_pg_differential
	./with-postgres.sh sh -c 'for mode in "" "--transaction" "--mvcc --transaction" "--foreign-keys"; do \
		$(PG_DIFFERENTIAL) --postgres-dsn "$$PG_DSN" --tursopg $(TURSOPG) \
			--cases $(DIFFERENTIAL_CASES) --failure-file pg-differential-failure.sql $$mode || exit 1; \
	done'

# Run the upstream PostgreSQL regress corpus with pass/fail statuses.
# Exit 0 means every test matched its status in run.py's STATUS table;
# regressions in blessed tests or unblessed new passes fail the run.
run-upstream:
	./run.py

# Clean build artifacts
clean:
	cargo clean --manifest-path $(RUNNER_DIR)/Cargo.toml
