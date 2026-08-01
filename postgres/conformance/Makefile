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

.PHONY: all build build-tursopg build-runner check run test run-one clean

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

# Clean build artifacts
clean:
	cargo clean --manifest-path $(RUNNER_DIR)/Cargo.toml
