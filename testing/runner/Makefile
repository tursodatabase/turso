# Turso runner-tests Makefile
#
# Tests in `testing/runner/tests/` are executed via the `test-runner` binary
# defined in `testing/sqltests/Cargo.toml`. This Makefile is a thin wrapper
# that lets `make -C testing/runner <target>` operate on this tests dir.

ROOT_DIR := $(shell cd ../.. && pwd)
SQLITE := $(ROOT_DIR)/.sqlite3/sqlite3

ifdef CI
    CARGO_PROFILE := --release
    TARGET_DIR := release
else
    CARGO_PROFILE :=
    TARGET_DIR := debug
endif

TURSODB := $(ROOT_DIR)/target/$(TARGET_DIR)/tursodb
TEST_RUNNER := cargo run --manifest-path $(ROOT_DIR)/testing/sqltests/Cargo.toml --bin test-runner $(CARGO_PROFILE) --

TESTS_DIR := $(ROOT_DIR)/testing/runner/tests

.PHONY: all build build-tursodb build-runner check run test test-sqlite run-cli install-sqlite clean

all: build

build: build-tursodb build-runner

build-tursodb:
	cd $(ROOT_DIR) && cargo build $(CARGO_PROFILE) --package turso_cli

build-runner:
	cd $(ROOT_DIR)/testing/sqltests && cargo build $(CARGO_PROFILE)

check: build-runner
	$(TEST_RUNNER) check $(TESTS_DIR)

install-sqlite:
	$(ROOT_DIR)/scripts/install-sqlite3.sh

test-sqlite: build-runner install-sqlite
	$(TEST_RUNNER) run $(TESTS_DIR) --backend cli --binary $(SQLITE)

run-cli: build
	$(TEST_RUNNER) run $(TESTS_DIR) --backend cli --binary $(TURSODB)

run: run-cli

test: run-cli

clean:
	cd $(ROOT_DIR)/testing/sqltests && cargo clean
