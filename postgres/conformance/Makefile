# Turso PostgreSQL conformance test Makefile
#
# The .sqltest corpora live here; the sqltest runner crate lives in testing/sqltest.

# Paths
ROOT_DIR := $(shell cd ../.. && pwd)
RUNNER_DIR := $(ROOT_DIR)/testing/sqltest

# Use release mode in CI, debug mode locally
ifdef CI
    CARGO_PROFILE := --release
    TARGET_DIR := release
    FUZZER_CARGO_PROFILE := --profile pg-dump-fuzzer
    FUZZER_TARGET_DIR := pg-dump-fuzzer
else
    CARGO_PROFILE :=
    TARGET_DIR := debug
    FUZZER_CARGO_PROFILE :=
    FUZZER_TARGET_DIR := debug
endif

# Honor a CARGO_TARGET_DIR override; default to the workspace target dir.
TARGET_ROOT := $(or $(CARGO_TARGET_DIR),$(ROOT_DIR)/target)
TURSOPG := $(TARGET_ROOT)/$(TARGET_DIR)/tursopg
PGDUMP_FUZZER := $(TARGET_ROOT)/$(FUZZER_TARGET_DIR)/pg-dump-fuzzer
PGDUMP_PROFILE ?= compatible
FUZZ_ARGS ?= --cases 100
SQLTEST := cargo run --manifest-path $(RUNNER_DIR)/Cargo.toml --bin sqltest $(CARGO_PROFILE)

# Test corpora (relative to this directory)
PG_TESTS := pg-sqltests

# 2 * num_cpu (arbitrary multiplier)
POSTGRES_JOBS ?= $(shell expr 2 \* $$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2))

.PHONY: all build build-tursopg build-runner check run test run-one run-postgres run-one-postgres test-pgdump fuzz-pgdump build-dump-fuzzer run-upstream clean

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
	$(SQLTEST) run $(PG_TESTS) --timeout 600 --backend tursopg --binary $(TURSOPG)

# Alias for run
test: run

# Run a single test file: make run-one FILE=pg-sqltests/select.sqltest
run-one: build
	$(SQLTEST) run $(FILE) --backend tursopg --binary $(TURSOPG)

# Run all tests against a real PostgreSQL server. Uses the server at PG_DSN,
# or starts a throwaway Docker container (see with-postgres.sh).
run-postgres: build-runner
	./with-postgres.sh sh -c '$(SQLTEST) run $(PG_TESTS) --jobs $(POSTGRES_JOBS) --backend postgres --postgres-dsn "$$PG_DSN"'

# Run a single test file against a real PostgreSQL server:
# make run-one-postgres FILE=pg-sqltests/select.sqltest
run-one-postgres: build-runner
	./with-postgres.sh sh -c '$(SQLTEST) run $(FILE) --jobs $(POSTGRES_JOBS) --backend postgres --postgres-dsn "$$PG_DSN"'

build-dump-fuzzer:
	cd $(ROOT_DIR) && cargo build $(FUZZER_CARGO_PROFILE) --package turso_pg_dump_fuzzer

test-pgdump: build-tursopg build-dump-fuzzer
	cd $(ROOT_DIR) && cargo test $(FUZZER_CARGO_PROFILE) --package turso_pg_dump_fuzzer
	@for fixture in $(or $(FILE),$(wildcard pgdump/*.sql)); do \
		POSTGRES_TEST_USER=turso POSTGRES_VERSION=$${POSTGRES_VERSION:-18} ./with-postgres.sh "$(PGDUMP_FUZZER)" --binary "$(TURSOPG)" --output pgdump/results replay "$$fixture" || exit $$?; \
	done

fuzz-pgdump: build-tursopg build-dump-fuzzer
	POSTGRES_TEST_USER=turso POSTGRES_VERSION=$${POSTGRES_VERSION:-18} ./with-postgres.sh "$(PGDUMP_FUZZER)" --binary "$(TURSOPG)" --output pgdump/results --profile $(PGDUMP_PROFILE) fuzz $(FUZZ_ARGS)

# Run the upstream PostgreSQL regress corpus with pass/fail statuses.
# Exit 0 means every test matched its status in run.py's STATUS table;
# regressions in blessed tests or unblessed new passes fail the run.
run-upstream:
	./run.py

# Clean build artifacts
clean:
	cargo clean --manifest-path $(RUNNER_DIR)/Cargo.toml
