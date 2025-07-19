MINIMUM_RUST_VERSION := 1.88.0
CURRENT_RUST_VERSION := $(shell rustc -V | sed -E 's/rustc ([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
CURRENT_RUST_TARGET := $(shell rustc -vV | grep host | cut -d ' ' -f 2)
RUSTUP := $(shell command -v rustup 2> /dev/null)
UNAME_S := $(shell uname -s)
MINIMUM_TCL_VERSION := 8.6

# Executable used to execute the compatibility tests.
SQLITE_EXEC ?= scripts/limbo-sqlite3
RUST_LOG := off

all: check-rust-version check-wasm-target limbo limbo-wasm
.PHONY: all

check-rust-version:
	@echo "Checking Rust version..."
	@if [ "$(shell printf '%s\n' "$(MINIMUM_RUST_VERSION)" "$(CURRENT_RUST_VERSION)" | sort -V | head -n1)" != "$(MINIMUM_RUST_VERSION)" ]; then \
		echo "Rust version greater than $(MINIMUM_RUST_VERSION) is required. Current version is $(CURRENT_RUST_VERSION)."; \
		if [ -n "$(RUSTUP)" ]; then \
			echo "Updating Rust..."; \
			rustup update stable; \
		else \
			echo "Please update Rust manually to a version greater than $(MINIMUM_RUST_VERSION)."; \
			exit 1; \
		fi; \
	else \
		echo "Rust version $(CURRENT_RUST_VERSION) is acceptable."; \
	fi
.PHONY: check-rust-version

check-tcl-version:
	@printf '%s\n' \
		'set need "$(MINIMUM_TCL_VERSION)"' \
		'set have [info patchlevel]' \
		'if {[package vcompare $$have $$need] < 0} {' \
		'    puts stderr "tclsh $$have found — need $$need+"' \
		'    exit 1' \
		'}' \
	| tclsh
.PHONY: check-tcl-version

check-wasm-target:
	@echo "Checking wasm32-wasip1 target..."
	@if ! rustup target list | grep -q "wasm32-wasip1 (installed)"; then \
		echo "Installing wasm32-wasip1 target..."; \
		rustup target add wasm32-wasip1; \
	fi
.PHONY: check-wasm-target

limbo:
	cargo build
.PHONY: limbo

limbo-c:
	cargo cbuild
.PHONY: limbo-c

limbo-wasm:
	rustup target add wasm32-wasip1
	cargo build --package limbo-wasm --target wasm32-wasip1
.PHONY: limbo-wasm

uv-sync:
	uv sync --all-packages
.PHONE: uv-sync

uv-sync-test:
	uv sync --all-extras --dev --package turso_test
.PHONE: uv-sync

test: limbo uv-sync-test test-compat test-vector test-sqlite3 test-shell test-memory test-write test-update test-constraint test-collate test-extensions test-mvcc
.PHONY: test

test-extensions: limbo uv-sync-test
	RUST_LOG=$(RUST_LOG) uv run --project limbo_test test-extensions
.PHONY: test-extensions

test-shell: limbo uv-sync-test
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-shell
.PHONY: test-shell

test-compat: check-tcl-version
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/all.test
.PHONY: test-compat

test-vector:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/vector.test
.PHONY: test-vector

test-time:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/time.test
.PHONY: test-time

reset-db:
	./scripts/clone_test_db.sh
.PHONY: reset-db

test-sqlite3: reset-db
	cargo test -p limbo_sqlite3 --test compat
	./scripts/clone_test_db.sh
	cargo test -p limbo_sqlite3 --test compat --features sqlite3
.PHONY: test-sqlite3

test-json:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/json.test
.PHONY: test-json

test-memory: limbo uv-sync-test
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-memory
.PHONY: test-memory

test-write: limbo uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-write; \
	else \
		echo "Skipping test-write: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-write

test-update: limbo uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-update; \
	else \
		echo "Skipping test-update: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-update

test-collate: limbo uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-collate; \
	else \
		echo "Skipping test-collate: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-collate

test-constraint: limbo uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-constraint; \
	else \
		echo "Skipping test-constraint: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-constraint

test-mvcc: limbo uv-sync-test
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-mvcc;
.PHONY: test-mvcc

bench-vfs: uv-sync-test
	cargo build --release
	RUST_LOG=$(RUST_LOG) uv run --project limbo_test bench-vfs "$(SQL)" "$(N)"

clickbench:
	./perf/clickbench/benchmark.sh
.PHONY: clickbench


bench-exclude-tpc-h:
	@benchmarks=$$(cargo bench --bench 2>&1 | grep -A 1000 '^Available bench targets:' | grep -v '^Available bench targets:' | grep -v '^ *$$' | grep -v 'tpc_h_benchmark' | xargs -I {} printf -- "--bench %s " {}); \
	if [ -z "$$benchmarks" ]; then \
		echo "No benchmarks found (excluding tpc_h_benchmark)."; \
		exit 1; \
	else \
		cargo bench $$benchmarks; \
	fi
.PHONY: bench-exclude-tpc-h
