MINIMUM_RUST_VERSION := 1.73.0
CURRENT_RUST_VERSION := $(shell rustc -V | sed -E 's/rustc ([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
CURRENT_RUST_TARGET := $(shell rustc -vV | grep host | cut -d ' ' -f 2)
RUSTUP := $(shell command -v rustup 2> /dev/null)
UNAME_S := $(shell uname -s)
MINIMUM_TCL_VERSION := 8.6

# Executable used to execute the compatibility tests.
SQLITE_EXEC ?= scripts/limbo-sqlite3
RUST_LOG := off

all: check-rust-version build 
.PHONY: all

install-sqlite:
	./scripts/install-sqlite3.sh
.PHONY: install-sqlite

check-rust-version:
	@echo "Checking Rust version..."
	@if [ "$(shell printf '%s\n' "$(MINIMUM_RUST_VERSION)" "$(CURRENT_RUST_VERSION)" | sort -V | head -n1)" = "$(CURRENT_RUST_VERSION)" ]; then \
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

build: check-rust-version
	cargo build
.PHONY: build

turso-c:
	cargo cbuild
.PHONY: turso-c

uv-sync:
	uv sync --all-packages
.PHONE: uv-sync

uv-sync-test:
	uv sync --all-extras --dev --package turso_test
.PHONE: uv-sync

test: build uv-sync-test test-compat test-alter-column test-vector test-sqlite3 test-shell test-memory test-write test-update test-constraint test-collate test-extensions test-mvcc test-matviews test-json
.PHONY: test

test-extensions: build uv-sync-test
	RUST_LOG=$(RUST_LOG) uv run --project limbo_test test-extensions
.PHONY: test-extensions

test-shell: build uv-sync-test
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-shell
.PHONY: test-shell

test-compat: check-tcl-version
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/all.test

test-compat-mvcc: check-tcl-version
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=scripts/turso-mvcc-sqlite3 ./testing/all-mvcc.test

test-single: check-tcl-version
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make test-single TEST=path/to/test.test"; \
		exit 1; \
	fi
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/$(TEST)
.PHONY: test-single
.PHONY: test-compat

test-vector:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/vector.test
.PHONY: test-vector

test-time:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/time.test
.PHONY: test-time

test-matviews:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/materialized_views.test
.PHONY: test-matviews

test-alter-column:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/alter_column.test
.PHONY: test-alter-column


reset-db:
	./scripts/clone_test_db.sh
.PHONY: reset-db

test-sqlite3: reset-db
	cargo test -p turso_sqlite3 --test compat -- --test-threads=1
	./scripts/clone_test_db.sh
	cargo test -p turso_sqlite3 --test compat --features sqlite3 -- --test-threads=1
.PHONY: test-sqlite3

test-json:
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/json.test
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) ./testing/json_object_star.test
.PHONY: test-json

test-memory: build uv-sync-test
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-memory
.PHONY: test-memory

test-write: build uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-write; \
	else \
		echo "Skipping test-write: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-write

test-update: build uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-update; \
	else \
		echo "Skipping test-update: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-update

test-collate: build uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-collate; \
	else \
		echo "Skipping test-collate: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-collate

test-constraint: build uv-sync-test
	@if [ "$(SQLITE_EXEC)" != "scripts/limbo-sqlite3" ]; then \
		RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-constraint; \
	else \
		echo "Skipping test-constraint: SQLITE_EXEC does not have indexes scripts/limbo-sqlite3"; \
	fi
.PHONY: test-constraint

test-mvcc: build uv-sync-test
	RUST_LOG=$(RUST_LOG) SQLITE_EXEC=$(SQLITE_EXEC) uv run --project limbo_test test-mvcc;
.PHONY: test-mvcc

bench-vfs: uv-sync-test build-release
	RUST_LOG=$(RUST_LOG) uv run --project limbo_test bench-vfs "$(SQL)" "$(N)"

bench-sqlite: uv-sync-test build-release
	RUST_LOG=$(RUST_LOG) uv run --project limbo_test bench-sqlite "$(VFS)" "$(SQL)" "$(N)"

clickbench:
	./perf/clickbench/benchmark.sh
.PHONY: clickbench

build-release: check-rust-version
	cargo build --bin tursodb --release --features=tracing_release

bench-exclude-tpc-h:
	@benchmarks=$$(cargo bench --bench 2>&1 | grep -A 1000 '^Available bench targets:' | grep -v '^Available bench targets:' | grep -v '^ *$$' | grep -v 'tpc_h_benchmark' | xargs -I {} printf -- "--bench %s " {}); \
	if [ -z "$$benchmarks" ]; then \
		echo "No benchmarks found (excluding tpc_h_benchmark)."; \
		exit 1; \
	else \
		cargo bench $$benchmarks; \
	fi
.PHONY: bench-exclude-tpc-h

docker-cli-build:
	docker build -f Dockerfile.cli -t turso-cli .

docker-cli-run:
	docker run -it -v ./:/app turso-cli

merge-pr:
ifndef PR
	$(error PR is required. Usage: make merge-pr PR=123)
endif
	@echo "Setting up environment for PR merge..."
	@if [ -z "$(GITHUB_REPOSITORY)" ]; then \
		REPO=$$(git remote get-url origin | sed -E 's|.*github\.com[:/]([^/]+/[^/]+?)(\.git)?$$|\1|'); \
		if [ -z "$$REPO" ]; then \
			echo "Error: Could not detect repository from git remote"; \
			exit 1; \
		fi; \
		export GITHUB_REPOSITORY="$$REPO"; \
	else \
		export GITHUB_REPOSITORY="$(GITHUB_REPOSITORY)"; \
	fi; \
	echo "Repository: $$REPO"; \
	AUTH=$$(gh auth status); \
	if [ -z "$$AUTH" ]; then \
		echo "auth: $$AUTH"; \
		echo "GitHub CLI not authenticated. Starting login process..."; \
		gh auth login --scopes repo,workflow; \
	else \
		if ! echo "$$AUTH" | grep -q "workflow"; then \
			echo "Warning: 'workflow' scope not detected. You may need to re-authenticate if merging PRs with workflow changes."; \
			echo "Run: gh auth refresh -s repo,workflow"; \
		fi; \
	fi; \
	if [ "$(LOCAL)" = "1" ]; then \
	    echo "merging PR #$(PR) locally"; \
		uv run scripts/merge-pr.py $(PR) --local; \
	else \
	    echo "merging PR #$(PR) on GitHub"; \
		uv run scripts/merge-pr.py $(PR); \
	fi

.PHONY: merge-pr

sim-schema: 
	mkdir -p  simulator/configs/custom
	cargo run -p limbo_sim -- print-schema > simulator/configs/custom/profile-schema.json

test-shuttle:
	RUSTFLAGS='--cfg tokio_unstable --cfg shuttle' cargo nextest run --profile shuttle --package turso_core

test-loom:
	RUSTFLAGS='--cfg tokio_unstable --cfg loom' cargo nextest run --profile loom --package turso_core