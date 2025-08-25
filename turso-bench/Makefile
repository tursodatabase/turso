# TursoDB Benchmark Tool Makefile

.PHONY: build release test clean examples help install

# Default target
all: build

# Build in debug mode
build:
	cargo build

# Build in release mode
release:
	cargo build --release

# Run tests
test:
	cargo test

# Clean build artifacts
clean:
	cargo clean
	rm -rf results/

# Run example benchmarks
examples: release
	@echo "Running example benchmarks..."
	./examples/run_benchmarks.sh

# Install dependencies and build
install: build

# Quick test run
quick-test: release
	@echo "Running quick benchmark test..."
	./target/release/turso-bench --access-mode 0 --transactions 100 --num-threads 1 --path ":memory:" --quiet

# Performance test with multiple threads
perf-test: release
	@echo "Running performance test..."
	./target/release/turso-bench \
		--access-mode 0 \
		--transactions 5000 \
		--num-threads 8 \
		--path ":memory:" \
		--enable-mvcc \
		--latency-file "perf_latency.txt"

# Memory benchmark
memory-test: release
	@echo "Running in-memory benchmark..."
	./target/release/turso-bench \
		--access-mode 0 \
		--transactions 10000 \
		--num-threads 4 \
		--path ":memory:" \
		--enable-mvcc \
		--enable-views

# File-based benchmark
file-test: release
	@echo "Running file-based benchmark..."
	mkdir -p test_data
	./target/release/turso-bench \
		--access-mode 0 \
		--transactions 1000 \
		--num-threads 2 \
		--path "./test_data/file_test.db" \
		--num-tables 5

# Help
help:
	@echo "TursoDB Benchmark Tool"
	@echo ""
	@echo "Available targets:"
	@echo "  build       - Build in debug mode"
	@echo "  release     - Build in release mode"
	@echo "  test        - Run tests"
	@echo "  clean       - Clean build artifacts"
	@echo "  examples    - Run example benchmarks"
	@echo "  quick-test  - Run a quick test"
	@echo "  perf-test   - Run performance test"
	@echo "  memory-test - Run in-memory benchmark"
	@echo "  file-test   - Run file-based benchmark"
	@echo "  help        - Show this help message"
	@echo ""
	@echo "For more options, run: ./target/release/turso-bench --help"