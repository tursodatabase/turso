#!/bin/bash

# TursoDB Benchmark Examples
# This script demonstrates various benchmark scenarios

set -e

echo "Building turso-bench..."
cargo build --release

TURSO_BENCH="./target/release/turso-bench"

echo "=========================================="
echo "TursoDB Benchmark Examples"
echo "=========================================="

# Create results directory
mkdir -p results

echo ""
echo "1. Basic Insert Performance Test"
echo "----------------------------------"
$TURSO_BENCH \
    --access-mode 0 \
    --transactions 1000 \
    --num-threads 1 \
    --path "./results/basic_insert.db" \
    --latency-file "./results/basic_insert_latency.txt"

echo ""
echo "2. Multi-threaded Insert Test"
echo "------------------------------"
$TURSO_BENCH \
    --access-mode 0 \
    --transactions 500 \
    --num-threads 4 \
    --path "./results/multithread_insert.db" \
    --latency-file "./results/multithread_insert_latency.txt"

echo ""
echo "3. Update Performance Test"
echo "--------------------------"
$TURSO_BENCH \
    --access-mode 1 \
    --transactions 1000 \
    --num-threads 2 \
    --path "./results/update_test.db" \
    --latency-file "./results/update_latency.txt"

echo ""
echo "4. In-Memory Database Test with MVCC"
echo "-------------------------------------"
$TURSO_BENCH \
    --access-mode 0 \
    --transactions 2000 \
    --num-threads 8 \
    --path ":memory:" \
    --enable-mvcc \
    --latency-file "./results/memory_mvcc_latency.txt"

echo ""
echo "5. Comprehensive Multi-Table Test"
echo "----------------------------------"
$TURSO_BENCH \
    --access-mode 0 \
    --transactions 1000 \
    --num-threads 4 \
    --num-tables 5 \
    --num-databases 2 \
    --path "./results/comprehensive_test.db" \
    --enable-mvcc \
    --enable-views \
    --latency-file "./results/comprehensive_latency.txt" \
    --iops-file "./results/comprehensive_iops.txt"

echo ""
echo "6. Delete Performance Test"
echo "--------------------------"
$TURSO_BENCH \
    --access-mode 2 \
    --transactions 500 \
    --num-threads 2 \
    --path "./results/delete_test.db" \
    --latency-file "./results/delete_latency.txt"

echo ""
echo "=========================================="
echo "All benchmarks completed!"
echo "Results saved in ./results/ directory"
echo "=========================================="

# Display summary of result files
echo ""
echo "Generated result files:"
ls -la results/