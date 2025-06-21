# Lazy Record Parsing Performance Analysis

## Overview

This document presents benchmark results comparing Limbo's performance with and without lazy record parsing implementation. The benchmarks demonstrate significant performance improvements, particularly when selecting a subset of columns from a table.

## Benchmark Methodology

### Test Setup

- **Database**: `testing.db` (1,212,416 bytes)
- **Table**: `users` table with multiple columns
- **Test Data**: 1000 rows per query
- **Hardware**: MacBook Air (M3, 8 cores - 4 performance + 4 efficiency, 16GB RAM)
- **Operating System**: macOS Sequoia 15.5
- **Benchmark Tool**: Criterion.rs with 50 samples per test

### Test Queries

1. **1_column**: `SELECT id FROM users LIMIT 1000`
2. **2_columns**: `SELECT id, email FROM users LIMIT 1000`
3. **3_columns**: `SELECT id, email, first_name FROM users LIMIT 1000`
4. **5_columns**: `SELECT id, email, first_name, last_name, age FROM users LIMIT 1000`
5. **all_columns**: `SELECT * FROM users LIMIT 1000`

## Results

### Performance Comparison (Time in microseconds)

| Test Case   | Limbo Main | Limbo Lazy | SQLite | Lazy vs Main     | Lazy vs SQLite |
| ----------- | ---------- | ---------- | ------ | ---------------- | -------------- |
| 1_column    | 121.72     | 64.05      | 16.59  | **47.4% faster** | 3.9x slower    |
| 2_columns   | 153.89     | 93.13      | 36.03  | **39.5% faster** | 2.6x slower    |
| 3_columns   | 180.69     | 118.26     | 43.95  | **34.6% faster** | 2.7x slower    |
| 5_columns   | 220.74     | 157.61     | 65.52  | **28.6% faster** | 2.4x slower    |
| all_columns | 343.84     | 284.23     | 108.32 | **17.3% faster** | 2.6x slower    |

### Visual Performance Improvement

```
Performance Improvement with Lazy Parsing
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1_column   ███████████████████████████████████████████████▍ 47.4%
2_columns  ███████████████████████████████████████▌ 39.5%
3_columns  ██████████████████████████████████▌ 34.6%
5_columns  ████████████████████████████▌ 28.6%
all_columns █████████████████▎ 17.3%
```

## Key Findings

### 1. Significant Performance Gains

- Lazy parsing provides **47.4% improvement** when selecting a single column
- Performance benefit decreases gradually as more columns are selected
- Even when selecting all columns, there's still a **17.3% improvement**

### 2. Consistent Absolute Time Savings

- The absolute time saved is remarkably consistent across all test cases (~55-62 µs)
- This suggests lazy parsing eliminates a fixed overhead in record processing

### 3. Column Selectivity Benefit

- The improvement is most pronounced with fewer columns, which is the expected behavior
- Real-world queries often select only needed columns, making this optimization valuable

### 4. Gap with SQLite

- While Limbo remains slower than SQLite, lazy parsing significantly narrows the gap
- For single column selection, the gap reduced from 7.4x to 3.9x slower
- For 5 columns, the gap is now only 2.4x slower (best relative performance)

## Implementation Impact

### Before Lazy Parsing

- All columns were parsed immediately when a row was accessed
- Unnecessary work for queries selecting only specific columns
- Higher memory allocation and CPU usage

### After Lazy Parsing

- Columns are parsed only when actually accessed
- Zero-copy design eliminates unnecessary allocations
- Streaming comparisons avoid materializing all values
- More efficient memory usage

## Conclusion

The lazy record parsing implementation delivers substantial performance improvements across all tested scenarios, with the greatest benefits for queries selecting fewer columns. This optimization makes Limbo more competitive and efficient for real-world usage patterns where applications typically select only the columns they need.
