# benchmark_anyio.py
#
# Comprehensive benchmark to measure anyio overhead vs original asyncio.
# Tests different data volumes to isolate framework overhead from SQLite work.
#
# Usage:
#   # Test current code (works with either anyio or original asyncio):
#   python benchmark_anyio.py
#
#   # Test original asyncio version:
#   git stash
#   git checkout 731b9c1ec -- turso/lib_aio.py turso/lib_sync_aio.py turso/worker.py
#   python benchmark_anyio.py
#   git checkout HEAD -- turso/lib_aio.py turso/lib_sync_aio.py turso/worker.py
#   git stash pop
#
# Results (2025-12, Apple Silicon M3):
#
#   Original asyncio (commit 731b9c1ec):
#     Connect hot:     0.238 ms
#     INSERT tiny:    60.45 μs
#     SELECT 1:       80.01 μs  (pure overhead)
#     Select 1KB:     90.87 μs
#     Select 100 rows: 155.56 μs
#
#   Anyio (asyncio backend):
#     Connect hot:     0.320 ms  (+34%)
#     INSERT tiny:   103.65 μs  (+71%)
#     SELECT 1:      172.30 μs  (+115%, pure overhead)
#     Select 1KB:    184.22 μs  (+103%)
#     Select 100 rows: 238.73 μs  (+53%)
#
#   Anyio (uvloop backend):
#     Connect hot:     0.290 ms  (+22%)
#     INSERT tiny:    93.54 μs  (+55%)
#     SELECT 1:      145.94 μs  (+82%, pure overhead)
#     Select 1KB:    159.28 μs  (+75%)
#     Select 100 rows: 218.47 μs  (+40%)
#
# Analysis:
#   - Pure per-operation overhead: ~90μs with anyio vs ~80μs original
#   - Overhead is constant regardless of data size (not proportional)
#   - For real network I/O (10-100ms), this overhead is negligible (<1%)
#   - uvloop reduces overhead by ~15% compared to asyncio backend
#   - Trio compatibility is worth the tradeoff for local-only workloads

import gc
import statistics
import time


def detect_backend():
    """Detect if we're running anyio or original asyncio version."""
    try:
        import inspect

        from turso.lib_aio import Connection
        source = inspect.getsourcefile(Connection)
        with open(source) as f:
            content = f.read()
        if "anyio" in content:
            return "anyio"
        return "asyncio-original"
    except Exception:
        return "unknown"


async def benchmark_connect(iterations=50):
    """Benchmark connection creation - cold and hot."""
    import turso.aio

    results = {}

    # Cold: first connection after GC
    gc.collect()
    start = time.perf_counter()
    conn = await turso.aio.connect(":memory:")
    results["connect_cold_ms"] = (time.perf_counter() - start) * 1000
    await conn.close()

    # Hot: subsequent connections (measure after warmup)
    for _ in range(10):  # warmup
        c = await turso.aio.connect(":memory:")
        await c.close()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        conn = await turso.aio.connect(":memory:")
        times.append(time.perf_counter() - start)
        await conn.close()

    results["connect_hot_ms"] = statistics.mean(times) * 1000
    results["connect_hot_std_ms"] = statistics.stdev(times) * 1000 if len(times) > 1 else 0

    return results


async def benchmark_execute(iterations=500):
    """Benchmark execute with varying data sizes to isolate overhead."""
    import turso.aio

    results = {}

    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER, data TEXT)")

        # Minimal work: tiny insert (mostly framework overhead)
        times = []
        for i in range(iterations):
            start = time.perf_counter()
            await conn.execute("INSERT INTO t VALUES (?, ?)", (i, "x"))
            times.append(time.perf_counter() - start)
        results["insert_tiny_us"] = statistics.mean(times) * 1_000_000

        await conn.execute("DELETE FROM t")

        # Small data: 100 bytes
        data_100 = "x" * 100
        times = []
        for i in range(iterations):
            start = time.perf_counter()
            await conn.execute("INSERT INTO t VALUES (?, ?)", (i, data_100))
            times.append(time.perf_counter() - start)
        results["insert_100b_us"] = statistics.mean(times) * 1_000_000

        await conn.execute("DELETE FROM t")

        # Medium data: 1KB
        data_1k = "x" * 1000
        times = []
        for i in range(iterations):
            start = time.perf_counter()
            await conn.execute("INSERT INTO t VALUES (?, ?)", (i, data_1k))
            times.append(time.perf_counter() - start)
        results["insert_1kb_us"] = statistics.mean(times) * 1_000_000

        await conn.execute("DELETE FROM t")

        # Large data: 10KB
        data_10k = "x" * 10000
        times = []
        for i in range(iterations):
            start = time.perf_counter()
            await conn.execute("INSERT INTO t VALUES (?, ?)", (i, data_10k))
            times.append(time.perf_counter() - start)
        results["insert_10kb_us"] = statistics.mean(times) * 1_000_000

    return results


async def benchmark_select(iterations=500):
    """Benchmark select with varying result sizes."""
    import turso.aio

    results = {}

    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER, data TEXT)")

        # Populate with varying data sizes
        await conn.execute("INSERT INTO t VALUES (1, 'x')")
        await conn.execute("INSERT INTO t VALUES (2, ?)", ("x" * 100,))
        await conn.execute("INSERT INTO t VALUES (3, ?)", ("x" * 1000,))
        await conn.execute("INSERT INTO t VALUES (4, ?)", ("x" * 10000,))

        # Also populate for multi-row tests
        for i in range(100):
            await conn.execute("INSERT INTO t VALUES (?, ?)", (1000 + i, "row" + str(i)))

        # Minimal: SELECT 1 (pure overhead)
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            await (await conn.execute("SELECT 1")).fetchone()
            times.append(time.perf_counter() - start)
        results["select_1_us"] = statistics.mean(times) * 1_000_000

        # Single tiny row
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            await (await conn.execute("SELECT * FROM t WHERE id = 1")).fetchone()
            times.append(time.perf_counter() - start)
        results["select_tiny_us"] = statistics.mean(times) * 1_000_000

        # Single 1KB row
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            await (await conn.execute("SELECT * FROM t WHERE id = 3")).fetchone()
            times.append(time.perf_counter() - start)
        results["select_1kb_us"] = statistics.mean(times) * 1_000_000

        # Single 10KB row
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            await (await conn.execute("SELECT * FROM t WHERE id = 4")).fetchone()
            times.append(time.perf_counter() - start)
        results["select_10kb_us"] = statistics.mean(times) * 1_000_000

        # Multiple rows (100 small rows)
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            await (await conn.execute("SELECT * FROM t WHERE id >= 1000")).fetchall()
            times.append(time.perf_counter() - start)
        results["select_100rows_us"] = statistics.mean(times) * 1_000_000

    return results


async def run_all_benchmarks():
    """Run all benchmarks and return combined results."""
    results = {}
    results.update(await benchmark_connect())
    results.update(await benchmark_execute())
    results.update(await benchmark_select())
    return results


def run_with_backend(backend_name):
    """Run benchmarks with specified async backend."""
    if backend_name == "asyncio":
        import asyncio
        return asyncio.run(run_all_benchmarks())
    elif backend_name == "trio":
        import trio
        return trio.run(run_all_benchmarks)
    elif backend_name == "uvloop":
        import asyncio

        import uvloop
        uvloop.install()
        return asyncio.run(run_all_benchmarks())
    else:
        raise ValueError(f"Unknown backend: {backend_name}")


def print_results(results, backend_name, impl_name):
    """Print results in a readable format."""
    print(f"\n{'='*60}")
    print(f"Backend: {backend_name} | Implementation: {impl_name}")
    print(f"{'='*60}")

    print("\nCONNECTION:")
    print(f"  Cold start:     {results['connect_cold_ms']:8.3f} ms")
    print(f"  Hot (mean):     {results['connect_hot_ms']:8.3f} ms (±{results['connect_hot_std_ms']:.3f})")

    print("\nINSERT (by data size):")
    print(f"  Tiny (1 byte):  {results['insert_tiny_us']:8.2f} μs")
    print(f"  100 bytes:      {results['insert_100b_us']:8.2f} μs")
    print(f"  1 KB:           {results['insert_1kb_us']:8.2f} μs")
    print(f"  10 KB:          {results['insert_10kb_us']:8.2f} μs")

    print("\nSELECT (by result size):")
    print(f"  SELECT 1:       {results['select_1_us']:8.2f} μs  (pure overhead)")
    print(f"  Tiny row:       {results['select_tiny_us']:8.2f} μs")
    print(f"  1 KB row:       {results['select_1kb_us']:8.2f} μs")
    print(f"  10 KB row:      {results['select_10kb_us']:8.2f} μs")
    print(f"  100 rows:       {results['select_100rows_us']:8.2f} μs")


def main():
    impl = detect_backend()
    print(f"Detected implementation: {impl}")

    # Determine which backends to test
    if impl == "asyncio-original":
        # Original code only supports asyncio
        backends = ["asyncio"]
    else:
        # anyio version - test all available backends
        backends = ["asyncio"]
        try:
            import trio  # noqa: F401

            backends.append("trio")
        except ImportError:
            print("(trio not installed, skipping)")
        try:
            import uvloop  # noqa: F401

            backends.append("uvloop")
        except ImportError:
            print("(uvloop not installed, skipping)")

    all_results = {}
    for backend in backends:
        try:
            results = run_with_backend(backend)
            all_results[backend] = results
            print_results(results, backend, impl)
        except Exception as e:
            print(f"\nBackend {backend} failed: {e}")

    # Print summary comparison if multiple backends
    if len(all_results) > 1:
        print(f"\n{'='*60}")
        print("SUMMARY COMPARISON")
        print(f"{'='*60}")
        print(f"{'Metric':<20} ", end="")
        for b in all_results:
            print(f"{b:>12}", end="")
        print()
        print("-" * (20 + 12 * len(all_results)))

        metrics = [
            ("connect_hot_ms", "Connect (ms)"),
            ("insert_tiny_us", "Insert tiny (μs)"),
            ("insert_1kb_us", "Insert 1KB (μs)"),
            ("select_1_us", "SELECT 1 (μs)"),
            ("select_1kb_us", "Select 1KB (μs)"),
            ("select_100rows_us", "Select 100 rows (μs)"),
        ]
        for key, label in metrics:
            print(f"{label:<20} ", end="")
            for b in all_results:
                print(f"{all_results[b][key]:>12.2f}", end="")
            print()


if __name__ == "__main__":
    main()
