# Testing in Limbo

Limbo supports a comprehensive testing system to ensure correctness, performance, and compatibility with SQLite.

## 1. Compatibility Tests

The `make test` target is the main entry point.

Most compatibility tests live in the testing/ directory and are written in SQLite’s TCL test format. These tests ensure that Limbo matches SQLite’s behavior exactly. The database used during these tests is located at testing/testing.db, which includes the following schema:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone_number TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    age INTEGER
);
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT,
    price REAL
);
CREATE INDEX age_idx ON users (age);
```

You can freely write queries against these tables during compatibility testing.

### Shell and Python-based Tests

For cases where output or behavior differs intentionally from SQLite (e.g. due to new features or limitations), tests should be placed in the testing/cli_tests/ directory and written in Python.

These tests use the TestLimboShell class:

```python
from cli_tests.common import TestLimboShell

def test_uuid():
    limbo = TestLimboShell()
    limbo.run_test_fn("SELECT uuid4_str();", lambda res: len(res) == 36)
    limbo.quit()
```

You can use run_test, run_test_fn, or run_debug to interact with the shell and validate results. 
The constructor takes an optional argument with the `sql` you want to initiate the tests with.  You can also enable blob testing or override the executable and flags.

Use these Python-based tests for validating:

  - Output formatting

  - Shell commands and .dot interactions

  - Limbo-specific extensions in `testing/cli_tests/extensions.py`

  - Any known divergence from SQLite behavior


> Logging and Tracing
If you wish to trace internal events during test execution, you can set the RUST_LOG environment variable before running the test. For example:

```bash
RUST_LOG=none,turso_core=trace make test
```

This will enable trace-level logs for the turso_core crate and disable logs elsewhere. Logging all internal traces to the `testing/test.log` file. 

**Note:** trace logs can be very verbose—it's not uncommon for a single test run to generate megabytes of logs.


## Deterministic Simulation Testing (DST):

Limbo simulator uses randomized deterministic simulations to test the Limbo database behaviors.

Each simulation begins with a random configurations:

- the database workload distribution(percentages of reads, writes, deletes...),
- database parameters(page size),
- number of reader or writers, etc.

Based on these parameters, we randomly generate **interaction plans**. Interaction plans consist of statements/queries, and assertions that will be executed in order. The building blocks of interaction plans are:

- Randomly generated SQL queries satisfying the workload distribution,
- Properties, which contain multiple matching queries with assertions indicating the expected result.

An example of a property is the following:

```sql
-- begin testing 'Select-Select-Optimizer'
-- ASSUME table marvelous_ideal exists;
SELECT ((devoted_ahmed = -9142609771.541502 AND loving_wicker = -1246708244.164486)) FROM marvelous_ideal WHERE TRUE;
SELECT * FROM marvelous_ideal WHERE (devoted_ahmed = -9142609771.541502 AND loving_wicker = -1246708244.164486);
-- ASSERT select queries should return the same amount of results;
-- end testing 'Select-Select-Optimizer'
```

The simulator starts from an initially empty database, adding random interactions based on the workload distribution. It can
add random queries unrelated to the properties without breaking the property invariants to reach more diverse states and respect the configured workload distribution.

The simulator executes the interaction plans in a loop, and checks the assertions. It can add random queries unrelated to the properties without
breaking the property invariants to reach more diverse states and respect the configured workload distribution.

## Usage

To run the simulator, you can use the following command:

```bash
RUST_LOG=turso_sim=debug cargo run --bin turso_sim
```

The simulator CLI has a few configuration options that you can explore via `--help` flag.

```txt
The Turso deterministic simulator

Usage: turso_sim [OPTIONS]

Options:
  -s, --seed <SEED>                  set seed for reproducible runs
  -d, --doublecheck                  enable doublechecking, run the simulator with the plan twice and check output equality
  -n, --maximum-size <MAXIMUM_SIZE>  change the maximum size of the randomly generated sequence of interactions [default: 5000]
  -k, --minimum-size <MINIMUM_SIZE>  change the minimum size of the randomly generated sequence of interactions [default: 1000]
  -t, --maximum-time <MAXIMUM_TIME>  change the maximum time of the simulation(in seconds) [default: 3600]
  -l, --load <LOAD>                  load plan from the bug base
  -w, --watch                        enable watch mode that reruns the simulation on file changes
      --differential                 run differential testing between sqlite and Limbo
  -h, --help                         Print help
  -V, --version                      Print version
```

## Fuzzing

TODO!



