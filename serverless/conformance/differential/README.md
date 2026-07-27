# Serverless Differential Tests

Property-based differential tests for the Turso serverless drivers. The
promise of `@tursodatabase/serverless` is that it behaves exactly like the
embedded driver, just over HTTP. This suite puts that promise under test:
it generates random sequences of database operations, runs each sequence
against the embedded driver and against the serverless driver talking to a
live Turso Cloud database, and fails on any divergence in results, result
shapes, value types, or error outcomes.

Because the workloads are generated, the suite finds divergences nobody
thought to write a test for: type mapping drift, transaction state leaking
across statements, parameter binding corner cases, error paths that leave a
connection wedged. When a case fails, fast-check shrinks it to a minimal
reproducing operation sequence.

All language harnesses share one operation vocabulary,
[`spec/ops.json`](spec/ops.json), described in
[`operations.md`](operations.md). This directory holds only the shared
spec; each harness lives next to the driver it tests. The JavaScript
harness is in [`serverless/javascript/differential`](../../javascript/differential)
and the Rust harness in [`serverless/rust/differential`](../../rust/differential);
harnesses for other serverless drivers join their drivers as those land.

## Running against `@tursodatabase/serverless`

### 1. Create a scratch database

The tests create, fill, and drop tables (named `t_<prefix>_<n>`). Point them
at a dedicated scratch database, never at one you care about:

```console
$ turso db create serverless-differential
$ export TURSO_DATABASE_URL="$(turso db show --url serverless-differential)"
$ export TURSO_AUTH_TOKEN="$(turso db tokens create serverless-differential)"
```

### 2. Build the two drivers

The embedded side is the native `@tursodatabase/database` package, which
needs a Rust toolchain to build:

```console
$ cd bindings/javascript
$ npm install
$ npm run build:native
```

The serverless side is built from `serverless/javascript`:

```console
$ cd serverless/javascript
$ npm install
$ npm run build
```

### 3. Run the suite

```console
$ cd serverless/javascript/differential
$ npm install
$ npm test
```

Without `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` set, the tests skip
themselves, so the suite is safe to run in environments without credentials.

### Tuning

Each property test runs 10 generated cases by default, sized for a remote
database over the network. For a thorough run, crank up the iteration count:

```console
$ HEGEL_NUM_RUNS=100 npm test
```

Every case can issue dozens of HTTP round trips, so wall clock time scales
with both the iteration count and your latency to the database region.

## Running against `turso_serverless`

The Rust harness compares the embedded `turso` crate, running on an
in-memory database, against `turso_serverless` talking to the same scratch
database as above. It reads the same environment variables and skips
itself when they are not set:

```console
$ export TURSO_DATABASE_URL="$(turso db show --url serverless-differential)"
$ export TURSO_AUTH_TOKEN="$(turso db tokens create serverless-differential)"
$ cargo test -p turso_serverless_differential
```

`HEGEL_NUM_RUNS` controls the iteration count here too. The properties are
driven by [hegel](https://crates.io/crates/hegeltest), which shrinks a
failing case to a minimal operation sequence and stores it locally so the
next run replays it first.

## What the harness checks

- **API parity**: random operation sequences (DDL, DML, queries, parameter
  binding, transactions, batches, triggers, error cases) must produce
  identical results from both drivers: success or failure, column names and
  counts, row counts, value type tags, cell values (with float epsilon),
  affected row counts, and last insert rowids. The Rust harness also
  compares declared column types.
- **Error recovery**: after any failing statement, the connection must still
  execute the next statement. Errors must never wedge a stream.
- **DDL in transactions**: a `CREATE TABLE` inside a transaction must be
  visible to later statements of the same transaction, including through
  `prepare()`, which in the serverless driver involves a server round trip
  on the same stream.

## Reading a failure

A failing case prints the operation it diverged on, both drivers' results,
and the full operation trace of the case, along with the fast-check seed
and counterexample. Re-running with the same seed replays the exact case;
fast-check shrinks failures to a minimal operation sequence first, so start
from the last (smallest) counterexample it reports.
