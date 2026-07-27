# Turso Serverless Driver for Python

A pure Python driver for Turso Cloud that speaks the [SQL over HTTP
protocol](https://github.com/tursodatabase/turso/blob/main/serverless/PROTOCOL.md).
Designed for serverless and edge environments: no persistent connections,
no native extensions, just HTTP requests from the standard library.

The API implements [DB-API 2.0](https://peps.python.org/pep-0249/) and
mirrors the embedded [`turso`](https://pypi.org/project/turso/) driver, so
the same application code can run against a local database or Turso Cloud.

## Usage

```python
import turso_serverless

conn = turso_serverless.connect(
    "libsql://my-db.turso.io",
    auth_token="...",
)

conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
conn.commit()

for row in conn.execute("SELECT id, name FROM users"):
    print(row)

conn.close()
```

Interactive transactions span multiple HTTP requests; the server keeps the
connection state alive between them:

```python
conn.execute("BEGIN")
conn.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
conn.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
conn.commit()
```

## Conformance tests

The test suite runs against a live database. Point it at a Turso Cloud
instance:

```console
$ export TURSO_DATABASE_URL=libsql://<your-db>.turso.io
$ export TURSO_AUTH_TOKEN=<your-token>
$ uv run --extra dev pytest
```

The tests skip themselves when the environment variables are not set.

## Differential tests

[`differential`](differential/) holds property-based parity tests: random
operation sequences generated from the shared spec
([`serverless/conformance/differential/spec/ops.json`](../conformance/differential/spec/ops.json))
run against both the embedded `pyturso` driver and this driver talking to
Turso Cloud, and any divergence in results, result shapes, value types, or
error outcomes fails the property. With the same environment variables set:

```console
$ uv run --extra differential pytest differential
```

The embedded driver is built from `bindings/python`, so a Rust toolchain
is required. `HEGEL_NUM_RUNS` tunes iterations per property (default 10,
sized for a remote database over the network).
