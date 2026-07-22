# Turso.Serverless.Client

Serverless database driver for [Turso Cloud](https://docs.turso.tech) using only `HttpClient` — the .NET counterpart of [`@tursodatabase/serverless`](https://www.npmjs.com/package/@tursodatabase/serverless). Connects to a database over the SQL over HTTP protocol with streaming cursor support.

Unlike the `Turso` and `Turso.Data.Sqlite` packages, this is a pure managed HTTP client — it has no native component.

## Getting started

```C#
using Turso.Serverless.Client;

await using var connection = new TursoServerlessConnection(new TursoServerlessConnectionOptions
{
    Url = Environment.GetEnvironmentVariable("TURSO_DATABASE_URL")!,
    AuthToken = Environment.GetEnvironmentVariable("TURSO_AUTH_TOKEN"),
});

// Query with positional arguments
var users = await connection.ExecuteAsync("SELECT id, name FROM users WHERE active = ?", [1]);
foreach (var row in users.Rows)
{
    Console.WriteLine($"{row.GetInt64("id")}: {row.GetString("name")}");
}

// Write with named arguments
await connection.ExecuteAsync(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    new Dictionary<string, object?> { [":name"] = "Iku", [":email"] = "iku@example.com" });

// Atomic batch — one round trip, BEGIN/COMMIT/ROLLBACK handled server-side
await connection.BatchAsync(
    [
        new TursoBatchStatement("INSERT INTO logs (msg) VALUES (?)", ["a"]),
        new TursoBatchStatement("INSERT INTO logs (msg) VALUES (?)", ["b"]),
    ],
    TursoBatchMode.Immediate);

// Interactive transaction
await using (var tx = await connection.BeginTransactionAsync(TursoBatchMode.Immediate))
{
    await tx.ExecuteAsync("UPDATE accounts SET balance = balance - 10 WHERE id = ?", [1]);
    await tx.ExecuteAsync("UPDATE accounts SET balance = balance + 10 WHERE id = ?", [2]);
    await tx.CommitAsync();
}
```

## Concurrency model

A `TursoServerlessConnection` is **single-stream**: it runs one statement at a time. This follows from the SQL over HTTP protocol, where each request carries a baton from the previous response to sequence operations on the server. Calls made while another is in flight automatically wait for it to finish.

For parallelism, create multiple connections — constructing one is cheap (no HTTP request is sent until the first statement executes) and `HttpClient` pools the underlying TCP/TLS connections. A shared static `HttpClient` is used by default; pass your own via the second constructor parameter to integrate with `IHttpClientFactory`.

## Features

- `ExecuteAsync` — single statement with positional or named arguments, full result set
- `BatchAsync` — multiple statements in one round trip, optionally atomic (`TursoBatchMode`)
- `SequenceAsync` — semicolon-separated statement scripts (schema migrations)
- `DescribeAsync` — column/parameter metadata without executing
- `BeginTransactionAsync` — interactive transactions (`await using`, rollback on dispose)
- `InTransaction` — authoritative server-side autocommit status
- Per-call or default query timeouts (`TursoTimeoutException`), cancellation tokens throughout
- Encrypted database support via `RemoteEncryptionKey` (`x-turso-encryption-key`)

## Protocol

The driver speaks the same protocol as the JavaScript serverless driver (`serverless/javascript`): `POST /v3/pipeline` for request/response operations and `POST /v3/cursor` for streamed (newline-delimited JSON) results. Integers map to `long`, floats to `double`, text to `string`, blobs to `byte[]`.
