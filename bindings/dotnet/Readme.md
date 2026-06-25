# Turso .NET

ADO.NET bindings for Turso local and remote databases.

The `Turso.Data.Sqlite` package includes both a SQLite-compatible `Turso.Data.Sqlite` facade and Turso-specific `System.Data.Common` types such as `TursoConnection`, `TursoCommand`, `TursoDataReader`, `TursoParameter`, `TursoTransaction`, and `TursoFactory`.

## Install

```bash
dotnet add package Turso.Data.Sqlite
```

Application code only needs to reference `Turso.Data.Sqlite`.

The package targets `net8.0`, `net9.0`, and `net10.0`. It includes native runtime assets for Windows, Linux, macOS, Android (`android-arm64`, `android-arm`, `android-x64`, and `android-x86`), and iOS as an XCFramework with device and simulator slices.

## Getting started

```C#
using Turso;

using var connection = new TursoConnection("Data Source=:memory:");
connection.Open();

connection.ExecuteNonQuery("CREATE TABLE t(a, b)");
var rowsAffected = connection.ExecuteNonQuery("INSERT INTO t(a, b) VALUES (1, 2), (3, 4)");
Console.WriteLine($"RowsAffected: {rowsAffected}");

using var command = connection.CreateCommand();
command.CommandText = "SELECT * FROM t";
using var reader = command.ExecuteReader();
while (reader.Read())
{
    var a = reader.GetInt32(0);
    var b = reader.GetInt32(1);
    Console.WriteLine($"Value1: {a}, Value2: {b}");
}
```

## ADO.NET usage

Code written against `DbConnection` can use `TursoConnection` directly:

```C#
using System.Data.Common;
using Turso;

await using DbConnection connection = new TursoConnection("Data Source=app.db");
connection.Open();

await using var command = connection.CreateCommand();
command.CommandText = "SELECT $value";
var parameter = command.CreateParameter();
parameter.ParameterName = "$value";
parameter.Value = 42;
command.Parameters.Add(parameter);

var value = command.ExecuteScalar();
```

Remote Turso/libSQL databases can use the same `TursoConnection` surface with a remote URL and auth token:

```C#
await using var connection = new TursoConnection(
    "Data Source=libsql://example-org.turso.io;Auth Token=eyJ...");
await connection.OpenAsync();

await using var command = connection.CreateCommand();
command.CommandText = "SELECT name FROM customers WHERE id = $id";
command.Parameters.Add(new TursoParameter("$id", 42));

var name = await command.ExecuteScalarAsync();
```

Remote mode uses the Hrana HTTP `/v2/pipeline` protocol. `libsql://` URLs default to HTTPS; `Tls=False` maps them to HTTP for local development. `ws://` and `wss://` URLs are accepted and mapped to the equivalent HTTP pipeline endpoint. `Auth Token` requires HTTPS unless the host is `localhost` or loopback.

Remote mode also supports ADO.NET `DbBatch` for latency-sensitive workloads that should be sent in one Hrana batch:

```C#
await using var batch = connection.CreateBatch();

var insert = batch.CreateBatchCommand();
insert.CommandText = "INSERT INTO customers(name) VALUES ($name)";
var name = insert.CreateParameter();
name.ParameterName = "$name";
name.Value = "Alice";
insert.Parameters.Add(name);
batch.BatchCommands.Add(insert);

var select = batch.CreateBatchCommand();
select.CommandText = "SELECT COUNT(*) FROM customers";
batch.BatchCommands.Add(select);

await using var reader = await batch.ExecuteReaderAsync();
```

Embedded replicas are not enabled yet in the .NET provider. `Replica Path` and `Sync Interval` are parsed so applications fail early with clear errors, and `TursoConnection.Sync()` / `SyncAsync(CancellationToken)` are reserved for that mode once `turso_sync_sdk_kit` is packaged and wired through .NET.

Provider factories are available through `TursoFactory.Instance`:

```C#
DbProviderFactory factory = TursoFactory.Instance;
using var connection = factory.CreateConnection();
connection!.ConnectionString = "Data Source=:memory:";
connection.Open();
```

## Migrating from Microsoft.Data.Sqlite

For common embedded SQLite usage, `Turso.Data.Sqlite` exposes a SQLite-compatible facade over the Turso engine:

```diff
- using Microsoft.Data.Sqlite;
+ using Turso.Data.Sqlite;

- using var connection = new SqliteConnection("Data Source=app.db");
+ using var connection = new SqliteConnection("Data Source=app.db");
```

Supported common connection string keywords include:

| Keyword | Notes |
| --- | --- |
| `Data Source` | Database path or `:memory:`. Aliases include `DataSource` and `Filename`. |
| `Mode` | Parsed and preserved for compatibility. |
| `Cache` | Parsed and preserved for compatibility. |
| `Foreign Keys` | Parsed and preserved for compatibility. |
| `Recursive Triggers` | Parsed and preserved for compatibility. |
| `Default Timeout` | Used as the default command timeout. Aliases include `Command Timeout`. |
| `Pooling` | Parsed and preserved for compatibility. |
| `Vfs` | Parsed and preserved for compatibility. |
| `Encryption Cipher` | Turso local encryption cipher. |
| `Encryption Key` | Hex-encoded encryption key used with `Encryption Cipher`. |
| `Auth Token` | Bearer token for remote Turso/libSQL URLs. Aliases include `AuthToken` and `Authentication Token`. |
| `Replica Path` | Reserved for embedded replicas. The .NET provider currently fails early with a clear unsupported error. |
| `Read Your Writes` | Keeps the remote Hrana session baton across commands. Defaults to `True`. Set `False` for stateless one-shot remote requests. |
| `Sync Interval` | Reserved for embedded replicas. Automatic sync is not enabled yet. |
| `Tls` | Optional override for `libsql://` development URLs. Conflicting values with explicit `http://` or `https://` schemes fail early. |

## SQLite-compatible facade coverage

- `Turso.Data.Sqlite` is the migration-oriented facade. It includes SQLite-style connection strings, commands, readers, schema metadata, transactions and savepoints, backup, SQL-backed blob streams, scalar and aggregate UDFs, custom collations, and disabled-by-default extension loading.
- Raw SQLitePCL `sqlite3*` handle interop is intentionally unsupported. `SqliteConnection.Handle` returns `null` rather than exposing a fake SQLite handle.
- `PRAGMA read_uncommitted` is tracked as connection-local state for API compatibility, but Turso does not currently implement SQLite shared-cache dirty reads.
- `SqliteBlob` preserves fixed-length blob stream behavior through SQL reads and writes. It is not yet backed by a native incremental-blob storage handle.
- SQLite virtual-table modules such as FTS3/FTS5 are not built in unless provided by a Turso extension/module.
- Async methods currently use the base ADO.NET behavior rather than a dedicated async native path.

## Entity Framework Core

`Turso.EntityFrameworkCore.Sqlite` adds a `UseTurso` provider hook for local and embedded Turso databases. It reuses EF Core SQLite's LINQ translation pipeline and executes generated SQL through `Turso.Data.Sqlite`.

```bash
dotnet add package Turso.EntityFrameworkCore.Sqlite
```

```C#
using Microsoft.EntityFrameworkCore;

public sealed class AppDbContext : DbContext
{
    public DbSet<Customer> Customers => Set<Customer>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseTurso("Data Source=app.db");
}
```

You can also pass an existing Turso SQLite-compatible connection:

```C#
using Microsoft.EntityFrameworkCore;
using Turso.Data.Sqlite;

await using var connection = new SqliteConnection("Data Source=app.db");
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseTurso(connection)
    .Options;
```

The local provider supports the normal EF Core SQLite query pipeline, including composed `IQueryable<T>` filters, navigation-property joins, ordering, paging, grouping, aggregates, async materialization, and `SaveChangesAsync`. Schema creation can use the standard EF Core SQLite mechanisms such as `EnsureCreated`, `EnsureCreatedAsync`, and migrations against local database files.

Remote `libsql://`/auth-token EF Core support is not part of the local provider. Use the local/embedded provider for EF Core today; remote/serverless EF support needs a separate connection, retry, and transaction design.
