using System.Data.Common;
using System.Text;
using AwesomeAssertions;
using Turso.Data.Sqlite;

namespace Turso.Tests.Support;

/// <summary>
/// Writes recognizable rows and checks that they never reach the database files in clear.
/// </summary>
internal static class PageCodecTestFiles
{
    public const string Marker = "PLAINTEXT-MARKER-6c1f0e";
    public const int RowCount = 500;

    public const string CreateTableSql = "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)";

    public static readonly string InsertRowsSql =
        $"INSERT INTO items(name) SELECT '{Marker}-' || value FROM generate_series(1, {RowCount})";

    public const string CountRowsSql = "SELECT count(*) FROM items WHERE name LIKE 'PLAINTEXT-MARKER-%'";

    public static void WriteRows(DbConnection connection)
    {
        Execute(connection, CreateTableSql);
        Execute(connection, InsertRowsSql);
    }

    public static long CountRows(DbConnection connection) => Convert.ToInt64(Scalar(connection, CountRowsSql));

    public static void Execute(DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    public static object? Scalar(DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    public static byte[] ReadFile(string path)
    {
        if (!File.Exists(path))
            return [];

        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }

    public static void AssertNoPlaintext(string path)
    {
        var bytes = ReadFile(path);
        bytes.AsSpan().IndexOf(Encoding.UTF8.GetBytes(Marker)).Should().Be(-1, $"{path} must not contain plaintext");
        bytes.AsSpan().IndexOf("CREATE TABLE"u8).Should().Be(-1, $"{path} must not contain the schema in clear");
    }
}

internal sealed class TemporaryDirectory : IDisposable
{
    public TemporaryDirectory()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.IO.Path.GetRandomFileName());
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public string File(string name) => System.IO.Path.Combine(Path, name);

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(Path))
            Directory.Delete(Path, recursive: true);
    }
}
