using System.Data;
using System.Data.SQLite;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using SQLitePCL;
using Turso;

namespace Benchmarks;

[MemoryDiagnoser]
public class Benchmarks
{
    private SQLiteConnection _systemDataSqliteConnection;
    private SqliteConnection _microsoftDataSqliteConnection;
    private TursoConnection _tursoConnection;

    [GlobalSetup]
    public void Setup()
    {
        _systemDataSqliteConnection = new SQLiteConnection("Data Source=:memory:");
        _systemDataSqliteConnection.Open();

        _microsoftDataSqliteConnection = new SqliteConnection("Data Source=:memory:");
        _microsoftDataSqliteConnection.Open();

        _tursoConnection = new TursoConnection("Data Source=:memory:");
        _tursoConnection.Open();
        CreateTable(_systemDataSqliteConnection);
        CreateTable(_microsoftDataSqliteConnection);
        CreateTable(_tursoConnection);
    }

    [Benchmark]
    public void TursoSelect() => Select(_tursoConnection);

    [Benchmark]
    public void SystemSqliteSelect() => Select(_systemDataSqliteConnection);

    [Benchmark]
    public void MicrososftSqliteSelect() => Select(_microsoftDataSqliteConnection);

    private void CreateTable(IDbConnection connection)
    {
        using var createTableCommand = connection.CreateCommand();
        createTableCommand.CommandText = "CREATE TABLE t(a, b)";
        createTableCommand.ExecuteNonQuery();

        using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"INSERT INTO t(a, b) VALUES (1, 2), (3, 4);";
        insertCommand.ExecuteNonQuery();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Select(IDbConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM t;";
        using var reader = command.ExecuteReader();
        var sum = 0;
        while (reader.Read()) 
            sum += reader.GetInt32(0);
        
        GC.KeepAlive(sum);
    }
}