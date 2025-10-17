using System.Data;
using System.Data.Common;
using Turso.Native;

namespace Turso;

public class TursoConnection : DbConnection
{
    private TursoNativeDatabase? _turso = null;

    private TursoConnectionOptions _connectionOptions;

    public override string ConnectionString
    {
        get => _connectionOptions.GetConnectionString();
        set => _connectionOptions = TursoConnectionOptions.Parse(value);
    }

    public override string Database => "main";

    public override string DataSource => _connectionOptions["Data Source"] ?? "";

    public override string ServerVersion => throw new NotImplementedException();

    public override ConnectionState State => _turso is not null ? ConnectionState.Open : ConnectionState.Closed;

    public TursoConnection() : this("")
    {
    }

    public TursoConnection(string connectionString)
    {
        _connectionOptions = TursoConnectionOptions.Parse(connectionString);
    }

    public override void Open()
    {
        var filename = _connectionOptions["Data Source"] ?? ":memory:";
        _turso = new TursoNativeDatabase(filename);
    }

    public override void Close()
    {
        _turso?.Dispose();
        _turso = null;
    }

    internal TursoNativeDatabase Turso => _turso;

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        if (_turso is null)
        {
            throw new Exception("Turso database is closed");
        }

        return new TursoTransaction(this, isolationLevel);
    }

    protected override DbCommand CreateDbCommand()
    {
        if (_turso is null)
        {
            throw new Exception("Turso database is closed");
        }

        return new TursoCommand(this);
    }

    public int ExecuteNonQuery(string sql)
    {
        using var command = CreateCommand();
        command.CommandText = sql;

        return command.ExecuteNonQuery();
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException();
    }
}