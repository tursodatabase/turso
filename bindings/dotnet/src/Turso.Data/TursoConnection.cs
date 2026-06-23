using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso;

public class TursoConnection : DbConnection
{
    private TursoDatabaseHandle? _turso;
    private TursoConnectionOptions _connectionOptions;
    private bool _disposed;
    private bool _readUncommitted;
    private ITursoPageCodec? _pageCodec;
    private byte _pageCodecReservedSpace;

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionOptions.GetConnectionString();
        set
        {
            if (State == ConnectionState.Open)
                throw new InvalidOperationException("ConnectionString cannot be set while the connection is open.");

            _connectionOptions = TursoConnectionOptions.Parse(value ?? string.Empty);
        }
    }

    public override string Database => "main";

    public override string DataSource => _connectionOptions["Data Source"] ?? "";

    public override string ServerVersion => typeof(TursoConnection).Assembly.GetName().Version?.ToString() ?? "0.0.0";

    public override ConnectionState State => _turso is not null ? ConnectionState.Open : ConnectionState.Closed;

    protected override DbProviderFactory DbProviderFactory => TursoFactory.Instance;

    public ITursoPageCodec? PageCodec
    {
        get => _pageCodec;
        set
        {
            if (State == ConnectionState.Open)
                throw new InvalidOperationException("PageCodec cannot be set while the connection is open.");

            _pageCodec = value;
        }
    }

    public byte PageCodecReservedSpace
    {
        get => _pageCodecReservedSpace;
        set
        {
            if (State == ConnectionState.Open)
                throw new InvalidOperationException("PageCodecReservedSpace cannot be set while the connection is open.");

            _pageCodecReservedSpace = value;
        }
    }

    public TursoConnection() : this("")
    {
    }

    public TursoConnection(string connectionString)
    {
        _connectionOptions = TursoConnectionOptions.Parse(connectionString);
    }

    public override void Open()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_turso is not null)
            throw new InvalidOperationException("The connection is already open.");

        var filename = _connectionOptions["Data Source"] ?? ":memory:";
        var cipher = _connectionOptions.GetEncryptionCipher();
        var hexkey = _connectionOptions["Encryption Key"];
        var readOnly = IsReadOnlyMode(_connectionOptions["Mode"]);

        if (_pageCodec is not null)
        {
            if (cipher.HasValue)
                throw new InvalidOperationException("PageCodec cannot be combined with Encryption Cipher.");

            _turso = readOnly
                ? TursoBindings.OpenDatabaseReadOnlyWithPageCodec(filename, _pageCodec, _pageCodecReservedSpace)
                : TursoBindings.OpenDatabaseWithPageCodec(filename, _pageCodec, _pageCodecReservedSpace);
        }
        else if (cipher.HasValue)
        {
            if (string.IsNullOrWhiteSpace(hexkey))
                throw new InvalidOperationException("Encryption Key is required when Encryption Cipher is specified.");

            _turso = TursoBindings.OpenDatabaseWithEncryption(filename, cipher.Value, hexkey);
        }
        else
        {
            _turso = TursoBindings.OpenDatabase(filename);
        }
    }

    private static bool IsReadOnlyMode(string? mode)
    {
        return mode is not null
            && (mode.Equals("ReadOnly", StringComparison.OrdinalIgnoreCase)
                || mode.Equals("Read Only", StringComparison.OrdinalIgnoreCase)
                || mode.Equals("ro", StringComparison.OrdinalIgnoreCase));
    }

    public override void Close()
    {
        _turso?.Dispose();
        _turso = null;
        _readUncommitted = false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();

        _disposed = true;
        base.Dispose(disposing);
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        if (_turso is null)
        {
            throw new InvalidOperationException("Turso database is closed.");
        }

        return new TursoTransaction(this, isolationLevel);
    }

    protected override DbCommand CreateDbCommand()
    {
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
        throw new NotSupportedException("Turso does not support changing the active database.");
    }

    internal int DefaultTimeout => _connectionOptions.DefaultTimeout;

    internal bool ReadUncommitted
    {
        get => _readUncommitted;
        set => _readUncommitted = value;
    }

    internal TursoDatabaseHandle Turso => _turso ?? throw new InvalidOperationException("Turso database is closed.");
}
