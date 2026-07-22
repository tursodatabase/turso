using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso;

public class TursoConnection : DbConnection
{
    private TursoDatabaseHandle? _turso;
    private TursoRemoteClient? _remoteClient;
    private TursoConnectionOptions _connectionOptions;
    private bool _disposed;
    private bool _readUncommitted;
    private bool _remoteTransactionActive;

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

    public override ConnectionState State => _turso is not null || _remoteClient is not null
        ? ConnectionState.Open
        : ConnectionState.Closed;

    public override bool CanCreateBatch => _connectionOptions.IsRemote && !_connectionOptions.IsReplica;

    protected override DbProviderFactory DbProviderFactory => TursoFactory.Instance;

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
        if (_turso is not null || _remoteClient is not null)
            throw new InvalidOperationException("The connection is already open.");

        if (_connectionOptions.IsRemote)
        {
            OpenRemote();
            return;
        }

        ValidateLocalOnlyOptions();

        var filename = _connectionOptions["Data Source"] ?? ":memory:";
        var cipher = _connectionOptions.GetEncryptionCipher();
        var hexkey = _connectionOptions["Encryption Key"];

        if (cipher.HasValue)
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

    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled(cancellationToken);

        Open();
        return Task.CompletedTask;
    }

    public override void Close()
    {
        if (_remoteClient is not null)
        {
            CloseRemote();
            return;
        }

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
        if (_turso is null && _remoteClient is null)
        {
            throw new InvalidOperationException("Turso database is closed.");
        }

        return new TursoTransaction(this, isolationLevel);
    }

    protected override DbCommand CreateDbCommand()
    {
        return new TursoCommand(this);
    }

    protected override DbBatch CreateDbBatch()
    {
        if (!CanCreateBatch)
            throw new NotSupportedException("Turso batch execution is currently supported only for remote connections.");

        return new TursoBatch(this);
    }

    public int ExecuteNonQuery(string sql)
    {
        using var command = CreateCommand();
        command.CommandText = sql;

        return command.ExecuteNonQuery();
    }

    public void Sync()
    {
        SyncAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public Task SyncAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled(cancellationToken);
        if (State != ConnectionState.Open)
            throw new InvalidOperationException("Turso database is closed.");
        if (!_connectionOptions.IsReplica)
            throw new NotSupportedException("Sync requires an embedded replica connection.");

        throw new NotSupportedException("Embedded replica sync is not supported yet by the .NET provider.");
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException("Turso does not support changing the active database.");
    }

    internal int DefaultTimeout => _connectionOptions.DefaultTimeout;

    internal bool IsRemote => _remoteClient is not null;

    internal bool ReadUncommitted
    {
        get => _readUncommitted;
        set => _readUncommitted = value;
    }

    internal TursoDatabaseHandle Turso => _turso ?? throw new InvalidOperationException("Turso database is closed.");

    internal async Task<RemoteStatementResult> ExecuteRemoteAsync(
        string sql,
        TursoParameterCollection parameters,
        bool wantRows,
        int commandTimeout,
        CancellationToken cancellationToken)
    {
        var remoteClient = _remoteClient ?? throw new InvalidOperationException("Turso database is closed.");
        var closeAfter = !_connectionOptions.ReadYourWrites && !_remoteTransactionActive;
        try
        {
            return await remoteClient.ExecuteAsync(sql, parameters, wantRows, commandTimeout, closeAfter, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TursoRemoteSqlException)
        {
            throw;
        }
        catch
        {
            InvalidateRemoteSession();
            throw;
        }
    }

    internal async Task<IReadOnlyList<RemoteStatementResult>> ExecuteRemoteBatchAsync(
        IReadOnlyList<TursoBatchCommand> batchCommands,
        int commandTimeout,
        bool wantRows,
        CancellationToken cancellationToken)
    {
        var remoteClient = _remoteClient ?? throw new InvalidOperationException("Turso database is closed.");
        var closeAfter = !_connectionOptions.ReadYourWrites && !_remoteTransactionActive;
        try
        {
            return await remoteClient.ExecuteBatchAsync(batchCommands, commandTimeout, wantRows, closeAfter, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TursoRemoteSqlException)
        {
            throw;
        }
        catch
        {
            InvalidateRemoteSession();
            throw;
        }
    }

    internal void BeginRemoteTransaction(IsolationLevel isolationLevel)
    {
        _ = isolationLevel;
        var remoteClient = _remoteClient ?? throw new InvalidOperationException("Turso database is closed.");
        if (_remoteTransactionActive)
            throw new InvalidOperationException("A transaction is already active on this connection.");

        _remoteTransactionActive = true;
        try
        {
            remoteClient
                .ExecuteAsync("BEGIN", new TursoParameterCollection(), wantRows: false, DefaultTimeout, closeAfter: false, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }
        catch (TursoRemoteSqlException)
        {
            _remoteTransactionActive = false;
            throw;
        }
        catch
        {
            InvalidateRemoteSession();
            throw;
        }
    }

    internal void CommitRemoteTransaction()
    {
        var remoteClient = _remoteClient ?? throw new InvalidOperationException("Turso database is closed.");
        if (!_remoteTransactionActive)
            throw new InvalidOperationException("No remote transaction is active on this connection.");

        try
        {
            remoteClient
                .ExecuteAsync("COMMIT", new TursoParameterCollection(), wantRows: false, DefaultTimeout, closeAfter: false, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }
        catch (TursoRemoteSqlException)
        {
            throw;
        }
        catch
        {
            InvalidateRemoteSession();
            throw;
        }

        _remoteTransactionActive = false;
    }

    internal void RollbackRemoteTransaction()
    {
        var remoteClient = _remoteClient ?? throw new InvalidOperationException("Turso database is closed.");
        if (!_remoteTransactionActive)
            throw new InvalidOperationException("No remote transaction is active on this connection.");

        try
        {
            remoteClient
                .ExecuteAsync("ROLLBACK", new TursoParameterCollection(), wantRows: false, DefaultTimeout, closeAfter: false, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
            _remoteTransactionActive = false;
        }
        catch
        {
            InvalidateRemoteSession();
            throw;
        }
    }

    internal void CloseRemoteSessionIfStateless()
    {
        if (_connectionOptions.ReadYourWrites || _remoteClient is not { HasOpenSession: true } remoteClient)
            return;

        try
        {
            remoteClient.CloseAsync(DefaultTimeout, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch
        {
            InvalidateRemoteSession();
        }
    }

    private void OpenRemote()
    {
        if (_connectionOptions.IsReplica)
            throw new NotSupportedException("Embedded replica connections are not supported yet by the .NET provider. Use a remote URL without Replica Path for direct remote execution.");

        if (_connectionOptions.SyncInterval > 0)
            throw new NotSupportedException("Sync Interval requires embedded replica support, which is not supported yet by the .NET provider.");

        if (_connectionOptions.GetEncryptionCipher().HasValue || !string.IsNullOrWhiteSpace(_connectionOptions["Encryption Key"]))
            throw new InvalidOperationException("Encryption Cipher and Encryption Key are local database options and cannot be used with remote Turso URLs.");

        _remoteClient = new TursoRemoteClient(_connectionOptions.GetRemoteUri(), _connectionOptions.AuthToken);
    }

    private void ValidateLocalOnlyOptions()
    {
        if (!string.IsNullOrWhiteSpace(_connectionOptions.AuthToken))
            throw new InvalidOperationException("Auth Token requires a remote Turso URL Data Source.");
        if (!string.IsNullOrWhiteSpace(_connectionOptions.ReplicaPath))
            throw new InvalidOperationException("Replica Path requires a remote Turso URL Data Source.");
        if (_connectionOptions.SyncInterval > 0)
            throw new InvalidOperationException("Sync Interval requires a remote embedded replica connection.");
        if (_connectionOptions.Tls.HasValue)
            throw new InvalidOperationException("Tls requires a remote Turso URL Data Source.");
    }

    private void CloseRemote()
    {
        var remoteClient = _remoteClient;
        if (remoteClient is null)
            return;

        Exception? closeError = null;
        try
        {
            if (_remoteTransactionActive)
            {
                remoteClient
                    .ExecuteAsync("ROLLBACK", new TursoParameterCollection(), wantRows: false, DefaultTimeout, closeAfter: true, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
            }
            else
            {
                remoteClient.CloseAsync(DefaultTimeout, CancellationToken.None).GetAwaiter().GetResult();
            }
        }
        catch (Exception ex)
        {
            closeError = ex;
        }
        finally
        {
            remoteClient.Dispose();
            _remoteClient = null;
            _remoteTransactionActive = false;
            _readUncommitted = false;
        }

        if (closeError is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(closeError).Throw();
    }

    private void InvalidateRemoteSession()
    {
        _remoteClient?.Dispose();
        _remoteClient = null;
        _remoteTransactionActive = false;
        _readUncommitted = false;
    }
}
