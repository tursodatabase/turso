using System.Data;
using System.Data.Common;
using Turso.Raw.Public;

namespace Turso;

public sealed class TursoBatch : DbBatch
{
    private readonly TursoBatchCommandCollection _batchCommands = new();
    private TursoConnection? _connection;
    private TursoTransaction? _transaction;
    private int _timeout = 30;

    public TursoBatch()
    {
    }

    public TursoBatch(TursoConnection connection)
    {
        _connection = connection;
        _timeout = connection.DefaultTimeout;
    }

    protected override DbBatchCommandCollection DbBatchCommands => _batchCommands;

    public new TursoBatchCommandCollection BatchCommands => _batchCommands;

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set
        {
            if (value is null)
            {
                _connection = null;
                return;
            }

            _connection = value as TursoConnection
                          ?? throw new ArgumentException("Connection must be a TursoConnection.", nameof(value));
            _timeout = _connection.DefaultTimeout;
        }
    }

    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set
        {
            if (value is null)
            {
                _transaction = null;
                return;
            }

            _transaction = value as TursoTransaction
                           ?? throw new ArgumentException("Transaction must be a TursoTransaction.", nameof(value));
        }
    }

    public override int Timeout
    {
        get => _timeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _timeout = value;
        }
    }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery()
    {
        var results = ExecuteBatch(wantRows: false, CancellationToken.None).GetAwaiter().GetResult();
        return SetRecordsAffected(results);
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var results = await ExecuteBatch(wantRows: false, cancellationToken).ConfigureAwait(false);
        return SetRecordsAffected(results);
    }

    public override object? ExecuteScalar()
    {
        using var reader = ExecuteDbDataReader(CommandBehavior.Default);
        return reader.Read() ? reader.GetValue(0) : null;
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? reader.GetValue(0) : null;
    }

    public override void Prepare()
    {
        ValidateBatch();
    }

    public override Task PrepareAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled(cancellationToken);

        Prepare();
        return Task.CompletedTask;
    }

    protected override DbBatchCommand CreateDbBatchCommand()
    {
        return new TursoBatchCommand();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var results = ExecuteBatch(wantRows: true, CancellationToken.None).GetAwaiter().GetResult();
        SetRecordsAffected(results);
        return new TursoRemoteDataReader(_connection, results, behavior);
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        var results = await ExecuteBatch(wantRows: true, cancellationToken).ConfigureAwait(false);
        SetRecordsAffected(results);
        return new TursoRemoteDataReader(_connection, results, behavior);
    }

    private Task<IReadOnlyList<RemoteStatementResult>> ExecuteBatch(bool wantRows, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<IReadOnlyList<RemoteStatementResult>>(cancellationToken);

        var connection = ValidateBatch();
        if (!connection.IsRemote)
            throw new NotSupportedException("Turso batch execution is currently supported only for remote connections.");

        return connection.ExecuteRemoteBatchAsync(_batchCommands.AsReadOnly(), Timeout, wantRows, cancellationToken);
    }

    private TursoConnection ValidateBatch()
    {
        var connection = _connection ?? throw new InvalidOperationException("Connection must be set before executing a batch.");
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Turso database is closed.");
        if (_transaction is { IsCompleted: true })
            throw new InvalidOperationException("The transaction associated with this batch has completed.");
        if (_transaction is not null && !ReferenceEquals(_transaction.Connection, connection))
            throw new InvalidOperationException("The transaction is not associated with the batch's connection.");
        if (_batchCommands.Count == 0)
            throw new InvalidOperationException("Batch must contain at least one command.");

        foreach (var command in _batchCommands.AsReadOnly())
        {
            if (string.IsNullOrWhiteSpace(command.CommandText))
                throw new InvalidOperationException("Batch command text must be set before executing a batch.");
            if (command.CommandType != CommandType.Text)
                throw new NotSupportedException("TursoBatchCommand only supports CommandType.Text.");
        }

        return connection;
    }

    private int SetRecordsAffected(IReadOnlyList<RemoteStatementResult> results)
    {
        if (results.Count != _batchCommands.Count)
            throw new TursoException($"Batch result count {results.Count} did not match command count {_batchCommands.Count}.");

        var total = 0;
        for (var i = 0; i < results.Count; i++)
        {
            var recordsAffected = checked((int)results[i].AffectedRowCount);
            _batchCommands.AsReadOnly()[i].SetRecordsAffected(recordsAffected);
            total = checked(total + recordsAffected);
        }

        return total;
    }
}
