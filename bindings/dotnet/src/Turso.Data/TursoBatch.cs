using System.Data;
using System.Data.Common;
using System.Text.Json;
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
        if (_connection?.IsRemote == true)
            return reader.Read() ? reader.GetValue(0) : null;

        return ReadScalar(reader);
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
        if (_connection?.IsRemote == true)
        {
            return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
                ? reader.GetValue(0)
                : null;
        }

        return await ReadScalarAsync(reader, cancellationToken).ConfigureAwait(false);
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
        return CreateDataReader(results, behavior);
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        var results = await ExecuteBatch(wantRows: true, cancellationToken).ConfigureAwait(false);
        SetRecordsAffected(results);
        return await CreateDataReaderAsync(results, behavior, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<RemoteStatementResult>> ExecuteBatch(
        bool wantRows,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var connection = ValidateBatch();
        if (connection.IsRemote)
        {
            return await connection
                .ExecuteRemoteBatchAsync(_batchCommands.AsReadOnly(), Timeout, wantRows, cancellationToken)
                .ConfigureAwait(false);
        }

        if (connection.SyncDatabase is null)
            throw new NotSupportedException("Turso batch execution requires a direct remote or embedded replica connection.");

        var results = new List<RemoteStatementResult>(_batchCommands.Count);
        foreach (var batchCommand in _batchCommands.AsReadOnly())
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var command = new TursoCommand(connection)
            {
                CommandText = batchCommand.CommandText,
                CommandTimeout = Timeout,
                Transaction = _transaction,
            };
            foreach (TursoParameter parameter in batchCommand.Parameters)
                command.Parameters.Add(parameter);

            results.Add(wantRows
                ? await BufferResultAsync(command, cancellationToken).ConfigureAwait(false)
                : new RemoteStatementResult
                {
                    AffectedRowCount = checked((ulong)Math.Max(
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false),
                        0)),
                });
        }

        return results;
    }

    private DbDataReader CreateDataReader(
        IReadOnlyList<RemoteStatementResult> results,
        CommandBehavior behavior)
    {
        IDisposable? syncOperation = null;
        try
        {
            if (_connection?.SyncDatabase is not null)
                syncOperation = _connection.EnterSyncOperation();

            var reader = new TursoRemoteDataReader(_connection, results, behavior, syncOperation);
            syncOperation = null;
            return reader;
        }
        finally
        {
            syncOperation?.Dispose();
        }
    }

    private async Task<DbDataReader> CreateDataReaderAsync(
        IReadOnlyList<RemoteStatementResult> results,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        IDisposable? syncOperation = null;
        try
        {
            if (_connection?.SyncDatabase is not null)
            {
                syncOperation = await _connection
                    .EnterSyncOperationAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            var reader = new TursoRemoteDataReader(_connection, results, behavior, syncOperation);
            syncOperation = null;
            return reader;
        }
        finally
        {
            syncOperation?.Dispose();
        }
    }

    private static async Task<RemoteStatementResult> BufferResultAsync(
        TursoCommand command,
        CancellationToken cancellationToken)
    {
        await using var reader = await command
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        var columns = new List<RemoteColumn>(reader.FieldCount);
        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            columns.Add(new RemoteColumn
            {
                Name = reader.GetName(ordinal),
                DeclType = reader.GetDataTypeName(ordinal),
            });
        }

        var rows = new List<List<RemoteResponseValue>>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = new List<RemoteResponseValue>(reader.FieldCount);
            for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
                row.Add(ToBufferedValue(reader.GetValue(ordinal)));
            rows.Add(row);
        }

        return new RemoteStatementResult
        {
            Columns = columns,
            Rows = rows,
            AffectedRowCount = checked((ulong)Math.Max(reader.RecordsAffected, 0)),
        };
    }

    private static RemoteResponseValue ToBufferedValue(object value)
    {
        return value switch
        {
            DBNull => new RemoteResponseValue { Type = "null" },
            byte[] bytes => new RemoteResponseValue
            {
                Type = "blob",
                Base64 = Convert.ToBase64String(bytes),
            },
            double number => new RemoteResponseValue
            {
                Type = "float",
                Value = JsonSerializer.SerializeToElement(number),
            },
            long number => new RemoteResponseValue
            {
                Type = "integer",
                Value = JsonSerializer.SerializeToElement(number),
            },
            string text => new RemoteResponseValue
            {
                Type = "text",
                Value = JsonSerializer.SerializeToElement(text),
            },
            _ => throw new InvalidOperationException(
                $"Cannot buffer a batch value of type {value.GetType().FullName}."),
        };
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

    private static object? ReadScalar(DbDataReader reader)
    {
        do
        {
            if (reader.FieldCount > 0 && reader.Read())
                return reader.GetValue(0);
        } while (reader.NextResult());

        return null;
    }

    private static async Task<object?> ReadScalarAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        do
        {
            if (reader.FieldCount > 0
                && await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return reader.GetValue(0);
            }
        } while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

        return null;
    }
}
