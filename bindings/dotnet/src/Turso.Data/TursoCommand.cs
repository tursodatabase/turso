using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso;

public class TursoCommand : DbCommand
{
    private TursoConnection? _connection;
    private readonly TursoParameterCollection _parameterCollection = new();

    private TursoTransaction? _transaction;
    private TursoStatementHandle? _statement;
    private int _commandTimeout = 30;

    public TursoCommand()
    {
    }

    public TursoCommand(TursoConnection connection, TursoTransaction? transaction = null)
    {
        _connection = connection;
        _transaction = transaction;
        _commandTimeout = connection.DefaultTimeout;
    }

    public TursoCommand(TursoConnection connection, string command)
    {
        _connection = connection;
        _transaction = null;
        _commandTimeout = connection.DefaultTimeout;
        CommandText = command;
    }

    [AllowNull]
    public override string CommandText { get; set; } = "";
    public override int CommandTimeout
    {
        get => _commandTimeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _commandTimeout = value;
        }
    }

    public override CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
                throw new NotSupportedException("TursoCommand only supports CommandType.Text.");
        }
    }

    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

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
            _commandTimeout = _connection.DefaultTimeout;
        }
    }

    protected override DbParameterCollection DbParameterCollection => _parameterCollection;

    public new virtual TursoParameterCollection Parameters => _parameterCollection;


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

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _statement?.Dispose();
    }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery()
    {
        if (_connection?.IsRemote == true)
            return ExecuteRemoteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

        using var reader = Execute();
        while (reader.Read())
        {
        }

        return reader.RecordsAffected;
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        if (_connection?.IsRemote == true)
            return await ExecuteRemoteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
        }

        return reader.RecordsAffected;
    }

    public override object? ExecuteScalar()
    {
        using var reader = Execute();
        return reader.Read()
            ? reader.GetValue(0)
            : null;
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? reader.GetValue(0)
            : null;
    }

    public override void Prepare()
    {
        if (_connection is null)
            throw new InvalidOperationException("Connection must be set before preparing a command.");
        if (string.IsNullOrWhiteSpace(CommandText))
            throw new InvalidOperationException("CommandText must be set before preparing a command.");
        ValidateTransaction();
        if (_connection.IsRemote)
            return;

        TursoStatementHandle? preparedStatement = null;
        try
        {
            var sql = RewriteFacadePragmas(CommandText, _connection);
            preparedStatement = TursoBindings.PrepareStatement(_connection.Turso, sql);
            var parameterCount = TursoBindings.GetParameterCount(preparedStatement);
            var boundParameters = new bool[parameterCount + 1];

            for (var i = 0; i < _parameterCollection.Count; i++)
            {
                var parameter = _parameterCollection[i] as TursoParameter;
                if (parameter == null)
                    throw new ArgumentException("Parameter must be of type TursoParameter");

                if (!string.IsNullOrEmpty(parameter.ParameterName))
                {
                    var parameterIndex = TursoBindings.BindNamedParameter(preparedStatement, parameter.ParameterName, parameter.ToValue());
                    if (parameterIndex == 0)
                        throw new InvalidOperationException($"Parameter {parameter.ParameterName} was not found in the SQL statement.");

                    boundParameters[parameterIndex] = true;
                }
                else
                {
                    var parameterIndex = i + 1;
                    if (parameterIndex > parameterCount)
                        throw new InvalidOperationException($"Parameter at position {parameterIndex} was not found in the SQL statement.");

                    TursoBindings.BindParameter(preparedStatement, parameterIndex, parameter.ToValue());
                    boundParameters[parameterIndex] = true;
                }
            }

            for (var i = 1; i <= parameterCount; i++)
            {
                if (!boundParameters[i])
                {
                    var parameterName = TursoBindings.GetParameterName(preparedStatement, i);
                    throw new InvalidOperationException(
                        parameterName is null
                            ? $"Missing value for parameter at position {i}."
                            : $"Missing value for parameter {parameterName}.");
                }
            }

            _statement?.Dispose();
            _statement = preparedStatement;
            preparedStatement = null;
        }
        finally
        {
            preparedStatement?.Dispose();
        }
    }

    protected override DbParameter CreateDbParameter()
    {
        return new TursoParameter();
    }


    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return Execute(behavior);
    }

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<DbDataReader>(cancellationToken);

        if (_connection?.IsRemote == true)
            return ExecuteRemoteAsync(behavior, cancellationToken);

        return Task.FromResult(Execute(behavior));
    }

    private static string RewriteFacadePragmas(string sql, TursoConnection connection)
    {
        var normalized = sql.Trim().TrimEnd(';').Trim();
        const string prefix = "PRAGMA read_uncommitted";
        if (!normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return sql;
        if (normalized.Length == prefix.Length)
            return "SELECT " + (connection.ReadUncommitted ? "1" : "0");

        var value = normalized[prefix.Length..].TrimStart();
        if (value.StartsWith("=", StringComparison.Ordinal))
        {
            connection.ReadUncommitted = ParsePragmaEnabled(value[1..].Trim());
            return "SELECT 1 WHERE 0";
        }

        return sql;
    }

    private static bool ParsePragmaEnabled(string value)
    {
        value = value.Trim('\'', '"');
        return long.TryParse(value, out var number)
            ? number != 0
            : value.Equals("ON", StringComparison.OrdinalIgnoreCase)
              || value.Equals("TRUE", StringComparison.OrdinalIgnoreCase)
              || value.Equals("YES", StringComparison.OrdinalIgnoreCase);
    }

    private DbDataReader Execute(CommandBehavior behavior = CommandBehavior.Default)
    {
        if (_connection is null)
            throw new InvalidOperationException("Connection must be set before executing a command.");

        if (_connection.IsRemote)
            return ExecuteRemoteAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

        Prepare();

        var statement = _statement ?? throw new InvalidOperationException("Command was not prepared.");
        _statement = null;
        var reader = new TursoDataReader(this, statement, behavior);
        return reader;
    }

    private async Task<DbDataReader> ExecuteRemoteAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (_connection is null)
            throw new InvalidOperationException("Connection must be set before executing a command.");
        if (string.IsNullOrWhiteSpace(CommandText))
            throw new InvalidOperationException("CommandText must be set before executing a command.");
        ValidateTransaction();

        cancellationToken.ThrowIfCancellationRequested();

        var sql = RewriteFacadePragmas(CommandText, _connection);
        var result = await _connection
            .ExecuteRemoteAsync(sql, _parameterCollection, wantRows: true, CommandTimeout, cancellationToken)
            .ConfigureAwait(false);
        return new TursoRemoteDataReader(this, result, behavior);
    }

    private async Task<int> ExecuteRemoteNonQueryAsync(CancellationToken cancellationToken)
    {
        if (_connection is null)
            throw new InvalidOperationException("Connection must be set before executing a command.");
        if (string.IsNullOrWhiteSpace(CommandText))
            throw new InvalidOperationException("CommandText must be set before executing a command.");
        ValidateTransaction();

        cancellationToken.ThrowIfCancellationRequested();

        var sql = RewriteFacadePragmas(CommandText, _connection);
        var result = await _connection
            .ExecuteRemoteAsync(sql, _parameterCollection, wantRows: false, CommandTimeout, cancellationToken)
            .ConfigureAwait(false);
        return checked((int)result.AffectedRowCount);
    }

    private void ValidateTransaction()
    {
        if (_transaction is null)
            return;
        if (_transaction.IsCompleted)
            throw new InvalidOperationException("The transaction associated with this command has completed.");
        if (!ReferenceEquals(_transaction.Connection, _connection))
            throw new InvalidOperationException("The transaction is not associated with the command's connection.");
    }
}
