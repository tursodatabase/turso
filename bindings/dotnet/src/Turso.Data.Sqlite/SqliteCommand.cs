using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso.Data.Sqlite;

public class SqliteCommand : DbCommand
{
    private SqliteConnection? _connection;
    private SqliteTransaction? _transaction;
    private TursoStatementHandle? _statement;
    private string _commandText = string.Empty;
    private int _commandTimeout = 30;
    private bool _hasOpenReader;

    public SqliteCommand()
    {
    }

    public SqliteCommand(string? commandText)
    {
        CommandText = commandText;
    }

    public SqliteCommand(SqliteConnection? connection)
    {
        Connection = connection;
    }

    public SqliteCommand(string? commandText, SqliteConnection? connection)
        : this(commandText)
    {
        Connection = connection;
    }

    public SqliteCommand(string? commandText, SqliteConnection? connection, SqliteTransaction? transaction)
        : this(commandText, connection)
    {
        Transaction = transaction;
    }

    public SqliteCommand(string? commandText, SqliteConnection? connection, DbTransaction? transaction)
        : this(commandText, connection)
    {
        Transaction = transaction as SqliteTransaction
                      ?? (transaction is null ? null : throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction)));
    }

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set
        {
            ThrowIfReaderOpen(nameof(CommandText));
            _commandText = value ?? string.Empty;
        }
    }

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
                throw new ArgumentException(Properties.Resources.InvalidCommandType(value));
        }
    }

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    public new SqliteConnection? Connection
    {
        get => _connection;
        set
        {
            ThrowIfReaderOpen(nameof(Connection));
            _connection = value;
            if (value is not null)
            {
                _commandTimeout = value.DefaultTimeout;
                _transaction ??= value.Transaction;
            }
        }
    }

    public new SqliteParameterCollection Parameters { get; } = new();

    public new SqliteTransaction? Transaction
    {
        get => _transaction;
        set
        {
            ThrowIfReaderOpen(nameof(Transaction));
            _transaction = value;
        }
    }

    protected override DbConnection? DbConnection
    {
        get => Connection;
        set => Connection = value as SqliteConnection
                            ?? (value is null ? null : throw new ArgumentException("Connection must be a SqliteConnection.", nameof(value)));
    }

    protected override DbParameterCollection DbParameterCollection => Parameters;

    protected override DbTransaction? DbTransaction
    {
        get => Transaction;
        set => Transaction = value as SqliteTransaction
                            ?? (value is null ? null : throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(value)));
    }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery()
    {
        using var reader = Execute("ExecuteNonQuery");
        while (reader.Read())
        {
        }

        if (IsTransactionControlCommand(CommandText))
            Connection?.Transaction?.MarkCompletedExternally(IsRollbackCommand(CommandText));

        return reader.RecordsAffected;
    }

    public override object? ExecuteScalar()
    {
        using var reader = Execute("ExecuteScalar");
        return reader.Read() ? reader.GetValue(0) : null;
    }

    public override void Prepare()
    {
        EnsureExecutable("Prepare");
        var statements = SplitStatements(CommandText);
        if (statements.Count != 1)
        {
            _statement?.Dispose();
            _statement = null;
            return;
        }

        TursoStatementHandle? preparedStatement = null;
        try
        {
            preparedStatement = PrepareSingleStatement(statements[0]);
            _statement?.Dispose();
            _statement = preparedStatement;
            preparedStatement = null;
        }
        catch (TursoException ex)
        {
            throw ToSqliteException(ex);
        }
        finally
        {
            preparedStatement?.Dispose();
        }
    }

    protected override DbParameter CreateDbParameter() => new SqliteParameter();

    public new SqliteDataReader ExecuteReader() => Execute("ExecuteReader");

    public new SqliteDataReader ExecuteReader(CommandBehavior behavior) => Execute("ExecuteReader", behavior);

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => Execute("ExecuteReader", behavior);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _statement?.Dispose();

        base.Dispose(disposing);
    }

    private SqliteDataReader Execute(string method, CommandBehavior behavior = CommandBehavior.Default)
    {
        EnsureExecutable(method);
        if (IsEmptyCommand(CommandText))
        {
            _hasOpenReader = true;
            Connection?.ReaderOpened();
            return new SqliteDataReader(this, -1, behavior, CloseReader);
        }

        if (Connection?.HasOpenReader == true && IsWriteCommand(CommandText))
        {
            Thread.Sleep(TimeSpan.FromSeconds(CommandTimeout));
            throw new SqliteException(Properties.Resources.SqliteNativeError(5, "database is locked"), 5);
        }
        if (Connection?.IsReadOnly == true && IsWriteCommand(CommandText))
            throw new SqliteException(Properties.Resources.SqliteNativeError(8, "attempt to write a readonly database"), 8);

        var recordsAffected = 0;
        var statements = SplitStatements(CommandText);
        try
        {
            for (var i = 0; i < statements.Count; i++)
            {
                if (TryHandleFacadeStatement(statements[i], out var sql))
                    continue;

                var statement = PrepareSingleStatement(sql);
                if (TursoBindings.GetFieldCount(statement) > 0)
                {
                    _hasOpenReader = true;
                    Connection?.ReaderOpened();
                    return new SqliteDataReader(this, statement, statements[i], statements.Skip(i + 1).ToList(), recordsAffected, behavior, CloseReader);
                }

                while (TursoBindings.Read(statement))
                {
                }

                if (CountsRowsAffected(statements[i]))
                    recordsAffected += TursoBindings.RowsAffected(statement);
                statement.Dispose();
            }
        }
        catch (TursoException ex)
        {
            throw ToSqliteException(ex);
        }

        _hasOpenReader = true;
        Connection?.ReaderOpened();
        return new SqliteDataReader(this, recordsAffected, behavior, CloseReader);
    }

    private void ThrowIfReaderOpen(string property)
    {
        if (_hasOpenReader)
            throw new InvalidOperationException(Properties.Resources.SetRequiresNoOpenReader(property));
    }

    private void EnsureExecutable(string method)
    {
        if (_hasOpenReader)
            throw new InvalidOperationException(Properties.Resources.DataReaderOpen);
        if (Connection is null || Connection.State != ConnectionState.Open)
            throw new InvalidOperationException(Properties.Resources.CallRequiresOpenConnection(method));
        if (Transaction is { IsCompleted: true } or { WasRolledBackExternally: true })
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);
        if (Transaction is not null && !ReferenceEquals(Transaction.Connection, Connection))
            throw new InvalidOperationException(Properties.Resources.TransactionConnectionMismatch);

        var connectionTransaction = Connection.Transaction;
        if (connectionTransaction is null || ReferenceEquals(Transaction, connectionTransaction))
            return;
        if (connectionTransaction.IsCompleted)
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);
        if (!IsTransactionControlCommand(CommandText))
            throw new InvalidOperationException(Properties.Resources.TransactionRequired);
    }

    private void CloseReader()
    {
        _hasOpenReader = false;
        Connection?.ReaderClosed();
    }

    internal TursoStatementHandle PrepareSingleStatement(string sql)
    {
        var connection = Connection!;
        sql = RewriteFacadeStatement(sql, connection);
        TursoStatementHandle statement;
        try
        {
            statement = TursoBindings.PrepareStatement(connection.DatabaseHandle, sql);
        }
        catch (TursoException ex)
        {
            throw ToSqliteException(ex, sql);
        }

        try
        {
            BindParameters(statement);
            return statement;
        }
        catch
        {
            statement.Dispose();
            throw;
        }
    }

    private void BindParameters(TursoStatementHandle statement)
    {
        var parameterCount = TursoBindings.GetParameterCount(statement);
        var boundParameters = new bool[parameterCount + 1];

        for (var i = 0; i < Parameters.Count; i++)
        {
            var parameter = Parameters[i];
            if (string.IsNullOrEmpty(parameter.ParameterName))
                throw new InvalidOperationException(Properties.Resources.RequiresSet(nameof(parameter.ParameterName)));
            if (!parameter.HasValue)
                throw new InvalidOperationException(Properties.Resources.RequiresSet(nameof(parameter.Value)));

            var parameterIndex = FindParameterIndex(statement, parameter.ParameterName, parameterCount);
            if (parameterIndex == 0)
                continue;

            TursoBindings.BindParameter(statement, parameterIndex, parameter.ToTursoValue());
            boundParameters[parameterIndex] = true;
        }

        for (var i = 1; i <= parameterCount; i++)
        {
            if (!boundParameters[i])
            {
                var parameterName = TursoBindings.GetParameterName(statement, i);
                throw new InvalidOperationException(
                    parameterName is null
                        ? Properties.Resources.MissingParameters(i)
                        : Properties.Resources.MissingParameters(parameterName));
            }
        }
    }

    private static bool IsEmptyCommand(string commandText)
    {
        foreach (var line in commandText.Split('\n'))
        {
            var trimmedLine = line.Trim();
            if (trimmedLine.Length != 0 && !trimmedLine.StartsWith("--", StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static bool IsTransactionControlCommand(string commandText)
    {
        var trimmed = commandText.TrimStart();
        return IsRollbackCommand(trimmed) || IsCommitCommand(trimmed);
    }

    private static bool IsRollbackCommand(string commandText)
    {
        var tail = GetCommandTail(commandText, "ROLLBACK");
        return tail is not null
               && !tail.StartsWith("TO", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsCommitCommand(string commandText)
        => GetCommandTail(commandText, "COMMIT") is not null;

    private static string? GetCommandTail(string commandText, string command)
    {
        var trimmed = commandText.TrimStart();
        if (!trimmed.StartsWith(command, StringComparison.OrdinalIgnoreCase))
            return null;
        if (trimmed.Length > command.Length && char.IsLetterOrDigit(trimmed[command.Length]))
            return null;

        return trimmed[command.Length..].TrimStart();
    }

    private static bool IsWriteCommand(string commandText)
    {
        var firstStatement = SplitStatements(commandText).FirstOrDefault();
        if (firstStatement is null)
            return false;

        var trimmed = firstStatement.TrimStart();
        return trimmed.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("DROP", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("ALTER", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("REPLACE", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("VACUUM", StringComparison.OrdinalIgnoreCase);
    }

    internal bool TryHandleFacadeStatement(string sql, out string rewrittenSql)
    {
        var connection = Connection!;
        var normalized = NormalizeSql(sql);
        if (TryParseReadUncommittedSetter(normalized, out var enabled))
        {
            connection.ReadUncommitted = enabled;
            rewrittenSql = EmptyResultSql;
            return true;
        }

        rewrittenSql = RewriteUnsupportedPragmas(normalized, sql, connection);
        return false;
    }

    private const string EmptyResultSql = "SELECT 1 WHERE 0";

    private static string RewriteFacadeStatement(string sql, SqliteConnection connection)
        => RewriteUnsupportedPragmas(NormalizeSql(sql), sql, connection);

    private static string RewriteUnsupportedPragmas(string normalized, string sql, SqliteConnection connection)
    {
        if (normalized.Equals("PRAGMA recursive_triggers", StringComparison.OrdinalIgnoreCase))
            return "SELECT " + (connection.RecursiveTriggers ? "1" : "0");
        if (TryParseReadUncommittedSetter(normalized, out _))
            return EmptyResultSql;
        if (normalized.Equals("PRAGMA read_uncommitted", StringComparison.OrdinalIgnoreCase))
            return "SELECT " + (connection.ReadUncommitted ? "1" : "0");
        if (normalized.Equals("PRAGMA compile_options", StringComparison.OrdinalIgnoreCase))
            return "SELECT CAST(NULL AS TEXT) AS compile_options WHERE 0";
        if (normalized.IndexOf("pragma_compile_options", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return normalized.IndexOf("count", StringComparison.OrdinalIgnoreCase) >= 0
                ? "SELECT 0"
                : "SELECT CAST(NULL AS TEXT) AS compile_options WHERE 0";
        }

        return sql;
    }

    private static string NormalizeSql(string sql)
        => sql.Trim().TrimEnd(';').Trim();

    private static bool TryParseReadUncommittedSetter(string normalized, out bool enabled)
    {
        enabled = false;
        const string prefix = "PRAGMA read_uncommitted";
        if (!normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;
        if (normalized.Length == prefix.Length)
            return false;

        var value = normalized[prefix.Length..].TrimStart();
        if (value.StartsWith("=", StringComparison.Ordinal))
            value = value[1..].Trim();
        else if (value.StartsWith("(", StringComparison.Ordinal) && value.EndsWith(")", StringComparison.Ordinal))
            value = value[1..^1].Trim();
        else
            return false;

        enabled = ParsePragmaEnabled(value);
        return true;
    }

    private static bool ParsePragmaEnabled(string value)
    {
        value = value.Trim('\'', '"');
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number)
            ? number != 0
            : value.Equals("ON", StringComparison.OrdinalIgnoreCase)
              || value.Equals("TRUE", StringComparison.OrdinalIgnoreCase)
              || value.Equals("YES", StringComparison.OrdinalIgnoreCase);
    }

    internal static bool CountsRowsAffected(string commandText)
    {
        var firstStatement = SplitStatements(commandText).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(firstStatement))
            return false;

        var trimmed = firstStatement.TrimStart();
        return trimmed.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("REPLACE", StringComparison.OrdinalIgnoreCase);
    }

    private static List<string> SplitStatements(string commandText)
    {
        var statements = new List<string>();
        var current = new StringBuilder();
        var inSingleQuote = false;
        var inDoubleQuote = false;
        var inLineComment = false;

        for (var i = 0; i < commandText.Length; i++)
        {
            var c = commandText[i];
            var next = i + 1 < commandText.Length ? commandText[i + 1] : '\0';

            if (inLineComment)
            {
                current.Append(c);
                if (c == '\n')
                    inLineComment = false;
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote && c == '-' && next == '-')
            {
                inLineComment = true;
                current.Append(c);
                continue;
            }

            if (c == '\'' && !inDoubleQuote)
            {
                current.Append(c);
                if (inSingleQuote && next == '\'')
                {
                    current.Append(next);
                    i++;
                    continue;
                }

                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (c == '"' && !inSingleQuote)
                inDoubleQuote = !inDoubleQuote;

            if (c == ';' && !inSingleQuote && !inDoubleQuote)
            {
                AddStatement(statements, current);
                current.Clear();
                continue;
            }

            current.Append(c);
        }

        AddStatement(statements, current);
        return statements;
    }

    private static void AddStatement(List<string> statements, StringBuilder current)
    {
        var statement = current.ToString().Trim();
        if (statement.Length != 0 && !IsEmptyCommand(statement))
            statements.Add(statement);
    }

    internal static SqliteException ToSqliteException(TursoException ex, string? sql = null)
    {
        var message = ex.Message;
        foreach (var prefix in new[] { "Unable to prepare statement: Parse error: ", "Parse error: " })
        {
            if (message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                message = message[prefix.Length..];
                break;
            }
        }
        if (message.StartsWith("Extension error: ", StringComparison.OrdinalIgnoreCase))
            message = message["Extension error: ".Length..];
        if (message.StartsWith("Error: cannot use aggregate, window functions or reference other tables in WHERE clause of CREATE INDEX", StringComparison.Ordinal))
            message = "non-deterministic functions prohibited in partial index WHERE clauses";
        const string sqliteErrorPrefix = "__turso_sqlite_error__:";
        if (message.StartsWith(sqliteErrorPrefix, StringComparison.Ordinal))
        {
            var codeEnd = message.IndexOf(':', sqliteErrorPrefix.Length);
            if (codeEnd > sqliteErrorPrefix.Length
                && int.TryParse(message[sqliteErrorPrefix.Length..codeEnd], NumberStyles.Integer, CultureInfo.InvariantCulture, out var errorCode))
            {
                var sqliteMessage = message[(codeEnd + 1)..];
                return new SqliteException(Properties.Resources.SqliteNativeError(errorCode, sqliteMessage), errorCode);
            }
        }

        if (sql is not null)
            message = PreserveNoSuchTableCase(message, sql);

        return new SqliteException(Properties.Resources.SqliteNativeError(1, message), 1);
    }

    private static string PreserveNoSuchTableCase(string message, string sql)
    {
        const string noSuchTable = "no such table: ";
        if (!message.StartsWith(noSuchTable, StringComparison.OrdinalIgnoreCase))
            return message;

        var tableName = message[noSuchTable.Length..];
        var sqlSpan = sql.AsSpan();
        for (var i = 0; i <= sqlSpan.Length - tableName.Length; i++)
        {
            if (MemoryExtensions.Equals(sqlSpan.Slice(i, tableName.Length), tableName, StringComparison.OrdinalIgnoreCase))
                return noSuchTable + sql.Substring(i, tableName.Length);
        }

        return message;
    }

    private static int FindParameterIndex(TursoStatementHandle statement, string parameterName, int parameterCount)
    {
        var index = FindExactParameterIndex(statement, parameterName, parameterCount);
        if (index != 0 || IsPrefixed(parameterName))
            return index;

        foreach (var prefix in new[] { '@', '$', ':' })
        {
            var prefixedIndex = FindExactParameterIndex(statement, prefix + parameterName, parameterCount);
            if (prefixedIndex == 0)
                continue;

            if (index != 0)
                throw new InvalidOperationException(Properties.Resources.AmbiguousParameterName(parameterName));

            index = prefixedIndex;
        }

        return index;
    }

    private static int FindExactParameterIndex(TursoStatementHandle statement, string parameterName, int parameterCount)
    {
        for (var i = 1; i <= parameterCount; i++)
        {
            if (string.Equals(TursoBindings.GetParameterName(statement, i), parameterName, StringComparison.Ordinal))
                return i;
        }

        return 0;
    }

    private static bool IsPrefixed(string parameterName)
        => parameterName.Length > 0 && parameterName[0] is '@' or '$' or ':';
}
