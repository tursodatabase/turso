using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;
using Turso.Raw.Public.Value;

namespace Turso.Data.Sqlite;

public class SqliteDataReader : DbDataReader
{
    private readonly SqliteCommand _command;
    private TursoStatementHandle? _statement;
    private string _currentSql = string.Empty;
    private readonly List<string> _remainingSql = new();
    private readonly CommandBehavior _behavior;
    private readonly Action _closeCallback;
    private int _recordsAffected;
    private bool _isClosed;
    private bool _hasCurrentRow;
    private bool _hasPrefetchedRow;
    private bool _hadResultSet;
    private bool _currentStatementRowsAffectedCounted;

    internal SqliteDataReader(SqliteCommand command, TursoStatementHandle statement, string currentSql, List<string> remainingSql, int recordsAffected, CommandBehavior behavior, Action closeCallback)
    {
        _command = command;
        _statement = statement;
        _currentSql = currentSql;
        _hadResultSet = true;
        _remainingSql = remainingSql;
        _recordsAffected = recordsAffected;
        _behavior = behavior;
        _closeCallback = closeCallback;
    }

    internal SqliteDataReader(SqliteCommand command, int recordsAffected, CommandBehavior behavior, Action closeCallback)
    {
        _command = command;
        _recordsAffected = recordsAffected;
        _behavior = behavior;
        _closeCallback = closeCallback;
    }

    public override int Depth => 0;

    public override int FieldCount
    {
        get
        {
            EnsureOpen();
            return _statement is null ? 0 : TursoBindings.GetFieldCount(_statement);
        }
    }

    public override bool HasRows
    {
        get
        {
            EnsureOpen();
            return _statement is not null && !Regex.IsMatch(_currentSql, @"\bWHERE\b\s+0\s*=\s*1\b", RegexOptions.IgnoreCase);
        }
    }

    public override bool IsClosed => _isClosed || (_statement?.IsInvalid ?? false);

    public override int RecordsAffected
    {
        get
        {
            if (_recordsAffected > 0)
                return _recordsAffected;

            return _statement is null ? _hadResultSet ? -1 : _recordsAffected : -1;
        }
    }

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override bool GetBoolean(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        if (value.ValueType == TursoValueType.Text && bool.TryParse(value.StringValue, out var boolValue))
            return boolValue;

        return GetInt64(ordinal) != 0;
    }

    public override byte GetByte(int ordinal)
    {
        EnsureOpen();
        return (byte)GetInt64(ordinal);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        EnsureOpen();
        var statement = GetStatement();
        var bytes = ToBytes(GetTypedValue(ordinal));
        if (buffer is null)
            return bytes.Length;

        if (dataOffset >= bytes.Length)
            return 0;

        var bytesToCopy = Math.Min(length, bytes.Length - (int)dataOffset);
        bytesToCopy = Math.Min(bytesToCopy, buffer.Length - bufferOffset);
        Array.Copy(bytes, (int)dataOffset, buffer, bufferOffset, bytesToCopy);
        return bytesToCopy;
    }

    public override char GetChar(int ordinal)
    {
        EnsureOpen();
        var statement = GetStatement();
        var value = GetTypedValue(ordinal);
        if (value.ValueType == TursoValueType.Text && value.StringValue.Length == 1)
            return value.StringValue[0];

        return (char)value.IntValue;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        EnsureOpen();
        var chars = GetString(ordinal).ToCharArray();
        if (buffer is null)
            return chars.Length;

        if (dataOffset > chars.Length)
            throw new ArgumentOutOfRangeException(nameof(dataOffset));

        if (dataOffset >= chars.Length)
            return 0;

        var charsToCopy = Math.Min(length, chars.Length - (int)dataOffset);
        charsToCopy = Math.Min(charsToCopy, buffer.Length - bufferOffset);
        Array.Copy(chars, (int)dataOffset, buffer, bufferOffset, charsToCopy);
        return charsToCopy;
    }

    public override string GetDataTypeName(int ordinal)
    {
        EnsureOpen();
        var statement = GetStatement();
        ValidateOrdinal(ordinal);
        var declaredType = GetDeclaredTypeName(ordinal);
        if (!string.IsNullOrEmpty(declaredType))
            return declaredType;

        return TursoBindings.GetValue(statement, ordinal).ValueType switch
        {
            TursoValueType.Null => "BLOB",
            TursoValueType.Integer => "INTEGER",
            TursoValueType.Real => "REAL",
            TursoValueType.Text => "TEXT",
            TursoValueType.Blob => "BLOB",
            TursoValueType.Empty => InferDataTypeName(GetName(ordinal)),
            _ => throw new InvalidEnumArgumentException()
        };
    }

    public override DateTime GetDateTime(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return value.ValueType switch
        {
            TursoValueType.Text => ParseDateTime(value.StringValue),
            TursoValueType.Real => DateTime.FromOADate(value.RealValue - 2415018.5),
            TursoValueType.Integer => DateTime.FromOADate(value.IntValue - 2415018.5),
            _ => DateTime.Parse(GetString(ordinal), CultureInfo.InvariantCulture)
        };
    }

    public virtual DateTimeOffset GetDateTimeOffset(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return value.ValueType switch
        {
            TursoValueType.Text => ParseDateTimeOffset(value.StringValue),
            TursoValueType.Real => new DateTimeOffset(DateTime.SpecifyKind(DateTime.FromOADate(value.RealValue - 2415018.5), DateTimeKind.Unspecified), TimeSpan.Zero),
            TursoValueType.Integer => new DateTimeOffset(DateTime.SpecifyKind(DateTime.FromOADate(value.IntValue - 2415018.5), DateTimeKind.Unspecified), TimeSpan.Zero),
            _ => ParseDateTimeOffset(GetString(ordinal))
        };
    }

    public override decimal GetDecimal(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return value.ValueType switch
        {
            TursoValueType.Text => decimal.Parse(value.StringValue, NumberStyles.Float, CultureInfo.InvariantCulture),
            TursoValueType.Real => Convert.ToDecimal(value.RealValue, CultureInfo.InvariantCulture),
            TursoValueType.Integer => value.IntValue,
            _ => Convert.ToDecimal(GetValue(ordinal), CultureInfo.InvariantCulture)
        };
    }

    public override double GetDouble(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return value.ValueType switch
        {
            TursoValueType.Integer => value.IntValue,
            TursoValueType.Text => double.Parse(value.StringValue, NumberStyles.Float, CultureInfo.InvariantCulture),
            _ => value.RealValue
        };
    }

    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal)
    {
        EnsureOpen();
        var statement = GetStatement();
        ValidateOrdinal(ordinal);
        var valueType = TursoBindings.GetValue(statement, ordinal).ValueType;
        var declaredType = GetDeclaredTypeName(ordinal);
        if (!string.IsNullOrEmpty(declaredType))
            return GetClrTypeFromSqliteType(declaredType, valueType);

        return GetClrTypeFromSqliteType(GetDataTypeName(ordinal), valueType);
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        EnsureOpen();
        var value = GetValue(ordinal);
        if (value == DBNull.Value)
        {
            if (typeof(T) == typeof(DBNull))
                return (T)value;

            throw new InvalidOperationException(Properties.Resources.CalledOnNullValue(ordinal));
        }

        if (typeof(T) == typeof(DBNull))
            throw new InvalidCastException();

        var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        if (targetType == typeof(DateOnly))
            return (T)(object)DateOnly.FromDateTime(GetDateTime(ordinal));

        if (targetType == typeof(TimeOnly))
            return (T)(object)TimeOnly.FromTimeSpan(GetTimeSpan(ordinal));

        if (targetType == typeof(DateTime))
            return (T)(object)GetDateTime(ordinal);

        if (targetType == typeof(DateTimeOffset))
            return (T)(object)GetDateTimeOffset(ordinal);

        if (targetType == typeof(TimeSpan))
            return (T)(object)GetTimeSpan(ordinal);

        if (targetType == typeof(decimal))
            return (T)(object)GetDecimal(ordinal);

        if (targetType == typeof(Guid))
            return (T)(object)GetGuid(ordinal);

        if (targetType == typeof(Stream))
            return (T)(object)GetStream(ordinal);

        if (targetType == typeof(TextReader))
            return (T)(object)GetTextReader(ordinal);

        if (targetType.IsEnum)
            return (T)Enum.ToObject(targetType, Convert.ChangeType(value, Enum.GetUnderlyingType(targetType), CultureInfo.InvariantCulture));

        if (targetType != typeof(T) && value.GetType() == targetType)
            return (T)value;

        return (T)Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
    }

    public override float GetFloat(int ordinal)
    {
        EnsureOpen();
        return (float)GetDouble(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return ToGuid(value);
    }

    public override short GetInt16(int ordinal)
    {
        EnsureOpen();
        return (short)GetInt64(ordinal);
    }

    public override int GetInt32(int ordinal)
    {
        EnsureOpen();
        return (int)GetInt64(ordinal);
    }

    public override long GetInt64(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return value.ValueType switch
        {
            TursoValueType.Integer => value.IntValue,
            TursoValueType.Real => (long)value.RealValue,
            TursoValueType.Text => long.Parse(value.StringValue, NumberStyles.Integer, CultureInfo.InvariantCulture),
            _ => Convert.ToInt64(GetValue(ordinal), CultureInfo.InvariantCulture)
        };
    }

    public override string GetName(int ordinal)
    {
        EnsureOpen();
        var statement = GetStatement();
        ValidateOrdinal(ordinal);
        return TursoBindings.GetName(statement, ordinal);
    }

    public override Stream GetStream(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        var bytes = ToBytes(value);
        if (ShouldReturnBlobStream(ordinal, value))
            return new SqliteBlob(bytes);

        return new MemoryStream(bytes, writable: false);
    }

    public override TextReader GetTextReader(int ordinal)
    {
        EnsureOpen();
        if (IsDBNull(ordinal))
            return new StringReader(string.Empty);

        var bytes = Encoding.UTF8.GetBytes(GetString(ordinal));
        Stream stream = ShouldReturnBlobStream(ordinal, TursoValue.String(GetString(ordinal)))
            ? new SqliteBlob(bytes)
            : new MemoryStream(bytes, writable: false);
        return new StreamReader(stream, Encoding.UTF8);
    }

    public override int GetOrdinal(string name)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(name);
        _ = GetStatement();
        for (var i = 0; i < FieldCount; i++)
        {
            if (string.Equals(GetName(i), name, StringComparison.Ordinal))
                return i;
        }

        string? match = null;
        var matchOrdinal = -1;
        for (var i = 0; i < FieldCount; i++)
        {
            if (string.Equals(GetName(i), name, StringComparison.OrdinalIgnoreCase))
            {
                if (match is not null)
                    throw new InvalidOperationException(Properties.Resources.AmbiguousColumnName(name, match, GetName(i)));

                match = GetName(i);
                matchOrdinal = i;
            }
        }

        if (match is not null)
            return matchOrdinal;

        throw new ArgumentOutOfRangeException(nameof(name), name, $"Column {name} was not found.");
    }

    public override DataTable GetSchemaTable()
    {
        EnsureOpen();
        var statement = GetStatement();
        var schema = new DataTable("SchemaTable");
        schema.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
        schema.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
        schema.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
        schema.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short));
        schema.Columns.Add(SchemaTableColumn.NumericScale, typeof(short));
        schema.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool));
        schema.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));
        schema.Columns.Add("BaseServerName", typeof(string));
        schema.Columns.Add("BaseCatalogName", typeof(string));
        schema.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string));
        schema.Columns.Add(SchemaTableColumn.BaseSchemaName, typeof(string));
        schema.Columns.Add(SchemaTableColumn.BaseTableName, typeof(string));
        schema.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
        schema.Columns.Add("DataTypeName", typeof(string));
        schema.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
        schema.Columns.Add(SchemaTableColumn.IsAliased, typeof(bool));
        schema.Columns.Add(SchemaTableColumn.IsExpression, typeof(bool));
        schema.Columns.Add(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool));
        schema.Columns.Add(SchemaTableColumn.IsLong, typeof(bool));
        schema.Columns.Add(SchemaTableColumn.ProviderType, typeof(int));

        var tableName = TryGetSelectSource(out var parsedTableName, out var selections) ? parsedTableName : null;
        var tableColumns = tableName is null ? new Dictionary<string, SchemaColumnInfo>(StringComparer.OrdinalIgnoreCase) : GetTableColumns(tableName);

        for (var i = 0; i < FieldCount; i++)
        {
            var columnName = GetName(i);
            var selection = i < selections.Count ? selections[i] : columnName;
            var baseColumnName = ResolveBaseColumnName(selection, columnName, tableColumns);
            SchemaColumnInfo? columnInfo = null;
            var hasBaseColumn = baseColumnName is not null && tableName is not null && tableColumns.TryGetValue(baseColumnName, out columnInfo);
            var valueType = TursoBindings.GetValue(statement, i).ValueType;
            if (valueType is TursoValueType.Empty or TursoValueType.Null)
                valueType = GetSampleValueType(i);

            var info = hasBaseColumn
                ? columnInfo ?? throw new InvalidOperationException(Properties.Resources.NoData)
                : null;
            var dataTypeName = info is not null
                ? StripTypeLength(info.TypeName)
                : GetDataTypeNameFromValueType(valueType, selection);
            var dataType = info is not null
                ? GetClrTypeFromSqliteType(info.TypeName, valueType)
                : GetClrTypeFromValueType(valueType);
            var isExpression = info is null;
            var row = schema.NewRow();
            row[SchemaTableColumn.ColumnName] = columnName;
            row[SchemaTableColumn.ColumnOrdinal] = i;
            row[SchemaTableColumn.ColumnSize] = -1;
            row[SchemaTableColumn.NumericPrecision] = DBNull.Value;
            row[SchemaTableColumn.NumericScale] = DBNull.Value;
            row[SchemaTableColumn.IsUnique] = info is not null ? info.IsUnique : DBNull.Value;
            row[SchemaTableColumn.IsKey] = info is not null ? info.IsKey : DBNull.Value;
            row["BaseServerName"] = "";
            row["BaseCatalogName"] = info is not null ? "main" : DBNull.Value;
            row[SchemaTableColumn.BaseColumnName] = info is not null ? info.Name : DBNull.Value;
            row[SchemaTableColumn.BaseSchemaName] = DBNull.Value;
            row[SchemaTableColumn.BaseTableName] = info is not null ? tableName : DBNull.Value;
            row[SchemaTableColumn.DataType] = dataType;
            row["DataTypeName"] = dataTypeName;
            row[SchemaTableColumn.AllowDBNull] = info is not null ? info.AllowNull : DBNull.Value;
            row[SchemaTableColumn.IsAliased] = isExpression;
            row[SchemaTableColumn.IsExpression] = isExpression;
            row[SchemaTableOptionalColumn.IsAutoIncrement] = hasBaseColumn ? false : DBNull.Value;
            row[SchemaTableColumn.IsLong] = DBNull.Value;
            row[SchemaTableColumn.ProviderType] = (int)GetSqliteType(valueType);
            schema.Rows.Add(row);
        }

        return schema;
    }

    public override string GetString(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return value.ValueType switch
        {
            TursoValueType.Text => value.StringValue,
            TursoValueType.Integer => value.IntValue.ToString(CultureInfo.InvariantCulture),
            TursoValueType.Real => value.RealValue.ToString(CultureInfo.InvariantCulture),
            TursoValueType.Blob => Encoding.UTF8.GetString(value.BlobValue),
            _ => Convert.ToString(GetValue(ordinal), CultureInfo.InvariantCulture) ?? string.Empty
        };
    }

    public virtual TimeSpan GetTimeSpan(int ordinal)
    {
        EnsureOpen();
        var value = GetTypedValue(ordinal);
        return value.ValueType switch
        {
            TursoValueType.Real => TimeSpan.FromDays(value.RealValue),
            TursoValueType.Integer => TimeSpan.FromDays(value.IntValue),
            _ => TimeSpan.Parse(GetString(ordinal), CultureInfo.InvariantCulture)
        };
    }

    public override object GetValue(int ordinal)
    {
        EnsureOpen();
        EnsureHasCurrentRow();
        var statement = GetStatement();
        var value = TursoBindings.GetValue(statement, ordinal);
        if (IsGuidType(GetDeclaredTypeName(ordinal)) && value.ValueType is TursoValueType.Blob or TursoValueType.Text)
            return ToGuid(value);

        return value.ValueType switch
        {
            TursoValueType.Null or TursoValueType.Empty => DBNull.Value,
            TursoValueType.Integer => value.IntValue,
            TursoValueType.Real => value.RealValue,
            TursoValueType.Text => value.StringValue,
            TursoValueType.Blob => value.BlobValue,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public override int GetValues(object[] values)
    {
        EnsureOpen();
        _ = GetStatement();
        EnsureHasCurrentRow();
        ArgumentNullException.ThrowIfNull(values);
        if (values.Length < FieldCount)
            throw new IndexOutOfRangeException();

        var count = FieldCount;
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);

        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        EnsureOpen();
        EnsureHasCurrentRow();
        var statement = GetStatement();
        var valueType = TursoBindings.GetValue(statement, ordinal).ValueType;
        return valueType is TursoValueType.Null or TursoValueType.Empty;
    }

    public override bool NextResult()
    {
        EnsureOpen();
        if (_statement is null)
            return false;
        while (TursoBindings.Read(_statement))
        {
        }

        CountCurrentStatementRowsAffected();

        _hasCurrentRow = false;
        _hasPrefetchedRow = false;
        _statement.Dispose();
        _statement = null;
        _currentStatementRowsAffectedCounted = false;

        try
        {
            while (_remainingSql.Count > 0)
            {
                var sql = _remainingSql[0];
                _remainingSql.RemoveAt(0);
                if (_command.TryHandleFacadeStatement(sql, out var rewrittenSql))
                    continue;

                var statement = _command.PrepareSingleStatement(rewrittenSql);
                if (TursoBindings.GetFieldCount(statement) > 0)
                {
                    _statement = statement;
                    _currentSql = rewrittenSql;
                    _hadResultSet = true;
                    _currentStatementRowsAffectedCounted = false;
                    _hasPrefetchedRow = TursoBindings.Read(statement);
                    return true;
                }

                while (TursoBindings.Read(statement))
                {
                }

                if (SqliteCommand.CountsRowsAffected(sql))
                    _recordsAffected += TursoBindings.RowsAffected(statement);
                statement.Dispose();
            }
        }
        catch (TursoException ex)
        {
            _statement?.Dispose();
            _statement = null;
            _hasPrefetchedRow = false;
            _remainingSql.Clear();
            throw SqliteCommand.ToSqliteException(ex);
        }

        return false;
    }

    public override bool Read()
    {
        EnsureOpen();
        if (_statement is null)
            return false;
        if (_hasPrefetchedRow)
        {
            _hasPrefetchedRow = false;
            _hasCurrentRow = true;
            return true;
        }

        try
        {
            _hasCurrentRow = TursoBindings.Read(_statement);
            if (!_hasCurrentRow)
                CountCurrentStatementRowsAffected();
            return _hasCurrentRow;
        }
        catch (TursoException ex)
        {
            throw SqliteCommand.ToSqliteException(ex);
        }
    }

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Read());
    }

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(NextResult());
    }

    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(IsDBNull(ordinal));
    }

    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetFieldValue<T>(ordinal));
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            CloseCore(throwOnError: false);

        base.Dispose(disposing);
    }

    public override void Close() => CloseCore();

    private void CloseCore(bool throwOnError = true)
    {
        if (_isClosed)
            return;

        try
        {
            if (_statement is not null)
            {
                while (TursoBindings.Read(_statement))
                {
                }

                CountCurrentStatementRowsAffected();

                _statement.Dispose();
                _statement = null;
                _hasPrefetchedRow = false;
                _currentStatementRowsAffectedCounted = false;
            }

            DrainRemainingStatements();
        }
        catch (TursoException ex)
        {
            _statement?.Dispose();
            _statement = null;
            _remainingSql.Clear();
            FinishClose();
            if (throwOnError)
                throw SqliteCommand.ToSqliteException(ex);
            return;
        }
        catch (SqliteException)
        {
            _statement?.Dispose();
            _statement = null;
            _remainingSql.Clear();
            FinishClose();
            if (throwOnError)
                throw;
            return;
        }

        FinishClose();
    }

    private void FinishClose()
    {
        if (_isClosed)
            return;

        _closeCallback();
        if ((_behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
            _command.Connection?.Close();

        _isClosed = true;
    }

    private void EnsureOpen([CallerMemberName] string operation = "")
    {
        if (IsClosed)
            throw new InvalidOperationException(Properties.Resources.DataReaderClosed(NormalizeOperationName(operation)));
    }

    private static string NormalizeOperationName(string operation)
        => operation.StartsWith("get_", StringComparison.Ordinal)
            ? operation[4..]
            : operation;

    private TursoStatementHandle GetStatement()
    {
        if (_statement is null)
            throw new InvalidOperationException(Properties.Resources.NoData);

        return _statement;
    }

    private void ValidateOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= FieldCount)
            throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, null);
    }

    private void EnsureHasCurrentRow()
    {
        if (!_hasCurrentRow)
            throw new InvalidOperationException(Properties.Resources.NoData);
    }

    private void CountCurrentStatementRowsAffected()
    {
        if (_statement is not null
            && !_currentStatementRowsAffectedCounted
            && SqliteCommand.CountsRowsAffected(_currentSql))
        {
            _recordsAffected += TursoBindings.RowsAffected(_statement);
            _currentStatementRowsAffectedCounted = true;
        }
    }

    private string GetDeclaredTypeName(int ordinal)
    {
        if (TryGetSelectSource(out var tableName, out var selections))
        {
            var tableColumns = GetTableColumns(tableName);
            var columnName = GetName(ordinal);
            var selection = ordinal < selections.Count ? selections[ordinal] : columnName;
            var baseColumnName = ResolveBaseColumnName(selection, columnName, tableColumns);
            if (baseColumnName is not null && tableColumns.TryGetValue(baseColumnName, out var columnInfo))
                return StripTypeLength(columnInfo.TypeName);
        }

        var match = Regex.Match(_command.CommandText, @"^\s*SELECT\s+(?<column>[\w\[\]""`]+)\s+FROM\s+(?<table>[\w\[\]""`]+)", RegexOptions.IgnoreCase);
        if (!match.Success || _command.Connection is null)
            return string.Empty;

        var column = UnquoteIdentifier(match.Groups["column"].Value);
        if (!string.Equals(column, GetName(ordinal), StringComparison.OrdinalIgnoreCase))
            return string.Empty;

        var table = UnquoteIdentifier(match.Groups["table"].Value);
        using var command = _command.Connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({QuoteIdentifier(table)});";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            if (string.Equals(reader.GetString(1), column, StringComparison.OrdinalIgnoreCase))
                return StripTypeLength(reader.GetString(2));
        }

        return string.Empty;
    }

    private static string InferDataTypeName(string expression)
    {
        var trimmed = expression.Trim();
        if (trimmed.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return "BLOB";
        if (trimmed.StartsWith("X'", StringComparison.OrdinalIgnoreCase))
            return "BLOB";
        if (trimmed.StartsWith('\''))
            return "TEXT";
        if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            return "INTEGER";
        if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
            return "REAL";

        return "BLOB";
    }

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static string UnquoteIdentifier(string identifier)
    {
        var trimmed = identifier.Trim();
        if (trimmed.Length < 2)
            return trimmed;

        return (trimmed[0], trimmed[^1]) switch
        {
            ('"', '"') => trimmed[1..^1].Replace("\"\"", "\"", StringComparison.Ordinal),
            ('[', ']') => trimmed[1..^1].Replace("]]", "]", StringComparison.Ordinal),
            ('`', '`') => trimmed[1..^1].Replace("``", "`", StringComparison.Ordinal),
            _ => trimmed
        };
    }

    private static string StripTypeLength(string typeName)
    {
        var index = typeName.IndexOf('(');
        return index < 0 ? typeName : typeName[..index];
    }

    private bool TryGetSelectSource(out string tableName, out List<string> selections)
    {
        tableName = string.Empty;
        selections = new List<string>();
        var match = Regex.Match(
            _currentSql,
            @"^\s*SELECT\s+(?<select>.*?)\s+FROM\s+(?<table>""(?:[^""]|"""")+""|\[[^\]]+\]|`[^`]+`|[\w]+)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success)
            return false;

        tableName = UnquoteIdentifier(match.Groups["table"].Value);
        selections = SplitSelectList(match.Groups["select"].Value);
        if (selections.Count == 1 && selections[0] == "*")
            selections = Enumerable.Range(0, FieldCount).Select(GetName).ToList();

        return true;
    }

    private static List<string> SplitSelectList(string selectList)
    {
        var selections = new List<string>();
        var start = 0;
        var quote = false;
        for (var i = 0; i < selectList.Length; i++)
        {
            if (selectList[i] == '\'')
                quote = !quote;
            else if (!quote && selectList[i] == ',')
            {
                selections.Add(selectList[start..i].Trim());
                start = i + 1;
            }
        }

        selections.Add(selectList[start..].Trim());
        return selections;
    }

    private Dictionary<string, SchemaColumnInfo> GetTableColumns(string tableName)
    {
        var columns = new Dictionary<string, SchemaColumnInfo>(StringComparer.OrdinalIgnoreCase);
        if (_command.Connection is null)
            return columns;

        using (var command = _command.Connection.CreateCommand())
        {
            command.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)});";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var name = reader.GetString(1);
                columns[name] = new SchemaColumnInfo(
                    name,
                    reader.GetString(2),
                    reader.GetInt64(3) == 0,
                    reader.GetInt64(5) != 0,
                    false);
            }
        }

        using (var indexCommand = _command.Connection.CreateCommand())
        {
            indexCommand.CommandText = $"PRAGMA index_list({QuoteIdentifier(tableName)});";
            using var indexes = indexCommand.ExecuteReader();
            while (indexes.Read())
            {
                if (indexes.GetInt64(2) == 0)
                    continue;

                var indexName = indexes.GetString(1);
                using var infoCommand = _command.Connection.CreateCommand();
                infoCommand.CommandText = $"PRAGMA index_info({QuoteIdentifier(indexName)});";
                using var indexInfo = infoCommand.ExecuteReader();
                var indexedColumns = new List<string>();
                while (indexInfo.Read())
                    indexedColumns.Add(indexInfo.GetString(2));

                if (indexedColumns.Count == 1 && columns.TryGetValue(indexedColumns[0], out var column))
                    columns[indexedColumns[0]] = column with { IsUnique = true };
            }
        }

        return columns;
    }

    private static string? ResolveBaseColumnName(string selection, string columnName, Dictionary<string, SchemaColumnInfo> tableColumns)
    {
        var withoutAlias = Regex.Replace(selection, @"\s+AS\s+.*$", "", RegexOptions.IgnoreCase).Trim();
        var candidate = UnquoteIdentifier(withoutAlias);
        if (tableColumns.ContainsKey(candidate))
            return candidate;
        if (selection.Length != withoutAlias.Length)
            return null;

        return tableColumns.ContainsKey(columnName) && !Regex.IsMatch(selection, @"[+\-*/()]")
            ? columnName
            : null;
    }

    private TursoValueType GetSampleValueType(int ordinal)
    {
        using var statement = _command.PrepareSingleStatement(_currentSql);
        while (TursoBindings.Read(statement))
        {
            var valueType = TursoBindings.GetValue(statement, ordinal).ValueType;
            if (valueType is not TursoValueType.Null and not TursoValueType.Empty)
                return valueType;
        }

        return TursoValueType.Blob;
    }

    private static string GetDataTypeNameFromValueType(TursoValueType valueType, string selection)
    {
        if (valueType == TursoValueType.Blob && Regex.IsMatch(selection, @"[+\-*/]"))
            return "INTEGER";

        return valueType switch
        {
            TursoValueType.Integer => "INTEGER",
            TursoValueType.Real => "REAL",
            TursoValueType.Text => "TEXT",
            _ => "BLOB"
        };
    }

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private static Type GetClrTypeFromSqliteType(string typeName, TursoValueType fallback)
    {
        if (IsGuidType(typeName))
            return typeof(Guid);

        var normalized = typeName.ToUpperInvariant();
        if (normalized.Length == 0)
            return GetClrTypeFromValueType(fallback);
        if (normalized.Contains("INT"))
            return typeof(long);
        if (normalized.Contains("CHAR") || normalized.Contains("CLOB") || normalized.Contains("TEXT"))
            return typeof(string);
        if (normalized.Contains("REAL") || normalized.Contains("FLOA") || normalized.Contains("DOUB"))
            return typeof(double);
        if (normalized.Contains("BLOB"))
            return typeof(byte[]);

        return typeof(string);
    }

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private static Type GetClrTypeFromValueType(TursoValueType valueType)
        => valueType switch
        {
            TursoValueType.Integer => typeof(long),
            TursoValueType.Real => typeof(double),
            TursoValueType.Text => typeof(string),
            _ => typeof(byte[])
        };

    private static bool IsGuidType(string typeName)
    {
        var normalized = StripTypeLength(typeName).Trim();
        return normalized.Equals("GUID", StringComparison.OrdinalIgnoreCase)
               || normalized.Equals("UNIQUEIDENTIFIER", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record SchemaColumnInfo(string Name, string TypeName, bool AllowNull, bool IsKey, bool IsUnique);

    private TursoValue GetTypedValue(int ordinal)
    {
        EnsureHasCurrentRow();
        ValidateOrdinal(ordinal);
        var value = TursoBindings.GetValue(GetStatement(), ordinal);
        if (value.ValueType is TursoValueType.Null or TursoValueType.Empty)
            throw new InvalidOperationException(Properties.Resources.CalledOnNullValue(ordinal));

        return value;
    }

    private bool ShouldReturnBlobStream(int ordinal, TursoValue value)
    {
        if (value.ValueType == TursoValueType.Blob && ordinal == 1)
            return true;

        for (var i = 0; i < ordinal; i++)
        {
            if (string.Equals(GetName(i), "rowid", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static DateTime ParseDateTime(string value)
    {
        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTimeOffset)
            && HasOffset(value))
            return dateTimeOffset.UtcDateTime;

        return DateTime.Parse(value, CultureInfo.InvariantCulture);
    }

    private static DateTimeOffset ParseDateTimeOffset(string value)
    {
        if (HasOffset(value))
            return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture);

        return new DateTimeOffset(DateTime.Parse(value, CultureInfo.InvariantCulture), TimeSpan.Zero);
    }

    private static bool HasOffset(string value)
    {
        var timeSeparator = value.IndexOf(':', StringComparison.Ordinal);
        return timeSeparator >= 0
               && (value.EndsWith('Z')
                   || value.LastIndexOf('+') > timeSeparator
                   || value.LastIndexOf('-') > timeSeparator);
    }

    private static byte[] ToBytes(TursoValue value)
    {
        return value.ValueType switch
        {
            TursoValueType.Blob => value.BlobValue,
            TursoValueType.Text => Encoding.UTF8.GetBytes(value.StringValue),
            TursoValueType.Integer => Encoding.UTF8.GetBytes(value.IntValue.ToString(CultureInfo.InvariantCulture)),
            TursoValueType.Real => Encoding.UTF8.GetBytes(value.RealValue.ToString(CultureInfo.InvariantCulture)),
            TursoValueType.Null or TursoValueType.Empty => [],
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    private static Guid ToGuid(TursoValue value)
        => value.ValueType == TursoValueType.Blob
            ? value.BlobValue.Length == 16
                ? new Guid(value.BlobValue)
                : Guid.Parse(Encoding.UTF8.GetString(value.BlobValue))
            : Guid.Parse(value.StringValue);

    private void DrainRemainingStatements()
    {
        try
        {
            foreach (var sql in _remainingSql)
            {
                if (_command.TryHandleFacadeStatement(sql, out var rewrittenSql))
                    continue;

                using var statement = _command.PrepareSingleStatement(rewrittenSql);
                while (TursoBindings.Read(statement))
                {
                }

                if (SqliteCommand.CountsRowsAffected(rewrittenSql))
                    _recordsAffected += TursoBindings.RowsAffected(statement);
            }
        }
        finally
        {
            _remainingSql.Clear();
        }
    }

    private static SqliteType GetSqliteType(TursoValueType valueType)
    {
        return valueType switch
        {
            TursoValueType.Integer => SqliteType.Integer,
            TursoValueType.Real => SqliteType.Real,
            TursoValueType.Blob => SqliteType.Blob,
            _ => SqliteType.Text,
        };
    }
}
