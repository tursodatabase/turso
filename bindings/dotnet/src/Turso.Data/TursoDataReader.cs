using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;
using Turso.Raw.Public.Value;

namespace Turso;

public class TursoDataReader : DbDataReader
{
    private readonly TursoCommand _command;
    private readonly TursoStatementHandle _statement;
    private readonly CommandBehavior _behavior;
    private readonly Type?[] _fieldTypes;
    private IDisposable? _syncOperation;
    private TursoConnection? _syncConnection;
    private bool _isClosed;

    public TursoDataReader(TursoCommand command, TursoStatementHandle statement, CommandBehavior behavior)
        : this(command, statement, behavior, syncOperation: null)
    {
    }

    public TursoDataReader(
        TursoCommand command,
        TursoStatementHandle statement,
        CommandBehavior behavior,
        IDisposable? syncOperation)
    {
        _command = command;
        _statement = statement;
        _behavior = behavior;
        _syncOperation = syncOperation;
        _fieldTypes = new Type?[TursoBindings.GetFieldCount(statement)];
        if (syncOperation is not null && command.Connection is TursoConnection connection)
        {
            _syncConnection = connection;
            connection.RegisterSyncReader(this);
        }
    }

    public override bool GetBoolean(int ordinal)
    {
        return TursoBindings.GetValue(_statement, ordinal).IntValue != 0;
    }

    public override byte GetByte(int ordinal)
    {
        return (byte)TursoBindings.GetValue(_statement, ordinal).IntValue;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        return GetArray(ordinal, dataOffset, buffer, bufferOffset, length);
    }

    public override char GetChar(int ordinal)
    {
        var value = TursoBindings.GetValue(_statement, ordinal);
        if (value.ValueType == TursoValueType.Text && value.StringValue.Length == 1)
        {
            return value.StringValue[0];
        }

        return (char)TursoBindings.GetValue(_statement, ordinal).IntValue;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        return GetArray(ordinal, dataOffset, buffer, bufferOffset, length);
    }

    public override string GetDataTypeName(int ordinal)
    {
        EnsureOpen();
        ValidateOrdinal(ordinal);
        var declaredType = TursoBindings.GetDeclaredTypeName(_statement, ordinal);
        if (!string.IsNullOrWhiteSpace(declaredType))
            return declaredType;

        var value = TursoBindings.GetValue(_statement, ordinal);
        return GetTypeName(value.ValueType);
    }

    public override DateTime GetDateTime(int ordinal)
    {
        var value = TursoBindings.GetValue(_statement, ordinal);
        switch (value.ValueType)
        {
            case TursoValueType.Text:
                return DateTime.Parse(GetString(ordinal), CultureInfo.InvariantCulture);
            default:
                return DateTime.MinValue;
        }
    }

    public override decimal GetDecimal(int ordinal)
    {
        return (decimal)TursoBindings.GetValue(_statement, ordinal).RealValue;
    }

    public override double GetDouble(int ordinal)
    {
        return TursoBindings.GetValue(_statement, ordinal).RealValue;
    }

    public override Type GetFieldType(int ordinal)
    {
        EnsureOpen();
        ValidateOrdinal(ordinal);
        if (_fieldTypes[ordinal] is { } fieldType)
            return fieldType;

        var declaredType = TursoBindings.GetDeclaredTypeName(_statement, ordinal);
        if (DataReaderCompatibility.TryGetClrTypeFromDeclaredType(declaredType, out fieldType))
        {
            _fieldTypes[ordinal] = fieldType;
            return fieldType;
        }

        var value = TursoBindings.GetValue(_statement, ordinal);
        fieldType = value.ValueType switch
        {
            TursoValueType.Integer => typeof(long),
            TursoValueType.Real => typeof(double),
            TursoValueType.Text => typeof(string),
            TursoValueType.Blob => typeof(byte[]),
            _ => typeof(object)
        };

        if (value.ValueType is not TursoValueType.Null and not TursoValueType.Empty)
            _fieldTypes[ordinal] = fieldType;

        return fieldType;
    }

    public override float GetFloat(int ordinal)
    {
        return (float)TursoBindings.GetValue(_statement, ordinal).RealValue;
    }

    public override Guid GetGuid(int ordinal)
    {
        return DataReaderCompatibility.ToGuid(GetValue(ordinal));
    }

    public override short GetInt16(int ordinal)
    {
        return (short)TursoBindings.GetValue(_statement, ordinal).IntValue;
    }

    public override int GetInt32(int ordinal)
    {
        return (int)TursoBindings.GetValue(_statement, ordinal).IntValue;
    }

    public override long GetInt64(int ordinal)
    {
        return TursoBindings.GetValue(_statement, ordinal).IntValue;
    }

    public override string GetName(int ordinal)
    {
        return TursoBindings.GetName(_statement, ordinal);
    }

    public override int GetOrdinal(string name)
    {
        var fields = TursoBindings.GetFieldCount(_statement);
        for (var i = 0; i < fields; i++)
        {
            var columnName = TursoBindings.GetName(_statement, i);
            if (columnName == name)
                return i;
        }

        throw new IndexOutOfRangeException($"column {name} not found");
    }

    public override string GetString(int ordinal)
    {
        return TursoBindings.GetValue(_statement, ordinal).StringValue;
    }

    public override object GetValue(int ordinal)
    {
        var value = TursoBindings.GetValue(_statement, ordinal);
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
        var i = 0;
        for (; i < FieldCount; i++)
        {
            values[i] = GetValue(i)!;
        }

        return i;
    }

    public override bool IsDBNull(int ordinal)
    {
        var valueType = TursoBindings.GetValue(_statement, ordinal).ValueType;
        return valueType == TursoValueType.Null;
    }

    public override int FieldCount => TursoBindings.GetFieldCount(_statement);

    public override DataTable GetSchemaTable()
    {
        EnsureOpen();
        return DataReaderCompatibility.CreateSchemaTable(this);
    }

    public override object this[int ordinal] => GetValue(ordinal)!;

    public override object this[string name]
    {
        get
        {
            var ordinal = GetOrdinal(name);
            return GetValue(ordinal)!;
        }
    }

    public override int RecordsAffected => TursoBindings.RowsAffected(_statement);
    public override bool HasRows => TursoBindings.HasRows(_statement);
    public override bool IsClosed => _isClosed || _statement.IsInvalid;

    public override bool NextResult()
    {
        EnsureOpen();
        while (TursoBindings.Read(_statement, RunExternalIo))
        {
        }

        return false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_isClosed)
        {
            _statement.Dispose();
            _syncOperation?.Dispose();
            _syncOperation = null;
            _syncConnection?.UnregisterSyncReader(this);
            _syncConnection = null;
            if ((_behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
                _command.Connection?.Close();
        }

        _isClosed = true;
        base.Dispose(disposing);
    }

    public override bool Read()
    {
        EnsureOpen();
        return TursoBindings.Read(_statement, RunExternalIo);
    }

    public override int Depth => 0;

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, closeReader: false);
    }

    private long GetArray<T>(int ordinal, long dataOffset, T[]? buffer, int bufferOffset, int length)
        where T : struct
    {
        var bytes = TursoBindings.GetValue(_statement, ordinal).BlobValue;
        if (buffer is null)
        {
            return Math.Min(bytes.Length - dataOffset, length);
        }

        var position = 0;
        for (; position < length; position++)
        {
            if (bufferOffset + position >= buffer.Length || position + dataOffset >= bytes.Length)
                break;

            buffer[bufferOffset + position] = Unsafe.As<byte, T>(ref bytes[position + dataOffset]);
        }

        return position;
    }

    private static string GetTypeName(TursoValueType valueType)
    {
        return valueType switch
        {
            TursoValueType.Empty => "",
            TursoValueType.Null => "NULL",
            TursoValueType.Integer => "INTEGER",
            TursoValueType.Real => "REAL",
            TursoValueType.Text => "TEXT",
            TursoValueType.Blob => "BLOB",
            _ => throw new InvalidEnumArgumentException(nameof(valueType))
        };
    }

    private void EnsureOpen()
    {
        if (IsClosed)
            throw new InvalidOperationException("The data reader is closed.");
    }

    private void RunExternalIo()
    {
        _syncConnection?.RunExternalIo();
    }

    private void ValidateOrdinal(int ordinal)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ordinal);
        if (ordinal >= FieldCount)
            throw new IndexOutOfRangeException($"column ordinal {ordinal} is out of range");
    }
}

internal static class DataReaderCompatibility
{
    public static DataTable CreateSchemaTable(DbDataReader reader)
    {
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
        schema.Columns.Add(SchemaTableOptionalColumn.IsHidden, typeof(bool));
        schema.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));
        schema.Columns.Add(SchemaTableOptionalColumn.IsRowVersion, typeof(bool));
        schema.Columns.Add(SchemaTableColumn.IsLong, typeof(bool));
        schema.Columns.Add(SchemaTableColumn.ProviderType, typeof(int));
        schema.Columns.Add(SchemaTableColumn.NonVersionedProviderType, typeof(int));
        schema.Columns.Add(SchemaTableOptionalColumn.ProviderSpecificDataType, typeof(Type));

        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            var fieldType = reader.GetFieldType(ordinal);
            var row = schema.NewRow();
            row[SchemaTableColumn.ColumnName] = reader.GetName(ordinal);
            row[SchemaTableColumn.ColumnOrdinal] = ordinal;
            row[SchemaTableColumn.ColumnSize] = -1;
            row[SchemaTableColumn.NumericPrecision] = DBNull.Value;
            row[SchemaTableColumn.NumericScale] = DBNull.Value;
            row[SchemaTableColumn.IsUnique] = false;
            row[SchemaTableColumn.IsKey] = false;
            row["BaseServerName"] = "";
            row["BaseCatalogName"] = "";
            row[SchemaTableColumn.BaseColumnName] = reader.GetName(ordinal);
            row[SchemaTableColumn.BaseSchemaName] = "";
            row[SchemaTableColumn.BaseTableName] = "";
            row[SchemaTableColumn.DataType] = fieldType;
            row["DataTypeName"] = reader.GetDataTypeName(ordinal);
            row[SchemaTableColumn.AllowDBNull] = true;
            row[SchemaTableColumn.IsAliased] = false;
            row[SchemaTableColumn.IsExpression] = false;
            row[SchemaTableOptionalColumn.IsAutoIncrement] = false;
            row[SchemaTableOptionalColumn.IsHidden] = false;
            row[SchemaTableOptionalColumn.IsReadOnly] = false;
            row[SchemaTableOptionalColumn.IsRowVersion] = false;
            row[SchemaTableColumn.IsLong] = fieldType == typeof(byte[]);
            var providerType = (int)GetDbType(fieldType);
            row[SchemaTableColumn.ProviderType] = providerType;
            row[SchemaTableColumn.NonVersionedProviderType] = providerType;
            row[SchemaTableOptionalColumn.ProviderSpecificDataType] = fieldType;
            schema.Rows.Add(row);
        }

        return schema;
    }

    public static bool TryGetClrTypeFromDeclaredType(string? declaredType, out Type fieldType)
    {
        fieldType = typeof(object);
        if (string.IsNullOrWhiteSpace(declaredType))
            return false;

        var normalized = declaredType.Trim().ToUpperInvariant();
        if (normalized.Contains("INT", StringComparison.Ordinal))
            fieldType = typeof(long);
        else if (normalized.Contains("REAL", StringComparison.Ordinal)
                 || normalized.Contains("FLOA", StringComparison.Ordinal)
                 || normalized.Contains("DOUB", StringComparison.Ordinal))
            fieldType = typeof(double);
        else if (normalized.Contains("TEXT", StringComparison.Ordinal)
                 || normalized.Contains("CHAR", StringComparison.Ordinal)
                 || normalized.Contains("CLOB", StringComparison.Ordinal))
            fieldType = typeof(string);
        else if (normalized.Contains("BLOB", StringComparison.Ordinal))
            fieldType = typeof(byte[]);
        else
            return false;

        return true;
    }

    public static Guid ToGuid(object value)
    {
        return value switch
        {
            Guid guid => guid,
            string text => Guid.Parse(text),
            byte[] bytes when bytes.Length == 16 => new Guid(bytes),
            byte[] bytes => Guid.Parse(Encoding.UTF8.GetString(bytes)),
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} value to Guid.")
        };
    }

    private static DbType GetDbType(Type fieldType)
    {
        if (fieldType == typeof(long))
            return DbType.Int64;
        if (fieldType == typeof(double))
            return DbType.Double;
        if (fieldType == typeof(string))
            return DbType.String;
        if (fieldType == typeof(byte[]))
            return DbType.Binary;
        if (fieldType == typeof(Guid))
            return DbType.Guid;

        return DbType.Object;
    }
}
