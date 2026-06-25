using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace Turso;

internal sealed class TursoRemoteDataReader : DbDataReader
{
    private readonly TursoConnection? _connection;
    private readonly RemoteStatementResult _result;
    private readonly CommandBehavior _behavior;
    private readonly int _recordsAffected;
    private int _rowIndex = -1;
    private bool _isClosed;

    public TursoRemoteDataReader(TursoCommand command, RemoteStatementResult result, CommandBehavior behavior)
        : this(command.Connection as TursoConnection, result, behavior)
    {
    }

    private TursoRemoteDataReader(TursoConnection? connection, RemoteStatementResult result, CommandBehavior behavior)
    {
        ArgumentNullException.ThrowIfNull(result);

        _connection = connection;
        _result = result;
        _behavior = behavior;
        _recordsAffected = checked((int)result.AffectedRowCount);
    }

    public override bool GetBoolean(int ordinal)
    {
        return CurrentValue(ordinal).GetInt64() != 0;
    }

    public override byte GetByte(int ordinal)
    {
        return checked((byte)CurrentValue(ordinal).GetInt64());
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        return CopyArray((byte[])CurrentValue(ordinal).ToClrValue(), dataOffset, buffer, bufferOffset, length);
    }

    public override char GetChar(int ordinal)
    {
        var value = CurrentValue(ordinal);
        if (value.Type == "text")
        {
            var text = (string)value.ToClrValue();
            if (text.Length == 1)
                return text[0];
        }

        return checked((char)value.GetInt64());
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        return CopyArray(GetString(ordinal).ToCharArray(), dataOffset, buffer, bufferOffset, length);
    }

    public override string GetDataTypeName(int ordinal)
    {
        var value = HasCurrentRow ? CurrentValue(ordinal) : null;
        if (value is not null)
            return GetTypeName(value.Type);

        return ordinal >= 0 && ordinal < CurrentResult.Columns.Count
            ? CurrentResult.Columns[ordinal].DeclType ?? string.Empty
            : string.Empty;
    }

    public override DateTime GetDateTime(int ordinal)
    {
        var value = CurrentValue(ordinal);
        return value.Type == "text"
            ? DateTime.Parse((string)value.ToClrValue(), CultureInfo.InvariantCulture)
            : throw new InvalidCastException($"Cannot convert remote {value.Type} value to DateTime.");
    }

    public override decimal GetDecimal(int ordinal)
    {
        return CurrentValue(ordinal).GetDecimal();
    }

    public override double GetDouble(int ordinal)
    {
        return CurrentValue(ordinal).GetDouble();
    }

    public override Type GetFieldType(int ordinal)
    {
        EnsureOpen();
        ValidateOrdinal(ordinal);

        if (HasCurrentRow)
            return GetClrType(CurrentResult.Rows[_rowIndex][ordinal].Type);

        if (ordinal < CurrentResult.Columns.Count
            && TryGetClrTypeFromDeclaredType(CurrentResult.Columns[ordinal].DeclType, out var declaredType))
        {
            return declaredType;
        }

        return CurrentResult.Rows.Count > 0 && ordinal < CurrentResult.Rows[0].Count
            ? GetClrType(CurrentResult.Rows[0][ordinal].Type)
            : typeof(object);
    }

    public override float GetFloat(int ordinal)
    {
        return (float)CurrentValue(ordinal).GetDouble();
    }

    public override Guid GetGuid(int ordinal)
    {
        return Guid.Parse(GetString(ordinal));
    }

    public override short GetInt16(int ordinal)
    {
        return checked((short)CurrentValue(ordinal).GetInt64());
    }

    public override int GetInt32(int ordinal)
    {
        return checked((int)CurrentValue(ordinal).GetInt64());
    }

    public override long GetInt64(int ordinal)
    {
        return CurrentValue(ordinal).GetInt64();
    }

    public override string GetName(int ordinal)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ordinal);
        return ordinal < CurrentResult.Columns.Count
            ? CurrentResult.Columns[ordinal].Name ?? string.Empty
            : string.Empty;
    }

    public override int GetOrdinal(string name)
    {
        for (var i = 0; i < FieldCount; i++)
        {
            if (GetName(i) == name)
                return i;
        }

        throw new IndexOutOfRangeException($"column {name} not found");
    }

    public override string GetString(int ordinal)
    {
        var value = CurrentValue(ordinal).ToClrValue();
        return value switch
        {
            string text => text,
            DBNull => throw new InvalidCastException("Cannot convert remote null value to String."),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };
    }

    public override object GetValue(int ordinal)
    {
        return CurrentValue(ordinal).ToClrValue();
    }

    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);

        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);

        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        return CurrentValue(ordinal).Type == "null";
    }

    public override int FieldCount => CurrentResult.Columns.Count > 0
        ? CurrentResult.Columns.Count
        : CurrentResult.Rows.Count > 0
            ? CurrentResult.Rows[0].Count
            : 0;

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int RecordsAffected => _recordsAffected;

    public override bool HasRows => CurrentResult.Rows.Count > 0;

    public override bool IsClosed => _isClosed;

    public override bool NextResult()
    {
        EnsureOpen();
        _rowIndex = CurrentResult.Rows.Count;
        return false;
    }

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        return cancellationToken.IsCancellationRequested
            ? Task.FromCanceled<bool>(cancellationToken)
            : Task.FromResult(NextResult());
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_isClosed && (_behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
            _connection?.Close();

        _isClosed = true;
        base.Dispose(disposing);
    }

    public override bool Read()
    {
        EnsureOpen();
        if (_rowIndex + 1 >= CurrentResult.Rows.Count)
        {
            _rowIndex = CurrentResult.Rows.Count;
            return false;
        }

        _rowIndex++;
        return true;
    }

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        return cancellationToken.IsCancellationRequested
            ? Task.FromCanceled<bool>(cancellationToken)
            : Task.FromResult(Read());
    }

    public override int Depth => 0;

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, closeReader: false);
    }

    private RemoteStatementResult CurrentResult => _result;

    private bool HasCurrentRow => _rowIndex >= 0 && _rowIndex < CurrentResult.Rows.Count;

    private RemoteResponseValue CurrentValue(int ordinal)
    {
        EnsureOpen();
        if (!HasCurrentRow)
            throw new InvalidOperationException("No current row. Call Read before accessing values.");

        ValidateOrdinal(ordinal);
        var row = CurrentResult.Rows[_rowIndex];
        if (ordinal >= row.Count)
            throw new IndexOutOfRangeException($"column ordinal {ordinal} is out of range");

        return row[ordinal];
    }

    private static long CopyArray<T>(T[] source, long dataOffset, T[]? buffer, int bufferOffset, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(dataOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(bufferOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        if (dataOffset >= source.LongLength)
            return 0;

        var available = source.LongLength - dataOffset;

        if (buffer is null)
            return available;

        if (bufferOffset >= buffer.Length)
            return 0;

        var count = checked((int)Math.Min(Math.Min(available, length), buffer.Length - bufferOffset));
        if (count <= 0)
            return 0;

        Array.Copy(source, checked((int)dataOffset), buffer, bufferOffset, count);

        return count;
    }

    private static string GetTypeName(string valueType)
    {
        return valueType switch
        {
            "null" => "NULL",
            "integer" => "INTEGER",
            "float" => "REAL",
            "text" => "TEXT",
            "blob" => "BLOB",
            _ => throw new ArgumentOutOfRangeException(nameof(valueType), valueType, null),
        };
    }

    private void ValidateOrdinal(int ordinal)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ordinal);
        if (ordinal >= FieldCount)
            throw new IndexOutOfRangeException($"column ordinal {ordinal} is out of range");
    }

    private static Type GetClrType(string valueType)
    {
        return valueType switch
        {
            "integer" => typeof(long),
            "float" => typeof(double),
            "text" => typeof(string),
            "blob" => typeof(byte[]),
            "null" => typeof(DBNull),
            _ => typeof(object),
        };
    }

    private static bool TryGetClrTypeFromDeclaredType(string? declaredType, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Type? clrType)
    {
        clrType = null;
        if (string.IsNullOrWhiteSpace(declaredType))
            return false;

        var normalized = declaredType.Trim().ToUpperInvariant();
        if (normalized.Contains("INT", StringComparison.Ordinal))
            clrType = typeof(long);
        else if (normalized.Contains("REAL", StringComparison.Ordinal)
                 || normalized.Contains("FLOA", StringComparison.Ordinal)
                 || normalized.Contains("DOUB", StringComparison.Ordinal))
            clrType = typeof(double);
        else if (normalized.Contains("TEXT", StringComparison.Ordinal)
                 || normalized.Contains("CHAR", StringComparison.Ordinal)
                 || normalized.Contains("CLOB", StringComparison.Ordinal))
            clrType = typeof(string);
        else if (normalized.Contains("BLOB", StringComparison.Ordinal))
            clrType = typeof(byte[]);

        return clrType is not null;
    }

    private void EnsureOpen()
    {
        if (IsClosed)
            throw new InvalidOperationException("The data reader is closed.");
    }
}
