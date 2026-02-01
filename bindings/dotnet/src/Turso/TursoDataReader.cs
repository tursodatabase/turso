using System.Collections;
using System.ComponentModel;
using System.Data.Common;
using System.Globalization;
using System.Runtime.CompilerServices;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;
using Turso.Raw.Public.Value;

namespace Turso;

public class TursoDataReader : DbDataReader
{
    private readonly TursoCommand _command;
    private readonly TursoStatementHandle _statement;

    public TursoDataReader(TursoCommand command, TursoStatementHandle statement)
    {
        _command = command;
        _statement = statement;
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
        var value = TursoBindings.GetValue(_statement, ordinal);
        return value.ValueType switch
        {
            TursoValueType.Integer => typeof(long),
            TursoValueType.Real => typeof(double),
            TursoValueType.Text => typeof(string),
            TursoValueType.Blob => typeof(byte[]),
            _ => typeof(object)
        };
    }

    public override float GetFloat(int ordinal)
    {
        return (float)TursoBindings.GetValue(_statement, ordinal).RealValue;
    }

    public override Guid GetGuid(int ordinal)
    {
        return Guid.Parse(TursoBindings.GetValue(_statement, ordinal).StringValue);
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

    public override object? GetValue(int ordinal)
    {
        var value = TursoBindings.GetValue(_statement, ordinal);
        return value.ValueType switch
        {
            TursoValueType.Null or TursoValueType.Empty => null,
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
    public override bool IsClosed => _statement.IsInvalid;

    public override bool NextResult()
    {
        while (TursoBindings.Read(_statement)) ;
        return true;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _command.Dispose();
    }

    public override bool Read()
    {
        return TursoBindings.Read(_statement);
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
}