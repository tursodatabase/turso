using System.Collections;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Turso.Native;
using Turso.RowValue;

namespace Turso;

public class TursoDataReader : DbDataReader
{
    private readonly TursoCommand _command;
    private readonly TursoNativeStatement _statement;

    public TursoDataReader(TursoCommand command, TursoNativeStatement statement)
    {
        _command = command;
        _statement = statement;
    }

    public override bool GetBoolean(int ordinal)
    {
        return _statement.GetRow(ordinal).IntValue != 0;
    }

    public override byte GetByte(int ordinal)
    {
        return (byte)_statement.GetRow(ordinal).IntValue;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        return GetArray(ordinal, dataOffset, buffer, bufferOffset, length);
    }

    public override char GetChar(int ordinal)
    {
        var row = _statement.GetRow(ordinal);
        if (row.ValueType == TursoValueType.Text && row.StringValue.Length == 1)
        {
            return row.StringValue[0];
        }

        return (char)_statement.GetRow(ordinal).IntValue;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        return GetArray(ordinal, dataOffset, buffer, bufferOffset, length);
    }

    public override string GetDataTypeName(int ordinal)
    {
        var row = _statement.GetRow(ordinal);
        return row.ValueType.GetTypeName();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override decimal GetDecimal(int ordinal)
    {
        return (decimal)_statement.GetRow(ordinal).RealValue;
    }

    public override double GetDouble(int ordinal)
    {
        return _statement.GetRow(ordinal).RealValue;
    }

    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override float GetFloat(int ordinal)
    {
        return (float)_statement.GetRow(ordinal).RealValue;
    }

    public override Guid GetGuid(int ordinal)
    {
        return Guid.Parse(_statement.GetRow(ordinal).StringValue);
    }

    public override short GetInt16(int ordinal)
    {
        return (short)_statement.GetRow(ordinal).IntValue;
    }

    public override int GetInt32(int ordinal)
    {
        return (int)_statement.GetRow(ordinal).IntValue;
    }

    public override long GetInt64(int ordinal)
    {
        return _statement.GetRow(ordinal).IntValue;
    }

    public override string GetName(int ordinal)
    {
        return _statement.GetName(ordinal);
    }

    public override int GetOrdinal(string name)
    {
        return _statement.GetOrdinal(name);
    }

    public override string GetString(int ordinal)
    {
        return _statement.GetRow(ordinal).StringValue;
    }

    public override object? GetValue(int ordinal)
    {
        var row = _statement.GetRow(ordinal);
        return row.ValueType switch
        {
            TursoValueType.Null or TursoValueType.Empty => null,
            TursoValueType.Integer => row.IntValue,
            TursoValueType.Real => row.RealValue,
            TursoValueType.Text => row.StringValue,
            TursoValueType.Blob => row.BlobValue,
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
        throw new NotImplementedException();
    }

    public override int FieldCount => _statement.GetFieldCount();

    public override object this[int ordinal] => GetValue(ordinal)!;

    public override object this[string name]
    {
        get
        {
            var ordinal = GetOrdinal(name);
            return GetValue(ordinal)!;
        }
    }

    public override int RecordsAffected => _statement.RowsAffected();
    public override bool HasRows => _statement.HasRows();
    public override bool IsClosed => _statement.IsClosed();

    public override bool NextResult()
    {
        while (_statement.Read()) ;
        return true;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _command.Dispose();
    }

    public override bool Read()
    {
        return _statement.Read();
    }

    public override int Depth => 0;

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, closeReader: false);
    }

    private long GetArray<T>(int ordinal, long dataOffset, T[]? buffer, int bufferOffset, int length)
        where T : struct
    {
        var bytes = _statement.GetRow(ordinal).BlobValue;
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
}