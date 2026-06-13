using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Turso.Raw.Public.Value;

namespace Turso;

public class TursoParameter : DbParameter
{
    private int _size;
    private static readonly Dictionary<Type, TursoValueType> TursoTypeMapping =
        new()
        {
            { typeof(bool), TursoValueType.Integer },
            { typeof(byte), TursoValueType.Integer },
            { typeof(byte[]), TursoValueType.Blob },
            { typeof(char), TursoValueType.Text },
            { typeof(DateTime), TursoValueType.Text },
            { typeof(DateTimeOffset), TursoValueType.Text },
            { typeof(DateOnly), TursoValueType.Text },
            { typeof(TimeOnly), TursoValueType.Text },
            { typeof(DBNull), TursoValueType.Null },
            { typeof(decimal), TursoValueType.Text },
            { typeof(double), TursoValueType.Real },
            { typeof(float), TursoValueType.Real },
            { typeof(Guid), TursoValueType.Text },
            { typeof(int), TursoValueType.Integer },
            { typeof(long), TursoValueType.Integer },
            { typeof(sbyte), TursoValueType.Integer },
            { typeof(short), TursoValueType.Integer },
            { typeof(string), TursoValueType.Text },
            { typeof(TimeSpan), TursoValueType.Text },
            { typeof(uint), TursoValueType.Integer },
            { typeof(ulong), TursoValueType.Integer },
            { typeof(ushort), TursoValueType.Integer }
        };

    public TursoParameter()
    {
    }

    public TursoParameter(object value)
    {
        Value = value;
    }

    public TursoParameter(string parameterName, object value)
    {
        ParameterName = parameterName;
        Value = value;
    }

    public TursoParameter(string parameterName, DbType dbType, object value)
    {
        ParameterName = parameterName;
        DbType = dbType;
        Value = value;
    }

    public override void ResetDbType()
    {
        DbType = DbType.String;
    }

    public override DbType DbType { get; set; } = DbType.String;

    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
            {
                throw new ArgumentException("Only input parameters are supported");
            }
        }
    }
    public override bool IsNullable { get; set; }
    [AllowNull]
    public override string ParameterName { get; set; } = "";

    [AllowNull]
    public override string SourceColumn { get; set; } = "";
    public override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }

    public TursoValue ToValue()
    {
        if (Value is null)
            return new TursoValue { ValueType = TursoValueType.Null };

        var valueType = Value.GetType();
        if (!TursoTypeMapping.TryGetValue(valueType, out var tursoValueType))
        {
            throw new ArgumentException($"Parameter type {valueType} is not supported");
        }

        return GetTursoValue(Value, tursoValueType);
    }

    public override int Size
    {
        get => _size;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, -1);
            _size = value;
        }
    }

    private static TursoValue GetTursoValue(object value, TursoValueType tursoValueType)
    {
        return tursoValueType switch
        {
            TursoValueType.Empty => new TursoValue() { ValueType = TursoValueType.Empty },
            TursoValueType.Null => new TursoValue() { ValueType = TursoValueType.Null },
            TursoValueType.Integer => new TursoValue() { ValueType = TursoValueType.Integer, IntValue = Convert.ToInt64(value) },
            TursoValueType.Real => new TursoValue() { ValueType = TursoValueType.Real, RealValue = Convert.ToDouble(value, CultureInfo.InvariantCulture) },
            TursoValueType.Text => new TursoValue() { ValueType = TursoValueType.Text, StringValue = ToInvariantString(value) },
            TursoValueType.Blob => new TursoValue() { ValueType = TursoValueType.Blob, BlobValue = (byte[])value },
            _ => throw new ArgumentOutOfRangeException(nameof(tursoValueType), tursoValueType, null)
        };
    }

    private static string ToInvariantString(object value)
    {
        return value switch
        {
            DateTime dateTime => dateTime.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture),
            DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", CultureInfo.InvariantCulture),
            DateOnly dateOnly => dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            TimeOnly timeOnly => timeOnly.ToString("HH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture),
            TimeSpan timeSpan => timeSpan.ToString("c", CultureInfo.InvariantCulture),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString()!
        };
    }
}
