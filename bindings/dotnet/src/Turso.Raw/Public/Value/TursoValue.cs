namespace Turso.Raw.Public.Value;

public struct TursoValue
{
    public TursoValueType ValueType;
    public long IntValue;
    public double RealValue;
    public string StringValue;
    public byte[] BlobValue;

    public static TursoValue Empty() => new() { ValueType = TursoValueType.Empty };
    public static TursoValue Null() => new() { ValueType = TursoValueType.Null };
    public static TursoValue Int(Int64 value) => new() { ValueType = TursoValueType.Integer, IntValue = value };
    public static TursoValue Real(Double value) => new() { ValueType = TursoValueType.Real, RealValue = value };
    public static TursoValue String(string value) => new() { ValueType = TursoValueType.Text, StringValue = value };
    public static TursoValue Blob(byte[] value) => new() { ValueType = TursoValueType.Blob, BlobValue = value };
}