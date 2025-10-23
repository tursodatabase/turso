using System.Runtime.InteropServices;

namespace Turso.Raw.Data;

[StructLayout(LayoutKind.Explicit)]
internal struct TursoNativeRowValueUnion
{
    [FieldOffset(0)]
    public Int64 IntValue;
    
    [FieldOffset(0)]
    public Double RealValue;
    
    [FieldOffset(0)]
    public TursoNativeArray StringValue;
    
    [FieldOffset(0)]
    public TursoNativeArray BlobValue;
}