using System.Runtime.InteropServices;
using Turso.Raw.Public.Value;

namespace Turso.Raw.Data;

[StructLayout(LayoutKind.Explicit)]
internal ref struct TursoNativeValue
{
    [FieldOffset(0)] 
    public TursoValueType ValueType;

    [FieldOffset(8)] 
    public TursoNativeRowValueUnion RowValueUnion;
}