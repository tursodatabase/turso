using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Turso.RowValue;

namespace Turso.Native;

[StructLayout(LayoutKind.Explicit)]
public struct TursoNativeValue
{
    [FieldOffset(0)] public TursoValueType ValueType;

    [FieldOffset(8)] public TursoNativeRowValueUnion RowValueUnion;
}