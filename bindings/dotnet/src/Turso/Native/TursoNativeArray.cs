using System.Runtime.InteropServices;

namespace Turso.Native;

[StructLayout(LayoutKind.Explicit)]
public struct TursoNativeArray
{
    [FieldOffset(0)]
    public IntPtr Data;
    
    [FieldOffset(8)]
    public UInt64 Length;
}