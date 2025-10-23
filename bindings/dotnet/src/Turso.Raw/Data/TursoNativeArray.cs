using System.Runtime.InteropServices;

namespace Turso.Raw.Data;

[StructLayout(LayoutKind.Sequential)]
internal struct TursoNativeArray
{
    public IntPtr Data;
    public UInt64 Length;
}