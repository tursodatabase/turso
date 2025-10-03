using System.Runtime.InteropServices;

namespace Turso.Native;

[StructLayout(LayoutKind.Explicit)]
public ref struct TursoErrorHandle
{
    [FieldOffset(0)]
    public IntPtr ErrorPtr;

    public unsafe IntPtr Ptr()
    {
        fixed(TursoErrorHandle* errorHandlePtr = &this)
            return (IntPtr)errorHandlePtr;
    }
}