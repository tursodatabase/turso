using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public class TursoStatementHandle() : SafeHandle(IntPtr.Zero, true)
{
    protected override bool ReleaseHandle()
    {
        TursoInterop.FreeStatement(handle);
        return true;
    }

    public static TursoStatementHandle FromPtr(IntPtr ptr)
    {
        var handle = new TursoStatementHandle();
        handle.SetHandle(ptr);
        return handle;
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

}