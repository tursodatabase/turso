using System.Runtime.InteropServices;

namespace Turso.Native;

public class StatementHandle() : SafeHandle(IntPtr.Zero, true)
{
    protected override bool ReleaseHandle()
    {
        TursoBindings.FreeStatement(handle);
        return true;
    }

    public static StatementHandle FromPtr(IntPtr ptr)
    {
        var handle = new StatementHandle();
        handle.SetHandle(ptr);
        return handle;
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

}