using System.Runtime.InteropServices;

namespace Turso.Native;

public class DatabaseHandle() : SafeHandle(IntPtr.Zero, true)
{
    protected override bool ReleaseHandle()
    {
        TursoBindings.CloseDatabase(handle);
        return true;
    }

    public static DatabaseHandle FromPtr(IntPtr ptr)
    {
        var handle = new DatabaseHandle();
        handle.SetHandle(ptr);
        return handle;
    }

    public override bool IsInvalid => handle == IntPtr.Zero;
}