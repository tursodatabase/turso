using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public class TursoDatabaseHandle() : SafeHandle(IntPtr.Zero, true)
{
    protected override bool ReleaseHandle()
    {
        TursoInterop.CloseDatabase(handle);
        return true;
    }

    public static TursoDatabaseHandle FromPtr(IntPtr ptr)
    {
        var handle = new TursoDatabaseHandle();
        handle.SetHandle(ptr);
        return handle;
    }

    public override bool IsInvalid => handle == IntPtr.Zero;
}