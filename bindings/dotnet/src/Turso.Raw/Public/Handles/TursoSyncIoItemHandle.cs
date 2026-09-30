using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public sealed class TursoSyncIoItemHandle() : SafeHandle(IntPtr.Zero, ownsHandle: true)
{
    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        TursoSyncInterop.IoItemDeinit(handle);
        handle = IntPtr.Zero;
        return true;
    }

    internal static TursoSyncIoItemHandle FromPtr(IntPtr pointer)
    {
        var result = new TursoSyncIoItemHandle();
        result.SetHandle(pointer);
        return result;
    }
}
