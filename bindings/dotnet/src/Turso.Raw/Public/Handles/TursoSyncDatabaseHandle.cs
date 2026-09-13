using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public sealed class TursoSyncDatabaseHandle() : SafeHandle(IntPtr.Zero, ownsHandle: true)
{
    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        TursoSyncInterop.DatabaseDeinit(handle);
        handle = IntPtr.Zero;
        return true;
    }

    internal static TursoSyncDatabaseHandle FromPtr(IntPtr pointer)
    {
        var result = new TursoSyncDatabaseHandle();
        result.SetHandle(pointer);
        return result;
    }
}
