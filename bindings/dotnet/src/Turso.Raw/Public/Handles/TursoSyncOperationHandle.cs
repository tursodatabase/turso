using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public sealed class TursoSyncOperationHandle() : SafeHandle(IntPtr.Zero, ownsHandle: true)
{
    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        TursoSyncInterop.OperationDeinit(handle);
        handle = IntPtr.Zero;
        return true;
    }

    internal static TursoSyncOperationHandle FromPtr(IntPtr pointer)
    {
        var result = new TursoSyncOperationHandle();
        result.SetHandle(pointer);
        return result;
    }
}
