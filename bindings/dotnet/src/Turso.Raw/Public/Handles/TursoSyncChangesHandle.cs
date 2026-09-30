using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public sealed class TursoSyncChangesHandle() : SafeHandle(IntPtr.Zero, ownsHandle: true)
{
    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        TursoSyncInterop.ChangesDeinit(handle);
        handle = IntPtr.Zero;
        return true;
    }

    internal IntPtr Consume()
    {
        if (IsInvalid || IsClosed)
            throw new ObjectDisposedException(nameof(TursoSyncChangesHandle));

        var pointer = handle;
        SetHandleAsInvalid();
        return pointer;
    }

    internal static TursoSyncChangesHandle FromPtr(IntPtr pointer)
    {
        var result = new TursoSyncChangesHandle();
        result.SetHandle(pointer);
        return result;
    }
}
