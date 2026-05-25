using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public class TursoDatabaseHandle() : SafeHandle(IntPtr.Zero, true)
{
    private IntPtr _database;

    protected override bool ReleaseHandle()
    {
        if (handle != IntPtr.Zero)
        {
            _ = TursoInterop.ConnectionClose(handle, out var errorPtr);
            if (errorPtr != IntPtr.Zero)
                TursoInterop.FreeString(errorPtr);

            TursoInterop.ConnectionDeinit(handle);
        }

        if (_database != IntPtr.Zero)
            TursoInterop.DatabaseDeinit(_database);

        handle = IntPtr.Zero;
        _database = IntPtr.Zero;
        return true;
    }

    public void ThrowIfInvalid()
    {
        if (IsInvalid)
            throw new NullReferenceException("database is invalid");
    }

    public static TursoDatabaseHandle FromPtrs(IntPtr database, IntPtr connection)
    {
        var handle = new TursoDatabaseHandle();
        handle._database = database;
        handle.SetHandle(connection);
        return handle;
    }

    public override bool IsInvalid => handle == IntPtr.Zero || _database == IntPtr.Zero;
}
