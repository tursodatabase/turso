using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

/// <summary>
/// An opened native database that any number of connections can be created from with
/// <see cref="TursoBindings.Connect(TursoSharedDatabaseHandle)"/>. Each connection keeps
/// the database alive, so disposing this handle only releases it once they are all closed.
/// </summary>
public sealed class TursoSharedDatabaseHandle() : SafeHandle(IntPtr.Zero, true)
{
    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        TursoInterop.DatabaseDeinit(handle);
        handle = IntPtr.Zero;
        return true;
    }

    internal static TursoSharedDatabaseHandle FromPtr(IntPtr database)
    {
        var result = new TursoSharedDatabaseHandle();
        result.SetHandle(database);
        return result;
    }
}
