using System.Runtime.InteropServices;

namespace Turso.Raw.Public.Handles;

public class TursoDatabaseHandle() : SafeHandle(IntPtr.Zero, true)
{
    private IntPtr _database;
    private SafeHandle? _owner;
    private bool _ownerReferenceAdded;

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

        if (_ownerReferenceAdded)
        {
            _owner!.DangerousRelease();
            _ownerReferenceAdded = false;
        }

        handle = IntPtr.Zero;
        _database = IntPtr.Zero;
        _owner = null;
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

    public static TursoDatabaseHandle FromConnectionPtr(IntPtr connection, SafeHandle owner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        if (connection == IntPtr.Zero)
            throw new ArgumentException("Connection pointer must not be null.", nameof(connection));
        if (owner.IsInvalid || owner.IsClosed)
            throw new ObjectDisposedException(nameof(owner));

        var ownerReferenceAdded = false;
        owner.DangerousAddRef(ref ownerReferenceAdded);
        try
        {
            var result = new TursoDatabaseHandle
            {
                _owner = owner,
                _ownerReferenceAdded = ownerReferenceAdded,
            };
            result.SetHandle(connection);
            return result;
        }
        catch
        {
            if (ownerReferenceAdded)
                owner.DangerousRelease();
            throw;
        }
    }

    public override bool IsInvalid => handle == IntPtr.Zero || _database == IntPtr.Zero && _owner is null;
}
