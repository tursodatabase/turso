using System.Runtime.InteropServices;

namespace Turso.Native;

public class TursoNativeDatabase : IDisposable
{
    private readonly DatabaseHandle _databaseHandle;
    private int _isDisposed = 0;

    public TursoNativeDatabase(string path)
    {
        var errorPtr = TursoBindings.OpenDatabase(path, out var dbPtr);
        if (errorPtr != IntPtr.Zero)
            TursoHelpers.ThrowException(errorPtr);

        _databaseHandle = DatabaseHandle.FromPtr(dbPtr);
    }

    ~TursoNativeDatabase() => Dispose(false);

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public TursoNativeStatement PrepareStatement(string sql)
    {
        var errorHandle = new TursoErrorHandle();
        var statementPtr = TursoBindings.PrepareStatement(_databaseHandle, sql, errorHandle.Ptr());
        if (errorHandle.ErrorPtr != IntPtr.Zero)
            TursoHelpers.ThrowException(errorHandle.ErrorPtr);
        return new TursoNativeStatement(statementPtr);
    }

    private void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) == 0)
        {
            TursoBindings.CloseDatabase(_databaseHandle);
        }
    }
}