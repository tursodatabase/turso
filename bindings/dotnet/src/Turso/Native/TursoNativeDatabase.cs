using System.Runtime.InteropServices;

namespace Turso.Native;

public class TursoNativeDatabase : IDisposable
{
    private readonly IntPtr _databasePtr;
    private int _isDisposed = 0;

    public TursoNativeDatabase(string path)
    {
        var errorHandle = new TursoErrorHandle();
        var databasePtr = TursoBindings.OpenDatabase(path, errorHandle.Ptr());
        if (errorHandle.ErrorPtr != IntPtr.Zero)
            TursoHelpers.ThrowException(errorHandle.ErrorPtr);

        _databasePtr = databasePtr;
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
        var statementPtr = TursoBindings.PrepareStatement(_databasePtr, sql, errorHandle.Ptr());
        if (errorHandle.ErrorPtr != IntPtr.Zero)
            TursoHelpers.ThrowException(errorHandle.ErrorPtr);
        return new TursoNativeStatement(_databasePtr, statementPtr);
    }

    private void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) == 0)
        {
            TursoBindings.CloseDatabase(_databasePtr);
        }
    }
}