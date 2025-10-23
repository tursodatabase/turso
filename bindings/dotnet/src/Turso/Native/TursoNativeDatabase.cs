using System.Runtime.InteropServices;

namespace Turso.Native;

public class TursoNativeDatabase : IDisposable
{
    private readonly DatabaseHandle _databaseHandle;
    private int _isDisposed = 0;

    public TursoNativeDatabase(string path)
    {
        var dbPtr = TursoBindings.OpenDatabase(path, out var errorPtr);
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
        var statementPtr = TursoBindings.PrepareStatement(_databaseHandle, sql, out var errorPtr);
        if (errorPtr != IntPtr.Zero)
            TursoHelpers.ThrowException(errorPtr);

        var statementHandle = StatementHandle.FromPtr(statementPtr);
        return new TursoNativeStatement(statementHandle);
    }

    private void Dispose(bool disposing)
    {
        _databaseHandle.Dispose();
    }
}