using System.Runtime.InteropServices;

namespace Turso.Native;

public class TursoHelpers
{
    public static void ThrowException(IntPtr errorPtr)
    {
        var error = Marshal.PtrToStructure<TursoError>(errorPtr);
        var errorMessage = Marshal.PtrToStringUTF8(error.ErrorMessage);
        var exception = new TursoException(errorMessage ?? "Internal error");
        TursoBindings.FreeError(errorPtr);
        throw exception;
    }
}