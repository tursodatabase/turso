using System.Runtime.InteropServices;

namespace Turso.Native;

public class TursoHelpers
{
    public static void ThrowException(IntPtr errorPtr)
    {
        var errorMessage = Marshal.PtrToStringUTF8(errorPtr);
        var exception = new TursoException(errorMessage ?? "Internal error");
        TursoBindings.FreeString(errorPtr);
        throw exception;
    }
}