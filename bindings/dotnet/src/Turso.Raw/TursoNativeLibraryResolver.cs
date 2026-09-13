using System.Reflection;
using System.Runtime.InteropServices;

namespace Turso.Raw;

internal static class TursoNativeLibraryResolver
{
    private static readonly object s_lock = new();
    private static bool s_initialized;

    internal static void EnsureInitialized()
    {
        if (!OperatingSystem.IsIOS())
            return;

        lock (s_lock)
        {
            if (s_initialized)
                return;

            NativeLibrary.SetDllImportResolver(typeof(TursoNativeLibraryResolver).Assembly, Resolve);
            s_initialized = true;
        }
    }

    private static IntPtr Resolve(
        string libraryName,
        Assembly assembly,
        DllImportSearchPath? searchPath)
    {
        return libraryName == TursoSyncInterop.DllName
            ? NativeLibrary.Load(
                $"Frameworks/lib{libraryName}.framework/lib{libraryName}",
                assembly,
                searchPath)
            : IntPtr.Zero;
    }
}
