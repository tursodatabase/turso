using System.Reflection;
using System.Runtime.InteropServices;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso.Raw;

internal enum TursoStatusCode : uint
{
    Ok = 0,
    Done = 1,
    Row = 2,
    Io = 3,
    Busy = 4,
    Interrupt = 5,
    BusySnapshot = 6,
    Error = 127,
    Misuse = 128,
    Constraint = 129,
    Readonly = 130,
    DatabaseFull = 131,
    NotADatabase = 132,
    Corrupt = 133,
    IoError = 134,
}

internal enum TursoNativeValueType : uint
{
    Unknown = 0,
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
}

[Flags]
internal enum TursoDatabaseOpenFlags : uint
{
    None = 0,
    ReadOnly = 1,
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoDatabaseConfig
{
    public ulong AsyncIo;
    public IntPtr Path;
    public IntPtr ExperimentalFeatures;
    public IntPtr Vfs;
    public IntPtr EncryptionCipher;
    public IntPtr EncryptionHexKey;
    public IntPtr PageCodec;
    public TursoDatabaseOpenFlags OpenFlags;
}

internal static class TursoInterop
{
    private const string DllName = "turso_sdk_kit";

    static TursoInterop()
    {
        NativeLibrary.SetDllImportResolver(typeof(TursoInterop).Assembly, ResolveNativeLibrary);
    }

    private static IntPtr ResolveNativeLibrary(
        string libraryName,
        Assembly assembly,
        DllImportSearchPath? searchPath)
    {
        if (!string.Equals(libraryName, DllName, StringComparison.Ordinal))
        {
            return IntPtr.Zero;
        }

        if (OperatingSystem.IsIOS())
        {
            return NativeLibrary.Load($"Frameworks/lib{libraryName}.framework/lib{libraryName}", assembly, searchPath);
        }

        var rid = GetRuntimeIdentifier();
        if (rid is null)
        {
            return IntPtr.Zero;
        }

        var libraryPath = Path.Combine(
            AppContext.BaseDirectory,
            "runtimes",
            rid,
            "native",
            GetNativeLibraryFileName());

        return File.Exists(libraryPath) ? NativeLibrary.Load(libraryPath) : IntPtr.Zero;
    }

    private static string? GetRuntimeIdentifier()
    {
        var architecture = RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X64 => "x64",
            Architecture.Arm64 => "arm64",
            _ => null,
        };

        if (architecture is null)
        {
            return null;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return $"win-{architecture}";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return $"linux-{architecture}";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return $"osx-{architecture}";
        }

        return null;
    }

    private static string GetNativeLibraryFileName()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return "turso_sdk_kit.dll";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return "libturso_sdk_kit.dylib";
        }

        return "libturso_sdk_kit.so";
    }

    [DllImport(DllName, EntryPoint = "turso_database_new", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode DatabaseNew(
        ref TursoDatabaseConfig config,
        out IntPtr database,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_database_open", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode DatabaseOpen(IntPtr database, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_database_connect", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode DatabaseConnect(
        IntPtr database,
        out IntPtr connection,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_database_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void DatabaseDeinit(IntPtr database);

    [DllImport(DllName, EntryPoint = "turso_connection_close", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode ConnectionClose(IntPtr connection, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void ConnectionDeinit(IntPtr connection);

    [DllImport(DllName, EntryPoint = "turso_connection_register_scalar_function", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode RegisterScalarFunction(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        int argc,
        [MarshalAs(UnmanagedType.I1)] bool deterministic,
        IntPtr context,
        TursoScalarFunctionCallback callback,
        TursoContextDestructorCallback contextDestructor,
        TursoValueDestructorCallback valueDestructor,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_register_aggregate_function", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode RegisterAggregateFunction(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        int argc,
        IntPtr context,
        TursoAggregateInitCallback init,
        TursoAggregateStepCallback step,
        TursoAggregateFinalCallback finalize,
        TursoContextDestructorCallback contextDestructor,
        TursoContextDestructorCallback aggregateDestructor,
        TursoValueDestructorCallback valueDestructor,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_unregister_function", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode UnregisterFunction(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_register_collation", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode RegisterCollation(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        IntPtr context,
        TursoCollationCallback callback,
        TursoContextDestructorCallback contextDestructor,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_unregister_collation", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode UnregisterCollation(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_enable_load_extension", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode EnableLoadExtension(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.I1)] bool enabled,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_load_extension", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode LoadExtension(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_connection_prepare_single", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode ConnectionPrepareSingle(
        TursoDatabaseHandle connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        out IntPtr statement,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_statement_finalize", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementFinalize(IntPtr statement, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_statement_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void StatementDeinit(IntPtr statement);

    [DllImport(DllName, EntryPoint = "turso_statement_step", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementStep(TursoStatementHandle statement, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_statement_run_io", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementRunIo(TursoStatementHandle statement, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_statement_n_change", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowsAffected(TursoStatementHandle statement);

    [DllImport(DllName, EntryPoint = "turso_statement_column_count", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementColumnCount(TursoStatementHandle statement);

    [DllImport(DllName, EntryPoint = "turso_statement_column_name", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementColumnName(TursoStatementHandle statement, UIntPtr index);

    [DllImport(DllName, EntryPoint = "turso_statement_row_value_kind", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoNativeValueType StatementRowValueKind(TursoStatementHandle statement, UIntPtr index);

    [DllImport(DllName, EntryPoint = "turso_statement_row_value_bytes_count", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowValueBytesCount(TursoStatementHandle statement, UIntPtr index);

    [DllImport(DllName, EntryPoint = "turso_statement_row_value_bytes_ptr", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementRowValueBytesPtr(TursoStatementHandle statement, UIntPtr index);

    [DllImport(DllName, EntryPoint = "turso_statement_row_value_int", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowValueInt(TursoStatementHandle statement, UIntPtr index);

    [DllImport(DllName, EntryPoint = "turso_statement_row_value_double", CallingConvention = CallingConvention.Cdecl)]
    public static extern double StatementRowValueDouble(TursoStatementHandle statement, UIntPtr index);

    [DllImport(DllName, EntryPoint = "turso_statement_named_position", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementNamedPosition(
        TursoStatementHandle statement,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(DllName, EntryPoint = "turso_statement_parameters_count", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementParameterCount(TursoStatementHandle statement);

    [DllImport(DllName, EntryPoint = "turso_statement_parameter_name", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementParameterName(TursoStatementHandle statement, long index);

    [DllImport(DllName, EntryPoint = "turso_statement_bind_positional_null", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementBindNull(TursoStatementHandle statement, UIntPtr position);

    [DllImport(DllName, EntryPoint = "turso_statement_bind_positional_int", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementBindInt(TursoStatementHandle statement, UIntPtr position, long value);

    [DllImport(DllName, EntryPoint = "turso_statement_bind_positional_double", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementBindDouble(TursoStatementHandle statement, UIntPtr position, double value);

    [DllImport(DllName, EntryPoint = "turso_statement_bind_positional_text", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementBindText(
        TursoStatementHandle statement,
        UIntPtr position,
        IntPtr ptr,
        UIntPtr len);

    [DllImport(DllName, EntryPoint = "turso_statement_bind_positional_blob", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatusCode StatementBindBlob(
        TursoStatementHandle statement,
        UIntPtr position,
        IntPtr ptr,
        UIntPtr len);

    [DllImport(DllName, EntryPoint = "turso_str_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeString(IntPtr stringPtr);
}
