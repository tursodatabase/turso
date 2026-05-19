using System.Runtime.InteropServices;
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

[StructLayout(LayoutKind.Sequential)]
internal struct TursoDatabaseConfig
{
    public ulong AsyncIo;
    public IntPtr Path;
    public IntPtr ExperimentalFeatures;
    public IntPtr Vfs;
    public IntPtr EncryptionCipher;
    public IntPtr EncryptionHexKey;
}

internal static class TursoInterop
{
    private const string DllName = "turso_sdk_kit";

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
