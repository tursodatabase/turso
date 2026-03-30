using System.Runtime.InteropServices;
using Turso.Raw.Data;
using Turso.Raw.Public.Handles;

namespace Turso.Raw;

internal static class TursoInterop
{
    private const string DllName = "turso_dotnet";
    
    [DllImport(DllName, EntryPoint = "db_open", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr OpenDatabase(string path, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "db_open_with_encryption", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr OpenDatabaseWithEncryption(string path, string? cipher, string? hexkey, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "db_close", CallingConvention = CallingConvention.Cdecl)]
    public static extern void CloseDatabase(IntPtr db);

    [DllImport(DllName, EntryPoint = "free_string", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeString(IntPtr stringPtr);
    
    [DllImport(DllName, EntryPoint = "db_prepare_statement", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr PrepareStatement(TursoDatabaseHandle db, string sql, out IntPtr errorPtr);
    
    [DllImport(DllName, EntryPoint = "free_statement", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeStatement(IntPtr statement);
    
    [DllImport(DllName, EntryPoint = "bind_parameter", CallingConvention = CallingConvention.Cdecl)]
    public static extern void BindParameter(TursoStatementHandle statement, int index, IntPtr tursoValue);

    [DllImport(DllName, EntryPoint = "bind_named_parameter", CallingConvention = CallingConvention.Cdecl)]
    public static extern void BindNamedParameter(TursoStatementHandle statement, string parameterName, IntPtr tursoValue);

    [DllImport(DllName, EntryPoint = "db_statement_execute_step", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static extern bool StatementExecuteStep(TursoStatementHandle statement, out IntPtr errorPtr);
    
    [DllImport(DllName, EntryPoint = "db_statement_nchange", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowsAffected(TursoStatementHandle statement);

    [DllImport(DllName, EntryPoint = "db_statement_get_value", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoNativeValue GetValueFromStatement(TursoStatementHandle statement, int columnIndex);
    
    [DllImport(DllName, EntryPoint = "db_statement_num_columns", CallingConvention = CallingConvention.Cdecl)]
    public static extern int StatementNumColumns(TursoStatementHandle statement);
    
    [DllImport(DllName, EntryPoint = "db_statement_column_name", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementColumnName(TursoStatementHandle statement, int index);
    
    [DllImport(DllName, EntryPoint = "db_statement_has_rows", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static extern bool StatementHasRows(TursoStatementHandle statement);

}