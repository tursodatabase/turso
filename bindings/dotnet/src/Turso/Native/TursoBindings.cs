using System.Runtime.InteropServices;

namespace Turso.Native;

public static class TursoBindings
{
    private const string DllName = "turso_dotnet.dll";

    [DllImport(DllName, EntryPoint = "db_open", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr OpenDatabase(string path, out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "db_close", CallingConvention = CallingConvention.Cdecl)]
    public static extern void CloseDatabase(IntPtr db);

    [DllImport(DllName, EntryPoint = "free_string", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeString(IntPtr stringPtr);
    
    [DllImport(DllName, EntryPoint = "db_prepare_statement", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr PrepareStatement(DatabaseHandle db, string statementSql, out IntPtr errorPtr);
    
    [DllImport(DllName, EntryPoint = "free_statement", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeStatement(IntPtr statement);
    
    [DllImport(DllName, EntryPoint = "bind_parameter", CallingConvention = CallingConvention.Cdecl)]
    public static extern void BindParameter(StatementHandle statement, int index, IntPtr tursoValue);

    [DllImport(DllName, EntryPoint = "bind_named_parameter", CallingConvention = CallingConvention.Cdecl)]
    public static extern void BindNamedParameter(StatementHandle statement, string parameterName, IntPtr tursoValue);

    [DllImport(DllName, EntryPoint = "db_statement_execute_step", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static extern bool StatementExecuteStep(StatementHandle statement, out IntPtr errorPtr);
    
    [DllImport(DllName, EntryPoint = "db_statement_nchange", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowsAffected(StatementHandle statement);

    [DllImport(DllName, EntryPoint = "db_statement_get_value", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoNativeValue GetValueFromStatement(StatementHandle statement, int columnIndex);
    
    
    [DllImport(DllName, EntryPoint = "db_statement_num_columns", CallingConvention = CallingConvention.Cdecl)]
    public static extern int StatementNumColumns(StatementHandle statement);

    
    [DllImport(DllName, EntryPoint = "db_statement_column_name", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementColumnName(StatementHandle statement, int index);

    
    [DllImport(DllName, EntryPoint = "db_statement_has_rows", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static extern bool StatementHasRows(StatementHandle statement);

}