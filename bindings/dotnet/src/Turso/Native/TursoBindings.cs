using System.Runtime.InteropServices;

namespace Turso.Native;

public static class TursoBindings
{
    private const string DllName = "turso_dotnet.dll";

    [DllImport(DllName, EntryPoint = "db_open", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr OpenDatabase(string path, out IntPtr db);

    [DllImport(DllName, EntryPoint = "db_close", CallingConvention = CallingConvention.Cdecl)]
    public static extern void CloseDatabase(IntPtr db);

    [DllImport(DllName, EntryPoint = "free_error", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeError(IntPtr errorPtr);
    
    [DllImport(DllName, EntryPoint = "db_prepare_statement", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr PrepareStatement(DatabaseHandle db, string statementSql, out IntPtr statement);
    
    [DllImport(DllName, EntryPoint = "free_statement", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeStatement(IntPtr statement);
    
    [DllImport(DllName, EntryPoint = "bind_parameter", CallingConvention = CallingConvention.Cdecl)]
    public static extern void BindParameter(StatementHandle statement, int index, IntPtr tursoValue);

    [DllImport(DllName, EntryPoint = "bind_named_parameter", CallingConvention = CallingConvention.Cdecl)]
    public static extern void BindNamedParameter(StatementHandle statement, string parameterName, IntPtr tursoValue);

    [DllImport(DllName, EntryPoint = "db_statement_execute_step", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementExecuteStep(StatementHandle statement, [MarshalAs(UnmanagedType.I1)] out bool hasData);
    
    [DllImport(DllName, EntryPoint = "db_statement_nchange", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowsAffected(StatementHandle statement);

    [DllImport(DllName, EntryPoint = "db_statement_get_value", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoNativeValue GetValueFromStatement(StatementHandle statement, int columnIndex);
}