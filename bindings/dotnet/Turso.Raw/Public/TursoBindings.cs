using System.Runtime.InteropServices;
using System.Text;
using Turso.Raw.Data;
using Turso.Raw.Public.Handles;
using Turso.Raw.Public.Value;

namespace Turso.Raw.Public;

public static class TursoBindings
{
    public static TursoDatabaseHandle OpenDatabase(string path)
    {
        var dbPtr = TursoInterop.OpenDatabase(path, out var errorPtr);
        if (errorPtr != IntPtr.Zero)
            ThrowException(errorPtr);

        return TursoDatabaseHandle.FromPtr(dbPtr);
    }

    public static TursoStatementHandle PrepareStatement(TursoDatabaseHandle db, string sql)
    {
        var statementPtr = TursoInterop.PrepareStatement(db, sql, out var errorPtr);
        if (errorPtr != IntPtr.Zero)
            ThrowException(errorPtr);

        return TursoStatementHandle.FromPtr(statementPtr);
    }

    public static void BindParameter(TursoStatementHandle statement, int index, TursoValue parameter)
    {
        var nativeValue = FromValue(parameter, out var handle);
        try
        {
            unsafe
            {
                var ptr = &nativeValue;
                TursoInterop.BindParameter(statement, index, (IntPtr)ptr);
            }
        }
        finally
        {
            if (handle.HasValue)
                handle.Value.Free();
        }
    }

    public static void BindNamedParameter(TursoStatementHandle statement, string name, TursoValue parameter)
    {
        var nativeValue = FromValue(parameter, out var handle);
        try
        {
            unsafe
            {
                var ptr = &nativeValue;
                TursoInterop.BindNamedParameter(statement, name, (IntPtr)ptr);
            }
        }
        finally
        {
            if (handle.HasValue)
                handle.Value.Free();
        }
    }

    public static bool Read(TursoStatementHandle statement)
    {
        var hasData = TursoInterop.StatementExecuteStep(statement, out var errorPtr);
        if (errorPtr != IntPtr.Zero)
            ThrowException(errorPtr);
        return hasData;
    }

    public static TursoValue GetValue(TursoStatementHandle statement, int columnIndex)
    {
        if (columnIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(columnIndex));

        var rowValue = TursoInterop.GetValueFromStatement(statement, columnIndex);
        return rowValue.ValueType switch
        {
            TursoValueType.Empty => TursoValue.Empty(),
            TursoValueType.Null => TursoValue.Null(),
            TursoValueType.Integer => TursoValue.Int(rowValue.RowValueUnion.IntValue),
            TursoValueType.Real => TursoValue.Real(rowValue.RowValueUnion.RealValue),
            TursoValueType.Text => TursoValue.String(
                Encoding.UTF8.GetString(ToArray(rowValue.RowValueUnion.StringValue))),
            TursoValueType.Blob => TursoValue.Blob(ToArray(rowValue.RowValueUnion.BlobValue)),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public static string GetName(TursoStatementHandle statement, int ordinal)
    {
        if (ordinal < 0)
            throw new ArgumentOutOfRangeException(nameof(ordinal));

        var cname = TursoInterop.StatementColumnName(statement, ordinal);
        try
        {
            return Marshal.PtrToStringUTF8(cname) ?? "";
        }
        finally
        {
            TursoInterop.FreeString(cname);
        }
    }

    public static int GetFieldCount(TursoStatementHandle statement)
    {
        return TursoInterop.StatementNumColumns(statement);
    }

    public static int RowsAffected(TursoStatementHandle statement)
    {
        return (int)TursoInterop.StatementRowsAffected(statement);
    }


    public static bool HasRows(TursoStatementHandle statement)
    {
        return TursoInterop.StatementHasRows(statement);
    }


    private static TursoNativeValue FromValue(TursoValue value, out GCHandle? handle)
    {
        handle = null;
        var union = new TursoNativeRowValueUnion();
        if (value.ValueType == TursoValueType.Integer)
            union.IntValue = value.IntValue;
        if (value.ValueType == TursoValueType.Real)
            union.RealValue = value.RealValue;
        if (value.ValueType == TursoValueType.Text)
        {
            var bytes = Encoding.UTF8.GetBytes(value.StringValue);
            handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
            union.StringValue = new TursoNativeArray
                { Data = handle.Value.AddrOfPinnedObject(), Length = (ulong)bytes.Length };
        }

        if (value.ValueType == TursoValueType.Blob)
        {
            handle = GCHandle.Alloc(value.BlobValue, GCHandleType.Pinned);
            union.BlobValue = new TursoNativeArray
                { Data = handle.Value.AddrOfPinnedObject(), Length = (ulong)value.BlobValue.Length };
        }

        return new TursoNativeValue
        {
            ValueType = value.ValueType,
            RowValueUnion = union,
        };
    }

    private static byte[] ToArray(TursoNativeArray array)
    {
        unsafe
        {
            var data = new Span<byte>((void*)array.Data, (int)array.Length);
            return data.ToArray();
        }
    }

    private static void ThrowException(IntPtr errorPtr)
    {
        var errorMessage = Marshal.PtrToStringUTF8(errorPtr);
        var exception = new TursoException(errorMessage ?? "Internal error");
        TursoInterop.FreeString(errorPtr);
        throw exception;
    }
}