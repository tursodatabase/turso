using System.Runtime.InteropServices;
using System.Text;
using Turso.RowValue;

namespace Turso.Native;

public class TursoNativeStatement : IDisposable
{
    private readonly IntPtr _dbPtr;
    private readonly IntPtr _statementPtr;
    private int _isDisposed = 0;

    public TursoNativeStatement(IntPtr dbPtr, IntPtr statementPtr)
    {
        _dbPtr = dbPtr;
        _statementPtr = statementPtr;
    }

    public void BindParameter(int index, TursoParameter parameter)
    {
        var value = parameter.ToValue();
        var nativeValue = FromValue(value, out var handle);
        try
        {
            unsafe
            {
                var ptr = &nativeValue;
                TursoBindings.BindParameter(_statementPtr, index, (IntPtr)ptr);
            }
        }
        finally
        {
            if (handle.HasValue)
                handle.Value.Free();
        }
    }

    public void BindNamedParameter(TursoParameter parameter)
    {
        var parameterName = parameter.ParameterName!;
        var value = parameter.ToValue();
        var nativeValue = FromValue(value, out var handle);
        try
        {
            unsafe
            {
                var ptr = &nativeValue;
                TursoBindings.BindNamedParameter(_statementPtr, parameterName, (IntPtr)ptr);
            }
        }
        finally
        {
            if (handle.HasValue)
                handle.Value.Free();
        }
    }

    public bool Read()
    {
        var errorHandle = new TursoErrorHandle();
        var hasData = TursoBindings.StatementExecuteStep(_statementPtr, errorHandle.Ptr());
        if (errorHandle.ErrorPtr != IntPtr.Zero)
            TursoHelpers.ThrowException(errorHandle.ErrorPtr);
        return hasData;
    }

    public int RowsAffected()
    {
        return (int)TursoBindings.StatementRowsAffected(_statementPtr);
    }


    public TursoValue GetRow(int columnIndex)
    {
        var rowValue = TursoBindings.GetValueFromStatement(_statementPtr, columnIndex);
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

    private byte[] ToArray(TursoNativeArray array)
    {
        unsafe
        {
            var data = new Span<byte>((void*)array.Data, (int)array.Length);
            return data.ToArray();
        }
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
            union.StringValue = new TursoNativeArray { Data = (IntPtr)handle, Length = (ulong)bytes.Length };
        }

        if (value.ValueType == TursoValueType.Blob)
        {
            handle = GCHandle.Alloc(value.BlobValue, GCHandleType.Pinned);
            union.BlobValue = new TursoNativeArray { Data = (IntPtr)handle, Length = (ulong)value.BlobValue.Length };
        }

        return new TursoNativeValue
        {
            ValueType = value.ValueType,
            RowValueUnion = union,
        };
    }


    private void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) == 0)
        {
            TursoBindings.FreeStatement(_statementPtr);
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~TursoNativeStatement()
    {
        Dispose(false);
    }
}