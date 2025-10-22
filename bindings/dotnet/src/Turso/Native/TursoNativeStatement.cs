using System.Runtime.InteropServices;
using System.Text;
using Turso.RowValue;

namespace Turso.Native;

public class TursoNativeStatement : IDisposable
{
    private readonly StatementHandle _statementHandle;
    private int _isDisposed = 0;

    public TursoNativeStatement(StatementHandle statementHandle)
    {
        _statementHandle = statementHandle;
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
                TursoBindings.BindParameter(_statementHandle, index, (IntPtr)ptr);
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
                TursoBindings.BindNamedParameter(_statementHandle, parameterName, (IntPtr)ptr);
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
        var errorPtr = TursoBindings.StatementExecuteStep(_statementHandle, out var hasData);
        if (errorPtr != IntPtr.Zero)
            TursoHelpers.ThrowException(errorPtr);
        return hasData;
    }

    public int RowsAffected()
    {
        return (int)TursoBindings.StatementRowsAffected(_statementHandle);
    }


    public TursoValue GetRow(int columnIndex)
    {
        var rowValue = TursoBindings.GetValueFromStatement(_statementHandle, columnIndex);
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
            union.StringValue = new TursoNativeArray { Data = handle.Value.AddrOfPinnedObject(), Length = (ulong)bytes.Length };
        }

        if (value.ValueType == TursoValueType.Blob)
        {
            handle = GCHandle.Alloc(value.BlobValue, GCHandleType.Pinned);
            union.BlobValue = new TursoNativeArray { Data = handle.Value.AddrOfPinnedObject(), Length = (ulong)value.BlobValue.Length };
        }

        return new TursoNativeValue
        {
            ValueType = value.ValueType,
            RowValueUnion = union,
        };
    }


    private void Dispose(bool disposing)
    {
        _statementHandle.Dispose();
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