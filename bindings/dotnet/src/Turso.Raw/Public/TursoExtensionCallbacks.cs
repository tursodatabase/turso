using System.Runtime.InteropServices;

namespace Turso.Raw.Public;

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate TursoExtensionValue TursoScalarFunctionCallback(IntPtr context, int argc, IntPtr argv, IntPtr contextDestructor, IntPtr valueDestructor);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate IntPtr TursoAggregateInitCallback(IntPtr context);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate TursoExtensionValue TursoAggregateStepCallback(IntPtr context, IntPtr aggregateContext, int argc, IntPtr argv);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate TursoExtensionValue TursoAggregateFinalCallback(IntPtr context, IntPtr aggregateContext);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate int TursoCollationCallback(IntPtr context, IntPtr leftPtr, UIntPtr leftLen, IntPtr rightPtr, UIntPtr rightLen);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoContextDestructorCallback(IntPtr context);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoValueDestructorCallback(IntPtr result);

public enum TursoExtensionValueType
{
    Null = 0,
    Integer = 1,
    Float = 2,
    Text = 3,
    Blob = 4,
    Error = 5,
}

[StructLayout(LayoutKind.Explicit)]
public struct TursoExtensionValue
{
    [FieldOffset(0)]
    public TursoExtensionValueType ValueType;

    [FieldOffset(8)]
    public TursoExtensionValueUnion Value;
}

[StructLayout(LayoutKind.Explicit)]
public struct TursoExtensionValueUnion
{
    [FieldOffset(0)]
    public long IntValue;

    [FieldOffset(0)]
    public double RealValue;

    [FieldOffset(0)]
    public IntPtr TextValue;

    [FieldOffset(0)]
    public IntPtr BlobValue;

    [FieldOffset(0)]
    public IntPtr ErrorValue;
}
