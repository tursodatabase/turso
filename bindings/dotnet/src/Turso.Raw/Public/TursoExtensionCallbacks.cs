using System.Runtime.InteropServices;

namespace Turso.Raw.Public;

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoScalarFunctionCallback(IntPtr context, int argc, IntPtr argv, IntPtr result);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate IntPtr TursoAggregateInitCallback(IntPtr context);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoAggregateStepCallback(IntPtr context, IntPtr aggregateContext, int argc, IntPtr argv, IntPtr result);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoAggregateFinalCallback(IntPtr context, IntPtr aggregateContext, IntPtr result);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate int TursoCollationCallback(IntPtr context, IntPtr leftPtr, UIntPtr leftLen, IntPtr rightPtr, UIntPtr rightLen);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoContextDestructorCallback(IntPtr context);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoValueDestructorCallback(IntPtr result);
