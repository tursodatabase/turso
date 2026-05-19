using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Turso.Raw.Public;

namespace Turso.Data.Sqlite;

public partial class SqliteConnection
{
    private static readonly TursoScalarFunctionCallback ScalarFunctionCallback = InvokeScalarFunction;
    private static readonly TursoContextDestructorCallback ContextDestructorCallback = NoopContextDestructor;
    private static readonly TursoValueDestructorCallback ValueDestructorCallback = DestroyFunctionValue;
    private readonly Dictionary<string, ScalarFunctionRegistration> _scalarFunctions = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<GCHandle> _nativeFunctionContexts = [];

    private void RegisterScalarFunction(string name, int argc, bool isDeterministic, Func<object?[], object?>? function)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (function is null)
        {
            _scalarFunctions.Remove(name);
            if (_database is not null)
                TursoBindings.UnregisterFunction(DatabaseHandle, name);
            return;
        }

        var registration = new ScalarFunctionRegistration(name, argc, isDeterministic, function);
        _scalarFunctions[name] = registration;
        if (_database is not null)
            _nativeFunctionContexts.Add(registration.Register(DatabaseHandle));
    }

    private void RegisterScalarFunctions()
    {
        foreach (var registration in _scalarFunctions.Values)
            _nativeFunctionContexts.Add(registration.Register(DatabaseHandle));
    }

    private void FreeNativeFunctionContexts()
    {
        foreach (var handle in _nativeFunctionContexts)
        {
            if (handle.Target is AggregateFunctionRegistration aggregate)
                aggregate.FreeInvocations();
            if (handle.IsAllocated)
                handle.Free();
        }

        _nativeFunctionContexts.Clear();
    }

    private static object? InvokeTypedFunction<T1, TResult>(string name, Func<T1, TResult> function, object?[] args)
        => function(ConvertArgument<T1>(name, args[0], 0));

    private static object? InvokeTypedFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> function, object?[] args)
        => function(ConvertArgument<T1>(name, args[0], 0), ConvertArgument<T2>(name, args[1], 1));

    private static object? InvokeTypedFunction<TState, TResult>(TState state, Func<TState, TResult> function, object?[] args)
        => function(state);

    private static object? InvokeTypedFunction<TState, T1, TResult>(string name, TState state, Func<TState, T1, TResult> function, object?[] args)
        => function(state, ConvertArgument<T1>(name, args[0], 0));

    private static object? InvokeTypedFunction<TState, T1, T2, TResult>(string name, TState state, Func<TState, T1, T2, TResult> function, object?[] args)
        => function(state, ConvertArgument<T1>(name, args[0], 0), ConvertArgument<T2>(name, args[1], 1));

    private static T ConvertArgument<T>(string functionName, object? value, int ordinal)
    {
        if (value is null or DBNull)
        {
            if (!typeof(T).IsValueType || Nullable.GetUnderlyingType(typeof(T)) is not null)
                return default!;

            throw new InvalidOperationException(Properties.Resources.UDFCalledWithNull(functionName, ordinal));
        }

        var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        if (targetType == typeof(object))
            return (T)value;
        if (targetType == typeof(byte[]) && value is byte[] bytes)
            return (T)(object)bytes;
        if (targetType == typeof(string))
            return (T)(object)Convert.ToString(value, CultureInfo.InvariantCulture)!;
        if (targetType == typeof(bool))
            return (T)(object)Convert.ToBoolean(value, CultureInfo.InvariantCulture);

        return (T)Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
    }

    private static void InvokeScalarFunction(IntPtr context, int argc, IntPtr argv, IntPtr result)
    {
        try
        {
            var registration = (ScalarFunctionRegistration?)GCHandle.FromIntPtr(context).Target
                ?? throw new ObjectDisposedException(nameof(ScalarFunctionRegistration));
            var args = ReadArguments(argc, argv);
            WriteResult(result, registration.Invoke(args));
        }
        catch (SqliteException ex)
        {
            WriteError(result, "__turso_sqlite_error__:" + ex.SqliteErrorCode.ToString(CultureInfo.InvariantCulture) + ":" + ex.Message);
        }
        catch (Exception ex)
        {
            WriteError(result, ex.Message);
        }
    }

    private static void NoopContextDestructor(IntPtr context)
    {
    }

    private static void DestroyFunctionValue(IntPtr result)
    {
        if (result == IntPtr.Zero)
            return;

        var value = Marshal.PtrToStructure<ExtensionResultValue>(result);
        if (value.ValueType is ExtensionValueType.Text or ExtensionValueType.Blob or ExtensionValueType.Error)
        {
            var data = value.Value.Bytes.Data;
            if (data != IntPtr.Zero)
                Marshal.FreeHGlobal(data);
        }
    }

    private static object?[] ReadArguments(int argc, IntPtr argv)
    {
        if (argc == 0)
            return [];

        var args = new object?[argc];
        var size = Marshal.SizeOf<ExtensionInputValue>();
        for (var i = 0; i < argc; i++)
        {
            var value = Marshal.PtrToStructure<ExtensionInputValue>(IntPtr.Add(argv, i * size));
            args[i] = value.ValueType switch
            {
                ExtensionValueType.Null => null,
                ExtensionValueType.Integer => value.Value.IntValue,
                ExtensionValueType.Real => value.Value.RealValue,
                ExtensionValueType.Text => ReadText(value.Value.TextValue),
                ExtensionValueType.Blob => ReadBlob(value.Value.BlobValue),
                _ => null
            };
        }

        return args;
    }

    private static string ReadText(IntPtr textValuePtr)
    {
        if (textValuePtr == IntPtr.Zero)
            return string.Empty;

        var textValue = Marshal.PtrToStructure<ExtensionTextValue>(textValuePtr);
        if (textValue.Text == IntPtr.Zero || textValue.Length == 0)
            return string.Empty;

        var bytes = new byte[textValue.Length];
        Marshal.Copy(textValue.Text, bytes, 0, bytes.Length);
        return Encoding.UTF8.GetString(bytes);
    }

    private static byte[] ReadBlob(IntPtr blobValuePtr)
    {
        if (blobValuePtr == IntPtr.Zero)
            return [];

        var blobValue = Marshal.PtrToStructure<ExtensionBlobValue>(blobValuePtr);
        if (blobValue.Data == IntPtr.Zero || blobValue.Length == 0)
            return [];

        var bytes = new byte[checked((int)blobValue.Length)];
        Marshal.Copy(blobValue.Data, bytes, 0, bytes.Length);
        return bytes;
    }

    private static void WriteResult(IntPtr result, object? value)
    {
        if (value is null or DBNull)
        {
            Marshal.StructureToPtr(ExtensionResultValue.Null(), result, false);
            return;
        }

        switch (value)
        {
            case bool boolValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(boolValue ? 1 : 0), result, false);
                break;
            case byte byteValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(byteValue), result, false);
                break;
            case sbyte sbyteValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(sbyteValue), result, false);
                break;
            case short shortValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(shortValue), result, false);
                break;
            case ushort ushortValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(ushortValue), result, false);
                break;
            case int intValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(intValue), result, false);
                break;
            case uint uintValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(uintValue), result, false);
                break;
            case long longValue:
                Marshal.StructureToPtr(ExtensionResultValue.Integer(longValue), result, false);
                break;
            case float floatValue:
                Marshal.StructureToPtr(ExtensionResultValue.Real(floatValue), result, false);
                break;
            case double doubleValue:
                Marshal.StructureToPtr(ExtensionResultValue.Real(doubleValue), result, false);
                break;
            case decimal decimalValue:
                Marshal.StructureToPtr(ExtensionResultValue.Real((double)decimalValue), result, false);
                break;
            case byte[] bytes:
                Marshal.StructureToPtr(ExtensionResultValue.Bytes(ExtensionValueType.Blob, bytes), result, false);
                break;
            default:
                Marshal.StructureToPtr(ExtensionResultValue.Bytes(ExtensionValueType.Text, Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty)), result, false);
                break;
        }
    }

    private static void WriteError(IntPtr result, string message)
        => Marshal.StructureToPtr(ExtensionResultValue.Bytes(ExtensionValueType.Error, Encoding.UTF8.GetBytes(message)), result, false);

    private sealed class ScalarFunctionRegistration(string name, int argc, bool isDeterministic, Func<object?[], object?> invoke)
    {
        public object? Invoke(object?[] args) => invoke(args);

        public GCHandle Register(Turso.Raw.Public.Handles.TursoDatabaseHandle database)
        {
            var handle = GCHandle.Alloc(this);
            try
            {
                TursoBindings.RegisterScalarFunction(
                    database,
                    name,
                    argc,
                    isDeterministic,
                    GCHandle.ToIntPtr(handle),
                    ScalarFunctionCallback,
                    ContextDestructorCallback,
                    ValueDestructorCallback);
                return handle;
            }
            catch
            {
                handle.Free();
                throw;
            }
        }
    }

    private enum ExtensionValueType
    {
        Null = 0,
        Integer = 1,
        Real = 2,
        Text = 3,
        Blob = 4,
        Error = 5,
    }

    [StructLayout(LayoutKind.Explicit)]
    private struct ExtensionInputValue
    {
        [FieldOffset(0)]
        public ExtensionValueType ValueType;

        [FieldOffset(8)]
        public ExtensionInputValueUnion Value;
    }

    [StructLayout(LayoutKind.Explicit)]
    private struct ExtensionInputValueUnion
    {
        [FieldOffset(0)]
        public long IntValue;

        [FieldOffset(0)]
        public double RealValue;

        [FieldOffset(0)]
        public IntPtr TextValue;

        [FieldOffset(0)]
        public IntPtr BlobValue;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionTextValue
    {
        public int Subtype;
        public IntPtr Text;
        public uint Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionBlobValue
    {
        public IntPtr Data;
        public ulong Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionBytes
    {
        public IntPtr Data;
        public nuint Length;
    }

    [StructLayout(LayoutKind.Explicit)]
    private struct ExtensionResultValue
    {
        [FieldOffset(0)]
        public ExtensionValueType ValueType;

        [FieldOffset(8)]
        public ExtensionResultValueUnion Value;

        public static ExtensionResultValue Null()
            => new() { ValueType = ExtensionValueType.Null };

        public static ExtensionResultValue Integer(long value)
            => new() { ValueType = ExtensionValueType.Integer, Value = new ExtensionResultValueUnion { IntValue = value } };

        public static ExtensionResultValue Real(double value)
            => new() { ValueType = ExtensionValueType.Real, Value = new ExtensionResultValueUnion { RealValue = value } };

        public static ExtensionResultValue Bytes(ExtensionValueType valueType, byte[] bytes)
        {
            var data = Marshal.AllocHGlobal(bytes.Length);
            if (bytes.Length != 0)
                Marshal.Copy(bytes, 0, data, bytes.Length);

            return new ExtensionResultValue
            {
                ValueType = valueType,
                Value = new ExtensionResultValueUnion
                {
                    Bytes = new ExtensionBytes { Data = data, Length = (nuint)bytes.Length }
                }
            };
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    private struct ExtensionResultValueUnion
    {
        [FieldOffset(0)]
        public long IntValue;

        [FieldOffset(0)]
        public double RealValue;

        [FieldOffset(0)]
        public ExtensionBytes Bytes;
    }
}
