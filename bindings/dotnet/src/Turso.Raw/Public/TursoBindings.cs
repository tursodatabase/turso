using System.Runtime.InteropServices;
using System.Text;
using Turso.Raw.Public.Handles;
using Turso.Raw.Public.Value;

namespace Turso.Raw.Public;

public static class TursoBindings
{
    private const string AdvancedExtensionApisMessage =
        "SQLite-compatible function, aggregate, collation, and extension-loading APIs require Turso core managed extension support.";

    public static TursoDatabaseHandle OpenDatabase(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return OpenDatabase(path, cipher: null, hexkey: null);
    }

    /// <summary>
    /// Opens a database with local encryption.
    /// </summary>
    /// <param name="path">The path to the database file.</param>
    /// <param name="cipher">The encryption cipher to use.</param>
    /// <param name="hexkey">The hex-encoded encryption key.</param>
    /// <returns>A handle to the opened database.</returns>
    public static TursoDatabaseHandle OpenDatabaseWithEncryption(string path, TursoEncryptionCipher cipher, string hexkey)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(hexkey);

        return OpenDatabase(path, cipher.ToRustString(), hexkey);
    }

    public static TursoStatementHandle PrepareStatement(TursoDatabaseHandle db, string sql)
    {
        db.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(sql);

        var status = TursoInterop.ConnectionPrepareSingle(db, sql, out var statementPtr, out var errorPtr);
        ThrowIfError(status, errorPtr, "Unable to prepare statement: ");

        return TursoStatementHandle.FromPtr(statementPtr);
    }

    public static void RegisterScalarFunction(
        TursoDatabaseHandle db,
        string name,
        int argc,
        bool deterministic,
        IntPtr context,
        TursoScalarFunctionCallback callback,
        TursoContextDestructorCallback contextDestructor,
        TursoValueDestructorCallback valueDestructor)
    {
        db.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentNullException.ThrowIfNull(contextDestructor);
        ArgumentNullException.ThrowIfNull(valueDestructor);

        throw new NotSupportedException(AdvancedExtensionApisMessage);
    }

    public static void RegisterAggregateFunction(
        TursoDatabaseHandle db,
        string name,
        int argc,
        bool deterministic,
        IntPtr context,
        TursoAggregateInitCallback init,
        TursoAggregateStepCallback step,
        TursoAggregateFinalCallback finalize,
        TursoContextDestructorCallback contextDestructor,
        TursoContextDestructorCallback aggregateDestructor,
        TursoValueDestructorCallback valueDestructor)
    {
        db.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(init);
        ArgumentNullException.ThrowIfNull(step);
        ArgumentNullException.ThrowIfNull(finalize);
        ArgumentNullException.ThrowIfNull(contextDestructor);
        ArgumentNullException.ThrowIfNull(aggregateDestructor);
        ArgumentNullException.ThrowIfNull(valueDestructor);

        throw new NotSupportedException(AdvancedExtensionApisMessage);
    }

    public static void UnregisterFunction(TursoDatabaseHandle db, string name)
    {
        db.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(name);

        throw new NotSupportedException(AdvancedExtensionApisMessage);
    }

    public static void RegisterCollation(
        TursoDatabaseHandle db,
        string name,
        IntPtr context,
        TursoCollationCallback callback,
        TursoContextDestructorCallback contextDestructor)
    {
        db.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentNullException.ThrowIfNull(contextDestructor);

        throw new NotSupportedException(AdvancedExtensionApisMessage);
    }

    public static void UnregisterCollation(TursoDatabaseHandle db, string name)
    {
        db.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(name);

        throw new NotSupportedException(AdvancedExtensionApisMessage);
    }

    public static void EnableLoadExtension(TursoDatabaseHandle db, bool enabled)
    {
        db.ThrowIfInvalid();
        throw new NotSupportedException(AdvancedExtensionApisMessage);
    }

    public static void LoadExtension(TursoDatabaseHandle db, string path)
    {
        db.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(path);

        throw new NotSupportedException(AdvancedExtensionApisMessage);
    }

    public static void BindParameter(TursoStatementHandle statement, int index, TursoValue parameter)
    {
        statement.ThrowIfInvalid();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(index);

        BindParameterAt(statement, index, parameter);
    }

    public static int BindNamedParameter(TursoStatementHandle statement, string name, TursoValue parameter)
    {
        statement.ThrowIfInvalid();
        ArgumentNullException.ThrowIfNull(name);

        var index = TursoInterop.StatementNamedPosition(statement, name);
        if (index < 1)
            return 0;
        if (index > int.MaxValue)
            throw new InvalidOperationException($"Parameter index {index} is too large.");

        BindParameterAt(statement, (int)index, parameter);
        return (int)index;
    }

    public static bool Read(TursoStatementHandle statement)
    {
        statement.ThrowIfInvalid();

        while (true)
        {
            var status = TursoInterop.StatementStep(statement, out var errorPtr);
            if (status == TursoStatusCode.Row)
                return true;
            if (status == TursoStatusCode.Done)
                return false;
            if (status == TursoStatusCode.Io)
            {
                var ioStatus = TursoInterop.StatementRunIo(statement, out errorPtr);
                ThrowIfError(ioStatus, errorPtr);
                continue;
            }

            ThrowIfError(status, errorPtr);
        }
    }

    public static TursoValue GetValue(TursoStatementHandle statement, int columnIndex)
    {
        statement.ThrowIfInvalid();
        ArgumentOutOfRangeException.ThrowIfNegative(columnIndex);

        var index = ToNativeIndex(columnIndex);
        return TursoInterop.StatementRowValueKind(statement, index) switch
        {
            TursoNativeValueType.Unknown => TursoValue.Empty(),
            TursoNativeValueType.Null => TursoValue.Null(),
            TursoNativeValueType.Integer => TursoValue.Int(TursoInterop.StatementRowValueInt(statement, index)),
            TursoNativeValueType.Real => TursoValue.Real(TursoInterop.StatementRowValueDouble(statement, index)),
            TursoNativeValueType.Text => TursoValue.String(Encoding.UTF8.GetString(ReadBytes(statement, index))),
            TursoNativeValueType.Blob => TursoValue.Blob(ReadBytes(statement, index)),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public static string GetName(TursoStatementHandle statement, int ordinal)
    {
        statement.ThrowIfInvalid();
        ArgumentOutOfRangeException.ThrowIfNegative(ordinal);

        var cname = TursoInterop.StatementColumnName(statement, ToNativeIndex(ordinal));
        if (cname == IntPtr.Zero)
            return "";

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
        statement.ThrowIfInvalid();

        return checked((int)TursoInterop.StatementColumnCount(statement));
    }

    public static int RowsAffected(TursoStatementHandle statement)
    {
        statement.ThrowIfInvalid();

        return checked((int)TursoInterop.StatementRowsAffected(statement));
    }

    public static bool HasRows(TursoStatementHandle statement)
    {
        statement.ThrowIfInvalid();

        return GetFieldCount(statement) > 0
               && TursoInterop.StatementRowValueKind(statement, UIntPtr.Zero) != TursoNativeValueType.Unknown;
    }

    public static int GetParameterCount(TursoStatementHandle statement)
    {
        statement.ThrowIfInvalid();

        return checked((int)TursoInterop.StatementParameterCount(statement));
    }

    public static string? GetParameterName(TursoStatementHandle statement, int index)
    {
        statement.ThrowIfInvalid();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(index);

        var name = TursoInterop.StatementParameterName(statement, index);
        if (name == IntPtr.Zero)
            return null;

        try
        {
            return Marshal.PtrToStringUTF8(name);
        }
        finally
        {
            TursoInterop.FreeString(name);
        }
    }

    private static TursoDatabaseHandle OpenDatabase(string path, string? cipher, string? hexkey)
    {
        using var pathString = NativeUtf8String.From(path);
        using var featuresString = NativeUtf8String.From(cipher is null ? null : "encryption");
        using var cipherString = NativeUtf8String.From(cipher);
        using var hexkeyString = NativeUtf8String.From(hexkey);

        var config = new TursoDatabaseConfig
        {
            AsyncIo = 0,
            Path = pathString.Pointer,
            ExperimentalFeatures = featuresString.Pointer,
            Vfs = IntPtr.Zero,
            EncryptionCipher = cipherString.Pointer,
            EncryptionHexKey = hexkeyString.Pointer,
        };

        var status = TursoInterop.DatabaseNew(ref config, out var databasePtr, out var errorPtr);
        ThrowIfError(status, errorPtr);

        var connectionPtr = IntPtr.Zero;
        try
        {
            status = TursoInterop.DatabaseOpen(databasePtr, out errorPtr);
            ThrowIfError(status, errorPtr);

            status = TursoInterop.DatabaseConnect(databasePtr, out connectionPtr, out errorPtr);
            ThrowIfError(status, errorPtr);

            return TursoDatabaseHandle.FromPtrs(databasePtr, connectionPtr);
        }
        catch
        {
            if (connectionPtr != IntPtr.Zero)
                TursoInterop.ConnectionDeinit(connectionPtr);
            if (databasePtr != IntPtr.Zero)
                TursoInterop.DatabaseDeinit(databasePtr);
            throw;
        }
    }

    private static void BindParameterAt(TursoStatementHandle statement, int index, TursoValue value)
    {
        var position = ToNativeIndex(index);
        var status = value.ValueType switch
        {
            TursoValueType.Empty or TursoValueType.Null => TursoInterop.StatementBindNull(statement, position),
            TursoValueType.Integer => TursoInterop.StatementBindInt(statement, position, value.IntValue),
            TursoValueType.Real => TursoInterop.StatementBindDouble(statement, position, value.RealValue),
            TursoValueType.Text => BindText(statement, position, value.StringValue),
            TursoValueType.Blob => BindBlob(statement, position, value.BlobValue),
            _ => throw new ArgumentOutOfRangeException()
        };

        ThrowIfError(status, IntPtr.Zero);
    }

    private static TursoStatusCode BindText(TursoStatementHandle statement, UIntPtr position, string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var bytes = Encoding.UTF8.GetBytes(value);
        return BindBytes(bytes, ptr => TursoInterop.StatementBindText(statement, position, ptr, ToNativeLength(bytes.Length)));
    }

    private static TursoStatusCode BindBlob(TursoStatementHandle statement, UIntPtr position, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);

        return BindBytes(value, ptr => TursoInterop.StatementBindBlob(statement, position, ptr, ToNativeLength(value.Length)));
    }

    private static TursoStatusCode BindBytes(byte[] bytes, Func<IntPtr, TursoStatusCode> bind)
    {
        if (bytes.Length == 0)
            return bind(IntPtr.Zero);

        var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        try
        {
            return bind(handle.AddrOfPinnedObject());
        }
        finally
        {
            handle.Free();
        }
    }

    private static byte[] ReadBytes(TursoStatementHandle statement, UIntPtr index)
    {
        var length = TursoInterop.StatementRowValueBytesCount(statement, index);
        if (length < 0)
            throw new TursoException("Unable to read native value bytes.");
        if (length == 0)
            return [];

        var ptr = TursoInterop.StatementRowValueBytesPtr(statement, index);
        if (ptr == IntPtr.Zero)
            throw new TursoException("Unable to read native value bytes.");

        unsafe
        {
            var data = new ReadOnlySpan<byte>((void*)ptr, checked((int)length));
            return data.ToArray();
        }
    }

    private static UIntPtr ToNativeIndex(int index) => checked((UIntPtr)(ulong)index);

    private static UIntPtr ToNativeLength(int length) => checked((UIntPtr)(ulong)length);

    private static void ThrowIfError(TursoStatusCode status, IntPtr errorPtr, string? messagePrefix = null)
    {
        if (errorPtr != IntPtr.Zero)
            ThrowException(errorPtr, messagePrefix);

        if (status is TursoStatusCode.Ok or TursoStatusCode.Done or TursoStatusCode.Row)
            return;

        throw new TursoException($"Turso native call failed with status {status}.");
    }

    private static void ThrowException(IntPtr errorPtr, string? messagePrefix = null)
    {
        var errorMessage = Marshal.PtrToStringUTF8(errorPtr);
        var exception = new TursoException($"{messagePrefix}{errorMessage ?? "Internal error"}");
        TursoInterop.FreeString(errorPtr);
        throw exception;
    }

    private sealed class NativeUtf8String : IDisposable
    {
        private NativeUtf8String(IntPtr pointer) => Pointer = pointer;

        public IntPtr Pointer { get; private set; }

        public static NativeUtf8String From(string? value)
        {
            return new NativeUtf8String(value is null ? IntPtr.Zero : Marshal.StringToCoTaskMemUTF8(value));
        }

        public void Dispose()
        {
            if (Pointer == IntPtr.Zero)
                return;

            Marshal.FreeCoTaskMem(Pointer);
            Pointer = IntPtr.Zero;
        }
    }
}
