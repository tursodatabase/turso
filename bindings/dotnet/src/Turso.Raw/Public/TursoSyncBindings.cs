using System.Runtime.InteropServices;
using System.Text;
using Turso.Raw.Public.Handles;

namespace Turso.Raw.Public;

public enum TursoSyncOperationState
{
    Continue,
    Io,
    Done,
}

public enum TursoSyncOperationResultKind
{
    None,
    Connection,
    Changes,
    Stats,
}

public enum TursoSyncIoKind
{
    None,
    Http,
    FullRead,
    FullWrite,
}

public sealed class TursoSyncDatabaseConfiguration
{
    public required string Path { get; init; }
    public string? RemoteUrl { get; init; }
    public string ClientName { get; init; } = "turso-sync-dotnet";
    public int LongPollTimeoutMilliseconds { get; init; }
    public bool BootstrapIfEmpty { get; init; } = true;
    public int ReservedBytes { get; init; }
    public int PartialBootstrapStrategyPrefix { get; init; }
    public string? PartialBootstrapStrategyQuery { get; init; }
    public nuint PartialBootstrapSegmentSize { get; init; }
    public bool PartialBootstrapPrefetch { get; init; }
    public string? RemoteEncryptionKey { get; init; }
    public string? RemoteEncryptionCipher { get; init; }
    public nuint PushOperationsThreshold { get; init; }
    public nuint PullBytesThreshold { get; init; }
    public bool LogicalMvccPull { get; init; }
    public string? ExperimentalFeatures { get; init; }
}

public sealed record TursoSyncHttpHeader(string Name, string Value);

public sealed record TursoSyncHttpRequest(
    string? Url,
    string Method,
    string Path,
    byte[] Body,
    IReadOnlyList<TursoSyncHttpHeader> Headers);

public sealed record TursoSyncFullWriteRequest(string Path, byte[] Content);

public sealed record TursoSyncStatistics(
    long CdcOperations,
    long MainWalSize,
    long RevertWalSize,
    long LastPullUnixTime,
    long LastPushUnixTime,
    long NetworkSentBytes,
    long NetworkReceivedBytes,
    string Revision);

public static class TursoSyncBindings
{
    public static TursoSyncDatabaseHandle NewDatabase(TursoSyncDatabaseConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(configuration.Path);
        ArgumentException.ThrowIfNullOrWhiteSpace(configuration.ClientName);
        ArgumentOutOfRangeException.ThrowIfNegative(configuration.LongPollTimeoutMilliseconds);
        ArgumentOutOfRangeException.ThrowIfNegative(configuration.ReservedBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(configuration.PartialBootstrapStrategyPrefix);

        using var path = NativeUtf8String.From(configuration.Path);
        using var remoteUrl = NativeUtf8String.From(configuration.RemoteUrl);
        using var clientName = NativeUtf8String.From(configuration.ClientName);
        using var query = NativeUtf8String.From(configuration.PartialBootstrapStrategyQuery);
        using var remoteEncryptionKey = NativeUtf8String.From(configuration.RemoteEncryptionKey);
        using var remoteEncryptionCipher = NativeUtf8String.From(configuration.RemoteEncryptionCipher);
        using var experimentalFeatures = NativeUtf8String.From(configuration.ExperimentalFeatures);

        var databaseConfig = new TursoDatabaseConfig
        {
            AsyncIo = 1,
            Path = path.Pointer,
            ExperimentalFeatures = experimentalFeatures.Pointer,
            Vfs = IntPtr.Zero,
            EncryptionCipher = IntPtr.Zero,
            EncryptionHexKey = IntPtr.Zero,
            PageCodec = IntPtr.Zero,
            OpenFlags = 0,
        };
        var syncConfig = new TursoSyncDatabaseConfigNative
        {
            Path = path.Pointer,
            RemoteUrl = remoteUrl.Pointer,
            ClientName = clientName.Pointer,
            LongPollTimeoutMs = configuration.LongPollTimeoutMilliseconds,
            BootstrapIfEmpty = configuration.BootstrapIfEmpty,
            ReservedBytes = configuration.ReservedBytes,
            PartialBootstrapStrategyPrefix = configuration.PartialBootstrapStrategyPrefix,
            PartialBootstrapStrategyQuery = query.Pointer,
            PartialBootstrapSegmentSize = configuration.PartialBootstrapSegmentSize,
            PartialBootstrapPrefetch = configuration.PartialBootstrapPrefetch,
            RemoteEncryptionKey = remoteEncryptionKey.Pointer,
            RemoteEncryptionCipher = remoteEncryptionCipher.Pointer,
            PushOperationsThreshold = configuration.PushOperationsThreshold,
            PullBytesThreshold = configuration.PullBytesThreshold,
            LogicalMvccPull = configuration.LogicalMvccPull,
        };

        var status = TursoSyncInterop.DatabaseNew(ref databaseConfig, ref syncConfig, out var database, out var errorPtr);
        ThrowIfError(status, errorPtr);
        return TursoSyncDatabaseHandle.FromPtr(database);
    }

    public static TursoSyncOperationHandle StartOpen(TursoSyncDatabaseHandle database)
        => StartOperation(database, TursoSyncInterop.DatabaseOpen);

    public static TursoSyncOperationHandle StartCreate(TursoSyncDatabaseHandle database)
        => StartOperation(database, TursoSyncInterop.DatabaseCreate);

    public static TursoSyncOperationHandle StartConnect(TursoSyncDatabaseHandle database)
        => StartOperation(database, TursoSyncInterop.DatabaseConnect);

    public static TursoSyncOperationHandle StartStats(TursoSyncDatabaseHandle database)
        => StartOperation(database, TursoSyncInterop.DatabaseStats);

    public static TursoSyncOperationHandle StartCheckpoint(TursoSyncDatabaseHandle database)
        => StartOperation(database, TursoSyncInterop.DatabaseCheckpoint);

    public static TursoSyncOperationHandle StartPush(TursoSyncDatabaseHandle database)
        => StartOperation(database, TursoSyncInterop.DatabasePushChanges);

    public static TursoSyncOperationHandle StartWaitChanges(TursoSyncDatabaseHandle database)
        => StartOperation(database, TursoSyncInterop.DatabaseWaitChanges);

    public static TursoSyncOperationHandle StartApplyChanges(
        TursoSyncDatabaseHandle database,
        TursoSyncChangesHandle changes)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(changes);

        var databaseReferenceAdded = false;
        database.DangerousAddRef(ref databaseReferenceAdded);
        try
        {
            var changesPointer = changes.Consume();
            var status = TursoSyncInterop.DatabaseApplyChanges(
                database.DangerousGetHandle(),
                changesPointer,
                out var operation,
                out var errorPtr);
            ThrowIfError(status, errorPtr);
            return TursoSyncOperationHandle.FromPtr(operation);
        }
        finally
        {
            if (databaseReferenceAdded)
                database.DangerousRelease();
        }
    }

    public static TursoSyncOperationState Resume(TursoSyncOperationHandle operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var status = TursoSyncInterop.OperationResume(operation, out var errorPtr);
        if (errorPtr != IntPtr.Zero)
            ThrowException(status, errorPtr);

        return status switch
        {
            TursoStatusCode.Ok => TursoSyncOperationState.Continue,
            TursoStatusCode.Io => TursoSyncOperationState.Io,
            TursoStatusCode.Done => TursoSyncOperationState.Done,
            _ => throw new TursoSyncNativeException(
                (uint)status,
                $"Turso sync operation failed with status {status}."),
        };
    }

    public static TursoSyncOperationResultKind GetResultKind(TursoSyncOperationHandle operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return TursoSyncInterop.OperationResultKind(operation) switch
        {
            TursoSyncOperationResultType.None => TursoSyncOperationResultKind.None,
            TursoSyncOperationResultType.Connection => TursoSyncOperationResultKind.Connection,
            TursoSyncOperationResultType.Changes => TursoSyncOperationResultKind.Changes,
            TursoSyncOperationResultType.Stats => TursoSyncOperationResultKind.Stats,
            var value => throw new TursoException($"Unknown Turso sync result kind {value}."),
        };
    }

    public static TursoDatabaseHandle ExtractConnection(
        TursoSyncDatabaseHandle database,
        TursoSyncOperationHandle operation)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(operation);

        var databaseReferenceAdded = false;
        var connection = IntPtr.Zero;
        database.DangerousAddRef(ref databaseReferenceAdded);
        try
        {
            var status = TursoSyncInterop.OperationExtractConnection(operation, out connection);
            ThrowIfError(status, IntPtr.Zero);
            return TursoDatabaseHandle.FromConnectionPtr(connection, database);
        }
        catch
        {
            if (connection != IntPtr.Zero)
                ReleaseConnection(connection);
            throw;
        }
        finally
        {
            if (databaseReferenceAdded)
                database.DangerousRelease();
        }
    }

    public static TursoSyncChangesHandle? ExtractChanges(TursoSyncOperationHandle operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var status = TursoSyncInterop.OperationExtractChanges(operation, out var changes);
        ThrowIfError(status, IntPtr.Zero);
        return changes == IntPtr.Zero ? null : TursoSyncChangesHandle.FromPtr(changes);
    }

    public static TursoSyncStatistics ExtractStats(TursoSyncOperationHandle operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var operationReferenceAdded = false;
        operation.DangerousAddRef(ref operationReferenceAdded);
        try
        {
            var status = TursoSyncInterop.OperationExtractStats(operation, out var stats);
            ThrowIfError(status, IntPtr.Zero);
            return new TursoSyncStatistics(
                stats.CdcOperations,
                stats.MainWalSize,
                stats.RevertWalSize,
                stats.LastPullUnixTime,
                stats.LastPushUnixTime,
                stats.NetworkSentBytes,
                stats.NetworkReceivedBytes,
                CopyUtf8(stats.Revision));
        }
        finally
        {
            if (operationReferenceAdded)
                operation.DangerousRelease();
        }
    }

    public static TursoSyncIoItemHandle? TakeIoItem(TursoSyncDatabaseHandle database)
    {
        ArgumentNullException.ThrowIfNull(database);
        var status = TursoSyncInterop.DatabaseTakeIoItem(database, out var item, out var errorPtr);
        ThrowIfError(status, errorPtr);
        return item == IntPtr.Zero ? null : TursoSyncIoItemHandle.FromPtr(item);
    }

    public static void StepIoCallbacks(TursoSyncDatabaseHandle database)
    {
        ArgumentNullException.ThrowIfNull(database);
        var status = TursoSyncInterop.DatabaseStepIoCallbacks(database, out var errorPtr);
        ThrowIfError(status, errorPtr);
    }

    public static TursoSyncIoKind GetIoKind(TursoSyncIoItemHandle item)
    {
        ArgumentNullException.ThrowIfNull(item);
        return TursoSyncInterop.IoRequestKind(item) switch
        {
            TursoSyncIoRequestType.None => TursoSyncIoKind.None,
            TursoSyncIoRequestType.Http => TursoSyncIoKind.Http,
            TursoSyncIoRequestType.FullRead => TursoSyncIoKind.FullRead,
            TursoSyncIoRequestType.FullWrite => TursoSyncIoKind.FullWrite,
            var value => throw new TursoException($"Unknown Turso sync I/O kind {value}."),
        };
    }

    public static TursoSyncHttpRequest GetHttpRequest(TursoSyncIoItemHandle item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var itemReferenceAdded = false;
        item.DangerousAddRef(ref itemReferenceAdded);
        try
        {
            var status = TursoSyncInterop.IoRequestHttp(item, out var request);
            ThrowIfError(status, IntPtr.Zero);
            if (request.HeaderCount < 0)
                throw new TursoException("Turso sync returned a negative HTTP header count.");

            var headers = new List<TursoSyncHttpHeader>(request.HeaderCount);
            for (var index = 0; index < request.HeaderCount; index++)
            {
                status = TursoSyncInterop.IoRequestHttpHeader(item, (nuint)index, out var header);
                ThrowIfError(status, IntPtr.Zero);
                headers.Add(new TursoSyncHttpHeader(CopyUtf8(header.Key), CopyUtf8(header.Value)));
            }

            var url = CopyUtf8(request.Url);
            return new TursoSyncHttpRequest(
                string.IsNullOrEmpty(url) ? null : url,
                CopyUtf8(request.Method),
                CopyUtf8(request.Path),
                CopyBytes(request.Body),
                headers);
        }
        finally
        {
            if (itemReferenceAdded)
                item.DangerousRelease();
        }
    }

    public static string GetFullReadPath(TursoSyncIoItemHandle item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var itemReferenceAdded = false;
        item.DangerousAddRef(ref itemReferenceAdded);
        try
        {
            var status = TursoSyncInterop.IoRequestFullRead(item, out var request);
            ThrowIfError(status, IntPtr.Zero);
            return CopyUtf8(request.Path);
        }
        finally
        {
            if (itemReferenceAdded)
                item.DangerousRelease();
        }
    }

    public static TursoSyncFullWriteRequest GetFullWriteRequest(TursoSyncIoItemHandle item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var itemReferenceAdded = false;
        item.DangerousAddRef(ref itemReferenceAdded);
        try
        {
            var status = TursoSyncInterop.IoRequestFullWrite(item, out var request);
            ThrowIfError(status, IntPtr.Zero);
            return new TursoSyncFullWriteRequest(CopyUtf8(request.Path), CopyBytes(request.Content));
        }
        finally
        {
            if (itemReferenceAdded)
                item.DangerousRelease();
        }
    }

    public static void SetIoStatus(TursoSyncIoItemHandle item, int statusCode)
    {
        ArgumentNullException.ThrowIfNull(item);
        ThrowIfError(TursoSyncInterop.IoStatus(item, statusCode), IntPtr.Zero);
    }

    public static void PushIoBuffer(TursoSyncIoItemHandle item, ReadOnlySpan<byte> buffer)
    {
        ArgumentNullException.ThrowIfNull(item);
        unsafe
        {
            fixed (byte* pointer = buffer)
            {
                var slice = new TursoSliceRef
                {
                    Pointer = (IntPtr)pointer,
                    Length = (nuint)buffer.Length,
                };
                ThrowIfError(TursoSyncInterop.IoPushBuffer(item, ref slice), IntPtr.Zero);
            }
        }
    }

    public static void PoisonIo(TursoSyncIoItemHandle item, string error)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(error);
        var bytes = Encoding.UTF8.GetBytes(error);
        unsafe
        {
            fixed (byte* pointer = bytes)
            {
                var slice = new TursoSliceRef
                {
                    Pointer = (IntPtr)pointer,
                    Length = (nuint)bytes.Length,
                };
                ThrowIfError(TursoSyncInterop.IoPoison(item, ref slice), IntPtr.Zero);
            }
        }
    }

    public static void CompleteIo(TursoSyncIoItemHandle item)
    {
        ArgumentNullException.ThrowIfNull(item);
        ThrowIfError(TursoSyncInterop.IoDone(item), IntPtr.Zero);
    }

    private delegate TursoStatusCode StartOperationDelegate(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    private static TursoSyncOperationHandle StartOperation(
        TursoSyncDatabaseHandle database,
        StartOperationDelegate start)
    {
        ArgumentNullException.ThrowIfNull(database);
        var status = start(database, out var operation, out var errorPtr);
        ThrowIfError(status, errorPtr);
        return TursoSyncOperationHandle.FromPtr(operation);
    }

    private static byte[] CopyBytes(TursoSliceRef slice)
    {
        if (slice.Length == 0)
            return [];
        if (slice.Pointer == IntPtr.Zero)
            throw new TursoException("Turso sync returned a null pointer for a non-empty slice.");
        if (slice.Length > int.MaxValue)
            throw new TursoException("Turso sync returned a slice that is too large for a managed buffer.");

        var result = new byte[(int)slice.Length];
        Marshal.Copy(slice.Pointer, result, 0, result.Length);
        return result;
    }

    private static string CopyUtf8(TursoSliceRef slice)
        => slice.Length == 0 ? string.Empty : Encoding.UTF8.GetString(CopyBytes(slice));

    private static void ThrowIfError(TursoStatusCode status, IntPtr errorPtr)
    {
        if (errorPtr != IntPtr.Zero)
            ThrowException(status, errorPtr);
        if (status == TursoStatusCode.Ok)
            return;

        throw new TursoSyncNativeException(
            (uint)status,
            $"Turso sync native call failed with status {status}.");
    }

    private static void ThrowException(TursoStatusCode status, IntPtr errorPtr)
    {
        var message = Marshal.PtrToStringUTF8(errorPtr) ?? "Internal error";
        TursoSyncInterop.FreeString(errorPtr);
        throw new TursoSyncNativeException((uint)status, message);
    }

    private static void ReleaseConnection(IntPtr connection)
    {
        _ = TursoInterop.ConnectionClose(connection, out var errorPtr);
        if (errorPtr != IntPtr.Zero)
            TursoInterop.FreeString(errorPtr);
        TursoInterop.ConnectionDeinit(connection);
    }

    private sealed class NativeUtf8String : IDisposable
    {
        private NativeUtf8String(IntPtr pointer) => Pointer = pointer;

        public IntPtr Pointer { get; private set; }

        public static NativeUtf8String From(string? value)
            => new(value is null ? IntPtr.Zero : Marshal.StringToCoTaskMemUTF8(value));

        public void Dispose()
        {
            if (Pointer == IntPtr.Zero)
                return;

            Marshal.FreeCoTaskMem(Pointer);
            Pointer = IntPtr.Zero;
        }
    }
}
