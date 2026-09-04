using System.Runtime.InteropServices;
using Turso.Raw.Public.Handles;

namespace Turso.Raw;

internal enum TursoSyncIoRequestType
{
    None = 0,
    Http = 1,
    FullRead = 2,
    FullWrite = 3,
}

internal enum TursoSyncOperationResultType
{
    None = 0,
    Connection = 1,
    Changes = 2,
    Stats = 3,
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoSliceRef
{
    public IntPtr Pointer;
    public nuint Length;
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncDatabaseConfigNative
{
    public IntPtr Path;
    public IntPtr RemoteUrl;
    public IntPtr ClientName;
    public int LongPollTimeoutMs;
    [MarshalAs(UnmanagedType.I1)]
    public bool BootstrapIfEmpty;
    public int ReservedBytes;
    public int PartialBootstrapStrategyPrefix;
    public IntPtr PartialBootstrapStrategyQuery;
    public nuint PartialBootstrapSegmentSize;
    [MarshalAs(UnmanagedType.I1)]
    public bool PartialBootstrapPrefetch;
    public IntPtr RemoteEncryptionKey;
    public IntPtr RemoteEncryptionCipher;
    public nuint PushOperationsThreshold;
    public nuint PullBytesThreshold;
    [MarshalAs(UnmanagedType.I1)]
    public bool LogicalMvccPull;
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncHttpRequestNative
{
    public TursoSliceRef Url;
    public TursoSliceRef Method;
    public TursoSliceRef Path;
    public TursoSliceRef Body;
    public int HeaderCount;
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncHttpHeaderNative
{
    public TursoSliceRef Key;
    public TursoSliceRef Value;
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncFullReadRequestNative
{
    public TursoSliceRef Path;
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncFullWriteRequestNative
{
    public TursoSliceRef Path;
    public TursoSliceRef Content;
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncStatsNative
{
    public long CdcOperations;
    public long MainWalSize;
    public long RevertWalSize;
    public long LastPullUnixTime;
    public long LastPushUnixTime;
    public long NetworkSentBytes;
    public long NetworkReceivedBytes;
    public TursoSliceRef Revision;
}

internal static class TursoSyncInterop
{
    internal const string DllName = "turso_sdk_kit";

    static TursoSyncInterop()
    {
        TursoNativeLibraryResolver.EnsureInitialized();
    }

    [DllImport(DllName, EntryPoint = "turso_sync_database_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseNew(
        ref TursoDatabaseConfig databaseConfig,
        ref TursoSyncDatabaseConfigNative syncConfig,
        out IntPtr database,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_open", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseOpen(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_create", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseCreate(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_connect", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseConnect(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_stats", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseStats(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_checkpoint", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseCheckpoint(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_push_changes", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabasePushChanges(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_wait_changes", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseWaitChanges(
        TursoSyncDatabaseHandle database,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_apply_changes", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseApplyChanges(
        IntPtr database,
        IntPtr changes,
        out IntPtr operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_operation_resume", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode OperationResume(
        TursoSyncOperationHandle operation,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_operation_result_kind", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoSyncOperationResultType OperationResultKind(
        TursoSyncOperationHandle operation);

    [DllImport(DllName, EntryPoint = "turso_sync_operation_result_extract_connection", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode OperationExtractConnection(
        TursoSyncOperationHandle operation,
        out IntPtr connection);

    [DllImport(DllName, EntryPoint = "turso_sync_operation_result_extract_changes", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode OperationExtractChanges(
        TursoSyncOperationHandle operation,
        out IntPtr changes);

    [DllImport(DllName, EntryPoint = "turso_sync_operation_result_extract_stats", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode OperationExtractStats(
        TursoSyncOperationHandle operation,
        out TursoSyncStatsNative stats);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_take_item", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseTakeIoItem(
        TursoSyncDatabaseHandle database,
        out IntPtr item,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_step_callbacks", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode DatabaseStepIoCallbacks(
        TursoSyncDatabaseHandle database,
        out IntPtr errorPtr);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_request_kind", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoSyncIoRequestType IoRequestKind(TursoSyncIoItemHandle item);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_request_http", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoRequestHttp(
        TursoSyncIoItemHandle item,
        out TursoSyncHttpRequestNative request);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_request_http_header", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoRequestHttpHeader(
        TursoSyncIoItemHandle item,
        nuint index,
        out TursoSyncHttpHeaderNative header);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_request_full_read", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoRequestFullRead(
        TursoSyncIoItemHandle item,
        out TursoSyncFullReadRequestNative request);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_request_full_write", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoRequestFullWrite(
        TursoSyncIoItemHandle item,
        out TursoSyncFullWriteRequestNative request);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_poison", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoPoison(
        TursoSyncIoItemHandle item,
        ref TursoSliceRef error);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_status", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoStatus(TursoSyncIoItemHandle item, int status);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_push_buffer", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoPushBuffer(
        TursoSyncIoItemHandle item,
        ref TursoSliceRef buffer);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_done", CallingConvention = CallingConvention.Cdecl)]
    internal static extern TursoStatusCode IoDone(TursoSyncIoItemHandle item);

    [DllImport(DllName, EntryPoint = "turso_sync_database_deinit", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DatabaseDeinit(IntPtr database);

    [DllImport(DllName, EntryPoint = "turso_sync_operation_deinit", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void OperationDeinit(IntPtr operation);

    [DllImport(DllName, EntryPoint = "turso_sync_database_io_item_deinit", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void IoItemDeinit(IntPtr item);

    [DllImport(DllName, EntryPoint = "turso_sync_changes_deinit", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ChangesDeinit(IntPtr changes);

    [DllImport(DllName, EntryPoint = "turso_str_deinit", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FreeString(IntPtr stringPtr);
}
