using System.Buffers;
using System.Net.Http.Headers;
using System.Runtime.ExceptionServices;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso;

public sealed class TursoSyncDatabase : IDisposable, IAsyncDisposable
{
    private const int IoBufferSize = 64 * 1024;

    private readonly TursoSyncDatabaseOptions _options;
    private readonly Uri _remoteUri;
    private readonly HttpClient _httpClient;
    private readonly bool _disposeHttpClient;
    private readonly TursoSyncDatabaseHandle _handle;
    private readonly SemaphoreSlim _operationLock = new(1, 1);
    private readonly AsyncLocal<int> _ioCallbackDepth = new();
    private SyncTransportContext? _lastTransportContext;
    private int _activeConnections;
    private int _disposeRequested;
    private int _resourcesDisposed;

    private TursoSyncDatabase(TursoSyncDatabaseOptions options)
    {
        options.Validate();
        _options = options;
        _remoteUri = options.GetNormalizedRemoteUri();
        _disposeHttpClient = options.HttpClient is null;
        _httpClient = options.HttpClient ?? new HttpClient
        {
            Timeout = Timeout.InfiniteTimeSpan,
        };
        try
        {
            _handle = TursoSyncBindings.NewDatabase(CreateNativeConfiguration(options, _remoteUri));
        }
        catch
        {
            if (_disposeHttpClient)
                _httpClient.Dispose();
            throw;
        }
    }

    public static TursoSyncDatabase Create(TursoSyncDatabaseOptions options)
    {
        var database = new TursoSyncDatabase(options);
        try
        {
            RunSynchronously(
                () => database.RunVoidOperationAsync(
                    TursoSyncBindings.StartCreate,
                    TursoSyncOperationKind.Create,
                    CancellationToken.None));
            return database;
        }
        catch
        {
            database.Dispose();
            throw;
        }
    }

    public static async Task<TursoSyncDatabase> CreateAsync(
        TursoSyncDatabaseOptions options,
        CancellationToken cancellationToken = default)
    {
        var database = new TursoSyncDatabase(options);
        try
        {
            await database.RunVoidOperationAsync(
                    TursoSyncBindings.StartCreate,
                    TursoSyncOperationKind.Create,
                    cancellationToken)
                .ConfigureAwait(false);
            return database;
        }
        catch
        {
            await database.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public static TursoSyncDatabase Open(TursoSyncDatabaseOptions options)
    {
        var database = new TursoSyncDatabase(options);
        try
        {
            RunSynchronously(
                () => database.RunVoidOperationAsync(
                    TursoSyncBindings.StartOpen,
                    TursoSyncOperationKind.Open,
                    CancellationToken.None));
            return database;
        }
        catch
        {
            database.Dispose();
            throw;
        }
    }

    public static async Task<TursoSyncDatabase> OpenAsync(
        TursoSyncDatabaseOptions options,
        CancellationToken cancellationToken = default)
    {
        var database = new TursoSyncDatabase(options);
        try
        {
            await database.RunVoidOperationAsync(
                    TursoSyncBindings.StartOpen,
                    TursoSyncOperationKind.Open,
                    cancellationToken)
                .ConfigureAwait(false);
            return database;
        }
        catch
        {
            await database.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public TursoConnection Connect()
        => RunSynchronously(() => ConnectAsync(CancellationToken.None));

    public async Task<TursoConnection> ConnectAsync(CancellationToken cancellationToken = default)
    {
        var connectionHandle = await ConnectHandleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return TursoConnection.CreateSyncConnection(connectionHandle, this);
        }
        catch
        {
            connectionHandle.Dispose();
            ReleaseConnection();
            throw;
        }
    }

    public bool Pull()
        => RunSynchronously(() => PullAsync(CancellationToken.None));

    public async Task<bool> PullAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _operationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            using var waitOperation = StartOperation(
                TursoSyncBindings.StartWaitChanges,
                TursoSyncOperationKind.Pull);
            await DriveOperationAsync(
                    waitOperation,
                    TursoSyncOperationKind.Pull,
                    cancellationToken)
                .ConfigureAwait(false);
            EnsureResultKind(waitOperation, TursoSyncOperationResultKind.Changes);
            using var changes = TursoSyncBindings.ExtractChanges(waitOperation);
            if (changes is null)
                return false;

            using var applyOperation = StartOperation(
                database => TursoSyncBindings.StartApplyChanges(database, changes),
                TursoSyncOperationKind.Apply);
            await DriveOperationAsync(
                    applyOperation,
                    TursoSyncOperationKind.Apply,
                    cancellationToken)
                .ConfigureAwait(false);
            EnsureResultKind(applyOperation, TursoSyncOperationResultKind.None);
            return true;
        }
        finally
        {
            _operationLock.Release();
        }
    }

    public void Push()
        => RunSynchronously(() => PushAsync(CancellationToken.None));

    public Task PushAsync(CancellationToken cancellationToken = default)
        => RunVoidOperationAsync(
            TursoSyncBindings.StartPush,
            TursoSyncOperationKind.Push,
            cancellationToken);

    public void Checkpoint()
        => RunSynchronously(() => CheckpointAsync(CancellationToken.None));

    public Task CheckpointAsync(CancellationToken cancellationToken = default)
        => RunVoidOperationAsync(
            TursoSyncBindings.StartCheckpoint,
            TursoSyncOperationKind.Checkpoint,
            cancellationToken);

    public TursoSyncStats GetStats()
        => RunSynchronously(() => GetStatsAsync(CancellationToken.None));

    public async Task<TursoSyncStats> GetStatsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _operationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            using var operation = StartOperation(
                TursoSyncBindings.StartStats,
                TursoSyncOperationKind.Stats);
            await DriveOperationAsync(
                    operation,
                    TursoSyncOperationKind.Stats,
                    cancellationToken)
                .ConfigureAwait(false);
            EnsureResultKind(operation, TursoSyncOperationResultKind.Stats);
            var stats = TursoSyncBindings.ExtractStats(operation);
            return new TursoSyncStats(
                stats.CdcOperations,
                stats.MainWalSize,
                stats.RevertWalSize,
                ToTimestamp(stats.LastPullUnixTime),
                ToTimestamp(stats.LastPushUnixTime),
                stats.NetworkSentBytes,
                stats.NetworkReceivedBytes,
                stats.Revision);
        }
        finally
        {
            _operationLock.Release();
        }
    }

    public void Dispose()
    {
        ThrowIfIoReentrant();
        if (Interlocked.Exchange(ref _disposeRequested, 1) != 0)
            return;

        if (Volatile.Read(ref _activeConnections) == 0)
            DisposeResources();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    internal void ProcessOneIo()
    {
        using var item = TursoSyncBindings.TakeIoItem(_handle);
        if (item is not null)
            RunSynchronously(() => HandleIoItemAsync(item, CancellationToken.None));
        TursoSyncBindings.StepIoCallbacks(_handle);
    }

    internal IDisposable EnterConnectionOperation()
    {
        ThrowIfIoReentrant();
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _resourcesDisposed) != 0, this);
        _operationLock.Wait();
        return new ConnectionOperationLease(_operationLock);
    }

    internal async ValueTask<IDisposable> EnterConnectionOperationAsync(CancellationToken cancellationToken)
    {
        ThrowIfIoReentrant();
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _resourcesDisposed) != 0, this);
        await _operationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new ConnectionOperationLease(_operationLock);
    }

    internal void ThrowIfIoReentrant()
    {
        if (_ioCallbackDepth.Value != 0)
            throw new InvalidOperationException("Turso sync operations cannot be reentered from the sync HTTP handler.");
    }

    internal async Task<TursoDatabaseHandle> ConnectHandleAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await _operationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            using var operation = StartOperation(
                TursoSyncBindings.StartConnect,
                TursoSyncOperationKind.Connect);
            await DriveOperationAsync(
                    operation,
                    TursoSyncOperationKind.Connect,
                    cancellationToken)
                .ConfigureAwait(false);
            EnsureResultKind(operation, TursoSyncOperationResultKind.Connection);
            var connectionHandle = TursoSyncBindings.ExtractConnection(_handle, operation);
            Interlocked.Increment(ref _activeConnections);
            return connectionHandle;
        }
        finally
        {
            _operationLock.Release();
        }
    }

    internal void ReleaseConnection()
    {
        var remaining = Interlocked.Decrement(ref _activeConnections);
        if (remaining < 0)
            throw new InvalidOperationException("Turso sync connection ownership is unbalanced.");
        if (remaining == 0 && Volatile.Read(ref _disposeRequested) != 0)
            DisposeResources();
    }

    private async Task RunVoidOperationAsync(
        Func<TursoSyncDatabaseHandle, TursoSyncOperationHandle> start,
        TursoSyncOperationKind operationKind,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await _operationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            using var operation = StartOperation(start, operationKind);
            await DriveOperationAsync(operation, operationKind, cancellationToken).ConfigureAwait(false);
            EnsureResultKind(operation, TursoSyncOperationResultKind.None);
        }
        finally
        {
            _operationLock.Release();
        }
    }

    private async Task DriveOperationAsync(
        TursoSyncOperationHandle operation,
        TursoSyncOperationKind operationKind,
        CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                switch (TursoSyncBindings.Resume(operation))
                {
                    case TursoSyncOperationState.Done:
                        return;
                    case TursoSyncOperationState.Continue:
                        continue;
                    case TursoSyncOperationState.Io:
                        await ProcessIoQueueAsync(cancellationToken).ConfigureAwait(false);
                        break;
                    default:
                        throw new InvalidOperationException("Unknown Turso sync operation state.");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            try
            {
                CancelQueuedIo();
            }
            catch
            {
                // Preserve cancellation after making the best effort to release native callbacks.
            }
            throw;
        }
        catch (TursoSyncException)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw CreateSyncException(operationKind, exception);
        }
    }

    private TursoSyncOperationHandle StartOperation(
        Func<TursoSyncDatabaseHandle, TursoSyncOperationHandle> start,
        TursoSyncOperationKind operationKind)
    {
        _lastTransportContext = null;
        try
        {
            return start(_handle);
        }
        catch (TursoSyncException)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw CreateSyncException(operationKind, exception);
        }
    }

    private async Task ProcessIoQueueAsync(CancellationToken cancellationToken)
    {
        ExceptionDispatchInfo? failure = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var item = TursoSyncBindings.TakeIoItem(_handle);
            if (item is null)
                break;

            if (failure is null)
            {
                try
                {
                    await HandleIoItemAsync(item, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    failure = ExceptionDispatchInfo.Capture(exception);
                }
            }
            else
            {
                try
                {
                    PoisonAndCompleteIo(item, RedactSecrets(failure.SourceException.Message));
                }
                catch
                {
                    // Keep draining queued work and preserve the first I/O failure.
                }
            }
        }

        try
        {
            TursoSyncBindings.StepIoCallbacks(_handle);
        }
        catch (Exception exception) when (failure is null)
        {
            failure = ExceptionDispatchInfo.Capture(exception);
        }
        catch
        {
            // Preserve the first I/O failure.
        }

        failure?.Throw();
    }

    private void CancelQueuedIo()
    {
        ExceptionDispatchInfo? failure = null;
        while (true)
        {
            using var item = TursoSyncBindings.TakeIoItem(_handle);
            if (item is null)
                break;
            try
            {
                PoisonAndCompleteIo(item, "Turso sync operation was canceled.");
            }
            catch (Exception exception) when (failure is null)
            {
                failure = ExceptionDispatchInfo.Capture(exception);
            }
            catch
            {
                // Keep draining queued work and preserve the first cleanup failure.
            }
        }

        try
        {
            TursoSyncBindings.StepIoCallbacks(_handle);
        }
        catch (Exception exception) when (failure is null)
        {
            failure = ExceptionDispatchInfo.Capture(exception);
        }
        catch
        {
            // Preserve the first cleanup failure.
        }

        failure?.Throw();
    }

    private static void PoisonAndCompleteIo(TursoSyncIoItemHandle item, string error)
    {
        ExceptionDispatchInfo? failure = null;
        try
        {
            TursoSyncBindings.PoisonIo(item, error);
        }
        catch (Exception exception)
        {
            failure = ExceptionDispatchInfo.Capture(exception);
        }

        try
        {
            TursoSyncBindings.CompleteIo(item);
        }
        catch when (failure is not null)
        {
            // Preserve the first cleanup failure.
        }

        failure?.Throw();
    }

    private async Task HandleIoItemAsync(
        TursoSyncIoItemHandle item,
        CancellationToken cancellationToken)
    {
        ExceptionDispatchInfo? failure = null;
        try
        {
            switch (TursoSyncBindings.GetIoKind(item))
            {
                case TursoSyncIoKind.None:
                    break;
                case TursoSyncIoKind.Http:
                    await HandleHttpAsync(item, cancellationToken).ConfigureAwait(false);
                    break;
                case TursoSyncIoKind.FullRead:
                    await HandleFullReadAsync(item, cancellationToken).ConfigureAwait(false);
                    break;
                case TursoSyncIoKind.FullWrite:
                    await HandleFullWriteAsync(item, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidOperationException("Unknown Turso sync I/O request.");
            }
        }
        catch (Exception exception)
        {
            failure = ExceptionDispatchInfo.Capture(exception);
            try
            {
                TursoSyncBindings.PoisonIo(item, RedactSecrets(exception.Message));
            }
            catch
            {
                // Preserve the I/O failure that caused poisoning.
            }
        }

        try
        {
            TursoSyncBindings.CompleteIo(item);
        }
        catch when (failure is not null)
        {
            // Preserve the original I/O failure.
        }

        failure?.Throw();
    }

    private async Task HandleHttpAsync(
        TursoSyncIoItemHandle item,
        CancellationToken cancellationToken)
    {
        var previousDepth = _ioCallbackDepth.Value;
        _ioCallbackDepth.Value = previousDepth + 1;
        try
        {
            await HandleHttpCoreAsync(item, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ioCallbackDepth.Value = previousDepth;
        }
    }

    private async Task HandleHttpCoreAsync(
        TursoSyncIoItemHandle item,
        CancellationToken cancellationToken)
    {
        var request = TursoSyncBindings.GetHttpRequest(item);
        var baseUri = request.Url is null ? _remoteUri : NormalizeRemoteUri(new Uri(request.Url, UriKind.Absolute));
        var requestUri = CombineUri(baseUri, request.Path);
        ValidateAuthTransport(requestUri, _remoteUri, _options.AuthToken);
        _lastTransportContext = new SyncTransportContext(
            request.Method,
            requestUri,
            StatusCode: null);

        using var message = new HttpRequestMessage(new HttpMethod(request.Method), requestUri);
        if (request.Body.Length > 0)
            message.Content = new ByteArrayContent(request.Body);
        foreach (var header in request.Headers)
        {
            if (message.Headers.TryAddWithoutValidation(header.Name, header.Value))
                continue;

            message.Content ??= new ByteArrayContent([]);
            if (!message.Content.Headers.TryAddWithoutValidation(header.Name, header.Value))
                throw new InvalidOperationException($"Invalid sync HTTP header: {header.Name}");
        }
        if (!string.IsNullOrWhiteSpace(_options.AuthToken))
            message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _options.AuthToken);

        using var response = await _httpClient
            .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        _lastTransportContext = _lastTransportContext with
        {
            StatusCode = response.StatusCode,
        };
        TursoSyncBindings.SetIoStatus(item, (int)response.StatusCode);

        await using var responseStream = await response.Content
            .ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);
        var buffer = ArrayPool<byte>.Shared.Rent(IoBufferSize);
        try
        {
            while (true)
            {
                var count = await responseStream
                    .ReadAsync(buffer.AsMemory(0, IoBufferSize), cancellationToken)
                    .ConfigureAwait(false);
                if (count == 0)
                    break;
                TursoSyncBindings.PushIoBuffer(item, buffer.AsSpan(0, count));
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static Task HandleFullReadAsync(
        TursoSyncIoItemHandle item,
        CancellationToken cancellationToken)
    {
        var path = TursoSyncBindings.GetFullReadPath(item);
        FileStream stream;
        try
        {
            stream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                IoBufferSize,
                FileOptions.SequentialScan);
        }
        catch (FileNotFoundException)
        {
            return Task.CompletedTask;
        }
        catch (DirectoryNotFoundException)
        {
            return Task.CompletedTask;
        }

        using (stream)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(IoBufferSize);
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var count = stream.Read(buffer, 0, IoBufferSize);
                    if (count == 0)
                        break;
                    TursoSyncBindings.PushIoBuffer(item, buffer.AsSpan(0, count));
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        return Task.CompletedTask;
    }

    private static Task HandleFullWriteAsync(
        TursoSyncIoItemHandle item,
        CancellationToken cancellationToken)
    {
        var request = TursoSyncBindings.GetFullWriteRequest(item);
        var directory = Path.GetDirectoryName(Path.GetFullPath(request.Path));
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var temporaryPath = request.Path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            using (var stream = new FileStream(
                       temporaryPath,
                       FileMode.CreateNew,
                       FileAccess.Write,
                       FileShare.None,
                       IoBufferSize,
                       FileOptions.WriteThrough))
            {
                stream.Write(request.Content);
                stream.Flush(flushToDisk: true);
            }

            File.Move(temporaryPath, request.Path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temporaryPath))
                File.Delete(temporaryPath);
        }

        return Task.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        ThrowIfIoReentrant();
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeRequested) != 0, this);
    }

    private void DisposeResources()
    {
        _operationLock.Wait();
        try
        {
            if (Volatile.Read(ref _activeConnections) != 0
                || Interlocked.Exchange(ref _resourcesDisposed, 1) != 0)
            {
                return;
            }

            _handle.Dispose();
            if (_disposeHttpClient)
                _httpClient.Dispose();
        }
        finally
        {
            _operationLock.Release();
        }
    }

    internal static void ValidateAuthTransport(
        Uri requestUri,
        Uri configuredRemoteUri,
        string? authToken)
    {
        if (string.IsNullOrWhiteSpace(authToken))
            return;
        if (!HasSameOrigin(requestUri, configuredRemoteUri))
        {
            throw new InvalidOperationException(
                "Refusing to send the sync auth token to an origin other than the configured remote.");
        }
        if (requestUri.Scheme != Uri.UriSchemeHttps && !requestUri.IsLoopback)
        {
            throw new InvalidOperationException(
                "Auth Token requires HTTPS sync requests unless the host is localhost or loopback.");
        }
    }

    private static bool HasSameOrigin(Uri left, Uri right)
    {
        return left.Scheme.Equals(right.Scheme, StringComparison.OrdinalIgnoreCase)
               && left.IdnHost.Equals(right.IdnHost, StringComparison.OrdinalIgnoreCase)
               && left.Port == right.Port;
    }

    private static Uri NormalizeRemoteUri(Uri uri)
    {
        var scheme = uri.Scheme.ToLowerInvariant() switch
        {
            "turso" or "libsql" => Uri.UriSchemeHttps,
            "http" => Uri.UriSchemeHttp,
            "https" => Uri.UriSchemeHttps,
            _ => throw new InvalidOperationException($"Unsupported sync URL scheme: {uri.Scheme}"),
        };
        return new UriBuilder(uri)
        {
            Scheme = scheme,
            Port = uri.IsDefaultPort ? -1 : uri.Port,
            UserName = string.Empty,
            Password = string.Empty,
        }.Uri;
    }

    private static Uri CombineUri(Uri baseUri, string path)
    {
        var baseText = baseUri.GetLeftPart(UriPartial.Path).TrimEnd('/');
        return new Uri(baseText + "/" + path.TrimStart('/'), UriKind.Absolute);
    }

    internal static TursoSyncDatabaseConfiguration CreateNativeConfiguration(
        TursoSyncDatabaseOptions options,
        Uri remoteUri)
    {
        var partial = options.PartialSync;
        var encryption = options.RemoteEncryption;
        return new TursoSyncDatabaseConfiguration
        {
            Path = options.Path,
            RemoteUrl = remoteUri.ToString(),
            ClientName = options.ClientName,
            LongPollTimeoutMilliseconds = options.LongPollTimeout is null
                ? 0
                : checked((int)options.LongPollTimeout.Value.TotalMilliseconds),
            BootstrapIfEmpty = options.BootstrapIfEmpty,
            ReservedBytes = encryption?.ReservedBytes ?? 0,
            PartialBootstrapStrategyPrefix = partial?.PrefixLength ?? 0,
            PartialBootstrapStrategyQuery = partial?.Query,
            PartialBootstrapSegmentSize = partial?.SegmentSize is null ? 0 : (nuint)partial.SegmentSize.Value,
            PartialBootstrapPrefetch = partial?.Prefetch ?? false,
            RemoteEncryptionKey = encryption?.Key,
            RemoteEncryptionCipher = encryption?.NativeName,
            PushOperationsThreshold = options.PushOperationsThreshold is null
                ? 0
                : (nuint)options.PushOperationsThreshold.Value,
            PullBytesThreshold = options.PullBytesThreshold is null
                ? 0
                : (nuint)options.PullBytesThreshold.Value,
            LogicalMvccPull = options.ForceLogicalMvccPull,
            ExperimentalFeatures = options.ExperimentalFeatures,
        };
    }

    private static void EnsureResultKind(
        TursoSyncOperationHandle operation,
        TursoSyncOperationResultKind expected)
    {
        var actual = TursoSyncBindings.GetResultKind(operation);
        if (actual != expected)
            throw new InvalidOperationException($"Expected Turso sync result {expected}, got {actual}.");
    }

    private static DateTimeOffset? ToTimestamp(long value)
        => value <= 0 ? null : DateTimeOffset.FromUnixTimeSeconds(value);

    private TursoSyncException CreateSyncException(
        TursoSyncOperationKind operation,
        Exception exception)
    {
        var transport = _lastTransportContext;
        var message = RedactSecrets(exception.Message);
        var innerException = ContainsSecret(exception.ToString())
            ? new Exception(message)
            : exception;
        return new TursoSyncException(
            operation,
            $"Turso sync {operation} failed: {message}",
            (exception as TursoSyncNativeException)?.StatusCode,
            transport?.Method,
            transport?.Endpoint,
            transport?.StatusCode,
            innerException);
    }

    private string RedactSecrets(string message)
    {
        return string.IsNullOrWhiteSpace(_options.AuthToken)
            ? message
            : message.Replace(_options.AuthToken, "[REDACTED]", StringComparison.Ordinal);
    }

    private bool ContainsSecret(string message)
    {
        return !string.IsNullOrWhiteSpace(_options.AuthToken)
               && message.Contains(_options.AuthToken, StringComparison.Ordinal);
    }

    private static void RunSynchronously(Func<Task> operation)
        => Task.Run(operation).GetAwaiter().GetResult();

    private static T RunSynchronously<T>(Func<Task<T>> operation)
        => Task.Run(operation).GetAwaiter().GetResult();

    private sealed record SyncTransportContext(
        string Method,
        Uri Endpoint,
        System.Net.HttpStatusCode? StatusCode);

    private sealed class ConnectionOperationLease(SemaphoreSlim operationLock) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                operationLock.Release();
        }
    }
}
