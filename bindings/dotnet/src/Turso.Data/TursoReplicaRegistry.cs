using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace Turso;

internal static class TursoReplicaRegistry
{
    private static readonly object Gate = new();
    private static readonly Dictionary<string, Entry> Entries = new(
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);

    public static async Task<Lease> AcquireAsync(
        TursoSyncDatabaseOptions options,
        bool pooling,
        TimeSpan syncInterval,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Path == ":memory:")
        {
            var database = await TursoSyncDatabase
                .CreateAsync(options, cancellationToken)
                .ConfigureAwait(false);
            return Lease.CreateStandalone(
                database,
                new TursoAutomaticSyncCoordinator(database, syncInterval, timeProvider));
        }

        var path = Path.GetFullPath(options.Path);
        var fingerprint = Fingerprint.Create(options, syncInterval);
        Entry entry;
        lock (Gate)
        {
            if (Entries.TryGetValue(path, out entry!))
            {
                if (entry.Closing)
                    throw new InvalidOperationException($"Embedded replica '{path}' is closing.");
                if (!pooling || !entry.Pooling)
                    throw new InvalidOperationException($"Embedded replica '{path}' is already open exclusively.");
                if (entry.Fingerprint != fingerprint)
                    throw new InvalidOperationException($"Embedded replica '{path}' is already open with different options.");

                checked
                {
                    entry.ReferenceCount++;
                }
            }
            else
            {
                entry = new Entry(path, pooling, fingerprint)
                {
                    ReferenceCount = 1,
                };
                entry.Initialization = InitializeAsync(entry, options, syncInterval, timeProvider);
                Entries.Add(path, entry);
            }
        }

        try
        {
            var holder = await entry.Initialization.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new Lease(entry, holder);
        }
        catch
        {
            ReleaseReference(entry);
            throw;
        }
    }

    private static async Task<Holder> InitializeAsync(
        Entry entry,
        TursoSyncDatabaseOptions options,
        TimeSpan syncInterval,
        TimeProvider timeProvider)
    {
        try
        {
            var database = await TursoSyncDatabase
                .CreateAsync(options, entry.InitializationCancellation.Token)
                .ConfigureAwait(false);
            var holder = new Holder(
                database,
                new TursoAutomaticSyncCoordinator(database, syncInterval, timeProvider));
            lock (Gate)
                entry.Holder = holder;
            return holder;
        }
        catch
        {
            lock (Gate)
            {
                if (Entries.TryGetValue(entry.Path, out var current) && ReferenceEquals(current, entry))
                    Entries.Remove(entry.Path);
            }
            throw;
        }
    }

    private static Exception? ReleaseReference(Entry entry)
    {
        Holder? holder;
        lock (Gate)
        {
            if (entry.ReferenceCount <= 0)
                throw new InvalidOperationException("Embedded replica lease ownership is unbalanced.");
            entry.ReferenceCount--;
            if (entry.ReferenceCount != 0)
            {
                var status = entry.Holder?.Coordinator.Status;
                return status?.State == TursoAutomaticSyncState.Faulted
                    ? status.LastException
                    : null;
            }

            entry.Closing = true;
            entry.InitializationCancellation.Cancel();
            holder = entry.Holder;
            if (holder is null)
            {
                _ = DisposeAfterInitializationAsync(entry);
                return null;
            }
        }

        var failure = DisposeHolder(holder);
        RemoveClosingEntry(entry);
        return failure;
    }

    private static async Task DisposeAfterInitializationAsync(Entry entry)
    {
        Holder? holder = null;
        try
        {
            holder = await entry.Initialization.ConfigureAwait(false);
        }
        catch
        {
            // Initialization failure already removed the entry.
        }

        if (holder is not null)
            _ = DisposeHolder(holder);
        RemoveClosingEntry(entry);
    }

    private static Exception? DisposeHolder(Holder holder)
    {
        var failure = holder.Coordinator.Stop();
        holder.Database.Dispose();
        return failure;
    }

    private static void RemoveClosingEntry(Entry entry)
    {
        lock (Gate)
        {
            if (Entries.TryGetValue(entry.Path, out var current) && ReferenceEquals(current, entry))
                Entries.Remove(entry.Path);
        }
        entry.InitializationCancellation.Dispose();
    }

    internal sealed class Lease : IDisposable
    {
        private readonly Entry? _entry;
        private readonly Holder _holder;
        private int _disposed;

        internal Lease(Entry? entry, Holder holder)
        {
            _entry = entry;
            _holder = holder;
        }

        public TursoSyncDatabase Database => _holder.Database;

        public TursoAutomaticSyncCoordinator Coordinator => _holder.Coordinator;

        public Exception? Release()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return null;
            if (_entry is not null)
                return ReleaseReference(_entry);

            return DisposeHolder(_holder);
        }

        public void Dispose()
        {
            _ = Release();
        }

        public static Lease CreateStandalone(
            TursoSyncDatabase database,
            TursoAutomaticSyncCoordinator coordinator)
        {
            return new Lease(null, new Holder(database, coordinator));
        }
    }

    internal sealed class Entry(string path, bool pooling, Fingerprint fingerprint)
    {
        public string Path { get; } = path;
        public bool Pooling { get; } = pooling;
        public Fingerprint Fingerprint { get; } = fingerprint;
        public Task<Holder> Initialization { get; set; } = null!;
        public Holder? Holder { get; set; }
        public int ReferenceCount { get; set; }
        public bool Closing { get; set; }
        public CancellationTokenSource InitializationCancellation { get; } = new();
    }

    internal sealed record Holder(
        TursoSyncDatabase Database,
        TursoAutomaticSyncCoordinator Coordinator);

    internal sealed record Fingerprint(
        string RemoteUri,
        string ClientName,
        long? LongPollMilliseconds,
        bool BootstrapIfEmpty,
        int? PartialPrefix,
        string? PartialQuery,
        long? PartialSegmentSize,
        bool PartialPrefetch,
        TursoRemoteEncryptionCipher? EncryptionCipher,
        string? EncryptionKeyHash,
        string? AuthTokenHash,
        long? PushOperationsThreshold,
        long? PullBytesThreshold,
        bool ForceLogicalMvccPull,
        string? ExperimentalFeatures,
        long SyncIntervalMilliseconds,
        int HttpClientIdentity)
    {
        public static Fingerprint Create(
            TursoSyncDatabaseOptions options,
            TimeSpan syncInterval)
        {
            return new Fingerprint(
                options.GetNormalizedRemoteUri().ToString(),
                options.ClientName,
                options.LongPollTimeout is null
                    ? null
                    : checked((long)options.LongPollTimeout.Value.TotalMilliseconds),
                options.BootstrapIfEmpty,
                options.PartialSync?.PrefixLength,
                options.PartialSync?.Query,
                options.PartialSync?.SegmentSize,
                options.PartialSync?.Prefetch ?? false,
                options.RemoteEncryption?.Cipher,
                HashSecret(options.RemoteEncryption?.Key),
                HashSecret(options.AuthToken),
                options.PushOperationsThreshold,
                options.PullBytesThreshold,
                options.ForceLogicalMvccPull,
                options.ExperimentalFeatures,
                checked((long)syncInterval.TotalMilliseconds),
                options.HttpClient is null ? 0 : RuntimeHelpers.GetHashCode(options.HttpClient));
        }

        private static string? HashSecret(string? value)
        {
            return string.IsNullOrEmpty(value)
                ? null
                : Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
        }
    }
}
