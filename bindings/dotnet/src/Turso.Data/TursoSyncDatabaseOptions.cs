namespace Turso;

public enum TursoRemoteEncryptionCipher
{
    Aes256Gcm,
    Aes128Gcm,
    ChaCha20Poly1305,
    Aegis128L,
    Aegis128X2,
    Aegis128X4,
    Aegis256,
    Aegis256X2,
    Aegis256X4,
}

public sealed class TursoRemoteEncryptionOptions
{
    public required string Key { get; init; }
    public required TursoRemoteEncryptionCipher Cipher { get; init; }

    internal int ReservedBytes => Cipher switch
    {
        TursoRemoteEncryptionCipher.Aes256Gcm
            or TursoRemoteEncryptionCipher.Aes128Gcm
            or TursoRemoteEncryptionCipher.ChaCha20Poly1305 => 28,
        TursoRemoteEncryptionCipher.Aegis128L
            or TursoRemoteEncryptionCipher.Aegis128X2
            or TursoRemoteEncryptionCipher.Aegis128X4 => 32,
        TursoRemoteEncryptionCipher.Aegis256
            or TursoRemoteEncryptionCipher.Aegis256X2
            or TursoRemoteEncryptionCipher.Aegis256X4 => 48,
        _ => throw new ArgumentOutOfRangeException(nameof(Cipher), Cipher, null),
    };

    internal string NativeName => Cipher switch
    {
        TursoRemoteEncryptionCipher.Aes256Gcm => "aes256gcm",
        TursoRemoteEncryptionCipher.Aes128Gcm => "aes128gcm",
        TursoRemoteEncryptionCipher.ChaCha20Poly1305 => "chacha20poly1305",
        TursoRemoteEncryptionCipher.Aegis128L => "aegis128l",
        TursoRemoteEncryptionCipher.Aegis128X2 => "aegis128x2",
        TursoRemoteEncryptionCipher.Aegis128X4 => "aegis128x4",
        TursoRemoteEncryptionCipher.Aegis256 => "aegis256",
        TursoRemoteEncryptionCipher.Aegis256X2 => "aegis256x2",
        TursoRemoteEncryptionCipher.Aegis256X4 => "aegis256x4",
        _ => throw new ArgumentOutOfRangeException(nameof(Cipher), Cipher, null),
    };

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Key);
        _ = ReservedBytes;
    }
}

public sealed class TursoPartialSyncOptions
{
    public int? PrefixLength { get; init; }
    public string? Query { get; init; }
    public long? SegmentSize { get; init; }
    public bool Prefetch { get; init; }

    internal void Validate()
    {
        if (Query is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(Query);

        var hasPrefix = PrefixLength.HasValue;
        var hasQuery = Query is not null;
        if (hasPrefix == hasQuery)
            throw new InvalidOperationException("Partial sync requires exactly one prefix or query bootstrap strategy.");
        if (PrefixLength is <= 0)
            throw new ArgumentOutOfRangeException(nameof(PrefixLength), PrefixLength, "Prefix length must be positive.");
        if (SegmentSize is <= 0)
            throw new ArgumentOutOfRangeException(nameof(SegmentSize), SegmentSize, "Segment size must be positive.");
        if (SegmentSize is { } segmentSize && (ulong)segmentSize > nuint.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(SegmentSize), SegmentSize, "Segment size exceeds the native platform size.");
    }
}

public sealed class TursoSyncDatabaseOptions
{
    public TursoSyncDatabaseOptions(string path, Uri remoteUri)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(remoteUri);
        Path = path;
        RemoteUri = remoteUri;
    }

    public string Path { get; }
    public Uri RemoteUri { get; }
    public string? AuthToken { get; init; }
    public string ClientName { get; init; } = "turso-sync-dotnet";
    public TimeSpan? LongPollTimeout { get; init; }
    public bool BootstrapIfEmpty { get; init; } = true;
    public TursoPartialSyncOptions? PartialSync { get; init; }
    public TursoRemoteEncryptionOptions? RemoteEncryption { get; init; }
    public long? PushOperationsThreshold { get; init; }
    public long? PullBytesThreshold { get; init; }
    public bool ForceLogicalMvccPull { get; init; }
    public HttpClient? HttpClient { get; init; }
    public string? ExperimentalFeatures { get; init; }

    internal Uri GetNormalizedRemoteUri()
    {
        if (!RemoteUri.IsAbsoluteUri)
            throw new ArgumentException("The sync remote URL must be absolute.", nameof(RemoteUri));
        if (!string.IsNullOrEmpty(RemoteUri.Query) || !string.IsNullOrEmpty(RemoteUri.Fragment))
            throw new ArgumentException("The sync remote URL must not include a query string or fragment.", nameof(RemoteUri));
        if (!string.IsNullOrEmpty(RemoteUri.UserInfo))
            throw new ArgumentException("Use AuthToken instead of embedding credentials in the sync URL.", nameof(RemoteUri));
        if (string.IsNullOrEmpty(RemoteUri.Host))
            throw new ArgumentException("The sync remote URL must include a host.", nameof(RemoteUri));

        var scheme = RemoteUri.Scheme.ToLowerInvariant() switch
        {
            "turso" or "libsql" => Uri.UriSchemeHttps,
            "http" => Uri.UriSchemeHttp,
            "https" => Uri.UriSchemeHttps,
            _ => throw new ArgumentException(
                "The sync remote URL must use turso, libsql, HTTP, or HTTPS.",
                nameof(RemoteUri)),
        };
        var builder = new UriBuilder(RemoteUri)
        {
            Scheme = scheme,
            Port = RemoteUri.IsDefaultPort ? -1 : RemoteUri.Port,
            UserName = string.Empty,
            Password = string.Empty,
        };
        return builder.Uri;
    }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Path);
        ArgumentException.ThrowIfNullOrWhiteSpace(ClientName);

        var normalizedUri = GetNormalizedRemoteUri();
        if (!string.IsNullOrWhiteSpace(AuthToken)
            && normalizedUri.Scheme != Uri.UriSchemeHttps
            && !normalizedUri.IsLoopback)
        {
            throw new InvalidOperationException(
                "Auth Token requires an HTTPS sync URL unless the host is localhost or loopback.");
        }

        if (LongPollTimeout is { } timeout
            && (timeout < TimeSpan.FromMilliseconds(1) || timeout.TotalMilliseconds > int.MaxValue))
        {
            throw new ArgumentOutOfRangeException(
                nameof(LongPollTimeout),
                timeout,
                $"Long-poll timeout must be between 1 and {int.MaxValue} milliseconds.");
        }

        ValidateNativeSize(PushOperationsThreshold, nameof(PushOperationsThreshold));
        ValidateNativeSize(PullBytesThreshold, nameof(PullBytesThreshold));
        PartialSync?.Validate();

        if (PartialSync is not null && !BootstrapIfEmpty)
            throw new InvalidOperationException("Partial sync requires BootstrapIfEmpty=True.");
        if (PartialSync is not null && RemoteEncryption is not null)
            throw new InvalidOperationException("Partial sync cannot be combined with remote encryption.");
        if (PartialSync?.Query is not null && PullBytesThreshold.HasValue)
        {
            throw new InvalidOperationException(
                "PullBytesThreshold cannot be combined with query partial bootstrap.");
        }
        if (PartialSync is not null && OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Partial sync on Windows requires native sparse-file hole detection that is not yet implemented.");
        }

        RemoteEncryption?.Validate();
    }

    private static void ValidateNativeSize(long? value, string parameterName)
    {
        if (value is null)
            return;
        if (value <= 0)
            throw new ArgumentOutOfRangeException(parameterName, value, "The value must be positive.");
        if ((ulong)value > nuint.MaxValue)
            throw new ArgumentOutOfRangeException(parameterName, value, "The value exceeds the native platform size.");
    }
}

public sealed record TursoSyncStats(
    long CdcOperations,
    long MainWalSize,
    long RevertWalSize,
    DateTimeOffset? LastPullTime,
    DateTimeOffset? LastPushTime,
    long NetworkSentBytes,
    long NetworkReceivedBytes,
    string Revision);
