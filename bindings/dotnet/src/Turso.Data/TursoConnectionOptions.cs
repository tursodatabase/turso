using System.Globalization;
using Turso.Raw.Public.Value;

namespace Turso;

public class TursoConnectionOptions
{
    private const int MaximumSyncIntervalSeconds = 4_294_967;

    private readonly TursoConnectionStringBuilder _builder;

    private TursoConnectionOptions(TursoConnectionStringBuilder builder)
    {
        _builder = builder;
    }

    public string GetConnectionString() => _builder.ConnectionString;

    public string? this[string keyword]
    {
        get => _builder.GetOption(keyword);
        set => _builder[keyword] = value ?? string.Empty;
    }

    public int DefaultTimeout => _builder.DefaultTimeout;

    public bool Pooling => _builder.Pooling;

    public string DataSource => _builder.DataSource;

    public string AuthToken => _builder.AuthToken;

    public string ReplicaPath => _builder.ReplicaPath;

    public bool ReadYourWrites => _builder.ReadYourWrites;

    public int SyncInterval
    {
        get
        {
            var value = _builder.SyncInterval;
            if (value is < 0 or > MaximumSyncIntervalSeconds)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(SyncInterval),
                    value,
                    $"Sync Interval must be between 0 and {MaximumSyncIntervalSeconds} seconds.");
            }

            return value;
        }
    }

    public string SyncClientName => _builder.SyncClientName;

    public int SyncLongPollTimeout => _builder.SyncLongPollTimeout;

    public bool BootstrapIfEmpty => _builder.BootstrapIfEmpty;

    public int PartialBootstrapPrefix => _builder.PartialBootstrapPrefix;

    public string PartialBootstrapQuery => _builder.PartialBootstrapQuery;

    public long PartialSyncSegmentSize => _builder.PartialSyncSegmentSize;

    public bool PartialSyncPrefetch => _builder.PartialSyncPrefetch;

    public string RemoteEncryptionCipher => _builder.RemoteEncryptionCipher;

    public string RemoteEncryptionKey => _builder.RemoteEncryptionKey;

    public long PushOperationsThreshold => _builder.PushOperationsThreshold;

    public long PullBytesThreshold => _builder.PullBytesThreshold;

    public bool ForceLogicalMvccPull => _builder.ForceLogicalMvccPull;

    public string SyncExperimentalFeatures => _builder.SyncExperimentalFeatures;

    public bool? Tls => _builder.Tls;

    public bool IsRemote => IsRemoteDataSource(DataSource);

    public bool IsReplica => IsRemote && !string.IsNullOrWhiteSpace(ReplicaPath);

    public bool HasAdvancedReplicaOptions =>
        HasOption("Sync Client Name")
        || HasOption("Sync Long Poll Timeout")
        || HasOption("Bootstrap If Empty")
        || HasOption("Partial Bootstrap Prefix")
        || HasOption("Partial Bootstrap Query")
        || HasOption("Partial Sync Segment Size")
        || HasOption("Partial Sync Prefetch")
        || HasOption("Remote Encryption Cipher")
        || HasOption("Remote Encryption Key")
        || HasOption("Push Operations Threshold")
        || HasOption("Pull Bytes Threshold")
        || HasOption("Force Logical MVCC Pull")
        || HasOption("Sync Experimental Features");

    public bool HasPartialSyncOptions =>
        HasOption("Partial Bootstrap Prefix")
        || HasOption("Partial Bootstrap Query")
        || HasOption("Partial Sync Segment Size")
        || HasOption("Partial Sync Prefetch");

    public TursoEncryptionCipher? GetEncryptionCipher() => _builder.GetEncryptionCipher();

    public TursoRemoteEncryptionOptions? GetRemoteEncryption()
    {
        var cipher = RemoteEncryptionCipher;
        var key = RemoteEncryptionKey;
        if (string.IsNullOrWhiteSpace(cipher) && string.IsNullOrWhiteSpace(key))
            return null;
        if (string.IsNullOrWhiteSpace(cipher) || string.IsNullOrWhiteSpace(key))
        {
            throw new InvalidOperationException(
                "Remote Encryption Cipher and Remote Encryption Key must be specified together.");
        }

        return new TursoRemoteEncryptionOptions
        {
            Cipher = TursoRemoteEncryptionOptions.ParseCipher(cipher),
            Key = key,
        };
    }

    public Uri GetRemoteUri()
    {
        if (!Uri.TryCreate(DataSource, UriKind.Absolute, out var uri) || !IsRemoteScheme(uri.Scheme))
            throw new InvalidOperationException($"Data Source is not a remote Turso URL: {DataSource}");

        if (!string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment))
            throw new InvalidOperationException("Remote Turso URLs must not include query strings or fragments.");
        if (!string.IsNullOrEmpty(uri.UserInfo))
            throw new InvalidOperationException("Remote Turso URLs must not include embedded user information; use Auth Token instead.");
        if (string.IsNullOrEmpty(uri.Host))
            throw new InvalidOperationException("Remote Turso URLs must include a host.");

        var scheme = uri.Scheme.ToLowerInvariant() switch
        {
            "libsql" => Tls == false ? "http" : "https",
            "turso" => ValidateTls(uri.Scheme, expectedTls: true, normalizedScheme: "https"),
            "http" => ValidateTls(uri.Scheme, expectedTls: false),
            "https" => ValidateTls(uri.Scheme, expectedTls: true),
            "ws" => ValidateTls(uri.Scheme, expectedTls: false, normalizedScheme: "http"),
            "wss" => ValidateTls(uri.Scheme, expectedTls: true, normalizedScheme: "https"),
            _ => throw new InvalidOperationException($"Unsupported remote Turso URL scheme: {uri.Scheme}")
        };

        var builder = new UriBuilder(uri)
        {
            Scheme = scheme,
            Port = uri.IsDefaultPort ? -1 : uri.Port,
            UserName = string.Empty,
            Password = string.Empty,
        };

        return builder.Uri;
    }

    public static TursoConnectionOptions Parse(string connectionString)
    {
        return new TursoConnectionOptions(new TursoConnectionStringBuilder(connectionString));
    }

    private bool HasOption(string keyword) => _builder.ContainsKey(keyword);

    private static bool IsRemoteDataSource(string dataSource)
    {
        return Uri.TryCreate(dataSource, UriKind.Absolute, out var uri)
               && IsRemoteScheme(uri.Scheme);
    }

    private static bool IsRemoteScheme(string scheme)
    {
        return scheme.Equals("libsql", StringComparison.OrdinalIgnoreCase)
               || scheme.Equals("turso", StringComparison.OrdinalIgnoreCase)
               || scheme.Equals("http", StringComparison.OrdinalIgnoreCase)
               || scheme.Equals("https", StringComparison.OrdinalIgnoreCase)
               || scheme.Equals("ws", StringComparison.OrdinalIgnoreCase)
               || scheme.Equals("wss", StringComparison.OrdinalIgnoreCase);
    }

    private string ValidateTls(string scheme, bool expectedTls, string? normalizedScheme = null)
    {
        if (Tls.HasValue && Tls.Value != expectedTls)
        {
            var actual = Tls.Value.ToString(CultureInfo.InvariantCulture);
            throw new InvalidOperationException($"Tls={actual} conflicts with the {scheme} URL scheme.");
        }

        return normalizedScheme ?? scheme;
    }
}
