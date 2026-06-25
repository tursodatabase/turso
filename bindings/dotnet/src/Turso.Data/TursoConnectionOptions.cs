using System.Globalization;
using Turso.Raw.Public.Value;

namespace Turso;

public class TursoConnectionOptions
{
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

    public string DataSource => _builder.DataSource;

    public string AuthToken => _builder.AuthToken;

    public string ReplicaPath => _builder.ReplicaPath;

    public bool ReadYourWrites => _builder.ReadYourWrites;

    public int SyncInterval => _builder.SyncInterval;

    public bool? Tls => _builder.Tls;

    public bool IsRemote => IsRemoteDataSource(DataSource);

    public bool IsReplica => IsRemote && !string.IsNullOrWhiteSpace(ReplicaPath);

    public TursoEncryptionCipher? GetEncryptionCipher() => _builder.GetEncryptionCipher();

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

    private static bool IsRemoteDataSource(string dataSource)
    {
        return Uri.TryCreate(dataSource, UriKind.Absolute, out var uri)
               && IsRemoteScheme(uri.Scheme);
    }

    private static bool IsRemoteScheme(string scheme)
    {
        return scheme.Equals("libsql", StringComparison.OrdinalIgnoreCase)
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
