namespace Turso;

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
    public HttpClient? HttpClient { get; init; }

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

    }
}
