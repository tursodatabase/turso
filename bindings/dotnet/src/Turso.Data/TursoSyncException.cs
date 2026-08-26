using System.Net;
using Turso.Raw.Public;

namespace Turso;

public enum TursoSyncOperationKind
{
    Create,
    Open,
    Connect,
    Pull,
    Apply,
}

public sealed class TursoSyncException : TursoException
{
    internal TursoSyncException(
        TursoSyncOperationKind operation,
        string message,
        uint? nativeStatusCode,
        string? httpMethod,
        Uri? endpoint,
        HttpStatusCode? httpStatusCode,
        Exception innerException)
        : base(message, innerException)
    {
        Operation = operation;
        NativeStatusCode = nativeStatusCode;
        HttpMethod = httpMethod;
        Endpoint = endpoint;
        HttpStatusCode = httpStatusCode;
    }

    public TursoSyncOperationKind Operation { get; }

    public uint? NativeStatusCode { get; }

    public string? HttpMethod { get; }

    public Uri? Endpoint { get; }

    public HttpStatusCode? HttpStatusCode { get; }
}
