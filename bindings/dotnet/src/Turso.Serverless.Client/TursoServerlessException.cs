namespace Turso.Serverless.Client;

/// <summary>
/// Error reported by the Turso server or the SQL over HTTP transport.
/// The .NET counterpart of the JavaScript driver's <c>DatabaseError</c>.
/// </summary>
public class TursoServerlessException : Exception
{
    /// <summary>Machine-readable error code (e.g. <c>SQLITE_CONSTRAINT</c>), when the server provided one.</summary>
    public string? Code { get; }

    public TursoServerlessException(string message, string? code = null, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = code;
    }
}

/// <summary>
/// Thrown when a query exceeds the configured timeout. <see cref="TursoServerlessException.Code"/> is <c>TIMEOUT</c>.
/// </summary>
public sealed class TursoTimeoutException : TursoServerlessException
{
    public TursoTimeoutException(string message = "Query timed out", Exception? innerException = null)
        : base(message, "TIMEOUT", innerException)
    {
    }
}
