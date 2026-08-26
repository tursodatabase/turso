namespace Turso.Raw.Public;

public class TursoException(string message) : Exception(message);

public sealed class TursoSyncNativeException(uint statusCode, string message) : TursoException(message)
{
    public uint StatusCode { get; } = statusCode;
}