namespace Turso.Raw.Public;

public class TursoException : Exception
{
    public TursoException(string message)
        : base(message)
    {
    }

    public TursoException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}

public sealed class TursoSyncNativeException : TursoException
{
    public TursoSyncNativeException(uint statusCode, string message)
        : base(message)
    {
        StatusCode = statusCode;
    }

    public uint StatusCode { get; }
}