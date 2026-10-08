namespace Turso.Raw.Public;

public class TursoException : Exception
{
    public TursoException(string message)
        : base(message)
    {
    }

    public TursoException(uint statusCode, string message)
        : base(message)
    {
        StatusCode = statusCode;
    }

    public TursoException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    public uint? StatusCode { get; }

    public bool IsInterrupt => StatusCode == 5;
}

public sealed class TursoSyncNativeException : TursoException
{
    public TursoSyncNativeException(uint statusCode, string message)
        : base(statusCode, message)
    {
        StatusCode = statusCode;
    }

    public new uint StatusCode { get; }
}