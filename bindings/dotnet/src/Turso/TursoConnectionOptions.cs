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

    public TursoEncryptionCipher? GetEncryptionCipher() => _builder.GetEncryptionCipher();

    public static TursoConnectionOptions Parse(string connectionString)
    {
        return new TursoConnectionOptions(new TursoConnectionStringBuilder(connectionString));
    }
}
