using System.Data.Common;

namespace Turso;

public sealed class TursoFactory : DbProviderFactory
{
    public static readonly TursoFactory Instance = new();

    private TursoFactory()
    {
    }

    public override DbCommand CreateCommand() => new TursoCommand();

    public override DbConnection CreateConnection() => new TursoConnection();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new TursoConnectionStringBuilder();

    public override DbParameter CreateParameter() => new TursoParameter();
}
