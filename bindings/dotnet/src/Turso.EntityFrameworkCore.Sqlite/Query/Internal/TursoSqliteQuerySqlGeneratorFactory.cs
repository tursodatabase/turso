using Microsoft.EntityFrameworkCore.Query;

namespace Turso.EntityFrameworkCore.Sqlite.Query.Internal;

public sealed class TursoSqliteQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies) : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create()
        => new TursoSqliteQuerySqlGenerator(dependencies);
}
