using Microsoft.EntityFrameworkCore.Query;

namespace Turso.EntityFrameworkCore.Sqlite.Query.Internal;

public sealed class TursoSqliteQueryableMethodTranslatingExpressionVisitorFactory(
    QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
    RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies) : IQueryableMethodTranslatingExpressionVisitorFactory
{
    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => new TursoSqliteQueryableMethodTranslatingExpressionVisitor(
            dependencies,
            relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext);
}
