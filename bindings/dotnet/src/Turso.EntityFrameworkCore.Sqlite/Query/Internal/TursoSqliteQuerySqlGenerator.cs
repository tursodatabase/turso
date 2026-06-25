using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Sqlite.Query.Internal;

namespace Turso.EntityFrameworkCore.Sqlite.Query.Internal;

public sealed class TursoSqliteQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : SqliteQuerySqlGenerator(dependencies)
{
    protected override Expression VisitOrdering(OrderingExpression orderingExpression)
    {
        if (ShouldUseDecimalCollation(orderingExpression.Expression))
        {
            var collatedExpression = new CollateExpression(orderingExpression.Expression, "EF_DECIMAL");
            return base.VisitOrdering(new OrderingExpression(collatedExpression, orderingExpression.IsAscending));
        }

        return base.VisitOrdering(orderingExpression);
    }

    private static bool ShouldUseDecimalCollation(SqlExpression expression)
    {
        if (expression is CollateExpression)
            return false;

        return IsDecimalType(expression.Type)
               || (expression.TypeMapping is not null && IsDecimalType(expression.TypeMapping.ClrType));
    }

    private static bool IsDecimalType(Type type)
        => (Nullable.GetUnderlyingType(type) ?? type) == typeof(decimal);
}
