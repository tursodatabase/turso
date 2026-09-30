using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Sqlite.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace Turso.EntityFrameworkCore.Sqlite.Update.Internal;

public sealed class TursoSqliteUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies) : UpdateAndSelectSqlGenerator(dependencies)
{
    public override ResultSetMapping AppendInsertOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyModificationCommand command,
            int commandPosition,
            out bool requiresTransaction)
        => CanUseReturningClause(command)
            ? AppendInsertReturningOperation(commandStringBuilder, command, commandPosition, out requiresTransaction)
            : AppendInsertAndSelectOperation(commandStringBuilder, command, commandPosition, out requiresTransaction);

    public override ResultSetMapping AppendUpdateOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyModificationCommand command,
            int commandPosition,
            out bool requiresTransaction)
        => CanUseReturningClause(command)
            ? AppendUpdateReturningOperation(commandStringBuilder, command, commandPosition, out requiresTransaction)
            : AppendUpdateAndSelectOperation(commandStringBuilder, command, commandPosition, out requiresTransaction);

    public override ResultSetMapping AppendDeleteOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyModificationCommand command,
            int commandPosition,
            out bool requiresTransaction)
        => CanUseReturningClause(command)
            ? AppendDeleteReturningOperation(commandStringBuilder, command, commandPosition, out requiresTransaction)
            : AppendDeleteAndSelectOperation(commandStringBuilder, command, commandPosition, out requiresTransaction);

    protected override void AppendIdentityWhereCondition(StringBuilder commandStringBuilder, IColumnModification columnModification)
    {
        SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, "rowid");
        commandStringBuilder.Append(" = last_insert_rowid()");
    }

    protected override ResultSetMapping AppendSelectAffectedCountCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        int commandPosition)
    {
        commandStringBuilder
            .Append("SELECT changes()")
            .AppendLine(SqlGenerationHelper.StatementTerminator)
            .AppendLine();

        return ResultSetMapping.LastInResultSet | ResultSetMapping.ResultSetWithRowsAffectedOnly;
    }

    protected override void AppendRowsAffectedWhereCondition(StringBuilder commandStringBuilder, int expectedRowsAffected)
        => commandStringBuilder.Append("changes() = ").Append(expectedRowsAffected);

    public override string GenerateNextSequenceValueOperation(string name, string? schema)
        => throw new NotSupportedException(SqliteStrings.SequencesNotSupported);

    protected override void AppendUpdateColumnValue(
        ISqlGenerationHelper updateSqlGeneratorHelper,
        IColumnModification columnModification,
        StringBuilder stringBuilder,
        string name,
        string? schema)
    {
        if (columnModification.JsonPath is not (null or "$"))
        {
            stringBuilder.Append("json_set(");
            updateSqlGeneratorHelper.DelimitIdentifier(stringBuilder, columnModification.ColumnName);
            stringBuilder.Append(", '");
            stringBuilder.Append(columnModification.JsonPath);
            stringBuilder.Append("', ");

            if (columnModification.Property is { IsPrimitiveCollection: false })
            {
                var providerClrType = (columnModification.Property.GetTypeMapping().Converter?.ProviderClrType
                    ?? columnModification.Property.ClrType);
                providerClrType = Nullable.GetUnderlyingType(providerClrType) ?? providerClrType;

                if (providerClrType == typeof(bool))
                    stringBuilder.Append("json(");

                base.AppendUpdateColumnValue(updateSqlGeneratorHelper, columnModification, stringBuilder, name, schema);

                if (providerClrType == typeof(bool))
                    stringBuilder.Append(")");
            }
            else
            {
                stringBuilder.Append("json(");
                base.AppendUpdateColumnValue(updateSqlGeneratorHelper, columnModification, stringBuilder, name, schema);
                stringBuilder.Append(")");
            }

            stringBuilder.Append(")");
        }
        else
        {
            base.AppendUpdateColumnValue(updateSqlGeneratorHelper, columnModification, stringBuilder, name, schema);
        }
    }

    private static bool CanUseReturningClause(IReadOnlyModificationCommand command)
        => command.Table?.IsSqlReturningClauseUsed() == true;
}
