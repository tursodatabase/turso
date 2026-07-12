using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using TursoSqliteConnection = Turso.Data.Sqlite.SqliteConnection;
using TursoSqliteConnectionStringBuilder = Turso.Data.Sqlite.SqliteConnectionStringBuilder;
using TursoSqliteOpenMode = Turso.Data.Sqlite.SqliteOpenMode;

namespace Turso.EntityFrameworkCore.Sqlite.Storage.Internal;

public class TursoSqliteRelationalConnection : SqliteRelationalConnection
{
    private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;
    private readonly IDiagnosticsLogger<DbLoggerCategory.Infrastructure> _logger;
    private readonly int? _commandTimeout;

    public TursoSqliteRelationalConnection(
        RelationalConnectionDependencies dependencies,
        IRawSqlCommandBuilder rawSqlCommandBuilder,
        IDiagnosticsLogger<DbLoggerCategory.Infrastructure> logger)
        : base(dependencies, rawSqlCommandBuilder, logger)
    {
        _rawSqlCommandBuilder = rawSqlCommandBuilder;
        _logger = logger;

        var relationalOptions = RelationalOptionsExtension.Extract(dependencies.ContextOptions);
        _commandTimeout = relationalOptions.CommandTimeout;
        if (relationalOptions.Connection is TursoSqliteConnection connection)
            InitializeTursoConnection(connection);
    }

    protected override DbConnection CreateDbConnection()
    {
        var connection = new TursoSqliteConnection(GetValidatedConnectionString());
        InitializeTursoConnection(connection);
        return connection;
    }

    public override ISqliteRelationalConnection CreateReadOnlyConnection()
    {
        var connectionStringBuilder = new TursoSqliteConnectionStringBuilder(GetValidatedConnectionString())
        {
            Mode = TursoSqliteOpenMode.ReadOnly,
            Pooling = false
        };

        var contextOptions = new DbContextOptionsBuilder()
            .UseTurso(connectionStringBuilder.ToString())
            .Options;

        return new TursoSqliteRelationalConnection(
            Dependencies with { ContextOptions = contextOptions },
            _rawSqlCommandBuilder,
            _logger);
    }

    private void InitializeTursoConnection(TursoSqliteConnection connection)
    {
        if (_commandTimeout.HasValue)
            connection.DefaultTimeout = _commandTimeout.Value;

        connection.CreateFunction<string, string, bool?>(
            "regexp",
            (pattern, input) => input is null || pattern is null
                ? null
                : Regex.IsMatch(input, pattern, RegexOptions.None, TimeSpan.FromMilliseconds(1000)),
            isDeterministic: true);

        connection.CreateFunction(
            "ef_mod",
            (decimal? dividend, decimal? divisor) => divisor == 0m ? null : dividend % divisor,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_add",
            (decimal? left, decimal? right) => left + right,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_divide",
            (decimal? dividend, decimal? divisor) => divisor == 0m ? null : dividend / divisor,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_compare",
            (decimal? left, decimal? right) => left.HasValue && right.HasValue
                ? decimal.Compare(left.Value, right.Value)
                : default(int?),
            isDeterministic: true);

        connection.CreateFunction(
            "ef_multiply",
            (decimal? left, decimal? right) => left * right,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_negate",
            (decimal? value) => -value,
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_avg",
            seed: (0m, 0ul),
            ((decimal Sum, ulong Count) accumulator, decimal? value) => value is null
                ? accumulator
                : (accumulator.Sum + value.Value, accumulator.Count + 1),
            ((decimal Sum, ulong Count) accumulator) => accumulator.Count == 0
                ? default(decimal?)
                : accumulator.Sum / accumulator.Count,
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_max",
            seed: null,
            (decimal? max, decimal? value) => max is null
                ? value
                : value is null
                    ? max
                    : decimal.Max(max.Value, value.Value),
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_min",
            seed: null,
            (decimal? min, decimal? value) => min is null
                ? value
                : value is null
                    ? min
                    : decimal.Min(min.Value, value.Value),
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_sum",
            seed: null,
            (decimal? sum, decimal? value) => value is null
                ? sum
                : sum is null
                    ? value
                    : sum.Value + value.Value,
            isDeterministic: true);

        connection.CreateCollation(
            "EF_DECIMAL",
            (left, right) => decimal.Compare(
                decimal.Parse(left, NumberStyles.Number, CultureInfo.InvariantCulture),
                decimal.Parse(right, NumberStyles.Number, CultureInfo.InvariantCulture)));
    }
}
