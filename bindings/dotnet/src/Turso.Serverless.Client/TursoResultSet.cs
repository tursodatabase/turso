using System.Collections;
using System.Globalization;

namespace Turso.Serverless.Client;

/// <summary>
/// A single result row with positional and column-name access. Values are decoded to
/// <see cref="long"/> (integer), <see cref="double"/> (float), <see cref="string"/> (text),
/// <see cref="byte"/>[] (blob) or <c>null</c>.
/// </summary>
public sealed class TursoRow : IReadOnlyList<object?>
{
    private readonly object?[] _values;
    private readonly IReadOnlyList<string> _columns;

    internal TursoRow(object?[] values, IReadOnlyList<string> columns)
    {
        _values = values;
        _columns = columns;
    }

    public object? this[int index] => _values[index];

    public object? this[string column] => _values[IndexOf(column)];

    public int Count => _values.Length;

    public IReadOnlyList<string> Columns => _columns;

    public bool IsNull(int index) => _values[index] is null;

    public bool IsNull(string column) => this[column] is null;

    public long GetInt64(int index) => Convert.ToInt64(_values[index], CultureInfo.InvariantCulture);

    public long GetInt64(string column) => Convert.ToInt64(this[column], CultureInfo.InvariantCulture);

    public double GetDouble(int index) => Convert.ToDouble(_values[index], CultureInfo.InvariantCulture);

    public double GetDouble(string column) => Convert.ToDouble(this[column], CultureInfo.InvariantCulture);

    public string? GetString(int index) => _values[index]?.ToString();

    public string? GetString(string column) => this[column]?.ToString();

    public byte[]? GetBytes(int index) => (byte[]?)_values[index];

    public byte[]? GetBytes(string column) => (byte[]?)this[column];

    public IEnumerator<object?> GetEnumerator() => ((IEnumerable<object?>)_values).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private int IndexOf(string column)
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            if (string.Equals(_columns[i], column, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new ArgumentException($"Unknown column: {column}", nameof(column));
    }
}

/// <summary>The fully materialized result of a SQL statement.</summary>
public sealed class TursoResultSet
{
    internal TursoResultSet(IReadOnlyList<string> columns, IReadOnlyList<string> columnTypes, IReadOnlyList<TursoRow> rows, long rowsAffected, long? lastInsertRowid)
    {
        Columns = columns;
        ColumnTypes = columnTypes;
        Rows = rows;
        RowsAffected = rowsAffected;
        LastInsertRowid = lastInsertRowid;
    }

    /// <summary>Result column names, in order.</summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>Declared column types (<c>decltype</c>), in column order; empty string when undeclared.</summary>
    public IReadOnlyList<string> ColumnTypes { get; }

    public IReadOnlyList<TursoRow> Rows { get; }

    public long RowsAffected { get; }

    public long? LastInsertRowid { get; }
}

/// <summary>Aggregate result of a <c>Batch</c> execution.</summary>
public sealed class TursoBatchResult
{
    internal TursoBatchResult(long rowsAffected, long? lastInsertRowid)
    {
        RowsAffected = rowsAffected;
        LastInsertRowid = lastInsertRowid;
    }

    /// <summary>Total affected rows across the batch's user statements.</summary>
    public long RowsAffected { get; }

    /// <summary>Last insert rowid produced by the batch's user statements, when any.</summary>
    public long? LastInsertRowid { get; }
}

/// <summary>Column and parameter metadata of a SQL statement (the protocol's <c>describe</c> answer).</summary>
public sealed class TursoStatementDescription
{
    internal TursoStatementDescription(IReadOnlyList<string> parameterNames, IReadOnlyList<string> columns, IReadOnlyList<string> columnTypes, bool isExplain, bool isReadonly)
    {
        ParameterNames = parameterNames;
        Columns = columns;
        ColumnTypes = columnTypes;
        IsExplain = isExplain;
        IsReadonly = isReadonly;
    }

    public IReadOnlyList<string> ParameterNames { get; }

    public IReadOnlyList<string> Columns { get; }

    public IReadOnlyList<string> ColumnTypes { get; }

    public bool IsExplain { get; }

    public bool IsReadonly { get; }
}

/// <summary>One statement of a batch: SQL plus optional positional or named arguments.</summary>
public sealed class TursoBatchStatement
{
    public TursoBatchStatement(string sql, IReadOnlyList<object?>? args = null, IReadOnlyDictionary<string, object?>? namedArgs = null)
    {
        Sql = sql;
        Args = args;
        NamedArgs = namedArgs;
    }

    public string Sql { get; }

    public IReadOnlyList<object?>? Args { get; }

    public IReadOnlyDictionary<string, object?>? NamedArgs { get; }

    public static implicit operator TursoBatchStatement(string sql) => new(sql);
}

/// <summary>
/// Locking mode for atomic batch execution and transactions. Mirrors the JavaScript driver's
/// <c>BatchMode</c> / <c>transaction(...)</c> variants.
/// </summary>
public enum TursoBatchMode
{
    Deferred,
    Immediate,
    Exclusive,
    Concurrent,
}
