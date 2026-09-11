using System.Collections;
using System.Data.Common;

namespace Turso.Data.Sqlite;

public class SqliteParameterCollection : DbParameterCollection
{
    private readonly List<SqliteParameter> _parameters = [];

    public override int Count => _parameters.Count;

    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    public new SqliteParameter this[int index]
    {
        get => _parameters[index];
        set => _parameters[index] = value;
    }

    public new SqliteParameter this[string parameterName]
    {
        get => (SqliteParameter)GetParameter(parameterName);
        set => SetParameter(parameterName, value);
    }

    public override int Add(object value)
    {
        var parameter = value switch
        {
            SqliteParameter sqliteParameter => sqliteParameter,
            _ => new SqliteParameter(null, value)
        };
        _parameters.Add(parameter);
        return _parameters.Count - 1;
    }

    public SqliteParameter Add(string? parameterName, SqliteType type)
    {
        var parameter = new SqliteParameter(parameterName, type);
        _parameters.Add(parameter);
        return parameter;
    }

    public SqliteParameter Add(string? parameterName, SqliteType type, int size)
    {
        var parameter = new SqliteParameter(parameterName, type, size);
        _parameters.Add(parameter);
        return parameter;
    }

    public SqliteParameter AddWithValue(string? parameterName, object? value)
    {
        var parameter = new SqliteParameter(parameterName, value);
        _parameters.Add(parameter);
        return parameter;
    }

    public override void AddRange(Array values)
    {
        ArgumentNullException.ThrowIfNull(values);
        foreach (var value in values)
            Add(value!);
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value)
    {
        return value is SqliteParameter parameter
            ? _parameters.Contains(parameter)
            : _parameters.Any(p => Equals(p.Value, value));
    }

    public override bool Contains(string value) => IndexOf(value) != -1;

    public override void CopyTo(Array array, int index)
    {
        ArgumentNullException.ThrowIfNull(array);
        for (var i = 0; i < _parameters.Count; i++)
            array.SetValue(_parameters[i], index + i);
    }

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override int IndexOf(object value)
    {
        return value is SqliteParameter parameter
            ? _parameters.IndexOf(parameter)
            : _parameters.FindIndex(p => Equals(p.Value, value));
    }

    public override int IndexOf(string parameterName)
    {
        return _parameters.FindIndex(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
    }

    public override void Insert(int index, object value)
    {
        _parameters.Insert(index, value as SqliteParameter ?? new SqliteParameter(null, value));
    }

    public override void Remove(object value)
    {
        var index = IndexOf(value);
        if (index == -1)
            throw new ArgumentException($"Parameter {value} not found.", nameof(value));

        _parameters.RemoveAt(index);
    }

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException($"Parameter {parameterName} not found.", nameof(parameterName));

        _parameters.RemoveAt(index);
    }

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        return index == -1
            ? throw new ArgumentException($"Parameter {parameterName} not found.", nameof(parameterName))
            : _parameters[index];
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        _parameters[index] = value as SqliteParameter
                             ?? throw new ArgumentException("Parameter must be a SqliteParameter.", nameof(value));
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException($"Parameter {parameterName} not found.", nameof(parameterName));

        SetParameter(index, value);
    }
}
