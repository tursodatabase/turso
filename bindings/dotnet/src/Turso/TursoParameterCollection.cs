using System.Collections;
using System.Data.Common;

namespace Turso;

public class TursoParameterCollection : DbParameterCollection
{
    private readonly List<TursoParameter> _parameters = new();
    public override int Count => _parameters.Count;

    public override object SyncRoot => throw new NotImplementedException();

    public override int Add(object value)
    {
        _parameters.Add(value as TursoParameter ?? new TursoParameter(value));
        return _parameters.Count - 1;
    }

    public int AddWithValue(string parameterName, object value)
    {
        _parameters.Add(new TursoParameter(parameterName, value));
        return _parameters.Count - 1;
    }
    
    public override void AddRange(Array values)
    {
        for (var i = 0; i < values.Length; i++)
        {
            var value = values.GetValue(i)!;
            var parameter = value as TursoParameter ?? new TursoParameter(value);
            _parameters.Add(parameter);
        }
    }

    public override void Clear()
    {
        _parameters.Clear();
    }

    public override bool Contains(object value)
    {
        return _parameters.Any(p => value is TursoParameter ? p == value : p.Value == value);
    }

    public override bool Contains(string value)
    {
        return _parameters.Any(p => p.ParameterName == value);
    }

    public override void CopyTo(Array array, int index)
    {
        _parameters.CopyTo((TursoParameter[])array, index);
    }

    public override IEnumerator GetEnumerator()
    {
        return _parameters.GetEnumerator();
    }

    public override int IndexOf(object value)
    {
        return _parameters.FindIndex(p => value is TursoParameter ? p == value : p.Value == value);
    }

    public override int IndexOf(string parameterName)
    {
        return _parameters.FindIndex(p => p.ParameterName == parameterName);
    }

    public override void Insert(int index, object value)
    {
        _parameters.Insert(index, value as TursoParameter ?? new TursoParameter(value));
    }

    public override void Remove(object value)
    {
        var index = IndexOf(value);
        if (index == -1)
            throw new ArgumentException($"Parameter {value} not found");
        _parameters.RemoveAt(index);
    }

    public override void RemoveAt(int index)
    {
        _parameters.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException($"Parameter {parameterName} not found");

        _parameters.RemoveAt(index);
    }

    protected override DbParameter GetParameter(int index)
    {
        return _parameters[index];
    }

    protected override DbParameter GetParameter(string parameterName)
    {
        return _parameters.Find(p => p.ParameterName == parameterName)
               ?? throw new ArgumentException($"Parameter {parameterName} not found");
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        _parameters[index] = value as TursoParameter
                             ?? throw new ArgumentException($"Parameter {value} is not a TursoParameter");
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException($"Parameter {parameterName} not found");
        _parameters[index] = value as TursoParameter
                             ?? throw new ArgumentException($"Parameter {value} is not a TursoParameter");
    }
}