using System.Data;
using System.Data.Common;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso;

public class TursoCommand : DbCommand
{
    private TursoConnection _connection;
    private TursoParameterCollection _parameterCollection = new();

    private TursoTransaction? _transaction;
    private TursoStatementHandle? _statement;

    public TursoCommand(TursoConnection connection, TursoTransaction? transaction = null)
    {
        _connection = connection;
        _transaction = transaction;
    }

    public TursoCommand(TursoConnection connection, string command)
    {
        _connection = connection;
        _transaction = null;
        CommandText = command;
    }


    public override string CommandText { get; set; } = "";
    public override int CommandTimeout { get; set; } = 30;

    public override CommandType CommandType
    {
        get => CommandType.Text;
        set => throw new NotSupportedException();
    }

    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = value as TursoConnection ?? throw new ArgumentException();
    }

    protected override DbParameterCollection DbParameterCollection => _parameterCollection;

    public new virtual TursoParameterCollection Parameters => _parameterCollection;


    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set => _transaction = value as TursoTransaction ?? throw new ArgumentException();
    }
    
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _statement?.Dispose();
    }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery()
    {
        var reader = Execute();
        reader.NextResult();
        return reader.RecordsAffected;
    }

    public override object? ExecuteScalar()
    {
        using var reader = Execute();
        return reader.Read()
            ? reader.GetValue(0)
            : null;
    }

    public override void Prepare()
    {
        _statement = TursoBindings.PrepareStatement(_connection.Turso, CommandText);
        for (var i = 0; i < _parameterCollection.Count; i++)
        {
            var parameter = _parameterCollection[i] as TursoParameter;
            if (parameter == null)
                throw new ArgumentException("Parameter must be of type TursoParameter");

            if (!string.IsNullOrEmpty(parameter.ParameterName))
            {
                TursoBindings.BindNamedParameter(_statement, parameter.ParameterName, parameter.ToValue());
            }
            else
            {
                TursoBindings.BindParameter(_statement, i + 1, parameter.ToValue());
            }
        }
    }

    protected override DbParameter CreateDbParameter()
    {
        return new TursoParameter();
    }


    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return Execute(behavior);
    }

    private DbDataReader Execute(CommandBehavior behavior = CommandBehavior.Default)
    {
        if (_statement is null)
            Prepare();

        var reader = new TursoDataReader(this, _statement);
        return reader;
    }
}