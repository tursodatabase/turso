using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Turso;

public sealed class TursoBatchCommand : DbBatchCommand
{
    private readonly TursoParameterCollection _parameters = new();
    private string _commandText = "";
    private CommandType _commandType = CommandType.Text;
    private int _recordsAffected = -1;

    public TursoBatchCommand()
    {
    }

    public TursoBatchCommand(string commandText)
    {
        CommandText = commandText;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? "";
    }

    public override CommandType CommandType
    {
        get => _commandType;
        set
        {
            if (value != CommandType.Text)
                throw new NotSupportedException("TursoBatchCommand only supports CommandType.Text.");

            _commandType = value;
        }
    }

    protected override DbParameterCollection DbParameterCollection => _parameters;

    public new TursoParameterCollection Parameters => _parameters;

    public override int RecordsAffected => _recordsAffected;

    public override bool CanCreateParameter => true;

    public override DbParameter CreateParameter()
    {
        return new TursoParameter();
    }

    internal void SetRecordsAffected(int recordsAffected)
    {
        _recordsAffected = recordsAffected;
    }
}
