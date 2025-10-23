using System.Data.Common;
using IsolationLevel = System.Data.IsolationLevel;

namespace Turso;

public class TursoTransaction : DbTransaction
{
    private TursoConnection _connection;
    private IsolationLevel _isolationLevel;
    private bool _completed = false;

    public TursoTransaction(TursoConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _isolationLevel = isolationLevel;

        if (isolationLevel == IsolationLevel.ReadUncommitted)
            connection.ExecuteNonQuery("PRAGMA read_uncommitted = 1;");

        connection.ExecuteNonQuery("BEGIN");
    }

    protected override void Dispose(bool disposing)
    {
        if (!_completed)
        {
            Rollback();
        }
    }

    public override IsolationLevel IsolationLevel => _isolationLevel;

    protected override DbConnection? DbConnection => _connection;

    public override void Commit()
    {
        _connection.ExecuteNonQuery("COMMIT;");
        CompleteTransaction();
    }

    public override void Rollback()
    {
        try
        {
            _connection.ExecuteNonQuery("ROLLBACK;");
        }
        finally
        {
            CompleteTransaction();
        }
    }

    private void CompleteTransaction()
    {
        if (_isolationLevel == IsolationLevel.ReadUncommitted)
            _connection.ExecuteNonQuery("PRAGMA read_uncommitted = 0;");
        _completed = true;
    }
}