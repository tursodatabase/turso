using System.Data.Common;
using IsolationLevel = System.Data.IsolationLevel;

namespace Turso;

public class TursoTransaction : DbTransaction
{
    private readonly TursoConnection _connection;
    private readonly IsolationLevel _isolationLevel;
    private bool _completed;

    public TursoTransaction(TursoConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _isolationLevel = NormalizeIsolationLevel(isolationLevel);

        if (_isolationLevel == IsolationLevel.ReadUncommitted)
            connection.ReadUncommitted = true;

        if (connection.IsRemote)
            connection.BeginRemoteTransaction(_isolationLevel);
        else
            connection.ExecuteNonQuery("BEGIN");
    }

    protected override void Dispose(bool disposing)
    {
        if (!_completed)
        {
            if (_connection.State == System.Data.ConnectionState.Closed)
                CompleteTransaction();
            else
                Rollback();
        }

        base.Dispose(disposing);
    }

    public override IsolationLevel IsolationLevel => _isolationLevel;

    internal bool IsCompleted => _completed;

    protected override DbConnection? DbConnection => _connection;

    public override void Commit()
    {
        ThrowIfCompleted();
        if (_connection.IsRemote)
        {
            try
            {
                _connection.CommitRemoteTransaction();
            }
            catch (TursoRemoteSqlException)
            {
                throw;
            }
            catch
            {
                CompleteTransaction();
                throw;
            }

            CompleteTransaction();
            _connection.CloseRemoteSessionIfStateless();
            return;
        }
        else
        {
            _connection.ExecuteNonQuery("COMMIT;");
            CompleteTransaction();
        }
    }

    public override void Rollback()
    {
        ThrowIfCompleted();
        if (_connection.IsRemote)
        {
            try
            {
                _connection.RollbackRemoteTransaction();
            }
            finally
            {
                CompleteTransaction();
            }

            _connection.CloseRemoteSessionIfStateless();
            return;
        }

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
            _connection.ReadUncommitted = false;
        _completed = true;
    }

    private void ThrowIfCompleted()
    {
        if (_completed)
            throw new InvalidOperationException("This transaction has already completed.");
    }

    private static IsolationLevel NormalizeIsolationLevel(IsolationLevel isolationLevel)
    {
        return isolationLevel switch
        {
            IsolationLevel.Unspecified => IsolationLevel.Serializable,
            IsolationLevel.Serializable => IsolationLevel.Serializable,
            IsolationLevel.ReadCommitted => IsolationLevel.Serializable,
            IsolationLevel.ReadUncommitted => IsolationLevel.ReadUncommitted,
            _ => throw new NotSupportedException($"Isolation level {isolationLevel} is not supported.")
        };
    }
}
