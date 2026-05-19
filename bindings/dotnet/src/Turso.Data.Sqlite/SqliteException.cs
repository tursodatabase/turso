using System.Data.Common;

namespace Turso.Data.Sqlite;

public class SqliteException : DbException
{
    public SqliteException(string message, int errorCode)
        : this(message, errorCode, errorCode)
    {
    }

    public SqliteException(string message, int errorCode, int extendedErrorCode)
        : base(message, errorCode)
    {
        SqliteErrorCode = errorCode;
        SqliteExtendedErrorCode = extendedErrorCode;
    }

    public int SqliteErrorCode { get; }

    public int SqliteExtendedErrorCode { get; }

    public static void ThrowExceptionForRC(int rc, object? db)
    {
        if (rc is not (0 or 100 or 101))
            throw new SqliteException(Properties.Resources.SqliteNativeError(rc, Properties.Resources.DefaultNativeError), rc);
    }
}
