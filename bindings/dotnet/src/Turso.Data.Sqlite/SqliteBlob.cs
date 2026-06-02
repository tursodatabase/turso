using System.Data;
using System.Text;

namespace Turso.Data.Sqlite;

public class SqliteBlob : Stream
{
    private readonly MemoryStream? _stream;
    private readonly SqliteConnection? _connection;
    private readonly string? _tableName;
    private readonly string? _columnName;
    private readonly long _rowId;
    private readonly bool _readOnly;
    private bool _disposed;

    public SqliteBlob(SqliteConnection connection, string tableName, string columnName, long rowId, bool readOnly = false)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(columnName);
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException(Properties.Resources.SqlBlobRequiresOpenConnection);

        _connection = connection;
        _tableName = tableName;
        _columnName = columnName;
        _rowId = rowId;
        _readOnly = readOnly;
        _stream = new MemoryStream(GetBlobValue(connection, tableName, columnName, rowId), writable: true);
    }

    internal SqliteBlob(byte[] value)
    {
        _stream = new MemoryStream(value, writable: false);
        _readOnly = true;
    }

    public override bool CanRead => !_disposed && (_stream?.CanRead ?? false);

    public override bool CanSeek => !_disposed && (_stream?.CanSeek ?? false);

    public override bool CanWrite => !_disposed && !_readOnly && (_stream?.CanWrite ?? false);

    public override long Length => GetStream().Length;

    public override long Position
    {
        get => GetStream().Position;
        set
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, message: null);

            GetStream().Position = value;
        }
    }

    public override void Flush()
    {
        Persist();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ValidateBuffer(buffer, offset, count);
        return GetStream().Read(buffer, offset, count);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        var stream = GetStream();
        var position = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => stream.Position + offset,
            SeekOrigin.End => stream.Length + offset,
            _ => throw new ArgumentException(Properties.Resources.InvalidEnumValue(typeof(SeekOrigin), origin), nameof(origin))
        };
        if (position < 0)
            throw new IOException(Properties.Resources.SeekBeforeBegin);

        stream.Position = position;
        return position;
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException(Properties.Resources.ResizeNotSupported);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        if (_readOnly)
            throw new NotSupportedException(Properties.Resources.WriteNotSupported);

        ValidateBuffer(buffer, offset, count);
        if (count == 0)
            return;

        var stream = GetStream();
        if (stream.Position + count > stream.Length)
            throw new NotSupportedException(Properties.Resources.ResizeNotSupported);

        stream.Write(buffer, offset, count);
        Persist();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
            _stream?.Dispose();

        _disposed = true;
        base.Dispose(disposing);
    }

    private MemoryStream GetStream()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _stream ?? throw new NotSupportedException("Incremental blob I/O is not yet supported by the Turso SQLite-compatible provider.");
    }

    private void Persist()
    {
        if (_connection is null || _tableName is null || _columnName is null || _readOnly)
            return;

        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE " + QuoteIdentifier(_tableName) + " SET " + QuoteIdentifier(_columnName) + " = $value WHERE rowid = $rowid;";
        command.Parameters.Add("$value", SqliteType.Blob).Value = GetStream().ToArray();
        command.Parameters.Add("$rowid", SqliteType.Integer).Value = _rowId;
        command.ExecuteNonQuery();
    }

    private static byte[] GetBlobValue(SqliteConnection connection, string tableName, string columnName, long rowId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT " + QuoteIdentifier(columnName) + " FROM " + QuoteIdentifier(tableName) + " WHERE rowid = $rowid;";
        command.Parameters.Add("$rowid", SqliteType.Integer).Value = rowId;
        var value = command.ExecuteScalar();
        return value switch
        {
            byte[] bytes => bytes,
            string text => Encoding.UTF8.GetBytes(text),
            null or DBNull => throw new SqliteException(Properties.Resources.SqliteNativeError(1, "no such rowid: " + rowId), 1),
            _ => Encoding.UTF8.GetBytes(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty)
        };
    }

    private static void ValidateBuffer(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset), offset, message: null);
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), count, message: null);
        if (offset > buffer.Length || count > buffer.Length - offset)
            throw new ArgumentException(Properties.Resources.InvalidOffsetAndCount);
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
