namespace Turso.Data.Sqlite;

public static class SqliteConnectionExtensions
{
    public static int ExecuteNonQuery(this SqliteConnection connection, string commandText)
    {
        using var command = new SqliteCommand(commandText, connection);
        return command.ExecuteNonQuery();
    }

    public static SqliteDataReader ExecuteReader(this SqliteConnection connection, string commandText)
    {
        using var command = new SqliteCommand(commandText, connection);
        return (SqliteDataReader)command.ExecuteReader();
    }

    public static object? ExecuteScalar(this SqliteConnection connection, string commandText)
    {
        using var command = new SqliteCommand(commandText, connection);
        return command.ExecuteScalar();
    }

    public static T ExecuteScalar<T>(this SqliteConnection connection, string commandText)
    {
        var value = ExecuteScalar(connection, commandText);
        if (value is null)
            return default!;
        if (value is DBNull)
            return typeof(T) == typeof(object) || typeof(T) == typeof(DBNull) ? (T)value : default!;
        if (value is T typedValue)
            return typedValue;

        return (T)Convert.ChangeType(value, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }
}
