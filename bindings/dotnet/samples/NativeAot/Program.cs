using Turso.Data.Sqlite;

using var connection = new SqliteConnection("Data Source=:memory:");
connection.Open();

using (var command = connection.CreateCommand())
{
    command.CommandText = """
        CREATE TABLE items (value TEXT NOT NULL);
        INSERT INTO items VALUES ('native'), ('aot'), ('turso');
        """;
    command.ExecuteNonQuery();
}

using (var command = connection.CreateCommand())
{
    command.CommandText = "SELECT COUNT(*) FROM items";
    Console.WriteLine($"Rows: {command.ExecuteScalar()}");
}
