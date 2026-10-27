using System.Data;
using AwesomeAssertions;
using Microsoft.EntityFrameworkCore;
using Turso.Data.Sqlite;

namespace Turso.Tests;

public class GuidBindingTests
{
    private static readonly Guid Value = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

    private static string BlobLiteral => $"X'{Convert.ToHexString(Value.ToByteArray())}'";

    [Test]
    public void FacadeBindsGuidAsTextByDefault()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT typeof(@g) || ':' || length(@g), @g";
        command.Parameters.AddWithValue("@g", Value);
        command.Parameters["@g"].SqliteType.Should().Be(SqliteType.Text);

        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("text:36");
        reader.GetString(1).Should().Be(Value.ToString().ToUpperInvariant());
        reader.GetGuid(1).Should().Be(Value);
    }

    [Test]
    public void FacadeBindsGuidAsBlobWhenOptedIn()
    {
        using var connection = new SqliteConnection("Data Source=:memory:;BinaryGUID=True");
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT typeof(@g) || ':' || length(@g), @g = {BlobLiteral}, @g";
            command.Parameters.AddWithValue("@g", Value);

            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("blob:16");
            reader.GetInt64(1).Should().Be(1);
            reader.GetGuid(2).Should().Be(Value);
        }

        // A parameter typed as a Guid or as text, which is what EF Core sends, stays TEXT.
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT typeof(@g) || ':' || length(@g), typeof(@t)";
            command.Parameters.Add(new SqliteParameter("@g", Value) { DbType = DbType.Guid });
            command.Parameters.Add(new SqliteParameter("@t", SqliteType.Text) { Value = Value });

            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("text:36");
            reader.GetString(1).Should().Be("text");
        }
    }

    [Test]
    public void TursoConnectionBindsGuidAsBlobWhenOptedIn()
    {
        using (var connection = new TursoConnection("Data Source=:memory:"))
        {
            connection.Open();
            Scalar(connection, "SELECT typeof(@g) || ':' || length(@g)").Should().Be("text:36");
            Scalar(connection, "SELECT @g").Should().Be(Value.ToString());
        }

        using (var connection = new TursoConnection("Data Source=:memory:;BinaryGUID=True"))
        {
            connection.Open();
            Scalar(connection, "SELECT typeof(@g) || ':' || length(@g)").Should().Be("blob:16");
            Scalar(connection, $"SELECT @g = {BlobLiteral}").Should().Be(1L);

            using var command = new TursoCommand(connection, "SELECT @g");
            command.Parameters.AddWithValue("@g", Value);
            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetGuid(0).Should().Be(Value);
        }

        using (var connection = new TursoConnection("Data Source=:memory:;BinaryGuid=True"))
        {
            connection.Open();
            using var command = new TursoCommand(connection, "SELECT typeof(@g)");
            command.Parameters.Add(new TursoParameter("@g", DbType.Guid, Value));
            command.ExecuteScalar().Should().Be("text");
        }

        using (var connection = new TursoConnection("Data Source=:memory:"))
        {
            connection.Open();
            using var command = new TursoCommand(connection, "SELECT typeof(@g)");
            command.Parameters.Add(new TursoParameter("@g", DbType.Binary, Value));
            command.ExecuteScalar().Should().Be("blob");
        }
    }

    [Test]
    public async Task EfCoreStoresGuidAsTextEvenWithBinaryGuid()
    {
        var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".db");
        try
        {
            await using (var context = CreateContext($"Data Source={path};BinaryGUID=True"))
            {
                await context.Database.EnsureCreatedAsync();
                context.Notes.Add(new Note { Id = Value, Text = "a" });
                await context.SaveChangesAsync();
            }

            await using (var context = CreateContext($"Data Source={path};BinaryGUID=True"))
            {
                (await context.Notes.SingleAsync(note => note.Id == Value)).Text.Should().Be("a");
                (await context.Database.SqlQueryRaw<string>("SELECT typeof(Id) AS Value FROM Notes").SingleAsync())
                    .Should().Be("text");
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in new[] { path, path + "-wal", path + "-shm" })
            {
                if (File.Exists(file))
                    File.Delete(file);
            }
        }
    }

    private static object? Scalar(TursoConnection connection, string sql)
    {
        using var command = new TursoCommand(connection, sql);
        command.Parameters.AddWithValue("@g", Value);
        return command.ExecuteScalar();
    }

    private static NotesContext CreateContext(string connectionString)
        => new(new DbContextOptionsBuilder<NotesContext>().UseTurso(connectionString).Options);

    private sealed class NotesContext(DbContextOptions<NotesContext> options) : DbContext(options)
    {
        public DbSet<Note> Notes => Set<Note>();
    }

    private sealed class Note
    {
        public Guid Id { get; set; }

        public string Text { get; set; } = "";
    }
}
