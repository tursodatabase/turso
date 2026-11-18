using System.Data;
using System.Net;
using System.Text;
using System.Text.Json;
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
    public void RemoteCommandsHonorBinaryGuid()
    {
        var base64 = Convert.ToBase64String(Value.ToByteArray());
        using var handler = new RecordingPipelineHandler();
        TursoConnection.RemoteMessageHandlerFactory = () => handler;
        try
        {
            using (var connection = new TursoConnection("Data Source=https://example.test"))
            {
                connection.Open();
                ExecuteRemote(connection, new TursoParameter { ParameterName = "@g", Value = Value });
            }

            using (var connection = new TursoConnection("Data Source=https://example.test;BinaryGUID=True"))
            {
                connection.Open();
                ExecuteRemote(connection, new TursoParameter { ParameterName = "@g", Value = Value });
                ExecuteRemote(connection, new TursoParameter("@g", DbType.Guid, Value));
            }

            using (var connection = new SqliteConnection("Data Source=https://example.test;BinaryGUID=True"))
            {
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT @g";
                command.Parameters.AddWithValue("@g", Value);
                command.ExecuteNonQuery();
            }
        }
        finally
        {
            TursoConnection.RemoteMessageHandlerFactory = null;
        }

        handler.Arguments.Should().Equal(
            $"text:{Value}",
            $"blob:{base64}",
            $"text:{Value}",
            $"blob:{base64}");
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

    private static void ExecuteRemote(TursoConnection connection, TursoParameter parameter)
    {
        using var command = new TursoCommand(connection, "SELECT @g");
        command.Parameters.Add(parameter);
        command.ExecuteNonQuery();
    }

    private sealed class RecordingPipelineHandler : HttpMessageHandler
    {
        public List<string> Arguments { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            using var document = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
            var results = new List<string>();
            foreach (var item in document.RootElement.GetProperty("requests").EnumerateArray())
            {
                if (item.GetProperty("type").GetString() == "close")
                {
                    results.Add("""{"type":"ok","response":{"type":"close"}}""");
                    continue;
                }

                if (item.TryGetProperty("stmt", out var stmt) && stmt.TryGetProperty("named_args", out var namedArgs))
                {
                    foreach (var arg in namedArgs.EnumerateArray())
                    {
                        var value = arg.GetProperty("value");
                        var type = value.GetProperty("type").GetString();
                        Arguments.Add(type == "blob"
                            ? $"blob:{value.GetProperty("base64").GetString()}"
                            : $"{type}:{value.GetProperty("value").GetString()}");
                    }
                }

                results.Add("""{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0}}}""");
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    $$"""{"baton":null,"results":[{{string.Join(",", results)}}]}""",
                    Encoding.UTF8,
                    "application/json"),
            };
        }
    }
}
