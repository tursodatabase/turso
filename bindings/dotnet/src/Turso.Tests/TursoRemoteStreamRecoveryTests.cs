using System.Net;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;

namespace Turso.Tests;

public sealed class TursoRemoteStreamRecoveryTests
{
    [Test]
    public async Task StatelessCommandRetriesOneExpiredStreamWithoutTheOldBaton()
    {
        using var handler = new ScriptedPipelineHandler(
            ExecuteSuccess("stale", 1),
            StreamExpired("stale stream", "stale-again"),
            ExecuteSuccess("fresh", 2));
        using var scope = UseRemoteHandler(handler);
        using var connection = new TursoConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        connection.Open();

        (await ExecuteScalarAsync(connection)).Should().Be(1L);
        (await ExecuteScalarAsync(connection)).Should().Be(2L);

        handler.Requests.Should().HaveCount(3);
        handler.GetBaton(1).Should().Be("stale");
        handler.GetBaton(2).Should().BeNull();
    }

    [Test]
    public async Task NonExpiryRemoteErrorIsNotRetried()
    {
        using var handler = new ScriptedPipelineHandler(
            ExecuteSuccess("stale", 1),
            RemoteError("SQLITE_CONSTRAINT", "stream expired"));
        using var scope = UseRemoteHandler(handler);
        using var connection = new TursoConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        connection.Open();
        await ExecuteScalarAsync(connection);

        var exception = Assert.ThrowsAsync<TursoRemoteSqlException>(
            async () => await ExecuteScalarAsync(connection));

        exception!.RemoteErrorCode.Should().Be("SQLITE_CONSTRAINT");
        exception.IsStreamExpired.Should().BeFalse();
        handler.Requests.Should().HaveCount(2);
    }

    [Test]
    public async Task PartiallyEvaluatedExpiredBatchIsNotRetried()
    {
        using var handler = new ScriptedPipelineHandler(
            PartiallyExpiredBatch(),
            ExecuteSuccess("fresh", 2));
        using var scope = UseRemoteHandler(handler);
        using var connection = new TursoConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        connection.Open();
        await using var batch = (TursoBatch)connection.CreateBatch();
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items VALUES (1)"));
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items VALUES (2)"));

        var batchAction = async () => await batch.ExecuteNonQueryAsync(CancellationToken.None);

        await batchAction.Should().ThrowAsync<TursoRemoteSqlException>()
            .Where(exception => exception.IsStreamExpired);
        (await ExecuteScalarAsync(connection)).Should().Be(2L);
        handler.Requests.Should().HaveCount(2);
        handler.GetBaton(1).Should().BeNull();
    }

    [TestCase("commit")]
    [TestCase("rollback")]
    public void ExpiredTransactionPreservesItsRootFailure(string completion)
    {
        using var handler = new ScriptedPipelineHandler(
            ExecuteSuccess("transaction", 0),
            StreamExpired("transaction stream expired"));
        using var scope = UseRemoteHandler(handler);
        using var connection = new TursoConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        connection.Open();
        using var transaction = (TursoTransaction)connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "UPDATE t SET value = 1";

        var rootFailure = Assert.Throws<TursoRemoteSqlException>(() => command.ExecuteNonQuery())!;
        rootFailure.RemoteErrorCode.Should().Be("STREAM_EXPIRED");

        Assert.Throws<TursoRemoteSqlException>(() => command.ExecuteNonQuery())
            .Should().BeSameAs(rootFailure);
        using var batch = (TursoBatch)connection.CreateBatch();
        batch.Transaction = transaction;
        batch.BatchCommands.Add(new TursoBatchCommand("UPDATE t SET value = 2"));
        Assert.Throws<TursoRemoteSqlException>(() => batch.ExecuteNonQuery())
            .Should().BeSameAs(rootFailure);
        var completionFailure = completion == "commit"
            ? Assert.Throws<TursoRemoteSqlException>(() => transaction.Commit())
            : Assert.Throws<TursoRemoteSqlException>(() => transaction.Rollback());
        completionFailure.Should().BeSameAs(rootFailure);
        handler.Requests.Should().HaveCount(2);
    }

    [Test]
    public void FaultedTransactionDisposalDoesNotMaskAnExceptionInFlight()
    {
        using var handler = new ScriptedPipelineHandler(
            ExecuteSuccess("transaction", 0),
            StreamExpired("transaction stream expired"));
        using var scope = UseRemoteHandler(handler);
        using var connection = new TursoConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        connection.Open();

        var marker = Assert.Throws<MarkerException>(() =>
        {
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "UPDATE t SET value = 1";
            Assert.Throws<TursoRemoteSqlException>(() => command.ExecuteNonQuery());
            throw new MarkerException();
        });

        marker.Should().NotBeNull();
        handler.Requests.Should().HaveCount(2);
    }

    [Test]
    public void CommitTimeExpiryIsPreservedAndCompletesTheTransaction()
    {
        using var handler = new ScriptedPipelineHandler(
            ExecuteSuccess("transaction", 0),
            StreamExpired("commit stream expired", "unusable"),
            ExecuteSuccess("fresh", 1));
        using var scope = UseRemoteHandler(handler);
        using var connection = new TursoConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        connection.Open();
        using var transaction = (TursoTransaction)connection.BeginTransaction();

        var failure = Assert.Throws<TursoRemoteSqlException>(() => transaction.Commit());

        failure!.RemoteErrorCode.Should().Be("STREAM_EXPIRED");
        transaction.IsCompleted.Should().BeTrue();
        ExecuteScalarAsync(connection).GetAwaiter().GetResult().Should().Be(1L);
        handler.Requests.Should().HaveCount(3);
        handler.GetBaton(2).Should().BeNull();
    }

    private static async Task<object?> ExecuteScalarAsync(TursoConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        return await command.ExecuteScalarAsync();
    }

    private static IDisposable UseRemoteHandler(HttpMessageHandler handler)
    {
        TursoConnection.RemoteMessageHandlerFactory = () => handler;
        return new Scope(() => TursoConnection.RemoteMessageHandlerFactory = null);
    }

    private static string ExecuteSuccess(string baton, long value)
        => """
           {"baton":"__BATON__","results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"value","decltype":"INTEGER"}],"rows":[[{"type":"integer","value":"__VALUE__"}]],"affected_row_count":0}}}]}
           """
            .Replace("__BATON__", baton, StringComparison.Ordinal)
            .Replace("__VALUE__", value.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal);

    private static string StreamExpired(string message, string? baton = null)
    {
        var error = RemoteError("STREAM_EXPIRED", message);
        return baton is null
            ? error
            : error.Replace("{\"results\"", $"{{\"baton\":\"{baton}\",\"results\"", StringComparison.Ordinal);
    }

    private static string RemoteError(string code, string message)
        => """
           {"results":[{"type":"error","error":{"message":"__MESSAGE__","code":"__CODE__"}}]}
           """
            .Replace("__MESSAGE__", message, StringComparison.Ordinal)
            .Replace("__CODE__", code, StringComparison.Ordinal);

    private static string PartiallyExpiredBatch()
        => """
           {
             "baton":"stale",
             "results":[{
               "type":"ok",
               "response":{
                 "type":"batch",
                 "result":{
                   "step_results":[
                     {"cols":[],"rows":[],"affected_row_count":1},
                     null
                   ],
                   "step_errors":[
                     null,
                     {"message":"stream expired","code":"STREAM_EXPIRED"}
                   ]
                 }
               }
             }]
           }
           """;

    private sealed class ScriptedPipelineHandler(params string[] responses) : HttpMessageHandler
    {
        private readonly Queue<string> _responses = new(responses);

        public List<JsonElement> Requests { get; } = [];

        public string? GetBaton(int index)
        {
            var request = Requests[index];
            return !request.TryGetProperty("baton", out var baton) || baton.ValueKind == JsonValueKind.Null
                ? null
                : baton.GetString();
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            using var document = JsonDocument.Parse(
                await request.Content!.ReadAsStringAsync(cancellationToken));
            var root = document.RootElement.Clone();
            Requests.Add(root);
            var requestType = root.GetProperty("requests")[0].GetProperty("type").GetString();
            var response = requestType == "close"
                ? """{"results":[{"type":"ok","response":{"type":"close"}}]}"""
                : _responses.Dequeue();
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(response, Encoding.UTF8, "application/json"),
            };
        }
    }

    private sealed class Scope(Action onDispose) : IDisposable
    {
        public void Dispose() => onDispose();
    }

    private sealed class MarkerException : Exception;
}
