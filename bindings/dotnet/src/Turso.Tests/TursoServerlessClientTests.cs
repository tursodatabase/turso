using System.Net;
using System.Text.Json;
using AwesomeAssertions;
using Turso.Serverless.Client;

namespace Turso.Tests;

public class TursoServerlessClientTests
{
    [Test]
    public async Task DisposingAnUncommittedTransactionRollsBack()
    {
        var handler = new ServerlessHandler();
        using var httpClient = new HttpClient(handler);
        await using var connection = CreateConnection(httpClient);

        await using (var transaction = await connection.BeginTransactionAsync())
        {
            connection.InTransaction.Should().BeTrue();
        }

        connection.InTransaction.Should().BeFalse();
        handler.Sql.Should().Equal("BEGIN DEFERRED", "ROLLBACK");
    }

    [Test]
    public async Task BatchInsideAnInteractiveTransactionDoesNotStartANestedTransaction()
    {
        var handler = new ServerlessHandler();
        using var httpClient = new HttpClient(handler);
        await using var connection = CreateConnection(httpClient);

        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await connection.BatchAsync([new TursoBatchStatement("SELECT 1")], TursoBatchMode.Immediate);
        }

        var expected = new[] { "SELECT 1" };
        handler.Batches.Should().ContainSingle(batch => batch.SequenceEqual(expected));
    }

    [Test]
    public async Task RawTransactionControlInABatchUpdatesTransactionState()
    {
        var handler = new ServerlessHandler();
        using var httpClient = new HttpClient(handler);
        await using var connection = CreateConnection(httpClient);

        await connection.BatchAsync([new TursoBatchStatement("BEGIN")]);
        connection.InTransaction.Should().BeTrue();

        await connection.BatchAsync([new TursoBatchStatement("ROLLBACK")]);
        connection.InTransaction.Should().BeFalse();
    }

    [Test]
    public async Task DisposingWaitsForAnActiveExecution()
    {
        var executionStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseExecution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ServerlessHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri!.AbsolutePath == "/v3/cursor")
            {
                executionStarted.TrySetResult();
                await releaseExecution.Task.WaitAsync(cancellationToken);
                return CursorResponse();
            }

            return PipelineResponse();
        });

        using var httpClient = new HttpClient(handler);
        var connection = CreateConnection(httpClient);
        var execution = connection.ExecuteAsync("SELECT 1");
        await executionStarted.Task;

        var disposal = connection.DisposeAsync().AsTask();
        disposal.IsCompleted.Should().BeFalse();

        releaseExecution.SetResult();
        await execution;
        await disposal;
    }

    private static TursoServerlessConnection CreateConnection(HttpClient httpClient)
    {
        return new TursoServerlessConnection(
            new TursoServerlessConnectionOptions { Url = "https://example.com" },
            httpClient);
    }

    private sealed class ServerlessHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? _handler;

        internal List<string> Sql { get; } = [];
        internal List<IReadOnlyList<string>> Batches { get; } = [];

        internal ServerlessHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? handler = null)
        {
            _handler = handler;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (_handler is not null)
            {
                return await _handler(request, cancellationToken);
            }

            if (request.RequestUri!.AbsolutePath == "/v3/cursor")
            {
                using var document = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
                var statements = document.RootElement.GetProperty("batch").GetProperty("steps")
                    .EnumerateArray()
                    .Select(static step => step.GetProperty("stmt").GetProperty("sql").GetString()!)
                    .ToArray();
                Sql.AddRange(statements);
                Batches.Add(statements);
                return CursorResponse();
            }

            return PipelineResponse();
        }
    }

    private static HttpResponseMessage CursorResponse()
    {
        return JsonResponse("""
            {"baton":"baton"}
            {"type":"step_begin","step":0}
            {"type":"step_end","affected_row_count":0}
            """);
    }

    private static HttpResponseMessage PipelineResponse()
    {
        return JsonResponse("""{"baton":null,"results":[]}""");
    }

    private static HttpResponseMessage JsonResponse(string content)
    {
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(content),
        };
    }
}
