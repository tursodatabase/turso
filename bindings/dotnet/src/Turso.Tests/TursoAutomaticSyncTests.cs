using System.Collections.Concurrent;
using System.Net;
using AwesomeAssertions;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoAutomaticSyncTests
{
    [Test]
    public async Task TerminalAutomaticSyncFailureIsObservableBeforeClose()
    {
        using var httpClient = new HttpClient(new FailingSyncHandler());
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=:memory:;"
            + "Bootstrap If Empty=False;Sync Interval=1");
        var faulted = new TaskCompletionSource<TursoAutomaticSyncStatus>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        connection.AutomaticSyncStatusChanged += (_, _) => throw new InvalidOperationException("observer failure");
        connection.AutomaticSyncStatusChanged += (_, args) =>
        {
            if (args.Status.State == TursoAutomaticSyncState.Faulted)
                faulted.TrySetResult(args.Status);
        };

        try
        {
            connection.Open();
            connection.AutomaticSyncStatus.State.Should().Be(TursoAutomaticSyncState.Waiting);

            var status = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));

            status.LastAttempt.Should().NotBeNull();
            status.LastException.Should().BeOfType<TursoSyncException>()
                .Which.Operation.Should().Be(TursoSyncOperationKind.Pull);
            status.NextAttempt.Should().BeNull();
            connection.AutomaticSyncStatus.Should().Be(status);
            connection.Invoking(x => x.Dispose()).Should().Throw<TursoSyncException>();
            connection.Invoking(x => x.Open()).Should().Throw<ObjectDisposedException>();
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
            connection.Dispose();
        }
    }

    [Test]
    public async Task StatusHandlerCanCloseTheLastReplicaWithoutDeadlock()
    {
        using var httpClient = new HttpClient(new FailingSyncHandler());
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=:memory:;"
            + "Bootstrap If Empty=False;Sync Interval=1");
        var states = new ConcurrentQueue<TursoAutomaticSyncState>();
        var closed = new TaskCompletionSource<Exception?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var stopped = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        connection.AutomaticSyncStatusChanged += (_, args) =>
        {
            if (args.Status.State != TursoAutomaticSyncState.Faulted)
                return;
            try
            {
                connection.Close();
                closed.TrySetResult(null);
            }
            catch (Exception exception)
            {
                closed.TrySetResult(exception);
            }
        };
        connection.AutomaticSyncStatusChanged += (_, args) =>
        {
            states.Enqueue(args.Status.State);
            if (args.Status.State == TursoAutomaticSyncState.Stopped)
                stopped.TrySetResult();
        };

        try
        {
            connection.Open();

            var closeError = await closed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await stopped.Task.WaitAsync(TimeSpan.FromSeconds(5));

            closeError.Should().BeOfType<TursoSyncException>();
            connection.State.Should().Be(System.Data.ConnectionState.Closed);
            connection.AutomaticSyncStatus.State.Should().Be(TursoAutomaticSyncState.Stopped);
            states.Should().Contain(TursoAutomaticSyncState.Waiting)
                .And.Contain(TursoAutomaticSyncState.Faulted)
                .And.Contain(TursoAutomaticSyncState.Stopped);
            await Task.Delay(100);
            connection.AutomaticSyncStatus.State.Should().Be(TursoAutomaticSyncState.Stopped);
            states.SkipWhile(state => state != TursoAutomaticSyncState.Stopped)
                .Should().OnlyContain(state => state == TursoAutomaticSyncState.Stopped);
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
            connection.Dispose();
        }
    }

    [TestCase(TransientFailureKind.Transport)]
    [TestCase(TransientFailureKind.Timeout)]
    public async Task TransientFailuresRetryTwiceBeforeBecomingTerminal(
        TransientFailureKind failureKind)
    {
        var handler = new TransientFailureHandler(failureKind);
        using var httpClient = new HttpClient(handler);
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=:memory:;"
            + "Bootstrap If Empty=False;Sync Interval=1");
        var faulted = new TaskCompletionSource<TursoAutomaticSyncStatus>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var states = new ConcurrentQueue<TursoAutomaticSyncState>();
        connection.AutomaticSyncStatusChanged += (_, args) =>
        {
            states.Enqueue(args.Status.State);
            if (args.Status.State == TursoAutomaticSyncState.Faulted)
                faulted.TrySetResult(args.Status);
        };

        try
        {
            connection.Open();

            var status = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));

            handler.RequestCount.Should().Be(3);
            status.Attempt.Should().Be(3);
            status.LastException.Should().BeOfType<TursoSyncException>();
            states.Should().Contain(TursoAutomaticSyncState.Retrying);
            connection.Invoking(x => x.Close()).Should().Throw<TursoSyncException>();
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
            connection.Dispose();
        }
    }

    private sealed class FailingSyncHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new ByteArrayContent([]),
            });
        }
    }

    private sealed class TransientFailureHandler(TransientFailureKind failureKind) : HttpMessageHandler
    {
        public int RequestCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestCount++;
            throw failureKind switch
            {
                TransientFailureKind.Transport =>
                    new HttpRequestException("transient transport failure"),
                TransientFailureKind.Timeout =>
                    new TaskCanceledException("request timeout", new TimeoutException()),
                _ => throw new ArgumentOutOfRangeException(nameof(failureKind)),
            };
        }
    }

    public enum TransientFailureKind
    {
        Transport,
        Timeout,
    }
}
