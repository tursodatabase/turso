using AwesomeAssertions;
using System.Net;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoReplicaPoolingTests
{
    [Test]
    public async Task PooledConnectionsShareOneSyncDatabaseUntilLastClose()
    {
        var directory = NewDirectory();
        var path = Path.Combine(directory, "replica.db");
        try
        {
            using var first = new TursoConnection(ConnectionString(path, pooling: true));
            using var second = new TursoConnection(ConnectionString(path, pooling: true));
            await Task.WhenAll(first.OpenAsync(), second.OpenAsync());

            first.SyncDatabase.Should().BeSameAs(second.SyncDatabase);
            first.ExecuteNonQuery("CREATE TABLE items(value TEXT)");
            first.ExecuteNonQuery("INSERT INTO items VALUES ('shared')");
            using (var query = new TursoCommand(second, "SELECT value FROM items"))
                query.ExecuteScalar().Should().Be("shared");

            first.Close();
            first.Close();
            using var afterFirstClose = new TursoCommand(second, "SELECT COUNT(*) FROM items");
            afterFirstClose.ExecuteScalar().Should().Be(1L);
            second.Close();

            using var reopened = new TursoConnection(ConnectionString(path, pooling: false));
            reopened.Open();
            using var persisted = new TursoCommand(reopened, "SELECT value FROM items");
            persisted.ExecuteScalar().Should().Be("shared");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task EquivalentAbsoluteAndRelativePathsShareOneReplica()
    {
        var directory = NewDirectory();
        var absolutePath = Path.Combine(directory, "replica.db");
        var relativePath = Path.GetRelativePath(Environment.CurrentDirectory, absolutePath);
        try
        {
            using var first = new TursoConnection(ConnectionString(absolutePath, pooling: true));
            using var second = new TursoConnection(ConnectionString(relativePath, pooling: true));

            await Task.WhenAll(first.OpenAsync(), second.OpenAsync());

            first.SyncDatabase.Should().BeSameAs(second.SyncDatabase);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task InitializationIsSingleFlightAndFailureCanBeRetried()
    {
        var directory = NewDirectory();
        var path = Path.Combine(directory, "replica.db");
        var handler = new BlockingFailureHandler();
        using var failingClient = new HttpClient(handler);
        using var retryClient = new HttpClient(new UnexpectedHttpHandler());
        HttpClient activeClient = failingClient;
        TursoConnection.SyncHttpClientFactory = () => activeClient;
        try
        {
            using var first = new TursoConnection(
                $"Data Source=turso://example.test;Replica Path={path};Pooling=True");
            using var second = new TursoConnection(
                $"Data Source=turso://example.test;Replica Path={path};Pooling=True");

            var firstOpen = first.OpenAsync();
            await handler.RequestStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var secondOpen = second.OpenAsync();
            handler.ReleaseResponse();

            var opening = async () => await Task.WhenAll(firstOpen, secondOpen);
            await opening.Should().ThrowAsync<TursoSyncException>();
            handler.RequestCount.Should().Be(1);

            activeClient = retryClient;
            using var retry = new TursoConnection(ConnectionString(path, pooling: true));
            await retry.OpenAsync();
            retry.State.Should().Be(System.Data.ConnectionState.Open);
        }
        finally
        {
            handler.ReleaseResponse();
            TursoConnection.SyncHttpClientFactory = null;
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public void MemoryReplicasAreNeverShared()
    {
        using var first = new TursoConnection(ConnectionString(":memory:", pooling: true));
        using var second = new TursoConnection(ConnectionString(":memory:", pooling: true));

        first.Open();
        second.Open();

        first.SyncDatabase.Should().NotBeSameAs(second.SyncDatabase);
        first.ExecuteNonQuery("CREATE TABLE only_in_first(value INTEGER)");
        using var query = new TursoCommand(
            second,
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'only_in_first'");
        query.ExecuteScalar().Should().Be(0L);
    }

    [Test]
    public void PooledConnectionsRejectConflictingOptionsWithoutLeakingSecrets()
    {
        var directory = NewDirectory();
        var path = Path.Combine(directory, "replica.db");
        const string firstSecret = "first-secret-value";
        const string secondSecret = "second-secret-value";
        try
        {
            using var first = new TursoConnection(
                ConnectionString(path, pooling: true) + $";Auth Token={firstSecret}");
            using var conflicting = new TursoConnection(
                ConnectionString(path, pooling: true) + $";Auth Token={secondSecret}");
            first.Open();

            var exception = conflicting.Invoking(x => x.Open())
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*different options*")
                .Which;
            exception.ToString().Should().NotContain(firstSecret).And.NotContain(secondSecret);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public void NonPooledReplicaHasAnExclusivePathLease()
    {
        var directory = NewDirectory();
        var path = Path.Combine(directory, "replica.db");
        try
        {
            using var first = new TursoConnection(ConnectionString(path, pooling: false));
            using var second = new TursoConnection(ConnectionString(path, pooling: false));
            first.Open();

            second.Invoking(x => x.Open())
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*already open exclusively*");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task PooledConnectionsUseOneAutomaticSyncScheduler()
    {
        var directory = NewDirectory();
        var path = Path.Combine(directory, "replica.db");
        var handler = new CountingFailureHandler();
        using var httpClient = new HttpClient(handler);
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        var first = new TursoConnection(
            ConnectionString(path, pooling: true) + ";Sync Interval=1");
        var second = new TursoConnection(
            ConnectionString(path, pooling: true) + ";Sync Interval=1");
        try
        {
            await Task.WhenAll(first.OpenAsync(), second.OpenAsync());
            await WaitForFaultAsync(first);

            first.AutomaticSyncStatus.State.Should().Be(TursoAutomaticSyncState.Faulted);
            second.AutomaticSyncStatus.State.Should().Be(TursoAutomaticSyncState.Faulted);
            handler.RequestCount.Should().Be(1);
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
            CloseFaulted(first);
            CloseFaulted(second);
            first.Dispose();
            second.Dispose();
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task WaitForFaultAsync(TursoConnection connection)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (connection.AutomaticSyncStatus.State != TursoAutomaticSyncState.Faulted
               && DateTime.UtcNow < deadline)
        {
            await Task.Delay(25);
        }
    }

    private static void CloseFaulted(TursoConnection connection)
    {
        try
        {
            connection.Close();
        }
        catch (TursoSyncException)
        {
        }
    }

    private static string ConnectionString(string path, bool pooling)
    {
        return $"Data Source=turso://example.test;Replica Path={path};"
               + $"Bootstrap If Empty=False;Pooling={pooling}";
    }

    private static string NewDirectory()
    {
        var path = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "turso-replica-pool-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class CountingFailureHandler : HttpMessageHandler
    {
        public int RequestCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestCount++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new ByteArrayContent([]),
            });
        }
    }

    private sealed class BlockingFailureHandler : HttpMessageHandler
    {
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource RequestStarted { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public int RequestCount { get; private set; }

        public void ReleaseResponse()
        {
            _release.TrySetResult();
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestCount++;
            RequestStarted.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new ByteArrayContent([]),
            };
        }
    }

    private sealed class UnexpectedHttpHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            throw new AssertionException($"Unexpected HTTP request: {request.RequestUri}");
        }
    }
}
