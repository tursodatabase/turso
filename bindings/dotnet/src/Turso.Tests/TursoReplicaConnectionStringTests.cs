using System.Data;
using System.Net;
using AwesomeAssertions;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoReplicaConnectionStringTests
{
    [Test]
    public void ReplicaConnectionStringMapsAdvancedSyncOptions()
    {
        var builder = new TursoConnectionStringBuilder
        {
            DataSource = "turso://example.test",
            AuthToken = "secret",
            ReplicaPath = "replica.db",
            SyncClientName = "consumer",
            SyncLongPollTimeout = 2500,
            BootstrapIfEmpty = true,
            PartialBootstrapPrefix = 8192,
            PartialSyncSegmentSize = 4096,
            PartialSyncPrefetch = true,
            PushOperationsThreshold = 100,
            PullBytesThreshold = 65536,
            ForceLogicalMvccPull = true,
            SyncExperimentalFeatures = "views",
        };

        using var connection = new TursoConnection(builder.ConnectionString);
        var options = connection.CreateReplicaOptions();

        options.Path.Should().Be("replica.db");
        options.RemoteUri.Should().Be(new Uri("https://example.test/"));
        options.AuthToken.Should().Be("secret");
        options.ClientName.Should().Be("consumer");
        options.LongPollTimeout.Should().Be(TimeSpan.FromMilliseconds(2500));
        options.BootstrapIfEmpty.Should().BeTrue();
        options.PartialSync!.PrefixLength.Should().Be(8192);
        options.PartialSync.SegmentSize.Should().Be(4096);
        options.PartialSync.Prefetch.Should().BeTrue();
        options.PushOperationsThreshold.Should().Be(100);
        options.PullBytesThreshold.Should().Be(65536);
        options.ForceLogicalMvccPull.Should().BeTrue();
        options.ExperimentalFeatures.Should().Be("views");
    }

    [Test]
    public void ReplicaConnectionStringMapsRemoteEncryption()
    {
        using var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=replica.db;"
            + "Remote Encryption Cipher=aes256gcm;Remote Encryption Key=base64-key");

        var options = connection.CreateReplicaOptions();

        options.RemoteEncryption!.Cipher.Should().Be(TursoRemoteEncryptionCipher.Aes256Gcm);
        options.RemoteEncryption.Key.Should().Be("base64-key");
    }

    [Test]
    public void LocalEncryptionKeysAreRejectedForRemoteReplicas()
    {
        using var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=replica.db;"
            + "Encryption Cipher=aes256gcm;Encryption Key=hex-key");

        connection.Invoking(x => x.CreateReplicaOptions())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*Remote Encryption Cipher*Remote Encryption Key*");
    }

    [Test]
    public void PartialSyncModifiersRequireABootstrapStrategy()
    {
        using var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=replica.db;"
            + "Partial Sync Segment Size=4096;Partial Sync Prefetch=True");

        connection.CreateReplicaOptions().Invoking(options => options.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*exactly one*");
    }

    [TestCase(-1)]
    [TestCase(4_294_968)]
    public void InvalidAutomaticSyncIntervalsAreRejected(int interval)
    {
        using var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=:memory:;"
            + $"Bootstrap If Empty=False;Sync Interval={interval}");

        connection.Invoking(x => x.Open())
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("SyncInterval");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task MemoryReplicaSupportsSynchronousAndAsynchronousOpen(bool openAsync)
    {
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        try
        {
            using var connection = new TursoConnection(
                "Data Source=turso://example.test;Replica Path=:memory:;Bootstrap If Empty=False");

            if (openAsync)
                await connection.OpenAsync();
            else
                connection.Open();

            connection.State.Should().Be(ConnectionState.Open);
            connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
            using var command = new TursoCommand(connection, "SELECT COUNT(*) FROM items");
            command.ExecuteScalar().Should().Be(0L);
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
        }
    }

    [Test]
    public void FileReplicaCanBeClosedAndReopened()
    {
        var directory = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "turso-connection-replica-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "replica.db");
        var connectionString =
            $"Data Source=turso://example.test;Replica Path={path};Bootstrap If Empty=False";
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        try
        {
            using (var created = new TursoConnection(connectionString))
            {
                created.Open();
                created.ExecuteNonQuery("CREATE TABLE persisted(value TEXT)");
                created.ExecuteNonQuery("INSERT INTO persisted VALUES ('yes')");
            }

            using var reopened = new TursoConnection(connectionString);
            reopened.Open();
            using var query = new TursoCommand(reopened, "SELECT value FROM persisted");
            query.ExecuteScalar().Should().Be("yes");
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task CanceledAsyncOpenLeavesConnectionClosedAndReusable()
    {
        var handler = new CancelThenRejectHandler();
        using var httpClient = new HttpClient(handler);
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        try
        {
            using var connection = new TursoConnection(
                "Data Source=turso://example.test;Replica Path=:memory:");
            using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

            var canceledOpen = async () => await connection.OpenAsync(cancellation.Token);
            await canceledOpen.Should().ThrowAsync<OperationCanceledException>();
            connection.State.Should().Be(ConnectionState.Closed);

            var nextOpen = async () => await connection.OpenAsync();
            await nextOpen.Should().ThrowAsync<TursoSyncException>();
            connection.State.Should().Be(ConnectionState.Closed);
            handler.RequestCount.Should().Be(2);
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
        }
    }

    [Test]
    public async Task ExplicitSyncPullsAndRecoversAfterCancellation()
    {
        var handler = new CancelThenRejectHandler();
        using var httpClient = new HttpClient(handler);
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        try
        {
            using var connection = new TursoConnection(
                "Data Source=turso://example.test;Replica Path=:memory:;Bootstrap If Empty=False");
            await connection.OpenAsync();
            using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

            var canceledSync = async () => await connection.SyncAsync(cancellation.Token);
            await canceledSync.Should().ThrowAsync<OperationCanceledException>();

            var nextSync = async () => await connection.SyncAsync();
            await nextSync.Should().ThrowAsync<TursoSyncException>();
            handler.RequestCount.Should().Be(2);
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
        }
    }

    [Test]
    public void ClosingReplicaClosesActiveReadersBeforeOwnedDatabase()
    {
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        TursoConnection.SyncHttpClientFactory = () => httpClient;
        try
        {
            using var connection = new TursoConnection(
                "Data Source=turso://example.test;Replica Path=:memory:;Bootstrap If Empty=False");
            connection.Open();
            connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
            connection.ExecuteNonQuery("INSERT INTO items VALUES (1)");
            using var command = new TursoCommand(connection, "SELECT value FROM items");
            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();

            connection.Close();

            connection.State.Should().Be(ConnectionState.Closed);
            reader.IsClosed.Should().BeTrue();
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
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

    private sealed class CancelThenRejectHandler : HttpMessageHandler
    {
        public int RequestCount { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestCount++;
            if (RequestCount == 1)
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);

            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new ByteArrayContent([]),
            };
        }
    }
}
