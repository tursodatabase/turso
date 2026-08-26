using System.Data;
using System.Net;
using AwesomeAssertions;
using Turso.Raw.Public;

namespace Turso.Tests;

public sealed class TursoManagedSyncTests
{
    [Test]
    public void SyncAuthTokenIsPinnedToConfiguredOrigin()
    {
        Action crossOrigin = () => TursoSyncDatabase.ValidateAuthTransport(
            new Uri("https://attacker.example/pull-updates"),
            new Uri("https://database.example"),
            "secret");

        crossOrigin.Should().Throw<InvalidOperationException>()
            .WithMessage("*configured remote*");

        TursoSyncDatabase.ValidateAuthTransport(
            new Uri("https://database.example/pull-updates"),
            new Uri("https://database.example"),
            "secret");
    }

    [Test]
    public void SyncOptionsRejectUnsafeAuthTransport()
    {
        new TursoSyncDatabaseOptions("replica.db", new Uri("http://example.test"))
        {
            AuthToken = "secret",
        }.Invoking(x => x.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*HTTPS*");
    }

    [Test]
    public async Task HighLevelCreateDrivesHttpAndSurfacesTheNativeFailure()
    {
        using var httpClient = new HttpClient(new FailingSyncHandler());
        var options = new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
        {
            HttpClient = httpClient,
            AuthToken = "secret",
        };

        var action = async () => await TursoSyncDatabase.CreateAsync(options);

        var exception = await action.Should().ThrowAsync<TursoSyncException>();
        exception.And.Message.Should().NotContain("secret");
        exception.And.Operation.Should().Be(TursoSyncOperationKind.Create);
        exception.And.NativeStatusCode.Should().NotBeNull();
        exception.And.HttpMethod.Should().Be("POST");
        exception.And.Endpoint.Should().Be(new Uri("https://example.test/pull-updates"));
        exception.And.HttpStatusCode.Should().Be(HttpStatusCode.InternalServerError);
    }

    [Test]
    public async Task HttpHandlerSecretsAreRedactedFromTheSyncFailureMessage()
    {
        using var httpClient = new HttpClient(new SecretLeakingHandler());
        var options = new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
        {
            HttpClient = httpClient,
            AuthToken = "secret",
        };

        var action = async () => await TursoSyncDatabase.CreateAsync(options);

        var exception = await action.Should().ThrowAsync<TursoSyncException>();
        exception.And.Message.Should().NotContain("secret");
        exception.And.Message.Should().Contain("[REDACTED]");
        exception.And.ToString().Should().NotContain("secret");
    }

    [Test]
    public void SynchronousCreateDoesNotDependOnTheCallersSynchronizationContext()
    {
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            SynchronizationContext.SetSynchronizationContext(new NonPumpingSynchronizationContext());
            try
            {
                using var httpClient = new HttpClient(new YieldingFailureHandler());
                TursoSyncDatabase.Create(
                    new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
                    {
                        HttpClient = httpClient,
                    });
            }
            catch (TursoSyncException)
            {
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        })
        {
            IsBackground = true,
        };

        thread.Start();

        thread.Join(TimeSpan.FromSeconds(5)).Should().BeTrue();
        failure.Should().BeNull();
    }

    [Test]
    public async Task DeferredBootstrapCreatesAUsableLocalConnection()
    {
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        var database = await TursoSyncDatabase.CreateAsync(
            new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
            {
                BootstrapIfEmpty = false,
                HttpClient = httpClient,
            });
        await using var connection = await database.ConnectAsync();

        connection.ExecuteNonQuery("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)");
        connection.ExecuteNonQuery("INSERT INTO items(name) VALUES ('local')");
        using (var command = new TursoCommand(connection, "SELECT name FROM items"))
            command.ExecuteScalar().Should().Be("local");

        await database.DisposeAsync();
        using var afterDispose = new TursoCommand(connection, "SELECT COUNT(*) FROM items");
        afterDispose.ExecuteScalar().Should().Be(1L);
    }

    [Test]
    public async Task CancellationStopsAnInFlightSyncHttpRequest()
    {
        var handler = new BlockingSyncHandler();
        using var httpClient = new HttpClient(handler);
        await using var database = await TursoSyncDatabase.CreateAsync(
            new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
            {
                BootstrapIfEmpty = false,
                HttpClient = httpClient,
            });
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var canceledPull = async () => await database.PullAsync(cancellation.Token);

        await canceledPull.Should().ThrowAsync<OperationCanceledException>();

        var nextPull = async () => await database.PullAsync();
        await nextPull.Should().ThrowAsync<TursoException>();
        handler.RequestCount.Should().Be(2);
    }

    [Test]
    public async Task FileReplicaCanBeCreatedClosedAndOpenedAgain()
    {
        var directory = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "turso-sync-open-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "replica.db");
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        var options = new TursoSyncDatabaseOptions(path, new Uri("https://example.test"))
        {
            BootstrapIfEmpty = false,
            HttpClient = httpClient,
        };

        try
        {
            await using (var created = await TursoSyncDatabase.CreateAsync(options))
            await using (var connection = await created.ConnectAsync())
            {
                connection.ExecuteNonQuery("CREATE TABLE persisted(value TEXT)");
                connection.ExecuteNonQuery("INSERT INTO persisted VALUES ('yes')");
            }

            await using var opened = await TursoSyncDatabase.OpenAsync(options);
            await using var reopenedConnection = await opened.ConnectAsync();
            using var query = new TursoCommand(reopenedConnection, "SELECT value FROM persisted");
            query.ExecuteScalar().Should().Be("yes");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task PullWaitsForActiveReaders()
    {
        using var httpClient = new HttpClient(new FailingSyncHandler());
        await using var database = await TursoSyncDatabase.CreateAsync(
            new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
            {
                BootstrapIfEmpty = false,
                HttpClient = httpClient,
            });
        await using var connection = await database.ConnectAsync();
        connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
        connection.ExecuteNonQuery("INSERT INTO items VALUES (1)");

        using var command = new TursoCommand(connection, "SELECT value FROM items");
        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();

        var pull = database.PullAsync();
        await Task.Delay(50);
        pull.IsCompleted.Should().BeFalse();

        reader.Dispose();
        var action = async () => await pull;
        await action.Should().ThrowAsync<TursoException>();
    }

    [Test]
    public async Task SyncHttpHandlerCannotReenterTheDatabase()
    {
        var handler = new ReentrantSyncHandler();
        using var httpClient = new HttpClient(handler);
        await using var database = await TursoSyncDatabase.CreateAsync(
            new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
            {
                BootstrapIfEmpty = false,
                HttpClient = httpClient,
            });
        handler.OnSend = database.Dispose;

        var action = async () => await database.PullAsync();

        await action.Should().ThrowAsync<TursoException>();
        handler.ReentryFailure.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("cannot be reentered");
    }

    [Test]
    public async Task ClosingAConnectionClosesItsActiveReaderBeforeReleasingSyncOwnership()
    {
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        await using var database = await TursoSyncDatabase.CreateAsync(
            new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
            {
                BootstrapIfEmpty = false,
                HttpClient = httpClient,
            });
        await using var connection = await database.ConnectAsync();
        connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
        connection.ExecuteNonQuery("INSERT INTO items VALUES (1)");
        using var command = new TursoCommand(connection, "SELECT value FROM items");
        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();

        connection.Close();

        connection.State.Should().Be(ConnectionState.Closed);
        reader.IsClosed.Should().BeTrue();
    }

    [Test]
    public async Task ConnectionQueuedBeforeDisposeIsRejectedAfterItGetsTheGate()
    {
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        var database = await TursoSyncDatabase.CreateAsync(
            new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
            {
                BootstrapIfEmpty = false,
                HttpClient = httpClient,
            });
        var gate = database.EnterConnectionOperation();
        var connecting = database.ConnectAsync();
        await Task.Delay(20);
        var disposing = Task.Run(database.Dispose);

        gate.Dispose();
        var connectAction = async () => await connecting;
        await connectAction.Should().ThrowAsync<ObjectDisposedException>();
        await disposing;
    }

    private sealed class FailingSyncHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            request.RequestUri.Should().Be(new Uri("https://example.test/pull-updates"));
            request.Headers.Authorization?.Scheme.Should().Be("Bearer");
            request.Headers.Authorization?.Parameter.Should().Be("secret");
            request.Content?.Headers.ContentType?.MediaType.Should().Be("application/protobuf");
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new ByteArrayContent([]),
            });
        }
    }

    private sealed class SecretLeakingHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            throw new HttpRequestException("transport exposed secret");
        }
    }

    private sealed class YieldingFailureHandler : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            await Task.Yield();
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new ByteArrayContent([]),
            };
        }
    }

    private sealed class NonPumpingSynchronizationContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback callback, object? state)
        {
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

    private sealed class BlockingSyncHandler : HttpMessageHandler
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

    private sealed class ReentrantSyncHandler : HttpMessageHandler
    {
        public Action? OnSend { get; set; }
        public Exception? ReentryFailure { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            try
            {
                OnSend?.Invoke();
            }
            catch (Exception exception)
            {
                ReentryFailure = exception;
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new ByteArrayContent([]),
            });
        }
    }
}
