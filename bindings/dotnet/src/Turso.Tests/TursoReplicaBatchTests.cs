using AwesomeAssertions;
using Turso.Raw.Public;

namespace Turso.Tests;

public sealed class TursoReplicaBatchTests
{
    [Test]
    public async Task ReplicaBatchReturnsOrderedResultsAndAffectedRows()
    {
        using var unopenedReplica = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=:memory:;Bootstrap If Empty=False");
        unopenedReplica.CanCreateBatch.Should().BeFalse();

        await using var database = await CreateDatabaseAsync();
        await using var connection = await database.ConnectAsync();
        connection.CanCreateBatch.Should().BeTrue();
        connection.ExecuteNonQuery("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)");
        await using var batch = (TursoBatch)connection.CreateBatch();

        var insert = new TursoBatchCommand("INSERT INTO items(name) VALUES ($name)");
        insert.Parameters.AddWithValue("$name", "alice");
        batch.BatchCommands.Add(insert);
        batch.BatchCommands.Add(new TursoBatchCommand("SELECT id, name FROM items ORDER BY id"));

        await using var reader = await batch.ExecuteReaderAsync();

        reader.Read().Should().BeFalse();
        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(1);
        reader.GetString(1).Should().Be("alice");
        reader.Read().Should().BeFalse();
        reader.NextResult().Should().BeFalse();
        insert.RecordsAffected.Should().Be(1);
        batch.BatchCommands[1].RecordsAffected.Should().Be(0);
        reader.RecordsAffected.Should().Be(1);
        await reader.DisposeAsync();

        await using var scalarBatch = (TursoBatch)connection.CreateBatch();
        scalarBatch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items(name) VALUES ('bob')"));
        scalarBatch.BatchCommands.Add(new TursoBatchCommand("SELECT name FROM items WHERE id = 2"));
        (await scalarBatch.ExecuteScalarAsync(CancellationToken.None)).Should().Be("bob");
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task ReplicaBatchParticipatesInExplicitTransaction(bool commit)
    {
        await using var database = await CreateDatabaseAsync();
        await using var connection = await database.ConnectAsync();
        connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
        using var transaction = (TursoTransaction)connection.BeginTransaction();
        await using var batch = (TursoBatch)connection.CreateBatch();
        batch.Transaction = transaction;
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items VALUES (1)"));
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items VALUES (2)"));

        (await batch.ExecuteNonQueryAsync(CancellationToken.None)).Should().Be(2);
        if (commit)
            transaction.Commit();
        else
            transaction.Rollback();

        using var count = new TursoCommand(connection, "SELECT COUNT(*) FROM items");
        count.ExecuteScalar().Should().Be(commit ? 2L : 0L);
    }

    [Test]
    public async Task ReplicaBatchWithoutTransactionStopsAfterFailureWithoutRollingBackPriorCommands()
    {
        await using var database = await CreateDatabaseAsync();
        await using var connection = await database.ConnectAsync();
        connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
        await using var batch = (TursoBatch)connection.CreateBatch();
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items VALUES (1)"));
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO missing VALUES (2)"));
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items VALUES (3)"));

        var action = async () => await batch.ExecuteNonQueryAsync(CancellationToken.None);

        await action.Should().ThrowAsync<TursoException>();
        using var values = new TursoCommand(connection, "SELECT group_concat(value) FROM items");
        values.ExecuteScalar().Should().Be("1");
    }

    [Test]
    public async Task CanceledReplicaBatchDoesNotExecuteCommands()
    {
        await using var database = await CreateDatabaseAsync();
        await using var connection = await database.ConnectAsync();
        connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
        await using var batch = (TursoBatch)connection.CreateBatch();
        batch.BatchCommands.Add(new TursoBatchCommand("INSERT INTO items VALUES (1)"));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var action = async () => await batch.ExecuteNonQueryAsync(cancellation.Token);

        await action.Should().ThrowAsync<OperationCanceledException>();
        using var count = new TursoCommand(connection, "SELECT COUNT(*) FROM items");
        count.ExecuteScalar().Should().Be(0L);
    }

    [Test]
    public async Task ActiveReplicaBatchReaderBlocksSyncUntilDisposed()
    {
        var handler = new BlockingHttpHandler();
        using var httpClient = new HttpClient(handler);
        await using var database = await CreateDatabaseAsync(httpClient);
        await using var connection = await database.ConnectAsync();
        connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
        await using var batch = (TursoBatch)connection.CreateBatch();
        batch.BatchCommands.Add(new TursoBatchCommand("SELECT 1"));
        var reader = await batch.ExecuteReaderAsync();
        using var cancellation = new CancellationTokenSource();

        var pull = database.PullAsync(cancellation.Token);
        await Task.Delay(100);

        handler.RequestStarted.Task.IsCompleted.Should().BeFalse();
        await reader.DisposeAsync();
        await handler.RequestStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        cancellation.Cancel();
        var pullAction = async () => await pull;
        await pullAction.Should().ThrowAsync<OperationCanceledException>();
    }

    [Test]
    public async Task ClosingReplicaClosesActiveBatchReader()
    {
        await using var database = await CreateDatabaseAsync();
        var connection = await database.ConnectAsync();
        connection.ExecuteNonQuery("CREATE TABLE items(value INTEGER)");
        await using var batch = (TursoBatch)connection.CreateBatch();
        batch.BatchCommands.Add(new TursoBatchCommand("SELECT 1"));
        var reader = await batch.ExecuteReaderAsync();

        connection.Close();

        reader.IsClosed.Should().BeTrue();
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
        await reader.DisposeAsync();
        await connection.DisposeAsync();
    }

    private static Task<TursoSyncDatabase> CreateDatabaseAsync(HttpClient? httpClient = null)
    {
        return TursoSyncDatabase.CreateAsync(
            new TursoSyncDatabaseOptions(":memory:", new Uri("https://example.test"))
            {
                BootstrapIfEmpty = false,
                HttpClient = httpClient,
            });
    }

    private sealed class BlockingHttpHandler : HttpMessageHandler
    {
        public TaskCompletionSource RequestStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestStarted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("The blocking sync request unexpectedly completed.");
        }
    }
}
