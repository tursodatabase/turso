using AwesomeAssertions;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoCloudAcceptanceTests
{
    [Test]
    public async Task AutomaticSyncPullsRemoteChanges()
    {
        var (remoteUrl, authToken) = GetCloudCredentials();
        var tableName = "dotnet_interval_" + Guid.NewGuid().ToString("N");
        var replicaDirectory = NewReplicaDirectory();
        var replicaPath = Path.Combine(replicaDirectory, "replica.db");
        var remoteConnectionString = $"Data Source={remoteUrl};Auth Token={authToken}";

        await using var remote = new TursoConnection(remoteConnectionString);
        await remote.OpenAsync();
        try
        {
            await ExecuteNonQueryAsync(remote, $"CREATE TABLE {tableName}(id INTEGER PRIMARY KEY, value INTEGER)");
            await ExecuteNonQueryAsync(remote, $"INSERT INTO {tableName} VALUES (1, 1)");
            await using var replica = new TursoConnection(
                remoteConnectionString
                + $";Replica Path={replicaPath};Pooling=False;Sync Interval=1");
            var synchronized = new TaskCompletionSource<TursoAutomaticSyncStatus>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            replica.AutomaticSyncStatusChanged += (_, args) =>
            {
                if (args.Status.LastPullAppliedChanges == true)
                    synchronized.TrySetResult(args.Status);
            };
            await replica.OpenAsync();

            await ExecuteNonQueryAsync(remote, $"UPDATE {tableName} SET value = 2 WHERE id = 1");
            var status = await synchronized.Task.WaitAsync(TimeSpan.FromSeconds(10));

            status.LastSuccess.Should().NotBeNull();
            await using var verify = replica.CreateCommand();
            verify.CommandText = $"SELECT value FROM {tableName} WHERE id = 1";
            (await verify.ExecuteScalarAsync()).Should().Be(2L);
        }
        finally
        {
            await ExecuteNonQueryAsync(remote, $"DROP TABLE IF EXISTS {tableName}");
            Directory.Delete(replicaDirectory, recursive: true);
        }
    }

    private static async Task ExecuteNonQueryAsync(TursoConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static (string RemoteUrl, string AuthToken) GetCloudCredentials()
    {
        var remoteUrl = Environment.GetEnvironmentVariable("TURSO_REMOTE_URL");
        var authToken = Environment.GetEnvironmentVariable("TURSO_AUTH_TOKEN");
        if (string.IsNullOrWhiteSpace(remoteUrl) || string.IsNullOrWhiteSpace(authToken))
            Assert.Ignore("Set TURSO_REMOTE_URL and TURSO_AUTH_TOKEN to run the Turso Cloud acceptance tests.");

        return (remoteUrl, authToken);
    }

    private static string NewReplicaDirectory()
    {
        var path = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "turso-cloud-interval-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
