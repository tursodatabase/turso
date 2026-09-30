using AwesomeAssertions;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoReplicaCloudTests
{
    [Test]
    public async Task ExplicitPullConvergesWithRemoteChanges()
    {
        var remoteUrl = Environment.GetEnvironmentVariable("TURSO_REMOTE_URL");
        var authToken = Environment.GetEnvironmentVariable("TURSO_AUTH_TOKEN");
        if (string.IsNullOrWhiteSpace(remoteUrl) || string.IsNullOrWhiteSpace(authToken))
            Assert.Ignore("Set TURSO_REMOTE_URL and TURSO_AUTH_TOKEN to run the replica Cloud test.");

        var tableName = "dotnet_replica_" + Guid.NewGuid().ToString("N");
        var directory = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "turso-cloud-replica-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var remoteConnectionString = $"Data Source={remoteUrl};Auth Token={authToken}";

        await using var remote = new TursoConnection(remoteConnectionString);
        await remote.OpenAsync();
        try
        {
            await ExecuteNonQueryAsync(remote, $"CREATE TABLE {tableName}(id INTEGER PRIMARY KEY, value INTEGER)");
            await ExecuteNonQueryAsync(remote, $"INSERT INTO {tableName} VALUES (1, 1)");

            await using var replica = new TursoConnection(
                remoteConnectionString
                + $";Replica Path={Path.Combine(directory, "replica.db")};Pooling=False");
            await replica.OpenAsync();
            await ExecuteNonQueryAsync(remote, $"UPDATE {tableName} SET value = 2 WHERE id = 1");

            await replica.SyncAsync();

            await using var query = replica.CreateCommand();
            query.CommandText = $"SELECT value FROM {tableName} WHERE id = 1";
            (await query.ExecuteScalarAsync()).Should().Be(2L);
        }
        finally
        {
            try
            {
                await ExecuteNonQueryAsync(remote, $"DROP TABLE IF EXISTS {tableName}");
            }
            finally
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    private static async Task ExecuteNonQueryAsync(TursoConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
}
