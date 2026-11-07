using System.Net;
using AwesomeAssertions;

namespace Turso.Tests;

public sealed class TursoSyncEncryptionTests
{
    [Test]
    public void ReplicaConnectionStringMapsIndependentLocalKey()
    {
        using var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=replica.db;"
            + "Remote Encryption Cipher=aes256gcm;Local Encryption Key=" + new string('1', 64));
        var options = connection.CreateReplicaOptions();
        options.Validate();
        var configuration = TursoSyncDatabase.CreateNativeConfiguration(options, options.GetNormalizedRemoteUri());
        configuration.LocalEncryptionKey.Should().Be(new string('1', 64));
        configuration.RemoteEncryptionCipher.Should().Be("aes256gcm");
        configuration.RemoteEncryptionKey.Should().BeNull();
    }

    [Test]
    public async Task EncryptedBootstrapCheckpointOfflineReopenAndWrongKey()
    {
        var directory = Path.Combine(Path.GetTempPath(), "turso-encryption-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "replica.db");
        var localKey = string.Concat(Enumerable.Repeat("16", 32));
        var cloudKey = Convert.ToBase64String(Enumerable.Repeat((byte)11, 32).ToArray());
        using var handler = new EmptyEncryptedSource(cloudKey);
        using var http = new HttpClient(handler);
        TursoSyncDatabaseOptions Options(string key, bool online) => new(path, new Uri("https://fixture.test"))
        {
            LocalEncryptionKey = key,
            RemoteEncryption = new TursoRemoteEncryptionOptions { Cipher = TursoRemoteEncryptionCipher.Aes256Gcm, Key = online ? cloudKey : null },
            BootstrapIfEmpty = online,
            HttpClient = http,
        };
        try
        {
            using (var db = await TursoSyncDatabase.CreateAsync(Options(localKey, true)))
            {
                using var connection = db.Connect();
                using var command = connection.CreateCommand();
                command.CommandText = "CREATE TABLE t(value TEXT)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t VALUES ('binding private marker')";
                command.ExecuteNonQuery();
                await db.CheckpointAsync();
            }
            File.ReadAllBytes(path + "-info").Take(8).Should().Equal("TSYNMETA"u8.ToArray());
            System.Text.Encoding.Latin1.GetString(File.ReadAllBytes(path)).Should().NotContain("binding private marker");
            handler.Offline = true;
            using (var db = await TursoSyncDatabase.OpenAsync(Options(localKey, false)))
            {
                using var connection = db.Connect();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT value FROM t";
                command.ExecuteScalar().Should().Be("binding private marker");
            }
            Func<Task> wrongKey = async () => { using var db = await TursoSyncDatabase.OpenAsync(Options(new string('7', 64), false)); };
            await wrongKey.Should().ThrowAsync<TursoSyncException>();
        }
        finally { Directory.Delete(directory, true); }
    }

    private sealed class EmptyEncryptedSource(string cloudKey) : HttpMessageHandler
    {
        public bool Offline { get; set; }
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Offline.Should().BeFalse("offline reopen must not make requests");
            request.Headers.GetValues("x-turso-encryption-key").Should().Equal(cloudKey);
            request.Headers.GetValues("x-turso-encryption-cipher").Should().Equal("aes256gcm");
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent([9, 10, 3, 114, 101, 118, 26, 0, 64, 1]),
            });
        }
    }
}
