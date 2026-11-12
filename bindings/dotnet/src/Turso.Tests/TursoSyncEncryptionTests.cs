using System.Net;
using System.Data.Common;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoSyncEncryptionTests
{
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task PooledReplicaRejectsDifferentOrBlankLocalKeys(bool openAsync, bool encrypted)
    {
        var directory = Path.Combine(Path.GetTempPath(), "turso-encryption-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "replica.db");
        var localKey = encrypted ? string.Concat(Enumerable.Repeat("16", 32)) : null;
        var cloudKey = Convert.ToBase64String(Enumerable.Repeat((byte)11, 32).ToArray());
        using var handler = new EmptyEncryptedSource(cloudKey);
        using var http = new HttpClient(handler);
        TursoConnection.SyncHttpClientFactory = () => http;
        try
        {
            using var first = new TursoConnection(ConnectionString(path, cloudKey, localKey));
            await Open(first);
            first.ExecuteNonQuery("CREATE TABLE t(value TEXT)");
            first.ExecuteNonQuery("INSERT INTO t VALUES ('pooled marker')");

            using var matching = new TursoConnection(ConnectionString(path, cloudKey, localKey));
            await Open(matching);
            using var query = new TursoCommand(matching, "SELECT value FROM t");
            query.ExecuteScalar().Should().Be("pooled marker");

            var rejectedKeys = encrypted
                ? new[] { new string('7', 64), null, "", " " }
                : new[] { "", " ", new string('7', 64) };
            foreach (var key in rejectedKeys)
            {
                using var rejected = new TursoConnection(ConnectionString(path, cloudKey, key));
                Func<Task> open = () => Open(rejected);
                await open.Should().ThrowAsync<Exception>();
                rejected.State.Should().Be(System.Data.ConnectionState.Closed);
            }
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
            Directory.Delete(directory, true);
        }

        Task Open(TursoConnection connection)
        {
            if (openAsync)
                return connection.OpenAsync();
            connection.Open();
            return Task.CompletedTask;
        }
    }

    [TestCase("Local Encryption Key", "")]
    [TestCase("LocalEncryptionKey", " ")]
    [TestCase("Remote Encryption Key", "")]
    [TestCase("RemoteEncryptionKey", " ")]
    public void ExplicitBlankEncryptionKeysAreRejected(string keyword, string value)
    {
        var builder = new TursoConnectionStringBuilder(ConnectionString("replica.db", "cloud-key", new string('1', 64)));
        builder[keyword] = value;
        using var connection = new TursoConnection(builder.ConnectionString);
        connection.Invoking(c => c.CreateReplicaOptions().Validate()).Should().Throw<ArgumentException>();
    }

    [TestCase(false, "Local Encryption Key", "")]
    [TestCase(false, "LocalEncryptionKey", " ")]
    [TestCase(false, "Local Encryption Key", "\"\"")]
    [TestCase(true, "Local Encryption Key", "")]
    [TestCase(true, "LocalEncryptionKey", " ")]
    [TestCase(true, "Local Encryption Key", "\"\"")]
    public void ProviderFactoryRejectsBlankLocalEncryptionKeys(bool sqlite, string keyword, string value)
    {
        DbProviderFactory factory = sqlite ? Turso.Data.Sqlite.SqliteFactory.Instance : TursoFactory.Instance;
        var builder = factory.CreateConnectionStringBuilder()!;
        var connectionString = ConnectionString("replica.db", "cloud-key", null) + $";{keyword}={value}";
        Action parse = () =>
        {
            builder.ConnectionString = connectionString;
            using var connection = new TursoConnection(builder.ConnectionString);
            connection.CreateReplicaOptions().Validate();
        };

        parse.Should().Throw<Exception>();
    }

    [TestCase(false)]
    [TestCase(true)]
    public void ProviderFactoryCanExplicitlyOmitLocalEncryptionKey(bool sqlite)
    {
        DbProviderFactory factory = sqlite ? Turso.Data.Sqlite.SqliteFactory.Instance : TursoFactory.Instance;
        var builder = factory.CreateConnectionStringBuilder()!;
        builder.ConnectionString = ConnectionString("replica.db", "cloud-key", new string('1', 64));

        using (var encrypted = new TursoConnection(builder.ConnectionString))
        {
            var options = encrypted.CreateReplicaOptions();
            options.Validate();
            options.LocalEncryptionKey.Should().Be(new string('1', 64));
        }

        Action remove = () => builder.Remove("LocalEncryptionKey");
        remove.Should().Throw<InvalidOperationException>();
        builder["LocalEncryptionKey"] = null;

        using var plaintext = new TursoConnection(builder.ConnectionString);
        var plaintextOptions = plaintext.CreateReplicaOptions();
        plaintextOptions.Validate();
        plaintextOptions.LocalEncryptionKey.Should().BeNull();
    }

    [Test]
    public void OmittedLocalKeyKeepsPlaintextConfiguration()
    {
        using var connection = new TursoConnection(ConnectionString("replica.db", "cloud-key", null));
        var options = connection.CreateReplicaOptions();
        options.Validate();
        options.LocalEncryptionKey.Should().BeNull();
        options.RemoteEncryption!.Key.Should().Be("cloud-key");
    }

    [TestCase("Remote Encryption Key")]
    [TestCase("RemoteEncryptionKey")]
    public void EmptyCloudKeyKeepsExistingStandaloneBehavior(string keyword)
    {
        using var connection = new TursoConnection($"Data Source=:memory:;{keyword}=");
        connection.Open();
        connection.State.Should().Be(System.Data.ConnectionState.Open);
    }

    [TestCase("Local Encryption Key")]
    [TestCase("LocalEncryptionKey")]
    public async Task SqliteProviderForwardsLocalEncryptionKey(string keyword)
    {
        var directory = Path.Combine(Path.GetTempPath(), "turso-encryption-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "replica.db");
        var key = new string('1', 64);
        var cloudKey = Convert.ToBase64String(Enumerable.Repeat((byte)11, 32).ToArray());
        using var handler = new EmptyEncryptedSource(cloudKey);
        using var http = new HttpClient(handler);
        TursoConnection.SyncHttpClientFactory = () => http;
        try
        {
            var builder = new Turso.Data.Sqlite.SqliteConnectionStringBuilder(
                ConnectionString(path, cloudKey, null));
            builder[keyword] = key;
            using var connection = new Turso.Data.Sqlite.SqliteConnection(builder.ConnectionString);
            await connection.OpenAsync();
            File.ReadAllBytes(path + "-info").Take(8).Should().Equal("TSYNMETA"u8.ToArray());
        }
        finally
        {
            TursoConnection.SyncHttpClientFactory = null;
            Directory.Delete(directory, true);
        }
    }

    [TestCase("")]
    [TestCase(" ")]
    [TestCase("1111111111111111111111111111111111111111111111111111111111111111")]
    public void SqliteProviderRejectsReplicaKeyForStandaloneDatabase(string key)
    {
        var builder = new System.Data.Common.DbConnectionStringBuilder
        {
            ["Data Source"] = ":memory:",
            ["Local Encryption Key"] = key,
        };
        Action open = () =>
        {
            using var connection = new Turso.Data.Sqlite.SqliteConnection(builder.ConnectionString);
            connection.Open();
        };
        open.Should().Throw<Exception>();
    }

    private static string ConnectionString(string path, string cloudKey, string? localKey)
    {
        var builder = new TursoConnectionStringBuilder
        {
            DataSource = "turso://example.test",
            ReplicaPath = path,
            Pooling = true,
            SyncExperimentalFeatures = "sync_encryption",
            RemoteEncryptionCipher = "aes256gcm",
            RemoteEncryptionKey = cloudKey,
        };
        if (localKey is not null)
            builder.LocalEncryptionKey = localKey;
        return builder.ConnectionString;
    }

    [TestCase(null)]
    [TestCase("encryption")]
    [TestCase("sync_encryption_extra")]
    public async Task LocalEncryptionRequiresExplicitExperimentalFeature(string? features)
    {
        var directory = Path.Combine(Path.GetTempPath(), "turso-sync-opt-in-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "replica.db");
        try
        {
            Func<Task> create = async () => {
                using var db = await TursoSyncDatabase.CreateAsync(new(path, new Uri("http://127.0.0.1:1")) {
                    ExperimentalFeatures = features, LocalEncryptionKey = string.Concat(Enumerable.Repeat("16", 32)),
                    RemoteEncryption = new() { Cipher = TursoRemoteEncryptionCipher.Aes256Gcm, Key = Convert.ToBase64String(Enumerable.Repeat((byte)11, 32).ToArray()) },
                });
            };
            await create.Should().ThrowAsync<Turso.Raw.Public.TursoSyncNativeException>().WithMessage("*sync_encryption*");
            File.Exists(path).Should().BeFalse();
            File.Exists(path + "-info").Should().BeFalse();
        }
        finally { Directory.Delete(directory, true); }
    }

    [TestCase(TursoRemoteEncryptionCipher.Aes256Gcm, "aes256gcm", 32)]
    [TestCase(TursoRemoteEncryptionCipher.Aes128Gcm, "aes128gcm", 16)]
    [TestCase(TursoRemoteEncryptionCipher.Aegis128L, "aegis128l", 16)]
    public async Task RealEncryptedIncrementalSyncAndCipherMappings(TursoRemoteEncryptionCipher cipher, string name, int keySize)
    {
        var directory = Path.Combine(Path.GetTempPath(), "turso-real-encryption-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "replica.db");
        var localKey = string.Concat(Enumerable.Repeat("16", keySize));
        var cloudKey = Convert.ToBase64String(Enumerable.Repeat((byte)11, keySize).ToArray());
        var keyPath = Path.Combine(directory, "key.hex");
        File.WriteAllText(keyPath, string.Concat(Enumerable.Repeat("0b", keySize)));
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(keyPath, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        var url = new Uri($"http://127.0.0.1:{port}");
        var executable = Environment.GetEnvironmentVariable("LOCAL_SYNC_SERVER");
        if (executable is null)
        {
            var root = new DirectoryInfo(Environment.CurrentDirectory);
            while (root is not null && !File.Exists(Path.Combine(root.FullName, "cli", "sync_server.rs"))) root = root.Parent;
            executable = Path.Combine(root?.FullName ?? throw new InvalidOperationException("Cannot find repository root"), "target", "debug", OperatingSystem.IsWindows() ? "tursodb.exe" : "tursodb");
        }
        var start = new ProcessStartInfo(executable) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in new[] { "--sync-server", $"127.0.0.1:{port}", "--sync-encryption-cipher", name,
            "--sync-encryption-key-file", keyPath, Path.Combine(directory, "source.db") }) start.ArgumentList.Add(argument);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Failed to start encrypted source");
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        async Task<JsonElement> Sql(string sql)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(url, "/v2/pipeline"));
            request.Headers.Add("x-turso-encryption-key", cloudKey);
            request.Headers.Add("x-turso-encryption-cipher", name);
            request.Content = new StringContent(JsonSerializer.Serialize(new { requests = new[] { new { type = "execute", stmt = new { sql } } } }), Encoding.UTF8, "application/json");
            using var response = await http.SendAsync(request);
            response.EnsureSuccessStatusCode();
            using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            var result = document.RootElement.GetProperty("results")[0];
            result.GetProperty("type").GetString().Should().Be("ok");
            return result.GetProperty("response").GetProperty("result").Clone();
        }
        TursoSyncDatabaseOptions Options(bool online) => new(path, url) {
            ExperimentalFeatures = "sync_encryption", LocalEncryptionKey = localKey, BootstrapIfEmpty = online,
            RemoteEncryption = new TursoRemoteEncryptionOptions { Cipher = cipher, Key = online ? cloudKey : null },
        };
        try
        {
            var ready = false;
            for (var attempt = 0; attempt < 200; attempt++)
            {
                if (process.HasExited) throw new InvalidOperationException(await stderr);
                try { using var response = await http.GetAsync(url); ready = true; break; }
                catch (HttpRequestException) { await Task.Delay(25); }
            }
            ready.Should().BeTrue("encrypted source must start");
            await Sql("PRAGMA journal_mode=mvcc");
            await Sql("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)");
            await Sql("INSERT INTO items VALUES (1, 'nonempty bootstrap')");
            using (var db = await TursoSyncDatabase.CreateAsync(Options(true)))
            {
                using var connection = db.Connect();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT value FROM items WHERE id=1";
                command.ExecuteScalar().Should().Be("nonempty bootstrap");
                command.CommandText = "INSERT INTO items VALUES (2, 'dotnet local marker')";
                command.ExecuteNonQuery();
                await db.PushAsync();
                (await Sql("SELECT count(*) FROM items")).GetProperty("rows")[0][0].GetProperty("value").GetString().Should().Be("2");
                await Sql("INSERT INTO items VALUES (3, hex(zeroblob(40000)))");
                await db.PullAsync();
                command.CommandText = "SELECT length(value) FROM items WHERE id=3";
                Convert.ToInt64(command.ExecuteScalar()).Should().Be(80000);
                await db.CheckpointAsync();
            }
            using (var db = await TursoSyncDatabase.OpenAsync(Options(false)))
            {
                using var connection = db.Connect();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT count(*) FROM items";
                Convert.ToInt64(command.ExecuteScalar()).Should().Be(3);
            }
        }
        finally
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
            await Task.WhenAll(stdout, stderr);
            Directory.Delete(directory, true);
        }
    }

    [Test]
    public void ReplicaConnectionStringMapsIndependentLocalKey()
    {
        using var connection = new TursoConnection(
            "Data Source=turso://example.test;Replica Path=replica.db;"
            + "Sync Experimental Features=sync_encryption;Remote Encryption Cipher=aes256gcm;Local Encryption Key=" + new string('1', 64));
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
            ExperimentalFeatures = "sync_encryption",
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
