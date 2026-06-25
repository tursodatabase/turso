using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using Turso.Raw.Public;

namespace Turso.Tests;

public class TursoRemoteTests
{
    [Test]
    public void TestConnectionStringBuilderNormalizesRemoteKeywords()
    {
        var builder = new TursoConnectionStringBuilder(
            "Data Source=libsql://example.turso.io;AuthToken=secret;ReadYourWrites=False;ReplicaPath=replica.db;SyncInterval=5;TLS=True");

        builder.DataSource.Should().Be("libsql://example.turso.io");
        builder.AuthToken.Should().Be("secret");
        builder.ReadYourWrites.Should().BeFalse();
        builder.ReplicaPath.Should().Be("replica.db");
        builder.SyncInterval.Should().Be(5);
        builder.Tls.Should().BeTrue();
        builder.ConnectionString.Should().Contain("Auth Token=secret");
        builder.ConnectionString.Should().Contain("Read Your Writes=False");
        builder.ContainsKey("AuthToken").Should().BeTrue();
    }

    [Test]
    public void TestRemoteReplicaFailsBeforeNetworkAccess()
    {
        using var connection = new TursoConnection(
            "Data Source=libsql://example.turso.io;Auth Token=secret;Replica Path=replica.db");

        connection.Invoking(x => x.Open())
            .Should().Throw<NotSupportedException>()
            .WithMessage("Embedded replica connections are not supported yet by the .NET provider.*");
    }

    [Test]
    public void TestRemoteTlsConflictFailsBeforeNetworkAccess()
    {
        using var connection = new TursoConnection("Data Source=http://localhost:8080;Tls=True");

        connection.Invoking(x => x.Open())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Tls=True conflicts with the http URL scheme.");
    }

    [Test]
    public void TestRemoteAuthTokenRequiresHttpsExceptLoopback()
    {
        using var httpConnection = new TursoConnection("Data Source=http://example.com;Auth Token=secret");
        httpConnection.Invoking(x => x.Open())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Auth Token requires an HTTPS remote Turso URL unless the host is localhost or loopback.");

        using var libsqlCleartextConnection = new TursoConnection("Data Source=libsql://example.com;Tls=False;Auth Token=secret");
        libsqlCleartextConnection.Invoking(x => x.Open())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Auth Token requires an HTTPS remote Turso URL unless the host is localhost or loopback.");

        using var loopbackConnection = new TursoConnection("Data Source=http://localhost:8080;Auth Token=secret");
        loopbackConnection.Open();
        loopbackConnection.State.Should().Be(System.Data.ConnectionState.Open);
    }

    [Test]
    public void TestLocalConnectionRejectsRemoteOnlyOptions()
    {
        using var connection = new TursoConnection("Data Source=:memory:;Auth Token=secret");

        connection.Invoking(x => x.Open())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Auth Token requires a remote Turso URL Data Source.");
    }

    [Test]
    public void TestRemoteOpenCloseStateDoesNotRequireNetwork()
    {
        using var connection = new TursoConnection("Data Source=http://localhost:8080;Read Your Writes=False");

        connection.Open();
        connection.State.Should().Be(System.Data.ConnectionState.Open);
        connection.Invoking(x => x.Open())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("The connection is already open.");

        connection.Close();
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    [Test]
    public void TestRemoteTransactionsAreUnsupportedBeforeNetworkAccess()
    {
        using var connection = new TursoConnection("Data Source=http://localhost:8080");
        connection.Open();

        connection.Invoking(x => x.BeginTransaction())
            .Should().Throw<NotSupportedException>()
            .WithMessage("Remote transactions are not supported yet by the .NET provider.");
    }

    [Test]
    public void TestRemoteCommandSerializesParametersAndReadsRows()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "execute",
                    "result": {
                      "cols": [
                        { "name": "n", "decltype": "INTEGER" },
                        { "name": "name", "decltype": "TEXT" }
                      ],
                      "rows": [
                        [
                          { "type": "integer", "value": "42" },
                          { "type": "text", "value": "alice" }
                        ]
                      ],
                      "affected_row_count": 0,
                      "last_insert_rowid": null
                    }
                  }
                },
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection(
            $"Data Source={server.Url};Auth Token=secret;Read Your Writes=False");
        connection.Open();

        using var command = (TursoCommand)connection.CreateCommand();
        command.CommandText = "SELECT ?, :name";
        command.Parameters.Add(42);
        command.Parameters.AddWithValue(":name", "alice");

        using var reader = command.ExecuteReader();
        reader.FieldCount.Should().Be(2);
        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(42);
        reader.GetString(1).Should().Be("alice");
        reader.Read().Should().BeFalse();

        server.RequestUri.Should().Be(new Uri(server.Url, "/v2/pipeline"));
        server.Authorization.Should().Be("Bearer secret");

        using var document = JsonDocument.Parse(server.RequestBody);
        var requests = document.RootElement.GetProperty("requests");
        requests.GetArrayLength().Should().Be(2);
        requests[0].GetProperty("type").GetString().Should().Be("execute");
        requests[0].GetProperty("stmt").GetProperty("sql").GetString().Should().Be("SELECT ?, :name");
        requests[0].GetProperty("stmt").GetProperty("args")[0].GetProperty("value").GetString().Should().Be("42");
        requests[0].GetProperty("stmt").GetProperty("named_args")[0].GetProperty("name").GetString().Should().Be(":name");
        requests[1].GetProperty("type").GetString().Should().Be("close");
    }

    [Test]
    public void TestRemoteCommandExecuteScalarUsesSingleStatementPipeline()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "execute",
                    "result": {
                      "cols": [
                        { "name": "value", "decltype": "INTEGER" }
                      ],
                      "rows": [
                        [
                          { "type": "integer", "value": "123" }
                        ]
                      ],
                      "affected_row_count": 0,
                      "last_insert_rowid": null
                    }
                  }
                },
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var command = (TursoCommand)connection.CreateCommand();
        command.CommandText = "SELECT $value";
        command.Parameters.AddWithValue("$value", 123);

        command.ExecuteScalar().Should().Be(123L);

        using var document = JsonDocument.Parse(server.RequestBody);
        var request = document.RootElement.GetProperty("requests")[0];
        request.GetProperty("type").GetString().Should().Be("execute");
        request.GetProperty("stmt").GetProperty("sql").GetString().Should().Be("SELECT $value");
        request.GetProperty("stmt").GetProperty("named_args")[0].GetProperty("name").GetString().Should().Be("$value");
        request.GetProperty("stmt").GetProperty("want_rows").GetBoolean().Should().BeTrue();
    }

    [Test]
    public void TestRemoteReaderDoesNotConvertNullToEmptyString()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "execute",
                    "result": {
                      "cols": [
                        { "name": "value", "decltype": "TEXT" }
                      ],
                      "rows": [
                        [
                          { "type": "null" }
                        ]
                      ],
                      "affected_row_count": 0,
                      "last_insert_rowid": null
                    }
                  }
                },
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT NULL";
        using var reader = command.ExecuteReader();

        reader.GetDataTypeName(0).Should().Be("TEXT");
        reader.GetFieldType(0).Should().Be(typeof(string));
        reader.Read().Should().BeTrue();
        reader.IsDBNull(0).Should().BeTrue();
        reader.GetFieldType(0).Should().Be(typeof(DBNull));
        reader.GetValue(0).Should().Be(DBNull.Value);
        reader.Invoking(x => x.GetString(0))
            .Should().Throw<InvalidCastException>()
            .WithMessage("Cannot convert remote null value to String.");
        reader.Invoking(x => x.GetDateTime(0))
            .Should().Throw<InvalidCastException>()
            .WithMessage("Cannot convert remote null value to DateTime.");
    }

    [Test]
    public void TestRemoteNonQueryUsesNoRowsAndIgnoresTrailingCloseError()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "execute",
                    "result": {
                      "cols": [],
                      "rows": [],
                      "affected_row_count": 1,
                      "last_insert_rowid": "1"
                    }
                  }
                },
                {
                  "type": "error",
                  "error": {
                    "message": "close failed",
                    "code": "CLOSE_FAILED"
                  }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO t VALUES (1)";
        command.ExecuteNonQuery().Should().Be(1);

        using var document = JsonDocument.Parse(server.RequestBody);
        var requests = document.RootElement.GetProperty("requests");
        requests.GetArrayLength().Should().Be(2);
        requests[0].GetProperty("stmt").GetProperty("want_rows").GetBoolean().Should().BeFalse();
        requests[1].GetProperty("type").GetString().Should().Be("close");
    }

    [Test]
    public void TestRemoteCommandSurfacesSqlErrors()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "error",
                  "error": {
                    "message": "no such table: missing",
                    "code": "SQLITE_ERROR"
                  }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM missing";

        command.Invoking(x => x.ExecuteScalar())
            .Should().Throw<TursoException>()
            .WithMessage("Remote SQL execution failed: no such table: missing (SQLITE_ERROR)");
    }

    [Test]
    public void TestRemoteCommandRejectsProtocolErrors()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "sequence",
                    "result": {}
                  }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=True");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";

        command.Invoking(x => x.ExecuteScalar())
            .Should().Throw<TursoException>()
            .WithMessage("Remote request returned unexpected response type: sequence");
    }

    [Test]
    public void TestRemoteCommandRejectsCleartextBaseUrlWithAuthToken()
    {
        const string responseJson = """
            {
              "base_url": "http://example.com",
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "execute",
                    "result": {
                      "cols": [],
                      "rows": [],
                      "affected_row_count": 0,
                      "last_insert_rowid": null
                    }
                  }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Auth Token=secret");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";

        command.Invoking(x => x.ExecuteScalar())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Auth Token requires an HTTPS remote Turso URL unless the host is localhost or loopback.");
    }

    private sealed class TestRemoteServer : IDisposable
    {
        private readonly CancellationTokenSource _cancellation = new();
        private readonly TcpListener _listener;
        private readonly Queue<string> _responseJson;
        private readonly Task _serverTask;

        public TestRemoteServer(params string[] responseJson)
        {
            _responseJson = new Queue<string>(responseJson);
            _listener = new TcpListener(IPAddress.Loopback, port: 0);
            _listener.Start();

            var endpoint = (IPEndPoint)_listener.LocalEndpoint;
            Url = new Uri($"http://127.0.0.1:{endpoint.Port}");
            _serverTask = Task.Run(AcceptLoopAsync);
        }

        public Uri Url { get; }

        public Uri? RequestUri { get; private set; }

        public string? Authorization { get; private set; }

        public string RequestBody { get; private set; } = "";

        public void Dispose()
        {
            _cancellation.Cancel();
            _listener.Stop();
            _cancellation.Dispose();
        }

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_cancellation.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_cancellation.Token);
                    await HandleClientAsync(client);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private async Task HandleClientAsync(TcpClient client)
        {
            if (_responseJson.Count == 0)
                throw new InvalidOperationException("No fake HTTP response is available.");

            await using var stream = client.GetStream();
            using var reader = new StreamReader(stream, Encoding.ASCII, leaveOpen: true);

            var requestLine = await reader.ReadLineAsync()
                              ?? throw new InvalidOperationException("HTTP request did not include a request line.");
            var requestParts = requestLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (requestParts.Length < 2)
                throw new InvalidOperationException($"Invalid HTTP request line: {requestLine}");

            var contentLength = 0;
            while (await reader.ReadLineAsync() is { Length: > 0 } headerLine)
            {
                var separatorIndex = headerLine.IndexOf(':', StringComparison.Ordinal);
                if (separatorIndex < 0)
                    continue;

                var name = headerLine[..separatorIndex];
                var value = headerLine[(separatorIndex + 1)..].Trim();
                if (name.Equals("Content-Length", StringComparison.OrdinalIgnoreCase))
                    contentLength = int.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
                else if (name.Equals("Authorization", StringComparison.OrdinalIgnoreCase))
                    Authorization = value;
                else if (name.Equals("Host", StringComparison.OrdinalIgnoreCase))
                    RequestUri = new Uri($"{Url.Scheme}://{value}{requestParts[1]}");
            }

            if (RequestUri is null)
                RequestUri = new Uri(Url, requestParts[1]);

            if (contentLength > 0)
            {
                var buffer = new char[contentLength];
                var read = 0;
                while (read < buffer.Length)
                {
                    var count = await reader.ReadAsync(buffer.AsMemory(read, buffer.Length - read));
                    if (count == 0)
                        break;

                    read += count;
                }

                RequestBody = new string(buffer, 0, read);
            }

            var body = Encoding.UTF8.GetBytes(_responseJson.Dequeue());
            var headers = Encoding.ASCII.GetBytes(
                $"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n");
            await stream.WriteAsync(headers);
            await stream.WriteAsync(body);
        }
    }
}
