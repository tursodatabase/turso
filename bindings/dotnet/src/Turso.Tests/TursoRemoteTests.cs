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
    public void TestCanCreateBatchIsRemoteOnly()
    {
        using var localConnection = new TursoConnection("Data Source=:memory:");
        localConnection.CanCreateBatch.Should().BeFalse();
        localConnection.Invoking(x => x.CreateBatch())
            .Should().Throw<NotSupportedException>()
            .WithMessage("Turso batch execution is currently supported only for remote connections.");

        using var remoteConnection = new TursoConnection("Data Source=https://example.com");
        remoteConnection.CanCreateBatch.Should().BeTrue();
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

    [Test]
    public void TestRemoteTransactionsUseBaton()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        const string commitResponseJson = """
            {
              "baton": "stream.2",
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

        const string closeResponseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(beginResponseJson, commitResponseJson, closeResponseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var transaction = connection.BeginTransaction();
        transaction.Commit();

        server.RequestBodies.Should().HaveCount(3);
        using var beginDocument = JsonDocument.Parse(server.RequestBodies[0]);
        beginDocument.RootElement.TryGetProperty("baton", out _).Should().BeFalse();
        beginDocument.RootElement.GetProperty("requests").GetArrayLength().Should().Be(1);
        beginDocument.RootElement.GetProperty("requests")[0].GetProperty("stmt").GetProperty("sql").GetString().Should().Be("BEGIN");

        using var commitDocument = JsonDocument.Parse(server.RequestBodies[1]);
        commitDocument.RootElement.GetProperty("baton").GetString().Should().Be("stream.1");
        var commitRequests = commitDocument.RootElement.GetProperty("requests");
        commitRequests.GetArrayLength().Should().Be(1);
        commitRequests[0].GetProperty("stmt").GetProperty("sql").GetString().Should().Be("COMMIT");

        using var closeDocument = JsonDocument.Parse(server.RequestBodies[2]);
        closeDocument.RootElement.GetProperty("baton").GetString().Should().Be("stream.2");
        closeDocument.RootElement.GetProperty("requests").GetArrayLength().Should().Be(1);
        closeDocument.RootElement.GetProperty("requests")[0].GetProperty("type").GetString().Should().Be("close");
    }

    [Test]
    public void TestRemoteCommandAndBatchRejectTransactionFromDifferentConnection()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        const string rollbackResponseJson = """
            {
              "baton": "stream.2",
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

        const string closeResponseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server1 = new TestRemoteServer(beginResponseJson, rollbackResponseJson, closeResponseJson);
        using var connection1 = new TursoConnection($"Data Source={server1.Url};Read Your Writes=False");
        connection1.Open();
        using var transaction = connection1.BeginTransaction();

        using var server2 = new TestRemoteServer();
        using var connection2 = new TursoConnection($"Data Source={server2.Url};Read Your Writes=True");
        connection2.Open();

        using var command = connection2.CreateCommand();
        command.CommandText = "SELECT 1";
        command.Transaction = transaction;
        command.Invoking(x => x.ExecuteScalar())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("The transaction is not associated with the command's connection.");

        using var batch = (TursoBatch)connection2.CreateBatch();
        batch.Transaction = transaction;
        var batchCommand = batch.CreateBatchCommand();
        batchCommand.CommandText = "SELECT 1";
        batch.BatchCommands.Add(batchCommand);
        batch.Invoking(x => x.ExecuteNonQuery())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("The transaction is not associated with the batch's connection.");

        server2.RequestBodies.Should().BeEmpty();
    }

    [Test]
    public void TestRemoteTransactionRollbackUsesErrorResponseBaton()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        const string commandErrorResponseJson = """
            {
              "baton": "stream.2",
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

        const string rollbackResponseJson = """
            {
              "baton": "stream.3",
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

        const string closeResponseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(beginResponseJson, commandErrorResponseJson, rollbackResponseJson, closeResponseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT * FROM missing";
        command.Invoking(x => x.ExecuteNonQuery())
            .Should().Throw<TursoException>()
            .WithMessage("Remote SQL execution failed: no such table: missing (SQLITE_ERROR)");

        transaction.Rollback();

        server.RequestBodies.Should().HaveCount(4);
        using var rollbackDocument = JsonDocument.Parse(server.RequestBodies[2]);
        rollbackDocument.RootElement.GetProperty("baton").GetString().Should().Be("stream.2");
        var rollbackRequests = rollbackDocument.RootElement.GetProperty("requests");
        rollbackRequests.GetArrayLength().Should().Be(1);
        rollbackRequests[0].GetProperty("stmt").GetProperty("sql").GetString().Should().Be("ROLLBACK");

        using var closeDocument = JsonDocument.Parse(server.RequestBodies[3]);
        closeDocument.RootElement.GetProperty("baton").GetString().Should().Be("stream.3");
        closeDocument.RootElement.GetProperty("requests").GetArrayLength().Should().Be(1);
        closeDocument.RootElement.GetProperty("requests")[0].GetProperty("type").GetString().Should().Be("close");
    }

    [Test]
    public void TestRemoteCommitDoesNotThrowWhenPostCommitCloseFails()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        const string commitResponseJson = """
            {
              "baton": "stream.2",
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

        const string closeErrorResponseJson = """
            {
              "results": [
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

        using var server = new TestRemoteServer(beginResponseJson, commitResponseJson, closeErrorResponseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var transaction = connection.BeginTransaction();
        transaction.Invoking(x => x.Commit()).Should().NotThrow();
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    [Test]
    public void TestRemoteCommitSqlErrorKeepsBatonForRollback()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        const string commitErrorResponseJson = """
            {
              "baton": "stream.2",
              "results": [
                {
                  "type": "error",
                  "error": {
                    "message": "FOREIGN KEY constraint failed",
                    "code": "SQLITE_CONSTRAINT_FOREIGNKEY"
                  }
                }
              ]
            }
            """;

        const string rollbackResponseJson = """
            {
              "baton": "stream.3",
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

        const string closeResponseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(beginResponseJson, commitErrorResponseJson, rollbackResponseJson, closeResponseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var transaction = connection.BeginTransaction();
        transaction.Invoking(x => x.Commit())
            .Should().Throw<TursoException>()
            .WithMessage("Remote SQL execution failed: FOREIGN KEY constraint failed (SQLITE_CONSTRAINT_FOREIGNKEY)");

        transaction.Rollback();

        using var rollbackDocument = JsonDocument.Parse(server.RequestBodies[2]);
        rollbackDocument.RootElement.GetProperty("baton").GetString().Should().Be("stream.2");
        rollbackDocument.RootElement.GetProperty("requests")[0].GetProperty("stmt").GetProperty("sql").GetString().Should().Be("ROLLBACK");
    }

    [Test]
    public void TestRemoteCommitTransportFailureInvalidatesConnection()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        using var server = new TestRemoteServer(beginResponseJson, "not json");
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var transaction = connection.BeginTransaction();
        transaction.Invoking(x => x.Commit())
            .Should().Throw<TursoException>()
            .WithMessage("Unable to parse remote response:*");
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    [Test]
    public void TestRemoteBeginTransportFailureInvalidatesConnection()
    {
        using var server = new TestRemoteServer("not json");
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=True");
        connection.Open();

        connection.Invoking(x => x.BeginTransaction())
            .Should().Throw<TursoException>()
            .WithMessage("Unable to parse remote response:*");
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    [Test]
    public void TestRemoteInTransactionTransportFailureInvalidatesConnection()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        using var server = new TestRemoteServer(beginResponseJson, "not json");
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=True");
        connection.Open();

        var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO t VALUES (1)";

        command.Invoking(x => x.ExecuteNonQuery())
            .Should().Throw<TursoException>()
            .WithMessage("Unable to parse remote response:*");
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
        transaction.Invoking(x => x.Dispose()).Should().NotThrow();
    }

    [Test]
    public void TestRemoteRollbackFailureInvalidatesConnection()
    {
        const string beginResponseJson = """
            {
              "baton": "stream.1",
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

        const string rollbackErrorResponseJson = """
            {
              "baton": "stream.2",
              "results": [
                {
                  "type": "error",
                  "error": {
                    "message": "rollback failed",
                    "code": "ROLLBACK_FAILED"
                  }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(beginResponseJson, rollbackErrorResponseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=False");
        connection.Open();

        using var transaction = connection.BeginTransaction();
        transaction.Invoking(x => x.Rollback())
            .Should().Throw<TursoException>()
            .WithMessage("Remote SQL execution failed: rollback failed (ROLLBACK_FAILED)");
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    [Test]
    public void TestRemoteBatchSerializesStatementsAndReadsMultipleResults()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "batch",
                    "result": {
                      "step_results": [
                        {
                          "cols": [
                            { "name": "n", "decltype": "INTEGER" }
                          ],
                          "rows": [
                            [
                              { "type": "integer", "value": "7" }
                            ]
                          ],
                          "affected_row_count": 0,
                          "last_insert_rowid": null
                        },
                        {
                          "cols": [],
                          "rows": [],
                          "affected_row_count": 2,
                          "last_insert_rowid": "3"
                        }
                      ],
                      "step_errors": [null, null]
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

        using var batch = (TursoBatch)connection.CreateBatch();
        var select = (TursoBatchCommand)batch.CreateBatchCommand();
        select.CommandText = "SELECT ?";
        select.Parameters.Add(7);
        batch.BatchCommands.Add(select);

        var insert = (TursoBatchCommand)batch.CreateBatchCommand();
        insert.CommandText = "INSERT INTO t VALUES (:name), (:other)";
        insert.Parameters.AddWithValue(":name", "alice");
        insert.Parameters.AddWithValue(":other", "bob");
        batch.BatchCommands.Add(insert);

        using var reader = batch.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(7);
        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeFalse();
        reader.RecordsAffected.Should().Be(2);
        select.RecordsAffected.Should().Be(0);
        insert.RecordsAffected.Should().Be(2);

        using var document = JsonDocument.Parse(server.RequestBody);
        var requests = document.RootElement.GetProperty("requests");
        requests.GetArrayLength().Should().Be(2);
        requests[0].GetProperty("type").GetString().Should().Be("batch");
        var steps = requests[0].GetProperty("batch").GetProperty("steps");
        steps.GetArrayLength().Should().Be(2);
        steps[0].GetProperty("stmt").GetProperty("sql").GetString().Should().Be("SELECT ?");
        steps[0].GetProperty("stmt").GetProperty("args")[0].GetProperty("value").GetString().Should().Be("7");
        steps[1].GetProperty("stmt").GetProperty("named_args")[0].GetProperty("name").GetString().Should().Be(":name");
        requests[1].GetProperty("type").GetString().Should().Be("close");
    }

    [Test]
    public void TestRemoteBatchNonQueryUsesNoRowsAndIgnoresTrailingCloseError()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "batch",
                    "result": {
                      "step_results": [
                        {
                          "cols": [],
                          "rows": [],
                          "affected_row_count": 3,
                          "last_insert_rowid": "3"
                        }
                      ],
                      "step_errors": [null]
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

        using var batch = (TursoBatch)connection.CreateBatch();
        var command = (TursoBatchCommand)batch.CreateBatchCommand();
        command.CommandText = "INSERT INTO t VALUES (1), (2), (3)";
        batch.BatchCommands.Add(command);

        batch.ExecuteNonQuery().Should().Be(3);
        command.RecordsAffected.Should().Be(3);

        using var document = JsonDocument.Parse(server.RequestBody);
        var requests = document.RootElement.GetProperty("requests");
        requests.GetArrayLength().Should().Be(2);
        requests[0].GetProperty("type").GetString().Should().Be("batch");
        var steps = requests[0].GetProperty("batch").GetProperty("steps");
        steps.GetArrayLength().Should().Be(1);
        steps[0].GetProperty("stmt").GetProperty("want_rows").GetBoolean().Should().BeFalse();
        requests[1].GetProperty("type").GetString().Should().Be("close");
    }

    [Test]
    public void TestRemoteBatchSurfacesStepErrors()
    {
        const string responseJson = """
            {
              "baton": "stream.1",
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "batch",
                    "result": {
                      "step_results": [null],
                      "step_errors": [
                        {
                          "message": "no such table: missing",
                          "code": "SQLITE_ERROR"
                        }
                      ]
                    }
                  }
                }
              ]
            }
            """;

        const string closeResponseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": { "type": "close" }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson, closeResponseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=True");
        connection.Open();

        using var batch = (TursoBatch)connection.CreateBatch();
        var command = (TursoBatchCommand)batch.CreateBatchCommand();
        command.CommandText = "SELECT * FROM missing";
        batch.BatchCommands.Add(command);

        batch.Invoking(x => x.ExecuteNonQuery())
            .Should().Throw<TursoException>()
            .WithMessage("Remote SQL execution failed: no such table: missing (SQLITE_ERROR)");
    }

    [Test]
    public void TestRemoteBatchRejectsMismatchedStepErrors()
    {
        const string responseJson = """
            {
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "batch",
                    "result": {
                      "step_results": [null],
                      "step_errors": []
                    }
                  }
                }
              ]
            }
            """;

        using var server = new TestRemoteServer(responseJson);
        using var connection = new TursoConnection($"Data Source={server.Url};Read Your Writes=True");
        connection.Open();

        using var batch = (TursoBatch)connection.CreateBatch();
        var command = (TursoBatchCommand)batch.CreateBatchCommand();
        command.CommandText = "SELECT * FROM missing";
        batch.BatchCommands.Add(command);

        batch.Invoking(x => x.ExecuteNonQuery())
            .Should().Throw<TursoException>()
            .WithMessage("Remote batch returned an unexpected result shape: 1 results, 0 errors, expected 1.");
    }

    [Test]
    public void TestSyncRequiresReplicaConnection()
    {
        using var connection = new TursoConnection("Data Source=http://localhost:8080");
        connection.Open();

        connection.Invoking(x => x.Sync())
            .Should().Throw<NotSupportedException>()
            .WithMessage("Sync requires an embedded replica connection.");
    }

    [Test]
    public async Task TestSyncAsyncRequiresReplicaConnection()
    {
        using var connection = new TursoConnection("Data Source=http://localhost:8080");
        connection.Open();

        var act = async () => await connection.SyncAsync();
        await act.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("Sync requires an embedded replica connection.");
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

        public List<string> RequestBodies { get; } = [];

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
            RequestBodies.Add(RequestBody);

            var body = Encoding.UTF8.GetBytes(_responseJson.Dequeue());
            var headers = Encoding.ASCII.GetBytes(
                $"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n");
            await stream.WriteAsync(headers);
            await stream.WriteAsync(body);
        }
    }
}
