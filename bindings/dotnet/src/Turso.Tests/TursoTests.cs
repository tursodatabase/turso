using System.Data.Common;
using AwesomeAssertions;
using Turso.Raw.Public;
using Turso.Raw.Public.Value;

namespace Turso.Tests;

public class TursoTests
{
    [Test]
    public void TestSimpleQuery()
    {
        using var connection = new TursoConnection();
        connection.Open();

        using var cmd = new TursoCommand(connection, "SELECT * FROM generate_series(1,2,1)");

        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);

        reader.Read().Should().BeFalse();
    }

    [Test]
    public void TestPrepareStatement()
    {
        using var connection = new TursoConnection();
        connection.Open();

        using var cmd = new TursoCommand(connection, "SELECT * FROM generate_series(?,?,?)");
        cmd.Parameters.Add(1);
        cmd.Parameters.Add(2);
        cmd.Parameters.Add(1);
        cmd.Prepare();

        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);

        reader.Read().Should().BeFalse();
    }

    [TestCase("stringValue", TestName = "TestStringValue")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5 }, TestName = "TestBlobValue")]
    [TestCase(1, TestName = "TestIntValue")]
    [TestCase(2.5, TestName = "TestRealValue")]
    public void TestDifferentTypes(object typedValue)
    {
        using var connection = new TursoConnection();
        connection.Open();

        using (var create = new TursoCommand(connection, "CREATE TABLE t(v)"))
        {
            create.ExecuteNonQuery().Should().Be(0);
        }

        using (var insert = new TursoCommand(connection, "INSERT INTO t VALUES (?)"))
        {
            insert.Parameters.Add(typedValue);
            insert.ExecuteNonQuery().Should().Be(1);
        }

        using var select = new TursoCommand(connection, "SELECT v FROM t");
        using var reader = select.ExecuteReader();

        reader.Read().Should().BeTrue();

        switch (typedValue)
        {
            case string s:
                reader.GetString(0).Should().Be(s);
                break;
            case byte[] bytes:
                ((byte[])reader.GetValue(0)).SequenceEqual(bytes).Should().BeTrue();
                break;
            case int i:
                reader.GetInt32(0).Should().Be(i);
                break;
            case double d:
                reader.GetDouble(0).Should().Be(d);
                break;
            default:
                throw new AssertionException($"Unsupported test type: {typedValue.GetType()}");
        }

        reader.Read().Should().BeFalse();
    }


    [Test]
    public void TestBindNamedParameter()
    {
        using var connection = new TursoConnection();
        connection.Open();

        using var cmd = new TursoCommand(connection, "SELECT * FROM generate_series(?,:stop,?1)");
        cmd.Parameters.Add(1);
        cmd.Parameters.AddWithValue(":stop", 2);
        cmd.Prepare();

        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);

        reader.Read().Should().BeFalse();
    }

    [Test]
    public void TestInsertData()
    {
        using var connection = new TursoConnection();
        connection.Open();

        using var create = new TursoCommand(connection, "CREATE TABLE t(id INTEGER, name TEXT)");
        create.ExecuteNonQuery().Should().Be(0);

        using var insert = new TursoCommand(connection, "INSERT INTO t(id, name) VALUES (1, 'alice'), (2, 'bob')");
        insert.ExecuteNonQuery().Should().Be(2);

        using var countCmd = new TursoCommand(connection, "SELECT COUNT(*) FROM t");
        using var reader = countCmd.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        reader.Read().Should().BeFalse();
    }

    [Test]
    public void TestFetchSpecificColumns()
    {
        using var connection = new TursoConnection();
        connection.Open();

        using var create = new TursoCommand(connection, "CREATE TABLE t(id INTEGER, name TEXT, age INTEGER)");
        create.ExecuteNonQuery().Should().Be(0);

        using var insert = new TursoCommand(connection, "INSERT INTO t VALUES (1,'alice',30),(2,'bob',40)");
        insert.ExecuteNonQuery().Should().Be(2);

        using var select = new TursoCommand(connection, "SELECT name, age FROM t WHERE id = 2");
        using var reader = select.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("bob");
        reader.GetInt32(1).Should().Be(40);
        reader.Read().Should().BeFalse();
    }

    [Test]
    public void TestQueryError()
    {
        using var connection = new TursoConnection();
        connection.Open();

        using var cmd = new TursoCommand(connection, "SELECT * FROM table_that_does_not_exist");
        cmd.Invoking(x => x.ExecuteReader()).Should().Throw<TursoException>()
            .WithMessage("Unable to prepare statement: Parse error: no such table: table_that_does_not_exist");
    }

    [Test]
    [Ignore("https://github.com/tursodatabase/turso/pull/3591")]
    public void TestCommitTransaction()
    {
        using var connection = new TursoConnection("Data Source=./turso.db");
        connection.Open();

        using var connection2 = new TursoConnection("Data Source=./turso.db");
        connection2.Open();

        connection.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS t(id INTEGER)");
        connection.ExecuteNonQuery("DELETE FROM t");

        using var tx = connection.BeginTransaction();

        using var insert = new TursoCommand(connection, "INSERT INTO t VALUES (1),(2)");
        insert.ExecuteNonQuery().Should().Be(2);

        using var selectBefore = new TursoCommand(connection2, "SELECT COUNT(*) FROM t");
        using (var readerBefore = selectBefore.ExecuteReader())
        {
            readerBefore.Read().Should().BeTrue();
            readerBefore.GetInt32(0).Should().Be(0);
        }

        tx.Commit();

        using var selectAfter = new TursoCommand(connection2, "SELECT COUNT(*) FROM t");
        using var readerAfter = selectAfter.ExecuteReader();
        readerAfter.Read().Should().BeTrue();
        readerAfter.GetInt32(0).Should().Be(2);
    }

    [Test]
    public void TestRollbackTransaction()
    {
        using var connection = new TursoConnection();
        connection.Open();


        using var create = new TursoCommand(connection, "CREATE TABLE t(id INTEGER)");
        create.ExecuteNonQuery().Should().Be(0);

        using var tx = connection.BeginTransaction();
        using var insert = new TursoCommand(connection, "INSERT INTO t VALUES (1),(2)");
        insert.ExecuteNonQuery().Should().Be(2);

        using var select = new TursoCommand(connection, "SELECT COUNT(*) FROM t");
        using var reader = select.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);

        tx.Rollback();

        using var select2 = new TursoCommand(connection, "SELECT COUNT(*) FROM t");
        using var reader2 = select2.ExecuteReader();
        reader2.Read().Should().BeTrue();
        reader2.GetInt32(0).Should().Be(0);
    }

    [Test]
    public void TestDataReaderEnumerable()
    {
        using var connection = new TursoConnection();
        connection.Open();

        using var create = new TursoCommand(connection, "CREATE TABLE t(id INTEGER, name TEXT, age INTEGER)");
        create.ExecuteNonQuery().Should().Be(0);

        using var insert = new TursoCommand(connection, "INSERT INTO t VALUES (1,'alice',30),(2,'bob',40),(3,'charlie',50)");
        insert.ExecuteNonQuery().Should().Be(3);

        using var select = new TursoCommand(connection, "SELECT id, name, age FROM t ORDER BY id");
        using var reader = select.ExecuteReader();

        var results = new List<(long id, string name, long age)>();

        foreach (DbDataRecord record in reader)
        {
            var id = record.GetInt64(0);
            var name = record.GetString(1);
            var age = record.GetInt64(2);
            results.Add((id, name, age));
        }

        results.Should().HaveCount(3);
        results[0].Should().Be((1, "alice", 30));
        results[1].Should().Be((2, "bob", 40));
        results[2].Should().Be((3, "charlie", 50));
    }

    [Test]
    public void TestEncryption()
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"turso_test_encrypted_{Guid.NewGuid()}.db");
        var hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
        var wrongKey = "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

        try
        {
            // Create encrypted database
            using (var connection = new TursoConnection($"Data Source={tempPath};Encryption Cipher=aegis256;Encryption Key={hexkey}"))
            {
                connection.Open();

                using var create = new TursoCommand(connection, "CREATE TABLE t(x TEXT)");
                create.ExecuteNonQuery();

                using var insert = new TursoCommand(connection, "INSERT INTO t VALUES ('secret')");
                insert.ExecuteNonQuery();

                using var checkpoint = new TursoCommand(connection, "PRAGMA wal_checkpoint(truncate)");
                checkpoint.ExecuteNonQuery();
            }

            // Verify data is encrypted on disk
            var content = File.ReadAllBytes(tempPath);
            content.Length.Should().BeGreaterThan(1024);
            var contentStr = System.Text.Encoding.UTF8.GetString(content);
            contentStr.Should().NotContain("secret");

            // Verify we can re-open with the same key
            using (var connection2 = new TursoConnection($"Data Source={tempPath};Encryption Cipher=aegis256;Encryption Key={hexkey}"))
            {
                connection2.Open();

                using var select = new TursoCommand(connection2, "SELECT * FROM t");
                using var reader = select.ExecuteReader();
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("secret");
            }

            // Verify opening with wrong key fails
            Action openWithWrongKey = () =>
            {
                using var conn = new TursoConnection($"Data Source={tempPath};Encryption Cipher=aegis256;Encryption Key={wrongKey}");
                conn.Open();
                using var select = new TursoCommand(conn, "SELECT * FROM t");
                using var reader = select.ExecuteReader();
                reader.Read();
            };
            openWithWrongKey.Should().Throw<Exception>();

            // Verify opening without encryption fails
            Action openWithoutEncryption = () =>
            {
                using var conn = new TursoConnection($"Data Source={tempPath}");
                conn.Open();
                using var select = new TursoCommand(conn, "SELECT * FROM t");
                using var reader = select.ExecuteReader();
                reader.Read();
            };
            openWithoutEncryption.Should().Throw<Exception>();
        }
        finally
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
        }
    }
}