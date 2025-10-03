using AwesomeAssertions;

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
            .WithMessage("Unable to prepare statment: Parse error: no such table: table_that_does_not_exist");
    }

    [Test]
    [Ignore("Need to fix read data after commit")]
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
        var readerBefore = selectBefore.ExecuteReader();
        readerBefore.Read().Should().BeTrue();
        readerBefore.GetInt32(0).Should().Be(0);

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
}