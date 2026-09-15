using System.Data;
using System.Data.Common;
using System.Text;
using AwesomeAssertions;

namespace Turso.Tests;

public class AdoNetCompatibilityTests
{
    private enum LongEnum : long
    {
        Value = 42,
    }

    private enum ByteEnum : byte
    {
        Value = 7,
    }

    [Test]
    public void EnumAndGuidParametersUseCompatibleStorageClasses()
    {
        var guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

        new TursoParameter(LongEnum.Value).ToValue().IntValue.Should().Be(42);
        new TursoParameter(ByteEnum.Value).ToValue().IntValue.Should().Be(7);
        var guidValue = new TursoParameter(guid).ToValue();
        guidValue.ValueType.Should().Be(Turso.Raw.Public.Value.TursoValueType.Text);
        guidValue.StringValue.Should().Be(guid.ToString());
    }

    [Test]
    public void TursoUrlUsesHttps()
    {
        var options = TursoConnectionOptions.Parse("Data Source=turso://example.turso.io");

        options.IsRemote.Should().BeTrue();
        options.GetRemoteUri().Should().Be(new Uri("https://example.turso.io/"));

        TursoConnectionOptions.Parse("Data Source=turso://example.turso.io;Tls=False")
            .Invoking(x => x.GetRemoteUri())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Tls=False conflicts with the turso URL scheme.");
    }

    [Test]
    public void RepeatableReadUsesSerializableTransactions()
    {
        using var connection = new TursoConnection();
        connection.Open();
        using var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);

        transaction.IsolationLevel.Should().Be(IsolationLevel.Serializable);
        transaction.Rollback();
    }

    [Test]
    public void LocalReaderExposesDeclaredTypesSchemaAndGuids()
    {
        var guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        using var connection = new TursoConnection();
        connection.Open();

        using (var create = new TursoCommand(
                   connection,
                   "CREATE TABLE values_table(id INTEGER, name TEXT, guid_text GUID, guid_binary GUID, guid_utf8 GUID)"))
        {
            create.ExecuteNonQuery();
        }

        using (var insert = new TursoCommand(
                   connection,
                   "INSERT INTO values_table VALUES(NULL, NULL, ?, ?, ?)"))
        {
            insert.Parameters.Add(guid.ToString());
            insert.Parameters.Add(guid.ToByteArray());
            insert.Parameters.Add(Encoding.UTF8.GetBytes(guid.ToString()));
            insert.ExecuteNonQuery();
        }

        using (var command = new TursoCommand(
                   connection,
                   "SELECT id, name, guid_text, guid_binary, guid_utf8 FROM values_table"))
        using (var reader = command.ExecuteReader())
        {
            reader.GetFieldType(0).Should().Be(typeof(long));
            reader.GetFieldType(1).Should().Be(typeof(string));
            reader.GetFieldType(2).Should().Be(typeof(object));
            reader.GetDataTypeName(2).Should().Be("GUID");
            reader.Invoking(x => x.GetFieldType(-1)).Should().Throw<ArgumentOutOfRangeException>();
            reader.Invoking(x => x.GetFieldType(reader.FieldCount)).Should().Throw<IndexOutOfRangeException>();

            var schema = reader.GetSchemaTable();
            schema.Should().NotBeNull();
            schema!.Rows.Count.Should().Be(5);
            schema.Rows[0][SchemaTableColumn.DataType].Should().Be(typeof(long));
            schema.Rows[0][SchemaTableColumn.ProviderType].Should().Be((int)DbType.Int64);
            schema.Rows[1][SchemaTableColumn.DataType].Should().Be(typeof(string));

            reader.Read().Should().BeTrue();
            reader.GetFieldType(0).Should().Be(typeof(long));
            reader.GetFieldType(2).Should().Be(typeof(string));
            reader.GetValue(0).Should().Be(DBNull.Value);
            reader.GetValue(1).Should().Be(DBNull.Value);
            reader.GetValue(2).Should().Be(guid.ToString());
            reader.GetValue(3).Should().BeOfType<byte[]>();
            reader.GetValue(4).Should().BeOfType<byte[]>();
            reader.GetGuid(2).Should().Be(guid);
            reader.GetGuid(3).Should().Be(guid);
            reader.GetGuid(4).Should().Be(guid);
        }

        using var loadCommand = new TursoCommand(connection, "SELECT id, name FROM values_table");
        using var loadReader = loadCommand.ExecuteReader();
        var table = new DataTable();
        table.Load(loadReader);
        table.Rows.Count.Should().Be(1);
        table.Columns["id"]!.DataType.Should().Be(typeof(long));
        table.Columns["name"]!.DataType.Should().Be(typeof(string));
    }
}
