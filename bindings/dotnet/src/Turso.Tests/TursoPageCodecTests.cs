using AwesomeAssertions;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;
using Turso.Tests.Support;
using static Turso.Tests.Support.PageCodecTestFiles;

namespace Turso.Tests;

public class TursoPageCodecTests
{
    [Test]
    public void RawOpenRoundTripsThroughCodecWithoutPlaintext()
    {
        using var directory = new TemporaryDirectory();
        var path = directory.File("codec.db");
        using var codec = new AesGcmTestPageCodec(AesGcmTestPageCodec.NewKey());

        using (var database = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
        {
            Execute(database, CreateTableSql);
            Execute(database, InsertRowsSql);

            ReadFile(path + "-wal").Length.Should().BeGreaterThan(0);
            AssertNoPlaintext(path + "-wal");

            Execute(database, "PRAGMA wal_checkpoint(TRUNCATE)");
            AssertNoPlaintext(path);
        }

        AssertNoPlaintext(path);
        ReadFile(path).AsSpan(0, 16).SequenceEqual(AesGcmTestPageCodec.Magic).Should().BeTrue();

        using var readOnly = TursoBindings.OpenDatabaseReadOnlyWithPageCodec(path, codec);
        Scalar(readOnly, CountRowsSql).Should().Be((long)RowCount);
        Scalar(readOnly, "PRAGMA integrity_check").Should().Be("ok");
        FluentActions.Invoking(() => Execute(readOnly, $"INSERT INTO items(name) VALUES ('{Marker}')"))
            .Should().Throw<TursoException>();
    }

    [Test]
    public void RawSharedDatabaseConnectionsUseCodec()
    {
        using var directory = new TemporaryDirectory();
        var path = directory.File("codec.db");
        using var codec = new AesGcmTestPageCodec(AesGcmTestPageCodec.NewKey());

        using (var database = TursoBindings.OpenSharedDatabase(path, codec))
        using (var writer = TursoBindings.Connect(database))
        using (var reader = TursoBindings.Connect(database))
        {
            Execute(writer, CreateTableSql);
            Execute(writer, InsertRowsSql);
            Scalar(reader, CountRowsSql).Should().Be((long)RowCount);
            AssertNoPlaintext(path + "-wal");
        }

        AssertNoPlaintext(path);
        AssertNoPlaintext(path + "-wal");

        using var wrongCodec = new AesGcmTestPageCodec(AesGcmTestPageCodec.NewKey());
        FluentActions.Invoking(() =>
            {
                using var database = TursoBindings.OpenSharedDatabase(path, wrongCodec);
                using var connection = TursoBindings.Connect(database);
                Scalar(connection, CountRowsSql);
            })
            .Should().Throw<TursoException>();
    }

    [Test]
    public void CodecExceptionFailsTheOperationInsteadOfCrossingTheCallback()
    {
        using var directory = new TemporaryDirectory();
        var path = directory.File("codec.db");
        using var inner = new AesGcmTestPageCodec(AesGcmTestPageCodec.NewKey());
        var codec = new ThrowingPageCodec(inner);

        using (var database = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
        {
            Execute(database, CreateTableSql);
            Execute(database, InsertRowsSql);
        }

        codec.ThrowOnDecode = true;
        FluentActions.Invoking(() =>
            {
                using var database = TursoBindings.OpenDatabaseWithPageCodec(path, codec);
                Scalar(database, CountRowsSql);
            })
            .Should().Throw<TursoException>();

        codec.ThrowOnDecode = false;
        using var reopened = TursoBindings.OpenDatabaseWithPageCodec(path, codec);
        Scalar(reopened, CountRowsSql).Should().Be((long)RowCount);
    }

    [Test]
    public void CodecIdMustBeSixteenNonZeroBytes()
    {
        FluentActions.Invoking(() => TursoPageCodecId.FromGuid(Guid.Empty)).Should().Throw<ArgumentException>();
        FluentActions.Invoking(() => TursoPageCodecId.FromBytes(new byte[16])).Should().Throw<ArgumentException>();
        FluentActions.Invoking(() => TursoPageCodecId.FromBytes(new byte[15])).Should().Throw<ArgumentException>();

        var bytes = Enumerable.Range(1, 16).Select(value => (byte)value).ToArray();
        TursoPageCodecId.FromBytes(bytes).Should().Be(TursoPageCodecId.FromBytes(bytes));
        TursoPageCodecId.FromBytes(bytes).Should().NotBe(TursoPageCodecId.FromGuid(Guid.NewGuid()));
    }

    private static void Execute(TursoDatabaseHandle database, string sql)
    {
        using var statement = TursoBindings.PrepareStatement(database, sql);
        while (TursoBindings.Read(statement))
        {
        }
    }

    private static object? Scalar(TursoDatabaseHandle database, string sql)
    {
        using var statement = TursoBindings.PrepareStatement(database, sql);
        TursoBindings.Read(statement).Should().BeTrue();
        var value = TursoBindings.GetValue(statement, 0);
        return value.ValueType switch
        {
            Turso.Raw.Public.Value.TursoValueType.Integer => value.IntValue,
            Turso.Raw.Public.Value.TursoValueType.Text => value.StringValue,
            _ => null,
        };
    }

    private sealed class ThrowingPageCodec(ITursoPageCodec inner) : ITursoPageCodec
    {
        public volatile bool ThrowOnDecode;

        public TursoPageCodecId CodecId => inner.CodecId;

        public byte ReservedSpace => inner.ReservedSpace;

        public TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix) => inner.ProbeHeader(rawPage1Prefix);

        public void DecodePage(uint pageNo, TursoPageCodecLocation location, ReadOnlySpan<byte> input, Span<byte> output)
        {
            if (ThrowOnDecode)
                throw new InvalidOperationException("decode failure requested by the test");

            inner.DecodePage(pageNo, location, input, output);
        }

        public void EncodePage(uint pageNo, TursoPageCodecLocation location, ReadOnlySpan<byte> input, Span<byte> output)
            => inner.EncodePage(pageNo, location, input, output);
    }
}
