using AwesomeAssertions;
using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;
using Turso.Tests.Support;

namespace Turso.Tests;

public class TursoPageCodecTests
{
    private const string LegacyPassword = "legacy-password";
    private const string WxSQLitePassword = "wxsqlite-password";
    private const string TursoEncryptionKey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    [Test]
    public void ManagedPageCodecRoundTripsDatabaseFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-codec-{Guid.NewGuid():N}.db");
        try
        {
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, new XorPageCodec(0xA5)))
            {
                Execute(db, "CREATE TABLE data(value TEXT)");
                Execute(db, "INSERT INTO data VALUES ('encrypted')");
            }

            Assert.Throws<TursoException>(() => TursoBindings.OpenDatabase(path));

            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, new XorPageCodec(0xA5)))
            {
                ScalarText(db, "SELECT value FROM data").Should().Be("encrypted");
                Execute(db, "INSERT INTO data VALUES ('written-through-codec')");
            }

            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, new XorPageCodec(0xA5)))
            {
                ScalarLong(db, "SELECT COUNT(*) FROM data").Should().Be(2);
            }
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
        }
    }

    [Test]
    public void RawPageCodecApiRejectsNullArguments()
    {
        Assert.Throws<ArgumentNullException>(() => TursoBindings.OpenDatabaseWithPageCodec(null!, new XorPageCodec(0xA5)));
        Assert.Throws<ArgumentNullException>(() => TursoBindings.OpenDatabaseWithPageCodec(":memory:", null!));
    }

    [Test]
    public void ReadOnlyRawOpenDoesNotCreateMissingDatabaseFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-readonly-missing-{Guid.NewGuid():N}.db");
        try
        {
            Assert.Throws<TursoException>(() => TursoBindings.OpenDatabaseReadOnly(path));
            File.Exists(path).Should().BeFalse();

            Assert.Throws<TursoException>(() =>
                TursoBindings.OpenDatabaseReadOnlyWithPageCodec(path, new XorPageCodec(0xA5)));
            File.Exists(path).Should().BeFalse();
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
        }
    }

    [Test]
    public void ManagedPageCodecCallbackErrorsPropagate()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-codec-error-{Guid.NewGuid():N}.db");
        try
        {
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, new XorPageCodec(0xA5)))
            {
                Execute(db, "CREATE TABLE data(value TEXT)");
            }

            var exception = Assert.Throws<TursoException>(() =>
                TursoBindings.OpenDatabaseWithPageCodec(path, new ThrowingProbeCodec()));
            exception!.Message.Should().Contain("probe failed");
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
        }
    }

    [Test]
    public void ManagedPageCodecEncodeErrorsPropagate()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-codec-encode-error-{Guid.NewGuid():N}.db");
        try
        {
            using var db = TursoBindings.OpenDatabaseWithPageCodec(path, new ThrowingEncodeCodec());
            var exception = Assert.Throws<TursoException>(() => Execute(db, "CREATE TABLE data(value TEXT)"));
            exception!.Message.Should().Contain("encode failed");
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
        }
    }

    [Test]
    public void ManagedPageCodecRejectsOverlappingCachedDatabaseOpen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-codec-cache-{Guid.NewGuid():N}.db");
        try
        {
            using var db = TursoBindings.OpenDatabaseWithPageCodec(path, new XorPageCodec(0xA5));
            Execute(db, "CREATE TABLE data(value TEXT)");

            Assert.Throws<TursoException>(() =>
                TursoBindings.OpenDatabaseWithPageCodec(path, new XorPageCodec(0xA5)));
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
        }
    }

    [Test]
    public void ManagedPageCodecNativeLifetimeKeepsCodecAliveUntilDatabaseDispose()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-codec-lifetime-{Guid.NewGuid():N}.db");
        try
        {
            var (db, codecReference) = OpenDatabaseWithTrackedCodec(path);
            using (db)
            {
                ForceFullCollection();
                codecReference.IsAlive.Should().BeTrue();
                Execute(db, "CREATE TABLE data(value TEXT)");
                Execute(db, "INSERT INTO data VALUES ('callback-still-alive')");
                ScalarText(db, "SELECT value FROM data").Should().Be("callback-still-alive");
            }

            ForceFullCollection();
            codecReference.IsAlive.Should().BeFalse();
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    public void TursoConnectionPageCodecRoundTripsDatabaseFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-connection-codec-{Guid.NewGuid():N}.db");
        try
        {
            using (var connection = new TursoConnection($"Data Source={path}"))
            {
                connection.PageCodec = new XorPageCodec(0xA5);
                connection.Open();
                connection.ExecuteNonQuery("CREATE TABLE data(value TEXT)");
                connection.ExecuteNonQuery("INSERT INTO data VALUES ('connection-codec')");
            }

            Assert.Throws<TursoException>(() =>
            {
                using var connection = new TursoConnection($"Data Source={path}");
                connection.Open();
                ScalarText(connection, "SELECT value FROM data");
            });

            using (var connection = new TursoConnection($"Data Source={path}"))
            {
                connection.PageCodec = new XorPageCodec(0xA5);
                connection.Open();
                ScalarText(connection, "SELECT value FROM data").Should().Be("connection-codec");
            }
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
        }
    }

    [Test]
    public void TursoConnectionPageCodecPropertiesCannotChangeWhileOpen()
    {
        using var connection = new TursoConnection("Data Source=:memory:");
        connection.PageCodec = new XorPageCodec(0xA5);
        connection.Open();

        Assert.Throws<InvalidOperationException>(() => connection.PageCodec = new XorPageCodec(0x5A));
        Assert.Throws<InvalidOperationException>(() => connection.PageCodecReservedSpace = 4);
    }

    [Test]
    public void TursoConnectionPageCodecCannotBeCombinedWithBuiltInEncryption()
    {
        using var connection = new TursoConnection(
            "Data Source=:memory:;Encryption Cipher=aegis256;Encryption Key=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327");
        connection.PageCodec = new XorPageCodec(0xA5);

        Assert.Throws<InvalidOperationException>(connection.Open);
    }

    [Test]
    public void TursoConnectionPageCodecReservedSpaceIsWrittenToDatabaseHeader()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-connection-codec-reserved-{Guid.NewGuid():N}.db");
        try
        {
            using (var connection = new TursoConnection($"Data Source={path}"))
            {
                connection.PageCodec = new XorPageCodec(0xA5);
                connection.PageCodecReservedSpace = 4;
                connection.Open();
                connection.ExecuteNonQuery("PRAGMA journal_mode=DELETE");
                connection.ExecuteNonQuery("CREATE TABLE data(value TEXT)");
            }

            ReadDecodedHeader(path, 0xA5)[20].Should().Be(4);
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
            TryDelete($"{path}-journal");
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void MigratesLegacyExternalCryptoToTursoEncryption()
    {
        var legacyPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-legacy-source-{Guid.NewGuid():N}.db");
        var tursoPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-turso-target-{Guid.NewGuid():N}.db");
        try
        {
            using (var legacyCodec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(LegacyPassword, encryptPage1: true))
            using (var legacySource = new TursoConnection($"Data Source={legacyPath}"))
            {
                legacySource.PageCodec = legacyCodec;
                legacySource.Open();
                CreateSampleData(legacySource);
            }

            using (var legacyCodec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(LegacyPassword, encryptPage1: true))
            using (var legacySource = new TursoConnection($"Data Source={legacyPath}"))
            using (var tursoTarget = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            {
                legacySource.PageCodec = legacyCodec;
                legacySource.Open();
                tursoTarget.Open();
                CopySampleData(legacySource, tursoTarget);
            }

            AssertPlainConnectionCannotRead(tursoPath);

            using (var tursoTarget = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            {
                tursoTarget.Open();
                ReadSampleData(tursoTarget).Should().Equal(ExpectedSampleRows());
            }

            Assert.That(() =>
            {
                using var legacyCodec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(LegacyPassword, encryptPage1: true);
                using var legacyConnection = new TursoConnection($"Data Source={tursoPath}");
                legacyConnection.PageCodec = legacyCodec;
                legacyConnection.Open();
                ReadSampleData(legacyConnection);
            }, Throws.Exception);
        }
        finally
        {
            DeleteDatabaseFiles(legacyPath);
            DeleteDatabaseFiles(tursoPath);
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void MigratesTursoEncryptionToLegacyExternalCrypto()
    {
        var tursoPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-turso-source-{Guid.NewGuid():N}.db");
        var legacyPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-legacy-target-{Guid.NewGuid():N}.db");
        try
        {
            using (var tursoSource = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            {
                tursoSource.Open();
                CreateSampleData(tursoSource);
            }

            using (var tursoSource = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            using (var legacyCodec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(LegacyPassword, encryptPage1: true))
            using (var legacyTarget = new TursoConnection($"Data Source={legacyPath}"))
            {
                legacyTarget.PageCodec = legacyCodec;
                tursoSource.Open();
                legacyTarget.Open();
                CopySampleData(tursoSource, legacyTarget);
            }

            AssertPlainConnectionCannotRead(legacyPath);

            using (var legacyCodec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(LegacyPassword, encryptPage1: true))
            using (var legacyTarget = new TursoConnection($"Data Source={legacyPath}"))
            {
                legacyTarget.PageCodec = legacyCodec;
                legacyTarget.Open();
                ReadSampleData(legacyTarget).Should().Equal(ExpectedSampleRows());
            }

            Assert.That(() =>
            {
                using var tursoConnection = new TursoConnection(TursoEncryptedConnectionString(legacyPath));
                tursoConnection.Open();
                ReadSampleData(tursoConnection);
            }, Throws.Exception);
        }
        finally
        {
            DeleteDatabaseFiles(tursoPath);
            DeleteDatabaseFiles(legacyPath);
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void SystemDataSQLiteLegacyCryptoApiCodecRoundTripsDatabaseFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-legacy-system-data-sqlite-codec-{Guid.NewGuid():N}.db");
        try
        {
            const string password = "legacy-password";
            using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(password))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                Execute(db, "CREATE TABLE data(value TEXT)");
                Execute(db, "INSERT INTO data VALUES ('legacy-cryptoapi')");
            }

            AssertPlainTursoCannotReadLegacyEncryptedData(path);

            using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(password))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                ScalarText(db, "SELECT value FROM data").Should().Be("legacy-cryptoapi");
                Execute(db, "INSERT INTO data VALUES ('written-through-codec')");
            }

            using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(password))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                ScalarLong(db, "SELECT COUNT(*) FROM data").Should().Be(2);
            }
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
            TryDelete($"{path}-journal");
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void SystemDataSQLiteLegacyCryptoApiCodecMatchesPageOneRules()
    {
        var page = CreateSqliteHeaderPage();
        Span<byte> encoded = stackalloc byte[page.Length];
        Span<byte> decoded = stackalloc byte[page.Length];

        using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword("legacy-password", encryptPage1: false))
        {
            codec.ProbeHeader(page).Should().Be(new TursoPageCodecHeaderInfo(512, 0));
            codec.EncodePage(1, TursoPageCodecLocation.Database, page, encoded);
            encoded.SequenceEqual(page).Should().BeTrue();
        }

        using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword("legacy-password", encryptPage1: true))
        {
            codec.EncodePage(1, TursoPageCodecLocation.Database, page, encoded);
            encoded[..16].SequenceEqual("SQLite format 3\0"u8).Should().BeFalse();
        }

        using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword("legacy-password", encryptPage1: false))
        {
            codec.ProbeHeader(encoded).Should().Be(new TursoPageCodecHeaderInfo(512, 0));
            codec.DecodePage(1, TursoPageCodecLocation.Database, encoded, decoded);
            decoded.SequenceEqual(page).Should().BeTrue();
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void SystemDataSQLiteLegacyCryptoApiDatabaseRoundTripsWithManagedCodec()
    {
        using var fixture = SystemDataSQLiteFixture.TryLoad();
        var path = Path.Combine(Path.GetTempPath(), $"turso-system-data-sqlite-codec-{Guid.NewGuid():N}.db");
        try
        {
            const string password = "legacy-password";
            fixture.ExecuteWithPassword(
                path,
                password,
                "PRAGMA journal_mode=DELETE",
                "CREATE TABLE data(value TEXT)",
                "INSERT INTO data VALUES ('system-data-sqlite')");
            var encryptPage1 = IsPageOneEncrypted(path);

            AssertPlainTursoCannotReadLegacyEncryptedData(path);

            using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(password, encryptPage1))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                ScalarText(db, "SELECT value FROM data").Should().Be("system-data-sqlite");
                Execute(db, "INSERT INTO data VALUES ('written-through-turso')");
            }

            fixture.ScalarLongWithPassword(path, password, "SELECT COUNT(*) FROM data").Should().Be(2);
            fixture.ScalarTextWithPassword(
                    path,
                    password,
                    "SELECT value FROM data ORDER BY rowid DESC LIMIT 1")
                .Should()
                .Be("written-through-turso");
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
            TryDelete($"{path}-journal");
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void SystemDataSQLiteLegacyCryptoApiWrongPasswordFails()
    {
        using var fixture = SystemDataSQLiteFixture.TryLoad();
        var path = Path.Combine(Path.GetTempPath(), $"turso-system-data-sqlite-codec-wrong-password-{Guid.NewGuid():N}.db");
        try
        {
            const string password = "legacy-password";
            fixture.ExecuteWithPassword(
                path,
                password,
                "CREATE TABLE data(value TEXT)",
                "INSERT INTO data VALUES ('system-data-sqlite')");
            var encryptPage1 = IsPageOneEncrypted(path);

            Assert.Throws<TursoException>(() =>
            {
                using var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword("wrong-password", encryptPage1);
                using var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec);
                ScalarText(db, "SELECT value FROM data");
            });
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
            TryDelete($"{path}-journal");
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void SystemDataSQLiteLegacyCryptoApiRekeyAndClearPassword()
    {
        using var fixture = SystemDataSQLiteFixture.TryLoad();
        var path = Path.Combine(Path.GetTempPath(), $"turso-system-data-sqlite-codec-rekey-{Guid.NewGuid():N}.db");
        try
        {
            const string oldPassword = "legacy-password";
            const string newPassword = "new-legacy-password";
            fixture.ExecuteWithPassword(
                path,
                oldPassword,
                "PRAGMA journal_mode=DELETE",
                "CREATE TABLE data(value TEXT)",
                "INSERT INTO data VALUES ('system-data-sqlite')");
            var encryptPage1 = IsPageOneEncrypted(path);

            fixture.ChangePassword(path, oldPassword, newPassword);

            Assert.Throws<TursoException>(() =>
            {
                using var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(oldPassword, encryptPage1);
                using var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec);
                ScalarText(db, "SELECT value FROM data");
            });

            using (var codec = SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(newPassword, encryptPage1))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                ScalarText(db, "SELECT value FROM data").Should().Be("system-data-sqlite");
            }

            fixture.ChangePassword(path, newPassword, string.Empty);

            using (var db = TursoBindings.OpenDatabase(path))
            {
                ScalarText(db, "SELECT value FROM data").Should().Be("system-data-sqlite");
            }
        }
        finally
        {
            TryDelete(path);
            TryDelete($"{path}-wal");
            TryDelete($"{path}-shm");
            TryDelete($"{path}-journal");
        }
    }

    [Test]
    public void WxSQLite3Aes128CbcCodecMatchesModernPageOneRules()
    {
        var page = CreateSqliteHeaderPage(pageSize: 4096);
        Span<byte> encoded = stackalloc byte[page.Length];
        Span<byte> decoded = stackalloc byte[page.Length];

        using var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword);
        codec.ProbeHeader(page).Should().Be(new TursoPageCodecHeaderInfo(4096, 0));
        codec.EncodePage(1, TursoPageCodecLocation.Database, page, encoded);

        encoded[..16].SequenceEqual("SQLite format 3\0"u8).Should().BeFalse();
        encoded[16..24].SequenceEqual(page.AsSpan(16, 8)).Should().BeTrue();
        codec.ProbeHeader(encoded).Should().Be(new TursoPageCodecHeaderInfo(4096, 0));

        codec.DecodePage(1, TursoPageCodecLocation.Database, encoded, decoded);
        decoded.SequenceEqual(page).Should().BeTrue();
    }

    [Test]
    public void WxSQLite3Aes128CbcCodecRoundTripsDatabaseFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-wxsqlite3-aes128-codec-{Guid.NewGuid():N}.db");
        try
        {
            using (var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                Execute(db, "CREATE TABLE data(value TEXT)");
                Execute(db, "INSERT INTO data VALUES ('wxsqlite3-aes128cbc')");
            }

            AssertPlainTursoCannotReadLegacyEncryptedData(path);

            using (var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                ScalarText(db, "SELECT value FROM data").Should().Be("wxsqlite3-aes128cbc");
                Execute(db, "INSERT INTO data VALUES ('written-through-codec')");
            }

            using (var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                ScalarLong(db, "SELECT COUNT(*) FROM data").Should().Be(2);
            }
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    public void DetectsWxSQLite3Aes128CbcCodecAfterEarlierCandidateFails()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-detect-wxsqlite3-aes128-{Guid.NewGuid():N}.db");
        try
        {
            using (var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                Execute(db, "CREATE TABLE data(value TEXT)");
                Execute(db, "INSERT INTO data VALUES ('detected-wxsqlite3')");
            }

            var candidates = new[]
            {
                new ExternalCodecCandidate("wrong xor codec", _ => new XorPageCodec(0x5A)),
                new ExternalCodecCandidate(
                    "wxSQLite3/SQLite3MC aes128cbc",
                    password => WxSQLite3Aes128CbcCodec.FromPassword(password)),
            };

            using var detected = EncryptedDatabaseDetector.Open(path, WxSQLitePassword, candidates);
            detected.CodecName.Should().Be("wxSQLite3/SQLite3MC aes128cbc");
            ScalarText(detected.Connection, "SELECT value FROM data").Should().Be("detected-wxsqlite3");
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    public void DetectingEncryptedDatabaseReportsCandidateFailures()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-detect-failure-{Guid.NewGuid():N}.db");
        try
        {
            using (var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                Execute(db, "CREATE TABLE data(value TEXT)");
            }

            var candidates = new[]
            {
                new ExternalCodecCandidate("wrong xor codec", _ => new XorPageCodec(0x5A)),
            };

            var exception = Assert.Throws<AggregateException>(() =>
                EncryptedDatabaseDetector.Open(path, WxSQLitePassword, candidates));
            exception!.Message.Should().Contain("None of the configured external SQLite encryption codecs");
            exception.InnerExceptions.Should().ContainSingle()
                .Which.Message.Should().Contain("wrong xor codec");
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    public void DetectingEncryptedDatabaseRejectsMissingFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"turso-detect-missing-{Guid.NewGuid():N}.db");
        var candidates = new[]
        {
            new ExternalCodecCandidate("would create if attempted", _ => new XorPageCodec(0xA5)),
        };

        Assert.Throws<FileNotFoundException>(() =>
            EncryptedDatabaseDetector.Open(path, WxSQLitePassword, candidates));
        File.Exists(path).Should().BeFalse();
    }

    [Test]
    public void MigratesWxSQLite3Aes128CbcExternalCryptoToTursoEncryption()
    {
        var wxPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-wxsqlite3-source-{Guid.NewGuid():N}.db");
        var tursoPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-wxsqlite3-turso-target-{Guid.NewGuid():N}.db");
        try
        {
            using (var wxCodec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var wxSource = new TursoConnection($"Data Source={wxPath}"))
            {
                wxSource.PageCodec = wxCodec;
                wxSource.Open();
                CreateSampleData(wxSource);
            }

            using (var wxCodec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var wxSource = new TursoConnection($"Data Source={wxPath}"))
            using (var tursoTarget = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            {
                wxSource.PageCodec = wxCodec;
                wxSource.Open();
                tursoTarget.Open();
                CopySampleData(wxSource, tursoTarget);
            }

            AssertPlainConnectionCannotRead(tursoPath);

            using (var tursoTarget = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            {
                tursoTarget.Open();
                ReadSampleData(tursoTarget).Should().Equal(ExpectedSampleRows());
            }
        }
        finally
        {
            DeleteDatabaseFiles(wxPath);
            DeleteDatabaseFiles(tursoPath);
        }
    }

    [Test]
    public void MigratesTursoEncryptionToWxSQLite3Aes128CbcExternalCrypto()
    {
        var tursoPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-turso-wxsqlite3-source-{Guid.NewGuid():N}.db");
        var wxPath = Path.Combine(Path.GetTempPath(), $"turso-migrate-wxsqlite3-target-{Guid.NewGuid():N}.db");
        try
        {
            using (var tursoSource = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            {
                tursoSource.Open();
                CreateSampleData(tursoSource);
            }

            using (var tursoSource = new TursoConnection(TursoEncryptedConnectionString(tursoPath)))
            using (var wxCodec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var wxTarget = new TursoConnection($"Data Source={wxPath}"))
            {
                wxTarget.PageCodec = wxCodec;
                tursoSource.Open();
                wxTarget.Open();
                CopySampleData(tursoSource, wxTarget);
            }

            AssertPlainConnectionCannotRead(wxPath);

            using (var wxCodec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var wxTarget = new TursoConnection($"Data Source={wxPath}"))
            {
                wxTarget.PageCodec = wxCodec;
                wxTarget.Open();
                ReadSampleData(wxTarget).Should().Equal(ExpectedSampleRows());
            }
        }
        finally
        {
            DeleteDatabaseFiles(tursoPath);
            DeleteDatabaseFiles(wxPath);
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void Sqlite3SecureAes128CbcDatabaseRoundTripsWithManagedCodec()
    {
        using var fixture = Sqlite3SecureFixture.TryLoad();
        fixture.DefaultCipherName.Should().Be("aes128cbc");

        var path = Path.Combine(Path.GetTempPath(), $"turso-sqlite3secure-aes128-codec-{Guid.NewGuid():N}.db");
        try
        {
            fixture.ExecuteWithPassword(
                path,
                WxSQLitePassword,
                "PRAGMA journal_mode=DELETE",
                "CREATE TABLE data(value TEXT)",
                "INSERT INTO data VALUES ('sqlite3secure')");

            AssertPlainTursoCannotReadLegacyEncryptedData(path);

            using (var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec))
            {
                Execute(db, "PRAGMA journal_mode=DELETE");
                ScalarText(db, "SELECT value FROM data").Should().Be("sqlite3secure");
                Execute(db, "INSERT INTO data VALUES ('written-through-turso')");
            }

            fixture.ScalarLongWithPassword(path, WxSQLitePassword, "SELECT COUNT(*) FROM data").Should().Be(2);
            fixture.ScalarTextWithPassword(
                    path,
                    WxSQLitePassword,
                    "SELECT value FROM data ORDER BY rowid DESC LIMIT 1")
                .Should()
                .Be("written-through-turso");
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void Sqlite3SecureAes128CbcWrongPasswordFails()
    {
        using var fixture = Sqlite3SecureFixture.TryLoad();
        fixture.DefaultCipherName.Should().Be("aes128cbc");

        var path = Path.Combine(Path.GetTempPath(), $"turso-sqlite3secure-aes128-wrong-password-{Guid.NewGuid():N}.db");
        try
        {
            fixture.ExecuteWithPassword(
                path,
                WxSQLitePassword,
                "CREATE TABLE data(value TEXT)",
                "INSERT INTO data VALUES ('sqlite3secure')");

            Assert.Throws<TursoException>(() =>
            {
                using var codec = WxSQLite3Aes128CbcCodec.FromPassword("wrong-password");
                using var db = TursoBindings.OpenDatabaseWithPageCodec(path, codec);
                ScalarText(db, "SELECT value FROM data");
            });
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    [Platform(Include = "Win")]
    public void Sqlite3SecureAes128CbcWalCheckpointRoundTripsWithManagedCodec()
    {
        using var fixture = Sqlite3SecureFixture.TryLoad();
        fixture.DefaultCipherName.Should().Be("aes128cbc");

        var path = Path.Combine(Path.GetTempPath(), $"turso-sqlite3secure-aes128-wal-{Guid.NewGuid():N}.db");
        try
        {
            fixture.ExecuteWithPassword(
                path,
                WxSQLitePassword,
                "PRAGMA journal_mode=WAL",
                "CREATE TABLE data(value TEXT)",
                "INSERT INTO data VALUES ('from-sqlite3secure')");

            using (var codec = WxSQLite3Aes128CbcCodec.FromPassword(WxSQLitePassword))
            using (var connection = new TursoConnection($"Data Source={path}"))
            {
                connection.PageCodec = codec;
                connection.Open();
                connection.ExecuteNonQuery("PRAGMA journal_mode=WAL");
                ScalarText(connection, "SELECT value FROM data ORDER BY rowid LIMIT 1").Should().Be("from-sqlite3secure");
                connection.ExecuteNonQuery("INSERT INTO data VALUES ('from-turso-wal')");
                connection.ExecuteNonQuery("PRAGMA wal_checkpoint(FULL)");
            }

            fixture.ScalarLongWithPassword(path, WxSQLitePassword, "SELECT COUNT(*) FROM data").Should().Be(2);
            fixture.ScalarTextWithPassword(
                    path,
                    WxSQLitePassword,
                    "SELECT value FROM data ORDER BY rowid DESC LIMIT 1")
                .Should()
                .Be("from-turso-wal");
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    private static byte[] CreateSqliteHeaderPage(int pageSize = 512)
    {
        if (pageSize is < 512 or > 65536 || (pageSize & (pageSize - 1)) != 0)
            throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize, "SQLite page size must be a power of two between 512 and 65536.");

        var page = new byte[pageSize];
        "SQLite format 3\0"u8.CopyTo(page);
        if (pageSize == 65536)
        {
            page[16] = 0x00;
            page[17] = 0x01;
        }
        else
        {
            page[16] = (byte)(pageSize >> 8);
            page[17] = (byte)pageSize;
        }

        page[18] = 1;
        page[19] = 1;
        page[20] = 0;
        page[21] = 64;
        page[22] = 32;
        page[23] = 32;
        page[100] = 13;
        return page;
    }

    private static bool IsPageOneEncrypted(string path)
    {
        Span<byte> header = stackalloc byte[16];
        using var file = File.OpenRead(path);
        file.ReadExactly(header);
        return !header.SequenceEqual("SQLite format 3\0"u8);
    }

    private static byte[] ReadDecodedHeader(string path, byte key)
    {
        var header = new byte[100];
        using var file = File.OpenRead(path);
        file.ReadExactly(header);
        for (var i = 0; i < header.Length; i++)
            header[i] ^= key;

        return header;
    }

    private static string TursoEncryptedConnectionString(string path)
    {
        return $"Data Source={path};Encryption Cipher=aegis256;Encryption Key={TursoEncryptionKey}";
    }

    private static (long Id, string Value)[] ExpectedSampleRows()
    {
        return [(1, "alpha"), (2, "bravo"), (3, "charlie")];
    }

    private static void CreateSampleData(TursoConnection connection)
    {
        connection.ExecuteNonQuery("PRAGMA journal_mode=DELETE");
        connection.ExecuteNonQuery("CREATE TABLE data(id INTEGER PRIMARY KEY, value TEXT NOT NULL)");
        foreach (var row in ExpectedSampleRows())
        {
            using var command = new TursoCommand(connection, "INSERT INTO data(id, value) VALUES (?, ?)");
            command.Parameters.Add(row.Id);
            command.Parameters.Add(row.Value);
            command.ExecuteNonQuery();
        }
    }

    private static void CopySampleData(TursoConnection source, TursoConnection target)
    {
        target.ExecuteNonQuery("PRAGMA journal_mode=DELETE");
        target.ExecuteNonQuery("CREATE TABLE data(id INTEGER PRIMARY KEY, value TEXT NOT NULL)");
        foreach (var row in ReadSampleData(source))
        {
            using var command = new TursoCommand(target, "INSERT INTO data(id, value) VALUES (?, ?)");
            command.Parameters.Add(row.Id);
            command.Parameters.Add(row.Value);
            command.ExecuteNonQuery();
        }
    }

    private static List<(long Id, string Value)> ReadSampleData(TursoConnection connection)
    {
        using var command = new TursoCommand(connection, "SELECT id, value FROM data ORDER BY id");
        using var reader = command.ExecuteReader();
        var rows = new List<(long Id, string Value)>();
        while (reader.Read())
            rows.Add((reader.GetInt64(0), reader.GetString(1)));

        return rows;
    }

    private static void AssertPlainConnectionCannotRead(string path)
    {
        Assert.That(() =>
        {
            using var connection = new TursoConnection($"Data Source={path}");
            connection.Open();
            ReadSampleData(connection);
        }, Throws.Exception);
    }

    private static void DeleteDatabaseFiles(string path)
    {
        TryDelete(path);
        TryDelete($"{path}-wal");
        TryDelete($"{path}-shm");
        TryDelete($"{path}-journal");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (TursoDatabaseHandle Database, WeakReference CodecReference) OpenDatabaseWithTrackedCodec(string path)
    {
        var codec = new XorPageCodec(0xA5);
        return (TursoBindings.OpenDatabaseWithPageCodec(path, codec), new WeakReference(codec));
    }

    private static void ForceFullCollection()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    private static void Execute(TursoDatabaseHandle db, string sql)
    {
        using var statement = TursoBindings.PrepareStatement(db, sql);
        while (TursoBindings.Read(statement))
        {
        }
    }

    private static string ScalarText(TursoDatabaseHandle db, string sql)
    {
        using var statement = TursoBindings.PrepareStatement(db, sql);
        TursoBindings.Read(statement).Should().BeTrue();
        var value = TursoBindings.GetValue(statement, 0);
        return value.StringValue;
    }

    private static long ScalarLong(TursoDatabaseHandle db, string sql)
    {
        using var statement = TursoBindings.PrepareStatement(db, sql);
        TursoBindings.Read(statement).Should().BeTrue();
        var value = TursoBindings.GetValue(statement, 0);
        return value.IntValue;
    }

    private static string ScalarText(TursoConnection connection, string sql)
    {
        using var command = new TursoCommand(connection, sql);
        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();
        return reader.GetString(0);
    }

    private static void AssertPlainTursoCannotReadLegacyEncryptedData(string path)
    {
        Assert.Throws<TursoException>(() =>
        {
            using var db = TursoBindings.OpenDatabase(path);
            ScalarText(db, "SELECT value FROM data");
        });
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class XorPageCodec(byte key) : ITursoPageCodec
    {
        public TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix)
        {
            Span<byte> header = stackalloc byte[100];
            for (var i = 0; i < header.Length; i++)
                header[i] = (byte)(rawPage1Prefix[i] ^ key);

            if (!header[..16].SequenceEqual("SQLite format 3\0"u8))
                return null;

            var pageSizeRaw = (ushort)((header[16] << 8) | header[17]);
            var pageSize = pageSizeRaw == 1 ? 65536u : pageSizeRaw;
            return new TursoPageCodecHeaderInfo(pageSize, header[20]);
        }

        public void DecodePage(
            uint pageNo,
            TursoPageCodecLocation location,
            ReadOnlySpan<byte> input,
            Span<byte> output)
        {
            Transform(input, output);
        }

        public void EncodePage(
            uint pageNo,
            TursoPageCodecLocation location,
            ReadOnlySpan<byte> input,
            Span<byte> output)
        {
            Transform(input, output);
        }

        private void Transform(ReadOnlySpan<byte> input, Span<byte> output)
        {
            input.Length.Should().Be(output.Length);
            for (var i = 0; i < input.Length; i++)
                output[i] = (byte)(input[i] ^ key);
        }
    }

    private sealed class ThrowingProbeCodec : ITursoPageCodec
    {
        public TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix)
        {
            throw new InvalidOperationException("probe failed");
        }

        public void DecodePage(
            uint pageNo,
            TursoPageCodecLocation location,
            ReadOnlySpan<byte> input,
            Span<byte> output)
        {
            input.CopyTo(output);
        }

        public void EncodePage(
            uint pageNo,
            TursoPageCodecLocation location,
            ReadOnlySpan<byte> input,
            Span<byte> output)
        {
            input.CopyTo(output);
        }
    }

    private sealed class ThrowingEncodeCodec : ITursoPageCodec
    {
        public TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix)
        {
            return null;
        }

        public void DecodePage(
            uint pageNo,
            TursoPageCodecLocation location,
            ReadOnlySpan<byte> input,
            Span<byte> output)
        {
            input.CopyTo(output);
        }

        public void EncodePage(
            uint pageNo,
            TursoPageCodecLocation location,
            ReadOnlySpan<byte> input,
            Span<byte> output)
        {
            throw new InvalidOperationException("encode failed");
        }
    }

    private sealed class SystemDataSQLiteFixture : IDisposable
    {
        private const string AssemblyPathVariable = "TURSO_TEST_SYSTEM_DATA_SQLITE_DLL";
        private const string InteropPathVariable = "TURSO_TEST_SQLITE_INTEROP_DLL";
        private readonly Type _connectionType;
        private readonly IntPtr _interopHandle;

        private SystemDataSQLiteFixture(Type connectionType, IntPtr interopHandle)
        {
            _connectionType = connectionType;
            _interopHandle = interopHandle;
        }

        public static SystemDataSQLiteFixture TryLoad()
        {
            var assemblyPath = Environment.GetEnvironmentVariable(AssemblyPathVariable);
            if (string.IsNullOrWhiteSpace(assemblyPath))
                Assert.Ignore($"{AssemblyPathVariable} must point at a codec-enabled System.Data.SQLite.dll.");

            if (!File.Exists(assemblyPath))
                Assert.Ignore($"{AssemblyPathVariable} does not exist: {assemblyPath}");

            var interopPath = Environment.GetEnvironmentVariable(InteropPathVariable);
            var interopHandle = IntPtr.Zero;
            if (!string.IsNullOrWhiteSpace(interopPath))
            {
                if (!File.Exists(interopPath))
                    Assert.Ignore($"{InteropPathVariable} does not exist: {interopPath}");

                interopHandle = NativeLibrary.Load(interopPath);
            }

            var assembly = Assembly.LoadFrom(assemblyPath);
            var connectionType = assembly.GetType("System.Data.SQLite.SQLiteConnection", throwOnError: true)!;
            return new SystemDataSQLiteFixture(connectionType, interopHandle);
        }

        public void ExecuteWithPassword(string path, string password, params string[] statements)
        {
            using var connection = CreateConnection(path, password);
            connection.Open();
            foreach (var statement in statements)
            {
                using var command = connection.CreateCommand();
                command.CommandText = statement;
                command.ExecuteNonQuery();
            }
        }

        public string ScalarTextWithPassword(string path, string password, string sql)
        {
            var value = ExecuteScalarWithPassword(path, password, sql);
            return value.Should().BeOfType<string>().Subject;
        }

        public long ScalarLongWithPassword(string path, string password, string sql)
        {
            var value = ExecuteScalarWithPassword(path, password, sql);
            return Convert.ToInt64(value);
        }

        public void ChangePassword(string path, string password, string newPassword)
        {
            using var connection = CreateConnection(path, password);
            connection.Open();

            var method = _connectionType.GetMethod(
                "ChangePassword",
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                types: [typeof(string)],
                modifiers: null);

            if (method is null)
                throw new MissingMethodException(_connectionType.FullName, "ChangePassword");

            method.Invoke(connection, [newPassword]);
        }

        public void Dispose()
        {
            if (_interopHandle != IntPtr.Zero)
                NativeLibrary.Free(_interopHandle);
        }

        private object? ExecuteScalarWithPassword(string path, string password, string sql)
        {
            using var connection = CreateConnection(path, password);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteScalar();
        }

        private DbConnection CreateConnection(string path, string password)
        {
            var connectionString = $"Data Source={path};Password={password};Pooling=False";
            var connection = Activator.CreateInstance(_connectionType, connectionString);
            return connection.Should().BeAssignableTo<DbConnection>().Subject;
        }
    }

    private sealed class Sqlite3SecureFixture : IDisposable
    {
        private const string LibraryPathVariable = "TURSO_TEST_SQLITE3SECURE_DLL";
        private readonly IntPtr _libraryHandle;
        private readonly Sqlite3Open _open;
        private readonly Sqlite3Close _close;
        private readonly Sqlite3Key _key;
        private readonly Sqlite3Exec _exec;
        private readonly Sqlite3ErrMsg _errmsg;
        private readonly Sqlite3Free _free;
        private readonly WxSqlite3Config _config;

        private Sqlite3SecureFixture(IntPtr libraryHandle)
        {
            _libraryHandle = libraryHandle;
            _open = GetExport<Sqlite3Open>(libraryHandle, "sqlite3_open");
            _close = GetExport<Sqlite3Close>(libraryHandle, "sqlite3_close");
            _key = GetExport<Sqlite3Key>(libraryHandle, "sqlite3_key");
            _exec = GetExport<Sqlite3Exec>(libraryHandle, "sqlite3_exec");
            _errmsg = GetExport<Sqlite3ErrMsg>(libraryHandle, "sqlite3_errmsg");
            _free = GetExport<Sqlite3Free>(libraryHandle, "sqlite3_free");
            _config = GetExport<WxSqlite3Config>(libraryHandle, "wxsqlite3_config");
        }

        public string DefaultCipherName
        {
            get
            {
                using var parameterName = new NativeUtf8String("default:cipher");
                return CipherName(_config(IntPtr.Zero, parameterName.Pointer, -1));
            }
        }

        public static Sqlite3SecureFixture TryLoad()
        {
            var libraryPath = Environment.GetEnvironmentVariable(LibraryPathVariable);
            if (string.IsNullOrWhiteSpace(libraryPath))
                Assert.Ignore($"{LibraryPathVariable} must point at sqlite3secure.dll from Devolutions.Sqlite3secure.");

            if (!File.Exists(libraryPath))
                Assert.Ignore($"{LibraryPathVariable} does not exist: {libraryPath}");

            var handle = NativeLibrary.Load(libraryPath);
            try
            {
                return new Sqlite3SecureFixture(handle);
            }
            catch
            {
                NativeLibrary.Free(handle);
                throw;
            }
        }

        public void ExecuteWithPassword(string path, string password, params string[] statements)
        {
            var db = OpenWithPassword(path, password);
            try
            {
                foreach (var statement in statements)
                    Execute(db, statement);
            }
            finally
            {
                _close(db);
            }
        }

        public string ScalarTextWithPassword(string path, string password, string sql)
        {
            return ExecuteScalarWithPassword(path, password, sql).Should().NotBeNull().And.BeOfType<string>().Subject;
        }

        public long ScalarLongWithPassword(string path, string password, string sql)
        {
            return long.Parse(ScalarTextWithPassword(path, password, sql));
        }

        public void Dispose()
        {
            NativeLibrary.Free(_libraryHandle);
        }

        private static T GetExport<T>(IntPtr libraryHandle, string name)
            where T : Delegate
        {
            var function = NativeLibrary.GetExport(libraryHandle, name);
            return Marshal.GetDelegateForFunctionPointer<T>(function);
        }

        private static string CipherName(int cipherIndex)
        {
            return cipherIndex switch
            {
                1 => "aes128cbc",
                2 => "aes256cbc",
                3 => "chacha20",
                4 => "sqlcipher",
                _ => $"unknown:{cipherIndex}"
            };
        }

        private IntPtr OpenWithPassword(string path, string password)
        {
            using var pathUtf8 = new NativeUtf8String(path);
            var rc = _open(pathUtf8.Pointer, out var db);
            if (rc != 0)
                throw new InvalidOperationException($"sqlite3_open failed: rc={rc}");

            try
            {
                var passwordBytes = Encoding.UTF8.GetBytes(password);
                try
                {
                    rc = _key(db, passwordBytes, passwordBytes.Length);
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(passwordBytes);
                }

                if (rc != 0)
                    throw new InvalidOperationException($"sqlite3_key failed: rc={rc}, message={ErrorMessage(db)}");

                return db;
            }
            catch
            {
                _close(db);
                throw;
            }
        }

        private object? ExecuteScalarWithPassword(string path, string password, string sql)
        {
            var db = OpenWithPassword(path, password);
            try
            {
                object? result = null;
                var callback = new Sqlite3ExecCallback((_, columns, values, _) =>
                {
                    if (columns > 0 && result is null)
                    {
                        var value = Marshal.ReadIntPtr(values);
                        result = value == IntPtr.Zero ? null : Marshal.PtrToStringUTF8(value);
                    }

                    return 0;
                });
                Execute(db, sql, callback);
                GC.KeepAlive(callback);
                return result;
            }
            finally
            {
                _close(db);
            }
        }

        private void Execute(IntPtr db, string sql, Sqlite3ExecCallback? callback = null)
        {
            callback ??= (_, _, _, _) => 0;

            using var sqlUtf8 = new NativeUtf8String(sql);
            var rc = _exec(db, sqlUtf8.Pointer, callback, IntPtr.Zero, out var error);
            GC.KeepAlive(callback);
            if (rc == 0)
                return;

            var message = error == IntPtr.Zero ? ErrorMessage(db) : Marshal.PtrToStringUTF8(error);
            if (error != IntPtr.Zero)
                _free(error);

            throw new InvalidOperationException($"sqlite3_exec failed: rc={rc}, message={message}");
        }

        private string ErrorMessage(IntPtr db)
        {
            return Marshal.PtrToStringUTF8(_errmsg(db)) ?? string.Empty;
        }

        private sealed class NativeUtf8String : IDisposable
        {
            public NativeUtf8String(string value)
            {
                Pointer = Marshal.StringToCoTaskMemUTF8(value);
            }

            public IntPtr Pointer { get; private set; }

            public void Dispose()
            {
                if (Pointer == IntPtr.Zero)
                    return;

                Marshal.FreeCoTaskMem(Pointer);
                Pointer = IntPtr.Zero;
            }
        }

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int Sqlite3Open(IntPtr filename, out IntPtr db);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int Sqlite3Close(IntPtr db);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int Sqlite3Key(IntPtr db, byte[] key, int keyLength);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int Sqlite3Exec(
            IntPtr db,
            IntPtr sql,
            Sqlite3ExecCallback callback,
            IntPtr arg,
            out IntPtr errorMessage);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int Sqlite3ExecCallback(IntPtr arg, int columns, IntPtr values, IntPtr names);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr Sqlite3ErrMsg(IntPtr db);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void Sqlite3Free(IntPtr value);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int WxSqlite3Config(IntPtr db, IntPtr paramName, int newValue);
    }
}
