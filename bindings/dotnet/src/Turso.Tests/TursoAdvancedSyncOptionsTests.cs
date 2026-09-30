using AwesomeAssertions;

namespace Turso.Tests;

public sealed class TursoAdvancedSyncOptionsTests
{
    [Test]
    public void ExplicitOptionsMapPartialSyncAndThresholds()
    {
        var options = new TursoSyncDatabaseOptions(
            "replica.db",
            new Uri("turso://example.test"))
        {
            AuthToken = "secret",
            ClientName = "consumer",
            LongPollTimeout = TimeSpan.FromMilliseconds(2500),
            PartialSync = new TursoPartialSyncOptions
            {
                PrefixLength = 8192,
                SegmentSize = 4096,
                Prefetch = true,
            },
            PushOperationsThreshold = 100,
            PullBytesThreshold = 65536,
            ForceLogicalMvccPull = true,
            ExperimentalFeatures = "views",
        };

        var configuration = TursoSyncDatabase.CreateNativeConfiguration(
            options,
            options.GetNormalizedRemoteUri());

        configuration.Path.Should().Be("replica.db");
        configuration.RemoteUrl.Should().Be("https://example.test/");
        configuration.ClientName.Should().Be("consumer");
        configuration.LongPollTimeoutMilliseconds.Should().Be(2500);
        configuration.BootstrapIfEmpty.Should().BeTrue();
        configuration.ReservedBytes.Should().Be(0);
        configuration.PartialBootstrapStrategyPrefix.Should().Be(8192);
        configuration.PartialBootstrapStrategyQuery.Should().BeNull();
        configuration.PartialBootstrapSegmentSize.Should().Be((nuint)4096);
        configuration.PartialBootstrapPrefetch.Should().BeTrue();
        configuration.RemoteEncryptionKey.Should().BeNull();
        configuration.RemoteEncryptionCipher.Should().BeNull();
        configuration.PushOperationsThreshold.Should().Be((nuint)100);
        configuration.PullBytesThreshold.Should().Be((nuint)65536);
        configuration.LogicalMvccPull.Should().BeTrue();
        configuration.ExperimentalFeatures.Should().Be("views");
    }

    [Test]
    public void QueryPartialSyncMapsNativeStrategy()
    {
        var options = new TursoSyncDatabaseOptions(
            "replica.db",
            new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions
            {
                Query = "SELECT * FROM users",
                SegmentSize = 4096,
                Prefetch = true,
            },
        };

        var configuration = TursoSyncDatabase.CreateNativeConfiguration(
            options,
            options.GetNormalizedRemoteUri());

        configuration.PartialBootstrapStrategyPrefix.Should().Be(0);
        configuration.PartialBootstrapStrategyQuery.Should().Be("SELECT * FROM users");
        configuration.PartialBootstrapSegmentSize.Should().Be((nuint)4096);
        configuration.PartialBootstrapPrefetch.Should().BeTrue();
    }

    [TestCase(TursoRemoteEncryptionCipher.Aes256Gcm, "aes256gcm", 28)]
    [TestCase(TursoRemoteEncryptionCipher.Aes128Gcm, "aes128gcm", 28)]
    [TestCase(TursoRemoteEncryptionCipher.ChaCha20Poly1305, "chacha20poly1305", 28)]
    [TestCase(TursoRemoteEncryptionCipher.Aegis128L, "aegis128l", 32)]
    [TestCase(TursoRemoteEncryptionCipher.Aegis128X2, "aegis128x2", 32)]
    [TestCase(TursoRemoteEncryptionCipher.Aegis128X4, "aegis128x4", 32)]
    [TestCase(TursoRemoteEncryptionCipher.Aegis256, "aegis256", 48)]
    [TestCase(TursoRemoteEncryptionCipher.Aegis256X2, "aegis256x2", 48)]
    [TestCase(TursoRemoteEncryptionCipher.Aegis256X4, "aegis256x4", 48)]
    public void RemoteEncryptionMapsCipherAndReservedBytes(
        TursoRemoteEncryptionCipher cipher,
        string nativeName,
        int reservedBytes)
    {
        var options = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            RemoteEncryption = new TursoRemoteEncryptionOptions
            {
                Key = "base64-key",
                Cipher = cipher,
            },
        };

        options.Validate();
        var configuration = TursoSyncDatabase.CreateNativeConfiguration(
            options,
            options.GetNormalizedRemoteUri());

        configuration.RemoteEncryptionKey.Should().Be("base64-key");
        configuration.RemoteEncryptionCipher.Should().Be(nativeName);
        configuration.ReservedBytes.Should().Be(reservedBytes);
    }

    [Test]
    public void PartialSyncRequiresExactlyOneBootstrapStrategy()
    {
        var missing = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions(),
        };
        var contradictory = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions
            {
                PrefixLength = 4096,
                Query = "SELECT * FROM users",
            },
        };

        missing.Invoking(options => options.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*exactly one*");
        contradictory.Invoking(options => options.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*exactly one*");
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void SyncThresholdsMustBePositive(long value)
    {
        var push = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PushOperationsThreshold = value,
        };
        var pull = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PullBytesThreshold = value,
        };

        push.Invoking(options => options.Validate())
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(TursoSyncDatabaseOptions.PushOperationsThreshold));
        pull.Invoking(options => options.Validate())
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(TursoSyncDatabaseOptions.PullBytesThreshold));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void PartialSyncSizesMustBePositive(int value)
    {
        var prefix = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions { PrefixLength = value },
        };
        var segment = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions
            {
                PrefixLength = 4096,
                SegmentSize = value,
            },
        };

        prefix.Invoking(options => options.Validate())
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(TursoPartialSyncOptions.PrefixLength));
        segment.Invoking(options => options.Validate())
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(TursoPartialSyncOptions.SegmentSize));
    }

    [Test]
    public void PartialSyncRequiresBootstrapIfEmpty()
    {
        var options = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            BootstrapIfEmpty = false,
            PartialSync = new TursoPartialSyncOptions { PrefixLength = 4096 },
        };

        options.Invoking(value => value.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*BootstrapIfEmpty=True*");
    }

    [Test]
    public void PartialSyncCannotUseRemoteEncryption()
    {
        var options = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions { PrefixLength = 4096 },
            RemoteEncryption = new TursoRemoteEncryptionOptions
            {
                Key = "base64-key",
                Cipher = TursoRemoteEncryptionCipher.Aes256Gcm,
            },
        };

        options.Invoking(value => value.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*cannot be combined*");
    }

    [Test]
    public void QueryPartialSyncCannotUsePullByteThreshold()
    {
        var options = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions { Query = "SELECT * FROM users" },
            PullBytesThreshold = 65536,
        };

        options.Invoking(value => value.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*PullBytesThreshold*");
    }

    [Test]
    public void RemoteEncryptionRequiresAKeyAndKnownCipher()
    {
        var missingKey = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            RemoteEncryption = new TursoRemoteEncryptionOptions
            {
                Key = " ",
                Cipher = TursoRemoteEncryptionCipher.Aes256Gcm,
            },
        };
        var unknownCipher = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            RemoteEncryption = new TursoRemoteEncryptionOptions
            {
                Key = "base64-key",
                Cipher = (TursoRemoteEncryptionCipher)int.MaxValue,
            },
        };

        missingKey.Invoking(options => options.Validate())
            .Should().Throw<ArgumentException>()
            .WithParameterName(nameof(TursoRemoteEncryptionOptions.Key));
        unknownCipher.Invoking(options => options.Validate())
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(TursoRemoteEncryptionOptions.Cipher));
    }

    [Test]
    public void PartialSyncOnWindowsFailsBeforeNativeExecution()
    {
        if (!OperatingSystem.IsWindows())
            Assert.Ignore("Windows-specific partial sync guard.");

        var options = new TursoSyncDatabaseOptions("replica.db", new Uri("https://example.test"))
        {
            PartialSync = new TursoPartialSyncOptions { PrefixLength = 4096 },
        };

        options.Invoking(value => value.Validate())
            .Should().Throw<PlatformNotSupportedException>()
            .WithMessage("*sparse-file hole detection*");
    }
}
