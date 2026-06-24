using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using Turso;
using Turso.Raw.Public;

namespace Turso.Tests.Support;

internal sealed record ExternalCodecCandidate(
    string Name,
    Func<string, ITursoPageCodec> CreateCodec,
    byte ReservedSpace = 0);

internal sealed class DetectedEncryptedDatabase(
    string codecName,
    TursoConnection connection,
    ITursoPageCodec codec) : IDisposable
{
    public string CodecName { get; } = codecName;

    public TursoConnection Connection { get; } = connection;

    public void Dispose()
    {
        Connection.Dispose();
        if (codec is IDisposable disposable)
            disposable.Dispose();
    }
}

internal static class EncryptedDatabaseDetector
{
    public static IReadOnlyList<ExternalCodecCandidate> DefaultCandidates()
    {
        var candidates = new List<ExternalCodecCandidate>
        {
            new(
                "wxSQLite3/SQLite3MC aes128cbc",
                password => WxSQLite3Aes128CbcCodec.FromPassword(password)),
        };

        if (OperatingSystem.IsWindows())
        {
            candidates.Add(new(
                "System.Data.SQLite legacy CryptoAPI RC4 (encrypted page 1)",
                password => SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(password, encryptPage1: true)));
            candidates.Add(new(
                "System.Data.SQLite legacy CryptoAPI RC4 (plaintext page 1 header)",
                password => SystemDataSQLiteLegacyCryptoApiCodec.FromPassword(password, encryptPage1: false)));
        }

        return candidates;
    }

    public static DetectedEncryptedDatabase Open(
        string path,
        string password,
        IEnumerable<ExternalCodecCandidate>? candidates = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(password);
        if (!File.Exists(path))
            throw new FileNotFoundException("Database file does not exist.", path);

        var failures = new List<Exception>();
        foreach (var candidate in candidates ?? DefaultCandidates())
        {
            ITursoPageCodec? codec = null;
            TursoConnection? connection = null;
            try
            {
                codec = candidate.CreateCodec(password);
                connection = new TursoConnection(ConnectionStringFor(path))
                {
                    PageCodec = codec,
                    PageCodecReservedSpace = candidate.ReservedSpace,
                };
                connection.Open();
                ValidateConnection(connection);
                return new DetectedEncryptedDatabase(candidate.Name, connection, codec);
            }
            catch (Exception ex) when (IsExpectedCandidateFailure(ex))
            {
                connection?.Dispose();
                if (codec is IDisposable disposable)
                    disposable.Dispose();

                failures.Add(new InvalidOperationException($"{candidate.Name}: {ex.Message}", ex));
            }
        }

        throw new AggregateException(
            $"None of the configured external SQLite encryption codecs could open '{path}' with the supplied password.",
            failures);
    }

    private static string ConnectionStringFor(string path)
    {
        return new TursoConnectionStringBuilder
        {
            DataSource = path,
            Mode = "ReadOnly",
        }.ConnectionString;
    }

    private static void ValidateConnection(TursoConnection connection)
    {
        using var command = new TursoCommand(connection, "SELECT count(*) FROM sqlite_schema");
        _ = Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private static bool IsExpectedCandidateFailure(Exception exception)
    {
        return exception is TursoException
            or CryptographicException
            or PlatformNotSupportedException
            or Win32Exception
            or ArgumentException;
    }
}
