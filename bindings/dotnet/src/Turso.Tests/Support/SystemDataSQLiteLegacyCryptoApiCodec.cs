using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Turso.Raw.Public;

namespace Turso.Tests.Support;

internal sealed class SystemDataSQLiteLegacyCryptoApiCodec : ITursoPageCodec, IDisposable
{
    private const uint ProvRsaFull = 1;
    private const uint CryptVerifyContext = 0xF0000000;
    private const uint CalgSha1 = 0x00008004;
    private const uint CalgRc4 = 0x00006801;
    private const string EnhancedProvider = "Microsoft Enhanced Cryptographic Provider v1.0";
    private const string LegacyEncryptPage1Variable = "SQLite_LegacyEncryptPage1";
    private static ReadOnlySpan<byte> SqliteHeader => "SQLite format 3\0"u8;

    private readonly byte[] _password;
    private readonly bool _encryptPage1;
    private readonly object _lock = new();
    private IntPtr _provider;
    private bool _disposed;

    private SystemDataSQLiteLegacyCryptoApiCodec(ReadOnlySpan<byte> password, bool? encryptPage1)
    {
        if (!OperatingSystem.IsWindows())
            throw new PlatformNotSupportedException("The legacy System.Data.SQLite codec uses Windows CryptoAPI.");

        if (password.IsEmpty)
            throw new ArgumentException("Password must not be empty.", nameof(password));

        _password = password.ToArray();
        _encryptPage1 = encryptPage1 ?? Environment.GetEnvironmentVariable(LegacyEncryptPage1Variable) is not null;

        if (!CryptAcquireContext(out _provider, null, EnhancedProvider, ProvRsaFull, CryptVerifyContext))
            ThrowLastWin32Error("CryptAcquireContext");
    }

    public static SystemDataSQLiteLegacyCryptoApiCodec FromPassword(string password, bool? encryptPage1 = null)
    {
        ArgumentNullException.ThrowIfNull(password);
        return new SystemDataSQLiteLegacyCryptoApiCodec(Encoding.UTF8.GetBytes(password), encryptPage1);
    }

    public static SystemDataSQLiteLegacyCryptoApiCodec FromRawPassword(
        ReadOnlySpan<byte> password,
        bool? encryptPage1 = null)
    {
        return new SystemDataSQLiteLegacyCryptoApiCodec(password, encryptPage1);
    }

    public TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix)
    {
        if (TryReadHeader(rawPage1Prefix, out var plainHeader))
            return plainHeader;

        var decrypted = new byte[rawPage1Prefix.Length];
        Transform(rawPage1Prefix, decrypted, decrypt: true);
        return TryReadHeader(decrypted, out var decryptedHeader) ? decryptedHeader : null;
    }

    public void DecodePage(
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output)
    {
        _ = location;
        TransformPage(pageNo, input, output, decrypt: true);
    }

    public void EncodePage(
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output)
    {
        _ = location;
        TransformPage(pageNo, input, output, decrypt: false);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        CryptographicOperations.ZeroMemory(_password);

        if (_provider != IntPtr.Zero)
        {
            CryptReleaseContext(_provider, 0);
            _provider = IntPtr.Zero;
        }

        _disposed = true;
    }

    private static bool TryReadHeader(
        ReadOnlySpan<byte> pagePrefix,
        out TursoPageCodecHeaderInfo headerInfo)
    {
        if (pagePrefix.Length < 100 || !pagePrefix[..SqliteHeader.Length].SequenceEqual(SqliteHeader))
        {
            headerInfo = default;
            return false;
        }

        var pageSizeRaw = (ushort)((pagePrefix[16] << 8) | pagePrefix[17]);
        var pageSize = pageSizeRaw == 1 ? 65536u : pageSizeRaw;
        if (!IsValidPageSize(pageSize) || !IsValidFileFormatVersion(pagePrefix[18]) || !IsValidFileFormatVersion(pagePrefix[19]))
        {
            headerInfo = default;
            return false;
        }

        headerInfo = new TursoPageCodecHeaderInfo(pageSize, pagePrefix[20]);
        return true;
    }

    private static bool IsValidPageSize(uint pageSize)
    {
        return pageSize is >= 512 and <= 65536 && (pageSize & (pageSize - 1)) == 0;
    }

    private static bool IsValidFileFormatVersion(byte version)
    {
        return version is 1 or 2;
    }

    private void TransformPage(uint pageNo, ReadOnlySpan<byte> input, Span<byte> output, bool decrypt)
    {
        if (pageNo == 0)
            throw new ArgumentOutOfRangeException(nameof(pageNo), pageNo, "SQLite page numbers are one-based.");

        if (input.Length != output.Length)
            throw new ArgumentException("Codec input and output page lengths must match.");

        if (!_encryptPage1 && pageNo == 1 && input.Length >= SqliteHeader.Length && input[..SqliteHeader.Length].SequenceEqual(SqliteHeader))
        {
            input.CopyTo(output);
            return;
        }

        Transform(input, output, decrypt);
    }

    private void Transform(ReadOnlySpan<byte> input, Span<byte> output, bool decrypt)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var buffer = input.ToArray();
        lock (_lock)
        {
            var key = DeriveKey();
            try
            {
                var dataLength = checked((uint)buffer.Length);
                var bufferLength = dataLength;
                var success = decrypt
                    ? CryptDecrypt(key, IntPtr.Zero, true, 0, buffer, ref dataLength)
                    : CryptEncrypt(key, IntPtr.Zero, true, 0, buffer, ref dataLength, bufferLength);

                if (!success)
                    ThrowLastWin32Error(decrypt ? "CryptDecrypt" : "CryptEncrypt");

                if (dataLength != bufferLength)
                    throw new InvalidOperationException("Legacy CryptoAPI page transform changed the page size.");
            }
            finally
            {
                CryptDestroyKey(key);
            }
        }

        buffer.CopyTo(output);
    }

    private IntPtr DeriveKey()
    {
        if (!CryptCreateHash(_provider, CalgSha1, IntPtr.Zero, 0, out var hash))
            ThrowLastWin32Error("CryptCreateHash");

        try
        {
            if (!CryptHashData(hash, _password, checked((uint)_password.Length), 0))
                ThrowLastWin32Error("CryptHashData");

            if (!CryptDeriveKey(_provider, CalgRc4, hash, 0, out var key))
                ThrowLastWin32Error("CryptDeriveKey");

            return key;
        }
        finally
        {
            CryptDestroyHash(hash);
        }
    }

    private static void ThrowLastWin32Error(string function)
    {
        throw new Win32Exception(Marshal.GetLastWin32Error(), function);
    }

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool CryptAcquireContext(
        out IntPtr provider,
        string? keyContainer,
        string? providerName,
        uint providerType,
        uint flags);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptCreateHash(
        IntPtr provider,
        uint algorithmId,
        IntPtr key,
        uint flags,
        out IntPtr hash);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptHashData(
        IntPtr hash,
        byte[] data,
        uint dataLength,
        uint flags);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptDeriveKey(
        IntPtr provider,
        uint algorithmId,
        IntPtr baseData,
        uint flags,
        out IntPtr key);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptEncrypt(
        IntPtr key,
        IntPtr hash,
        bool final,
        uint flags,
        byte[] data,
        ref uint dataLength,
        uint bufferLength);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptDecrypt(
        IntPtr key,
        IntPtr hash,
        bool final,
        uint flags,
        byte[] data,
        ref uint dataLength);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptDestroyHash(IntPtr hash);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptDestroyKey(IntPtr key);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool CryptReleaseContext(IntPtr provider, uint flags);
}
