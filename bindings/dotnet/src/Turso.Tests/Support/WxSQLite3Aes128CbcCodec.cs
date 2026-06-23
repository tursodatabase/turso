using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Turso.Raw.Public;

namespace Turso.Tests.Support;

internal sealed class WxSQLite3Aes128CbcCodec : ITursoPageCodec, IDisposable
{
    private static ReadOnlySpan<byte> SqliteHeader => "SQLite format 3\0"u8;
    private static ReadOnlySpan<byte> PageKeySalt => "sAlT"u8;
    private static ReadOnlySpan<byte> PasswordPadding =>
    [
        0x28, 0xBF, 0x4E, 0x5E, 0x4E, 0x75, 0x8A, 0x41,
        0x64, 0x00, 0x4E, 0x56, 0xFF, 0xFA, 0x01, 0x08,
        0x2E, 0x2E, 0x00, 0xB6, 0xD0, 0x68, 0x3E, 0x80,
        0x2F, 0x0C, 0xA9, 0xFE, 0x64, 0x53, 0x69, 0x7A
    ];

    private readonly byte[] _masterKey;
    private bool _disposed;

    private WxSQLite3Aes128CbcCodec(ReadOnlySpan<byte> password)
    {
        if (password.IsEmpty)
            throw new ArgumentException("Password must not be empty.", nameof(password));

        _masterKey = DeriveMasterKey(password);
    }

    public static WxSQLite3Aes128CbcCodec FromPassword(string password)
    {
        ArgumentNullException.ThrowIfNull(password);

        var passwordBytes = Encoding.UTF8.GetBytes(password);
        try
        {
            return new WxSQLite3Aes128CbcCodec(passwordBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(passwordBytes);
        }
    }

    public static WxSQLite3Aes128CbcCodec FromRawPassword(ReadOnlySpan<byte> password)
    {
        return new WxSQLite3Aes128CbcCodec(password);
    }

    public TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix)
    {
        if (TryReadSqliteHeader(rawPage1Prefix, out var plainHeader))
            return plainHeader;

        if (TryReadModernAesHeader(rawPage1Prefix, out var encryptedHeader))
            return encryptedHeader;

        return null;
    }

    public void DecodePage(
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output)
    {
        _ = location;
        TransformPage(pageNo, input, output, encrypt: false);
    }

    public void EncodePage(
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output)
    {
        _ = location;
        TransformPage(pageNo, input, output, encrypt: true);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        CryptographicOperations.ZeroMemory(_masterKey);
        _disposed = true;
    }

    private static bool TryReadSqliteHeader(
        ReadOnlySpan<byte> pagePrefix,
        out TursoPageCodecHeaderInfo headerInfo)
    {
        if (pagePrefix.Length < 100 || !pagePrefix[..SqliteHeader.Length].SequenceEqual(SqliteHeader))
        {
            headerInfo = default;
            return false;
        }

        return TryReadHeaderFields(pagePrefix, out headerInfo);
    }

    private static bool TryReadModernAesHeader(
        ReadOnlySpan<byte> pagePrefix,
        out TursoPageCodecHeaderInfo headerInfo)
    {
        if (pagePrefix.Length < 24 || pagePrefix[..SqliteHeader.Length].SequenceEqual(SqliteHeader))
        {
            headerInfo = default;
            return false;
        }

        return TryReadHeaderFields(pagePrefix, out headerInfo);
    }

    private static bool TryReadHeaderFields(
        ReadOnlySpan<byte> pagePrefix,
        out TursoPageCodecHeaderInfo headerInfo)
    {
        var pageSizeRaw = (ushort)((pagePrefix[16] << 8) | pagePrefix[17]);
        var pageSize = pageSizeRaw == 1 ? 65536u : pageSizeRaw;
        if (!IsValidPageSize(pageSize)
            || !IsValidFileFormatVersion(pagePrefix[18])
            || !IsValidFileFormatVersion(pagePrefix[19])
            || pagePrefix[21] != 64
            || pagePrefix[22] != 32
            || pagePrefix[23] != 32)
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

    private void TransformPage(uint pageNo, ReadOnlySpan<byte> input, Span<byte> output, bool encrypt)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (pageNo == 0)
            throw new ArgumentOutOfRangeException(nameof(pageNo), pageNo, "SQLite page numbers are one-based.");

        if (input.Length != output.Length)
            throw new ArgumentException("Codec input and output page lengths must match.");

        if (input.Length < 512 || input.Length % 16 != 0)
            throw new ArgumentException("wxSQLite3 AES-128-CBC pages must be at least 512 bytes and block aligned.");

        var buffer = input.ToArray();
        if (encrypt)
            EncryptPage(checked((int)pageNo), buffer);
        else
            DecryptPage(checked((int)pageNo), buffer);

        buffer.CopyTo(output);
    }

    private void EncryptPage(int pageNo, Span<byte> data)
    {
        if (pageNo != 1)
        {
            TransformAes(pageNo, data, data, encrypt: true);
            return;
        }

        Span<byte> dbHeader = stackalloc byte[8];
        data[16..24].CopyTo(dbHeader);

        TransformAes(pageNo, data[..16], data[..16], encrypt: true);
        TransformAes(pageNo, data[16..], data[16..], encrypt: true);

        data[16..24].CopyTo(data[8..16]);
        dbHeader.CopyTo(data[16..24]);
    }

    private void DecryptPage(int pageNo, Span<byte> data)
    {
        if (pageNo != 1)
        {
            TransformAes(pageNo, data, data, encrypt: false);
            return;
        }

        Span<byte> dbHeader = stackalloc byte[8];
        data[16..24].CopyTo(dbHeader);

        var offset = 0;
        if (IsModernPageOneHeader(dbHeader))
        {
            data[8..16].CopyTo(data[16..24]);
            offset = 16;
        }

        TransformAes(pageNo, data[offset..], data[offset..], encrypt: false);
        if (offset != 0 && data[16..24].SequenceEqual(dbHeader))
            SqliteHeader.CopyTo(data);
    }

    private static bool IsModernPageOneHeader(ReadOnlySpan<byte> dbHeader)
    {
        var dbPageSize = (dbHeader[0] << 8) | (dbHeader[1] << 16);
        return IsValidPageSize((uint)dbPageSize)
            && dbHeader[5] == 0x40
            && dbHeader[6] == 0x20
            && dbHeader[7] == 0x20;
    }

    private void TransformAes(int pageNo, ReadOnlySpan<byte> input, Span<byte> output, bool encrypt)
    {
        if (input.Length % 16 != 0)
            throw new ArgumentException("AES-CBC input length must be block aligned.");

        Span<byte> pageKey = stackalloc byte[16];
        DerivePageKey(pageNo, pageKey);

        Span<byte> iv = stackalloc byte[16];
        GenerateInitialVector(pageNo, iv);

        using var aes = Aes.Create();
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.None;
        aes.Key = pageKey.ToArray();
        aes.IV = iv.ToArray();

        using var transform = encrypt ? aes.CreateEncryptor() : aes.CreateDecryptor();
        var transformed = transform.TransformFinalBlock(input.ToArray(), 0, input.Length);
        transformed.CopyTo(output);
    }

    private void DerivePageKey(int pageNo, Span<byte> pageKey)
    {
        Span<byte> keyMaterial = stackalloc byte[24];
        _masterKey.CopyTo(keyMaterial);
        BinaryPrimitives.WriteInt32LittleEndian(keyMaterial[16..20], pageNo);
        PageKeySalt.CopyTo(keyMaterial[20..24]);
        MD5.HashData(keyMaterial, pageKey);
    }

    private static void GenerateInitialVector(int pageNo, Span<byte> iv)
    {
        Span<byte> initKey = stackalloc byte[16];
        var z = (long)pageNo + 1;
        for (var j = 0; j < 4; j++)
        {
            var q = z / 52774;
            z = 40692 * (z - 52774 * q) - 3791 * q;
            if (z < 0)
                z += 2147483399L;

            BinaryPrimitives.WriteInt32LittleEndian(initKey[(4 * j)..(4 * j + 4)], (int)z);
        }

        MD5.HashData(initKey, iv);
    }

    private static byte[] DeriveMasterKey(ReadOnlySpan<byte> password)
    {
        var userPad = PadPassword(password);
        var ownerPad = PadPassword([]);

        var digest = MD5.HashData(ownerPad);
        for (var k = 0; k < 50; k++)
            digest = MD5.HashData(digest);

        var ownerKey = userPad.ToArray();
        Span<byte> rc4Key = stackalloc byte[16];
        for (var i = 0; i < 20; i++)
        {
            for (var j = 0; j < rc4Key.Length; j++)
                rc4Key[j] = (byte)(digest[j] ^ i);

            Rc4(rc4Key, ownerKey, ownerKey);
        }

        var encryptionKeyMaterial = new byte[64];
        userPad.CopyTo(encryptionKeyMaterial);
        ownerKey.CopyTo(encryptionKeyMaterial.AsSpan(32));

        digest = MD5.HashData(encryptionKeyMaterial);
        for (var k = 0; k < 50; k++)
            digest = MD5.HashData(digest);

        CryptographicOperations.ZeroMemory(userPad);
        CryptographicOperations.ZeroMemory(ownerPad);
        CryptographicOperations.ZeroMemory(ownerKey);
        CryptographicOperations.ZeroMemory(encryptionKeyMaterial);
        return digest;
    }

    private static byte[] PadPassword(ReadOnlySpan<byte> password)
    {
        var padded = new byte[32];
        var passwordLength = Math.Min(password.Length, padded.Length);
        password[..passwordLength].CopyTo(padded);
        PasswordPadding[..(padded.Length - passwordLength)].CopyTo(padded.AsSpan(passwordLength));
        return padded;
    }

    private static void Rc4(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, Span<byte> output)
    {
        Span<byte> state = stackalloc byte[256];
        for (var i = 0; i < state.Length; i++)
            state[i] = (byte)i;

        var j = 0;
        for (var i = 0; i < state.Length; i++)
        {
            var value = state[i];
            j = (j + value + key[i % key.Length]) & 0xFF;
            state[i] = state[j];
            state[j] = value;
        }

        var a = 0;
        var b = 0;
        for (var i = 0; i < input.Length; i++)
        {
            a = (a + 1) & 0xFF;
            var value = state[a];
            b = (b + value) & 0xFF;
            state[a] = state[b];
            state[b] = value;
            var k = state[(state[a] + state[b]) & 0xFF];
            output[i] = (byte)(input[i] ^ k);
        }
    }
}
