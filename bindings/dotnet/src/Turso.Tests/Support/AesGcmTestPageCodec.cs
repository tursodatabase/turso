using System.Buffers.Binary;
using System.Security.Cryptography;
using Turso.Raw.Public;

namespace Turso.Tests.Support;

/// <summary>
/// AES-256-GCM page codec used by the tests.
/// </summary>
/// <remarks>
/// The last 28 bytes of every page (its reserved space) hold a random 12-byte nonce and the 16-byte
/// tag, and the page number is authenticated so pages cannot be swapped. Page 1 replaces the
/// "SQLite format 3" magic with <see cref="Magic"/> and keeps header bytes 16 to 23 (page size, file
/// format bytes, reserved space) in clear but authenticated, so <see cref="ProbeHeader"/> can read the
/// layout. Everything else is encrypted.
/// </remarks>
internal sealed class AesGcmTestPageCodec : ITursoPageCodec, IDisposable
{
    public const int KeySize = 32;
    private const int NonceSize = 12;
    private const int TagSize = 16;
    private const int MagicSize = 16;
    private const int ClearHeaderEnd = 24;

    public static ReadOnlySpan<byte> Magic => "TURSO-TEST-AESGC"u8;
    private static ReadOnlySpan<byte> SqliteMagic => "SQLite format 3\0"u8;

    private readonly byte[] _key;
    private readonly ThreadLocal<AesGcm> _aes;

    public AesGcmTestPageCodec(ReadOnlySpan<byte> key)
    {
        if (key.Length != KeySize)
            throw new ArgumentException($"Key must be {KeySize} bytes.", nameof(key));

        _key = key.ToArray();
        // AesGcm is not documented as thread-safe and the engine may call the codec from several
        // threads, so keep one instance per thread.
        _aes = new ThreadLocal<AesGcm>(() => new AesGcm(_key, TagSize), trackAllValues: true);

        // The ID must change with the key; an HMAC identifies the key without revealing it.
        Span<byte> mac = stackalloc byte[32];
        HMACSHA256.HashData(key, "turso.tests.page-codec.aes256gcm.v1"u8, mac);
        CodecId = TursoPageCodecId.FromBytes(mac[..16]);
    }

    public static byte[] NewKey() => RandomNumberGenerator.GetBytes(KeySize);

    public TursoPageCodecId CodecId { get; }

    public byte ReservedSpace => NonceSize + TagSize;

    public TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix)
    {
        if (rawPage1Prefix.Length < ClearHeaderEnd || !rawPage1Prefix[..MagicSize].SequenceEqual(Magic))
            return null;

        var pageSize = BinaryPrimitives.ReadUInt16BigEndian(rawPage1Prefix[16..18]);
        return new TursoPageCodecHeaderInfo(pageSize == 1 ? 65536u : pageSize, rawPage1Prefix[20]);
    }

    public void EncodePage(uint pageNo, TursoPageCodecLocation location, ReadOnlySpan<byte> input, Span<byte> output)
    {
        CheckPage(input, output);
        var body = input.Length - ReservedSpace;
        var nonce = output.Slice(body, NonceSize);
        var tag = output.Slice(body + NonceSize, TagSize);
        RandomNumberGenerator.Fill(nonce);

        Span<byte> aad = stackalloc byte[4 + ClearHeaderEnd - MagicSize];
        BinaryPrimitives.WriteUInt32LittleEndian(aad, pageNo);
        if (pageNo != 1)
        {
            _aes.Value!.Encrypt(nonce, input[..body], output[..body], tag, aad[..4]);
            return;
        }

        if (!input[..MagicSize].SequenceEqual(SqliteMagic))
            throw new CryptographicException("Page 1 does not start with the SQLite header.");

        Magic.CopyTo(output);
        input[MagicSize..ClearHeaderEnd].CopyTo(output[MagicSize..ClearHeaderEnd]);
        input[MagicSize..ClearHeaderEnd].CopyTo(aad[4..]);
        _aes.Value!.Encrypt(nonce, input[ClearHeaderEnd..body], output[ClearHeaderEnd..body], tag, aad);
    }

    public void DecodePage(uint pageNo, TursoPageCodecLocation location, ReadOnlySpan<byte> input, Span<byte> output)
    {
        CheckPage(input, output);
        var body = input.Length - ReservedSpace;
        var nonce = input.Slice(body, NonceSize);
        var tag = input.Slice(body + NonceSize, TagSize);

        Span<byte> aad = stackalloc byte[4 + ClearHeaderEnd - MagicSize];
        BinaryPrimitives.WriteUInt32LittleEndian(aad, pageNo);
        if (pageNo != 1)
        {
            _aes.Value!.Decrypt(nonce, input[..body], tag, output[..body], aad[..4]);
        }
        else
        {
            if (!input[..MagicSize].SequenceEqual(Magic))
                throw new CryptographicException("Page 1 does not carry the codec magic.");

            input[MagicSize..ClearHeaderEnd].CopyTo(aad[4..]);
            _aes.Value!.Decrypt(nonce, input[ClearHeaderEnd..body], tag, output[ClearHeaderEnd..body], aad);
            SqliteMagic.CopyTo(output);
            input[MagicSize..ClearHeaderEnd].CopyTo(output[MagicSize..ClearHeaderEnd]);
        }

        // The reserved tail is opaque to the b-tree; hand it back zeroed.
        output[body..].Clear();
    }

    public void Dispose()
    {
        foreach (var aes in _aes.Values)
            aes.Dispose();
        _aes.Dispose();
        CryptographicOperations.ZeroMemory(_key);
    }

    private void CheckPage(ReadOnlySpan<byte> input, Span<byte> output)
    {
        if (input.Length != output.Length || input.Length < 512)
            throw new ArgumentException("Invalid page buffer length.");
    }
}
