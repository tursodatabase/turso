using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace Turso.Raw.Public;

/// <summary>
/// Where a page passed to an <see cref="ITursoPageCodec"/> is stored.
/// </summary>
public enum TursoPageCodecLocation : uint
{
    /// <summary>The main database file.</summary>
    Database = 0,
    /// <summary>A frame body in the write-ahead log.</summary>
    Wal = 1,
}

/// <summary>
/// The page layout of an existing database, as read by <see cref="ITursoPageCodec.ProbeHeader"/>.
/// </summary>
public readonly record struct TursoPageCodecHeaderInfo(uint PageSize, byte ReservedSpace);

/// <summary>
/// A stable, non-secret, non-zero 16-byte identifier for a page codec configuration.
/// </summary>
/// <remarks>
/// The engine shares one database per path in a process and refuses a connection whose codec
/// ID differs from the one the database was opened with. The ID must therefore change whenever
/// encoded bytes could differ, for example when the key changes, or a wrong key would join a
/// database already opened with the right one.
/// </remarks>
public readonly record struct TursoPageCodecId
{
    private readonly Guid _value;

    private TursoPageCodecId(Guid value)
    {
        if (value == Guid.Empty)
            throw new ArgumentException("Page codec ID must be non-zero.", nameof(value));

        _value = value;
    }

    public static TursoPageCodecId FromGuid(Guid value) => new(value);

    public static TursoPageCodecId FromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16)
            throw new ArgumentException("Page codec ID must be exactly 16 bytes.", nameof(bytes));

        return new TursoPageCodecId(new Guid(bytes));
    }

    public override string ToString() => _value.ToString("N");

    internal void CopyTo(Span<byte> destination)
    {
        if (_value == Guid.Empty)
            throw new InvalidOperationException("Page codec ID must be non-zero.");
        if (!_value.TryWriteBytes(destination))
            throw new ArgumentException("Destination must be at least 16 bytes.", nameof(destination));
    }
}

/// <summary>
/// Transforms database and WAL pages on their way to and from storage, for example to encrypt them.
/// </summary>
/// <remarks>
/// <para>
/// Pages keep their size: <c>input</c> and <c>output</c> are separate buffers of exactly one page,
/// reserved tail included. Per-page metadata such as a nonce or an authentication tag belongs in
/// the last <see cref="ReservedSpace"/> bytes of each page, which SQLite's b-tree never uses.
/// </para>
/// <para>
/// The engine may call the codec concurrently from any thread that steps a statement, so
/// implementations must be thread-safe, must not call back into Turso, and must stay usable for
/// as long as a database opened with them is alive. Throwing from a method fails the page
/// operation; the exception does not reach the caller of the query, which sees a generic page
/// codec error instead.
/// </para>
/// </remarks>
public interface ITursoPageCodec
{
    /// <summary>Identifies this codec configuration. See <see cref="TursoPageCodecId"/>.</summary>
    TursoPageCodecId CodecId { get; }

    /// <summary>
    /// The number of bytes the codec reserves at the end of every page. A new database records it
    /// in its header; opening an existing database fails unless the header holds the same value.
    /// </summary>
    byte ReservedSpace { get; }

    /// <summary>
    /// Reads the page size and reserved space from the raw, still encoded first bytes of an existing
    /// database, or returns <see langword="null"/> when the file was not written by this codec.
    /// </summary>
    TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix);

    void DecodePage(uint pageNo, TursoPageCodecLocation location, ReadOnlySpan<byte> input, Span<byte> output);

    void EncodePage(uint pageNo, TursoPageCodecLocation location, ReadOnlySpan<byte> input, Span<byte> output);
}

/// <summary>
/// Builds the native <c>turso_page_codec_v1_t</c> table for an <see cref="ITursoPageCodec"/>.
/// </summary>
/// <remarks>
/// The callbacks are static <see cref="UnmanagedCallersOnlyAttribute"/> methods taken as unmanaged
/// function pointers, so no delegate is marshalled and no reverse P/Invoke thunk is generated at run
/// time, which full AOT (iOS) cannot do.
/// </remarks>
internal static unsafe class TursoNativePageCodec
{
    private const uint AbiVersion = 1;

    // The engine copies the error message as soon as the callback returns, so process-lifetime
    // strings are enough and nothing is allocated per failure.
    private static readonly byte* RejectedPageError = AllocUtf8("page rejected by the codec (wrong key or corrupted page)");
    private static readonly byte* InvalidPageError = AllocUtf8("invalid page buffer");
    private static readonly byte* ProbeError = AllocUtf8("unexpected error while probing the database header");
    private static readonly byte* UnexpectedError = AllocUtf8("unexpected page codec error");

    [StructLayout(LayoutKind.Sequential)]
    internal struct HeaderInfo
    {
        public uint PageSize;
        public byte ReservedSpace;
        public byte IsSupported;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct CodecV1
    {
        public uint AbiVersion;
        public IntPtr Context;
        public byte ReservedSpace;
        public fixed byte CodecId[16];
        public delegate* unmanaged[Cdecl]<IntPtr, void> Destroy;
        public delegate* unmanaged[Cdecl]<IntPtr, byte*, nuint, HeaderInfo*, byte**, int> ProbeHeader;
        public delegate* unmanaged[Cdecl]<IntPtr, uint, TursoPageCodecLocation, byte*, nuint, byte*, nuint, byte**, int> DecodePage;
        public delegate* unmanaged[Cdecl]<IntPtr, uint, TursoPageCodecLocation, byte*, nuint, byte*, nuint, byte**, int> EncodePage;
    }

    /// <summary>
    /// Fills a codec table whose context is a new <see cref="GCHandle"/> to <paramref name="codec"/>.
    /// Once <c>turso_database_new</c> succeeds the engine owns the context and frees it through
    /// <c>destroy</c>; if it fails, the engine never calls <c>destroy</c> and the caller must call
    /// <see cref="Release"/>.
    /// </summary>
    public static CodecV1 Create(ITursoPageCodec codec)
    {
        ArgumentNullException.ThrowIfNull(codec);

        // Read the codec's properties before allocating, so a throwing getter leaks nothing.
        var codecId = codec.CodecId;
        var reservedSpace = codec.ReservedSpace;

        var table = new CodecV1
        {
            AbiVersion = AbiVersion,
            ReservedSpace = reservedSpace,
            Destroy = &Destroy,
            ProbeHeader = &Probe,
            DecodePage = &Decode,
            EncodePage = &Encode,
        };
        codecId.CopyTo(new Span<byte>(table.CodecId, 16));
        table.Context = GCHandle.ToIntPtr(GCHandle.Alloc(codec));
        return table;
    }

    public static void Release(IntPtr context)
    {
        if (context != IntPtr.Zero)
            GCHandle.FromIntPtr(context).Free();
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void Destroy(IntPtr context)
    {
        try
        {
            Release(context);
        }
        catch
        {
            // An exception must never cross an [UnmanagedCallersOnly] boundary.
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int Probe(IntPtr context, byte* rawPage1Prefix, nuint rawLen, HeaderInfo* headerInfo, byte** error)
    {
        try
        {
            var codec = GetCodec(context);
            var result = codec.ProbeHeader(new ReadOnlySpan<byte>(rawPage1Prefix, checked((int)rawLen)));
            *headerInfo = result is { } info
                ? new HeaderInfo { PageSize = info.PageSize, ReservedSpace = info.ReservedSpace, IsSupported = 1 }
                : default;
            return 0;
        }
        catch
        {
            *error = ProbeError;
            return 1;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int Decode(
        IntPtr context,
        uint pageNo,
        TursoPageCodecLocation location,
        byte* input,
        nuint inputLen,
        byte* output,
        nuint outputLen,
        byte** error)
        => Transform(context, pageNo, location, input, inputLen, output, outputLen, error, encode: false);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int Encode(
        IntPtr context,
        uint pageNo,
        TursoPageCodecLocation location,
        byte* input,
        nuint inputLen,
        byte* output,
        nuint outputLen,
        byte** error)
        => Transform(context, pageNo, location, input, inputLen, output, outputLen, error, encode: true);

    private static int Transform(
        IntPtr context,
        uint pageNo,
        TursoPageCodecLocation location,
        byte* input,
        nuint inputLen,
        byte* output,
        nuint outputLen,
        byte** error,
        bool encode)
    {
        // Every exception becomes a status code: one escaping an [UnmanagedCallersOnly] method
        // terminates the process.
        try
        {
            var codec = GetCodec(context);
            var source = new ReadOnlySpan<byte>(input, checked((int)inputLen));
            var destination = new Span<byte>(output, checked((int)outputLen));
            if (encode)
                codec.EncodePage(pageNo, location, source, destination);
            else
                codec.DecodePage(pageNo, location, source, destination);
            return 0;
        }
        catch (CryptographicException)
        {
            *error = RejectedPageError;
            return 1;
        }
        catch (ArgumentException)
        {
            *error = InvalidPageError;
            return 1;
        }
        catch
        {
            *error = UnexpectedError;
            return 1;
        }
    }

    private static ITursoPageCodec GetCodec(IntPtr context)
    {
        if (context == IntPtr.Zero)
            throw new InvalidOperationException("Page codec context is not configured.");

        return GCHandle.FromIntPtr(context).Target as ITursoPageCodec
               ?? throw new InvalidOperationException("Page codec context is invalid.");
    }

    private static byte* AllocUtf8(string value) => (byte*)Marshal.StringToCoTaskMemUTF8(value);
}
