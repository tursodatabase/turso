using System.Runtime.InteropServices;

namespace Turso.Raw.Public;

public enum TursoPageCodecLocation : uint
{
    Database = 0,
    Wal = 1,
}

public readonly record struct TursoPageCodecHeaderInfo(uint PageSize, byte ReservedSpace);

public interface ITursoPageCodec
{
    TursoPageCodecHeaderInfo? ProbeHeader(ReadOnlySpan<byte> rawPage1Prefix);

    void DecodePage(
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output);

    void EncodePage(
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output);
}

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal unsafe delegate int TursoPageCodecProbeHeaderCallback(
    IntPtr context,
    byte* rawPage1Prefix,
    UIntPtr rawLen,
    TursoNativePageCodecHeaderInfo* headerInfo,
    IntPtr* error);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal unsafe delegate int TursoPageCodecTransformCallback(
    IntPtr context,
    uint pageNo,
    TursoPageCodecLocation location,
    byte* input,
    UIntPtr inputLen,
    byte* output,
    UIntPtr outputLen,
    IntPtr* error);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void TursoPageCodecDestroyCallback(IntPtr context);

[StructLayout(LayoutKind.Sequential)]
internal struct TursoNativePageCodecHeaderInfo
{
    public uint PageSize;
    public byte ReservedSpace;
    public byte IsSupported;
}

[StructLayout(LayoutKind.Sequential)]
internal struct TursoNativePageCodecV1
{
    public uint AbiVersion;
    public IntPtr Context;
    public byte ReservedSpace;
    public IntPtr Destroy;
    public IntPtr ProbeHeader;
    public IntPtr DecodePage;
    public IntPtr EncodePage;
}

internal sealed unsafe class TursoPageCodec : IDisposable
{
    private sealed class State(ITursoPageCodec codec) : IDisposable
    {
        private readonly object _lock = new();
        private readonly List<IntPtr> _errors = [];
        private bool _disposed;

        public ITursoPageCodec Codec { get; } = codec;

        public IntPtr SetError(Exception exception)
        {
            var message = Marshal.StringToCoTaskMemUTF8(exception.Message);
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                _errors.Add(message);
            }

            return message;
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed)
                    return;

                foreach (var error in _errors)
                    Marshal.FreeCoTaskMem(error);

                _errors.Clear();
                _disposed = true;
            }
        }
    }

    private delegate void PageTransform(
        ITursoPageCodec codec,
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output);

    private static readonly TursoPageCodecProbeHeaderCallback ProbeHeaderCallback = ProbeHeader;
    private static readonly TursoPageCodecTransformCallback DecodePageCallback = DecodePage;
    private static readonly TursoPageCodecTransformCallback EncodePageCallback = EncodePage;
    private static readonly TursoPageCodecDestroyCallback DestroyCallback = Destroy;

    private readonly IntPtr _nativeCodec;
    private readonly GCHandle _stateHandle;
    private bool _ownsState = true;
    private bool _disposed;

    public TursoPageCodec(ITursoPageCodec codec, byte reservedSpace)
    {
        ArgumentNullException.ThrowIfNull(codec);
        _stateHandle = GCHandle.Alloc(new State(codec));

        var native = new TursoNativePageCodecV1
        {
            AbiVersion = 1,
            Context = GCHandle.ToIntPtr(_stateHandle),
            ReservedSpace = reservedSpace,
            Destroy = Marshal.GetFunctionPointerForDelegate(DestroyCallback),
            ProbeHeader = Marshal.GetFunctionPointerForDelegate(ProbeHeaderCallback),
            DecodePage = Marshal.GetFunctionPointerForDelegate(DecodePageCallback),
            EncodePage = Marshal.GetFunctionPointerForDelegate(EncodePageCallback),
        };

        _nativeCodec = Marshal.AllocHGlobal(Marshal.SizeOf<TursoNativePageCodecV1>());
        Marshal.StructureToPtr(native, _nativeCodec, false);
    }

    internal IntPtr NativePointer
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _nativeCodec;
        }
    }

    public void TransferNativeOwnership()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _ownsState = false;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Marshal.FreeHGlobal(_nativeCodec);
        if (_ownsState)
            Destroy(GCHandle.ToIntPtr(_stateHandle));

        _disposed = true;
    }

    private static int ProbeHeader(
        IntPtr context,
        byte* rawPage1Prefix,
        UIntPtr rawLen,
        TursoNativePageCodecHeaderInfo* headerInfo,
        IntPtr* error)
    {
        var state = GetState(context);
        try
        {
            var raw = new ReadOnlySpan<byte>(rawPage1Prefix, checked((int)rawLen));
            var result = state.Codec.ProbeHeader(raw);
            if (result is null)
            {
                *headerInfo = default;
            }
            else
            {
                *headerInfo = new TursoNativePageCodecHeaderInfo
                {
                    PageSize = result.Value.PageSize,
                    ReservedSpace = result.Value.ReservedSpace,
                    IsSupported = 1,
                };
            }
            *error = IntPtr.Zero;
            return 0;
        }
        catch (Exception ex)
        {
            *error = state.SetError(ex);
            return 1;
        }
    }

    private static int DecodePage(
        IntPtr context,
        uint pageNo,
        TursoPageCodecLocation location,
        byte* input,
        UIntPtr inputLen,
        byte* output,
        UIntPtr outputLen,
        IntPtr* error)
    {
        return Transform(context, pageNo, location, input, inputLen, output, outputLen, error, DecodePage);
    }

    private static int EncodePage(
        IntPtr context,
        uint pageNo,
        TursoPageCodecLocation location,
        byte* input,
        UIntPtr inputLen,
        byte* output,
        UIntPtr outputLen,
        IntPtr* error)
    {
        return Transform(context, pageNo, location, input, inputLen, output, outputLen, error, EncodePage);
    }

    private static int Transform(
        IntPtr context,
        uint pageNo,
        TursoPageCodecLocation location,
        byte* input,
        UIntPtr inputLen,
        byte* output,
        UIntPtr outputLen,
        IntPtr* error,
        PageTransform transform)
    {
        var state = GetState(context);
        try
        {
            var inputSpan = new ReadOnlySpan<byte>(input, checked((int)inputLen));
            var outputSpan = new Span<byte>(output, checked((int)outputLen));
            transform(state.Codec, pageNo, location, inputSpan, outputSpan);
            *error = IntPtr.Zero;
            return 0;
        }
        catch (Exception ex)
        {
            *error = state.SetError(ex);
            return 1;
        }
    }

    private static void DecodePage(
        ITursoPageCodec codec,
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output)
    {
        codec.DecodePage(pageNo, location, input, output);
    }

    private static void EncodePage(
        ITursoPageCodec codec,
        uint pageNo,
        TursoPageCodecLocation location,
        ReadOnlySpan<byte> input,
        Span<byte> output)
    {
        codec.EncodePage(pageNo, location, input, output);
    }

    private static State GetState(IntPtr context)
    {
        if (context == IntPtr.Zero)
            throw new InvalidOperationException("Page codec context is not configured.");

        return GCHandle.FromIntPtr(context).Target as State
            ?? throw new InvalidOperationException("Page codec context is invalid.");
    }

    private static void Destroy(IntPtr context)
    {
        if (context == IntPtr.Zero)
            return;

        var handle = GCHandle.FromIntPtr(context);
        if (handle.Target is State state)
            state.Dispose();

        handle.Free();
    }
}
