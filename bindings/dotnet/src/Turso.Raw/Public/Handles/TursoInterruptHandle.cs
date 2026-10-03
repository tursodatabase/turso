namespace Turso.Raw.Public.Handles;

public sealed class TursoInterruptHandle : IDisposable
{
    private readonly object _gate = new();
    private TursoDatabaseHandle? _owner;
    private IntPtr _connection;
    private bool _ownerReferenceAdded;

    internal TursoInterruptHandle(TursoDatabaseHandle owner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        if (owner.IsClosed || owner.IsInvalid)
            throw new ObjectDisposedException(nameof(owner));

        owner.DangerousAddRef(ref _ownerReferenceAdded);
        _owner = owner;
        _connection = owner.DangerousGetHandle();
    }

    ~TursoInterruptHandle()
    {
        Dispose();
    }

    public bool TryInterrupt()
    {
        lock (_gate)
        {
            if (_owner is null)
                return false;

            TursoInterop.ConnectionInterruptRetained(_connection);
            return true;
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_ownerReferenceAdded)
            {
                _owner!.DangerousRelease();
                _ownerReferenceAdded = false;
            }

            _owner = null;
            _connection = IntPtr.Zero;
        }

        GC.SuppressFinalize(this);
    }
}
