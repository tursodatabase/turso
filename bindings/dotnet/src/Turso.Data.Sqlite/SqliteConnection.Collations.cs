using System.Runtime.InteropServices;
using System.Text;
using Turso.Raw.Public;

namespace Turso.Data.Sqlite;

public partial class SqliteConnection
{
    private static readonly TursoCollationCallback CollationCallback = InvokeCollation;
    private readonly Dictionary<string, CollationRegistration> _collations = new(StringComparer.OrdinalIgnoreCase);

    private void RegisterCollation(string name, Func<string, string, int>? comparison)
    {
        ThrowIfDirectRemote("Custom collations");
        ArgumentNullException.ThrowIfNull(name);
        if (comparison is null)
        {
            _collations.Remove(name);
            if (HasNativeCallbackHandle)
            {
                using var syncOperation = _managedConnection?.EnterSyncOperation();
                TursoBindings.UnregisterCollation(DatabaseHandle, name);
            }
            return;
        }

        var registration = new CollationRegistration(name, comparison);
        _collations[name] = registration;
        if (HasNativeCallbackHandle)
        {
            using var syncOperation = _managedConnection?.EnterSyncOperation();
            TrackNativeFunctionContext(registration.Register(DatabaseHandle));
        }
    }

    private void RegisterCollations()
    {
        foreach (var registration in _collations.Values)
            TrackNativeFunctionContext(registration.Register(DatabaseHandle));
    }

    private static int InvokeCollation(IntPtr context, IntPtr leftPtr, UIntPtr leftLen, IntPtr rightPtr, UIntPtr rightLen)
    {
        var registration = (CollationRegistration?)GCHandle.FromIntPtr(context).Target
            ?? throw new ObjectDisposedException(nameof(CollationRegistration));
        return registration.Compare(ReadUtf8(leftPtr, checked((int)leftLen)), ReadUtf8(rightPtr, checked((int)rightLen)));
    }

    private static string ReadUtf8(IntPtr ptr, int length)
    {
        if (ptr == IntPtr.Zero || length == 0)
            return string.Empty;

        var bytes = new byte[length];
        Marshal.Copy(ptr, bytes, 0, bytes.Length);
        return Encoding.UTF8.GetString(bytes);
    }

    private sealed class CollationRegistration(string name, Func<string, string, int> compare)
    {
        private GCHandle _handle;

        public int Compare(string left, string right) => compare(left, right);

        public GCHandle Register(Turso.Raw.Public.Handles.TursoDatabaseHandle database)
        {
            var createdHandle = false;
            if (!_handle.IsAllocated)
            {
                _handle = GCHandle.Alloc(this);
                createdHandle = true;
            }

            try
            {
                TursoBindings.RegisterCollation(
                    database,
                    name,
                    GCHandle.ToIntPtr(_handle),
                    CollationCallback,
                    ContextDestructorCallback);
                return _handle;
            }
            catch
            {
                if (createdHandle && _handle.IsAllocated)
                {
                    _handle.Free();
                    _handle = default;
                }

                throw;
            }
        }
    }
}
