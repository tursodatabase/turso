using System.Collections.Concurrent;

namespace Turso.Data.Sqlite;

internal static class NativeCallbackContexts
{
    private static readonly ConcurrentDictionary<IntPtr, object> Contexts = new();
    private static long _lastId;

    public static IntPtr Add(object target)
    {
        var id = (IntPtr)Interlocked.Increment(ref _lastId);
        Contexts[id] = target;
        return id;
    }

    public static T? Find<T>(IntPtr id)
        where T : class
    {
        Contexts.TryGetValue(id, out var target);
        return target as T;
    }

    public static object? Remove(IntPtr id)
        => Contexts.TryRemove(id, out var target) ? target : null;
}
