using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso.Data.Sqlite;

/// <summary>
/// Keeps local database files open between connections when <c>Pooling</c> is enabled.
/// </summary>
/// <remarks>
/// Opening a native database reads the schema and replays the WAL, which dominates the cost of
/// <see cref="SqliteConnection.Open"/>. The pool keeps one opened database per file and gives each
/// <see cref="SqliteConnection"/> a fresh native connection on it, so per-connection state such as
/// PRAGMAs, functions and temp tables never leaks between opens. Native connections are never
/// reused, so this pools databases rather than connections. Each entry also holds one idle anchor
/// connection: closing the last connection to a database checkpoints the whole WAL, and the anchor
/// keeps that from happening on every <see cref="SqliteConnection.Close"/>.
/// </remarks>
internal static class SqliteDatabasePool
{
    private static readonly object Gate = new();
    private static readonly Dictionary<string, Entry> Entries = new(
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
    private static bool _processExitHooked;

    public static TursoDatabaseHandle Connect(string filename)
    {
        var key = Path.GetFullPath(filename);
        while (true)
        {
            Entry? entry;
            lock (Gate)
            {
                if (!Entries.TryGetValue(key, out entry))
                {
                    entry = new Entry();
                    Entries.Add(key, entry);
                }

                if (!_processExitHooked)
                {
                    // Close idle databases on exit so their WAL is checkpointed, as it would be
                    // when the last connection closes without a pool.
                    AppDomain.CurrentDomain.ProcessExit += (_, _) => ClearAll();
                    _processExitHooked = true;
                }
            }

            lock (entry)
            {
                if (entry.Cleared)
                    continue;

                if (entry.Database is null)
                {
                    TursoSharedDatabaseHandle? database = null;
                    try
                    {
                        database = TursoBindings.OpenSharedDatabase(filename);
                        entry.Anchor = TursoBindings.Connect(database);
                    }
                    catch
                    {
                        database?.Dispose();
                        RemoveFailedEntry(key, entry);
                        throw;
                    }

                    entry.Database = database;
                }

                return TursoBindings.Connect(entry.Database);
            }
        }
    }

    // Called with the entry lock held. Taking Gate here is safe because Gate is never held
    // while waiting for an entry lock. Marking the entry cleared sends connections waiting on
    // it back to the dictionary, so none of them opens a database on an entry that is no
    // longer reachable from ClearPool.
    private static void RemoveFailedEntry(string key, Entry entry)
    {
        entry.Cleared = true;
        lock (Gate)
        {
            if (Entries.TryGetValue(key, out var current) && ReferenceEquals(current, entry))
                Entries.Remove(key);
        }
    }

    public static void Clear(string filename)
    {
        var key = Path.GetFullPath(filename);
        Entry? entry;
        lock (Gate)
        {
            if (!Entries.Remove(key, out entry))
                return;
        }

        entry.Release();
    }

    public static void ClearAll()
    {
        List<Entry> entries;
        lock (Gate)
        {
            entries = [.. Entries.Values];
            Entries.Clear();
        }

        foreach (var entry in entries)
            entry.Release();
    }

    private sealed class Entry
    {
        public TursoSharedDatabaseHandle? Database;
        public TursoDatabaseHandle? Anchor;
        public bool Cleared;

        // Connections still open on this database keep it alive until they close.
        public void Release()
        {
            lock (this)
            {
                Cleared = true;
                Anchor?.Dispose();
                Database?.Dispose();
                Anchor = null;
                Database = null;
            }
        }
    }
}
