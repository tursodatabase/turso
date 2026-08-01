namespace Turso.Serverless.Client;

/// <summary>Configuration for connecting to a Turso database over HTTP.</summary>
public sealed class TursoServerlessConnectionOptions
{
    /// <summary>Database URL (<c>https://...</c> or <c>libsql://...</c>).</summary>
    public required string Url { get; init; }

    /// <summary>Authentication token (optional for local development with <c>turso dev</c>).</summary>
    public string? AuthToken { get; init; }

    /// <summary>Encryption key (base64) for an encrypted Turso Cloud database.</summary>
    public string? RemoteEncryptionKey { get; init; }

    /// <summary>Default maximum query execution time before interruption.</summary>
    public TimeSpan? DefaultQueryTimeout { get; init; }
}

/// <summary>
/// A connection to a Turso database over the SQL over HTTP protocol with streaming cursor
/// support. The .NET counterpart of the JavaScript driver's <c>Connection</c>.
///
/// <para><b>Concurrency model.</b> A connection is single-stream: it runs one statement at a
/// time. This follows from the protocol — each request carries a baton from the previous
/// response to sequence operations on the server, so concurrent calls would race on that baton.
/// Calls made while another is in flight automatically wait for it to finish. For parallelism,
/// create multiple connections; constructing one is cheap and no HTTP request is sent until the
/// first statement executes.</para>
/// </summary>
public sealed class TursoServerlessConnection : IAsyncDisposable
{
    private static readonly HttpClient SharedHttpClient = new();

    private readonly TursoServerlessConnectionOptions _options;
    private readonly TursoSession _session;
    private readonly SemaphoreSlim _execLock = new(1, 1);
    private bool _isOpen = true;

    public TursoServerlessConnection(TursoServerlessConnectionOptions options)
        : this(options, SharedHttpClient)
    {
    }

    /// <summary>Creates a connection using a caller-provided <see cref="HttpClient"/> (not disposed by this class).</summary>
    public TursoServerlessConnection(TursoServerlessConnectionOptions options, HttpClient httpClient)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(httpClient);
        if (string.IsNullOrEmpty(options.Url))
        {
            throw new ArgumentException("invalid config: url is required", nameof(options));
        }

        _options = options;
        _session = new TursoSession(options, httpClient);
    }

    /// <summary>
    /// Whether the connection is currently inside a transaction, as reported by the server or
    /// established by a successful cursor transaction-control statement.
    /// </summary>
    public bool InTransaction => _session.InTransaction;

    /// <summary>Executes a SQL statement with optional positional arguments and returns the full result set.</summary>
    public Task<TursoResultSet> ExecuteAsync(string sql, IReadOnlyList<object?>? args = null, TimeSpan? queryTimeout = null, CancellationToken cancellationToken = default)
    {
        return LockedExecuteAsync(SqlArgs.ToStatement(sql, args, namedArgs: null, wantRows: true), queryTimeout, cancellationToken);
    }

    /// <summary>Executes a SQL statement with named arguments (e.g. <c>:name</c>, <c>@name</c>, <c>$name</c>) and returns the full result set.</summary>
    public Task<TursoResultSet> ExecuteAsync(string sql, IReadOnlyDictionary<string, object?> namedArgs, TimeSpan? queryTimeout = null, CancellationToken cancellationToken = default)
    {
        return LockedExecuteAsync(SqlArgs.ToStatement(sql, args: null, namedArgs, wantRows: true), queryTimeout, cancellationToken);
    }

    /// <summary>
    /// Executes multiple SQL statements in one round trip. When <paramref name="mode"/> is set,
    /// the batch runs atomically: the request also carries <c>BEGIN</c>/<c>COMMIT</c>/<c>ROLLBACK</c>
    /// steps gated on server-side conditions. When omitted, the statements run as-is under
    /// autocommit (or whatever transaction is already active on this stream).
    /// </summary>
    public async Task<TursoBatchResult> BatchAsync(IReadOnlyList<TursoBatchStatement> statements, TursoBatchMode? mode = null, TimeSpan? queryTimeout = null, CancellationToken cancellationToken = default)
    {
        var encoded = statements.Select(static s => SqlArgs.ToStatement(s.Sql, s.Args, s.NamedArgs, wantRows: false)).ToList();

        await _execLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            var effectiveMode = _session.InTransaction ? null : mode;
            return await _session.BatchAsync(encoded, effectiveMode, queryTimeout ?? _options.DefaultQueryTimeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _execLock.Release();
        }
    }

    /// <summary>Executes a sequence of SQL statements separated by semicolons (no result rows).</summary>
    public async Task SequenceAsync(string sql, TimeSpan? queryTimeout = null, CancellationToken cancellationToken = default)
    {
        await _execLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            await _session.SequenceAsync(sql, queryTimeout ?? _options.DefaultQueryTimeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _execLock.Release();
        }
    }

    /// <summary>Describes a SQL statement: column metadata and parameter names, without executing it.</summary>
    public async Task<TursoStatementDescription> DescribeAsync(string sql, TimeSpan? queryTimeout = null, CancellationToken cancellationToken = default)
    {
        await _execLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            return await _session.DescribeAsync(sql, queryTimeout ?? _options.DefaultQueryTimeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _execLock.Release();
        }
    }

    /// <summary>
    /// Begins an interactive transaction on this connection's stream. Dispose without committing
    /// to roll back.
    /// </summary>
    public async Task<TursoServerlessTransaction> BeginTransactionAsync(TursoBatchMode mode = TursoBatchMode.Deferred, CancellationToken cancellationToken = default)
    {
        await ExecuteAsync($"BEGIN {mode.ToString().ToUpperInvariant()}", args: null, queryTimeout: null, cancellationToken).ConfigureAwait(false);
        return new TursoServerlessTransaction(this);
    }

    /// <summary>Closes the connection, releasing the server-side stream.</summary>
    public async Task CloseAsync()
    {
        await _execLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!_isOpen)
            {
                return;
            }

            _isOpen = false;
            await _session.CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            _execLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
    }

    private async Task<TursoResultSet> LockedExecuteAsync(HranaStatement statement, TimeSpan? queryTimeout, CancellationToken cancellationToken)
    {
        await _execLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            return await _session.ExecuteAsync(statement.Sql, statement, queryTimeout ?? _options.DefaultQueryTimeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _execLock.Release();
        }
    }

    private void ThrowIfClosed()
    {
        if (!_isOpen)
        {
            throw new InvalidOperationException("The database connection is not open");
        }
    }
}

/// <summary>
/// An interactive transaction on a <see cref="TursoServerlessConnection"/>. Statements executed on
/// the owning connection while the transaction is open participate in it (the protocol stream is
/// the transaction scope). Dispose without committing to roll back.
/// </summary>
public sealed class TursoServerlessTransaction : IAsyncDisposable
{
    private readonly TursoServerlessConnection _connection;
    private bool _completed;

    internal TursoServerlessTransaction(TursoServerlessConnection connection)
    {
        _connection = connection;
    }

    public Task<TursoResultSet> ExecuteAsync(string sql, IReadOnlyList<object?>? args = null, CancellationToken cancellationToken = default)
    {
        return _connection.ExecuteAsync(sql, args, queryTimeout: null, cancellationToken);
    }

    public Task<TursoResultSet> ExecuteAsync(string sql, IReadOnlyDictionary<string, object?> namedArgs, CancellationToken cancellationToken = default)
    {
        return _connection.ExecuteAsync(sql, namedArgs, queryTimeout: null, cancellationToken);
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await _connection.ExecuteAsync("COMMIT", args: null, queryTimeout: null, cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        await _connection.ExecuteAsync("ROLLBACK", args: null, queryTimeout: null, cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_completed && _connection.InTransaction)
        {
            try
            {
                await RollbackAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best-effort rollback on dispose; the server also rolls back when the stream expires.
            }
        }
    }
}
