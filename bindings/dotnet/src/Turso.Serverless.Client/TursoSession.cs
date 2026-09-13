using System.Text.Json;

namespace Turso.Serverless.Client;

/// <summary>
/// A database session managing the SQL over HTTP connection state (baton, base URL and
/// transaction status). Port of serverless/javascript/src/session.ts.
/// </summary>
internal sealed class TursoSession
{
    private readonly TursoServerlessConnectionOptions _options;
    private readonly HttpClient _httpClient;

    private string? _baton;
    private string _baseUrl;

    // Cached autocommit status from the server's last `get_autocommit` answer or a successful
    // cursor transaction-control statement. A fresh connection is in autocommit.
    private bool _autocommit = true;

    internal TursoSession(TursoServerlessConnectionOptions options, HttpClient httpClient)
    {
        _options = options;
        _httpClient = httpClient;
        _baseUrl = NormalizeUrl(options.Url);
    }

    /// <summary>
    /// Whether the connection is currently inside a transaction, derived from the server's
    /// authoritative <c>get_autocommit</c> answer or the last successful cursor transaction-control
    /// statement. A non-null baton does NOT imply a transaction — the server also keeps the stream
    /// open for other state.
    /// </summary>
    internal bool InTransaction => !_autocommit;

    internal async Task<TursoStatementDescription> DescribeAsync(string sql, TimeSpan? queryTimeout, CancellationToken cancellationToken)
    {
        var request = new HranaPipelineRequest
        {
            Baton = _baton,
            Requests =
            [
                new HranaPipelineRequestItem { Type = "describe", Sql = sql },
                new HranaPipelineRequestItem { Type = "get_autocommit" },
            ],
        };

        var response = await RunPipelineAsync(request, queryTimeout, cancellationToken).ConfigureAwait(false);

        if (response.Results is [var result, ..])
        {
            if (result.Type == "error")
            {
                throw new TursoServerlessException(result.Error?.Message ?? "Describe execution failed", result.Error?.Code);
            }

            if (result.Response is { Type: "describe", Result: { } resultElement })
            {
                var describe = resultElement.Deserialize<HranaDescribeResult>(new JsonSerializerOptions(JsonSerializerDefaults.Web));
                if (describe is not null)
                {
                    return new TursoStatementDescription(
                        parameterNames: describe.Params?.Select(static p => p.Name ?? "").ToArray() ?? [],
                        columns: describe.Cols?.Select(static c => c.Name ?? "").ToArray() ?? [],
                        columnTypes: describe.Cols?.Select(static c => c.Decltype ?? "").ToArray() ?? [],
                        isExplain: describe.IsExplain,
                        isReadonly: describe.IsReadonly);
                }
            }
        }

        throw new TursoServerlessException("Unexpected describe response");
    }

    internal async Task<TursoResultSet> ExecuteAsync(string sql, HranaStatement statement, TimeSpan? queryTimeout, CancellationToken cancellationToken)
    {
        var request = new HranaCursorRequest
        {
            Baton = _baton,
            Batch = new HranaBatch { Steps = [new HranaBatchStep { Statement = statement }] },
        };

        var entries = await RunCursorAsync(request, queryTimeout, cancellationToken).ConfigureAwait(false);

        var columns = new List<string>();
        var columnTypes = new List<string>();
        var rows = new List<TursoRow>();
        var rowsAffected = 0L;
        long? lastInsertRowid = null;
        IReadOnlyList<string>? rowColumns = null;

        await foreach (var entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            switch (entry.Type)
            {
                case "step_begin":
                    if (entry.Cols is not null)
                    {
                        columns = entry.Cols.Select(static c => c.Name ?? "").ToList();
                        columnTypes = entry.Cols.Select(static c => c.Decltype ?? "").ToList();
                        rowColumns = columns;
                    }

                    break;
                case "row":
                    if (entry.Row is not null)
                    {
                        var values = entry.Row.Select(static v => v.Decode()).ToArray();
                        rows.Add(new TursoRow(values, rowColumns ?? []));
                    }

                    break;
                case "step_end":
                    if (entry.AffectedRowCount is { } affected)
                    {
                        rowsAffected = affected;
                    }

                    if (entry.LastInsertRowid is { } rowid)
                    {
                        lastInsertRowid = rowid;
                    }

                    break;
                case "step_error":
                case "error":
                    throw new TursoServerlessException(entry.Error?.Message ?? "SQL execution failed", entry.Error?.Code);
            }
        }

        UpdateAutocommitForTransactionControlStatement(sql);
        return new TursoResultSet(columns, columnTypes, rows, rowsAffected, lastInsertRowid);
    }

    internal async Task<TursoBatchResult> BatchAsync(IReadOnlyList<HranaStatement> statements, TursoBatchMode? mode, TimeSpan? queryTimeout, CancellationToken cancellationToken)
    {
        var userSteps = statements.Select(static s => new HranaBatchStep { Statement = s }).ToList();

        List<HranaBatchStep> steps;
        var firstUserStepIdx = 0;
        var lastUserStepIdx = userSteps.Count - 1;
        var beginIdx = -1;
        var commitIdx = -1;
        var rollbackIdx = -1;

        if (mode is null)
        {
            steps = userSteps;
        }
        else
        {
            // Atomic batch: BEGIN <mode>, then each user step gated on its predecessor succeeding,
            // then COMMIT gated on the last user step succeeding, then ROLLBACK gated on BEGIN having
            // succeeded *and* COMMIT not having succeeded. The extra ok(BEGIN) guard prevents ROLLBACK
            // from aborting a transaction the caller opened on this stream out of band.
            beginIdx = 0;
            firstUserStepIdx = 1;
            lastUserStepIdx = userSteps.Count; // 1..userSteps.Count inclusive
            commitIdx = lastUserStepIdx + 1;
            rollbackIdx = commitIdx + 1;

            steps = [BareStep($"BEGIN {mode.Value.ToString().ToUpperInvariant()}")];
            for (var i = 0; i < userSteps.Count; i++)
            {
                steps.Add(new HranaBatchStep
                {
                    Statement = userSteps[i].Statement,
                    Condition = HranaBatchCondition.Ok(i == 0 ? beginIdx : firstUserStepIdx + i - 1),
                });
            }

            steps.Add(new HranaBatchStep
            {
                Statement = BareStep("COMMIT").Statement,
                Condition = HranaBatchCondition.Ok(lastUserStepIdx),
            });
            steps.Add(new HranaBatchStep
            {
                Statement = BareStep("ROLLBACK").Statement,
                Condition = HranaBatchCondition.And([
                    HranaBatchCondition.Ok(beginIdx),
                    HranaBatchCondition.Not(HranaBatchCondition.Ok(commitIdx)),
                ]),
            });
        }

        var request = new HranaCursorRequest
        {
            Baton = _baton,
            Batch = new HranaBatch { Steps = steps },
        };

        var entries = await RunCursorAsync(request, queryTimeout, cancellationToken).ConfigureAwait(false);

        var totalRowsAffected = 0L;
        long? lastInsertRowid = null;
        TursoServerlessException? deferredError = null;

        // step_end entries don't carry a step index on the wire; the server only puts `step` on
        // step_begin / step_error. Track the current step ourselves so we know which step_end
        // belongs to a user statement when running in atomic mode.
        int? currentStep = null;
        bool IsUserStep(int? step) => mode is null || (step is { } s && s >= firstUserStepIdx && s <= lastUserStepIdx);

        await foreach (var entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            switch (entry.Type)
            {
                case "step_begin":
                    currentStep = entry.Step;
                    break;
                case "step_end":
                    if (mode is null && currentStep is { } step)
                    {
                        if (step < 0 || step >= statements.Count)
                        {
                            throw new TursoServerlessException("Unexpected batch step index");
                        }

                        UpdateAutocommitForTransactionControlStatement(statements[step].Sql);
                    }

                    if (IsUserStep(currentStep))
                    {
                        if (entry.AffectedRowCount is { } affected)
                        {
                            totalRowsAffected += affected;
                        }

                        if (entry.LastInsertRowid is { } rowid)
                        {
                            lastInsertRowid = rowid;
                        }
                    }

                    currentStep = null;
                    break;
                case "step_error":
                    if (mode is null)
                    {
                        throw new TursoServerlessException(entry.Error?.Message ?? "Batch execution failed", entry.Error?.Code);
                    }

                    // Atomic batch: capture the first error from BEGIN, any user step, or COMMIT and
                    // keep draining so ROLLBACK has a chance to clean up. Errors on the synthetic
                    // ROLLBACK step are suppressed — they would mask the real cause already captured.
                    if (deferredError is null && entry.Step != rollbackIdx)
                    {
                        deferredError = new TursoServerlessException(entry.Error?.Message ?? "Batch execution failed", entry.Error?.Code);
                    }

                    currentStep = null;
                    break;
                case "error":
                    throw new TursoServerlessException(entry.Error?.Message ?? "Batch execution failed", entry.Error?.Code);
            }
        }

        if (deferredError is not null)
        {
            throw deferredError;
        }

        return new TursoBatchResult(totalRowsAffected, lastInsertRowid);
    }

    internal async Task SequenceAsync(string sql, TimeSpan? queryTimeout, CancellationToken cancellationToken)
    {
        var request = new HranaPipelineRequest
        {
            Baton = _baton,
            Requests =
            [
                new HranaPipelineRequestItem { Type = "sequence", Sql = sql },
                new HranaPipelineRequestItem { Type = "get_autocommit" },
            ],
        };

        var response = await RunPipelineAsync(request, queryTimeout, cancellationToken).ConfigureAwait(false);

        if (response.Results is [var result, ..] && result.Type == "error")
        {
            throw new TursoServerlessException(result.Error?.Message ?? "Sequence execution failed", result.Error?.Code);
        }
    }

    /// <summary>
    /// Close the session: sends a close request so the server cleans up the stream, then resets local state.
    /// </summary>
    internal async Task CloseAsync()
    {
        if (_baton is not null)
        {
            try
            {
                var request = new HranaPipelineRequest
                {
                    Baton = _baton,
                    Requests = [new HranaPipelineRequestItem { Type = "close" }],
                };

                await HranaHttp.ExecutePipelineAsync(_httpClient, _baseUrl, _options.AuthToken, _options.RemoteEncryptionKey, request, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Ignore errors during close — the stream might already be gone or the baton stale.
            }
        }

        _baton = null;
        _autocommit = true;
    }

    private static HranaBatchStep BareStep(string sql)
    {
        return new HranaBatchStep { Statement = new HranaStatement { Sql = sql, WantRows = false } };
    }

    private static string NormalizeUrl(string url)
    {
        return url.StartsWith("libsql://", StringComparison.OrdinalIgnoreCase)
            ? string.Concat("https://", url.AsSpan("libsql://".Length))
            : url;
    }

    private async Task<HranaPipelineResponse> RunPipelineAsync(HranaPipelineRequest request, TimeSpan? queryTimeout, CancellationToken cancellationToken)
    {
        HranaPipelineResponse response;
        var callerToken = cancellationToken;
        using var timeout = CreateTimeoutScope(queryTimeout, ref cancellationToken);
        try
        {
            response = await HranaHttp.ExecutePipelineAsync(_httpClient, _baseUrl, _options.AuthToken, _options.RemoteEncryptionKey, request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _baton = null;
            _autocommit = true;
            throw Translate(e, timeout, callerToken);
        }

        _baton = response.Baton;
        if (response.BaseUrl is not null)
        {
            _baseUrl = response.BaseUrl;
        }

        UpdateAutocommit(response);
        return response;
    }

    private async Task<IAsyncEnumerable<HranaCursorEntry>> RunCursorAsync(HranaCursorRequest request, TimeSpan? queryTimeout, CancellationToken cancellationToken)
    {
        // The timeout scope must survive until the entries are drained; the caller's enumeration
        // observes it through the cancellation token captured by the enumerator.
        var callerToken = cancellationToken;
        var timeout = CreateTimeoutScope(queryTimeout, ref cancellationToken);
        try
        {
            var (response, entries) = await HranaHttp.ExecuteCursorAsync(_httpClient, _baseUrl, _options.AuthToken, _options.RemoteEncryptionKey, request, cancellationToken).ConfigureAwait(false);

            _baton = response.Baton;
            if (response.BaseUrl is not null)
            {
                _baseUrl = response.BaseUrl;
            }

            return WrapEntries(entries, timeout, callerToken);
        }
        catch (Exception e)
        {
            _baton = null;
            _autocommit = true;
            var translated = Translate(e, timeout, callerToken);
            timeout?.Dispose();
            throw translated;
        }
    }

    // callerToken is deliberately not an [EnumeratorCancellation] token: the transport already observes
    // the linked token captured by the inner enumerator; this one only distinguishes a caller-initiated
    // cancellation from a query timeout when translating exceptions.
#pragma warning disable CS8425
    private async IAsyncEnumerable<HranaCursorEntry> WrapEntries(IAsyncEnumerable<HranaCursorEntry> entries, CancellationTokenSource? timeout, CancellationToken callerToken)
#pragma warning restore CS8425
    {
        using (timeout)
        {
            var enumerator = entries.GetAsyncEnumerator(CancellationToken.None);
            await using (enumerator.ConfigureAwait(false))
            {
                while (true)
                {
                    bool moved;
                    try
                    {
                        moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _baton = null;
                        _autocommit = true;
                        throw Translate(e, timeout, callerToken);
                    }

                    if (!moved)
                    {
                        yield break;
                    }

                    yield return enumerator.Current;
                }
            }
        }
    }

    private static CancellationTokenSource? CreateTimeoutScope(TimeSpan? queryTimeout, ref CancellationToken cancellationToken)
    {
        var effective = queryTimeout;
        if (effective is null or { Ticks: <= 0 })
        {
            return null;
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(effective.Value);
        cancellationToken = cts.Token;
        return cts;
    }

    private static Exception Translate(Exception e, CancellationTokenSource? timeout, CancellationToken callerToken)
    {
        // A cancellation triggered by the timeout scope — not by the caller's own token — is a query timeout.
        if (e is OperationCanceledException && !callerToken.IsCancellationRequested && timeout is { IsCancellationRequested: true })
        {
            return new TursoTimeoutException(innerException: e);
        }

        return e;
    }

    private void UpdateAutocommit(HranaPipelineResponse response)
    {
        if (response.Results is null)
        {
            return;
        }

        foreach (var result in response.Results)
        {
            if (result.Type == "ok" && result.Response is { Type: "get_autocommit", IsAutocommit: { } isAutocommit })
            {
                _autocommit = isAutocommit;
                return;
            }
        }
    }

    private void UpdateAutocommitForTransactionControlStatement(string sql)
    {
        var command = sql.Trim().TrimEnd(';').TrimEnd();
        if (command.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase)
            && (command.Length == "BEGIN".Length || char.IsWhiteSpace(command["BEGIN".Length])))
        {
            _autocommit = false;
        }
        else if (command.Equals("COMMIT", StringComparison.OrdinalIgnoreCase)
                 || command.Equals("END", StringComparison.OrdinalIgnoreCase)
                 || command.Equals("ROLLBACK", StringComparison.OrdinalIgnoreCase))
        {
            _autocommit = true;
        }
    }
}
