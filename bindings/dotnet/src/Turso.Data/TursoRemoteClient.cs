using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Turso.Raw.Public;
using Turso.Raw.Public.Value;

namespace Turso;

internal sealed class TursoRemoteClient : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly HttpClient _httpClient;
    private readonly string? _authToken;
    private readonly bool _disposeHttpClient;
    private Uri _pipelineUri;
    private string? _baton;

    public TursoRemoteClient(Uri endpoint, string? authToken)
        : this(new HttpClient(), endpoint, authToken, disposeHttpClient: true)
    {
    }

    internal TursoRemoteClient(HttpClient httpClient, Uri endpoint, string? authToken, bool disposeHttpClient = false)
    {
        ArgumentNullException.ThrowIfNull(httpClient);
        ArgumentNullException.ThrowIfNull(endpoint);

        _httpClient = httpClient;
        _pipelineUri = CreatePipelineUri(endpoint);
        _authToken = string.IsNullOrWhiteSpace(authToken) ? null : authToken;
        ValidateAuthTokenTransport(_pipelineUri, _authToken);
        _disposeHttpClient = disposeHttpClient;
    }

    public bool HasOpenSession => _baton is not null;

    public async Task<RemoteStatementResult> ExecuteAsync(
        string sql,
        TursoParameterCollection parameters,
        bool wantRows,
        int commandTimeout,
        bool closeAfter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);

        var request = new RemotePipelineRequest
        {
            Baton = _baton,
            Requests =
            [
                RemoteStreamRequest.Execute(BuildStatement(sql, parameters, wantRows)),
            ],
        };

        if (closeAfter)
            request.Requests.Add(RemoteStreamRequest.Close());

        var response = await SendPipelineAsync(request, commandTimeout, cancellationToken).ConfigureAwait(false);
        UpdateSession(response, closeAfter);
        return ExtractExecuteResult(response);
    }

    public async Task<IReadOnlyList<RemoteStatementResult>> ExecuteBatchAsync(
        IReadOnlyList<TursoBatchCommand> commands,
        int commandTimeout,
        bool wantRows,
        bool closeAfter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(commands);
        if (commands.Count == 0)
            throw new InvalidOperationException("Batch must contain at least one command.");

        var steps = new List<RemoteBatchStep>(commands.Count);
        foreach (var command in commands)
            steps.Add(new RemoteBatchStep { Statement = BuildStatement(command.CommandText, command.Parameters, wantRows) });

        var request = new RemotePipelineRequest
        {
            Baton = _baton,
            Requests =
            [
                RemoteStreamRequest.Batch(new RemoteBatch { Steps = steps }),
            ],
        };

        if (closeAfter)
            request.Requests.Add(RemoteStreamRequest.Close());

        var response = await SendPipelineAsync(request, commandTimeout, cancellationToken).ConfigureAwait(false);
        UpdateSession(response, closeAfter);
        return ExtractBatchResults(response, commands.Count);
    }

    public async Task CloseAsync(int commandTimeout, CancellationToken cancellationToken)
    {
        if (_baton is null)
            return;

        var request = new RemotePipelineRequest
        {
            Baton = _baton,
            Requests = [RemoteStreamRequest.Close()],
        };

        var response = await SendPipelineAsync(request, commandTimeout, cancellationToken).ConfigureAwait(false);
        UpdateSession(response, closeAfter: false);
        ValidateCloseResult(response);
        _baton = null;
    }

    public void Dispose()
    {
        if (_disposeHttpClient)
            _httpClient.Dispose();
    }

    private async Task<RemotePipelineResponse> SendPipelineAsync(
        RemotePipelineRequest request,
        int commandTimeout,
        CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeout(commandTimeout, cancellationToken);
        var effectiveCancellationToken = timeout?.Token ?? cancellationToken;

        var json = JsonSerializer.Serialize(request, JsonOptions);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, _pipelineUri)
        {
            Content = content,
        };

        if (_authToken is not null)
            httpRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _authToken);

        using var response = await _httpClient
            .SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, effectiveCancellationToken)
            .ConfigureAwait(false);

        var body = await response.Content.ReadAsStringAsync(effectiveCancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new TursoException(
                $"Remote request failed with HTTP {(int)response.StatusCode} {response.ReasonPhrase}: {body}");
        }

        try
        {
            return JsonSerializer.Deserialize<RemotePipelineResponse>(body, JsonOptions)
                   ?? throw new TursoException("Remote request returned an empty response.");
        }
        catch (JsonException ex)
        {
            throw new TursoException($"Unable to parse remote response: {ex.Message}");
        }
    }

    private static CancellationTokenSource? CreateTimeout(int commandTimeout, CancellationToken cancellationToken)
    {
        if (commandTimeout <= 0)
            return null;

        var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(commandTimeout));
        return timeout;
    }

    private static void ValidateAuthTokenTransport(Uri endpoint, string? authToken)
    {
        if (authToken is null
            || endpoint.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)
            || endpoint.IsLoopback)
        {
            return;
        }

        throw new InvalidOperationException("Auth Token requires an HTTPS remote Turso URL unless the host is localhost or loopback.");
    }

    private static RemoteStatement BuildStatement(string sql, TursoParameterCollection parameters, bool wantRows)
    {
        var statement = new RemoteStatement
        {
            Sql = sql,
            WantRows = wantRows,
        };

        foreach (TursoParameter parameter in parameters)
        {
            var value = RemoteRequestValue.FromTursoValue(parameter.ToValue());
            if (string.IsNullOrEmpty(parameter.ParameterName))
            {
                statement.Args.Add(value);
            }
            else
            {
                statement.NamedArgs.Add(new RemoteNamedArg
                {
                    Name = parameter.ParameterName,
                    Value = value,
                });
            }
        }

        return statement;
    }

    private static RemoteStatementResult ExtractExecuteResult(RemotePipelineResponse response)
    {
        if (response.Results.Count == 0)
            throw new TursoException("Remote request returned no results.");

        var result = response.Results[0];
        RemoteStatementResult statementResult;
        switch (result.Type)
        {
            case "ok":
                if (result.Response is null)
                    throw new TursoException("Remote request returned an empty ok response.");
                if (result.Response.Type != "execute")
                    throw new TursoException($"Remote request returned unexpected response type: {result.Response.Type}");
                statementResult = result.Response.DeserializeResult<RemoteStatementResult>();
                break;

            case "error":
                throw CreateRemoteError(result.Error);

            default:
                throw new TursoException($"Remote request returned unexpected result type: {result.Type}");
        }

        ValidateOptionalTrailingClose(response, "Remote request");
        return statementResult;
    }

    private static IReadOnlyList<RemoteStatementResult> ExtractBatchResults(RemotePipelineResponse response, int expectedCount)
    {
        if (response.Results.Count == 0)
            throw new TursoException("Remote batch returned no results.");

        var result = response.Results[0];
        List<RemoteStatementResult> statementResults;
        switch (result.Type)
        {
            case "ok":
                if (result.Response is null)
                    throw new TursoException("Remote batch returned an empty ok response.");
                if (result.Response.Type != "batch")
                    throw new TursoException($"Remote batch returned unexpected response type: {result.Response.Type}");

                var batch = result.Response.DeserializeResult<RemoteBatchResult>();
                statementResults = ExtractBatchStepResults(batch, expectedCount);
                break;

            case "error":
                throw CreateRemoteError(result.Error);

            default:
                throw new TursoException($"Remote batch returned unexpected result type: {result.Type}");
        }

        ValidateOptionalTrailingClose(response, "Remote batch");
        return statementResults;
    }

    private static void ValidateOptionalTrailingClose(RemotePipelineResponse response, string operation)
    {
        if (response.Results.Count == 1)
            return;
        if (response.Results.Count > 2)
            throw new TursoException($"{operation} returned too many results.");

        var result = response.Results[1];
        switch (result.Type)
        {
            case "ok":
                if (result.Response?.Type != "close")
                    throw new TursoException($"{operation} returned unexpected response type: {result.Response?.Type}");
                break;

            case "error":
                break;

            default:
                throw new TursoException($"{operation} returned unexpected result type: {result.Type}");
        }
    }

    private static List<RemoteStatementResult> ExtractBatchStepResults(RemoteBatchResult batch, int expectedCount)
    {
        if (batch.StepErrors.Count != expectedCount || batch.StepResults.Count != expectedCount)
        {
            throw new TursoException(
                $"Remote batch returned an unexpected result shape: {batch.StepResults.Count} results, {batch.StepErrors.Count} errors, expected {expectedCount}.");
        }

        for (var i = 0; i < batch.StepErrors.Count; i++)
        {
            if (batch.StepErrors[i] is { } error)
                throw CreateRemoteError(error);
        }

        var statementResults = new List<RemoteStatementResult>(expectedCount);
        for (var i = 0; i < batch.StepResults.Count; i++)
        {
            var stepResult = batch.StepResults[i]
                             ?? throw new TursoException($"Remote batch did not return a result for step {i}.");
            statementResults.Add(stepResult);
        }

        return statementResults;
    }

    private static void ValidateCloseResult(RemotePipelineResponse response)
    {
        foreach (var result in response.Results)
        {
            switch (result.Type)
            {
                case "ok":
                    if (result.Response?.Type is not "close")
                        throw new TursoException($"Remote close returned unexpected response type: {result.Response?.Type}");
                    break;

                case "error":
                    throw CreateRemoteError(result.Error);

                default:
                    throw new TursoException($"Remote close returned unexpected result type: {result.Type}");
            }
        }
    }

    private static TursoException CreateRemoteError(RemoteError? error)
    {
        if (error is null)
            return new TursoRemoteSqlException("Remote SQL execution failed.");

        return string.IsNullOrWhiteSpace(error.Code)
            ? new TursoRemoteSqlException($"Remote SQL execution failed: {error.Message}")
            : new TursoRemoteSqlException($"Remote SQL execution failed: {error.Message} ({error.Code})");
    }

    private void UpdateSession(RemotePipelineResponse response, bool closeAfter)
    {
        if (!string.IsNullOrWhiteSpace(response.BaseUrl))
        {
            var pipelineUri = CreatePipelineUri(new Uri(_pipelineUri, response.BaseUrl));
            ValidateAuthTokenTransport(pipelineUri, _authToken);
            _pipelineUri = pipelineUri;
        }

        _baton = closeAfter ? null : response.Baton;
    }

    private static Uri CreatePipelineUri(Uri endpoint)
    {
        var builder = new UriBuilder(endpoint)
        {
            Query = string.Empty,
            Fragment = string.Empty,
        };

        var path = builder.Path;
        builder.Path = string.IsNullOrEmpty(path) || path == "/"
            ? "/v2/pipeline"
            : path.TrimEnd('/').EndsWith("/v2/pipeline", StringComparison.OrdinalIgnoreCase)
                ? path
                : path.TrimEnd('/') + "/v2/pipeline";

        return builder.Uri;
    }

    private sealed class RemotePipelineRequest
    {
        [JsonPropertyName("baton")]
        public string? Baton { get; init; }

        [JsonPropertyName("requests")]
        public List<RemoteStreamRequest> Requests { get; init; } = [];
    }

    private sealed class RemoteStreamRequest
    {
        [JsonPropertyName("type")]
        public string Type { get; init; } = "";

        [JsonPropertyName("stmt")]
        public RemoteStatement? Statement { get; init; }

        [JsonPropertyName("batch")]
        public RemoteBatch? BatchRequest { get; init; }

        public static RemoteStreamRequest Execute(RemoteStatement statement)
        {
            return new RemoteStreamRequest
            {
                Type = "execute",
                Statement = statement,
            };
        }

        public static RemoteStreamRequest Batch(RemoteBatch batch)
        {
            return new RemoteStreamRequest
            {
                Type = "batch",
                BatchRequest = batch,
            };
        }

        public static RemoteStreamRequest Close()
        {
            return new RemoteStreamRequest
            {
                Type = "close",
            };
        }
    }

    private sealed class RemoteStatement
    {
        [JsonPropertyName("sql")]
        public string Sql { get; init; } = "";

        [JsonPropertyName("args")]
        public List<RemoteRequestValue> Args { get; } = [];

        [JsonPropertyName("named_args")]
        public List<RemoteNamedArg> NamedArgs { get; } = [];

        [JsonPropertyName("want_rows")]
        public bool WantRows { get; init; }
    }

    private sealed class RemoteBatch
    {
        [JsonPropertyName("steps")]
        public List<RemoteBatchStep> Steps { get; init; } = [];
    }

    private sealed class RemoteBatchStep
    {
        [JsonPropertyName("stmt")]
        public RemoteStatement Statement { get; init; } = new();
    }

    private sealed class RemoteNamedArg
    {
        [JsonPropertyName("name")]
        public string Name { get; init; } = "";

        [JsonPropertyName("value")]
        public RemoteRequestValue Value { get; init; } = RemoteRequestValue.Null();
    }

    private sealed class RemoteRequestValue
    {
        [JsonPropertyName("type")]
        public string Type { get; init; } = "";

        [JsonPropertyName("value")]
        public object? Value { get; init; }

        [JsonPropertyName("base64")]
        public string? Base64 { get; init; }

        public static RemoteRequestValue Null()
        {
            return new RemoteRequestValue
            {
                Type = "null",
            };
        }

        public static RemoteRequestValue FromTursoValue(TursoValue value)
        {
            return value.ValueType switch
            {
                TursoValueType.Empty or TursoValueType.Null => Null(),
                TursoValueType.Integer => new RemoteRequestValue
                {
                    Type = "integer",
                    Value = value.IntValue.ToString(CultureInfo.InvariantCulture),
                },
                TursoValueType.Real => new RemoteRequestValue
                {
                    Type = "float",
                    Value = value.RealValue,
                },
                TursoValueType.Text => new RemoteRequestValue
                {
                    Type = "text",
                    Value = value.StringValue ?? string.Empty,
                },
                TursoValueType.Blob => new RemoteRequestValue
                {
                    Type = "blob",
                    Base64 = Convert.ToBase64String(value.BlobValue ?? []),
                },
                _ => throw new ArgumentOutOfRangeException(nameof(value), value.ValueType, null),
            };
        }
    }
}

internal sealed class RemotePipelineResponse
{
    [JsonPropertyName("baton")]
    public string? Baton { get; init; }

    [JsonPropertyName("base_url")]
    public string? BaseUrl { get; init; }

    [JsonPropertyName("results")]
    public List<RemoteStreamResult> Results { get; init; } = [];
}

internal sealed class RemoteStreamResult
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("response")]
    public RemoteStreamResponse? Response { get; init; }

    [JsonPropertyName("error")]
    public RemoteError? Error { get; init; }
}

internal sealed class RemoteStreamResponse
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("result")]
    public JsonElement Result { get; init; }

    public T DeserializeResult<T>()
    {
        if (Result.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
            throw new TursoException($"Remote response {Type} did not include a result.");

        try
        {
            return Result.Deserialize<T>()
                   ?? throw new TursoException($"Remote response {Type} returned an empty result.");
        }
        catch (JsonException ex)
        {
            throw new TursoException($"Unable to parse remote {Type} response: {ex.Message}");
        }
    }
}

internal sealed class RemoteError
{
    [JsonPropertyName("message")]
    public string Message { get; init; } = "";

    [JsonPropertyName("code")]
    public string? Code { get; init; }
}

internal sealed class TursoRemoteSqlException(string message) : TursoException(message);

internal sealed class RemoteBatchResult
{
    [JsonPropertyName("step_results")]
    public List<RemoteStatementResult?> StepResults { get; init; } = [];

    [JsonPropertyName("step_errors")]
    public List<RemoteError?> StepErrors { get; init; } = [];
}

internal sealed class RemoteStatementResult
{
    [JsonPropertyName("cols")]
    public List<RemoteColumn> Columns { get; init; } = [];

    [JsonPropertyName("rows")]
    public List<List<RemoteResponseValue>> Rows { get; init; } = [];

    [JsonPropertyName("affected_row_count")]
    public ulong AffectedRowCount { get; init; }

    [JsonPropertyName("last_insert_rowid")]
    public JsonElement LastInsertRowId { get; init; }
}

internal sealed class RemoteColumn
{
    [JsonPropertyName("name")]
    public string? Name { get; init; }

    [JsonPropertyName("decltype")]
    public string? DeclType { get; init; }
}

internal sealed class RemoteResponseValue
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("value")]
    public JsonElement Value { get; init; }

    [JsonPropertyName("base64")]
    public string? Base64 { get; init; }

    public object ToClrValue()
    {
        return Type switch
        {
            "null" => DBNull.Value,
            "integer" => ParseInteger(),
            "float" => ParseFloat(),
            "text" => Value.GetString() ?? string.Empty,
            "blob" => DecodeBase64(Base64 ?? string.Empty),
            _ => throw new TursoException($"Remote response returned unsupported value type: {Type}"),
        };
    }

    public long GetInt64()
    {
        return Type switch
        {
            "integer" => ParseInteger(),
            "float" => checked((long)ParseFloat()),
            "text" => long.Parse(Value.GetString() ?? "", CultureInfo.InvariantCulture),
            _ => throw new InvalidCastException($"Cannot convert remote {Type} value to Int64."),
        };
    }

    public double GetDouble()
    {
        return Type switch
        {
            "float" => ParseFloat(),
            "integer" => ParseInteger(),
            "text" => double.Parse(Value.GetString() ?? "", CultureInfo.InvariantCulture),
            _ => throw new InvalidCastException($"Cannot convert remote {Type} value to Double."),
        };
    }

    public decimal GetDecimal()
    {
        return Type switch
        {
            "float" => Convert.ToDecimal(ParseFloat(), CultureInfo.InvariantCulture),
            "integer" => ParseInteger(),
            "text" => decimal.Parse(Value.GetString() ?? "", CultureInfo.InvariantCulture),
            _ => throw new InvalidCastException($"Cannot convert remote {Type} value to Decimal."),
        };
    }

    private long ParseInteger()
    {
        return Value.ValueKind == JsonValueKind.String
            ? long.Parse(Value.GetString() ?? "", CultureInfo.InvariantCulture)
            : Value.GetInt64();
    }

    private double ParseFloat()
    {
        return Value.ValueKind == JsonValueKind.String
            ? double.Parse(Value.GetString() ?? "", CultureInfo.InvariantCulture)
            : Value.GetDouble();
    }

    private static byte[] DecodeBase64(string value)
    {
        var padding = value.Length % 4;
        if (padding != 0)
            value = value.PadRight(value.Length + 4 - padding, '=');

        return Convert.FromBase64String(value);
    }
}
