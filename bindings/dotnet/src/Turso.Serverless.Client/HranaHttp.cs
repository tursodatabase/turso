using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Turso.Serverless.Client;

/// <summary>
/// HTTP transport for the SQL over HTTP protocol: <c>/v3/pipeline</c> (JSON request/response)
/// and <c>/v3/cursor</c> (JSON request, newline-delimited JSON streaming response).
/// </summary>
internal static class HranaHttp
{
    /// <summary>HTTP header carrying the encryption key of an encrypted Turso Cloud database.</summary>
    internal const string EncryptionKeyHeader = "x-turso-encryption-key";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    internal static async Task<HranaPipelineResponse> ExecutePipelineAsync(
        HttpClient httpClient,
        string baseUrl,
        string? authToken,
        string? remoteEncryptionKey,
        HranaPipelineRequest request,
        CancellationToken cancellationToken)
    {
        using var httpRequest = CreateRequest($"{baseUrl}/v3/pipeline", authToken, remoteEncryptionKey, request);

        using var response = await httpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new TursoServerlessException($"HTTP error! status: {(int)response.StatusCode}");
        }

        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        return JsonSerializer.Deserialize<HranaPipelineResponse>(body, JsonOptions)
            ?? throw new TursoServerlessException("Empty pipeline response");
    }

    internal static async Task<(HranaCursorResponse Response, IAsyncEnumerable<HranaCursorEntry> Entries)> ExecuteCursorAsync(
        HttpClient httpClient,
        string baseUrl,
        string? authToken,
        string? remoteEncryptionKey,
        HranaCursorRequest request,
        CancellationToken cancellationToken)
    {
        var httpRequest = CreateRequest($"{baseUrl}/v3/cursor", authToken, remoteEncryptionKey, request);

        HttpResponseMessage? response = null;
        StreamReader? reader = null;
        try
        {
            response = await httpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                var errorMessage = $"HTTP error! status: {(int)response.StatusCode}";
                try
                {
                    var errorBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    using var errorDoc = JsonDocument.Parse(errorBody);
                    if (errorDoc.RootElement.TryGetProperty("message", out var message) && message.GetString() is { } messageText)
                    {
                        errorMessage = messageText;
                    }
                }
                catch
                {
                    // If the error body cannot be parsed, keep the default HTTP error message.
                }

                throw new TursoServerlessException(errorMessage);
            }

            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            reader = new StreamReader(stream, Encoding.UTF8);

            // The first non-empty line is the cursor response; the rest are cursor entries.
            HranaCursorResponse? cursorResponse = null;
            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                if (line.Trim() is not ("" or null) and var trimmed)
                {
                    cursorResponse = JsonSerializer.Deserialize<HranaCursorResponse>(trimmed, JsonOptions);
                    break;
                }
            }

            if (cursorResponse is null)
            {
                throw new TursoServerlessException("No cursor response received");
            }

            var entries = ReadEntriesAsync(response, reader, cancellationToken);
            response = null; // Ownership transferred to the entry enumerator.
            reader = null;
            return (cursorResponse, entries);
        }
        finally
        {
            reader?.Dispose();
            response?.Dispose();
            httpRequest.Dispose();
        }
    }

    private static async IAsyncEnumerable<HranaCursorEntry> ReadEntriesAsync(
        HttpResponseMessage response,
        StreamReader reader,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using (response)
        using (reader)
        {
            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                var trimmed = line.Trim();
                if (trimmed.Length == 0)
                {
                    continue;
                }

                var entry = JsonSerializer.Deserialize<HranaCursorEntry>(trimmed, JsonOptions);
                if (entry is not null)
                {
                    yield return entry;
                }
            }
        }
    }

    private static HttpRequestMessage CreateRequest<T>(string url, string? authToken, string? remoteEncryptionKey, T body)
    {
        var httpRequest = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(JsonSerializer.Serialize(body, JsonOptions), Encoding.UTF8, "application/json"),
        };

        if (!string.IsNullOrEmpty(authToken))
        {
            httpRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", authToken);
        }

        if (!string.IsNullOrEmpty(remoteEncryptionKey))
        {
            httpRequest.Headers.Add(EncryptionKeyHeader, remoteEncryptionKey);
        }

        return httpRequest;
    }
}
