using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Turso.Serverless.Client;

// Wire types for the SQL over HTTP protocol (Hrana 3), mirroring serverless/javascript/src/protocol.ts.
// Everything here is internal; the public surface re-exposes results through TursoResultSet/TursoRow.

[JsonConverter(typeof(HranaValueConverter))]
internal readonly struct HranaValue
{
    public string Type { get; init; }

    /// <summary>Integer (as decimal string) or text payload.</summary>
    public string? StringValue { get; init; }

    /// <summary>Float payload.</summary>
    public double? DoubleValue { get; init; }

    /// <summary>Blob payload (raw bytes; base64 on the wire).</summary>
    public byte[]? BlobValue { get; init; }

    public static readonly HranaValue Null = new() { Type = "null" };

    public static HranaValue Encode(object? value)
    {
        switch (value)
        {
            case null or DBNull:
                return Null;
            case bool b:
                return new HranaValue { Type = "integer", StringValue = b ? "1" : "0" };
            case sbyte or byte or short or ushort or int or uint or long:
                return new HranaValue { Type = "integer", StringValue = Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture) };
            case ulong ul:
                return new HranaValue { Type = "integer", StringValue = ul.ToString(CultureInfo.InvariantCulture) };
            case float f:
                return double.IsFinite(f)
                    ? new HranaValue { Type = "float", DoubleValue = f }
                    : throw new ArgumentException("Only finite numbers (not Infinity or NaN) can be passed as arguments");
            case double d:
                return double.IsFinite(d)
                    ? new HranaValue { Type = "float", DoubleValue = d }
                    : throw new ArgumentException("Only finite numbers (not Infinity or NaN) can be passed as arguments");
            case decimal m:
                return new HranaValue { Type = "float", DoubleValue = (double)m };
            case string s:
                return new HranaValue { Type = "text", StringValue = s };
            case char c:
                return new HranaValue { Type = "text", StringValue = c.ToString() };
            case Guid g:
                return new HranaValue { Type = "text", StringValue = g.ToString() };
            case DateTime dt:
                return new HranaValue { Type = "text", StringValue = dt.ToString("o", CultureInfo.InvariantCulture) };
            case DateTimeOffset dto:
                return new HranaValue { Type = "text", StringValue = dto.ToString("o", CultureInfo.InvariantCulture) };
            case byte[] bytes:
                return new HranaValue { Type = "blob", BlobValue = bytes };
            case ReadOnlyMemory<byte> memory:
                return new HranaValue { Type = "blob", BlobValue = memory.ToArray() };
            case Memory<byte> memory:
                return new HranaValue { Type = "blob", BlobValue = memory.ToArray() };
            default:
                throw new ArgumentException($"Unsupported argument type: {value.GetType()}");
        }
    }

    public object? Decode()
    {
        return Type switch
        {
            "null" => null,
            "integer" => long.Parse(StringValue!, CultureInfo.InvariantCulture),
            "float" => DoubleValue ?? 0d,
            "text" => StringValue,
            "blob" => BlobValue ?? [],
            _ => null,
        };
    }
}

internal sealed class HranaValueConverter : JsonConverter<HranaValue>
{
    public override HranaValue Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        var type = root.GetProperty("type").GetString() ?? "null";

        switch (type)
        {
            case "integer":
                // The server sends the integer as a decimal string; tolerate a raw number too.
                var intValue = root.GetProperty("value");
                return new HranaValue
                {
                    Type = type,
                    StringValue = intValue.ValueKind == JsonValueKind.String
                        ? intValue.GetString()
                        : intValue.GetInt64().ToString(CultureInfo.InvariantCulture),
                };
            case "float":
                return new HranaValue { Type = type, DoubleValue = root.GetProperty("value").GetDouble() };
            case "text":
                return new HranaValue { Type = type, StringValue = root.GetProperty("value").GetString() };
            case "blob":
                if (root.TryGetProperty("base64", out var b64Element) && b64Element.GetString() is { } b64)
                {
                    // The server may omit base64 padding.
                    var padding = (4 - b64.Length % 4) % 4;
                    return new HranaValue { Type = type, BlobValue = Convert.FromBase64String(b64 + new string('=', padding)) };
                }

                return new HranaValue { Type = type, BlobValue = [] };
            default:
                return HranaValue.Null;
        }
    }

    public override void Write(Utf8JsonWriter writer, HranaValue value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("type", value.Type);

        switch (value.Type)
        {
            case "integer":
            case "text":
                writer.WriteString("value", value.StringValue);
                break;
            case "float":
                writer.WriteNumber("value", value.DoubleValue ?? 0d);
                break;
            case "blob":
                writer.WriteString("base64", Convert.ToBase64String(value.BlobValue ?? []));
                break;
        }

        writer.WriteEndObject();
    }
}

internal sealed class HranaColumn
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("decltype")]
    public string? Decltype { get; set; }
}

internal sealed class HranaNamedArg
{
    [JsonPropertyName("name")]
    public required string Name { get; set; }

    [JsonPropertyName("value")]
    public HranaValue Value { get; set; }
}

internal sealed class HranaStatement
{
    [JsonPropertyName("sql")]
    public required string Sql { get; set; }

    [JsonPropertyName("args")]
    public List<HranaValue> Args { get; set; } = [];

    [JsonPropertyName("named_args")]
    public List<HranaNamedArg> NamedArgs { get; set; } = [];

    [JsonPropertyName("want_rows")]
    public bool WantRows { get; set; }
}

internal sealed class HranaBatchCondition
{
    [JsonPropertyName("type")]
    public required string Type { get; set; }

    [JsonPropertyName("step")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Step { get; set; }

    [JsonPropertyName("cond")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public HranaBatchCondition? Cond { get; set; }

    [JsonPropertyName("conds")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public List<HranaBatchCondition>? Conds { get; set; }

    public static HranaBatchCondition Ok(int step) => new() { Type = "ok", Step = step };

    public static HranaBatchCondition Not(HranaBatchCondition cond) => new() { Type = "not", Cond = cond };

    public static HranaBatchCondition And(params HranaBatchCondition[] conds) => new() { Type = "and", Conds = conds.ToList() };
}

internal sealed class HranaBatchStep
{
    [JsonPropertyName("stmt")]
    public required HranaStatement Statement { get; set; }

    [JsonPropertyName("condition")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public HranaBatchCondition? Condition { get; set; }
}

internal sealed class HranaBatch
{
    [JsonPropertyName("steps")]
    public required List<HranaBatchStep> Steps { get; set; }
}

internal sealed class HranaPipelineRequestItem
{
    [JsonPropertyName("type")]
    public required string Type { get; set; }

    [JsonPropertyName("sql")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Sql { get; set; }
}

internal sealed class HranaPipelineRequest
{
    [JsonPropertyName("baton")]
    public string? Baton { get; set; }

    [JsonPropertyName("requests")]
    public required List<HranaPipelineRequestItem> Requests { get; set; }
}

internal sealed class HranaError
{
    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }
}

internal sealed class HranaPipelineResultResponse
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("result")]
    public JsonElement? Result { get; set; }

    [JsonPropertyName("is_autocommit")]
    public bool? IsAutocommit { get; set; }
}

internal sealed class HranaPipelineResult
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("response")]
    public HranaPipelineResultResponse? Response { get; set; }

    [JsonPropertyName("error")]
    public HranaError? Error { get; set; }
}

internal sealed class HranaPipelineResponse
{
    [JsonPropertyName("baton")]
    public string? Baton { get; set; }

    [JsonPropertyName("base_url")]
    public string? BaseUrl { get; set; }

    [JsonPropertyName("results")]
    public List<HranaPipelineResult>? Results { get; set; }
}

internal sealed class HranaCursorRequest
{
    [JsonPropertyName("baton")]
    public string? Baton { get; set; }

    [JsonPropertyName("batch")]
    public required HranaBatch Batch { get; set; }
}

internal sealed class HranaCursorResponse
{
    [JsonPropertyName("baton")]
    public string? Baton { get; set; }

    [JsonPropertyName("base_url")]
    public string? BaseUrl { get; set; }
}

internal sealed class HranaCursorEntry
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("step")]
    public int? Step { get; set; }

    [JsonPropertyName("cols")]
    public List<HranaColumn>? Cols { get; set; }

    [JsonPropertyName("row")]
    public List<HranaValue>? Row { get; set; }

    [JsonPropertyName("affected_row_count")]
    public long? AffectedRowCount { get; set; }

    [JsonPropertyName("last_insert_rowid")]
    [JsonConverter(typeof(LongFromStringOrNumberConverter))]
    public long? LastInsertRowid { get; set; }

    [JsonPropertyName("error")]
    public HranaError? Error { get; set; }
}

internal sealed class HranaDescribeParam
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }
}

internal sealed class HranaDescribeResult
{
    [JsonPropertyName("params")]
    public List<HranaDescribeParam>? Params { get; set; }

    [JsonPropertyName("cols")]
    public List<HranaColumn>? Cols { get; set; }

    [JsonPropertyName("is_explain")]
    public bool IsExplain { get; set; }

    [JsonPropertyName("is_readonly")]
    public bool IsReadonly { get; set; }
}

/// <summary>The server sends <c>last_insert_rowid</c> as either a JSON string or a number.</summary>
internal sealed class LongFromStringOrNumberConverter : JsonConverter<long?>
{
    public override long? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Null => null,
            JsonTokenType.String => long.Parse(reader.GetString()!, CultureInfo.InvariantCulture),
            JsonTokenType.Number => reader.GetInt64(),
            _ => throw new JsonException($"Unexpected token for last_insert_rowid: {reader.TokenType}"),
        };
    }

    public override void Write(Utf8JsonWriter writer, long? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
        }
        else
        {
            writer.WriteNumberValue(value.Value);
        }
    }
}
