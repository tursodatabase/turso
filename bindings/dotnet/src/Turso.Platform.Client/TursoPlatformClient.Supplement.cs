namespace Turso.Platform.Client;

/// <summary>
/// The group-extension union from the Platform API. It is serialized as either the string
/// <c>"all"</c> or an array of extension names.
/// </summary>
[System.Text.Json.Serialization.JsonConverter(typeof(ExtensionsJsonConverter))]
public partial class Extensions
{
    private Extensions(IReadOnlyList<string>? names)
    {
        Names = names;
    }

    /// <summary>Enables all extensions available to the group.</summary>
    public static Extensions All { get; } = new(null);

    /// <summary>Enables only the supplied extension names.</summary>
    public static Extensions FromNames(IEnumerable<string> names)
    {
        ArgumentNullException.ThrowIfNull(names);
        return new Extensions(names.ToArray());
    }

    /// <summary>The explicitly enabled extension names, or <see langword="null"/> for <see cref="All"/>.</summary>
    public IReadOnlyList<string>? Names { get; }
}

internal sealed class ExtensionsJsonConverter : System.Text.Json.Serialization.JsonConverter<Extensions>
{
    public override Extensions Read(ref System.Text.Json.Utf8JsonReader reader, System.Type typeToConvert, System.Text.Json.JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            System.Text.Json.JsonTokenType.String when reader.GetString() == "all" => Extensions.All,
            System.Text.Json.JsonTokenType.StartArray => Extensions.FromNames(
                System.Text.Json.JsonSerializer.Deserialize<string[]>(ref reader, options)
                ?? throw new System.Text.Json.JsonException("Extensions array cannot be null.")),
            _ => throw new System.Text.Json.JsonException("Extensions must be \"all\" or an array of extension names."),
        };
    }

    public override void Write(System.Text.Json.Utf8JsonWriter writer, Extensions value, System.Text.Json.JsonSerializerOptions options)
    {
        if (value.Names is null)
        {
            writer.WriteStringValue("all");
            return;
        }

        System.Text.Json.JsonSerializer.Serialize(writer, value.Names, options);
    }
}
