using Turso.Raw.Public.Value;

namespace Turso;

public class TursoConnectionOptions
{
    private Dictionary<string, string> _options = new();


    private void AddOption(string keyword, string value)
    {
        if (!_valid_keywords.Contains(keyword))
        {
            throw new InvalidOperationException($"Unsupported keyword: {keyword}");
        }

        _options[keyword] = value;
    }

    public string GetConnectionString()
    {
        var parts = new List<string>();
        foreach (var keyword in _valid_keywords)
        {
            var option = GetOption(keyword);
            if (option is not null)
            {
                parts.Add($"{keyword}={option}");
            }    
        }

        return string.Join(";", parts);
    }

    private string? GetOption(string keyword)
    {
        return _options.GetValueOrDefault(keyword);
    }

    public string? this[string keyword]
    {
        get => GetOption(keyword);
        set => AddOption(keyword, value  ?? "");
    }

    /// <summary>
    /// Gets the encryption cipher from the connection options.
    /// </summary>
    /// <returns>The cipher enum value, or null if not specified or invalid.</returns>
    public TursoEncryptionCipher? GetEncryptionCipher()
    {
        var cipherStr = GetOption("Encryption Cipher");
        if (cipherStr is null) return null;

        return cipherStr.ToLowerInvariant() switch
        {
            "aes128gcm" => TursoEncryptionCipher.Aes128Gcm,
            "aes256gcm" => TursoEncryptionCipher.Aes256Gcm,
            "aegis256" => TursoEncryptionCipher.Aegis256,
            "aegis256x2" => TursoEncryptionCipher.Aegis256x2,
            "aegis128l" => TursoEncryptionCipher.Aegis128l,
            "aegis128x2" => TursoEncryptionCipher.Aegis128x2,
            "aegis128x4" => TursoEncryptionCipher.Aegis128x4,
            _ => throw new InvalidOperationException($"Unknown encryption cipher: {cipherStr}")
        };
    }

    private readonly string[] _valid_keywords = [
        "Data Source",
        "Mode",
        "Cache",
        "Password",
        "Foreign Keys",
        "Recursive Triggers",
        "Default Timeout",
        "Pooling",
        "Vfs",
        "Encryption Cipher",
        "Encryption Key"
    ];

    public static TursoConnectionOptions Parse(string connectionString)
    {
        var options = new TursoConnectionOptions();



        foreach (var optionPart in connectionString.Split(";"))
        {
            var separatorIndex = optionPart.IndexOf('=');
            if (separatorIndex == -1)
                continue;

            var keyword = optionPart.Substring(0, separatorIndex);
            var value = optionPart.Substring(separatorIndex + 1);

            options.AddOption(keyword, value);
        }

        return options;
    }
}
