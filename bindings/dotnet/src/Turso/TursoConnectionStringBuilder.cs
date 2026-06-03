using System.Collections.ObjectModel;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Turso.Raw.Public.Value;

namespace Turso;

public sealed class TursoConnectionStringBuilder : DbConnectionStringBuilder
{
    private static readonly Dictionary<string, string> KeywordMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Data Source"] = "Data Source",
        ["DataSource"] = "Data Source",
        ["Filename"] = "Data Source",
        ["Mode"] = "Mode",
        ["Cache"] = "Cache",
        ["Password"] = "Password",
        ["Foreign Keys"] = "Foreign Keys",
        ["ForeignKeys"] = "Foreign Keys",
        ["Recursive Triggers"] = "Recursive Triggers",
        ["RecursiveTriggers"] = "Recursive Triggers",
        ["Default Timeout"] = "Default Timeout",
        ["DefaultTimeout"] = "Default Timeout",
        ["Command Timeout"] = "Default Timeout",
        ["CommandTimeout"] = "Default Timeout",
        ["Pooling"] = "Pooling",
        ["Vfs"] = "Vfs",
        ["Encryption Cipher"] = "Encryption Cipher",
        ["EncryptionCipher"] = "Encryption Cipher",
        ["Encryption Key"] = "Encryption Key",
        ["EncryptionKey"] = "Encryption Key",
    };

    public TursoConnectionStringBuilder()
    {
    }

    public TursoConnectionStringBuilder(string? connectionString)
    {
        ConnectionString = connectionString ?? string.Empty;
    }

    public string DataSource
    {
        get => GetString("Data Source");
        set => SetString("Data Source", value);
    }

    public string Mode
    {
        get => GetString("Mode");
        set => SetString("Mode", value);
    }

    public string Cache
    {
        get => GetString("Cache");
        set => SetString("Cache", value);
    }

    public string Password
    {
        get => GetString("Password");
        set => SetString("Password", value);
    }

    public bool? ForeignKeys
    {
        get => GetNullableBool("Foreign Keys");
        set => SetNullable("Foreign Keys", value);
    }

    public bool RecursiveTriggers
    {
        get => GetBool("Recursive Triggers");
        set => this["Recursive Triggers"] = value;
    }

    public int DefaultTimeout
    {
        get => GetInt("Default Timeout", 30);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Default Timeout"] = value;
        }
    }

    public bool Pooling
    {
        get => GetBool("Pooling");
        set => this["Pooling"] = value;
    }

    public string Vfs
    {
        get => GetString("Vfs");
        set => SetString("Vfs", value);
    }

    public string EncryptionCipher
    {
        get => GetString("Encryption Cipher");
        set => SetString("Encryption Cipher", value);
    }

    public string EncryptionKey
    {
        get => GetString("Encryption Key");
        set => SetString("Encryption Key", value);
    }

    [AllowNull]
    public override object this[string keyword]
    {
        get => base[NormalizeKeyword(keyword)];
        set
        {
            var normalizedKeyword = NormalizeKeyword(keyword);
            if (value is null)
            {
                Remove(normalizedKeyword);
                return;
            }

            base[normalizedKeyword] = value;
        }
    }

    public override bool ContainsKey(string keyword) => base.ContainsKey(NormalizeKeyword(keyword));

    public override bool Remove(string keyword) => base.Remove(NormalizeKeyword(keyword));

    public override bool TryGetValue(string keyword, out object value)
    {
        var found = base.TryGetValue(NormalizeKeyword(keyword), out var result);
        value = result!;
        return found;
    }

    internal static ReadOnlyCollection<string> ValidKeywords { get; } =
        new(KeywordMap.Values.Distinct(StringComparer.OrdinalIgnoreCase).ToArray());

    internal string? GetOption(string keyword)
    {
        return TryGetValue(keyword, out var value)
            ? Convert.ToString(value, CultureInfo.InvariantCulture)
            : null;
    }

    internal TursoEncryptionCipher? GetEncryptionCipher()
    {
        var cipher = GetOption("Encryption Cipher");
        if (string.IsNullOrWhiteSpace(cipher))
            return null;

        return cipher.ToLowerInvariant() switch
        {
            "aes128gcm" => TursoEncryptionCipher.Aes128Gcm,
            "aes256gcm" => TursoEncryptionCipher.Aes256Gcm,
            "aegis256" => TursoEncryptionCipher.Aegis256,
            "aegis256x2" => TursoEncryptionCipher.Aegis256x2,
            "aegis128l" => TursoEncryptionCipher.Aegis128l,
            "aegis128x2" => TursoEncryptionCipher.Aegis128x2,
            "aegis128x4" => TursoEncryptionCipher.Aegis128x4,
            _ => throw new InvalidOperationException($"Unknown encryption cipher: {cipher}")
        };
    }

    private static string NormalizeKeyword(string keyword)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyword);
        if (KeywordMap.TryGetValue(keyword, out var normalizedKeyword))
            return normalizedKeyword;

        throw new ArgumentException($"Unsupported keyword: {keyword}", nameof(keyword));
    }

    private string GetString(string keyword) => GetOption(keyword) ?? string.Empty;

    private void SetString(string keyword, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        this[keyword] = value;
    }

    private bool GetBool(string keyword)
    {
        return TryGetValue(keyword, out var value) && Convert.ToBoolean(value, CultureInfo.InvariantCulture);
    }

    private bool? GetNullableBool(string keyword)
    {
        return TryGetValue(keyword, out var value)
            ? Convert.ToBoolean(value, CultureInfo.InvariantCulture)
            : null;
    }

    private int GetInt(string keyword, int defaultValue)
    {
        return TryGetValue(keyword, out var value)
            ? Convert.ToInt32(value, CultureInfo.InvariantCulture)
            : defaultValue;
    }

    private void SetNullable<T>(string keyword, T? value)
        where T : struct
    {
        if (value.HasValue)
            this[keyword] = value.Value;
        else
            Remove(keyword);
    }
}
