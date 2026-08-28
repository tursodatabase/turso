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
        ["Auth Token"] = "Auth Token",
        ["AuthToken"] = "Auth Token",
        ["Authentication Token"] = "Auth Token",
        ["AuthenticationToken"] = "Auth Token",
        ["Replica Path"] = "Replica Path",
        ["ReplicaPath"] = "Replica Path",
        ["Read Your Writes"] = "Read Your Writes",
        ["ReadYourWrites"] = "Read Your Writes",
        ["Sync Interval"] = "Sync Interval",
        ["SyncInterval"] = "Sync Interval",
        ["Sync Client Name"] = "Sync Client Name",
        ["SyncClientName"] = "Sync Client Name",
        ["Sync Long Poll Timeout"] = "Sync Long Poll Timeout",
        ["SyncLongPollTimeout"] = "Sync Long Poll Timeout",
        ["Bootstrap If Empty"] = "Bootstrap If Empty",
        ["BootstrapIfEmpty"] = "Bootstrap If Empty",
        ["Partial Bootstrap Prefix"] = "Partial Bootstrap Prefix",
        ["PartialBootstrapPrefix"] = "Partial Bootstrap Prefix",
        ["Partial Bootstrap Query"] = "Partial Bootstrap Query",
        ["PartialBootstrapQuery"] = "Partial Bootstrap Query",
        ["Partial Sync Segment Size"] = "Partial Sync Segment Size",
        ["PartialSyncSegmentSize"] = "Partial Sync Segment Size",
        ["Partial Sync Prefetch"] = "Partial Sync Prefetch",
        ["PartialSyncPrefetch"] = "Partial Sync Prefetch",
        ["Remote Encryption Cipher"] = "Remote Encryption Cipher",
        ["RemoteEncryptionCipher"] = "Remote Encryption Cipher",
        ["Remote Encryption Key"] = "Remote Encryption Key",
        ["RemoteEncryptionKey"] = "Remote Encryption Key",
        ["Push Operations Threshold"] = "Push Operations Threshold",
        ["PushOperationsThreshold"] = "Push Operations Threshold",
        ["Pull Bytes Threshold"] = "Pull Bytes Threshold",
        ["PullBytesThreshold"] = "Pull Bytes Threshold",
        ["Force Logical MVCC Pull"] = "Force Logical MVCC Pull",
        ["ForceLogicalMvccPull"] = "Force Logical MVCC Pull",
        ["Sync Experimental Features"] = "Sync Experimental Features",
        ["SyncExperimentalFeatures"] = "Sync Experimental Features",
        ["Tls"] = "Tls",
        ["TLS"] = "Tls",
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

    public string AuthToken
    {
        get => GetString("Auth Token");
        set => SetString("Auth Token", value);
    }

    public string ReplicaPath
    {
        get => GetString("Replica Path");
        set => SetString("Replica Path", value);
    }

    public bool ReadYourWrites
    {
        get => GetBool("Read Your Writes", defaultValue: true);
        set => this["Read Your Writes"] = value;
    }

    public int SyncInterval
    {
        get => GetInt("Sync Interval", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Sync Interval"] = value;
        }
    }

    public string SyncClientName
    {
        get => GetString("Sync Client Name");
        set => SetString("Sync Client Name", value);
    }

    public int SyncLongPollTimeout
    {
        get => GetInt("Sync Long Poll Timeout", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Sync Long Poll Timeout"] = value;
        }
    }

    public bool BootstrapIfEmpty
    {
        get => GetBool("Bootstrap If Empty", defaultValue: true);
        set => this["Bootstrap If Empty"] = value;
    }

    public int PartialBootstrapPrefix
    {
        get => GetInt("Partial Bootstrap Prefix", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Partial Bootstrap Prefix"] = value;
        }
    }

    public string PartialBootstrapQuery
    {
        get => GetString("Partial Bootstrap Query");
        set => SetString("Partial Bootstrap Query", value);
    }

    public long PartialSyncSegmentSize
    {
        get => GetLong("Partial Sync Segment Size", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Partial Sync Segment Size"] = value;
        }
    }

    public bool PartialSyncPrefetch
    {
        get => GetBool("Partial Sync Prefetch");
        set => this["Partial Sync Prefetch"] = value;
    }

    public string RemoteEncryptionCipher
    {
        get => GetString("Remote Encryption Cipher");
        set => SetString("Remote Encryption Cipher", value);
    }

    public string RemoteEncryptionKey
    {
        get => GetString("Remote Encryption Key");
        set => SetString("Remote Encryption Key", value);
    }

    public long PushOperationsThreshold
    {
        get => GetLong("Push Operations Threshold", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Push Operations Threshold"] = value;
        }
    }

    public long PullBytesThreshold
    {
        get => GetLong("Pull Bytes Threshold", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Pull Bytes Threshold"] = value;
        }
    }

    public bool ForceLogicalMvccPull
    {
        get => GetBool("Force Logical MVCC Pull");
        set => this["Force Logical MVCC Pull"] = value;
    }

    public string SyncExperimentalFeatures
    {
        get => GetString("Sync Experimental Features");
        set => SetString("Sync Experimental Features", value);
    }

    public bool? Tls
    {
        get => GetNullableBool("Tls");
        set => SetNullable("Tls", value);
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

    private bool GetBool(string keyword, bool defaultValue = false)
    {
        return TryGetValue(keyword, out var value)
            ? Convert.ToBoolean(value, CultureInfo.InvariantCulture)
            : defaultValue;
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

    private long GetLong(string keyword, long defaultValue)
    {
        return TryGetValue(keyword, out var value)
            ? Convert.ToInt64(value, CultureInfo.InvariantCulture)
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
