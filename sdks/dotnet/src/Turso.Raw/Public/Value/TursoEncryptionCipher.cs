namespace Turso.Raw.Public.Value;

/// <summary>
/// Supported encryption ciphers for local database encryption.
/// </summary>
public enum TursoEncryptionCipher
{
    /// <summary>AES-128-GCM cipher</summary>
    Aes128Gcm,
    /// <summary>AES-256-GCM cipher</summary>
    Aes256Gcm,
    /// <summary>AEGIS-256 cipher</summary>
    Aegis256,
    /// <summary>AEGIS-256X2 cipher</summary>
    Aegis256x2,
    /// <summary>AEGIS-128L cipher</summary>
    Aegis128l,
    /// <summary>AEGIS-128X2 cipher</summary>
    Aegis128x2,
    /// <summary>AEGIS-128X4 cipher</summary>
    Aegis128x4,
}

internal static class TursoEncryptionCipherExtensions
{
    public static string ToRustString(this TursoEncryptionCipher cipher)
    {
        return cipher switch
        {
            TursoEncryptionCipher.Aes128Gcm => "aes128gcm",
            TursoEncryptionCipher.Aes256Gcm => "aes256gcm",
            TursoEncryptionCipher.Aegis256 => "aegis256",
            TursoEncryptionCipher.Aegis256x2 => "aegis256x2",
            TursoEncryptionCipher.Aegis128l => "aegis128l",
            TursoEncryptionCipher.Aegis128x2 => "aegis128x2",
            TursoEncryptionCipher.Aegis128x4 => "aegis128x4",
            _ => throw new ArgumentOutOfRangeException(nameof(cipher), cipher, null)
        };
    }
}
