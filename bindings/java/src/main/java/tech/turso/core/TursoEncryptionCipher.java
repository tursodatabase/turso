package tech.turso.core;

/**
 * Supported encryption ciphers for local database encryption.
 */
public enum TursoEncryptionCipher {
    /** AES-128-GCM cipher */
    AES_128_GCM("aes128gcm"),
    /** AES-256-GCM cipher */
    AES_256_GCM("aes256gcm"),
    /** AEGIS-256 cipher */
    AEGIS_256("aegis256"),
    /** AEGIS-256X2 cipher */
    AEGIS_256X2("aegis256x2"),
    /** AEGIS-128L cipher */
    AEGIS_128L("aegis128l"),
    /** AEGIS-128X2 cipher */
    AEGIS_128X2("aegis128x2"),
    /** AEGIS-128X4 cipher */
    AEGIS_128X4("aegis128x4");

    private final String value;

    TursoEncryptionCipher(String value) {
        this.value = value;
    }

    /**
     * Returns the string value of the cipher for internal use.
     */
    public String getValue() {
        return value;
    }
}
