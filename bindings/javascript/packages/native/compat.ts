import { DatabaseCompat, NativeDatabase, SqliteError, DatabaseOpts, EncryptionCipher, sql, SqlQuery } from "@tursodatabase/database-common"
import { Database as NativeDB, EncryptionCipher as NativeEncryptionCipher } from "#index";

// Map string cipher names to native enum values (lazy to avoid errors if native module lacks encryption)
function getCipherValue(cipher: EncryptionCipher): number {
    if (!NativeEncryptionCipher) {
        throw new Error('Encryption is not supported in this build');
    }
    const cipherMap: Record<EncryptionCipher, number> = {
        'aes128gcm': NativeEncryptionCipher.Aes128Gcm,
        'aes256gcm': NativeEncryptionCipher.Aes256Gcm,
        'aegis256': NativeEncryptionCipher.Aegis256,
        'aegis256x2': NativeEncryptionCipher.Aegis256x2,
        'aegis128l': NativeEncryptionCipher.Aegis128l,
        'aegis128x2': NativeEncryptionCipher.Aegis128x2,
        'aegis128x4': NativeEncryptionCipher.Aegis128x4,
    };
    return cipherMap[cipher];
}

class Database extends DatabaseCompat {
    constructor(path: string, opts: DatabaseOpts = {}) {
        const nativeOpts: any = { ...opts };
        if (opts.encryption) {
            nativeOpts.encryption = {
                cipher: getCipherValue(opts.encryption.cipher),
                hexkey: opts.encryption.hexkey,
            };
        }
        super(new NativeDB(path, nativeOpts) as unknown as NativeDatabase)
    }
}

export { Database, SqliteError, sql, SqlQuery }
