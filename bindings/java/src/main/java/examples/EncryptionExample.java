package examples;

import tech.turso.core.TursoConnection;
import tech.turso.core.TursoDB;
import tech.turso.core.TursoEncryptionCipher;
import tech.turso.core.TursoResultSet;
import tech.turso.core.TursoStatement;

import java.io.File;
import java.nio.file.Files;

/**
 * Local Database Encryption Example
 *
 * This example demonstrates how to use local database encryption
 * with the Turso Java SDK.
 *
 * Supported ciphers:
 *   - AES_128_GCM
 *   - AES_256_GCM
 *   - AEGIS_256
 *   - AEGIS_256X2
 *   - AEGIS_128L
 *   - AEGIS_128X2
 *   - AEGIS_128X4
 */
public class EncryptionExample {
    private static final String DB_PATH = "encrypted.db";
    // 32-byte hex key for aegis256 (256 bits = 32 bytes = 64 hex chars)
    private static final String ENCRYPTION_KEY =
        "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    public static void main(String[] args) throws Exception {
        System.out.println("=== Turso Local Encryption Example ===\n");

        // Create an encrypted database
        System.out.println("1. Creating encrypted database...");
        TursoDB db = TursoDB.createWithEncryption(
            "jdbc:turso:" + DB_PATH,
            DB_PATH,
            TursoEncryptionCipher.AEGIS_256,
            ENCRYPTION_KEY
        );
        TursoConnection conn = new TursoConnection("jdbc:turso:" + DB_PATH, db);

        // Create a table and insert sensitive data
        System.out.println("2. Creating table and inserting data...");
        TursoStatement stmt = conn.prepare("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, ssn TEXT)");
        stmt.execute();
        stmt.close();

        stmt = conn.prepare("INSERT INTO users (name, ssn) VALUES ('Alice', '123-45-6789')");
        stmt.execute();
        stmt.close();

        stmt = conn.prepare("INSERT INTO users (name, ssn) VALUES ('Bob', '987-65-4321')");
        stmt.execute();
        stmt.close();

        // Checkpoint to flush data to disk
        stmt = conn.prepare("PRAGMA wal_checkpoint(truncate)");
        stmt.execute();
        stmt.close();

        // Query the data
        System.out.println("3. Querying data...");
        stmt = conn.prepare("SELECT * FROM users");
        stmt.execute();
        TursoResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            System.out.println("   User: id=" + rs.get(1) +
                             ", name=" + rs.get(2) +
                             ", ssn=" + rs.get(3));
        }
        stmt.close();
        conn.close();
        db.close();

        // Verify the data is encrypted on disk
        System.out.println("\n4. Verifying encryption...");
        byte[] rawContent = Files.readAllBytes(new File(DB_PATH).toPath());
        String contentStr = new String(rawContent);
        boolean containsPlaintext = contentStr.contains("Alice") || contentStr.contains("123-45-6789");

        if (containsPlaintext) {
            System.out.println("   WARNING: Data appears to be unencrypted!");
        } else {
            System.out.println("   Data is encrypted on disk (plaintext not found)");
        }

        // Reopen with the same key
        System.out.println("\n5. Reopening database with correct key...");
        TursoDB db2 = TursoDB.createWithEncryption(
            "jdbc:turso:" + DB_PATH,
            DB_PATH,
            TursoEncryptionCipher.AEGIS_256,
            ENCRYPTION_KEY
        );
        TursoConnection conn2 = new TursoConnection("jdbc:turso:" + DB_PATH, db2);
        TursoStatement stmt2 = conn2.prepare("SELECT name FROM users");
        stmt2.execute();
        TursoResultSet rs2 = stmt2.getResultSet();
        System.out.print("   Successfully read users: ");
        while (rs2.next()) {
            System.out.print(rs2.get(1) + " ");
        }
        System.out.println();
        stmt2.close();
        conn2.close();
        db2.close();

        // Demonstrate that wrong key fails
        System.out.println("\n6. Attempting to open with wrong key (should fail)...");
        try {
            TursoDB db3 = TursoDB.createWithEncryption(
                "jdbc:turso:" + DB_PATH,
                DB_PATH,
                TursoEncryptionCipher.AEGIS_256,
                "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"
            );
            TursoConnection conn3 = new TursoConnection("jdbc:turso:" + DB_PATH, db3);
            TursoStatement stmt3 = conn3.prepare("SELECT * FROM users");
            stmt3.execute();
            System.out.println("   ERROR: Should have failed with wrong key!");
        } catch (Exception e) {
            System.out.println("   Correctly failed: " + e.getMessage());
        }

        // Cleanup
        new File(DB_PATH).delete();
        System.out.println("\n=== Example completed successfully ===");
    }
}
