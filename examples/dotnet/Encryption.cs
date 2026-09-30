/**
 * Local Database Encryption Example
 *
 * This example demonstrates how to use local database encryption
 * with the Turso .NET SDK.
 *
 * Supported ciphers:
 *   - Aes128Gcm
 *   - Aes256Gcm
 *   - Aegis256
 *   - Aegis256x2
 *   - Aegis128l
 *   - Aegis128x2
 *   - Aegis128x4
 */

using System;
using System.IO;
using System.Text;
using Turso;

class EncryptionExample
{
    private const string DB_PATH = "encrypted.db";
    // 32-byte hex key for aegis256 (256 bits = 32 bytes = 64 hex chars)
    private const string ENCRYPTION_KEY =
        "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    public static void Main(string[] args)
    {
        Console.WriteLine("=== Turso Local Encryption Example ===\n");

        // Create an encrypted database
        Console.WriteLine("1. Creating encrypted database...");
        using (var connection = new TursoConnection(
            $"Data Source={DB_PATH};Encryption Cipher=aegis256;Encryption Key={ENCRYPTION_KEY}"))
        {
            connection.Open();

            // Create a table and insert sensitive data
            Console.WriteLine("2. Creating table and inserting data...");
            using (var create = new TursoCommand(connection,
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, ssn TEXT)"))
            {
                create.ExecuteNonQuery();
            }

            using (var insert = new TursoCommand(connection,
                "INSERT INTO users (name, ssn) VALUES ('Alice', '123-45-6789')"))
            {
                insert.ExecuteNonQuery();
            }

            using (var insert = new TursoCommand(connection,
                "INSERT INTO users (name, ssn) VALUES ('Bob', '987-65-4321')"))
            {
                insert.ExecuteNonQuery();
            }

            // Checkpoint to flush data to disk
            using (var checkpoint = new TursoCommand(connection,
                "PRAGMA wal_checkpoint(truncate)"))
            {
                checkpoint.ExecuteNonQuery();
            }

            // Query the data
            Console.WriteLine("3. Querying data...");
            using (var select = new TursoCommand(connection, "SELECT * FROM users"))
            using (var reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    Console.WriteLine($"   User: id={reader.GetInt64(0)}, name={reader.GetString(1)}, ssn={reader.GetString(2)}");
                }
            }
        }

        // Verify the data is encrypted on disk
        Console.WriteLine("\n4. Verifying encryption...");
        var rawContent = File.ReadAllBytes(DB_PATH);
        var contentStr = Encoding.UTF8.GetString(rawContent);
        var containsPlaintext = contentStr.Contains("Alice") || contentStr.Contains("123-45-6789");

        if (containsPlaintext)
        {
            Console.WriteLine("   WARNING: Data appears to be unencrypted!");
        }
        else
        {
            Console.WriteLine("   Data is encrypted on disk (plaintext not found)");
        }

        // Reopen with the same key
        Console.WriteLine("\n5. Reopening database with correct key...");
        using (var connection2 = new TursoConnection(
            $"Data Source={DB_PATH};Encryption Cipher=aegis256;Encryption Key={ENCRYPTION_KEY}"))
        {
            connection2.Open();
            using (var select = new TursoCommand(connection2, "SELECT name FROM users"))
            using (var reader = select.ExecuteReader())
            {
                Console.Write("   Successfully read users: ");
                var names = new System.Collections.Generic.List<string>();
                while (reader.Read())
                {
                    names.Add(reader.GetString(0));
                }
                Console.WriteLine(string.Join(", ", names));
            }
        }

        // Demonstrate that wrong key fails
        Console.WriteLine("\n6. Attempting to open with wrong key (should fail)...");
        try
        {
            using (var connection3 = new TursoConnection(
                $"Data Source={DB_PATH};Encryption Cipher=aegis256;Encryption Key=aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"))
            {
                connection3.Open();
                using (var select = new TursoCommand(connection3, "SELECT * FROM users"))
                using (var reader = select.ExecuteReader())
                {
                    reader.Read();
                    Console.WriteLine("   ERROR: Should have failed with wrong key!");
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"   Correctly failed: {e.Message}");
        }

        // Cleanup
        File.Delete(DB_PATH);
        Console.WriteLine("\n=== Example completed successfully ===");
    }
}
