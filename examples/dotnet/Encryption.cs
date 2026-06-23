/**
 * Local Database Encryption Example
 *
 * This example demonstrates how to use local database encryption
 * with the Turso .NET SDK.
 *
 * Supported ciphers include:
 *   - aes128gcm
 *   - aes256gcm
 *   - aegis256
 *   - aegis256x2
 *   - aegis128l
 *   - aegis128x2
 *   - aegis128x4
 */

using System.Text;
using Turso;

const string DatabasePath = "encrypted.db";

// 32-byte hex key for aegis256 (256 bits = 32 bytes = 64 hex chars).
const string EncryptionKey =
    "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

Console.WriteLine("=== Turso Local Encryption Example ===");

DeleteDatabaseFiles(DatabasePath);

Console.WriteLine("\n1. Creating encrypted database...");
using (var connection = OpenEncryptedConnection(DatabasePath, EncryptionKey))
{
    using (var create = new TursoCommand(
        connection,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, ssn TEXT NOT NULL)"))
    {
        create.ExecuteNonQuery();
    }

    using (var insert = new TursoCommand(
        connection,
        "INSERT INTO users (name, ssn) VALUES ('Alice', '123-45-6789'), ('Bob', '987-65-4321')"))
    {
        insert.ExecuteNonQuery();
    }

    using (var checkpoint = new TursoCommand(connection, "PRAGMA wal_checkpoint(truncate)"))
    {
        checkpoint.ExecuteNonQuery();
    }

    Console.WriteLine("2. Querying encrypted database...");
    using (var select = new TursoCommand(connection, "SELECT id, name, ssn FROM users ORDER BY id"))
    using (var reader = select.ExecuteReader())
    {
        while (reader.Read())
        {
            Console.WriteLine(
                $"   User: id={reader.GetInt64(0)}, name={reader.GetString(1)}, ssn={reader.GetString(2)}");
        }
    }
}

Console.WriteLine("\n3. Verifying encryption on disk...");
var rawContent = File.ReadAllBytes(DatabasePath);
var content = Encoding.UTF8.GetString(rawContent);
if (content.Contains("Alice", StringComparison.Ordinal) ||
    content.Contains("123-45-6789", StringComparison.Ordinal))
{
    throw new InvalidOperationException("Plaintext data was found in the encrypted database file.");
}

Console.WriteLine("   Plaintext values were not found in the database file.");

Console.WriteLine("\n4. Reopening database with the correct key...");
using (var connection = OpenEncryptedConnection(DatabasePath, EncryptionKey))
using (var select = new TursoCommand(connection, "SELECT group_concat(name, ', ') FROM users"))
using (var reader = select.ExecuteReader())
{
    reader.Read();
    Console.WriteLine($"   Successfully read users: {reader.GetString(0)}");
}

Console.WriteLine("\n5. Attempting to read with the wrong key...");
var wrongKeySucceeded = false;
try
{
    using var connection = OpenEncryptedConnection(
        DatabasePath,
        "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327");
    using var select = new TursoCommand(connection, "SELECT count(*) FROM users");
    select.ExecuteScalar();

    wrongKeySucceeded = true;
}
catch (Exception ex)
{
    Console.WriteLine($"   Correctly failed: {ex.Message}");
}

if (wrongKeySucceeded)
{
    throw new InvalidOperationException("Reading with the wrong encryption key unexpectedly succeeded.");
}

DeleteDatabaseFiles(DatabasePath);

Console.WriteLine("\n=== Example completed successfully ===");

static TursoConnection OpenEncryptedConnection(string path, string key)
{
    var connection = new TursoConnection(
        $"Data Source={path};Encryption Cipher=aegis256;Encryption Key={key}");
    connection.Open();
    return connection;
}

static void DeleteDatabaseFiles(string path)
{
    File.Delete(path);
    File.Delete(path + "-wal");
    File.Delete(path + "-shm");
}
