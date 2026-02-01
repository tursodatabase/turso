// Local Database Encryption Example
//
// This example demonstrates how to use local database encryption
// with the Turso Go SDK.
//
// Prerequisites: Build the native library first:
//   cargo build -p turso_sync_sdk_kit --release
//
// Run with (macOS):
//   DYLD_LIBRARY_PATH=../../target/release go run encryption.go
//
// Run with (Linux):
//   LD_LIBRARY_PATH=../../target/release go run encryption.go

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	turso "turso.tech/database/tursogo"
	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
)

const (
	dbPath = "encrypted.db"
	// 32-byte hex key for aegis256 (256 bits = 32 bytes = 64 hex chars)
	encryptionKey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"
)

func main() {
	fmt.Println("=== Turso Local Encryption Example (Go) ===\n")

	// Initialize the turso library with the "mixed" strategy
	// which downloads the appropriate native library for your platform
	turso.InitLibrary(turso_libs.LoadTursoLibraryConfig{LoadStrategy: "mixed"})

	// Create an encrypted database
	fmt.Println("1. Creating encrypted database...")
	dsn := fmt.Sprintf("%s?experimental=encryption&encryption_cipher=aegis256&encryption_hexkey=%s", dbPath, encryptionKey)
	db, err := sql.Open("turso", dsn)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Create a table and insert sensitive data
	fmt.Println("2. Creating table and inserting data...")
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, ssn TEXT)")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (name, ssn) VALUES ('Alice', '123-45-6789')")
	if err != nil {
		log.Fatalf("Failed to insert Alice: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (name, ssn) VALUES ('Bob', '987-65-4321')")
	if err != nil {
		log.Fatalf("Failed to insert Bob: %v", err)
	}

	// Checkpoint to flush data to disk
	_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		log.Fatalf("Failed to checkpoint: %v", err)
	}

	// Query the data
	fmt.Println("3. Querying data...")
	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		log.Fatalf("Failed to query users: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name, ssn string
		if err := rows.Scan(&id, &name, &ssn); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("   User: id=%d, name=%s, ssn=%s\n", id, name, ssn)
	}

	db.Close()

	// Verify the data is encrypted on disk
	fmt.Println("\n4. Verifying encryption...")
	rawContent, err := os.ReadFile(dbPath)
	if err != nil {
		log.Fatalf("Failed to read db file: %v", err)
	}

	containsPlaintext := strings.Contains(string(rawContent), "Alice") || strings.Contains(string(rawContent), "123-45-6789")

	if containsPlaintext {
		fmt.Println("   WARNING: Data appears to be unencrypted!")
	} else {
		fmt.Println("   Data is encrypted on disk (plaintext not found)")
	}

	// Reopen with the same key
	fmt.Println("\n5. Reopening database with correct key...")
	db2, err := sql.Open("turso", dsn)
	if err != nil {
		log.Fatalf("Failed to reopen database: %v", err)
	}

	rows2, err := db2.Query("SELECT name FROM users")
	if err != nil {
		log.Fatalf("Failed to query users after reopen: %v", err)
	}
	defer rows2.Close()

	var names []string
	for rows2.Next() {
		var name string
		if err := rows2.Scan(&name); err != nil {
			log.Fatalf("Failed to scan name: %v", err)
		}
		names = append(names, name)
	}
	fmt.Printf("   Successfully read users: %v\n", names)
	db2.Close()

	// Demonstrate that wrong key fails
	fmt.Println("\n6. Attempting to open with wrong key (should fail)...")
	wrongKey := "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"
	wrongDsn := fmt.Sprintf("%s?experimental=encryption&encryption_cipher=aegis256&encryption_hexkey=%s", dbPath, wrongKey)
	db3, err := sql.Open("turso", wrongDsn)
	if err != nil {
		log.Fatalf("Failed to open db with wrong key: %v", err)
	}

	_, err = db3.Query("SELECT * FROM users")
	if err != nil {
		fmt.Printf("   Correctly failed: %v\n", err)
	} else {
		fmt.Println("   ERROR: Should have failed with wrong key!")
	}
	db3.Close()

	// Cleanup
	os.Remove(dbPath)
	fmt.Println("\n=== Example completed successfully ===")
}
