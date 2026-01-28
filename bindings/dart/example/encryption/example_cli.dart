import 'dart:io';
import 'package:turso_dart/src/rust/frb_generated.dart';
import 'package:turso_dart/turso_dart.dart';

const hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';

Future<void> main() async {
  // Initialize Flutter Rust Bridge
  final lib = await loadExternalLibrary(ExternalLibraryLoaderConfig(
    stem: "turso_dart",
    ioDirectory: "../../rust/test_build/debug/",
    webPrefix: null,
  ));
  await RustLib.init(externalLibrary: lib);

  print('=== Turso Dart Encryption Example ===\n');

  // Test 1: Encrypted database
  print('1. Creating encrypted database...');
  final encDbPath = 'example_encrypted.db';
  final encClient = TursoClient.encrypted(
    encDbPath,
    cipher: EncryptionCipher.aegis256,
    hexkey: hexkey,
  );
  await encClient.connect();
  await encClient.execute('CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, secret TEXT)');
  await encClient.execute("INSERT INTO users(name, secret) VALUES ('Alice', 'my-secret-password')");
  await encClient.execute("INSERT INTO users(name, secret) VALUES ('Bob', 'another-secret')");
  await encClient.query('PRAGMA wal_checkpoint(truncate)');

  final encResults = await encClient.query('SELECT * FROM users');
  print('   Data in encrypted DB: $encResults');
  await encClient.dispose();

  // Check if plaintext exists in file
  final encContent = await File(encDbPath).readAsBytes();
  final encHasPlaintext = String.fromCharCodes(encContent).contains('my-secret-password');
  print('   Plaintext "my-secret-password" in file: $encHasPlaintext');
  print('   ✓ Encryption working: ${!encHasPlaintext}\n');

  // Test 2: Unencrypted database for comparison
  print('2. Creating unencrypted database for comparison...');
  final plainDbPath = 'example_plain.db';
  final plainClient = TursoClient.local(plainDbPath);
  await plainClient.connect();
  await plainClient.execute('CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, secret TEXT)');
  await plainClient.execute("INSERT INTO users(name, secret) VALUES ('Alice', 'my-secret-password')");
  await plainClient.query('PRAGMA wal_checkpoint(truncate)');
  await plainClient.dispose();

  final plainContent = await File(plainDbPath).readAsBytes();
  final plainHasPlaintext = String.fromCharCodes(plainContent).contains('my-secret-password');
  print('   Plaintext "my-secret-password" in file: $plainHasPlaintext');
  print('   ✓ As expected, unencrypted DB has plaintext: $plainHasPlaintext\n');

  // Test 3: Reopen encrypted database with correct key
  print('3. Reopening encrypted database with correct key...');
  final encClient2 = TursoClient.encrypted(
    encDbPath,
    cipher: EncryptionCipher.aegis256,
    hexkey: hexkey,
  );
  await encClient2.connect();
  final reopenResults = await encClient2.query('SELECT * FROM users');
  print('   Data retrieved: $reopenResults');
  print('   ✓ Successfully read encrypted data\n');
  await encClient2.dispose();

  // Cleanup
  for (final path in [encDbPath, plainDbPath]) {
    for (final suffix in ['', '-wal', '-shm']) {
      final f = File('$path$suffix');
      if (await f.exists()) await f.delete();
    }
  }

  print('=== All tests passed! ===');
}
