import 'dart:io';
import 'package:flutter_test/flutter_test.dart';
import 'package:turso_dart/src/rust/frb_generated.dart';
import 'package:turso_dart/turso_dart.dart';

const _hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';

void main() async {
  setUpAll(() async {
    final lib = await loadExternalLibrary(ExternalLibraryLoaderConfig(
      stem: "turso_dart",
      ioDirectory: "rust/test_build/debug/",
      webPrefix: null,
    ));
    await RustLib.init(externalLibrary: lib);
  });

  test('encryption enabled', () async {
    final dbPath = 'test_enc_${DateTime.now().millisecondsSinceEpoch}.db';
    try {
      final client = TursoClient.encrypted(dbPath, cipher: EncryptionCipher.aegis256, hexkey: _hexkey);
      await client.connect();
      await client.execute('CREATE TABLE t(x)');
      await client.execute("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)");
      await client.query('PRAGMA wal_checkpoint(truncate)');
      await client.dispose();

      final content = await File(dbPath).readAsBytes();
      expect(content.length, greaterThan(16 * 1024));
      expect(String.fromCharCodes(content).contains('secret'), isFalse, reason: 'plaintext found in encrypted file');
    } finally {
      await _cleanup(dbPath);
    }
  });

  test('encryption disabled', () async {
    final dbPath = 'test_plain_${DateTime.now().millisecondsSinceEpoch}.db';
    try {
      final client = TursoClient.local(dbPath);
      await client.connect();
      await client.execute('CREATE TABLE t(x)');
      await client.execute("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)");
      await client.query('PRAGMA wal_checkpoint(truncate)');
      await client.dispose();

      final content = await File(dbPath).readAsBytes();
      expect(content.length, greaterThan(16 * 1024));
      expect(String.fromCharCodes(content).contains('secret'), isTrue);
    } finally {
      await _cleanup(dbPath);
    }
  });

  test('encryption reopen and wrong key', () async {
    final dbPath = 'test_reopen_${DateTime.now().millisecondsSinceEpoch}.db';
    const wrongKey = 'aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';
    try {
      // Create and populate
      final client1 = TursoClient.encrypted(dbPath, cipher: EncryptionCipher.aegis256, hexkey: _hexkey);
      await client1.connect();
      await client1.execute('CREATE TABLE t(x)');
      await client1.execute("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)");
      await client1.query('PRAGMA wal_checkpoint(truncate)');
      await client1.dispose();

      // Reopen with correct key
      final client2 = TursoClient.encrypted(dbPath, cipher: EncryptionCipher.aegis256, hexkey: _hexkey);
      await client2.connect();
      final result = await client2.query('SELECT count(*) as cnt FROM t');
      expect(result[0]['cnt'], equals(1024));
      await client2.dispose();

      // Wrong key should fail
      expect(() async {
        final client3 = TursoClient.encrypted(dbPath, cipher: EncryptionCipher.aegis256, hexkey: wrongKey);
        await client3.connect();
        await client3.query('SELECT * FROM t');
      }, throwsException);
    } finally {
      await _cleanup(dbPath);
    }
  });
}

Future<void> _cleanup(String dbPath) async {
  for (final suffix in ['', '-wal', '-shm']) {
    final f = File('$dbPath$suffix');
    if (await f.exists()) await f.delete();
  }
}
