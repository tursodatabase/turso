import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:turso/turso.dart';

void main() {
  late Database db;

  setUp(() => db = Database.memory());
  tearDown(() => db.close());

  test('runs a query that needs no table', () {
    final rows = db.query('SELECT 1 + 1 AS total');
    expect(rows, hasLength(1));
    expect(rows.single['total'], 2);
    expect(rows.single[0], 2);
  });

  test('creates a table, inserts, and reads back', () {
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute('INSERT INTO users (name) VALUES (?)', ['alice']);
    db.execute('INSERT INTO users (name) VALUES (?)', ['bob']);

    final rows = db.query('SELECT id, name FROM users ORDER BY id');
    expect(rows.map((row) => row['name']), ['alice', 'bob']);
    expect(rows.first['id'], 1);
  });

  test('round-trips every value type', () {
    db.execute('CREATE TABLE t (i INTEGER, f REAL, s TEXT, b BLOB, n TEXT)');
    final blob = Uint8List.fromList([0, 1, 2, 253, 254, 255]);
    db.execute('INSERT INTO t VALUES (?, ?, ?, ?, ?)', [
      -9007199254740993,
      3.5,
      'héllo 🌍',
      blob,
      null,
    ]);

    final row = db.query('SELECT i, f, s, b, n FROM t').single;
    expect(row['i'], -9007199254740993);
    expect(row['f'], 3.5);
    expect(row['s'], 'héllo 🌍');
    expect(row['b'], blob);
    expect(row['n'], isNull);
  });

  test('binds booleans as integers', () {
    db.execute('CREATE TABLE flags (on_ INTEGER)');
    db.execute('INSERT INTO flags VALUES (?)', [true]);
    expect(db.query('SELECT on_ FROM flags').single['on_'], 1);
  });

  test('reuses a prepared statement across parameter sets', () {
    db.execute('CREATE TABLE nums (v INTEGER)');
    final insert = db.prepare('INSERT INTO nums VALUES (?)');
    try {
      for (var i = 0; i < 5; i++) {
        insert.execute([i]);
      }
    } finally {
      insert.dispose();
    }

    final select = db.prepare('SELECT v FROM nums WHERE v >= ? ORDER BY v');
    try {
      expect(select.query([3]).map((row) => row['v']), [3, 4]);
      // Re-running must clear the previous bindings, not accumulate them.
      expect(select.query([0]).map((row) => row['v']), [0, 1, 2, 3, 4]);
    } finally {
      select.dispose();
    }
  });

  test('reports column names', () {
    final statement = db.prepare('SELECT 1 AS a, 2 AS b');
    addTearDown(statement.dispose);
    expect(statement.columnNames, ['a', 'b']);
  });

  test('tracks lastInsertRowId and changes', () {
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)');
    db.execute('INSERT INTO t (v) VALUES (10)');
    expect(db.lastInsertRowId, 1);
    db.execute('INSERT INTO t (v) VALUES (20)');
    expect(db.lastInsertRowId, 2);

    db.execute('UPDATE t SET v = v + 1');
    expect(db.changes, 2);
  });

  test('commits and rolls back transactions', () {
    db.execute('CREATE TABLE t (v INTEGER)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('COMMIT');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('ROLLBACK');

    expect(db.query('SELECT v FROM t').map((row) => row['v']), [1]);
  });

  test('reports a syntax error instead of crashing', () {
    expect(
      () => db.query('SELECT FROM WHERE'),
      throwsA(isA<TursoException>()),
    );
  });

  test('reports a constraint violation', () {
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    expect(
      () => db.execute('INSERT INTO t VALUES (1)'),
      throwsA(isA<TursoException>()),
    );
  });

  test('rejects the wrong number of parameters', () {
    expect(
      () => db.query('SELECT ?, ?', [1]),
      throwsA(isA<ArgumentError>()),
    );
  });

  test('rejects a value type it cannot bind', () {
    expect(
      () => db.query('SELECT ?', [DateTime.now()]),
      throwsA(isA<ArgumentError>()),
    );
  });

  test('rejects an unknown column name', () {
    final row = db.query('SELECT 1 AS a').single;
    expect(() => row['nope'], throwsA(isA<ArgumentError>()));
  });

  test('closing releases statements left open', () {
    final other = Database.memory();
    other.execute('CREATE TABLE t (v INTEGER)');
    final statement = other.prepare('SELECT v FROM t');
    expect(statement.query(), isEmpty);

    // The C layer frees the connection without finalizing its statements, and
    // finalizing afterwards reads through the freed handle. Closing has to
    // release the stray statement or this segfaults.
    other.close();
    expect(statement.isDisposed, isTrue);
  });

  test('rejects use after close or dispose', () {
    final closed = Database.memory();
    final statement = closed.prepare('SELECT 1');
    closed.close();
    expect(closed.isClosed, isTrue);
    expect(() => closed.query('SELECT 1'), throwsStateError);
    expect(() => statement.query(), throwsStateError);

    // Both are safe to call twice.
    closed.close();
    statement.dispose();
  });

  test('persists data to a file across connections', () {
    final dir = Directory.systemTemp.createTempSync('turso_dart_test');
    addTearDown(() => dir.deleteSync(recursive: true));
    final path = '${dir.path}/test.db';

    final writer = Database.open(path);
    writer.execute('CREATE TABLE t (v TEXT)');
    writer.execute('INSERT INTO t VALUES (?)', ['persisted']);
    writer.close();

    final reader = Database.open(path);
    addTearDown(reader.close);
    expect(reader.query('SELECT v FROM t').single['v'], 'persisted');
  });
}
