/// Dart bindings for Turso, an in-process SQL database written in Rust.
///
/// The raw FFI surface in `src/bindings.g.dart` is generated from
/// `bindings/c/include/sqlite3.h` by package:ffigen, and binds to the code
/// asset that `hook/build.dart` produces. Dart and Flutter bundle that library
/// automatically, so there is nothing to load or ship by hand.
///
/// This library is native-only. `dart:ffi` does not exist on the web, so the
/// package cannot be used in a browser; see the README.
library;

import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'src/bindings.g.dart';

export 'src/bindings.g.dart';

/// Tells the C API to copy bound text/blob data instead of borrowing the
/// caller's buffer. We free our buffers as soon as the bind returns, so
/// borrowing would leave the statement pointing at freed memory.
final Pointer<Void> _sqliteTransient = Pointer<Void>.fromAddress(-1);

/// An error reported by the database engine.
class TursoException implements Exception {
  TursoException(this.message, this.code);

  final String message;

  /// The `SQLITE_*` result code behind the failure.
  final int code;

  @override
  String toString() => 'TursoException($code): $message';
}

bool _initialized = false;

/// One row of a result set. Values are read by column index or column name.
class Row {
  Row(this._names, this.values);

  final List<String> _names;

  /// Column values in declaration order.
  final List<Object?> values;

  List<String> get columnNames => _names;

  /// Reads a column by index (`int`) or by name (`String`).
  Object? operator [](Object key) {
    if (key is int) return values[key];
    final index = _names.indexOf(key as String);
    if (index < 0) throw ArgumentError.value(key, 'key', 'No such column');
    return values[index];
  }

  /// Collapses the row into a map. Duplicate column names keep the last value.
  Map<String, Object?> toMap() => Map.fromIterables(_names, values);

  @override
  String toString() => toMap().toString();
}

/// A connection to a Turso database.
class Database {
  Database._(this._handle);

  Pointer<sqlite3> _handle;

  /// Statements still alive on this connection.
  ///
  /// The C layer frees the connection without finalizing them, and a later
  /// finalize then reads through the freed handle, so we finalize them here
  /// before closing rather than leaving the crash to the caller.
  final Set<Statement> _statements = Set.identity();

  /// Opens the database at [path], creating it if needed.
  factory Database.open(String path) {
    if (!_initialized) {
      sqlite3_initialize();
      _initialized = true;
    }
    final out = calloc<Pointer<sqlite3>>();
    final cPath = path.toNativeUtf8();
    try {
      final rc = sqlite3_open(cPath.cast(), out);
      if (rc != SQLITE_OK) {
        throw TursoException('Failed to open database at $path', rc);
      }
      return Database._(out.value);
    } finally {
      calloc.free(cPath);
      calloc.free(out);
    }
  }

  /// Opens a private, temporary in-memory database.
  factory Database.memory() => Database.open(':memory:');

  bool get isClosed => _handle == nullptr;

  /// Compiles [sql] into a reusable statement. The caller must dispose it.
  Statement prepare(String sql) {
    _checkOpen();
    final out = calloc<Pointer<sqlite3_stmt>>();
    final cSql = sql.toNativeUtf8();
    try {
      final rc = sqlite3_prepare_v2(_handle, cSql.cast(), -1, out, nullptr);
      if (rc != SQLITE_OK) throw _error(rc);
      if (out.value == nullptr) {
        throw TursoException('Statement has no SQL to run: $sql', SQLITE_ERROR);
      }
      final statement = Statement._(this, out.value);
      _statements.add(statement);
      return statement;
    } finally {
      calloc.free(cSql);
      calloc.free(out);
    }
  }

  /// Runs [sql] and discards any rows it produces.
  void execute(String sql, [List<Object?> parameters = const []]) {
    final statement = prepare(sql);
    try {
      statement.execute(parameters);
    } finally {
      statement.dispose();
    }
  }

  /// Runs [sql] and collects every row it produces.
  List<Row> query(String sql, [List<Object?> parameters = const []]) {
    final statement = prepare(sql);
    try {
      return statement.query(parameters);
    } finally {
      statement.dispose();
    }
  }

  /// The rowid of the most recent successful insert on this connection.
  int get lastInsertRowId {
    _checkOpen();
    return sqlite3_last_insert_rowid(_handle);
  }

  /// Rows changed by the most recently completed statement.
  int get changes {
    _checkOpen();
    return sqlite3_changes(_handle);
  }

  /// Closes the connection, releasing any statements left open on it.
  ///
  /// Calling it twice is harmless.
  void close() {
    if (_handle == nullptr) return;
    for (final statement in _statements.toList()) {
      statement.dispose();
    }
    final rc = sqlite3_close(_handle);
    _handle = nullptr;
    if (rc != SQLITE_OK) {
      throw TursoException('Failed to close database', rc);
    }
  }

  void _checkOpen() {
    if (_handle == nullptr) {
      throw StateError('Database is already closed');
    }
  }

  TursoException _error(int code) {
    final message = sqlite3_errmsg(_handle);
    return TursoException(
      message == nullptr ? 'error $code' : message.cast<Utf8>().toDartString(),
      code,
    );
  }
}

/// A compiled SQL statement.
class Statement {
  Statement._(this._db, this._handle);

  final Database _db;
  Pointer<sqlite3_stmt> _handle;

  bool get isDisposed => _handle == nullptr;

  /// Names of the columns this statement returns.
  List<String> get columnNames {
    _checkAlive();
    return [
      for (var i = 0; i < sqlite3_column_count(_handle); i++)
        sqlite3_column_name(_handle, i).cast<Utf8>().toDartString(),
    ];
  }

  /// Runs the statement to completion, discarding any rows.
  void execute([List<Object?> parameters = const []]) {
    _restart(parameters);
    while (true) {
      final rc = sqlite3_step(_handle);
      if (rc == SQLITE_DONE) return;
      if (rc != SQLITE_ROW) throw _db._error(rc);
    }
  }

  /// Runs the statement and collects every row it produces.
  List<Row> query([List<Object?> parameters = const []]) {
    _restart(parameters);
    final names = columnNames;
    final rows = <Row>[];
    while (true) {
      final rc = sqlite3_step(_handle);
      if (rc == SQLITE_DONE) return rows;
      if (rc != SQLITE_ROW) throw _db._error(rc);
      rows.add(Row(names, [
        for (var i = 0; i < names.length; i++) _readColumn(i),
      ]));
    }
  }

  /// Releases the statement. Calling it twice is harmless.
  void dispose() {
    if (_handle == nullptr) return;
    sqlite3_finalize(_handle);
    _handle = nullptr;
    _db._statements.remove(this);
  }

  void _restart(List<Object?> parameters) {
    _checkAlive();
    sqlite3_reset(_handle);
    sqlite3_clear_bindings(_handle);

    final expected = sqlite3_bind_parameter_count(_handle);
    if (parameters.length != expected) {
      throw ArgumentError(
        'Statement wants $expected parameters, got ${parameters.length}',
      );
    }
    for (var i = 0; i < parameters.length; i++) {
      _bind(i + 1, parameters[i]);
    }
  }

  void _bind(int index, Object? value) {
    final int rc;
    switch (value) {
      case null:
        rc = sqlite3_bind_null(_handle, index);
      case bool b:
        rc = sqlite3_bind_int64(_handle, index, b ? 1 : 0);
      case int i:
        rc = sqlite3_bind_int64(_handle, index, i);
      case double d:
        rc = sqlite3_bind_double(_handle, index, d);
      case String s:
        final cText = s.toNativeUtf8();
        try {
          rc = sqlite3_bind_text(
            _handle,
            index,
            cText.cast(),
            cText.length,
            _sqliteTransient,
          );
        } finally {
          calloc.free(cText);
        }
      case List<int> bytes:
        final buffer = calloc<Uint8>(bytes.length == 0 ? 1 : bytes.length);
        try {
          buffer.asTypedList(bytes.length).setAll(0, bytes);
          rc = sqlite3_bind_blob(
            _handle,
            index,
            buffer.cast(),
            bytes.length,
            _sqliteTransient,
          );
        } finally {
          calloc.free(buffer);
        }
      default:
        throw ArgumentError.value(
          value,
          'parameter $index',
          'Cannot bind ${value.runtimeType}',
        );
    }
    if (rc != SQLITE_OK) throw _db._error(rc);
  }

  Object? _readColumn(int index) {
    switch (sqlite3_column_type(_handle, index)) {
      case SQLITE_NULL:
        return null;
      case SQLITE_INTEGER:
        return sqlite3_column_int64(_handle, index);
      case SQLITE_FLOAT:
        return sqlite3_column_double(_handle, index);
      case SQLITE_BLOB:
        final bytes = sqlite3_column_bytes(_handle, index);
        final data = sqlite3_column_blob(_handle, index);
        if (data == nullptr || bytes == 0) return Uint8List(0);
        // Copy: the engine owns this buffer until the next step or reset.
        return Uint8List.fromList(data.cast<Uint8>().asTypedList(bytes));
      default:
        final text = sqlite3_column_text(_handle, index);
        return text == nullptr ? null : text.cast<Utf8>().toDartString();
    }
  }

  void _checkAlive() {
    if (_handle == nullptr) throw StateError('Statement is already disposed');
    _db._checkOpen();
  }
}
