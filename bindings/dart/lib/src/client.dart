import 'package:turso_dart/src/helpers.dart';
import 'package:turso_dart/src/rust/api/connection.dart';
import 'package:turso_dart/src/rust/frb_generated.dart';
import 'package:turso_dart/src/rust/helpers/params.dart';
import 'package:turso_dart/src/rust/helpers/value.dart';
import 'package:turso_dart/src/statement.dart';

import 'rust/api/connect.dart' as c;

// Re-export encryption types for public API
export 'rust/api/connect.dart' show EncryptionCipher, EncryptionOpts;

class TursoClient {
  TursoClient(this.url, {this.encryption});

  TursoClient.memory() : url = ':memory:', encryption = null;

  TursoClient.local(this.url, {this.encryption});

  /// Create an encrypted local database client
  ///
  /// # Args
  /// * `url` - Path to the database file
  /// * `cipher` - Encryption cipher to use
  /// * `hexkey` - Hex-encoded encryption key
  TursoClient.encrypted(
    this.url, {
    required c.EncryptionCipher cipher,
    required String hexkey,
  }) : encryption = c.EncryptionOpts(cipher: cipher, hexkey: hexkey);

  final String url;
  final c.EncryptionOpts? encryption;

  RustConnection? _connection;

  /// Connect the database, must be called first after creation
  Future<void> connect() async {
    if (!RustLib.instance.initialized) {
      await RustLib.init();
    }
    _connection = await c.connect(
      args: c.ConnectArgs(url: url, encryption: encryption),
    );
  }

  /// Query the database, you can provide either named or positional parameters
  ///
  /// # Args
  /// * `sql` - SQL query
  /// * `named` - Named parameters
  /// * `positional` - Positional parameters
  ///
  /// # Returns
  /// Returns a list of object, eg: [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}]
  Future<List<Map<String, dynamic>>> query(
    String sql, {
    Map<String, dynamic>? named,
    List<dynamic>? positional,
  }) async {
    if (_connection == null) throw Exception('Database is not connected');
    final res = await _connection!.query(
      sql: sql,
      params: named != null
          ? Params.named(
              named.entries
                  .map<(String, Value)>(
                    (entry) => (entry.key, toValue(entry.value)),
                  )
                  .toList(),
            )
          : positional != null
          ? Params.positional(positional.map(toValue).toList())
          : const Params.none(),
    );
    return res.rows
        .map(
          (row) => Map.fromEntries(
            row.entries.map(
              (entry) => MapEntry(
                entry.key,
                entry.value.mapOrNull(
                  integer: (integer) => integer.field0,
                  real: (real) => real.field0,
                  text: (text) => text.field0,
                  blob: (blob) => blob.field0,
                  null_: (_) => null,
                ),
              ),
            ),
          ),
        )
        .toList();
  }

  /// Execute the statement, you can provide either named or positional parameters
  ///
  /// # Args
  /// * `sql` - SQL query
  /// * `named` - Named parameters
  /// * `positional` - Positional parameters
  ///
  /// # Returns
  /// Number of rows affected by the statement
  Future<int> execute(
    String sql, {
    Map<String, dynamic>? named,
    List<dynamic>? positional,
  }) async {
    if (_connection == null) throw Exception('Database is not connected');
    final res = await _connection!.execute(
      sql: sql,
      params: named != null
          ? Params.named(
              named.entries
                  .map<(String, Value)>(
                    (entry) => (entry.key, toValue(entry.value)),
                  )
                  .toList(),
            )
          : positional != null
          ? Params.positional(positional.map(toValue).toList())
          : const Params.none(),
    );
    return res.rowsAffected.toInt();
  }

  /// Create a prepared statement
  ///
  /// # Args
  /// * `sql` - SQL query
  ///
  /// # Returns
  /// Statement object
  Future<Statement> prepare(String sql) async {
    if (_connection == null) throw Exception('Database is not connected');
    final inner = await _connection!.prepare(sql: sql);
    return Statement(inner);
  }

  /// Close the database
  Future<void> dispose() async {
    _connection?.dispose();
  }
}
