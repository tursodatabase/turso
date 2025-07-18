import 'package:turso_dart/src/helpers.dart';
import 'package:turso_dart/src/rust/api/statement.dart';
import 'package:turso_dart/src/rust/helpers/params.dart';
import 'package:turso_dart/src/rust/helpers/value.dart';

// This is for internal only
class Statement {
  Statement(this.inner);

  final RustStatement inner;

  Future<void> reset() async {
    inner.reset();
  }

  /// Query the statement, you can provide either named or positional parameters
  ///
  /// # Args
  /// * `named` - Named parameters
  /// * `positional` - Positional parameters
  ///
  /// # Returns
  /// Returns a list of object, eg: [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}]
  Future<List<Map<String, dynamic>>> query({
    Map<String, dynamic>? named,
    List<dynamic>? positional,
  }) async {
    final res = await inner.query(
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
  Future<int> execute({
    Map<String, dynamic>? named,
    List<dynamic>? positional,
  }) async {
    final res = await inner.execute(
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
}
