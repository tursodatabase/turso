import 'dart:io';

import 'package:path_provider/path_provider.dart';
import 'package:turso/turso.dart';

/// A single todo.
class Todo {
  const Todo({required this.id, required this.title, required this.done});

  final int id;
  final String title;
  final bool done;
}

/// Stores todos in a Turso database file.
///
/// Every method here is a plain synchronous call into the native library. Only
/// [open] is async, because finding the app's documents directory is.
class TodoStore {
  TodoStore._(this._db, this.path);

  final Database _db;

  /// Where the database file lives, shown in the app's about dialog.
  final String path;

  static Future<TodoStore> open() async {
    final directory = await getApplicationDocumentsDirectory();
    await Directory(directory.path).create(recursive: true);
    final path = '${directory.path}/todos.db';
    return TodoStore.openAt(path);
  }

  /// Opens a store at an explicit path. Handy for tests.
  factory TodoStore.openAt(String path) {
    final db = Database.open(path);
    db.execute('''
      CREATE TABLE IF NOT EXISTS todos (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        done INTEGER NOT NULL DEFAULT 0
      )
    ''');
    return TodoStore._(db, path);
  }

  List<Todo> all() => [
    for (final row in _db.query(
      'SELECT id, title, done FROM todos ORDER BY done, id DESC',
    ))
      Todo(
        id: row['id'] as int,
        title: row['title'] as String,
        done: row['done'] as int != 0,
      ),
  ];

  /// Adds a todo and returns it, so callers do not have to re-read the table.
  Todo add(String title) {
    _db.execute('INSERT INTO todos (title) VALUES (?)', [title]);
    return Todo(id: _db.lastInsertRowId, title: title, done: false);
  }

  void setDone(int id, {required bool done}) =>
      _db.execute('UPDATE todos SET done = ? WHERE id = ?', [done, id]);

  void remove(int id) => _db.execute('DELETE FROM todos WHERE id = ?', [id]);

  void clearCompleted() => _db.execute('DELETE FROM todos WHERE done != 0');

  void close() => _db.close();
}
