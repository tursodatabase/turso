import 'dart:async';

import 'package:path_provider/path_provider.dart';
import 'package:turso_dart/turso_dart.dart';
import 'package:turso_dart_example/features/task/models/task.dart';
import 'package:turso_dart_example/features/task/repositories/repositories.dart';
import 'package:watcher/watcher.dart';

class TursoTaskRepository extends TaskRepository {
  final TursoClient _client;

  TursoTaskRepository(this._client);

  @override
  Future<void> addTask(Task task) async {
    await _client.execute(
      "insert into tasks (title, description, completed) values (?, ?, ?)",
      positional: [task.title, task.description, task.completed ? 1 : 0],
    );
  }

  @override
  Future<void> deleteTask(int id) async {
    final statement = await _client.prepare("delete from tasks where id = :id");
    await statement.execute(named: {":id": id});
  }

  @override
  Future<List<Task>> getTasks() async {
    return _client
        .query("select * from tasks")
        .then(
          (value) => value
              .map(
                (row) => Task(
                  id: row["id"],
                  title: row["title"],
                  description: row["description"],
                  completed: row["completed"] == 1,
                ),
              )
              .toList(),
        );
  }

  @override
  Future<void> markTasksAsCompleted(List<int> ids) async {
    await _client.execute(
      "update tasks set completed = 1 where id in (${ids.join(",")})",
    );
  }

  @override
  Future<Stream?> replicaChanges() async {
    if (_client.url == ":memory:") return null;
    final dir = await getApplicationCacheDirectory();
    return DirectoryWatcher(dir.path).events;
  }
}
