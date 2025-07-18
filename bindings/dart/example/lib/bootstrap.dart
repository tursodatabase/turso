import 'package:turso_dart/turso_dart.dart';

Future<void> bootstrapDatabase(TursoClient client, {bool sync = false}) async {
  await client.connect();
  await client.execute("drop table if exists tasks");
  await client.execute(
    "create table if not exists tasks (id integer primary key, title text, description text, completed integer)",
  );
}
