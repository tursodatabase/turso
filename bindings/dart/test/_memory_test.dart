import 'package:flutter_test/flutter_test.dart';
import 'package:turso_dart/src/rust/frb_generated.dart';
import 'package:turso_dart/turso_dart.dart';

void main() async {
  setUpAll(() async {
    final lib = await loadExternalLibrary(
      ExternalLibraryLoaderConfig(
        stem: "turso_dart",
        ioDirectory: "rust/test_build/debug/",
        webPrefix: null,
      ),
    );
    await RustLib.init(externalLibrary: lib);
  });

  test('should be able to perform queries', () async {
    final client = TursoClient.memory();
    await client.connect();

    await client.execute(
      "create table if not exists tasks (id integer primary key, title text, description text, completed integer)",
    );

    final rowsAffected = await client.execute(
      "insert into tasks (title, description, completed) values (?, ?, ?)",
      positional: ["title", "description", 0],
    );
    expect(rowsAffected, equals(1));

    final result = await client.query("select * from tasks");
    expect(
      result,
      equals([
        {
          "id": 1,
          "title": "title",
          "description": "description",
          "completed": 0,
        },
      ]),
    );
  });
}
