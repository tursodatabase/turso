import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';
import 'package:provider/provider.dart';
import 'package:turso_dart/turso_dart.dart';
import 'package:turso_dart_example/bootstrap.dart';
import 'package:turso_dart_example/features/task/repositories/task_repository.dart';
import 'package:turso_dart_example/features/task/task_list.dart';
import 'package:turso_dart_example/infra/turso_task_repository.dart';

late TursoClient memoryClient;
late TursoClient localClient;

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  final dir = await getApplicationCacheDirectory();
  await dir.delete(recursive: true);
  await dir.create(recursive: true);

  memoryClient = TursoClient.memory();

  localClient = TursoClient.local("${dir.path}/local.db");

  await bootstrapDatabase(memoryClient);
  await bootstrapDatabase(localClient);

  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('Turso Dart Example')),
        body: Padding(
          padding: const EdgeInsets.all(24),
          child: Builder(
            builder: (context) {
              return Center(
                child: Column(
                  spacing: 16,
                  children: [
                    FilledButton(
                      onPressed: () {
                        Navigator.of(context).push(
                          MaterialPageRoute(
                            builder: (context) => Provider<TaskRepository>(
                              create: (context) =>
                                  TursoTaskRepository(memoryClient),
                              child: const TaskList(),
                            ),
                          ),
                        );
                      },
                      child: const Text("Memory"),
                    ),
                    FilledButton(
                      onPressed: () {
                        Navigator.of(context).push(
                          MaterialPageRoute(
                            builder: (context) => Provider<TaskRepository>(
                              create: (context) =>
                                  TursoTaskRepository(localClient),
                              child: const TaskList(),
                            ),
                          ),
                        );
                      },
                      child: const Text("Local"),
                    ),
                  ],
                ),
              );
            },
          ),
        ),
      ),
    );
  }
}
