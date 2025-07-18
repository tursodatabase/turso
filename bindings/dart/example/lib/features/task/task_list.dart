import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:turso_dart_example/features/task/blocs/blocs.dart';
import 'package:turso_dart_example/features/task/models/task.dart';
import 'package:turso_dart_example/features/task/repositories/repositories.dart';
import 'package:turso_dart_example/features/task/task_add.dart';

class TaskList extends StatefulWidget {
  const TaskList({super.key, this.syncOnNetworkChange = false});

  final bool syncOnNetworkChange;

  @override
  State<TaskList> createState() => _TaskListState();
}

class _TaskListState extends State<TaskList> {
  @override
  Widget build(BuildContext context) {
    return BlocProvider(
      create: (context) => TaskListCubit(context.read<TaskRepository>()),
      child: SafeArea(
        child: Scaffold(
          appBar: AppBar(title: const Text('Tasks')),
          floatingActionButton: Builder(
            builder: (context) {
              return FloatingActionButton(
                onPressed: () async {
                  final taskData =
                      await showModalBottomSheet<Map<String, dynamic>>(
                        context: context,
                        isScrollControlled: true,
                        builder: (_) => const TaskAdd(),
                      );
                  if (taskData == null || !context.mounted) return;
                  await context.read<TaskListCubit>().addTask(
                    Task(
                      id: -1,
                      title: taskData["title"],
                      description: taskData["description"],
                      completed: false,
                    ),
                  );
                },
                child: const Icon(Icons.add),
              );
            },
          ),
          body: Padding(
            padding: const EdgeInsets.all(24),
            child: BlocBuilder<TaskListCubit, TaskListState>(
              builder: (context, state) {
                return switch (state) {
                  TaskListInitial() => const SizedBox.shrink(),
                  TaskListLoading() => const CircularProgressIndicator(),
                  TaskListLoaded(tasks: final tasks) => ListView.builder(
                    itemCount: tasks.length,
                    itemBuilder: (context, index) {
                      return Dismissible(
                        background: Container(color: Colors.red),
                        key: ValueKey(tasks[index].id),
                        onDismissed: (_) {
                          context.read<TaskListCubit>().deleteTask(
                            tasks[index].id,
                          );
                        },
                        child: CheckboxListTile(
                          value: tasks[index].completed,
                          title: Text(tasks[index].title),
                          subtitle: Text(tasks[index].description),
                          onChanged: tasks[index].completed
                              ? null
                              : (value) {
                                  context
                                      .read<TaskListCubit>()
                                      .markTasksAsCompleted([tasks[index].id]);
                                },
                        ),
                      );
                    },
                  ),
                  TaskListError(message: final message) => Text(message),
                  _ => throw Exception("Invalid state"),
                };
              },
            ),
          ),
        ),
      ),
    );
  }
}

class _TaskAdd extends StatefulWidget {
  const _TaskAdd();

  @override
  State<_TaskAdd> createState() => __TaskAddState();
}

class __TaskAddState extends State<_TaskAdd> {
  @override
  Widget build(BuildContext context) {
    return const Placeholder();
  }
}
