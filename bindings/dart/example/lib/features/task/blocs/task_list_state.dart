import 'package:turso_dart_example/features/task/models/models.dart';

abstract class TaskListState {}

class TaskListInitial extends TaskListState {}

class TaskListLoading extends TaskListState {}

class TaskListLoaded extends TaskListState {
  final List<Task> tasks;
  TaskListLoaded(this.tasks);
}

class TaskListError extends TaskListState {
  final String message;
  TaskListError(this.message);
}
