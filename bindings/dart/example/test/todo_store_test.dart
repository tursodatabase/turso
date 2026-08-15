import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:turso_todo/todo_store.dart';

void main() {
  late Directory dir;
  late TodoStore store;

  setUp(() {
    dir = Directory.systemTemp.createTempSync('turso_todo_test');
    store = TodoStore.openAt('${dir.path}/todos.db');
  });

  tearDown(() {
    store.close();
    dir.deleteSync(recursive: true);
  });

  test('starts empty', () {
    expect(store.all(), isEmpty);
  });

  test('adds todos and reads them back', () {
    final added = store.add('buy milk');
    expect(added.title, 'buy milk');
    expect(added.done, isFalse);
    expect(store.all().map((todo) => todo.title), ['buy milk']);
  });

  test('marks a todo done and sorts it below the open ones', () {
    final first = store.add('first');
    store.add('second');
    store.setDone(first.id, done: true);

    final todos = store.all();
    expect(todos.map((todo) => todo.title), ['second', 'first']);
    expect(todos.last.done, isTrue);
  });

  test('removes a todo', () {
    final todo = store.add('temporary');
    store.remove(todo.id);
    expect(store.all(), isEmpty);
  });

  test('clears only the completed todos', () {
    final done = store.add('done');
    store.add('open');
    store.setDone(done.id, done: true);
    store.clearCompleted();

    expect(store.all().map((todo) => todo.title), ['open']);
  });

  test('keeps todos across reopens', () {
    store.add('persisted');
    store.close();

    store = TodoStore.openAt('${dir.path}/todos.db');
    expect(store.all().map((todo) => todo.title), ['persisted']);
  });
}
