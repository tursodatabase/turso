// Todo app backed by Turso. The native library is bundled by the build hook in
// the `turso` package, so there is nothing to load or ship here.

import 'package:flutter/material.dart';

import 'todo_store.dart';

void main() => runApp(const TodoApp());

class TodoApp extends StatelessWidget {
  const TodoApp({super.key});

  @override
  Widget build(BuildContext context) => MaterialApp(
    title: 'Turso Todos',
    debugShowCheckedModeBanner: false,
    theme: ThemeData(
      colorSchemeSeed: const Color(0xFF4F46E5),
      brightness: Brightness.light,
    ),
    darkTheme: ThemeData(
      colorSchemeSeed: const Color(0xFF4F46E5),
      brightness: Brightness.dark,
    ),
    home: const TodoPage(),
  );
}

class TodoPage extends StatefulWidget {
  const TodoPage({super.key});

  @override
  State<TodoPage> createState() => _TodoPageState();
}

class _TodoPageState extends State<TodoPage> {
  final _controller = TextEditingController();
  TodoStore? _store;
  List<Todo> _todos = const [];
  Object? _error;

  @override
  void initState() {
    super.initState();
    _openStore();
  }

  Future<void> _openStore() async {
    try {
      final store = await TodoStore.open();
      setState(() {
        _store = store;
        _todos = store.all();
      });
    } catch (error) {
      setState(() => _error = error);
    }
  }

  /// Re-reads the table after a write, so the list always shows what the
  /// database actually holds rather than what we assume it holds.
  void _refresh() => setState(() => _todos = _store!.all());

  void _add() {
    final title = _controller.text.trim();
    if (title.isEmpty) return;
    _store!.add(title);
    _controller.clear();
    _refresh();
  }

  @override
  void dispose() {
    _controller.dispose();
    _store?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final remaining = _todos.where((todo) => !todo.done).length;
    return Scaffold(
      appBar: AppBar(
        title: const Text('Turso Todos'),
        actions: [
          if (_todos.any((todo) => todo.done))
            IconButton(
              tooltip: 'Clear completed',
              icon: const Icon(Icons.cleaning_services_outlined),
              onPressed: () {
                _store!.clearCompleted();
                _refresh();
              },
            ),
        ],
        bottom: PreferredSize(
          preferredSize: const Size.fromHeight(24),
          child: Padding(
            padding: const EdgeInsets.only(bottom: 8, left: 16, right: 16),
            child: Align(
              alignment: Alignment.centerLeft,
              child: Text(
                _store == null
                    ? 'Opening database...'
                    : '$remaining left · ${_store!.path}',
                style: Theme.of(context).textTheme.bodySmall,
                overflow: TextOverflow.ellipsis,
              ),
            ),
          ),
        ),
      ),
      body: _buildBody(),
      bottomNavigationBar: SafeArea(
        child: Padding(
          padding: const EdgeInsets.fromLTRB(16, 8, 16, 8),
          child: Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _controller,
                  enabled: _store != null,
                  onSubmitted: (_) => _add(),
                  decoration: const InputDecoration(
                    hintText: 'What needs doing?',
                    border: OutlineInputBorder(),
                    isDense: true,
                  ),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton(
                onPressed: _store == null ? null : _add,
                child: const Text('Add'),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildBody() {
    if (_error != null) {
      return Center(
        child: Padding(
          padding: const EdgeInsets.all(24),
          child: Text('Could not open the database:\n\n$_error'),
        ),
      );
    }
    if (_store == null) {
      return const Center(child: CircularProgressIndicator());
    }
    if (_todos.isEmpty) {
      return const Center(child: Text('Nothing yet. Add a todo below.'));
    }
    return ListView.builder(
      itemCount: _todos.length,
      itemBuilder: (context, index) {
        final todo = _todos[index];
        return Dismissible(
          key: ValueKey(todo.id),
          background: Container(
            color: Theme.of(context).colorScheme.errorContainer,
            alignment: Alignment.centerRight,
            padding: const EdgeInsets.only(right: 20),
            child: const Icon(Icons.delete_outline),
          ),
          direction: DismissDirection.endToStart,
          onDismissed: (_) {
            _store!.remove(todo.id);
            _refresh();
          },
          child: CheckboxListTile(
            value: todo.done,
            title: Text(
              todo.title,
              style: TextStyle(
                decoration: todo.done ? TextDecoration.lineThrough : null,
              ),
            ),
            onChanged: (value) {
              _store!.setDone(todo.id, done: value ?? false);
              _refresh();
            },
          ),
        );
      },
    );
  }
}
