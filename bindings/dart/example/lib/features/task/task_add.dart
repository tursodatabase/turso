import 'package:flutter/material.dart';

class TaskAdd extends StatefulWidget {
  const TaskAdd({super.key});

  @override
  State<TaskAdd> createState() => _TaskAddState();
}

class _TaskAddState extends State<TaskAdd> {
  final formKey = GlobalKey<FormState>();
  final titleController = TextEditingController();
  final descriptionController = TextEditingController();

  @override
  Widget build(BuildContext context) {
    return BottomSheet(
      onClosing: () {},
      enableDrag: false,
      builder: (context) {
        return SingleChildScrollView(
          child: Form(
            key: formKey,
            autovalidateMode: AutovalidateMode.disabled,
            child: Padding(
              padding: const EdgeInsets.all(24).add(EdgeInsets.only(
                  bottom: MediaQuery.of(context).viewInsets.bottom)),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  const Text('Add new task', style: TextStyle(fontSize: 24)),
                  const SizedBox(height: 16),
                  const Text('Name'),
                  TextFormField(
                    controller: titleController,
                    textCapitalization: TextCapitalization.sentences,
                    onTap: () {
                      titleController.selection = TextSelection(
                        baseOffset: 0,
                        extentOffset: titleController.text.length,
                      );
                    },
                    validator: (value) {
                      if (value?.isEmpty ?? true) {
                        return 'Must not be empty';
                      }
                      return null;
                    },
                  ),
                  const SizedBox(height: 16),
                  const Text('Description'),
                  TextFormField(
                    controller: descriptionController,
                    textCapitalization: TextCapitalization.sentences,
                    onTap: () {
                      titleController.selection = TextSelection(
                        baseOffset: 0,
                        extentOffset: titleController.text.length,
                      );
                    },
                    validator: (value) {
                      if (value?.isEmpty ?? true) {
                        return 'Must not be empty';
                      }
                      return null;
                    },
                  ),
                  const SizedBox(height: 32),
                  FilledButton(
                    onPressed: () {
                      if (!formKey.currentState!.validate()) return;
                      Navigator.maybePop(context, {
                        'title': titleController.text,
                        'description': descriptionController.text,
                      });
                    },
                    child: const Text('Add task'),
                  ),
                ],
              ),
            ),
          ),
        );
      },
    );
  }
}
