import 'dart:typed_data';

import 'package:turso_dart/src/rust/helpers/value.dart';

Value toValue(dynamic value) {
  if (value is int) {
    return Value.integer(value);
  }
  if (value is double) {
    return Value.real(value);
  }
  if (value is String) {
    return Value.text(value);
  }
  if (value is Uint8List) {
    return Value.blob(value);
  }
  return const Value.null_();
}
