// coverage:ignore-file
// GENERATED CODE - DO NOT MODIFY BY HAND
// ignore_for_file: type=lint
// ignore_for_file: unused_element, deprecated_member_use, deprecated_member_use_from_same_package, use_function_type_syntax_for_parameters, unnecessary_const, avoid_init_to_null, invalid_override_different_default_values_named, prefer_expression_function_bodies, annotate_overrides, invalid_annotation_target, unnecessary_question_mark

part of 'value.dart';

// **************************************************************************
// FreezedGenerator
// **************************************************************************

T _$identity<T>(T value) => value;

final _privateConstructorUsedError = UnsupportedError(
  'It seems like you constructed your class using `MyClass._()`. This constructor is only meant to be used by freezed and you are not supposed to need it nor use it.\nPlease check the documentation here for more information: https://github.com/rrousselGit/freezed#adding-getters-and-methods-to-our-models',
);

/// @nodoc
mixin _$Value {
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(int field0) integer,
    required TResult Function(double field0) real,
    required TResult Function(String field0) text,
    required TResult Function(Uint8List field0) blob,
    required TResult Function() null_,
  }) => throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(int field0)? integer,
    TResult? Function(double field0)? real,
    TResult? Function(String field0)? text,
    TResult? Function(Uint8List field0)? blob,
    TResult? Function()? null_,
  }) => throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(int field0)? integer,
    TResult Function(double field0)? real,
    TResult Function(String field0)? text,
    TResult Function(Uint8List field0)? blob,
    TResult Function()? null_,
    required TResult orElse(),
  }) => throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(Value_Integer value) integer,
    required TResult Function(Value_Real value) real,
    required TResult Function(Value_Text value) text,
    required TResult Function(Value_Blob value) blob,
    required TResult Function(Value_Null value) null_,
  }) => throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(Value_Integer value)? integer,
    TResult? Function(Value_Real value)? real,
    TResult? Function(Value_Text value)? text,
    TResult? Function(Value_Blob value)? blob,
    TResult? Function(Value_Null value)? null_,
  }) => throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(Value_Integer value)? integer,
    TResult Function(Value_Real value)? real,
    TResult Function(Value_Text value)? text,
    TResult Function(Value_Blob value)? blob,
    TResult Function(Value_Null value)? null_,
    required TResult orElse(),
  }) => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $ValueCopyWith<$Res> {
  factory $ValueCopyWith(Value value, $Res Function(Value) then) =
      _$ValueCopyWithImpl<$Res, Value>;
}

/// @nodoc
class _$ValueCopyWithImpl<$Res, $Val extends Value>
    implements $ValueCopyWith<$Res> {
  _$ValueCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
}

/// @nodoc
abstract class _$$Value_IntegerImplCopyWith<$Res> {
  factory _$$Value_IntegerImplCopyWith(
    _$Value_IntegerImpl value,
    $Res Function(_$Value_IntegerImpl) then,
  ) = __$$Value_IntegerImplCopyWithImpl<$Res>;
  @useResult
  $Res call({int field0});
}

/// @nodoc
class __$$Value_IntegerImplCopyWithImpl<$Res>
    extends _$ValueCopyWithImpl<$Res, _$Value_IntegerImpl>
    implements _$$Value_IntegerImplCopyWith<$Res> {
  __$$Value_IntegerImplCopyWithImpl(
    _$Value_IntegerImpl _value,
    $Res Function(_$Value_IntegerImpl) _then,
  ) : super(_value, _then);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({Object? field0 = null}) {
    return _then(
      _$Value_IntegerImpl(
        null == field0
            ? _value.field0
            : field0 // ignore: cast_nullable_to_non_nullable
                  as int,
      ),
    );
  }
}

/// @nodoc

class _$Value_IntegerImpl extends Value_Integer {
  const _$Value_IntegerImpl(this.field0) : super._();

  @override
  final int field0;

  @override
  String toString() {
    return 'Value.integer(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$Value_IntegerImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$Value_IntegerImplCopyWith<_$Value_IntegerImpl> get copyWith =>
      __$$Value_IntegerImplCopyWithImpl<_$Value_IntegerImpl>(this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(int field0) integer,
    required TResult Function(double field0) real,
    required TResult Function(String field0) text,
    required TResult Function(Uint8List field0) blob,
    required TResult Function() null_,
  }) {
    return integer(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(int field0)? integer,
    TResult? Function(double field0)? real,
    TResult? Function(String field0)? text,
    TResult? Function(Uint8List field0)? blob,
    TResult? Function()? null_,
  }) {
    return integer?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(int field0)? integer,
    TResult Function(double field0)? real,
    TResult Function(String field0)? text,
    TResult Function(Uint8List field0)? blob,
    TResult Function()? null_,
    required TResult orElse(),
  }) {
    if (integer != null) {
      return integer(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(Value_Integer value) integer,
    required TResult Function(Value_Real value) real,
    required TResult Function(Value_Text value) text,
    required TResult Function(Value_Blob value) blob,
    required TResult Function(Value_Null value) null_,
  }) {
    return integer(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(Value_Integer value)? integer,
    TResult? Function(Value_Real value)? real,
    TResult? Function(Value_Text value)? text,
    TResult? Function(Value_Blob value)? blob,
    TResult? Function(Value_Null value)? null_,
  }) {
    return integer?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(Value_Integer value)? integer,
    TResult Function(Value_Real value)? real,
    TResult Function(Value_Text value)? text,
    TResult Function(Value_Blob value)? blob,
    TResult Function(Value_Null value)? null_,
    required TResult orElse(),
  }) {
    if (integer != null) {
      return integer(this);
    }
    return orElse();
  }
}

abstract class Value_Integer extends Value {
  const factory Value_Integer(final int field0) = _$Value_IntegerImpl;
  const Value_Integer._() : super._();

  int get field0;

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$Value_IntegerImplCopyWith<_$Value_IntegerImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$Value_RealImplCopyWith<$Res> {
  factory _$$Value_RealImplCopyWith(
    _$Value_RealImpl value,
    $Res Function(_$Value_RealImpl) then,
  ) = __$$Value_RealImplCopyWithImpl<$Res>;
  @useResult
  $Res call({double field0});
}

/// @nodoc
class __$$Value_RealImplCopyWithImpl<$Res>
    extends _$ValueCopyWithImpl<$Res, _$Value_RealImpl>
    implements _$$Value_RealImplCopyWith<$Res> {
  __$$Value_RealImplCopyWithImpl(
    _$Value_RealImpl _value,
    $Res Function(_$Value_RealImpl) _then,
  ) : super(_value, _then);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({Object? field0 = null}) {
    return _then(
      _$Value_RealImpl(
        null == field0
            ? _value.field0
            : field0 // ignore: cast_nullable_to_non_nullable
                  as double,
      ),
    );
  }
}

/// @nodoc

class _$Value_RealImpl extends Value_Real {
  const _$Value_RealImpl(this.field0) : super._();

  @override
  final double field0;

  @override
  String toString() {
    return 'Value.real(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$Value_RealImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$Value_RealImplCopyWith<_$Value_RealImpl> get copyWith =>
      __$$Value_RealImplCopyWithImpl<_$Value_RealImpl>(this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(int field0) integer,
    required TResult Function(double field0) real,
    required TResult Function(String field0) text,
    required TResult Function(Uint8List field0) blob,
    required TResult Function() null_,
  }) {
    return real(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(int field0)? integer,
    TResult? Function(double field0)? real,
    TResult? Function(String field0)? text,
    TResult? Function(Uint8List field0)? blob,
    TResult? Function()? null_,
  }) {
    return real?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(int field0)? integer,
    TResult Function(double field0)? real,
    TResult Function(String field0)? text,
    TResult Function(Uint8List field0)? blob,
    TResult Function()? null_,
    required TResult orElse(),
  }) {
    if (real != null) {
      return real(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(Value_Integer value) integer,
    required TResult Function(Value_Real value) real,
    required TResult Function(Value_Text value) text,
    required TResult Function(Value_Blob value) blob,
    required TResult Function(Value_Null value) null_,
  }) {
    return real(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(Value_Integer value)? integer,
    TResult? Function(Value_Real value)? real,
    TResult? Function(Value_Text value)? text,
    TResult? Function(Value_Blob value)? blob,
    TResult? Function(Value_Null value)? null_,
  }) {
    return real?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(Value_Integer value)? integer,
    TResult Function(Value_Real value)? real,
    TResult Function(Value_Text value)? text,
    TResult Function(Value_Blob value)? blob,
    TResult Function(Value_Null value)? null_,
    required TResult orElse(),
  }) {
    if (real != null) {
      return real(this);
    }
    return orElse();
  }
}

abstract class Value_Real extends Value {
  const factory Value_Real(final double field0) = _$Value_RealImpl;
  const Value_Real._() : super._();

  double get field0;

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$Value_RealImplCopyWith<_$Value_RealImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$Value_TextImplCopyWith<$Res> {
  factory _$$Value_TextImplCopyWith(
    _$Value_TextImpl value,
    $Res Function(_$Value_TextImpl) then,
  ) = __$$Value_TextImplCopyWithImpl<$Res>;
  @useResult
  $Res call({String field0});
}

/// @nodoc
class __$$Value_TextImplCopyWithImpl<$Res>
    extends _$ValueCopyWithImpl<$Res, _$Value_TextImpl>
    implements _$$Value_TextImplCopyWith<$Res> {
  __$$Value_TextImplCopyWithImpl(
    _$Value_TextImpl _value,
    $Res Function(_$Value_TextImpl) _then,
  ) : super(_value, _then);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({Object? field0 = null}) {
    return _then(
      _$Value_TextImpl(
        null == field0
            ? _value.field0
            : field0 // ignore: cast_nullable_to_non_nullable
                  as String,
      ),
    );
  }
}

/// @nodoc

class _$Value_TextImpl extends Value_Text {
  const _$Value_TextImpl(this.field0) : super._();

  @override
  final String field0;

  @override
  String toString() {
    return 'Value.text(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$Value_TextImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$Value_TextImplCopyWith<_$Value_TextImpl> get copyWith =>
      __$$Value_TextImplCopyWithImpl<_$Value_TextImpl>(this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(int field0) integer,
    required TResult Function(double field0) real,
    required TResult Function(String field0) text,
    required TResult Function(Uint8List field0) blob,
    required TResult Function() null_,
  }) {
    return text(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(int field0)? integer,
    TResult? Function(double field0)? real,
    TResult? Function(String field0)? text,
    TResult? Function(Uint8List field0)? blob,
    TResult? Function()? null_,
  }) {
    return text?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(int field0)? integer,
    TResult Function(double field0)? real,
    TResult Function(String field0)? text,
    TResult Function(Uint8List field0)? blob,
    TResult Function()? null_,
    required TResult orElse(),
  }) {
    if (text != null) {
      return text(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(Value_Integer value) integer,
    required TResult Function(Value_Real value) real,
    required TResult Function(Value_Text value) text,
    required TResult Function(Value_Blob value) blob,
    required TResult Function(Value_Null value) null_,
  }) {
    return text(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(Value_Integer value)? integer,
    TResult? Function(Value_Real value)? real,
    TResult? Function(Value_Text value)? text,
    TResult? Function(Value_Blob value)? blob,
    TResult? Function(Value_Null value)? null_,
  }) {
    return text?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(Value_Integer value)? integer,
    TResult Function(Value_Real value)? real,
    TResult Function(Value_Text value)? text,
    TResult Function(Value_Blob value)? blob,
    TResult Function(Value_Null value)? null_,
    required TResult orElse(),
  }) {
    if (text != null) {
      return text(this);
    }
    return orElse();
  }
}

abstract class Value_Text extends Value {
  const factory Value_Text(final String field0) = _$Value_TextImpl;
  const Value_Text._() : super._();

  String get field0;

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$Value_TextImplCopyWith<_$Value_TextImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$Value_BlobImplCopyWith<$Res> {
  factory _$$Value_BlobImplCopyWith(
    _$Value_BlobImpl value,
    $Res Function(_$Value_BlobImpl) then,
  ) = __$$Value_BlobImplCopyWithImpl<$Res>;
  @useResult
  $Res call({Uint8List field0});
}

/// @nodoc
class __$$Value_BlobImplCopyWithImpl<$Res>
    extends _$ValueCopyWithImpl<$Res, _$Value_BlobImpl>
    implements _$$Value_BlobImplCopyWith<$Res> {
  __$$Value_BlobImplCopyWithImpl(
    _$Value_BlobImpl _value,
    $Res Function(_$Value_BlobImpl) _then,
  ) : super(_value, _then);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({Object? field0 = null}) {
    return _then(
      _$Value_BlobImpl(
        null == field0
            ? _value.field0
            : field0 // ignore: cast_nullable_to_non_nullable
                  as Uint8List,
      ),
    );
  }
}

/// @nodoc

class _$Value_BlobImpl extends Value_Blob {
  const _$Value_BlobImpl(this.field0) : super._();

  @override
  final Uint8List field0;

  @override
  String toString() {
    return 'Value.blob(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$Value_BlobImpl &&
            const DeepCollectionEquality().equals(other.field0, field0));
  }

  @override
  int get hashCode =>
      Object.hash(runtimeType, const DeepCollectionEquality().hash(field0));

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$Value_BlobImplCopyWith<_$Value_BlobImpl> get copyWith =>
      __$$Value_BlobImplCopyWithImpl<_$Value_BlobImpl>(this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(int field0) integer,
    required TResult Function(double field0) real,
    required TResult Function(String field0) text,
    required TResult Function(Uint8List field0) blob,
    required TResult Function() null_,
  }) {
    return blob(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(int field0)? integer,
    TResult? Function(double field0)? real,
    TResult? Function(String field0)? text,
    TResult? Function(Uint8List field0)? blob,
    TResult? Function()? null_,
  }) {
    return blob?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(int field0)? integer,
    TResult Function(double field0)? real,
    TResult Function(String field0)? text,
    TResult Function(Uint8List field0)? blob,
    TResult Function()? null_,
    required TResult orElse(),
  }) {
    if (blob != null) {
      return blob(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(Value_Integer value) integer,
    required TResult Function(Value_Real value) real,
    required TResult Function(Value_Text value) text,
    required TResult Function(Value_Blob value) blob,
    required TResult Function(Value_Null value) null_,
  }) {
    return blob(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(Value_Integer value)? integer,
    TResult? Function(Value_Real value)? real,
    TResult? Function(Value_Text value)? text,
    TResult? Function(Value_Blob value)? blob,
    TResult? Function(Value_Null value)? null_,
  }) {
    return blob?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(Value_Integer value)? integer,
    TResult Function(Value_Real value)? real,
    TResult Function(Value_Text value)? text,
    TResult Function(Value_Blob value)? blob,
    TResult Function(Value_Null value)? null_,
    required TResult orElse(),
  }) {
    if (blob != null) {
      return blob(this);
    }
    return orElse();
  }
}

abstract class Value_Blob extends Value {
  const factory Value_Blob(final Uint8List field0) = _$Value_BlobImpl;
  const Value_Blob._() : super._();

  Uint8List get field0;

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$Value_BlobImplCopyWith<_$Value_BlobImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$Value_NullImplCopyWith<$Res> {
  factory _$$Value_NullImplCopyWith(
    _$Value_NullImpl value,
    $Res Function(_$Value_NullImpl) then,
  ) = __$$Value_NullImplCopyWithImpl<$Res>;
}

/// @nodoc
class __$$Value_NullImplCopyWithImpl<$Res>
    extends _$ValueCopyWithImpl<$Res, _$Value_NullImpl>
    implements _$$Value_NullImplCopyWith<$Res> {
  __$$Value_NullImplCopyWithImpl(
    _$Value_NullImpl _value,
    $Res Function(_$Value_NullImpl) _then,
  ) : super(_value, _then);

  /// Create a copy of Value
  /// with the given fields replaced by the non-null parameter values.
}

/// @nodoc

class _$Value_NullImpl extends Value_Null {
  const _$Value_NullImpl() : super._();

  @override
  String toString() {
    return 'Value.null_()';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType && other is _$Value_NullImpl);
  }

  @override
  int get hashCode => runtimeType.hashCode;

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(int field0) integer,
    required TResult Function(double field0) real,
    required TResult Function(String field0) text,
    required TResult Function(Uint8List field0) blob,
    required TResult Function() null_,
  }) {
    return null_();
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(int field0)? integer,
    TResult? Function(double field0)? real,
    TResult? Function(String field0)? text,
    TResult? Function(Uint8List field0)? blob,
    TResult? Function()? null_,
  }) {
    return null_?.call();
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(int field0)? integer,
    TResult Function(double field0)? real,
    TResult Function(String field0)? text,
    TResult Function(Uint8List field0)? blob,
    TResult Function()? null_,
    required TResult orElse(),
  }) {
    if (null_ != null) {
      return null_();
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(Value_Integer value) integer,
    required TResult Function(Value_Real value) real,
    required TResult Function(Value_Text value) text,
    required TResult Function(Value_Blob value) blob,
    required TResult Function(Value_Null value) null_,
  }) {
    return null_(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(Value_Integer value)? integer,
    TResult? Function(Value_Real value)? real,
    TResult? Function(Value_Text value)? text,
    TResult? Function(Value_Blob value)? blob,
    TResult? Function(Value_Null value)? null_,
  }) {
    return null_?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(Value_Integer value)? integer,
    TResult Function(Value_Real value)? real,
    TResult Function(Value_Text value)? text,
    TResult Function(Value_Blob value)? blob,
    TResult Function(Value_Null value)? null_,
    required TResult orElse(),
  }) {
    if (null_ != null) {
      return null_(this);
    }
    return orElse();
  }
}

abstract class Value_Null extends Value {
  const factory Value_Null() = _$Value_NullImpl;
  const Value_Null._() : super._();
}
