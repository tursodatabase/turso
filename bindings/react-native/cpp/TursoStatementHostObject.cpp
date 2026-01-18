#include "TursoStatementHostObject.h"

extern "C" {
#include <turso.h>
}

namespace turso {

TursoStatementHostObject::~TursoStatementHostObject() {
    if (stmt_) {
        turso_statement_deinit(stmt_);
        stmt_ = nullptr;
    }
}

void TursoStatementHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown error");
}

jsi::Value TursoStatementHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    auto propName = name.utf8(rt);

    if (propName == "bindPositionalNull") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->bindPositionalNull(rt, args, count);
            });
    }
    if (propName == "bindPositionalInt") {
        return jsi::Function::createFromHostFunction(rt, name, 2,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->bindPositionalInt(rt, args, count);
            });
    }
    if (propName == "bindPositionalDouble") {
        return jsi::Function::createFromHostFunction(rt, name, 2,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->bindPositionalDouble(rt, args, count);
            });
    }
    if (propName == "bindPositionalBlob") {
        return jsi::Function::createFromHostFunction(rt, name, 2,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->bindPositionalBlob(rt, args, count);
            });
    }
    if (propName == "bindPositionalText") {
        return jsi::Function::createFromHostFunction(rt, name, 2,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->bindPositionalText(rt, args, count);
            });
    }
    if (propName == "execute") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->execute(rt);
            });
    }
    if (propName == "step") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->step(rt);
            });
    }
    if (propName == "runIo") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->runIo(rt);
            });
    }
    if (propName == "reset") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->reset(rt);
            });
    }
    if (propName == "finalize") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->finalize(rt);
            });
    }
    if (propName == "nChange") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->nChange(rt);
            });
    }
    if (propName == "columnCount") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->columnCount(rt);
            });
    }
    if (propName == "columnName") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->columnName(rt, args, count);
            });
    }
    if (propName == "rowValueKind") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->rowValueKind(rt, args, count);
            });
    }
    if (propName == "rowValueBytesCount") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->rowValueBytesCount(rt, args, count);
            });
    }
    if (propName == "rowValueBytesPtr") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->rowValueBytesPtr(rt, args, count);
            });
    }
    if (propName == "rowValueInt") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->rowValueInt(rt, args, count);
            });
    }
    if (propName == "rowValueDouble") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->rowValueDouble(rt, args, count);
            });
    }
    if (propName == "namedPosition") {
        return jsi::Function::createFromHostFunction(rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->namedPosition(rt, args, count);
            });
    }
    if (propName == "parametersCount") {
        return jsi::Function::createFromHostFunction(rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->parametersCount(rt);
            });
    }

    return jsi::Value::undefined();
}

void TursoStatementHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only object
}

std::vector<jsi::PropNameID> TursoStatementHostObject::getPropertyNames(jsi::Runtime &rt) {
    std::vector<jsi::PropNameID> props;
    props.emplace_back(jsi::PropNameID::forAscii(rt, "bindPositionalNull"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "bindPositionalInt"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "bindPositionalDouble"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "bindPositionalBlob"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "bindPositionalText"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "execute"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "step"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "runIo"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "reset"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "finalize"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "nChange"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "columnCount"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "columnName"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "rowValueKind"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "rowValueBytesCount"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "rowValueBytesPtr"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "rowValueInt"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "rowValueDouble"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "namedPosition"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "parametersCount"));
    return props;
}

// 1:1 C API mapping - NO logic, just calls through to C API

jsi::Value TursoStatementHostObject::bindPositionalNull(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "bindPositionalNull: expected number argument (position)");
    }
    size_t position = static_cast<size_t>(args[0].asNumber());
    turso_status_code_t status = turso_statement_bind_positional_null(stmt_, position);
    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoStatementHostObject::bindPositionalInt(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 2 || !args[0].isNumber() || !args[1].isNumber()) {
        throw jsi::JSError(rt, "bindPositionalInt: expected two number arguments (position, value)");
    }
    size_t position = static_cast<size_t>(args[0].asNumber());
    int64_t value = static_cast<int64_t>(args[1].asNumber());
    turso_status_code_t status = turso_statement_bind_positional_int(stmt_, position, value);
    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoStatementHostObject::bindPositionalDouble(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 2 || !args[0].isNumber() || !args[1].isNumber()) {
        throw jsi::JSError(rt, "bindPositionalDouble: expected two number arguments (position, value)");
    }
    size_t position = static_cast<size_t>(args[0].asNumber());
    double value = args[1].asNumber();
    turso_status_code_t status = turso_statement_bind_positional_double(stmt_, position, value);
    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoStatementHostObject::bindPositionalBlob(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 2 || !args[0].isNumber() || !args[1].isObject()) {
        throw jsi::JSError(rt, "bindPositionalBlob: expected number and ArrayBuffer arguments");
    }
    size_t position = static_cast<size_t>(args[0].asNumber());

    // Get ArrayBuffer
    auto arrayBuffer = args[1].asObject(rt).getArrayBuffer(rt);
    const char* data = reinterpret_cast<const char*>(arrayBuffer.data(rt));
    size_t len = arrayBuffer.size(rt);

    turso_status_code_t status = turso_statement_bind_positional_blob(stmt_, position, data, len);
    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoStatementHostObject::bindPositionalText(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 2 || !args[0].isNumber() || !args[1].isString()) {
        throw jsi::JSError(rt, "bindPositionalText: expected number and string arguments");
    }
    size_t position = static_cast<size_t>(args[0].asNumber());
    std::string value = args[1].asString(rt).utf8(rt);
    turso_status_code_t status = turso_statement_bind_positional_text(stmt_, position, value.c_str(), value.length());
    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoStatementHostObject::execute(jsi::Runtime &rt) {
    uint64_t rows_changed = 0;
    const char* error = nullptr;
    turso_status_code_t status = turso_statement_execute(stmt_, &rows_changed, &error);

    if (status != TURSO_OK && status != TURSO_DONE && status != TURSO_IO) {
        throwError(rt, error);
    }

    // Return object with status and rows_changed
    jsi::Object result(rt);
    result.setProperty(rt, "status", jsi::Value(static_cast<int>(status)));
    result.setProperty(rt, "rowsChanged", jsi::Value(static_cast<double>(rows_changed)));
    return result;
}

jsi::Value TursoStatementHostObject::step(jsi::Runtime &rt) {
    const char* error = nullptr;
    turso_status_code_t status = turso_statement_step(stmt_, &error);

    if (status != TURSO_OK && status != TURSO_DONE && status != TURSO_ROW && status != TURSO_IO) {
        throwError(rt, error);
    }

    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoStatementHostObject::runIo(jsi::Runtime &rt) {
    const char* error = nullptr;
    turso_status_code_t status = turso_statement_run_io(stmt_, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoStatementHostObject::reset(jsi::Runtime &rt) {
    const char* error = nullptr;
    turso_status_code_t status = turso_statement_reset(stmt_, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    return jsi::Value::undefined();
}

jsi::Value TursoStatementHostObject::finalize(jsi::Runtime &rt) {
    const char* error = nullptr;
    turso_status_code_t status = turso_statement_finalize(stmt_, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    return jsi::Value::undefined();
}

jsi::Value TursoStatementHostObject::nChange(jsi::Runtime &rt) {
    int64_t n = turso_statement_n_change(stmt_);
    return jsi::Value(static_cast<double>(n));
}

jsi::Value TursoStatementHostObject::columnCount(jsi::Runtime &rt) {
    int64_t count = turso_statement_column_count(stmt_);
    return jsi::Value(static_cast<double>(count));
}

jsi::Value TursoStatementHostObject::columnName(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "columnName: expected number argument (index)");
    }
    size_t index = static_cast<size_t>(args[0].asNumber());
    const char* name = turso_statement_column_name(stmt_, index);

    if (!name) {
        return jsi::Value::null();
    }

    std::string nameStr(name);
    turso_str_deinit(name);  // Free the C string

    return jsi::String::createFromUtf8(rt, nameStr);
}

jsi::Value TursoStatementHostObject::rowValueKind(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "rowValueKind: expected number argument (index)");
    }
    size_t index = static_cast<size_t>(args[0].asNumber());
    turso_type_t kind = turso_statement_row_value_kind(stmt_, index);
    return jsi::Value(static_cast<int>(kind));
}

jsi::Value TursoStatementHostObject::rowValueBytesCount(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "rowValueBytesCount: expected number argument (index)");
    }
    size_t index = static_cast<size_t>(args[0].asNumber());
    int64_t bytes = turso_statement_row_value_bytes_count(stmt_, index);
    return jsi::Value(static_cast<double>(bytes));
}

jsi::Value TursoStatementHostObject::rowValueBytesPtr(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "rowValueBytesPtr: expected number argument (index)");
    }
    size_t index = static_cast<size_t>(args[0].asNumber());
    const char* ptr = turso_statement_row_value_bytes_ptr(stmt_, index);
    int64_t bytes = turso_statement_row_value_bytes_count(stmt_, index);

    if (!ptr || bytes <= 0) {
        return jsi::Value::null();
    }

    // Create ArrayBuffer and copy data
    jsi::Function arrayBufferCtor = rt.global().getPropertyAsFunction(rt, "ArrayBuffer");
    jsi::Object arrayBuffer = arrayBufferCtor.callAsConstructor(rt, static_cast<int>(bytes)).asObject(rt);
    jsi::ArrayBuffer buf = arrayBuffer.getArrayBuffer(rt);
    memcpy(buf.data(rt), ptr, bytes);

    return arrayBuffer;
}

jsi::Value TursoStatementHostObject::rowValueInt(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "rowValueInt: expected number argument (index)");
    }
    size_t index = static_cast<size_t>(args[0].asNumber());
    int64_t value = turso_statement_row_value_int(stmt_, index);
    return jsi::Value(static_cast<double>(value));
}

jsi::Value TursoStatementHostObject::rowValueDouble(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "rowValueDouble: expected number argument (index)");
    }
    size_t index = static_cast<size_t>(args[0].asNumber());
    double value = turso_statement_row_value_double(stmt_, index);
    return jsi::Value(value);
}

jsi::Value TursoStatementHostObject::namedPosition(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "namedPosition: expected string argument (name)");
    }
    std::string name = args[0].asString(rt).utf8(rt);
    int64_t position = turso_statement_named_position(stmt_, name.c_str());
    return jsi::Value(static_cast<double>(position));
}

jsi::Value TursoStatementHostObject::parametersCount(jsi::Runtime &rt) {
    int64_t count = turso_statement_parameters_count(stmt_);
    return jsi::Value(static_cast<double>(count));
}

} // namespace turso
