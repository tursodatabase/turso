#include "TursoConnectionHostObject.h"
#include "TursoStatementHostObject.h"

extern "C" {
#include <turso.h>
}

namespace turso {

TursoConnectionHostObject::~TursoConnectionHostObject() {
    if (conn_) {
        turso_connection_deinit(conn_);
        conn_ = nullptr;
    }
}

void TursoConnectionHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown error");
}

jsi::Value TursoConnectionHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    auto propName = name.utf8(rt);

    if (propName == "prepareSingle") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->prepareSingle(rt, args, count);
            }
        );
    }

    if (propName == "prepareFirst") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->prepareFirst(rt, args, count);
            }
        );
    }

    if (propName == "lastInsertRowid") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->lastInsertRowid(rt);
            }
        );
    }

    if (propName == "getAutocommit") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->getAutocommit(rt);
            }
        );
    }

    if (propName == "setBusyTimeout") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->setBusyTimeout(rt, args, count);
            }
        );
    }

    if (propName == "close") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->close(rt);
            }
        );
    }

    return jsi::Value::undefined();
}

void TursoConnectionHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only object
}

std::vector<jsi::PropNameID> TursoConnectionHostObject::getPropertyNames(jsi::Runtime &rt) {
    std::vector<jsi::PropNameID> props;
    props.emplace_back(jsi::PropNameID::forAscii(rt, "prepareSingle"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "prepareFirst"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "lastInsertRowid"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "getAutocommit"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "setBusyTimeout"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "close"));
    return props;
}

// 1:1 C API mapping - NO logic, just calls through to C API
jsi::Value TursoConnectionHostObject::prepareSingle(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "prepareSingle: expected string argument");
    }

    std::string sql = args[0].asString(rt).utf8(rt);
    turso_statement_t* statement = nullptr;
    const char* error = nullptr;

    turso_status_code_t status = turso_connection_prepare_single(conn_, sql.c_str(), &statement, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    // Wrap statement in TursoStatementHostObject
    auto statementObj = std::make_shared<TursoStatementHostObject>(statement);
    return jsi::Object::createFromHostObject(rt, statementObj);
}

jsi::Value TursoConnectionHostObject::prepareFirst(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "prepareFirst: expected string argument");
    }

    std::string sql = args[0].asString(rt).utf8(rt);
    turso_statement_t* statement = nullptr;
    size_t tail_idx = 0;
    const char* error = nullptr;

    turso_status_code_t status = turso_connection_prepare_first(conn_, sql.c_str(), &statement, &tail_idx, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    // If statement is null, return null (no valid statement parsed)
    if (!statement) {
        return jsi::Value::null();
    }

    // Return object with statement and tail_idx
    jsi::Object result(rt);
    auto statementObj = std::make_shared<TursoStatementHostObject>(statement);
    result.setProperty(rt, "statement", jsi::Object::createFromHostObject(rt, statementObj));
    result.setProperty(rt, "tailIdx", jsi::Value(static_cast<double>(tail_idx)));

    return result;
}

jsi::Value TursoConnectionHostObject::lastInsertRowid(jsi::Runtime &rt) {
    int64_t rowid = turso_connection_last_insert_rowid(conn_);
    return jsi::Value(static_cast<double>(rowid));
}

jsi::Value TursoConnectionHostObject::getAutocommit(jsi::Runtime &rt) {
    bool autocommit = turso_connection_get_autocommit(conn_);
    return jsi::Value(autocommit);
}

jsi::Value TursoConnectionHostObject::setBusyTimeout(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "setBusyTimeout: expected number argument");
    }

    int64_t timeout_ms = static_cast<int64_t>(args[0].asNumber());
    turso_connection_set_busy_timeout_ms(conn_, timeout_ms);

    return jsi::Value::undefined();
}

jsi::Value TursoConnectionHostObject::close(jsi::Runtime &rt) {
    if (!conn_) {
        return jsi::Value::undefined();
    }

    const char* error = nullptr;
    turso_status_code_t status = turso_connection_close(conn_, &error);
    conn_ = nullptr; // Prevent destructor from calling deinit on closed connection

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    return jsi::Value::undefined();
}

} // namespace turso
