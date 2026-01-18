#include "TursoDatabaseHostObject.h"
#include "TursoConnectionHostObject.h"
#include <turso.h>

namespace turso {

TursoDatabaseHostObject::~TursoDatabaseHostObject() {
    if (db_) {
        turso_database_deinit(db_);
        db_ = nullptr;
    }
}

void TursoDatabaseHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown error");
}

jsi::Value TursoDatabaseHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    auto propName = name.utf8(rt);

    if (propName == "open") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->open(rt);
            }
        );
    }

    if (propName == "connect") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->connect(rt);
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

void TursoDatabaseHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only object
}

std::vector<jsi::PropNameID> TursoDatabaseHostObject::getPropertyNames(jsi::Runtime &rt) {
    return {
        jsi::PropNameID::forAscii(rt, "open"),
        jsi::PropNameID::forAscii(rt, "connect"),
        jsi::PropNameID::forAscii(rt, "close")
    };
}

// 1:1 C API mapping - NO logic, just calls through to C API
jsi::Value TursoDatabaseHostObject::open(jsi::Runtime &rt) {
    const char* error = nullptr;
    turso_status_code_t status = turso_database_open(db_, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    return jsi::Value::undefined();
}

jsi::Value TursoDatabaseHostObject::connect(jsi::Runtime &rt) {
    turso_connection_t* connection = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_database_connect(db_, &connection, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    // Wrap connection in TursoConnectionHostObject
    auto connectionObj = std::make_shared<TursoConnectionHostObject>(connection);
    return jsi::Object::createFromHostObject(rt, connectionObj);
}

jsi::Value TursoDatabaseHostObject::close(jsi::Runtime &rt) {
    // turso_database_close doesn't exist in the C API
    // Closing happens in destructor via turso_database_deinit
    return jsi::Value::undefined();
}

} // namespace turso
