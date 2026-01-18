#include "TursoSyncDatabaseHostObject.h"
#include "TursoSyncOperationHostObject.h"
#include "TursoSyncIoItemHostObject.h"
#include "TursoSyncChangesHostObject.h"
#include <turso_sync.h>

namespace turso {

TursoSyncDatabaseHostObject::~TursoSyncDatabaseHostObject() {
    if (db_) {
        turso_sync_database_deinit(db_);
        db_ = nullptr;
    }
}

void TursoSyncDatabaseHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown error");
}

jsi::Value TursoSyncDatabaseHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    auto propName = name.utf8(rt);

    if (propName == "open") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->open(rt);
            }
        );
    }

    if (propName == "create") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->create(rt);
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

    if (propName == "stats") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->stats(rt);
            }
        );
    }

    if (propName == "checkpoint") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->checkpoint(rt);
            }
        );
    }

    if (propName == "pushChanges") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->pushChanges(rt);
            }
        );
    }

    if (propName == "waitChanges") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->waitChanges(rt);
            }
        );
    }

    if (propName == "applyChanges") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->applyChanges(rt, args, count);
            }
        );
    }

    if (propName == "ioTakeItem") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->ioTakeItem(rt);
            }
        );
    }

    if (propName == "ioStepCallbacks") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->ioStepCallbacks(rt);
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

void TursoSyncDatabaseHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only object
}

std::vector<jsi::PropNameID> TursoSyncDatabaseHostObject::getPropertyNames(jsi::Runtime &rt) {
    std::vector<jsi::PropNameID> props;
    props.emplace_back(jsi::PropNameID::forAscii(rt, "open"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "create"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "connect"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "stats"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "checkpoint"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "pushChanges"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "waitChanges"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "applyChanges"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "ioTakeItem"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "ioStepCallbacks"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "close"));
    return props;
}

// 1:1 C API mapping - NO logic, just calls through to C API

jsi::Value TursoSyncDatabaseHostObject::open(jsi::Runtime &rt) {
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_open(db_, &operation, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    // Wrap operation in TursoSyncOperationHostObject
    // Cast away const since we're transferring ownership
    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::create(jsi::Runtime &rt) {
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_create(db_, &operation, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::connect(jsi::Runtime &rt) {
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_connect(db_, &operation, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::stats(jsi::Runtime &rt) {
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_stats(db_, &operation, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::checkpoint(jsi::Runtime &rt) {
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_checkpoint(db_, &operation, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::pushChanges(jsi::Runtime &rt) {
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_push_changes(db_, &operation, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::waitChanges(jsi::Runtime &rt) {
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_wait_changes(db_, &operation, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::applyChanges(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isObject()) {
        throw jsi::JSError(rt, "applyChanges: expected TursoSyncChanges object");
    }

    // Extract the TursoSyncChangesHostObject from the JSI object
    auto changesHostObj = std::dynamic_pointer_cast<TursoSyncChangesHostObject>(
        args[0].asObject(rt).asHostObject(rt)
    );

    if (!changesHostObj) {
        throw jsi::JSError(rt, "applyChanges: invalid TursoSyncChanges object");
    }

    const turso_sync_changes_t* changes = changesHostObj->getChanges();
    const turso_sync_operation_t* operation = nullptr;
    const char* error = nullptr;

    turso_status_code_t status = turso_sync_database_apply_changes(db_, changes, &operation, &error);

    // Note: changes ownership is transferred to turso_sync_database_apply_changes
    // Mark the changes object as consumed so it won't try to deinit
    changesHostObj->markConsumed();

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    auto operationObj = std::make_shared<TursoSyncOperationHostObject>(
        const_cast<turso_sync_operation_t*>(operation)
    );
    return jsi::Object::createFromHostObject(rt, operationObj);
}

jsi::Value TursoSyncDatabaseHostObject::ioTakeItem(jsi::Runtime &rt) {
    const turso_sync_io_item_t* item = nullptr;
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_io_take_item(db_, &item, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    // If no item available, return null
    if (!item) {
        return jsi::Value::null();
    }

    auto itemObj = std::make_shared<TursoSyncIoItemHostObject>(
        const_cast<turso_sync_io_item_t*>(item)
    );
    return jsi::Object::createFromHostObject(rt, itemObj);
}

jsi::Value TursoSyncDatabaseHostObject::ioStepCallbacks(jsi::Runtime &rt) {
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_database_io_step_callbacks(db_, &error);

    if (status != TURSO_OK) {
        throwError(rt, error);
    }

    return jsi::Value::undefined();
}

jsi::Value TursoSyncDatabaseHostObject::close(jsi::Runtime &rt) {
    // turso_sync_database_close doesn't exist in the C API
    // Closing happens in destructor via turso_sync_database_deinit
    return jsi::Value::undefined();
}

} // namespace turso
