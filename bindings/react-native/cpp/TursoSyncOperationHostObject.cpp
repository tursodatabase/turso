#include "TursoSyncOperationHostObject.h"
#include "TursoConnectionHostObject.h"
#include "TursoSyncChangesHostObject.h"
#include <turso_sync.h>

namespace turso {

TursoSyncOperationHostObject::~TursoSyncOperationHostObject() {
    if (op_) {
        turso_sync_operation_deinit(op_);
        op_ = nullptr;
    }
}

void TursoSyncOperationHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown error");
}

jsi::Value TursoSyncOperationHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    auto propName = name.utf8(rt);

    if (propName == "resume") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->resume(rt);
            }
        );
    }

    if (propName == "resultKind") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->resultKind(rt);
            }
        );
    }

    if (propName == "extractConnection") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->extractConnection(rt);
            }
        );
    }

    if (propName == "extractChanges") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->extractChanges(rt);
            }
        );
    }

    if (propName == "extractStats") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->extractStats(rt);
            }
        );
    }

    return jsi::Value::undefined();
}

void TursoSyncOperationHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only object
}

std::vector<jsi::PropNameID> TursoSyncOperationHostObject::getPropertyNames(jsi::Runtime &rt) {
    return {
        jsi::PropNameID::forAscii(rt, "resume"),
        jsi::PropNameID::forAscii(rt, "resultKind"),
        jsi::PropNameID::forAscii(rt, "extractConnection"),
        jsi::PropNameID::forAscii(rt, "extractChanges"),
        jsi::PropNameID::forAscii(rt, "extractStats")
    };
}

// 1:1 C API mapping - NO logic, just calls through to C API

jsi::Value TursoSyncOperationHostObject::resume(jsi::Runtime &rt) {
    const char* error = nullptr;
    turso_status_code_t status = turso_sync_operation_resume(op_, &error);

    if (status != TURSO_OK && status != TURSO_DONE && status != TURSO_IO) {
        throwError(rt, error);
    }

    // Return status code (TURSO_DONE, TURSO_IO, etc.)
    return jsi::Value(static_cast<int>(status));
}

jsi::Value TursoSyncOperationHostObject::resultKind(jsi::Runtime &rt) {
    turso_sync_operation_result_type_t kind = turso_sync_operation_result_kind(op_);
    return jsi::Value(static_cast<int>(kind));
}

jsi::Value TursoSyncOperationHostObject::extractConnection(jsi::Runtime &rt) {
    const turso_connection_t* connection = nullptr;
    turso_status_code_t status = turso_sync_operation_result_extract_connection(op_, &connection);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to extract connection from operation result");
    }

    auto connectionObj = std::make_shared<TursoConnectionHostObject>(
        const_cast<turso_connection_t*>(connection)
    );
    return jsi::Object::createFromHostObject(rt, connectionObj);
}

jsi::Value TursoSyncOperationHostObject::extractChanges(jsi::Runtime &rt) {
    const turso_sync_changes_t* changes = nullptr;
    turso_status_code_t status = turso_sync_operation_result_extract_changes(op_, &changes);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to extract changes from operation result");
    }

    // If no changes, return null
    if (!changes) {
        return jsi::Value::null();
    }

    auto changesObj = std::make_shared<TursoSyncChangesHostObject>(
        const_cast<turso_sync_changes_t*>(changes)
    );
    return jsi::Object::createFromHostObject(rt, changesObj);
}

jsi::Value TursoSyncOperationHostObject::extractStats(jsi::Runtime &rt) {
    turso_sync_stats_t stats;
    turso_status_code_t status = turso_sync_operation_result_extract_stats(op_, &stats);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to extract stats from operation result");
    }

    // Convert stats to JS object
    jsi::Object result(rt);
    result.setProperty(rt, "cdcOperations", jsi::Value(static_cast<double>(stats.cdc_operations)));
    result.setProperty(rt, "mainWalSize", jsi::Value(static_cast<double>(stats.main_wal_size)));
    result.setProperty(rt, "revertWalSize", jsi::Value(static_cast<double>(stats.revert_wal_size)));
    result.setProperty(rt, "lastPullUnixTime", jsi::Value(static_cast<double>(stats.last_pull_unix_time)));
    result.setProperty(rt, "lastPushUnixTime", jsi::Value(static_cast<double>(stats.last_push_unix_time)));
    result.setProperty(rt, "networkSentBytes", jsi::Value(static_cast<double>(stats.network_sent_bytes)));
    result.setProperty(rt, "networkReceivedBytes", jsi::Value(static_cast<double>(stats.network_received_bytes)));

    // Convert revision slice to string (if available)
    if (stats.revision.ptr && stats.revision.len > 0) {
        std::string revision(static_cast<const char*>(stats.revision.ptr), stats.revision.len);
        result.setProperty(rt, "revision", jsi::String::createFromUtf8(rt, revision));
    } else {
        result.setProperty(rt, "revision", jsi::Value::null());
    }

    return result;
}

} // namespace turso
