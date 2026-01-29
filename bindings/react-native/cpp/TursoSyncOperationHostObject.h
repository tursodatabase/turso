#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_sync_operation;
    struct turso_connection;
    struct turso_sync_changes;
    typedef struct turso_sync_operation turso_sync_operation_t;
    typedef struct turso_connection turso_connection_t;
    typedef struct turso_sync_changes turso_sync_changes_t;
}

namespace turso {

using namespace facebook;

/**
 * TursoSyncOperationHostObject wraps turso_sync_operation_t* (async operation handle).
 * This is a THIN wrapper - 1:1 mapping of SDK-KIT C API with NO logic.
 * All logic belongs in TypeScript or Rust, not here.
 */
class TursoSyncOperationHostObject : public jsi::HostObject {
public:
    TursoSyncOperationHostObject(turso_sync_operation_t* op) : op_(op) {}
    ~TursoSyncOperationHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

    // Direct access to wrapped pointer (for internal use)
    turso_sync_operation_t* getOperation() const { return op_; }

private:
    turso_sync_operation_t* op_ = nullptr;

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // 1:1 C API mapping methods (NO logic - just calls through to C API)
    jsi::Value resume(jsi::Runtime &rt);
    jsi::Value resultKind(jsi::Runtime &rt);
    jsi::Value extractConnection(jsi::Runtime &rt);
    jsi::Value extractChanges(jsi::Runtime &rt);
    jsi::Value extractStats(jsi::Runtime &rt);
};

} // namespace turso
