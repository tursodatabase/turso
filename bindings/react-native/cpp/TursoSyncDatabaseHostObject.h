#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_sync_database;
    struct turso_sync_operation;
    struct turso_sync_io_item;
    struct turso_sync_changes;
    typedef struct turso_sync_database turso_sync_database_t;
    typedef struct turso_sync_operation turso_sync_operation_t;
    typedef struct turso_sync_io_item turso_sync_io_item_t;
    typedef struct turso_sync_changes turso_sync_changes_t;
}

namespace turso {

using namespace facebook;

/**
 * TursoSyncDatabaseHostObject wraps turso_sync_database_t* (sync SDK-KIT type for embedded replica).
 * This is a THIN wrapper - 1:1 mapping of SDK-KIT C API with NO logic.
 * All logic belongs in TypeScript or Rust, not here.
 */
class TursoSyncDatabaseHostObject : public jsi::HostObject {
public:
    TursoSyncDatabaseHostObject(turso_sync_database_t* db) : db_(db) {}
    ~TursoSyncDatabaseHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

    // Direct access to wrapped pointer (for internal use)
    turso_sync_database_t* getSyncDatabase() const { return db_; }

private:
    turso_sync_database_t* db_ = nullptr;

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // 1:1 C API mapping methods (NO logic - just calls through to C API)
    jsi::Value open(jsi::Runtime &rt);
    jsi::Value create(jsi::Runtime &rt);
    jsi::Value connect(jsi::Runtime &rt);
    jsi::Value stats(jsi::Runtime &rt);
    jsi::Value checkpoint(jsi::Runtime &rt);
    jsi::Value pushChanges(jsi::Runtime &rt);
    jsi::Value waitChanges(jsi::Runtime &rt);
    jsi::Value applyChanges(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value ioTakeItem(jsi::Runtime &rt);
    jsi::Value ioStepCallbacks(jsi::Runtime &rt);
    jsi::Value close(jsi::Runtime &rt);
};

} // namespace turso
