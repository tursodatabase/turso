#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_database;
    struct turso_connection;
    typedef struct turso_database turso_database_t;
    typedef struct turso_connection turso_connection_t;
}

namespace turso {

using namespace facebook;

/**
 * TursoDatabaseHostObject wraps turso_database_t* (core SDK-KIT type for local database).
 * This is a THIN wrapper - 1:1 mapping of SDK-KIT C API with NO logic.
 * All logic belongs in TypeScript or Rust, not here.
 */
class TursoDatabaseHostObject : public jsi::HostObject {
public:
    TursoDatabaseHostObject(turso_database_t* db) : db_(db) {}
    ~TursoDatabaseHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

    // Direct access to wrapped pointer (for internal use)
    turso_database_t* getDatabase() const { return db_; }

private:
    turso_database_t* db_ = nullptr;

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // 1:1 C API mapping methods (NO logic - just calls through to C API)
    jsi::Value open(jsi::Runtime &rt);
    jsi::Value connect(jsi::Runtime &rt);
    jsi::Value close(jsi::Runtime &rt);
};

} // namespace turso
