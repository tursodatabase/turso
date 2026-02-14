#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_connection;
    struct turso_statement;
    typedef struct turso_connection turso_connection_t;
    typedef struct turso_statement turso_statement_t;
}

namespace turso {

using namespace facebook;

/**
 * TursoConnectionHostObject wraps turso_connection_t* (core SDK-KIT type for database connection).
 * This is a THIN wrapper - 1:1 mapping of SDK-KIT C API with NO logic.
 * All logic belongs in TypeScript or Rust, not here.
 */
class TursoConnectionHostObject : public jsi::HostObject {
public:
    TursoConnectionHostObject(turso_connection_t* conn) : conn_(conn) {}
    ~TursoConnectionHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

    // Direct access to wrapped pointer (for internal use)
    turso_connection_t* getConnection() const { return conn_; }

private:
    turso_connection_t* conn_ = nullptr;

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // 1:1 C API mapping methods (NO logic - just calls through to C API)
    jsi::Value prepareSingle(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value prepareFirst(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value lastInsertRowid(jsi::Runtime &rt);
    jsi::Value getAutocommit(jsi::Runtime &rt);
    jsi::Value setBusyTimeout(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value close(jsi::Runtime &rt);
};

} // namespace turso
