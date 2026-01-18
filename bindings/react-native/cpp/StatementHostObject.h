#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>
#include <vector>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_statement;
    struct turso_connection;
    typedef struct turso_statement turso_statement_t;
    typedef struct turso_connection turso_connection_t;
}

namespace turso {

using namespace facebook;

class DBHostObject;

/**
 * StatementHostObject wraps a prepared Turso statement.
 * Exposed to JavaScript with methods like bind(), run(), get(), all(), finalize().
 */
class StatementHostObject : public jsi::HostObject {
public:
    StatementHostObject(turso_statement_t *statement, turso_connection_t *connection);
    ~StatementHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

private:
    turso_statement_t *statement_ = nullptr;
    turso_connection_t *connection_ = nullptr;
    bool finalized_ = false;

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // Method implementations
    jsi::Value bind(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value run(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value getOne(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value getAll(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value finalize(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value reset(jsi::Runtime &rt, const jsi::Value *args, size_t count);

    // Bind helpers
    void bindValue(jsi::Runtime &rt, size_t index, const jsi::Value &value);
    void bindParams(jsi::Runtime &rt, const jsi::Value *args, size_t count);

    // Row conversion
    jsi::Value rowToObject(jsi::Runtime &rt);
    jsi::Array rowToArray(jsi::Runtime &rt);

    // Execute statement to completion, collecting results
    std::vector<jsi::Value> executeAndCollect(jsi::Runtime &rt, bool singleRow);
};

} // namespace turso
