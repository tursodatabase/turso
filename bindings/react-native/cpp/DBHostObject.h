#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>
#include <unordered_map>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_database;
    struct turso_connection;
    typedef struct turso_database turso_database_t;
    typedef struct turso_connection turso_connection_t;
}

namespace turso {

using namespace facebook;

class StatementHostObject;

/**
 * DBHostObject wraps a Turso database and connection.
 * Exposed to JavaScript as a HostObject with methods like prepare(), exec(), close().
 */
class DBHostObject : public jsi::HostObject {
public:
    DBHostObject(const std::string &path, const std::string &basePath);
    ~DBHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

    // Database operations
    void close();
    bool isOpen() const { return isOpen_; }

    // Get the connection for statement operations
    turso_connection_t* getConnection() const { return connection_; }

private:
    std::string path_;
    std::string fullPath_;
    turso_database_t *database_ = nullptr;
    turso_connection_t *connection_ = nullptr;
    bool isOpen_ = false;

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // Method implementations
    jsi::Value prepare(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value exec(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value run(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value getOne(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value getAll(jsi::Runtime &rt, const jsi::Value *args, size_t count);

    // Property getters
    int64_t getLastInsertRowId() const;
    int64_t getChanges() const;
    bool getAutocommit() const;

    // Cache for method functions
    std::unordered_map<std::string, jsi::Value> methodCache_;
};

} // namespace turso
