#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_sync_changes;
    typedef struct turso_sync_changes turso_sync_changes_t;
}

namespace turso {

using namespace facebook;

/**
 * TursoSyncChangesHostObject wraps turso_sync_changes_t* (changes fetched from remote).
 * This is a THIN wrapper - 1:1 mapping of SDK-KIT C API with NO logic.
 * All logic belongs in TypeScript or Rust, not here.
 *
 * IMPORTANT: This object can be consumed by applyChanges(), after which it must NOT be used.
 */
class TursoSyncChangesHostObject : public jsi::HostObject {
public:
    TursoSyncChangesHostObject(turso_sync_changes_t* changes) : changes_(changes), consumed_(false) {}
    ~TursoSyncChangesHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

    // Direct access to wrapped pointer (for internal use)
    turso_sync_changes_t* getChanges() const { return changes_; }

    // Mark this object as consumed (ownership transferred to applyChanges)
    void markConsumed() { consumed_ = true; }

private:
    turso_sync_changes_t* changes_ = nullptr;
    bool consumed_ = false;  // If true, changes ownership was transferred

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // 1:1 C API mapping methods (NO logic - just calls through to C API)
    // Currently, there are no operations on turso_sync_changes_t other than passing it to applyChanges
    // The C API doesn't expose methods like isEmpty() yet, so this object is mostly opaque
};

} // namespace turso
