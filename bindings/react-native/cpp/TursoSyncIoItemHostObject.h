#pragma once

#include <jsi/jsi.h>
#include <memory>
#include <string>

// Forward declarations for Turso C API types
extern "C" {
    struct turso_sync_io_item;
    typedef struct turso_sync_io_item turso_sync_io_item_t;
}

namespace turso {

using namespace facebook;

/**
 * TursoSyncIoItemHostObject wraps turso_sync_io_item_t* (IO queue item for fetch/fs operations).
 * This is a THIN wrapper - 1:1 mapping of SDK-KIT C API with NO logic.
 * All logic belongs in TypeScript or Rust, not here.
 */
class TursoSyncIoItemHostObject : public jsi::HostObject {
public:
    TursoSyncIoItemHostObject(turso_sync_io_item_t* item) : item_(item) {}
    ~TursoSyncIoItemHostObject();

    // JSI HostObject interface
    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override;
    void set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) override;
    std::vector<jsi::PropNameID> getPropertyNames(jsi::Runtime &rt) override;

    // Direct access to wrapped pointer (for internal use)
    turso_sync_io_item_t* getIoItem() const { return item_; }

private:
    turso_sync_io_item_t* item_ = nullptr;

    // Helper to throw JS errors
    void throwError(jsi::Runtime &rt, const char *error);

    // 1:1 C API mapping methods (NO logic - just calls through to C API)
    jsi::Value getKind(jsi::Runtime &rt);
    jsi::Value getHttpRequest(jsi::Runtime &rt);
    jsi::Value getFullReadPath(jsi::Runtime &rt);
    jsi::Value getFullWriteRequest(jsi::Runtime &rt);
    jsi::Value poison(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value setStatus(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value pushBuffer(jsi::Runtime &rt, const jsi::Value *args, size_t count);
    jsi::Value done(jsi::Runtime &rt);
};

} // namespace turso
