#include "TursoSyncChangesHostObject.h"

extern "C" {
#include <turso_sync.h>
}

namespace turso {

TursoSyncChangesHostObject::~TursoSyncChangesHostObject() {
    // Only deinit if not consumed (ownership not transferred to applyChanges)
    if (changes_ && !consumed_) {
        turso_sync_changes_deinit(changes_);
        changes_ = nullptr;
    }
}

void TursoSyncChangesHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown error");
}

jsi::Value TursoSyncChangesHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    auto propName = name.utf8(rt);

    // Currently, turso_sync_changes_t is mostly opaque in the C API
    // No methods exposed yet (like isEmpty)
    // This object is primarily meant to be passed to applyChanges()

    // If the C API adds methods in the future, they can be added here

    return jsi::Value::undefined();
}

void TursoSyncChangesHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only object
}

std::vector<jsi::PropNameID> TursoSyncChangesHostObject::getPropertyNames(jsi::Runtime &rt) {
    return {};
}

} // namespace turso
