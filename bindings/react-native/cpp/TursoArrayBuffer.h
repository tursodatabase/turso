#pragma once

#include <jsi/jsi.h>
#include <cstdint>
#include <limits>
#include <string>

namespace turso {

using namespace facebook;

inline jsi::ArrayBuffer createArrayBuffer(jsi::Runtime &rt, uint64_t length) {
    if (length > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
        throw jsi::JSError(rt, "Cannot create an ArrayBuffer of " + std::to_string(length) + " bytes");
    }

    jsi::Function arrayBufferCtor = rt.global().getPropertyAsFunction(rt, "ArrayBuffer");
    jsi::ArrayBuffer arrayBuffer = arrayBufferCtor.callAsConstructor(rt, static_cast<double>(length)).asObject(rt).getArrayBuffer(rt);
    if (arrayBuffer.size(rt) != length) {
        throw jsi::JSError(rt, "ArrayBuffer has " + std::to_string(arrayBuffer.size(rt)) + " bytes instead of " + std::to_string(length));
    }

    return arrayBuffer;
}

} // namespace turso
