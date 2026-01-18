#include "TursoSyncIoItemHostObject.h"
#include <turso_sync.h>

namespace turso {

TursoSyncIoItemHostObject::~TursoSyncIoItemHostObject() {
    if (item_) {
        turso_sync_database_io_item_deinit(item_);
        item_ = nullptr;
    }
}

void TursoSyncIoItemHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown error");
}

jsi::Value TursoSyncIoItemHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    auto propName = name.utf8(rt);

    if (propName == "getKind") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->getKind(rt);
            }
        );
    }

    if (propName == "getHttpRequest") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->getHttpRequest(rt);
            }
        );
    }

    if (propName == "getFullReadPath") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->getFullReadPath(rt);
            }
        );
    }

    if (propName == "getFullWriteRequest") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->getFullWriteRequest(rt);
            }
        );
    }

    if (propName == "poison") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->poison(rt, args, count);
            }
        );
    }

    if (propName == "setStatus") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->setStatus(rt, args, count);
            }
        );
    }

    if (propName == "pushBuffer") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value {
                return this->pushBuffer(rt, args, count);
            }
        );
    }

    if (propName == "done") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value {
                return this->done(rt);
            }
        );
    }

    return jsi::Value::undefined();
}

void TursoSyncIoItemHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only object
}

std::vector<jsi::PropNameID> TursoSyncIoItemHostObject::getPropertyNames(jsi::Runtime &rt) {
    std::vector<jsi::PropNameID> props;
    props.emplace_back(jsi::PropNameID::forAscii(rt, "getKind"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "getHttpRequest"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "getFullReadPath"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "getFullWriteRequest"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "poison"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "setStatus"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "pushBuffer"));
    props.emplace_back(jsi::PropNameID::forAscii(rt, "done"));
    return props;
}

// 1:1 C API mapping - NO logic, just calls through to C API

jsi::Value TursoSyncIoItemHostObject::getKind(jsi::Runtime &rt) {
    turso_sync_io_request_type_t kind = turso_sync_database_io_request_kind(item_);

    // Return string representation
    switch (kind) {
        case TURSO_SYNC_IO_HTTP:
            return jsi::String::createFromAscii(rt, "HTTP");
        case TURSO_SYNC_IO_FULL_READ:
            return jsi::String::createFromAscii(rt, "FULL_READ");
        case TURSO_SYNC_IO_FULL_WRITE:
            return jsi::String::createFromAscii(rt, "FULL_WRITE");
        default:
            return jsi::String::createFromAscii(rt, "NONE");
    }
}

jsi::Value TursoSyncIoItemHostObject::getHttpRequest(jsi::Runtime &rt) {
    turso_sync_io_http_request_t request;
    turso_status_code_t status = turso_sync_database_io_request_http(item_, &request);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to get HTTP request");
    }

    jsi::Object result(rt);

    // URL (may be null/empty)
    if (request.url.ptr && request.url.len > 0) {
        std::string url(static_cast<const char*>(request.url.ptr), request.url.len);
        result.setProperty(rt, "url", jsi::String::createFromUtf8(rt, url));
    } else {
        result.setProperty(rt, "url", jsi::Value::null());
    }

    // Method
    if (request.method.ptr && request.method.len > 0) {
        std::string method(static_cast<const char*>(request.method.ptr), request.method.len);
        result.setProperty(rt, "method", jsi::String::createFromUtf8(rt, method));
    }

    // Path
    if (request.path.ptr && request.path.len > 0) {
        std::string path(static_cast<const char*>(request.path.ptr), request.path.len);
        result.setProperty(rt, "path", jsi::String::createFromUtf8(rt, path));
    }

    // Body (may be empty)
    if (request.body.ptr && request.body.len > 0) {
        // Create ArrayBuffer for body
        jsi::Function arrayBufferCtor = rt.global().getPropertyAsFunction(rt, "ArrayBuffer");
        jsi::Object arrayBuffer = arrayBufferCtor.callAsConstructor(rt, static_cast<int>(request.body.len)).asObject(rt);
        jsi::ArrayBuffer buf = arrayBuffer.getArrayBuffer(rt);
        memcpy(buf.data(rt), request.body.ptr, request.body.len);
        result.setProperty(rt, "body", arrayBuffer);
    } else {
        result.setProperty(rt, "body", jsi::Value::null());
    }

    // Headers
    jsi::Object headers(rt);
    for (int32_t i = 0; i < request.headers; i++) {
        turso_sync_io_http_header_t header;
        turso_status_code_t header_status = turso_sync_database_io_request_http_header(item_, i, &header);

        if (header_status == TURSO_OK && header.key.ptr && header.value.ptr) {
            std::string key(static_cast<const char*>(header.key.ptr), header.key.len);
            std::string value(static_cast<const char*>(header.value.ptr), header.value.len);
            headers.setProperty(rt, key.c_str(), jsi::String::createFromUtf8(rt, value));
        }
    }
    result.setProperty(rt, "headers", headers);

    return result;
}

jsi::Value TursoSyncIoItemHostObject::getFullReadPath(jsi::Runtime &rt) {
    turso_sync_io_full_read_request_t request;
    turso_status_code_t status = turso_sync_database_io_request_full_read(item_, &request);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to get full read request");
    }

    if (request.path.ptr && request.path.len > 0) {
        std::string path(static_cast<const char*>(request.path.ptr), request.path.len);
        return jsi::String::createFromUtf8(rt, path);
    }

    return jsi::Value::null();
}

jsi::Value TursoSyncIoItemHostObject::getFullWriteRequest(jsi::Runtime &rt) {
    turso_sync_io_full_write_request_t request;
    turso_status_code_t status = turso_sync_database_io_request_full_write(item_, &request);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to get full write request");
    }

    jsi::Object result(rt);

    // Path
    if (request.path.ptr && request.path.len > 0) {
        std::string path(static_cast<const char*>(request.path.ptr), request.path.len);
        result.setProperty(rt, "path", jsi::String::createFromUtf8(rt, path));
    }

    // Content
    if (request.content.ptr && request.content.len > 0) {
        jsi::Function arrayBufferCtor = rt.global().getPropertyAsFunction(rt, "ArrayBuffer");
        jsi::Object arrayBuffer = arrayBufferCtor.callAsConstructor(rt, static_cast<int>(request.content.len)).asObject(rt);
        jsi::ArrayBuffer buf = arrayBuffer.getArrayBuffer(rt);
        memcpy(buf.data(rt), request.content.ptr, request.content.len);
        result.setProperty(rt, "content", arrayBuffer);
    } else {
        result.setProperty(rt, "content", jsi::Value::null());
    }

    return result;
}

jsi::Value TursoSyncIoItemHostObject::poison(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "poison: expected string argument (error message)");
    }

    std::string error = args[0].asString(rt).utf8(rt);
    turso_slice_ref_t error_slice;
    error_slice.ptr = error.c_str();
    error_slice.len = error.length();

    turso_status_code_t status = turso_sync_database_io_poison(item_, &error_slice);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to poison IO item");
    }

    return jsi::Value::undefined();
}

jsi::Value TursoSyncIoItemHostObject::setStatus(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isNumber()) {
        throw jsi::JSError(rt, "setStatus: expected number argument (status code)");
    }

    int32_t status_code = static_cast<int32_t>(args[0].asNumber());
    turso_status_code_t status = turso_sync_database_io_status(item_, status_code);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to set IO status");
    }

    return jsi::Value::undefined();
}

jsi::Value TursoSyncIoItemHostObject::pushBuffer(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count < 1 || !args[0].isObject()) {
        throw jsi::JSError(rt, "pushBuffer: expected ArrayBuffer argument");
    }

    auto arrayBuffer = args[0].asObject(rt).getArrayBuffer(rt);
    const char* data = reinterpret_cast<const char*>(arrayBuffer.data(rt));
    size_t len = arrayBuffer.size(rt);

    turso_slice_ref_t buffer_slice;
    buffer_slice.ptr = data;
    buffer_slice.len = len;

    turso_status_code_t status = turso_sync_database_io_push_buffer(item_, &buffer_slice);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to push buffer to IO item");
    }

    return jsi::Value::undefined();
}

jsi::Value TursoSyncIoItemHostObject::done(jsi::Runtime &rt) {
    turso_status_code_t status = turso_sync_database_io_done(item_);

    if (status != TURSO_OK) {
        throw jsi::JSError(rt, "Failed to mark IO item as done");
    }

    return jsi::Value::undefined();
}

} // namespace turso
