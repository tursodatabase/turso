#include "StatementHostObject.h"
#include "DBHostObject.h"

// Include the Turso C API header
extern "C" {
#include "turso.h"
}

namespace turso {

using namespace facebook;

StatementHostObject::StatementHostObject(turso_statement_t *statement, turso_connection_t *connection)
    : statement_(statement), connection_(connection), finalized_(false) {
}

StatementHostObject::~StatementHostObject() {
    if (statement_ && !finalized_) {
        turso_statement_finalize(statement_, nullptr);
        turso_statement_deinit(statement_);
    }
}

void StatementHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown statement error");
}

void StatementHostObject::bindValue(jsi::Runtime &rt, size_t index, const jsi::Value &value) {
    if (value.isNull() || value.isUndefined()) {
        turso_statement_bind_positional_null(statement_, index);
    } else if (value.isNumber()) {
        double val = value.asNumber();
        if (val == static_cast<int64_t>(val)) {
            turso_statement_bind_positional_int(statement_, index, static_cast<int64_t>(val));
        } else {
            turso_statement_bind_positional_double(statement_, index, val);
        }
    } else if (value.isString()) {
        boundStrings_.push_back(value.asString(rt).utf8(rt));
        const std::string& str = boundStrings_.back();
        turso_statement_bind_positional_text(statement_, index, str.c_str(), str.length());
    } else if (value.isBool()) {
        turso_statement_bind_positional_int(statement_, index, value.getBool() ? 1 : 0);
    } else if (value.isObject()) {
        jsi::Object obj = value.asObject(rt);
        if (obj.isArrayBuffer(rt)) {
            jsi::ArrayBuffer buffer = obj.getArrayBuffer(rt);
            turso_statement_bind_positional_blob(
                statement_, index,
                reinterpret_cast<const char*>(buffer.data(rt)),
                buffer.size(rt)
            );
        } else if (obj.isArray(rt)) {
            // Arrays not supported as bind values
            throw jsi::JSError(rt, "Cannot bind array as parameter");
        } else {
            // Try to get as Uint8Array for blob
            if (obj.hasProperty(rt, "buffer") && obj.hasProperty(rt, "byteLength")) {
                // Likely a TypedArray
                jsi::Value bufferProp = obj.getProperty(rt, "buffer");
                if (bufferProp.isObject() && bufferProp.asObject(rt).isArrayBuffer(rt)) {
                    jsi::ArrayBuffer buffer = bufferProp.asObject(rt).getArrayBuffer(rt);
                    jsi::Value byteOffset = obj.getProperty(rt, "byteOffset");
                    jsi::Value byteLength = obj.getProperty(rt, "byteLength");
                    size_t offset = byteOffset.isNumber() ? static_cast<size_t>(byteOffset.asNumber()) : 0;
                    size_t length = byteLength.isNumber() ? static_cast<size_t>(byteLength.asNumber()) : buffer.size(rt);
                    turso_statement_bind_positional_blob(
                        statement_, index,
                        reinterpret_cast<const char*>(buffer.data(rt)) + offset,
                        length
                    );
                    return;
                }
            }
            throw jsi::JSError(rt, "Cannot bind object as parameter");
        }
    }
}

void StatementHostObject::bindParams(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (count == 0) return;

    // Check if first arg is an array
    if (args[0].isObject()) {
        jsi::Object obj = args[0].asObject(rt);
        if (obj.isArray(rt)) {
            jsi::Array arr = obj.asArray(rt);
            size_t len = arr.size(rt);
            for (size_t i = 0; i < len; i++) {
                bindValue(rt, i + 1, arr.getValueAtIndex(rt, i));
            }
            return;
        }
        // Check if it's an object with named parameters
        if (!obj.isArrayBuffer(rt)) {
            jsi::Array names = obj.getPropertyNames(rt);
            size_t numProps = names.size(rt);
            for (size_t i = 0; i < numProps; i++) {
                std::string name = names.getValueAtIndex(rt, i).asString(rt).utf8(rt);
                // Try with : prefix
                std::string colonName = ":" + name;
                int64_t pos = turso_statement_named_position(statement_, colonName.c_str());
                if (pos > 0) {
                    bindValue(rt, pos, obj.getProperty(rt, name.c_str()));
                    continue;
                }
                // Try with @ prefix
                std::string atName = "@" + name;
                pos = turso_statement_named_position(statement_, atName.c_str());
                if (pos > 0) {
                    bindValue(rt, pos, obj.getProperty(rt, name.c_str()));
                    continue;
                }
                // Try with $ prefix
                std::string dollarName = "$" + name;
                pos = turso_statement_named_position(statement_, dollarName.c_str());
                if (pos > 0) {
                    bindValue(rt, pos, obj.getProperty(rt, name.c_str()));
                }
            }
            return;
        }
    }

    // Positional parameters
    for (size_t i = 0; i < count; i++) {
        bindValue(rt, i + 1, args[i]);
    }
}

jsi::Value StatementHostObject::bind(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (finalized_) {
        throw jsi::JSError(rt, "Statement is finalized");
    }

    bindParams(rt, args, count);
    return jsi::Value::undefined();
}

jsi::Value StatementHostObject::run(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (finalized_) {
        throw jsi::JSError(rt, "Statement is finalized");
    }

    // Reset and bind parameters
    turso_statement_reset(statement_, nullptr);
    boundStrings_.clear(); // Clear previous bound strings
    bindParams(rt, args, count);

    // Execute
    const char *error = nullptr;
    uint64_t rowsChanged = 0;
    turso_status_code_t status = turso_statement_execute(statement_, &rowsChanged, &error);

    while (status == TURSO_IO) {
        turso_statement_run_io(statement_, nullptr);
        status = turso_statement_execute(statement_, &rowsChanged, &error);
    }

    if (status != TURSO_DONE && status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to execute statement";
        if (error) turso_str_deinit(error);
        throw jsi::JSError(rt, errorMsg);
    }

    int64_t changes = turso_statement_n_change(statement_);
    int64_t lastId = turso_connection_last_insert_rowid(connection_);

    // Return { changes, lastInsertRowid }
    jsi::Object result(rt);
    result.setProperty(rt, "changes", jsi::Value(static_cast<double>(changes)));
    result.setProperty(rt, "lastInsertRowid", jsi::Value(static_cast<double>(lastId)));
    return result;
}

jsi::Value StatementHostObject::rowToObject(jsi::Runtime &rt) {
    int64_t colCount = turso_statement_column_count(statement_);
    jsi::Object row(rt);

    for (int64_t i = 0; i < colCount; i++) {
        const char *colName = turso_statement_column_name(statement_, i);
        std::string name = colName ? colName : "";
        if (colName) turso_str_deinit(colName);

        turso_type_t type = turso_statement_row_value_kind(statement_, i);
        jsi::Value value;

        switch (type) {
            case TURSO_TYPE_NULL:
                value = jsi::Value::null();
                break;
            case TURSO_TYPE_INTEGER:
                value = jsi::Value(static_cast<double>(turso_statement_row_value_int(statement_, i)));
                break;
            case TURSO_TYPE_REAL:
                value = jsi::Value(turso_statement_row_value_double(statement_, i));
                break;
            case TURSO_TYPE_TEXT: {
                const char *ptr = turso_statement_row_value_bytes_ptr(statement_, i);
                int64_t len = turso_statement_row_value_bytes_count(statement_, i);
                if (ptr && len >= 0) {
                    value = jsi::String::createFromUtf8(rt, reinterpret_cast<const uint8_t*>(ptr), len);
                } else {
                    value = jsi::Value::null();
                }
                break;
            }
            case TURSO_TYPE_BLOB: {
                const char *ptr = turso_statement_row_value_bytes_ptr(statement_, i);
                int64_t len = turso_statement_row_value_bytes_count(statement_, i);
                if (ptr && len >= 0) {
                    jsi::ArrayBuffer buffer = rt.global()
                        .getPropertyAsFunction(rt, "ArrayBuffer")
                        .callAsConstructor(rt, static_cast<int>(len))
                        .asObject(rt)
                        .getArrayBuffer(rt);
                    memcpy(buffer.data(rt), ptr, len);
                    value = std::move(buffer);
                } else {
                    value = jsi::Value::null();
                }
                break;
            }
            default:
                value = jsi::Value::null();
                break;
        }

        row.setProperty(rt, name.c_str(), std::move(value));
    }

    return row;
}

jsi::Value StatementHostObject::getOne(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (finalized_) {
        throw jsi::JSError(rt, "Statement is finalized");
    }

    // Reset and bind parameters
    turso_statement_reset(statement_, nullptr);
    boundStrings_.clear(); // Clear previous bound strings
    bindParams(rt, args, count);

    // Step to get a row
    const char *error = nullptr;
    jsi::Value result = jsi::Value::undefined();
    bool gotRow = false;

    while (true) {
        turso_status_code_t status = turso_statement_step(statement_, &error);

        if (status == TURSO_IO) {
            turso_statement_run_io(statement_, nullptr);
            continue;
        }

        if (status == TURSO_DONE) {
            break;
        }

        if (status == TURSO_ROW && !gotRow) {
            result = rowToObject(rt);
            gotRow = true;
            continue;
        }

        if (status != TURSO_ROW) {
            if (error) {
                std::string errorMsg = error;
                turso_str_deinit(error);
                throw jsi::JSError(rt, errorMsg);
            }
            break;
        }
    }

    return result;
}

jsi::Value StatementHostObject::getAll(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (finalized_) {
        throw jsi::JSError(rt, "Statement is finalized");
    }

    // Reset and bind parameters
    turso_statement_reset(statement_, nullptr);
    boundStrings_.clear(); // Clear previous bound strings
    bindParams(rt, args, count);

    // Collect all rows
    const char *error = nullptr;
    std::vector<jsi::Value> rows;

    while (true) {
        turso_status_code_t status = turso_statement_step(statement_, &error);

        if (status == TURSO_IO) {
            turso_statement_run_io(statement_, nullptr);
            continue;
        }

        if (status == TURSO_DONE) {
            break;
        }

        if (status == TURSO_ROW) {
            rows.push_back(rowToObject(rt));
            continue;
        }

        if (error) {
            std::string errorMsg = error;
            turso_str_deinit(error);
            throw jsi::JSError(rt, errorMsg);
        }
        break;
    }

    // Convert to JS array
    jsi::Array result = jsi::Array(rt, rows.size());
    for (size_t i = 0; i < rows.size(); i++) {
        result.setValueAtIndex(rt, i, std::move(rows[i]));
    }

    return result;
}

jsi::Value StatementHostObject::finalize(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (!finalized_ && statement_) {
        turso_statement_finalize(statement_, nullptr);
        turso_statement_deinit(statement_);
        statement_ = nullptr;
        finalized_ = true;
    }
    return jsi::Value::undefined();
}

jsi::Value StatementHostObject::reset(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (finalized_) {
        throw jsi::JSError(rt, "Statement is finalized");
    }

    turso_statement_reset(statement_, nullptr);
    boundStrings_.clear(); // Clear bound strings on reset
    return jsi::Value::undefined();
}

jsi::Value StatementHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    std::string propName = name.utf8(rt);

    if (propName == "bind") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->bind(rt, args, count);
            }
        );
    }
    if (propName == "run") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->run(rt, args, count);
            }
        );
    }
    if (propName == "get") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->getOne(rt, args, count);
            }
        );
    }
    if (propName == "all") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->getAll(rt, args, count);
            }
        );
    }
    if (propName == "finalize") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->finalize(rt, args, count);
            }
        );
    }
    if (propName == "reset") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->reset(rt, args, count);
            }
        );
    }

    return jsi::Value::undefined();
}

void StatementHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only
}

std::vector<jsi::PropNameID> StatementHostObject::getPropertyNames(jsi::Runtime &rt) {
    std::vector<jsi::PropNameID> props;
    props.push_back(jsi::PropNameID::forAscii(rt, "bind"));
    props.push_back(jsi::PropNameID::forAscii(rt, "run"));
    props.push_back(jsi::PropNameID::forAscii(rt, "get"));
    props.push_back(jsi::PropNameID::forAscii(rt, "all"));
    props.push_back(jsi::PropNameID::forAscii(rt, "finalize"));
    props.push_back(jsi::PropNameID::forAscii(rt, "reset"));
    return props;
}

} // namespace turso
