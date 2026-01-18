#include "DBHostObject.h"
#include "StatementHostObject.h"

// Include the Turso C API header
extern "C" {
#include "turso.h"
}

namespace turso {

using namespace facebook;

DBHostObject::DBHostObject(const std::string &path, const std::string &basePath) : path_(path) {
    // Determine full path
    if (path == ":memory:") {
        fullPath_ = ":memory:";
    } else if (path[0] == '/') {
        // Absolute path
        fullPath_ = path;
    } else {
        // Relative to base path
        fullPath_ = basePath + "/" + path;
    }

    // Create database config
    turso_database_config_t config = {};
    config.path = fullPath_.c_str();
    config.async_io = 0; // Synchronous I/O for React Native

    // Create database
    const char *error = nullptr;
    turso_status_code_t status = turso_database_new(&config, const_cast<const turso_database_t**>(&database_), &error);
    if (status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to create database";
        if (error) turso_str_deinit(error);
        throw std::runtime_error(errorMsg);
    }

    // Open database
    status = turso_database_open(database_, &error);
    if (status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to open database";
        if (error) turso_str_deinit(error);
        turso_database_deinit(database_);
        database_ = nullptr;
        throw std::runtime_error(errorMsg);
    }

    // Create connection
    status = turso_database_connect(database_, &connection_, &error);
    if (status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to connect to database";
        if (error) turso_str_deinit(error);
        turso_database_deinit(database_);
        database_ = nullptr;
        throw std::runtime_error(errorMsg);
    }

    isOpen_ = true;
}

DBHostObject::~DBHostObject() {
    close();
}

void DBHostObject::close() {
    if (connection_) {
        turso_connection_close(connection_, nullptr);
        turso_connection_deinit(connection_);
        connection_ = nullptr;
    }
    if (database_) {
        turso_database_deinit(database_);
        database_ = nullptr;
    }
    isOpen_ = false;
}

void DBHostObject::throwError(jsi::Runtime &rt, const char *error) {
    throw jsi::JSError(rt, error ? error : "Unknown database error");
}

int64_t DBHostObject::getLastInsertRowId() const {
    if (!connection_) return 0;
    return turso_connection_last_insert_rowid(connection_);
}

int64_t DBHostObject::getChanges() const {
    // Note: Changes are tracked per-statement, not per-connection in Turso
    // This would need a separate tracking mechanism
    return 0;
}

bool DBHostObject::getAutocommit() const {
    if (!connection_) return true;
    return turso_connection_get_autocommit(connection_);
}

jsi::Value DBHostObject::prepare(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (!isOpen_) {
        throw jsi::JSError(rt, "Database is closed");
    }
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "prepare() requires a SQL string");
    }

    std::string sql = args[0].asString(rt).utf8(rt);

    turso_statement_t *statement = nullptr;
    const char *error = nullptr;
    turso_status_code_t status = turso_connection_prepare_single(
        connection_, sql.c_str(), &statement, &error
    );

    if (status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to prepare statement";
        if (error) turso_str_deinit(error);
        throw jsi::JSError(rt, errorMsg);
    }

    auto stmtObj = std::make_shared<StatementHostObject>(statement, connection_);
    return jsi::Object::createFromHostObject(rt, stmtObj);
}

jsi::Value DBHostObject::exec(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (!isOpen_) {
        throw jsi::JSError(rt, "Database is closed");
    }
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "exec() requires a SQL string");
    }

    std::string sql = args[0].asString(rt).utf8(rt);
    const char *remaining = sql.c_str();
    size_t offset = 0;

    // Execute all statements in the SQL string
    while (offset < sql.length()) {
        turso_statement_t *statement = nullptr;
        size_t tailIdx = 0;
        const char *error = nullptr;

        turso_status_code_t status = turso_connection_prepare_first(
            connection_, remaining, &statement, &tailIdx, &error
        );

        if (status != TURSO_OK) {
            std::string errorMsg = error ? error : "Failed to prepare statement";
            if (error) turso_str_deinit(error);
            throw jsi::JSError(rt, errorMsg);
        }

        if (statement == nullptr) {
            // No more statements
            break;
        }

        // Execute the statement
        uint64_t rowsChanged = 0;
        status = turso_statement_execute(statement, &rowsChanged, &error);

        // Handle async I/O if needed
        while (status == TURSO_IO) {
            turso_statement_run_io(statement, nullptr);
            status = turso_statement_execute(statement, &rowsChanged, &error);
        }

        turso_statement_deinit(statement);

        if (status != TURSO_DONE && status != TURSO_OK) {
            std::string errorMsg = error ? error : "Failed to execute statement";
            if (error) turso_str_deinit(error);
            throw jsi::JSError(rt, errorMsg);
        }

        remaining += tailIdx;
        offset += tailIdx;
    }

    return jsi::Value::undefined();
}

jsi::Value DBHostObject::run(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (!isOpen_) {
        throw jsi::JSError(rt, "Database is closed");
    }
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "run() requires a SQL string");
    }

    std::string sql = args[0].asString(rt).utf8(rt);

    turso_statement_t *statement = nullptr;
    const char *error = nullptr;
    turso_status_code_t status = turso_connection_prepare_single(
        connection_, sql.c_str(), &statement, &error
    );

    if (status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to prepare statement";
        if (error) turso_str_deinit(error);
        throw jsi::JSError(rt, errorMsg);
    }

    // Bind parameters if provided
    if (count > 1) {
        auto stmtObj = std::make_shared<StatementHostObject>(statement, connection_);
        // Bind parameters starting from index 1
        for (size_t i = 1; i < count; i++) {
            const jsi::Value &param = args[i];
            size_t bindIndex = i; // 1-based binding

            if (param.isNull() || param.isUndefined()) {
                turso_statement_bind_positional_null(statement, bindIndex);
            } else if (param.isNumber()) {
                double val = param.asNumber();
                if (val == static_cast<int64_t>(val)) {
                    turso_statement_bind_positional_int(statement, bindIndex, static_cast<int64_t>(val));
                } else {
                    turso_statement_bind_positional_double(statement, bindIndex, val);
                }
            } else if (param.isString()) {
                std::string str = param.asString(rt).utf8(rt);
                turso_statement_bind_positional_text(statement, bindIndex, str.c_str(), str.length());
            } else if (param.isBool()) {
                turso_statement_bind_positional_int(statement, bindIndex, param.getBool() ? 1 : 0);
            }
        }
    }

    // Execute
    uint64_t rowsChanged = 0;
    status = turso_statement_execute(statement, &rowsChanged, &error);

    while (status == TURSO_IO) {
        turso_statement_run_io(statement, nullptr);
        status = turso_statement_execute(statement, &rowsChanged, &error);
    }

    int64_t changes = turso_statement_n_change(statement);
    int64_t lastId = turso_connection_last_insert_rowid(connection_);

    turso_statement_deinit(statement);

    if (status != TURSO_DONE && status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to execute statement";
        if (error) turso_str_deinit(error);
        throw jsi::JSError(rt, errorMsg);
    }

    // Return { changes, lastInsertRowid }
    jsi::Object result(rt);
    result.setProperty(rt, "changes", jsi::Value(static_cast<double>(changes)));
    result.setProperty(rt, "lastInsertRowid", jsi::Value(static_cast<double>(lastId)));
    return result;
}

jsi::Value DBHostObject::getOne(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (!isOpen_) {
        throw jsi::JSError(rt, "Database is closed");
    }
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "get() requires a SQL string");
    }

    std::string sql = args[0].asString(rt).utf8(rt);

    turso_statement_t *statement = nullptr;
    const char *error = nullptr;
    turso_status_code_t status = turso_connection_prepare_single(
        connection_, sql.c_str(), &statement, &error
    );

    if (status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to prepare statement";
        if (error) turso_str_deinit(error);
        throw jsi::JSError(rt, errorMsg);
    }

    // Bind parameters if provided
    for (size_t i = 1; i < count; i++) {
        const jsi::Value &param = args[i];
        size_t bindIndex = i;

        if (param.isNull() || param.isUndefined()) {
            turso_statement_bind_positional_null(statement, bindIndex);
        } else if (param.isNumber()) {
            double val = param.asNumber();
            if (val == static_cast<int64_t>(val)) {
                turso_statement_bind_positional_int(statement, bindIndex, static_cast<int64_t>(val));
            } else {
                turso_statement_bind_positional_double(statement, bindIndex, val);
            }
        } else if (param.isString()) {
            std::string str = param.asString(rt).utf8(rt);
            turso_statement_bind_positional_text(statement, bindIndex, str.c_str(), str.length());
        } else if (param.isBool()) {
            turso_statement_bind_positional_int(statement, bindIndex, param.getBool() ? 1 : 0);
        }
    }

    // Step to get a row
    jsi::Value result = jsi::Value::undefined();
    bool gotRow = false;

    while (true) {
        status = turso_statement_step(statement, &error);

        if (status == TURSO_IO) {
            turso_statement_run_io(statement, nullptr);
            continue;
        }

        if (status == TURSO_DONE) {
            break;
        }

        if (status == TURSO_ROW && !gotRow) {
            // Get column count
            int64_t colCount = turso_statement_column_count(statement);

            // Build result object
            jsi::Object row(rt);
            for (int64_t i = 0; i < colCount; i++) {
                const char *colName = turso_statement_column_name(statement, i);
                std::string name = colName ? colName : "";
                if (colName) turso_str_deinit(colName);

                turso_type_t type = turso_statement_row_value_kind(statement, i);
                jsi::Value value;

                switch (type) {
                    case TURSO_TYPE_NULL:
                        value = jsi::Value::null();
                        break;
                    case TURSO_TYPE_INTEGER:
                        value = jsi::Value(static_cast<double>(turso_statement_row_value_int(statement, i)));
                        break;
                    case TURSO_TYPE_REAL:
                        value = jsi::Value(turso_statement_row_value_double(statement, i));
                        break;
                    case TURSO_TYPE_TEXT: {
                        const char *ptr = turso_statement_row_value_bytes_ptr(statement, i);
                        int64_t len = turso_statement_row_value_bytes_count(statement, i);
                        if (ptr && len >= 0) {
                            value = jsi::String::createFromUtf8(rt, reinterpret_cast<const uint8_t*>(ptr), len);
                        } else {
                            value = jsi::Value::null();
                        }
                        break;
                    }
                    case TURSO_TYPE_BLOB: {
                        const char *ptr = turso_statement_row_value_bytes_ptr(statement, i);
                        int64_t len = turso_statement_row_value_bytes_count(statement, i);
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

            result = std::move(row);
            gotRow = true;
            continue;
        }

        if (status != TURSO_ROW) {
            break;
        }
    }

    turso_statement_deinit(statement);
    return result;
}

jsi::Value DBHostObject::getAll(jsi::Runtime &rt, const jsi::Value *args, size_t count) {
    if (!isOpen_) {
        throw jsi::JSError(rt, "Database is closed");
    }
    if (count < 1 || !args[0].isString()) {
        throw jsi::JSError(rt, "all() requires a SQL string");
    }

    std::string sql = args[0].asString(rt).utf8(rt);

    turso_statement_t *statement = nullptr;
    const char *error = nullptr;
    turso_status_code_t status = turso_connection_prepare_single(
        connection_, sql.c_str(), &statement, &error
    );

    if (status != TURSO_OK) {
        std::string errorMsg = error ? error : "Failed to prepare statement";
        if (error) turso_str_deinit(error);
        throw jsi::JSError(rt, errorMsg);
    }

    // Bind parameters if provided
    for (size_t i = 1; i < count; i++) {
        const jsi::Value &param = args[i];
        size_t bindIndex = i;

        if (param.isNull() || param.isUndefined()) {
            turso_statement_bind_positional_null(statement, bindIndex);
        } else if (param.isNumber()) {
            double val = param.asNumber();
            if (val == static_cast<int64_t>(val)) {
                turso_statement_bind_positional_int(statement, bindIndex, static_cast<int64_t>(val));
            } else {
                turso_statement_bind_positional_double(statement, bindIndex, val);
            }
        } else if (param.isString()) {
            std::string str = param.asString(rt).utf8(rt);
            turso_statement_bind_positional_text(statement, bindIndex, str.c_str(), str.length());
        } else if (param.isBool()) {
            turso_statement_bind_positional_int(statement, bindIndex, param.getBool() ? 1 : 0);
        }
    }

    // Collect all rows
    std::vector<jsi::Object> rows;
    int64_t colCount = turso_statement_column_count(statement);
    std::vector<std::string> colNames;

    while (true) {
        status = turso_statement_step(statement, &error);

        if (status == TURSO_IO) {
            turso_statement_run_io(statement, nullptr);
            continue;
        }

        if (status == TURSO_DONE) {
            break;
        }

        if (status == TURSO_ROW) {
            // Get column names on first row
            if (colNames.empty()) {
                for (int64_t i = 0; i < colCount; i++) {
                    const char *colName = turso_statement_column_name(statement, i);
                    colNames.push_back(colName ? colName : "");
                    if (colName) turso_str_deinit(colName);
                }
            }

            // Build row object
            jsi::Object row(rt);
            for (int64_t i = 0; i < colCount; i++) {
                turso_type_t type = turso_statement_row_value_kind(statement, i);
                jsi::Value value;

                switch (type) {
                    case TURSO_TYPE_NULL:
                        value = jsi::Value::null();
                        break;
                    case TURSO_TYPE_INTEGER:
                        value = jsi::Value(static_cast<double>(turso_statement_row_value_int(statement, i)));
                        break;
                    case TURSO_TYPE_REAL:
                        value = jsi::Value(turso_statement_row_value_double(statement, i));
                        break;
                    case TURSO_TYPE_TEXT: {
                        const char *ptr = turso_statement_row_value_bytes_ptr(statement, i);
                        int64_t len = turso_statement_row_value_bytes_count(statement, i);
                        if (ptr && len >= 0) {
                            value = jsi::String::createFromUtf8(rt, reinterpret_cast<const uint8_t*>(ptr), len);
                        } else {
                            value = jsi::Value::null();
                        }
                        break;
                    }
                    case TURSO_TYPE_BLOB: {
                        const char *ptr = turso_statement_row_value_bytes_ptr(statement, i);
                        int64_t len = turso_statement_row_value_bytes_count(statement, i);
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

                row.setProperty(rt, colNames[i].c_str(), std::move(value));
            }

            rows.push_back(std::move(row));
            continue;
        }

        break;
    }

    turso_statement_deinit(statement);

    // Convert to JS array
    jsi::Array result = jsi::Array(rt, rows.size());
    for (size_t i = 0; i < rows.size(); i++) {
        result.setValueAtIndex(rt, i, std::move(rows[i]));
    }

    return result;
}

jsi::Value DBHostObject::get(jsi::Runtime &rt, const jsi::PropNameID &name) {
    std::string propName = name.utf8(rt);

    // Properties
    if (propName == "open") {
        return jsi::Value(isOpen_);
    }
    if (propName == "inTransaction") {
        return jsi::Value(!getAutocommit());
    }
    if (propName == "lastInsertRowid") {
        return jsi::Value(static_cast<double>(getLastInsertRowId()));
    }
    if (propName == "path") {
        return jsi::String::createFromUtf8(rt, fullPath_);
    }
    if (propName == "memory") {
        return jsi::Value(path_ == ":memory:");
    }

    // Methods
    if (propName == "prepare") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->prepare(rt, args, count);
            }
        );
    }
    if (propName == "exec") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->exec(rt, args, count);
            }
        );
    }
    if (propName == "run") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->run(rt, args, count);
            }
        );
    }
    if (propName == "get") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->getOne(rt, args, count);
            }
        );
    }
    if (propName == "all") {
        return jsi::Function::createFromHostFunction(
            rt, name, 1,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                return this->getAll(rt, args, count);
            }
        );
    }
    if (propName == "close") {
        return jsi::Function::createFromHostFunction(
            rt, name, 0,
            [this](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) {
                this->close();
                return jsi::Value::undefined();
            }
        );
    }

    return jsi::Value::undefined();
}

void DBHostObject::set(jsi::Runtime &rt, const jsi::PropNameID &name, const jsi::Value &value) {
    // Read-only properties
}

std::vector<jsi::PropNameID> DBHostObject::getPropertyNames(jsi::Runtime &rt) {
    std::vector<jsi::PropNameID> props;
    props.push_back(jsi::PropNameID::forAscii(rt, "open"));
    props.push_back(jsi::PropNameID::forAscii(rt, "inTransaction"));
    props.push_back(jsi::PropNameID::forAscii(rt, "lastInsertRowid"));
    props.push_back(jsi::PropNameID::forAscii(rt, "path"));
    props.push_back(jsi::PropNameID::forAscii(rt, "memory"));
    props.push_back(jsi::PropNameID::forAscii(rt, "prepare"));
    props.push_back(jsi::PropNameID::forAscii(rt, "exec"));
    props.push_back(jsi::PropNameID::forAscii(rt, "run"));
    props.push_back(jsi::PropNameID::forAscii(rt, "get"));
    props.push_back(jsi::PropNameID::forAscii(rt, "all"));
    props.push_back(jsi::PropNameID::forAscii(rt, "close"));
    return props;
}

} // namespace turso
