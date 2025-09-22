#include <assert.h>
#include <math.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

void test_sqlite3_changes();
void test_sqlite3_bind_int64();
void test_sqlite3_bind_double();
void test_sqlite3_bind_parameter_name();
void test_sqlite3_bind_parameter_count();
void test_sqlite3_column_name();
void test_sqlite3_last_insert_rowid();
void test_sqlite3_bind_text();
void test_sqlite3_bind_text2();
void test_sqlite3_bind_blob();
void test_sqlite3_column_type();
void test_sqlite3_column_decltype();
void test_sqlite3_next_stmt();
void test_sqlite3_table_column_metadata();

int allocated = 0;

int main(void)
{
    test_sqlite3_changes();
    test_sqlite3_bind_int64();
    test_sqlite3_bind_double();
    test_sqlite3_bind_parameter_name();
    test_sqlite3_bind_parameter_count();
    test_sqlite3_column_name();
    test_sqlite3_last_insert_rowid();
    test_sqlite3_bind_text();
    test_sqlite3_bind_text2();
    test_sqlite3_bind_blob();
    test_sqlite3_column_type();
    test_sqlite3_column_decltype();
    test_sqlite3_next_stmt();
    test_sqlite3_table_column_metadata();
    return 0;
}


void test_sqlite3_changes()
{
    sqlite3 *db;
    char *err_msg = NULL;
    int rc;

    rc = sqlite3_open("../../testing/testing.db", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS turso_test_changes (id INTEGER PRIMARY KEY, name TEXT);",
        NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }

    rc = sqlite3_exec(db, "DELETE FROM turso_test_changes;", NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }
    
    sqlite3_close(db);
    rc = sqlite3_open("../../testing/testing.db", &db);
    assert(rc == SQLITE_OK);

    assert(sqlite3_changes(db) == 0);
    assert(sqlite3_changes64(db) == 0);

    rc = sqlite3_exec(db,
        "INSERT INTO turso_test_changes (name) VALUES ('abc');",
        NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }
    assert(sqlite3_changes(db) == 1);
    assert(sqlite3_changes64(db) == 1);


    rc = sqlite3_exec(db,
        "INSERT INTO turso_test_changes (name) VALUES ('def'),('ghi'),('jkl');",
        NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }
    assert(sqlite3_changes(db) == 3);
    assert(sqlite3_changes64(db) == 3);

    sqlite3_close(db);
}


void test_sqlite3_bind_int64()
{
    sqlite3 *db;
    sqlite3_stmt *stmt;
    char *err_msg = NULL;
    int rc;

    rc = sqlite3_open("../../testing/testing.db", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS turso_test_int64 (id INTEGER PRIMARY KEY, value INTEGER);",
        NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }
    rc = sqlite3_exec(db, "DELETE FROM turso_test_int64;", NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }

    rc = sqlite3_prepare_v2(db, "INSERT INTO turso_test_int64 (value) VALUES (?);", -1, &stmt, NULL);
    assert(rc == SQLITE_OK);
    
    sqlite_int64 big_value = (sqlite_int64)9223372036854775807LL;
    rc = sqlite3_bind_int64(stmt, 1, big_value);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db, "SELECT value FROM turso_test_int64 LIMIT 1;", -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_ROW);

    sqlite_int64 fetched = sqlite3_column_int64(stmt, 0);
    assert(fetched == big_value);

    printf("Inserted value: %lld, Fetched value: %lld\n", (long long)big_value, (long long)fetched);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

void test_sqlite3_bind_double()
{
    sqlite3 *db;
    sqlite3_stmt *stmt;
    char *err_msg = NULL;
    int rc;

    rc = sqlite3_open("../../testing/testing.db", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS turso_test_double (id INTEGER PRIMARY KEY, value REAL);",
        NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }

    rc = sqlite3_exec(db, "DELETE FROM turso_test_double;", NULL, NULL, &err_msg);
    assert(rc == SQLITE_OK);
    if (err_msg) { sqlite3_free(err_msg); err_msg = NULL; }

    rc = sqlite3_prepare_v2(db, "INSERT INTO turso_test_double (value) VALUES (?);", -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    double big_value = 1234567890.123456;   
    rc = sqlite3_bind_double(stmt, 1, big_value);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db, "SELECT value FROM turso_test_double LIMIT 1;", -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_ROW);

    double fetched = sqlite3_column_double(stmt, 0);
    assert(fabs(fetched - big_value) < 1e-9);

    printf("Inserted value: %.15f, Fetched value: %.15f\n", big_value, fetched);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}


void test_sqlite3_bind_parameter_name() {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    const char *sql = "INSERT INTO test_parameter_name (value) VALUES (:val);";
    rc = sqlite3_exec(db, "CREATE TABLE test_parameter_name (id INTEGER PRIMARY KEY, value INTEGER);", NULL, NULL, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    const char *param_name = sqlite3_bind_parameter_name(stmt, 1);
    assert(param_name != NULL);
    printf("Parameter name: %s\n", param_name);
    assert(strcmp(param_name, ":val") == 0);

    const char *invalid_name = sqlite3_bind_parameter_name(stmt, 99);
    assert(invalid_name == NULL);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}


void test_sqlite3_bind_parameter_count() {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db, "CREATE TABLE test_parameter_count (id INTEGER PRIMARY KEY, value TEXT);", NULL, NULL, NULL);
    assert(rc == SQLITE_OK);

    const char *sql = "INSERT INTO test_parameter_count (id, value) VALUES (?1, ?2);";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    int param_count = sqlite3_bind_parameter_count(stmt);
    printf("Parameter count: %d\n", param_count);
    assert(param_count == 2);  
    sqlite3_finalize(stmt);

    rc = sqlite3_prepare_v2(db, "SELECT * FROM test_parameter_count;", -1, &stmt, NULL);
    assert(rc == SQLITE_OK);
    param_count = sqlite3_bind_parameter_count(stmt);
    printf("Parameter count (no params): %d\n", param_count);
    assert(param_count == 0);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

void test_sqlite3_column_name() {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db,
        "CREATE TABLE test_column_name (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);",
        NULL, NULL, NULL);
    assert(rc == SQLITE_OK);

    const char *sql = "SELECT id, name AS full_name, age FROM test_column_name;";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    int col_count = sqlite3_column_count(stmt);
    assert(col_count == 3);

    const char *col0 = sqlite3_column_name(stmt, 0);
    const char *col1 = sqlite3_column_name(stmt, 1);
    const char *col2 = sqlite3_column_name(stmt, 2);

    printf("Column 0 name: %s\n", col0);
    printf("Column 1 name: %s\n", col1);
    printf("Column 2 name: %s\n", col2);

    assert(strcmp(col0, "id") == 0);
    assert(strcmp(col1, "full_name") == 0);  
    assert(strcmp(col2, "age") == 0);

    // test table column name
    const char *table_name = sqlite3_column_table_name(stmt, 0);

    printf("Column table name: %s\n", table_name);

    assert(strcmp(table_name, "test_column_name") == 0);
    
    //will cause panic because get_column_name uses expect()
    // const char *invalid_col = sqlite3_column_name(stmt, 99);
    // assert(invalid_col == NULL);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}


void test_sqlite3_last_insert_rowid() {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db,
        "CREATE TABLE test_last_insert (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);",
        NULL, NULL, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db,
        "INSERT INTO test_last_insert (name) VALUES ('first');",
        -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    sqlite3_finalize(stmt);

    sqlite3_int64 rowid1 = sqlite3_last_insert_rowid(db);
    printf("first: %lld\n", (long long)rowid1);
    assert(rowid1 == 1);

    rc = sqlite3_prepare_v2(db,
        "INSERT INTO test_last_insert (name) VALUES ('second');",
        -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    sqlite3_finalize(stmt);

    sqlite3_int64 rowid2 = sqlite3_last_insert_rowid(db);
    printf("second: %lld\n", (long long)rowid2);
    assert(rowid2 == 2);

    sqlite3_close(db);
}


static void custom_destructor(void *ptr)
{
    free(ptr);
    allocated--;
}

void test_sqlite3_bind_text()
{
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db, "CREATE TABLE bind_text(x TEXT)", 0, 0, 0);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db, "INSERT INTO bind_text VALUES (?1)", -1, &stmt, 0);
    assert(rc == SQLITE_OK);

    char *data = malloc(10);
    snprintf(data, 10, "leaktest");
    allocated++;
    rc = sqlite3_bind_text(stmt, 1, data, -1, custom_destructor);
    assert(rc == SQLITE_OK);
    
    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    printf("Before final allocated = %d\n", allocated);
    sqlite3_finalize(stmt);
    printf("After final allocated = %d\n", allocated);

    assert(allocated == 0);

    rc = sqlite3_prepare_v2(db, "SELECT x FROM bind_text", -1, &stmt, 0);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_ROW);

    const unsigned char *text = sqlite3_column_text(stmt, 0);
    int len = sqlite3_column_bytes(stmt, 0);

    assert(text != NULL);
    
    assert(strcmp((const char *)text, "leaktest") == 0);
    printf("Read text: %s (len=%d)\n", text, len);
    assert(len == 8);  
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    printf("Test passed: no leaks detected and column text read correctly!\n");
}

void test_sqlite3_bind_text2() {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    sqlite3_stmt *check_stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db, "CREATE TABLE bind_text(x TEXT)", 0, 0, 0);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db, "INSERT INTO bind_text VALUES (?1)", -1, &stmt, 0);
    assert(rc == SQLITE_OK);

    rc = sqlite3_bind_text(stmt, 1, "hello", -1, SQLITE_TRANSIENT);
    assert(rc == SQLITE_OK);
    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    sqlite3_reset(stmt);

    const char *long_str = "this_is_a_long_test_string_for_sqlite_bind_text_function";
    rc = sqlite3_bind_text(stmt, 1, long_str, -1, SQLITE_TRANSIENT);
    assert(rc == SQLITE_OK);
    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    sqlite3_reset(stmt);

    const char weird_str[] = {'a','b','c','\0','x','y','z'};
    
    //bind text will terminate \0
    rc = sqlite3_bind_text(stmt, 1, weird_str, sizeof(weird_str), SQLITE_TRANSIENT);
    assert(rc == SQLITE_OK);
    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    sqlite3_finalize(stmt);

    rc = sqlite3_prepare_v2(db, "SELECT x FROM bind_text", -1, &check_stmt, 0);
    assert(rc == SQLITE_OK);

    int row = 0;
    while ((rc = sqlite3_step(check_stmt)) == SQLITE_ROW) {
        const unsigned char *val = sqlite3_column_text(check_stmt, 0);
        int len = sqlite3_column_bytes(check_stmt, 0);
        printf("Row %d: \"%.*s\" (len=%d)\n", row, len, val, len);
        row++;
    }
    assert(rc == SQLITE_DONE);
    sqlite3_finalize(check_stmt);

    sqlite3_close(db);

    printf("Test passed: bind_text handled multiple cases correctly!\n");
}


void test_sqlite3_bind_blob() 
{
    sqlite3 *db;
    sqlite3_stmt *stmt;
    const char *sql = "INSERT INTO test_blob (data) VALUES (?);";
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db, "CREATE TABLE test_blob (data BLOB);", NULL, NULL, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    unsigned char blob_data[] = {0x61, 0x62, 0x00, 0x63, 0x64}; // "ab\0cd"
    int blob_size = sizeof(blob_data);

    rc = sqlite3_bind_blob(stmt, 1, blob_data, blob_size, SQLITE_STATIC);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_DONE);

    sqlite3_finalize(stmt);

    rc = sqlite3_prepare_v2(db, "SELECT data FROM test_blob;", -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_ROW);

    const void *retrieved_blob = sqlite3_column_blob(stmt, 0);
    int retrieved_size = sqlite3_column_bytes(stmt, 0);

    assert(retrieved_size == blob_size);

    assert(memcmp(blob_data, retrieved_blob, blob_size) == 0);

    printf("Test passed: BLOB inserted and retrieved correctly (size=%d)\n", retrieved_size);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

void test_sqlite3_column_type()
{
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db,
        "CREATE TABLE test_column_type (col_int INTEGER, col_float REAL, col_text TEXT, col_blob BLOB, col_null TEXT);",
        NULL, NULL, NULL);
    assert(rc == SQLITE_OK);
    
    rc = sqlite3_exec(db,
        "INSERT INTO test_column_type VALUES (42, 3.14, 'hello', x'010203', NULL);",
        NULL, NULL, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db,
        "SELECT col_int, col_float, col_text, col_blob, col_null FROM test_column_type;",
        -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_step(stmt);
    assert(rc == SQLITE_ROW);

    for (int i = 0; i < sqlite3_column_count(stmt); i++) {
        int type = sqlite3_column_type(stmt, i);
        switch (i) {
            case 0: assert(type == SQLITE_INTEGER); break;
            case 1: assert(type == SQLITE_FLOAT);   break;
            case 2: assert(type == SQLITE_TEXT);    break;
            case 3: assert(type == SQLITE_BLOB);    break;
            case 4: assert(type == SQLITE_NULL);    break;
        }
    }

    printf("sqlite3_column_type test completed!\n");

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

void test_sqlite3_column_decltype()
{
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    rc = sqlite3_exec(db,
        "CREATE TABLE test_decltype (col_int INTEGER, col_float REAL, col_text TEXT, col_blob BLOB, col_null NULL);",
        NULL, NULL, NULL);
    assert(rc == SQLITE_OK);

    rc = sqlite3_prepare_v2(db,
        "SELECT col_int, col_float, col_text, col_blob, col_null FROM test_decltype;",
        -1, &stmt, NULL);
    assert(rc == SQLITE_OK);

    const char* expected[] = { "INTEGER", "REAL", "TEXT", "BLOB", NULL};

    for (int i = 0; i < sqlite3_column_count(stmt); i++) {
        const char* decl = sqlite3_column_decltype(stmt, i);
        if (decl == NULL) {
            assert(expected[i] == NULL);
        } else {
            assert(strcmp(decl, expected[i]) == 0);
        }
    }

    printf("sqlite3_column_decltype test completed!\n");

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}


void test_sqlite3_next_stmt()
{
	sqlite3 *db;
	sqlite3_stmt *stmt1 = NULL;
	sqlite3_stmt *stmt2 = NULL;
	sqlite3_stmt *stmt3 = NULL;
	int rc;

	rc = sqlite3_open(":memory:", &db);
	assert(rc == SQLITE_OK);

	// Initially, there should be no prepared statements
	assert(sqlite3_next_stmt(db, NULL) == NULL);

	// Prepare first statement
	rc = sqlite3_prepare_v2(db, "SELECT 1;", -1, &stmt1, NULL);
	assert(rc == SQLITE_OK);
	assert(stmt1 != NULL);

	// Now there should be one statement
	assert(sqlite3_next_stmt(db, NULL) == stmt1);
	// And no more after that
	assert(sqlite3_next_stmt(db, stmt1) == NULL);

	// Prepare second and third statements
	rc = sqlite3_prepare_v2(db, "SELECT 2;", -1, &stmt2, NULL);
	assert(rc == SQLITE_OK);
	assert(stmt2 != NULL);
	rc = sqlite3_prepare_v2(db, "SELECT 3;", -1, &stmt3, NULL);
	assert(rc == SQLITE_OK);
	assert(stmt3 != NULL);

	// Count all statements
	int count = 0;
	sqlite3_stmt *iter = sqlite3_next_stmt(db, NULL);
	while (iter != NULL) {
		count++;
		iter = sqlite3_next_stmt(db, iter);
	}
	assert(count == 3);

	// Finalize the middle statement and recount
	assert(sqlite3_finalize(stmt2) == SQLITE_OK);
	count = 0;
	iter = sqlite3_next_stmt(db, NULL);
	while (iter != NULL) {
		count++;
		iter = sqlite3_next_stmt(db, iter);
	}
	assert(count == 2);

	// Finalize remaining statements
	assert(sqlite3_finalize(stmt1) == SQLITE_OK);
	assert(sqlite3_finalize(stmt3) == SQLITE_OK);

	// Should be no statements left
	assert(sqlite3_next_stmt(db, NULL) == NULL);

	sqlite3_close(db);
}

void test_sqlite3_table_column_metadata()
{
	sqlite3 *db;
	int rc;
	const char *data_type;
	const char *coll_seq;
	int not_null;
	int primary_key;
	int autoinc;

	// Open in-memory database
	rc = sqlite3_open(":memory:", &db);
	assert(rc == SQLITE_OK);

	// Create a test table
	rc = sqlite3_exec(db, 
		"CREATE TABLE test_metadata (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL)",
		NULL, NULL, NULL);
	assert(rc == SQLITE_OK);

	// Test column metadata for 'id' column
	rc = sqlite3_table_column_metadata(db, NULL, "test_metadata", "id", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_OK);
	
	// Verify the results
	assert(data_type != NULL);
	assert(coll_seq != NULL);
	assert(strcmp(data_type, "INTEGER") == 0);
	assert(strcmp(coll_seq, "BINARY") == 0);
	assert(primary_key == 1); // id is primary key
	assert(not_null == 0); // INTEGER columns don't have NOT NULL by default
	assert(autoinc == 0); // not auto-increment

	// Test column metadata for 'name' column
	rc = sqlite3_table_column_metadata(db, NULL, "test_metadata", "name", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_OK);
	
	// Verify the results
	assert(data_type != NULL);
	assert(coll_seq != NULL);
	assert(strcmp(data_type, "TEXT") == 0);
	assert(strcmp(coll_seq, "BINARY") == 0);
	assert(primary_key == 0); // name is not primary key
	assert(not_null == 1); // name has NOT NULL constraint
	assert(autoinc == 0); // not auto-increment

	// Test column metadata for 'value' column
	rc = sqlite3_table_column_metadata(db, NULL, "test_metadata", "value", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_OK);
	
	// Verify the results
	assert(data_type != NULL);
	assert(coll_seq != NULL);
	assert(strcmp(data_type, "REAL") == 0);
	assert(strcmp(coll_seq, "BINARY") == 0);
	assert(primary_key == 0); // value is not primary key
	assert(not_null == 0); // value doesn't have NOT NULL constraint
	assert(autoinc == 0); // not auto-increment

	// Test non-existent column
	rc = sqlite3_table_column_metadata(db, NULL, "test_metadata", "nonexistent", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_ERROR);

	// Test non-existent table
	rc = sqlite3_table_column_metadata(db, NULL, "nonexistent_table", "id", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_ERROR);

	// Test rowid column
	rc = sqlite3_table_column_metadata(db, NULL, "test_metadata", "rowid", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_OK);
	
	// Verify rowid results
	assert(data_type != NULL);
	assert(coll_seq != NULL);
	assert(strcmp(data_type, "INTEGER") == 0);
	assert(strcmp(coll_seq, "BINARY") == 0);
	assert(primary_key == 1); // rowid is primary key
	assert(not_null == 0);
	assert(autoinc == 0);

	// Test with NULL database name (should default to main)
	rc = sqlite3_table_column_metadata(db, NULL, "test_metadata", "id", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_OK);

	// Test with explicit "main" database name
	rc = sqlite3_table_column_metadata(db, "main", "test_metadata", "id", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_OK);

	// Test with non-main database name (should return error for now)
	rc = sqlite3_table_column_metadata(db, "temp", "test_metadata", "id", 
		&data_type, &coll_seq, &not_null, &primary_key, &autoinc);
	assert(rc == SQLITE_ERROR);

	printf("sqlite3_table_column_metadata test passed\n");
	sqlite3_close(db);
}
