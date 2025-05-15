# DSL

## test and test_error functions

To create a SQL test, you need to invoke the test or the test error function

test(TestKind, Name, Statement(s), Expected Output)

test_error(TestKind, Name, Statement(s))

## Ignore tests

To ignore a test annotate it like this:

```
#[ignore = "message"]
test(
        pragma_table_info_equal_syntax,
        "PRAGMA table_info=sqlite_schema",
        [
            [0, "type", "TEXT", 0, Null, 0],
            [1, "name", "TEXT", 0, Null, 0],
            [2, "tbl_name", "TEXT", 0, Null, 0],
            [3, "rootpage", "INT", 0, Null, 0],
            [4, "sql", "TEXT", 0, Null, 0]
        ]
    )
```

## Comments
You may add comments before a test like this: 

```
// Comment 1
// Comment 2
test(
        pragma_table_info_equal_syntax,
        "PRAGMA table_info=sqlite_schema",
        [
            [0, "type", "TEXT", 0, Null, 0],
            [1, "name", "TEXT", 0, Null, 0],
            [2, "tbl_name", "TEXT", 0, Null, 0],
            [3, "rootpage", "INT", 0, Null, 0],
            [4, "sql", "TEXT", 0, Null, 0]
        ]
    )
```

### TestKinds
- Specific Databases -> [database_path, database_path, ...]
- Memory -> memory
- Regex -> r"\d+\.\d+\.\d+"
When you omit the TestKind argument, the test defaults to using the default databases you select

### Statement(s)

- Single -> "SELECT count(*) FROM demo WHERE value = NULL"
- Many -> [
            "CREATE TABLE temp (a, b, c)",
            "INSERT INTO temp VALUES (1, 2, 3)",
            "INSERT INTO temp VALUES (4, 5, 6)",
            "UPDATE temp SET a = 10, b = 20, c = 30",
            "SELECT * FROM temp"
          ]

### Output

Single Value -> "hi"
Single Row -> [0, x"FE10", 1.4]
Rows and Columsn -> [
                        [0, "type", "TEXT", 0, Null, 0],
                        [1, "name", "TEXT", 0, Null, 0],
                        [2, "tbl_name", "TEXT", 0, Null, 0],
                        [3, "rootpage", "INT", 0, Null, 0],
                        [4, "sql", "TEXT", 0, Null, 0]
                    ]

 This is the list of allowed values:

- **boolean** -> converted to `Value::Integer`
- **integer** -> converted to `Value::Integer`
- **float** -> converted to `Value::Real`
- **string literals** -> converted to `Value::Text`
- **byte string** (e.g x"example") -> converted to `Value::Blob` and compile checked with `hex` crate to ensure it is a valid hex value
- **`Null`** identifier -> converted to `Value::Null`
- **`Inf`** identifier -> converted to `Value::Real(std::f64::INFINITY)`
- **`-Inf`** identifier -> converted to `Value::Real(std::f64::NEG_INFINITY)`
- **`None`** identifier -> converted to an Empty `Vec<Vec<Value>>`

#### Custom Databases
```
    test(
        ["testing/testing_small.db"],
        where_equals_null,
        "SELECT count(*) FROM demo WHERE value = NULL",
        0
    )
```

#### Memory test with many statements
```
    test(
        memory,
        update_multiple_columns,
        [
            "CREATE TABLE temp (a, b, c)",
            "INSERT INTO temp VALUES (1, 2, 3)",
            "INSERT INTO temp VALUES (4, 5, 6)",
            "UPDATE temp SET a = 10, b = 20, c = 30",
            "SELECT * FROM temp"
        ],
        [[10, 20, 30], [10, 20, 30]]
    )
```

#### Regex
```
    test(
        r"\d+\.\d+\.\d+",
        regex_sqlite_version_should_return_valid_output,
        "SELECT sqlite_version()"
    )
```

#### No output
```
    test(pragma_table_info_invalid_table, "PRAGMA table_info=pekka")
```

#### Test with Default Databases
```
    test(
        pragma_table_info_equal_syntax,
        "PRAGMA table_info=sqlite_schema",
        [
            [0, "type", "TEXT", 0, Null, 0],
            [1, "name", "TEXT", 0, Null, 0],
            [2, "tbl_name", "TEXT", 0, Null, 0],
            [3, "rootpage", "INT", 0, Null, 0],
            [4, "sql", "TEXT", 0, Null, 0]
        ]
    )
```

#### Memory Expect Error
```
    test_error(
        memory,
        strict_require_datatype,
        ["CREATE TABLE test2(id INTEGER, name) STRICT"]
    )
```

## Define expected values

TODO
