#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// WAT source for a simple extension with two functions and turso_ext_init.
    ///
    /// - turso_ext_init: returns a TAG_TEXT JSON manifest listing "ext_add" and "ext_const42"
    /// - ext_add(a, b): returns a + b (integers)
    /// - ext_const42(): returns i64 42
    ///
    /// The manifest JSON is:
    /// {"functions":[{"name":"ext_add","export":"ext_add","narg":2},{"name":"ext_const42","export":"ext_const42","narg":0}]}
    const EXT_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))

  ;; The manifest JSON bytes embedded as data
  (data (i32.const 2048) "{\"functions\":[{\"name\":\"ext_add\",\"export\":\"ext_add\",\"narg\":2},{\"name\":\"ext_const42\",\"export\":\"ext_const42\",\"narg\":0}]}")

  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )

  ;; turso_ext_init: returns TAG_TEXT pointer to the JSON manifest
  ;; We build [TAG_TEXT=0x03][json bytes][0x00] at the bump pointer
  (func (export "turso_ext_init") (param $argc i32) (param $argv i32) (result i64)
    (local $ptr i32)
    (local $json_len i32)
    (local $i i32)
    ;; json length = 117 bytes
    i32.const 117
    local.set $json_len

    ;; allocate 1 + json_len + 1 bytes
    global.get $bump
    local.set $ptr
    global.get $bump
    i32.const 119  ;; 1 + 117 + 1
    i32.add
    global.set $bump

    ;; write TAG_TEXT (0x03)
    local.get $ptr
    i32.const 3
    i32.store8

    ;; copy json from data segment at offset 2048
    i32.const 0
    local.set $i
    (block $break
      (loop $copy
        local.get $i
        local.get $json_len
        i32.ge_u
        br_if $break
        ;; dst[ptr+1+i] = src[2048+i]
        local.get $ptr
        i32.const 1
        i32.add
        local.get $i
        i32.add
        i32.const 2048
        local.get $i
        i32.add
        i32.load8_u
        i32.store8
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $copy
      )
    )

    ;; write null terminator
    local.get $ptr
    i32.const 1
    i32.add
    local.get $json_len
    i32.add
    i32.const 0
    i32.store8

    ;; return pointer as i64
    local.get $ptr
    i64.extend_i32_u
  )

  ;; ext_add(a, b) -> a + b
  (func (export "ext_add") (param $argc i32) (param $argv i32) (result i64)
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.add
  )

  ;; ext_const42() -> 42
  (func (export "ext_const42") (param $argc i32) (param $argv i32) (result i64)
    i64.const 42
  )
)
"#;

    fn wat_to_wasm(wat: &str) -> Vec<u8> {
        wat::parse_str(wat).expect("Failed to parse WAT")
    }

    fn wasm_to_hex(wasm: &[u8]) -> String {
        wasm.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn opts_with_wasm_runtime() -> turso_core::DatabaseOpts {
        let runtime = turso_wasm_wasmtime::WasmtimeRuntime::new().unwrap();
        turso_core::DatabaseOpts::new().with_unstable_wasm_runtime(Arc::new(runtime))
    }

    #[test]
    fn test_create_extension_from_blob() {
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("test_ext.db");

        let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Both functions from the extension should work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(40, 2)");
        assert_eq!(rows, vec![(42,)]);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_const42()");
        assert_eq!(rows, vec![(42,)]);
    }

    #[test]
    fn test_create_extension_if_not_exists() {
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // IF NOT EXISTS should not error
        let sql = format!("CREATE EXTENSION IF NOT EXISTS myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Functions still work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(5, 5)");
        assert_eq!(rows, vec![(10,)]);
    }

    #[test]
    fn test_create_extension_duplicate_errors() {
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Should error without IF NOT EXISTS
        let result = conn.execute(&sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_extension() {
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Functions work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(1, 2)");
        assert_eq!(rows, vec![(3,)]);

        // Drop the extension
        conn.execute("DROP EXTENSION myext").unwrap();

        // Functions should no longer work
        let result = conn.execute("SELECT ext_add(1, 2)");
        assert!(result.is_err());

        let result = conn.execute("SELECT ext_const42()");
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_extension_if_exists() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Should not error
        conn.execute("DROP EXTENSION IF EXISTS nonexistent")
            .unwrap();
    }

    #[test]
    fn test_drop_extension_nonexistent_errors() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let result = conn.execute("DROP EXTENSION nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_extension_persistence_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);
        let db_path = tmp.path().join("persist_ext.db");

        // First session: create extension and verify
        {
            let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
            let conn = db.connect_limbo();

            let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
            conn.execute(&sql).unwrap();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(10, 32)");
            assert_eq!(rows, vec![(42,)]);
            conn.close().unwrap();
        }

        // Second session: extension and its functions should still work
        {
            let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
            let conn = db.connect_limbo();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(100, 200)");
            assert_eq!(
                rows,
                vec![(300,)],
                "Extension functions should persist and work after database reopen"
            );

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_const42()");
            assert_eq!(rows, vec![(42,)]);
            conn.close().unwrap();
        }
    }

    #[test]
    fn test_extension_invalid_wasm_errors() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Invalid WASM bytes should produce a WASM runtime error
        let result = conn.execute("CREATE EXTENSION myext LANGUAGE wasm AS X'deadbeef'");
        assert!(result.is_err());
    }

    // ── WAT for an extension that registers types, vtabs, and index methods ──

    /// WAT for an extension that also declares a custom type and a vtab.
    /// The manifest JSON includes:
    /// - functions: ext_add, ext_const42
    /// - types: [{ name: "mytype", base: "TEXT" }]
    /// - vtabs: [{ name: "test_series", columns: [{name:"value",type:"INTEGER"},...], open/filter/column/next/eof/rowid exports }]
    /// - index_methods: [{ name: "test_im", patterns: [] }]
    const EXT_FULL_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))

  ;; Manifest JSON
  (data (i32.const 2048) "{\"functions\":[{\"name\":\"ext_add\",\"export\":\"ext_add\",\"narg\":2},{\"name\":\"ext_const42\",\"export\":\"ext_const42\",\"narg\":0}],\"types\":[{\"name\":\"mytype\",\"base\":\"TEXT\"}],\"vtabs\":[{\"name\":\"test_series\",\"columns\":[{\"name\":\"value\",\"type\":\"INTEGER\",\"hidden\":false},{\"name\":\"start_val\",\"type\":\"INTEGER\",\"hidden\":true},{\"name\":\"stop_val\",\"type\":\"INTEGER\",\"hidden\":true}],\"open\":\"ts_open\",\"filter\":\"ts_filter\",\"column\":\"ts_column\",\"next\":\"ts_next\",\"eof\":\"ts_eof\",\"rowid\":\"ts_rowid\"}],\"index_methods\":[{\"name\":\"test_im\",\"patterns\":[]}]}")

  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )

  ;; turso_ext_init: returns TAG_TEXT pointer to the JSON manifest
  (func (export "turso_ext_init") (param $argc i32) (param $argv i32) (result i64)
    (local $ptr i32)
    (local $json_len i32)
    (local $i i32)
    ;; Count the JSON length by scanning from 2048 until we find 0 or reach a max
    ;; The JSON is about 500 bytes, let's use 600 as safe upper bound
    i32.const 600
    local.set $json_len
    ;; Find the actual length
    i32.const 0
    local.set $i
    (block $done
      (loop $scan
        local.get $i
        i32.const 600
        i32.ge_u
        br_if $done
        i32.const 2048
        local.get $i
        i32.add
        i32.load8_u
        i32.eqz
        (if (then
          local.get $i
          local.set $json_len
          br $done
        ))
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $scan
      )
    )

    ;; allocate 1 + json_len + 1 bytes
    global.get $bump
    local.set $ptr
    global.get $bump
    i32.const 2
    local.get $json_len
    i32.add
    i32.add
    global.set $bump

    ;; write TAG_TEXT (0x03)
    local.get $ptr
    i32.const 3
    i32.store8

    ;; copy json from data segment at offset 2048
    i32.const 0
    local.set $i
    (block $break
      (loop $copy
        local.get $i
        local.get $json_len
        i32.ge_u
        br_if $break
        local.get $ptr
        i32.const 1
        i32.add
        local.get $i
        i32.add
        i32.const 2048
        local.get $i
        i32.add
        i32.load8_u
        i32.store8
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $copy
      )
    )

    ;; write null terminator
    local.get $ptr
    i32.const 1
    i32.add
    local.get $json_len
    i32.add
    i32.const 0
    i32.store8

    ;; return pointer as i64
    local.get $ptr
    i64.extend_i32_u
  )

  ;; ext_add(a, b) -> a + b
  (func (export "ext_add") (param $argc i32) (param $argv i32) (result i64)
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.add
  )

  ;; ext_const42() -> 42
  (func (export "ext_const42") (param $argc i32) (param $argv i32) (result i64)
    i64.const 42
  )

  ;; ─── VTab: test_series ────────────────────────────────
  ;; Simple cursor stored in linear memory at a fixed offset (3000)
  ;; Layout at offset 3000: [current:i64][stop:i64][is_eof:i32]

  ;; ts_open: return cursor handle (pointer to cursor state)
  (func (export "ts_open") (param $argc i32) (param $argv i32) (result i64)
    ;; Initialize cursor state at offset 3000
    ;; current = 0
    i32.const 3000
    i64.const 0
    i64.store
    ;; stop = 0
    i32.const 3008
    i64.const 0
    i64.store
    ;; is_eof = 1
    i32.const 3016
    i32.const 1
    i32.store
    ;; return cursor handle
    i64.const 3000
  )

  ;; ts_filter(cursor_handle, idx_num, start, stop): initialize iteration
  ;; returns 0 = has rows, 1 = empty
  (func (export "ts_filter") (param $argc i32) (param $argv i32) (result i64)
    (local $start i64)
    (local $stop i64)
    ;; Read start from argv[2]
    local.get $argv
    i32.const 16
    i32.add
    i64.load
    local.set $start
    ;; Read stop from argv[3]
    local.get $argv
    i32.const 24
    i32.add
    i64.load
    local.set $stop
    ;; Set cursor state
    i32.const 3000
    local.get $start
    i64.store
    i32.const 3008
    local.get $stop
    i64.store
    ;; is_eof = (start > stop) ? 1 : 0
    local.get $start
    local.get $stop
    i64.gt_s
    (if (then
      i32.const 3016
      i32.const 1
      i32.store
      i64.const 1
      return
    ))
    i32.const 3016
    i32.const 0
    i32.store
    i64.const 0
  )

  ;; ts_column(cursor_handle, col_idx): return column value
  (func (export "ts_column") (param $argc i32) (param $argv i32) (result i64)
    ;; Always return the current value (column 0)
    i32.const 3000
    i64.load
  )

  ;; ts_next(cursor_handle): advance cursor
  ;; Returns 0 = has more, 1 = EOF
  (func (export "ts_next") (param $argc i32) (param $argv i32) (result i64)
    (local $current i64)
    (local $stop i64)
    ;; current += 1
    i32.const 3000
    i64.load
    i64.const 1
    i64.add
    local.set $current
    i32.const 3000
    local.get $current
    i64.store
    ;; check if current > stop
    i32.const 3008
    i64.load
    local.set $stop
    local.get $current
    local.get $stop
    i64.gt_s
    (if (then
      i32.const 3016
      i32.const 1
      i32.store
      i64.const 1
      return
    ))
    i64.const 0
  )

  ;; ts_eof(cursor_handle): return 0=not_eof, 1=eof
  (func (export "ts_eof") (param $argc i32) (param $argv i32) (result i64)
    i32.const 3016
    i32.load
    i64.extend_i32_u
  )

  ;; ts_rowid(cursor_handle): return current value as rowid
  (func (export "ts_rowid") (param $argc i32) (param $argv i32) (result i64)
    i32.const 3000
    i64.load
  )
)
"#;

    #[test]
    fn test_extension_with_types_and_vtab() {
        let wasm_bytes = wat_to_wasm(EXT_FULL_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION fullext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Functions from the extension should work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(10, 20)");
        assert_eq!(rows, vec![(30,)]);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_const42()");
        assert_eq!(rows, vec![(42,)]);
    }

    #[test]
    fn test_drop_extension_cleans_all() {
        let wasm_bytes = wat_to_wasm(EXT_FULL_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION fullext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Functions work before drop
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(1, 2)");
        assert_eq!(rows, vec![(3,)]);

        // Drop the extension
        conn.execute("DROP EXTENSION fullext").unwrap();

        // Functions should no longer work
        let result = conn.execute("SELECT ext_add(1, 2)");
        assert!(result.is_err());

        let result = conn.execute("SELECT ext_const42()");
        assert!(result.is_err());
    }

    /// Pre-compiled rot13.wasm built from unmodified SQLite ext/misc/rot13.c
    /// + the universal sqlite3_wasm_shim.c.
    const ROT13_WASM: &[u8] = include_bytes!("../../wasm-sdk/examples/sqlite3/rot13.wasm");

    #[test]
    fn test_sqlite3_rot13_extension() {
        let hex = wasm_to_hex(ROT13_WASM);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION rot13ext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Basic rot13 encoding
        let rows: Vec<(String,)> = conn.exec_rows("SELECT rot13('Hello')");
        assert_eq!(rows, vec![("Uryyb".to_string(),)]);

        // Round-trip: rot13(rot13(x)) == x
        let rows: Vec<(String,)> = conn.exec_rows("SELECT rot13(rot13('Hello'))");
        assert_eq!(rows, vec![("Hello".to_string(),)]);

        // NULL passthrough — rot13(NULL) returns NULL
        {
            let mut stmt = conn.prepare("SELECT rot13(NULL)").unwrap();
            stmt.run_with_row_callback(|row| {
                assert!(
                    matches!(row.get_value(0), turso_core::Value::Null),
                    "Expected NULL, got {:?}",
                    row.get_value(0)
                );
                Ok(())
            })
            .unwrap();
        }

        // Longer text
        let rows: Vec<(String,)> = conn.exec_rows("SELECT rot13('The Quick Brown Fox')");
        assert_eq!(rows, vec![("Gur Dhvpx Oebja Sbk".to_string(),)]);

        // Empty string
        let rows: Vec<(String,)> = conn.exec_rows("SELECT rot13('')");
        assert_eq!(rows, vec![("".to_string(),)]);

        // Non-alphabetic characters pass through unchanged
        let rows: Vec<(String,)> = conn.exec_rows("SELECT rot13('123!@#')");
        assert_eq!(rows, vec![("123!@#".to_string(),)]);
    }

    #[test]
    fn test_extension_no_runtime_errors() {
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        // Database without WASM runtime
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        let result = conn.execute(&sql);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("no WASM runtime registered"),
            "Expected 'no WASM runtime registered' error, got: {err_msg}"
        );
    }

    #[test]
    fn test_pragma_function_list_shows_wasm_narg() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::new_with_existent_with_opts(
            &tmp.path().join("test.db"),
            opts_with_wasm_runtime(),
        );
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // pragma_function_list columns: name, builtin, type, enc, narg, flags
        let rows: Vec<(String, i64, String, String, i64, i64)> =
            conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'ext_add'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "ext_add");
        assert_eq!(rows[0].1, 0); // builtin = 0
        assert_eq!(rows[0].2, "s"); // scalar
        assert_eq!(rows[0].4, 2); // narg = 2

        let rows: Vec<(String, i64, String, String, i64, i64)> =
            conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'ext_const42'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "ext_const42");
        assert_eq!(rows[0].4, 0); // narg = 0
    }

    // ── Regression tests for ret_integer bug ────────────────────────────────
    //
    // ret_integer() must return tagged pointers (like ret_real, ret_text, etc.),
    // not raw i64 values. unmarshal_result interprets return values < memory_size
    // as tagged pointers, reading a tag byte from WASM memory at that address.
    // If ret_integer returns a raw small integer, the byte at that address may
    // match a tag constant, causing misinterpretation.
    //
    // The primary regression test is test_sdk_echo_int_small_values in wasm_udf.rs,
    // which uses the SDK-compiled echo_int function (exercises ret_integer directly).
    //
    // This WAT-based test verifies the tagged return protocol works correctly
    // even when tag-like bytes are planted at low WASM memory addresses.

    /// WAT module that returns properly tagged integers using the same protocol
    /// as the fixed ret_integer: allocate [TAG_INTEGER=0x01][8-byte LE i64]
    /// via turso_malloc and return the pointer.
    ///
    /// Tag-like bytes are planted at addresses 0..4 to verify that tagged
    /// returns (which point to heap addresses >= 1024) are never affected.
    const TAGGED_INT_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))

  ;; Plant tag-like bytes at addresses 0..4
  (data (i32.const 0) "\01\02\03\04\05")

  ;; Manifest: 0-arg functions const0..const4
  (data (i32.const 2048) "{\"functions\":[{\"name\":\"const0\",\"export\":\"const0\",\"narg\":0},{\"name\":\"const1\",\"export\":\"const1\",\"narg\":0},{\"name\":\"const2\",\"export\":\"const2\",\"narg\":0},{\"name\":\"const3\",\"export\":\"const3\",\"narg\":0},{\"name\":\"const4\",\"export\":\"const4\",\"narg\":0}]}")

  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )

  ;; turso_ext_init
  (func (export "turso_ext_init") (param $argc i32) (param $argv i32) (result i64)
    (local $ptr i32)
    (local $json_len i32)
    (local $i i32)
    i32.const 0
    local.set $i
    (block $done
      (loop $scan
        local.get $i
        i32.const 600
        i32.ge_u
        br_if $done
        i32.const 2048
        local.get $i
        i32.add
        i32.load8_u
        i32.eqz
        (if (then
          local.get $i
          local.set $json_len
          br $done
        ))
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $scan
      )
    )
    global.get $bump
    local.set $ptr
    global.get $bump
    i32.const 2
    local.get $json_len
    i32.add
    i32.add
    global.set $bump
    local.get $ptr
    i32.const 3
    i32.store8
    i32.const 0
    local.set $i
    (block $break
      (loop $copy
        local.get $i
        local.get $json_len
        i32.ge_u
        br_if $break
        local.get $ptr
        i32.const 1
        i32.add
        local.get $i
        i32.add
        i32.const 2048
        local.get $i
        i32.add
        i32.load8_u
        i32.store8
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $copy
      )
    )
    local.get $ptr
    i32.const 1
    i32.add
    local.get $json_len
    i32.add
    i32.const 0
    i32.store8
    local.get $ptr
    i64.extend_i32_u
  )

  ;; Helper: allocate [TAG_INTEGER=0x01][8-byte LE i64] and return pointer.
  (func $ret_int (param $val i64) (result i64)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    i32.const 9
    i32.add
    global.set $bump
    local.get $ptr
    i32.const 1
    i32.store8
    local.get $ptr
    i32.const 1
    i32.add
    local.get $val
    i64.store
    local.get $ptr
    i64.extend_i32_u
  )

  (func (export "const0") (param $argc i32) (param $argv i32) (result i64)
    i64.const 0
    call $ret_int
  )
  (func (export "const1") (param $argc i32) (param $argv i32) (result i64)
    i64.const 1
    call $ret_int
  )
  (func (export "const2") (param $argc i32) (param $argv i32) (result i64)
    i64.const 2
    call $ret_int
  )
  (func (export "const3") (param $argc i32) (param $argv i32) (result i64)
    i64.const 3
    call $ret_int
  )
  (func (export "const4") (param $argc i32) (param $argv i32) (result i64)
    i64.const 4
    call $ret_int
  )
)
"#;

    // ── dump/restore tests ─────────────────────────────────────────────

    /// Helper: read the stored SQL from an internal table.
    fn read_internal_sql(conn: &Arc<turso_core::Connection>, table: &str) -> Vec<String> {
        let query = format!("SELECT sql FROM {table} ORDER BY rowid");
        let rows: Vec<(String,)> = conn.exec_rows(&query);
        rows.into_iter().map(|(s,)| s).collect()
    }

    /// Test that CREATE EXTENSION AS X'...' stores the WASM binary inline
    /// and can be restored into a fresh database.
    #[test]
    fn test_dump_restore_extension_blob() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);
        let db_path = tmp.path().join("source.db");

        let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
        let conn = db.connect_limbo();
        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        // Extension functions work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(10, 20)");
        assert_eq!(rows, vec![(30,)]);
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_const42()");
        assert_eq!(rows, vec![(42,)]);

        // Stored SQL must contain inline blob
        let stored = read_internal_sql(&conn, turso_core::schema::TURSO_WASM_TABLE_NAME);
        assert_eq!(stored.len(), 1);
        assert!(
            stored[0].contains("AS X'"),
            "stored SQL should contain inline blob: {}",
            stored[0]
        );

        // Restore into fresh database
        drop(conn);
        drop(db);

        let restore_path = tmp.path().join("restored.db");
        let db2 =
            TempDatabase::new_with_existent_with_opts(&restore_path, opts_with_wasm_runtime());
        let conn2 = db2.connect_limbo();
        conn2.execute(&stored[0]).unwrap();

        // Functions must work after restore
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT ext_add(100, 23)");
        assert_eq!(rows, vec![(123,)]);
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT ext_const42()");
        assert_eq!(rows, vec![(42,)]);
    }

    /// Test that CREATE EXTENSION AS X'...' (inline blob) roundtrips correctly.
    #[test]
    fn test_dump_restore_extension_inline_blob() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = wat_to_wasm(EXT_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db_path = tmp.path().join("source.db");
        let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
        let conn = db.connect_limbo();
        let sql = format!("CREATE EXTENSION myext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(3, 4)");
        assert_eq!(rows, vec![(7,)]);

        // Read stored SQL and restore
        let stored = read_internal_sql(&conn, turso_core::schema::TURSO_WASM_TABLE_NAME);
        assert_eq!(stored.len(), 1);

        drop(conn);
        drop(db);

        let restore_path = tmp.path().join("restored.db");
        let db2 =
            TempDatabase::new_with_existent_with_opts(&restore_path, opts_with_wasm_runtime());
        let conn2 = db2.connect_limbo();
        conn2.execute(&stored[0]).unwrap();

        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT ext_add(5, 6)");
        assert_eq!(rows, vec![(11,)]);
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT ext_const42()");
        assert_eq!(rows, vec![(42,)]);
    }

    // ── ret_integer regression test ──────────────────────────────────────

    /// Verify that properly tagged integer returns work correctly even when
    /// WASM memory at low addresses contains tag-like bytes.
    #[test]
    fn test_tagged_integer_returns_correct_values() {
        let wasm_bytes = wat_to_wasm(TAGGED_INT_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE EXTENSION tagint_ext LANGUAGE wasm AS X'{hex}'");
        conn.execute(&sql).unwrap();

        for val in 0..=4i64 {
            let query = format!("SELECT const{val}()");
            let mut stmt = conn.prepare(&query).unwrap();
            let mut found = false;
            stmt.run_with_row_callback(|row| {
                let v = row.get_value(0);
                match v {
                    turso_core::Value::Numeric(n) => {
                        assert_eq!(
                            n.to_f64() as i64,
                            val,
                            "const{val}() returned wrong integer value"
                        );
                    }
                    other => {
                        panic!("const{val}() should return integer {val}, got {other:?}");
                    }
                }
                found = true;
                Ok(())
            })
            .unwrap();
            assert!(found, "const{val}() returned no rows");
        }
    }

    /// CREATE EXTENSION must fail when its functions conflict with an existing function.
    #[test]
    fn test_create_extension_function_conflict_with_existing_function() {
        let ext_bytes = wat_to_wasm(EXT_WAT);
        let ext_hex = wasm_to_hex(&ext_bytes);

        // A separate WASM module that exports a function named "ext_add" — same as
        // one of the functions in EXT_WAT.
        let add_bytes = wat_to_wasm(
            r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump  local.set $ptr
    global.get $bump  local.get $size  i32.add  global.set $bump
    local.get $ptr
  )
  (func (export "ext_add") (param $argc i32) (param $argv i32) (result i64)
    i64.const 999
  )
)
"#,
        );
        let add_hex = wasm_to_hex(&add_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Register a standalone function named "ext_add"
        conn.execute(format!(
            "CREATE FUNCTION ext_add LANGUAGE wasm AS X'{add_hex}' EXPORT 'ext_add'"
        ))
        .unwrap();

        // Now try to load an extension that also exports "ext_add" — should fail
        let result = conn.execute(format!(
            "CREATE EXTENSION myext LANGUAGE wasm AS X'{ext_hex}'"
        ));
        assert!(
            result.is_err(),
            "CREATE EXTENSION should fail when its function conflicts with an existing function"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("ext_add") && err.contains("conflicts"),
            "error should mention the conflicting function name, got: {err}"
        );
    }

    /// Two extensions with overlapping function names must fail on the second load.
    #[test]
    fn test_create_extension_function_conflict_between_extensions() {
        let ext_bytes = wat_to_wasm(EXT_WAT);
        let ext_hex = wasm_to_hex(&ext_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // First extension registers ext_add and ext_const42
        conn.execute(format!(
            "CREATE EXTENSION ext1 LANGUAGE wasm AS X'{ext_hex}'"
        ))
        .unwrap();

        // Second extension with the same WASM (same function names) should fail
        let result = conn.execute(format!(
            "CREATE EXTENSION ext2 LANGUAGE wasm AS X'{ext_hex}'"
        ));
        assert!(
            result.is_err(),
            "second extension with overlapping function names should fail"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("conflicts"),
            "error should mention conflict, got: {err}"
        );

        // Original extension functions should still work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT ext_add(10, 20)");
        assert_eq!(rows, vec![(30,)]);
    }

    /// Extension whose manifest contains an unrecognized field must fail to load.
    /// This ensures forwards-compatibility: if a newer extension uses manifest
    /// fields that this build doesn't understand, we reject loudly rather than
    /// silently ignoring resources the extension expects to be registered.
    #[test]
    fn test_unknown_manifest_field_rejected() {
        // Manifest with a bogus top-level field "quantum_entanglement".
        // The JSON is short enough to embed directly.
        let json = r#"{"functions":[],"quantum_entanglement":[{"name":"qbit"}]}"#;
        let json_bytes = json.as_bytes();
        let json_len = json_bytes.len(); // 57

        // Build a minimal WAT that returns this JSON as TAG_TEXT from turso_ext_init.
        let data_escape = json.replace('\\', "\\\\").replace('"', "\\\"");
        let alloc_size = 1 + json_len + 1; // tag + json + nul
        let wat = format!(
            r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
  (data (i32.const 2048) "{data_escape}")

  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )

  (func (export "turso_ext_init") (param $argc i32) (param $argv i32) (result i64)
    (local $ptr i32)
    (local $i i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    i32.const {alloc_size}
    i32.add
    global.set $bump

    ;; TAG_TEXT = 0x03
    local.get $ptr
    i32.const 3
    i32.store8

    ;; copy json bytes
    i32.const 0
    local.set $i
    (block $break
      (loop $copy
        local.get $i
        i32.const {json_len}
        i32.ge_u
        br_if $break
        local.get $ptr
        i32.const 1
        i32.add
        local.get $i
        i32.add
        i32.const 2048
        local.get $i
        i32.add
        i32.load8_u
        i32.store8
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $copy
      )
    )

    ;; null terminator
    local.get $ptr
    i32.const 1
    i32.add
    i32.const {json_len}
    i32.add
    i32.const 0
    i32.store8

    local.get $ptr
    i64.extend_i32_u
  )
)
"#
        );

        let wasm_bytes = wat_to_wasm(&wat);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let result = conn.execute(format!("CREATE EXTENSION badext LANGUAGE wasm AS X'{hex}'"));
        assert!(
            result.is_err(),
            "extension with unknown manifest field should fail to load"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("quantum_entanglement"),
            "error should mention the unknown field name, got: {err}"
        );
    }
}
