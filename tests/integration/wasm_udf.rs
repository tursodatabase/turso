#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use std::sync::Arc;
    use tempfile::TempDir;
    use turso_core::StepResult;

    /// WAT source for a simple add(a, b) -> a + b function.
    /// Memory has 2 pages (128KB), bump allocator starts at 1024.
    const ADD_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "add") (param $argc i32) (param $argv i32) (result i64)
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.add
  )
)
"#;

    /// Compile WAT to WASM binary bytes.
    fn wat_to_wasm(wat: &str) -> Vec<u8> {
        wat::parse_str(wat).expect("Failed to parse WAT")
    }

    /// Convert WASM bytes to a hex string for use in AS X'...' syntax.
    fn wasm_to_hex(wasm: &[u8]) -> String {
        wasm.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Create DatabaseOpts with wasmtime runtime injected.
    fn opts_with_wasm_runtime() -> turso_core::DatabaseOpts {
        let runtime = turso_wasm_wasmtime::WasmtimeRuntime::new().unwrap();
        turso_core::DatabaseOpts::new().with_unstable_wasm_runtime(Arc::new(runtime))
    }

    #[test]
    fn test_create_function_from_blob() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(40, 2)");
        assert_eq!(rows, vec![(42,)]);
    }

    #[test]
    fn test_create_function_inline_blob() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(10, 20)");
        assert_eq!(rows, vec![(30,)]);
    }

    #[test]
    fn test_drop_function() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        // Function works
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(1, 2)");
        assert_eq!(rows, vec![(3,)]);

        // Drop it
        conn.execute("DROP FUNCTION add2").unwrap();

        // Should fail now
        let result = conn.execute("SELECT add2(1, 2)");
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_function_if_exists() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Should not error
        conn.execute("DROP FUNCTION IF EXISTS nonexistent").unwrap();
    }

    #[test]
    fn test_drop_function_nonexistent_errors() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let result = conn.execute("DROP FUNCTION nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_function_if_not_exists() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        // Should not error
        let sql =
            format!("CREATE FUNCTION IF NOT EXISTS add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        // Function still works
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(5, 5)");
        assert_eq!(rows, vec![(10,)]);
    }

    #[test]
    fn test_create_function_duplicate_errors() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        // Should error
        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        let result = conn.execute(&sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_or_replace_function() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        // Replace should succeed
        let sql = format!("CREATE OR REPLACE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        // Function still works
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(100, 200)");
        assert_eq!(rows, vec![(300,)]);
    }

    #[test]
    fn test_create_function_invalid_wasm_bytes() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Pass garbage bytes that are not valid WASM
        let result = conn.execute("CREATE FUNCTION foo LANGUAGE wasm AS X'deadbeef' EXPORT 'bar'");
        assert!(
            result.is_err(),
            "invalid WASM bytes should produce an error"
        );
    }

    #[test]
    fn test_no_runtime_gives_clear_error() {
        // Database without any WASM runtime
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        let result = conn.execute(&sql);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("no WASM runtime registered"),
            "Expected 'no WASM runtime registered' error, got: {err_msg}"
        );
    }

    #[test]
    fn test_function_persistence_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);
        let db_path = tmp.path().join("persist_func.db");

        // First session: create function and verify
        {
            let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
            let conn = db.connect_limbo();

            let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
            conn.execute(&sql).unwrap();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(10, 32)");
            assert_eq!(rows, vec![(42,)]);
            conn.close().unwrap();
        }

        // Second session: function should still work
        {
            let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
            let conn = db.connect_limbo();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(100, 200)");
            assert_eq!(
                rows,
                vec![(300,)],
                "Function should persist and work after database reopen"
            );
            conn.close().unwrap();
        }
    }

    /// Build the C SDK example project and return the WASM bytes.
    /// Returns None if no WASM-capable C compiler is available.
    fn build_c_sdk_examples() -> Option<Vec<u8>> {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let example_dir = manifest_dir.parent().unwrap().join("wasm-sdk/examples/c");
        let status = std::process::Command::new("make")
            .current_dir(&example_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
        let wasm_path = example_dir.join("udfs.wasm");
        if wasm_path.exists() {
            Some(std::fs::read(&wasm_path).unwrap())
        } else {
            None
        }
    }

    #[test]
    fn test_c_sdk_integer_add() {
        let Some(wasm_bytes) = build_c_sdk_examples() else {
            eprintln!("skipping C SDK test: no WASM-capable C compiler");
            return;
        };
        let hex = wasm_to_hex(&wasm_bytes);
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION c_add LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
        ))
        .unwrap();

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT c_add(40, 2)");
        assert_eq!(rows, vec![(42,)]);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT c_add(-10, 10)");
        assert_eq!(rows, vec![(0,)]);
    }

    #[test]
    fn test_c_sdk_text_upper() {
        let Some(wasm_bytes) = build_c_sdk_examples() else {
            eprintln!("skipping C SDK test: no WASM-capable C compiler");
            return;
        };
        let hex = wasm_to_hex(&wasm_bytes);
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION c_upper LANGUAGE wasm AS X'{hex}' EXPORT 'upper'"
        ))
        .unwrap();

        let rows: Vec<(String,)> = conn.exec_rows("SELECT c_upper('hello')");
        assert_eq!(rows, vec![("HELLO".to_string(),)]);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT c_upper('Hello World!')");
        assert_eq!(rows, vec![("HELLO WORLD!".to_string(),)]);
    }

    /// WAT source for a function that loops forever.
    const INFINITE_LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "infinite") (param $argc i32) (param $argv i32) (result i64)
    (loop $loop
      (br $loop)
    )
    i64.const 0
  )
)
"#;

    /// Verify that an infinite-loop WASM UDF is interrupted by the epoch timeout
    /// and does not block other connections.
    #[test]
    fn test_infinite_loop_does_not_block_other_connections() {
        let wasm_bytes = wat_to_wasm(INFINITE_LOOP_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let tmp_db = Arc::new(
            TempDatabase::builder()
                .with_opts(opts_with_wasm_runtime())
                .build(),
        );

        // Register the infinite-loop UDF
        let conn = tmp_db.connect_limbo();
        conn.execute(format!(
            "CREATE FUNCTION infinite_loop LANGUAGE wasm AS X'{hex}' EXPORT 'infinite'"
        ))
        .unwrap();

        let tmp_db2 = tmp_db.clone();
        let loop_thread = std::thread::spawn(move || {
            let loop_conn = tmp_db2.connect_limbo();
            // This should return an error after epoch timeout (~2s), not block forever
            loop_conn.execute("SELECT infinite_loop()")
        });

        // Another connection should work fine while the infinite loop runs
        let conn2 = tmp_db.connect_limbo();
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT 1");
        assert_eq!(
            rows,
            vec![(1,)],
            "Second connection must work while first is in infinite loop"
        );

        // The loop thread should terminate with an error (epoch timeout)
        let result = loop_thread.join().expect("loop thread panicked");
        assert!(result.is_err(), "infinite loop must return an error");
    }

    /// Verify that calling an infinite-loop WASM UDF returns an error within ~3s
    /// due to epoch-based timeout, and the error message mentions the timeout.
    #[test]
    fn test_sync_udf_timeout_aborts() {
        let wasm_bytes = wat_to_wasm(INFINITE_LOOP_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION infinite_loop LANGUAGE wasm AS X'{hex}' EXPORT 'infinite'"
        ))
        .unwrap();

        let start = std::time::Instant::now();
        let result = conn.execute("SELECT infinite_loop()");
        let elapsed = start.elapsed();

        assert!(result.is_err(), "infinite loop UDF must return an error");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "epoch timeout must trigger within 5s (actual: {elapsed:?})"
        );

        // Connection should still be usable after timeout
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT 1");
        assert_eq!(
            rows,
            vec![(1,)],
            "connection must remain usable after timeout"
        );
    }

    /// WAT that hits an `unreachable` trap.
    const UNREACHABLE_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "boom") (param $argc i32) (param $argv i32) (result i64)
    unreachable
  )
)
"#;

    /// WAT that does an out-of-bounds memory load.
    const OOB_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "oob") (param $argc i32) (param $argv i32) (result i64)
    ;; 2 pages = 128KiB = 131072 bytes; load from way past that
    i32.const 999999
    i64.load
  )
)
"#;

    /// Verify that a WASM trap (unreachable) returns an error instead of
    /// killing the process, and the connection remains usable afterward.
    #[test]
    fn test_udf_crash_unreachable_returns_error() {
        let wasm_bytes = wat_to_wasm(UNREACHABLE_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION boom LANGUAGE wasm AS X'{hex}' EXPORT 'boom'"
        ))
        .unwrap();

        let err = conn.execute("SELECT boom()").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unreachable"),
            "expected 'unreachable' in error, got: {msg}"
        );

        // Connection should still be usable
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT 1");
        assert_eq!(rows, vec![(1,)]);
    }

    /// Verify that an out-of-bounds memory access in WASM returns an error
    /// instead of killing the process, and the connection remains usable.
    #[test]
    fn test_udf_crash_oob_memory_returns_error() {
        let wasm_bytes = wat_to_wasm(OOB_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION oob LANGUAGE wasm AS X'{hex}' EXPORT 'oob'"
        ))
        .unwrap();

        let err = conn.execute("SELECT oob()").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("out of bounds memory access"),
            "expected 'out of bounds memory access' in error, got: {msg}"
        );

        // Connection should still be usable
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT 1");
        assert_eq!(rows, vec![(1,)]);
    }

    /// WAT for a function that ignores arguments and returns i64 42.
    /// Body is a single instruction: `i64.const 42`.
    /// Wasmtime charges exactly 2 fuel per call (1 entry block + 1 const).
    const CONST42_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "const42") (param $argc i32) (param $argv i32) (result i64)
    i64.const 42
  )
)
"#;

    /// Exact fuel cost of const42, verified in wasm-runtime-wasmtime unit tests.
    const CONST42_FUEL: u64 = 2;

    #[test]
    fn test_wasm_instructions_metric_exact() {
        let wasm_bytes = wat_to_wasm(CONST42_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION const42 LANGUAGE wasm AS X'{hex}' EXPORT 'const42'"
        ))
        .unwrap();

        // Single call: metrics must equal exactly CONST42_FUEL
        let mut stmt = conn.prepare("SELECT const42()").unwrap();
        stmt.run_with_row_callback(|row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 42);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            stmt.metrics().wasm_instructions,
            CONST42_FUEL,
            "single UDF call must report exactly {CONST42_FUEL} wasm_instructions"
        );

        // Two calls in one query: metrics must accumulate to 2 * CONST42_FUEL
        let mut stmt = conn.prepare("SELECT const42(), const42()").unwrap();
        stmt.run_with_row_callback(|_| Ok(())).unwrap();
        assert_eq!(
            stmt.metrics().wasm_instructions,
            2 * CONST42_FUEL,
            "two UDF calls in one statement must report exactly {} wasm_instructions",
            2 * CONST42_FUEL
        );

        // Non-WASM query: must report 0
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        stmt.run_with_row_callback(|_| Ok(())).unwrap();
        assert_eq!(
            stmt.metrics().wasm_instructions,
            0,
            "non-WASM statement must report 0 wasm_instructions"
        );
    }

    /// Exact fuel cost of the ADD function (7 wasm instructions),
    /// verified in wasm-runtime-wasmtime unit tests.
    const ADD_FUEL: u64 = 8;

    #[test]
    fn test_wasm_instructions_metric_accumulates_across_rows() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Create a table with 3 rows
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
        ))
        .unwrap();

        // UDF depends on table column → must be called once per row
        let mut stmt = conn.prepare("SELECT add2(x, 10) FROM t").unwrap();
        let mut row_count = 0;
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => row_count += 1,
                StepResult::Done => break,
                StepResult::IO => stmt.get_pager().io.step().unwrap(),
                _ => unreachable!(),
            }
        }
        assert_eq!(row_count, 3);
        assert_eq!(
            stmt.metrics().wasm_instructions,
            3 * ADD_FUEL,
            "UDF called once per row over 3 rows must report exactly {} wasm_instructions",
            3 * ADD_FUEL
        );
    }

    /// WAT with a loop that burns many instructions so yielding is guaranteed.
    /// loop_add(a, b): adds a+b in a loop 100 times (returns a+b, but burns ~500+ fuel).
    const LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "loop_add") (param $argc i32) (param $argv i32) (result i64)
    (local $i i32)
    (local $result i64)
    ;; result = argv[0] + argv[1]
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.add
    local.set $result
    ;; loop 100 times (burning fuel)
    i32.const 0
    local.set $i
    (block $break
      (loop $continue
        local.get $i
        i32.const 100
        i32.ge_u
        br_if $break
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $continue
      )
    )
    local.get $result
  )
)
"#;

    /// Verify that the loop_add UDF completes synchronously with correct result and fuel tracking.
    #[test]
    fn test_loop_udf_completes() {
        let wasm_bytes = wat_to_wasm(LOOP_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION loop_add LANGUAGE wasm AS X'{hex}' EXPORT 'loop_add'"
        ))
        .unwrap();

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT loop_add(10, 32)");
        assert_eq!(rows, vec![(42,)], "loop_add(10, 32) must return 42");

        // Verify fuel is tracked through the sync path
        let mut stmt = conn.prepare("SELECT loop_add(10, 32)").unwrap();
        stmt.run_with_row_callback(|_| Ok(())).unwrap();
        assert!(
            stmt.metrics().wasm_instructions > 0,
            "WASM fuel must be tracked through sync path"
        );
    }

    /// Build the SDK example project and return the WASM bytes.
    fn build_sdk_examples() -> Vec<u8> {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let example_dir = manifest_dir
            .parent()
            .unwrap()
            .join("wasm-sdk/examples/rust");
        let status = std::process::Command::new("cargo")
            .args(["build", "--target", "wasm32-unknown-unknown", "--release"])
            .current_dir(&example_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("failed to run cargo build for SDK example project");
        assert!(status.success(), "SDK example project build failed");
        let wasm_path =
            example_dir.join("target/wasm32-unknown-unknown/release/turso_udf_examples.wasm");
        assert!(
            wasm_path.exists(),
            "WASM file not found: {}",
            wasm_path.display()
        );
        std::fs::read(&wasm_path).unwrap()
    }

    /// Load all UDFs from the SDK example WASM bytes into a connection.
    fn load_sdk_udfs(conn: &std::sync::Arc<turso_core::Connection>, wasm_bytes: &[u8]) {
        let hex = wasm_to_hex(wasm_bytes);
        for (sql_name, export_name) in [
            ("sdk_add", "add"),
            ("sdk_upper", "upper"),
            ("sdk_nullable_len", "nullable_len"),
            ("sdk_distance_sq", "distance_sq"),
            ("sdk_echo_int", "echo_int"),
            ("sdk_float_mul", "float_mul"),
            ("sdk_blob_reverse", "blob_reverse"),
            ("sdk_opt_double", "opt_double"),
            ("sdk_opt_negate", "opt_negate"),
            ("sdk_do_nothing", "do_nothing"),
            ("sdk_opt_upper", "opt_upper"),
            ("sdk_opt_blob_len", "opt_blob_len"),
        ] {
            conn.execute(format!(
                "CREATE FUNCTION {sql_name} LANGUAGE wasm AS X'{hex}' EXPORT '{export_name}'"
            ))
            .unwrap();
        }
    }

    #[test]
    fn test_sdk_integer_add() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT sdk_add(40, 2)");
        assert_eq!(rows, vec![(42,)]);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT sdk_add(-10, 10)");
        assert_eq!(rows, vec![(0,)]);
    }

    #[test]
    fn test_sdk_text_upper() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT sdk_upper('hello')");
        assert_eq!(rows, vec![("HELLO".to_string(),)]);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT sdk_upper('Hello World!')");
        assert_eq!(rows, vec![("HELLO WORLD!".to_string(),)]);
    }

    #[test]
    fn test_sdk_real_distance() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        // distance_sq(0, 0, 3, 4) = 3^2 + 4^2 = 25.0
        let rows: Vec<(f64,)> = conn.exec_rows("SELECT sdk_distance_sq(0.0, 0.0, 3.0, 4.0)");
        assert_eq!(rows, vec![(25.0,)]);
    }

    #[test]
    fn test_sdk_nullable_args() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT sdk_nullable_len('hello')");
        assert_eq!(rows, vec![(5,)]);

        // NULL argument should return NULL — verify by checking TYPEOF
        let rows: Vec<(String,)> = conn.exec_rows("SELECT TYPEOF(sdk_nullable_len(NULL))");
        assert_eq!(rows, vec![("null".to_string(),)]);
    }

    // ── Typed ABI tests ────────────────────────────────────────────────

    #[test]
    fn test_sdk_float_mul() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(f64,)> = conn.exec_rows("SELECT sdk_float_mul(2.5, 4.0)");
        assert_eq!(rows, vec![(10.0,)]);

        let rows: Vec<(f64,)> = conn.exec_rows("SELECT sdk_float_mul(-1.5, 2.0)");
        assert_eq!(rows, vec![(-3.0,)]);
    }

    #[test]
    fn test_sdk_blob_reverse() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        // Verify blob reverse by converting to hex
        let rows: Vec<(String,)> = conn.exec_rows("SELECT HEX(sdk_blob_reverse(X'010203'))");
        assert_eq!(rows, vec![("030201".to_string(),)]);
    }

    #[test]
    fn test_sdk_opt_double() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT sdk_opt_double(5)");
        assert_eq!(rows, vec![(10,)]);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT TYPEOF(sdk_opt_double(NULL))");
        assert_eq!(rows, vec![("null".to_string(),)]);
    }

    #[test]
    fn test_sdk_opt_negate() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(f64,)> = conn.exec_rows("SELECT sdk_opt_negate(2.5)");
        assert_eq!(rows, vec![(-2.5,)]);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT TYPEOF(sdk_opt_negate(NULL))");
        assert_eq!(rows, vec![("null".to_string(),)]);
    }

    #[test]
    fn test_sdk_void_function() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT TYPEOF(sdk_do_nothing())");
        assert_eq!(rows, vec![("null".to_string(),)]);
    }

    #[test]
    fn test_sdk_opt_upper() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT sdk_opt_upper('hi')");
        assert_eq!(rows, vec![("HI".to_string(),)]);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT TYPEOF(sdk_opt_upper(NULL))");
        assert_eq!(rows, vec![("null".to_string(),)]);
    }

    #[test]
    fn test_sdk_opt_blob_len() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT sdk_opt_blob_len(X'0102')");
        assert_eq!(rows, vec![(2,)]);

        let rows: Vec<(String,)> = conn.exec_rows("SELECT TYPEOF(sdk_opt_blob_len(NULL))");
        assert_eq!(rows, vec![("null".to_string(),)]);
    }

    // ── dump/restore tests ─────────────────────────────────────────────

    /// Helper: read the stored SQL from an internal table.
    fn read_internal_sql(conn: &Arc<turso_core::Connection>, table: &str) -> Vec<String> {
        let query = format!("SELECT sql FROM {table} ORDER BY rowid");
        let rows: Vec<(String,)> = conn.exec_rows(&query);
        rows.into_iter().map(|(s,)| s).collect()
    }

    /// Test that CREATE FUNCTION AS X'...' stores the WASM binary inline
    /// and the stored SQL can recreate the function in a fresh database.
    #[test]
    fn test_dump_restore_function_from_blob() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);
        let db_path = tmp.path().join("source.db");

        // Create source DB with a function loaded from inline blob
        let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
        let conn = db.connect_limbo();
        let sql = format!("CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        // Verify function works
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(10, 20)");
        assert_eq!(rows, vec![(30,)]);

        // Read stored SQL — must contain AS X'...'
        let stored = read_internal_sql(&conn, turso_core::schema::TURSO_WASM_TABLE_NAME);
        assert_eq!(stored.len(), 1);
        assert!(
            stored[0].contains("AS X'"),
            "stored SQL should contain inline blob: {}",
            stored[0]
        );

        // Restore into a fresh database by replaying the stored SQL
        drop(conn);
        drop(db);

        let restore_path = tmp.path().join("restored.db");
        let db2 =
            TempDatabase::new_with_existent_with_opts(&restore_path, opts_with_wasm_runtime());
        let conn2 = db2.connect_limbo();
        conn2.execute(&stored[0]).unwrap();

        // Function must work in restored DB
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT add2(100, 23)");
        assert_eq!(rows, vec![(123,)]);
    }

    /// Test that CREATE FUNCTION AS X'...' (inline blob) roundtrips through
    /// dump/restore correctly.
    #[test]
    fn test_dump_restore_function_inline_blob() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db_path = tmp.path().join("source.db");
        let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
        let conn = db.connect_limbo();
        let sql = format!("CREATE FUNCTION myadd LANGUAGE wasm AS X'{hex}' EXPORT 'add'");
        conn.execute(&sql).unwrap();

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT myadd(3, 4)");
        assert_eq!(rows, vec![(7,)]);

        // Read stored SQL and restore into fresh DB
        let stored = read_internal_sql(&conn, turso_core::schema::TURSO_WASM_TABLE_NAME);
        assert_eq!(stored.len(), 1);

        drop(conn);
        drop(db);

        let restore_path = tmp.path().join("restored.db");
        let db2 =
            TempDatabase::new_with_existent_with_opts(&restore_path, opts_with_wasm_runtime());
        let conn2 = db2.connect_limbo();
        conn2.execute(&stored[0]).unwrap();

        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT myadd(5, 6)");
        assert_eq!(rows, vec![(11,)]);
    }

    // ── ret_integer regression test ──────────────────────────────────────

    /// Regression test: echo_int uses ret_integer (old convention) to return
    /// its integer argument. This catches the bug where ret_integer returned
    /// raw i64 values without a tag — unmarshal_result would misinterpret
    /// small values as tagged pointers into WASM memory.
    ///
    /// Tests small values (0..5) which are the most vulnerable to the bug:
    /// if the WASM memory at those addresses contains bytes matching tag
    /// constants (0x01..0x05), the result is misinterpreted.
    #[test]
    fn test_sdk_echo_int_small_values() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        // Small values 0..5 are addresses where tag-like bytes may live.
        // These are the values that triggered the bug in bench_counter vtab.
        for val in 0..=5i64 {
            let query = format!("SELECT sdk_echo_int({val})");
            let mut stmt = conn.prepare(&query).unwrap();
            let mut found = false;
            stmt.run_with_row_callback(|row| {
                let v = row.get_value(0);
                match v {
                    turso_core::Value::Numeric(n) => {
                        assert_eq!(
                            n.to_f64() as i64,
                            val,
                            "sdk_echo_int({val}) returned wrong value"
                        );
                    }
                    other => {
                        panic!(
                            "sdk_echo_int({val}) should return integer {val}, got {other:?} \
                             (ret_integer likely returned raw i64, and unmarshal_result \
                             misread memory[{val}] as a tag byte)"
                        );
                    }
                }
                found = true;
                Ok(())
            })
            .unwrap();
            assert!(found, "sdk_echo_int({val}) returned no rows");
        }

        // Also verify typeof is integer, not blob/null/real
        for val in 0..=5i64 {
            let rows: Vec<(String,)> =
                conn.exec_rows(&format!("SELECT TYPEOF(sdk_echo_int({val}))"));
            assert_eq!(
                rows,
                vec![("integer".to_string(),)],
                "sdk_echo_int({val}) should have type 'integer'"
            );
        }

        // Larger values should also work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT sdk_echo_int(1000000)");
        assert_eq!(rows, vec![(1000000,)]);

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT sdk_echo_int(-42)");
        assert_eq!(rows, vec![(-42,)]);
    }

    // ── Schema change tests ─────────────────────────────────────────────

    /// WAT for multiply(a, b) -> a * b, used as a replacement for add.
    const MUL_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "mul") (param $argc i32) (param $argv i32) (result i64)
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.mul
  )
)
"#;

    /// UDF must still work after an unrelated schema change (CREATE TABLE).
    #[test]
    fn test_udf_survives_unrelated_schema_change() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
        ))
        .unwrap();

        // UDF works before schema change
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(10, 20)");
        assert_eq!(rows, vec![(30,)]);

        // Unrelated schema change: create a table
        conn.execute("CREATE TABLE unrelated(x INTEGER)").unwrap();
        conn.execute("INSERT INTO unrelated VALUES (99)").unwrap();

        // UDF must still work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(10, 20)");
        assert_eq!(rows, vec![(30,)], "UDF broken after CREATE TABLE");

        // The new table must also work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM unrelated");
        assert_eq!(rows, vec![(99,)]);
    }

    /// UDF must still work after multiple unrelated schema changes.
    #[test]
    fn test_udf_survives_multiple_schema_changes() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
        ))
        .unwrap();

        for i in 0..5 {
            conn.execute(format!("CREATE TABLE t{i}(x INTEGER)"))
                .unwrap();
            // Verify UDF after each schema change
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(1, 2)");
            assert_eq!(rows, vec![(3,)], "UDF broken after creating table t{i}");
        }
    }

    /// CREATE OR REPLACE FUNCTION with different WASM must return new behavior.
    #[test]
    fn test_udf_replace_changes_behavior() {
        let add_bytes = wat_to_wasm(ADD_WAT);
        let add_hex: String = add_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let mul_bytes = wat_to_wasm(MUL_WAT);
        let mul_hex: String = mul_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Register as add
        conn.execute(format!(
            "CREATE FUNCTION myfn LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
        ))
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT myfn(3, 4)");
        assert_eq!(rows, vec![(7,)], "expected 3+4=7");

        // Replace with multiply
        conn.execute(format!(
            "CREATE OR REPLACE FUNCTION myfn LANGUAGE wasm AS X'{mul_hex}' EXPORT 'mul'"
        ))
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT myfn(3, 4)");
        assert_eq!(rows, vec![(12,)], "expected 3*4=12 after replace");
    }

    /// DROP + re-CREATE with different WASM must return new behavior.
    #[test]
    fn test_udf_drop_and_recreate() {
        let add_bytes = wat_to_wasm(ADD_WAT);
        let add_hex: String = add_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let mul_bytes = wat_to_wasm(MUL_WAT);
        let mul_hex: String = mul_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Create as add
        conn.execute(format!(
            "CREATE FUNCTION myfn LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
        ))
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT myfn(3, 4)");
        assert_eq!(rows, vec![(7,)]);

        // Drop
        conn.execute("DROP FUNCTION myfn").unwrap();

        // Re-create as multiply
        conn.execute(format!(
            "CREATE FUNCTION myfn LANGUAGE wasm AS X'{mul_hex}' EXPORT 'mul'"
        ))
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT myfn(3, 4)");
        assert_eq!(rows, vec![(12,)], "expected 3*4=12 after drop+recreate");
    }

    /// UDF with table data must work after unrelated schema change.
    #[test]
    fn test_udf_with_table_data_survives_schema_change() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute("CREATE TABLE data(a INTEGER, b INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO data VALUES (10, 20), (30, 40)")
            .unwrap();

        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
        ))
        .unwrap();

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(a, b) FROM data ORDER BY a");
        assert_eq!(rows, vec![(30,), (70,)]);

        // Unrelated schema change
        conn.execute("CREATE TABLE other(z TEXT)").unwrap();

        // UDF over table data must still work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(a, b) FROM data ORDER BY a");
        assert_eq!(
            rows,
            vec![(30,), (70,)],
            "UDF over table data broken after schema change"
        );
    }

    /// Two databases sharing one `WasmtimeEngine` must share compiled modules
    /// for identical WASM bytes (content-addressed by SHA-256), while different
    /// WASM bytes produce distinct cache entries.
    #[test]
    fn test_shared_engine_deduplicates_modules() {
        let engine = Arc::new(turso_wasm_wasmtime::WasmtimeEngine::new().unwrap());

        let add_bytes = wat_to_wasm(ADD_WAT);
        let mul_bytes = wat_to_wasm(MUL_WAT);
        let add_hex: String = add_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let mul_hex: String = mul_bytes.iter().map(|b| format!("{b:02x}")).collect();

        // db1: "same" → ADD, "diff" → ADD
        let runtime1 = engine.create_runtime();
        let opts1 = turso_core::DatabaseOpts::new().with_unstable_wasm_runtime(Arc::new(runtime1));
        let db1 = TempDatabase::builder().with_opts(opts1).build();
        let conn1 = db1.connect_limbo();
        conn1
            .execute(format!(
                "CREATE FUNCTION same LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
            ))
            .unwrap();
        conn1
            .execute(format!(
                "CREATE FUNCTION diff LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
            ))
            .unwrap();

        // db2: "same" → ADD (identical bytes), "diff" → MUL (different bytes)
        let runtime2 = engine.create_runtime();
        let opts2 = turso_core::DatabaseOpts::new().with_unstable_wasm_runtime(Arc::new(runtime2));
        let db2 = TempDatabase::builder().with_opts(opts2).build();
        let conn2 = db2.connect_limbo();
        conn2
            .execute(format!(
                "CREATE FUNCTION same LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
            ))
            .unwrap();
        conn2
            .execute(format!(
                "CREATE FUNCTION diff LANGUAGE wasm AS X'{mul_hex}' EXPORT 'mul'"
            ))
            .unwrap();

        // Verify db1 behavior
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT same(3, 4)");
        assert_eq!(rows, vec![(7,)], "db1: same(3,4) should be 3+4=7");
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT diff(3, 4)");
        assert_eq!(rows, vec![(7,)], "db1: diff(3,4) should be 3+4=7");

        // Verify db2 behavior (same function name, different semantics for "diff")
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT same(3, 4)");
        assert_eq!(rows, vec![(7,)], "db2: same(3,4) should be 3+4=7");
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT diff(3, 4)");
        assert_eq!(rows, vec![(12,)], "db2: diff(3,4) should be 3*4=12");

        // Only 2 compiled modules should exist: ADD and MUL.
        // All "same" registrations (db1 + db2) share the ADD module.
        assert_eq!(
            engine.compiled_module_count(),
            2,
            "cache must have exactly 2 entries (ADD and MUL), not one per registration"
        );
    }

    /// Verify that `pragma_function_list` reports correct narg for SDK functions
    /// that use `#[turso_wasm]` (embedded in turso_sig custom section) vs manual
    /// exports (no custom section → narg = -1).
    #[test]
    fn test_pragma_function_list_shows_narg_from_custom_section() {
        let wasm_bytes = build_sdk_examples();
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();
        load_sdk_udfs(&conn, &wasm_bytes);

        // pragma_function_list columns: name, builtin, type, enc, narg, flags
        // sdk_add uses #[turso_wasm] with 2 params → narg = 2
        let rows: Vec<(String, i64, String, String, i64, i64)> =
            conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'sdk_add'");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].4, 2,
            "sdk_add (2 params via #[turso_wasm]) should have narg=2"
        );

        // sdk_upper uses #[turso_wasm] with 1 param → narg = 1
        let rows: Vec<(String, i64, String, String, i64, i64)> =
            conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'sdk_upper'");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].4, 1,
            "sdk_upper (1 param via #[turso_wasm]) should have narg=1"
        );

        // sdk_nullable_len uses #[turso_wasm] with 1 param → narg = 1
        let rows: Vec<(String, i64, String, String, i64, i64)> =
            conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'sdk_nullable_len'");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].4, 1,
            "sdk_nullable_len (1 param via #[turso_wasm]) should have narg=1"
        );

        // sdk_distance_sq is manually written (no #[turso_wasm]) → no custom section → narg = -1
        let rows: Vec<(String, i64, String, String, i64, i64)> =
            conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'sdk_distance_sq'");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].4, -1,
            "sdk_distance_sq (manual, no #[turso_wasm]) should have narg=-1"
        );
    }

    /// Verify that narg from the custom section is correct after database reopen.
    #[test]
    fn test_narg_persists_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let wasm_bytes = build_sdk_examples();
        let hex = wasm_to_hex(&wasm_bytes);
        let db_path = tmp.path().join("narg_persist.db");

        // First session: create function
        {
            let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
            let conn = db.connect_limbo();
            conn.execute(format!(
                "CREATE FUNCTION sdk_upper LANGUAGE wasm AS X'{hex}' EXPORT 'upper'"
            ))
            .unwrap();

            let rows: Vec<(String, i64, String, String, i64, i64)> =
                conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'sdk_upper'");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].4, 1, "narg should be 1 in first session");
            conn.close().unwrap();
        }

        // Second session: narg should still be correct
        {
            let db = TempDatabase::new_with_existent_with_opts(&db_path, opts_with_wasm_runtime());
            let conn = db.connect_limbo();

            let rows: Vec<(String, i64, String, String, i64, i64)> =
                conn.exec_rows("SELECT * FROM pragma_function_list WHERE name = 'sdk_upper'");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].4, 1, "narg should persist as 1 after reopen");
            conn.close().unwrap();
        }
    }

    /// Multiple UDFs from different WASM modules must all survive schema changes.
    #[test]
    fn test_multiple_udfs_survive_schema_change() {
        let add_bytes = wat_to_wasm(ADD_WAT);
        let add_hex: String = add_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let mul_bytes = wat_to_wasm(MUL_WAT);
        let mul_hex: String = mul_bytes.iter().map(|b| format!("{b:02x}")).collect();

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute(format!(
            "CREATE FUNCTION my_add LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE FUNCTION my_mul LANGUAGE wasm AS X'{mul_hex}' EXPORT 'mul'"
        ))
        .unwrap();

        // Both work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT my_add(3, 4)");
        assert_eq!(rows, vec![(7,)]);
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT my_mul(3, 4)");
        assert_eq!(rows, vec![(12,)]);

        // Schema change
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();

        // Both must still work
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT my_add(3, 4)");
        assert_eq!(rows, vec![(7,)], "my_add broken after schema change");
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT my_mul(3, 4)");
        assert_eq!(rows, vec![(12,)], "my_mul broken after schema change");
    }

    /// WAT source for a subtract(a, b) -> a - b function.
    /// Uses 1 page of memory (64KB).
    const SUB_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $bump (mut i32) (i32.const 1024))
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
  (func (export "sub") (param $argc i32) (param $argv i32) (result i64)
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.sub
  )
)
"#;

    /// Helper: register 3 WASM UDFs (add2, mul2, sub2) from inline blobs.
    fn register_three_funcs(conn: &Arc<turso_core::Connection>) {
        let add_hex = wasm_to_hex(&wat_to_wasm(ADD_WAT));
        let mul_hex = wasm_to_hex(&wat_to_wasm(MUL_WAT));
        let sub_hex = wasm_to_hex(&wat_to_wasm(SUB_WAT));
        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE FUNCTION mul2 LANGUAGE wasm AS X'{mul_hex}' EXPORT 'mul'"
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE FUNCTION sub2 LANGUAGE wasm AS X'{sub_hex}' EXPORT 'sub'"
        ))
        .unwrap();
    }

    // ── PRAGMA wasm_cache_size tests ──────────────────────────────────────────

    #[test]
    fn test_pragma_wasm_cache_size_default_returns_used_and_max() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // Default: used=0, max=10 MiB
        let rows: Vec<(i64, i64)> = conn.exec_rows("PRAGMA wasm_cache_size");
        assert_eq!(rows.len(), 1);
        let (used, max) = rows[0];
        assert_eq!(used, 0, "used should be 0 when no statement is running");
        assert_eq!(max, 10 * 1024 * 1024, "default max should be 10 MiB");
    }

    #[test]
    fn test_pragma_wasm_cache_size_set_and_get() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        conn.execute("PRAGMA wasm_cache_size = 500000").unwrap();
        let rows: Vec<(i64, i64)> = conn.exec_rows("PRAGMA wasm_cache_size");
        assert_eq!(rows.len(), 1);
        let (_used, max) = rows[0];
        assert_eq!(max, 500000, "max should reflect the set value");

        // Set to 0
        conn.execute("PRAGMA wasm_cache_size = 0").unwrap();
        let rows: Vec<(i64, i64)> = conn.exec_rows("PRAGMA wasm_cache_size");
        assert_eq!(rows[0].1, 0, "max should be 0 after setting to 0");
    }

    // ── Eviction behaviour tests (via statement metrics) ─────────────────────
    //
    // The WASM instance cache lives in ProgramState (per-statement). Budget is
    // released when the statement ends. So we test eviction behaviour via
    // statement metrics, not PRAGMA used.

    #[test]
    fn test_cache_hit_metrics_within_statement() {
        // A UDF called on multiple rows within one statement should be a miss on
        // the first row and a hit on every subsequent row.
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let add_hex = wasm_to_hex(&wat_to_wasm(ADD_WAT));
        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
        ))
        .unwrap();

        // Create table with 5 rows
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        for i in 1..=5 {
            conn.execute(format!("INSERT INTO t VALUES ({i})")).unwrap();
        }

        // SELECT add2(x, x) FROM t — calls add2 5 times on same instance
        let mut stmt = conn.prepare("SELECT add2(x, x) FROM t").unwrap();
        let mut results = Vec::new();
        stmt.run_with_row_callback(|row| {
            results.push(row.get::<i64>(0).unwrap());
            Ok(())
        })
        .unwrap();

        assert_eq!(results.len(), 5, "should get 5 rows");
        // Each x is doubled: [2, 4, 6, 8, 10]
        for (i, val) in results.iter().enumerate() {
            let expected = ((i + 1) * 2) as i64;
            assert_eq!(*val, expected, "add2({0}, {0}) should be {expected}", i + 1);
        }

        // 1 miss (first call creates instance), 4 hits (reused)
        assert_eq!(stmt.metrics().wasm_cache_misses, 1);
        assert_eq!(stmt.metrics().wasm_cache_hits, 4);
        assert_eq!(stmt.metrics().wasm_instances_created, 1);
        assert_eq!(stmt.metrics().wasm_cache_evictions, 0);
    }

    #[test]
    fn test_eviction_within_single_statement_two_slot_cache() {
        // Three UDFs called in a single query over multiple rows.
        // Cache holds 2 instances — so the third UDF evicts on every call cycle.
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        register_three_funcs(&conn);

        // Determine one instance's memory size by running a single call
        // We use a temporary SELECT to create one instance, then check budget used
        // (the budget is still live while the statement is running, but we need to
        // measure from a completed stmt). Instead, we'll use a known heuristic:
        // WASM 1-page modules are 64KiB = 65536 bytes of linear memory.
        // The wasmtime Store overhead makes total memory_size() around 131072 (2 pages for ADD_WAT).
        // Set cache to exactly 2 * that.

        // First: figure out the exact instance memory by running one call and reading budget
        // We can't read used from inside a statement, so we'll set a generous-then-shrink approach:
        // Run add2 once with huge budget, then the budget tells us used.
        // But: budget is released on statement drop! So we must use a different approach.
        //
        // We'll set cache to a small absolute value. ADD_WAT has 2 memory pages = 131072 bytes.
        // MUL_WAT/SUB_WAT have 1 memory page = 65536 bytes.
        // Two instances of 1-page funcs = 131072.
        // We set cache to 2 * 65536 = 131072. This fits mul2 + sub2 but not all three.
        //
        // Actually, a wasmtime instance's memory_size() is the linear memory size only.
        // 1 page = 65536 bytes, 2 pages = 131072 bytes.
        // The budget is based on memory_size() from the trait.
        // So: add2 (2 pages) = 131072, mul2 (1 page) = 65536, sub2 (1 page) = 65536.
        // Cache of 131072 fits: add2 alone, or mul2+sub2, but not add2+mul2 together.
        //
        // For a clean "holds exactly 2" test, let's use all 1-page funcs.
        // We'll use mul2 and sub2 (both 1-page) plus a third 1-page func.
        // But we already have add2 as 2-page. Let's just set cache to fit 2 of the smaller ones.
        // Cache = 65536 * 2 = 131072. This fits mul2+sub2 but when we call add2 (131072),
        // it needs to evict both mul2 and sub2 to make room.
        //
        // Better: just make the budget big enough for 2 * 131072 (for add2-sized instances).
        // That way it can hold 2 big instances (add2-sized) or 4 small ones.
        // But we want exactly 2 slots. Let's set it to 2 * 131072 = 262144.
        // add2=131072 + mul2=65536 = 196608, fits. Add sub2=65536 → 262144, barely fits.
        // That's 3 funcs fitting. Not what we want.
        //
        // Let's take a different approach: set cache to 131072 + 65536 - 1 = 196607.
        // add2 (131072) + mul2 (65536) = 196608 > 196607. Doesn't fit!
        // add2 (131072) alone fits. mul2 (65536) alone fits.
        // add2 + mul2 doesn't fit → calling mul2 after add2 evicts add2.
        //
        // Simplest: use only the 1-page functions (mul2, sub2) and create a third 1-page func.
        // Actually let's just use a cache size of 65536 * 2 = 131072 and register three 1-page functions.
        // We have mul2 (1 page) and sub2 (1 page). We need a third 1-page function.
        // Let's use add2 BUT note ADD_WAT has 2 pages. We need a 1-page add.

        // For simplicity, let's just test with the functions we have and use metrics to verify.
        // Set cache to 196000 (enough for add2 131072 + one 1-page 65536 = 196608, so barely not enough)
        // Actually let's just set it to 65536*2 = 131072, which means:
        // - mul2 alone: fits (65536 <= 131072)
        // - sub2 alone: fits
        // - mul2 + sub2: fits (65536 + 65536 = 131072 <= 131072)
        // - add2 alone: fits (131072 <= 131072)
        // - add2 + mul2: doesn't fit (131072 + 65536 = 196608 > 131072)
        // So calling add2 then mul2 will force eviction of add2.

        conn.execute("PRAGMA wasm_cache_size = 131072").unwrap();

        // Create a table with 3 rows so we exercise per-row eviction in a single statement
        conn.execute("CREATE TABLE nums(x INTEGER)").unwrap();
        conn.execute("INSERT INTO nums VALUES (1)").unwrap();
        conn.execute("INSERT INTO nums VALUES (2)").unwrap();
        conn.execute("INSERT INTO nums VALUES (3)").unwrap();

        // Call add2 and mul2 on each row. add2 is 2 pages (131072), mul2 is 1 page (65536).
        // They can't both fit in 131072 byte budget.
        // Row 1: add2 → miss, create (used=131072). mul2 → miss, evict add2, create (used=65536).
        // Row 2: add2 → miss, evict mul2, create (used=131072). mul2 → miss, evict add2, create (used=65536).
        // Row 3: same pattern.
        // Total: 6 misses, 0 hits, 6 creates, 4 evictions (first add2 and first mul2 don't evict).
        // Wait: row 1 add2 is the first call (miss, no eviction). mul2 needs 65536 but used is
        // 131072, total would be 196608 > 131072. So mul2 evicts add2. That's 1 eviction.
        // row 2: add2 again: used is 65536 (mul2). add2 needs 131072. 65536+131072=196608 > 131072.
        // So add2 evicts mul2. That's 2 evictions total.
        // Then mul2: used is 131072 (add2). mul2 needs 65536: 131072+65536 > 131072. Evicts add2.
        // 3 evictions.
        // row 3: same as row 2: add2 evicts mul2 (4 evictions), mul2 evicts add2 (5 evictions).
        // Total: 6 misses, 0 hits, 6 instances created, 5 evictions.

        let mut stmt = conn
            .prepare("SELECT add2(x, x), mul2(x, x) FROM nums")
            .unwrap();
        let mut results = Vec::new();
        stmt.run_with_row_callback(|row| {
            results.push((row.get::<i64>(0).unwrap(), row.get::<i64>(1).unwrap()));
            Ok(())
        })
        .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (2, 1), "add2(1,1)=2, mul2(1,1)=1");
        assert_eq!(results[1], (4, 4), "add2(2,2)=4, mul2(2,2)=4");
        assert_eq!(results[2], (6, 9), "add2(3,3)=6, mul2(3,3)=9");

        // Verify eviction actually happened
        assert!(
            stmt.metrics().wasm_cache_evictions > 0,
            "evictions should be > 0 when two UDFs don't fit in cache, got {}",
            stmt.metrics().wasm_cache_evictions
        );
        assert_eq!(
            stmt.metrics().wasm_cache_hits,
            0,
            "no hits since each call evicts the other"
        );
        // Every call is a miss (6 calls: 3 rows * 2 funcs)
        assert_eq!(stmt.metrics().wasm_cache_misses, 6);
        assert_eq!(stmt.metrics().wasm_instances_created, 6);
    }

    #[test]
    fn test_three_functions_cycle_in_single_statement() {
        // 3 functions, cache holds 2. Each row calls all 3 UDFs.
        // Verifies correct results and eviction under pressure.
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        register_three_funcs(&conn);

        // mul2 = 65536 (1 page), sub2 = 65536 (1 page), add2 = 131072 (2 pages)
        // Set cache to 131072: fits mul2+sub2 but not add2+anything_else.
        conn.execute("PRAGMA wasm_cache_size = 131072").unwrap();

        conn.execute("CREATE TABLE nums(x INTEGER)").unwrap();
        for i in 1..=4 {
            conn.execute(format!("INSERT INTO nums VALUES ({i})"))
                .unwrap();
        }

        // Each row evaluates add2(x,x), mul2(x,x), sub2(x*3, x) — all 3 funcs per row.
        let mut stmt = conn
            .prepare("SELECT add2(x, x), mul2(x, x), sub2(x*3, x) FROM nums")
            .unwrap();
        let mut results = Vec::new();
        stmt.run_with_row_callback(|row| {
            results.push((
                row.get::<i64>(0).unwrap(),
                row.get::<i64>(1).unwrap(),
                row.get::<i64>(2).unwrap(),
            ));
            Ok(())
        })
        .unwrap();

        assert_eq!(results.len(), 4);
        for (i, &(add_r, mul_r, sub_r)) in results.iter().enumerate() {
            let x = (i + 1) as i64;
            assert_eq!(add_r, x * 2, "add2({x},{x}) should be {}", x * 2);
            assert_eq!(mul_r, x * x, "mul2({x},{x}) should be {}", x * x);
            assert_eq!(sub_r, x * 2, "sub2({},{x}) should be {}", x * 3, x * 2);
        }

        // With 4 rows * 3 funcs = 12 calls total, and add2 (131072) fills the entire
        // budget by itself, every row starts by evicting everything cached to fit add2,
        // then mul2 evicts add2, then sub2 fits alongside mul2.
        // Result: all 12 calls are misses, no hits.
        assert!(
            stmt.metrics().wasm_cache_evictions > 0,
            "must have evictions with 3 funcs that don't all fit"
        );
        assert_eq!(
            stmt.metrics().wasm_cache_hits + stmt.metrics().wasm_cache_misses,
            12,
            "total calls (hits + misses) should be 12"
        );
        // add2 is 131072 = full budget, so every row cycle flushes and recreates everything
        assert_eq!(
            stmt.metrics().wasm_cache_misses,
            12,
            "all calls are misses because add2 fills the entire budget"
        );
    }

    #[test]
    fn test_eviction_metrics_across_separate_statements() {
        // Each prepare() creates a fresh ProgramState with its own cache.
        // Verify that statement-level metrics accurately reflect per-statement behavior.
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        register_three_funcs(&conn);

        // Set cache big enough for all 3 (no eviction expected)
        conn.execute("PRAGMA wasm_cache_size = 10485760").unwrap(); // 10 MiB

        // Statement 1: first call is always a miss
        let mut stmt = conn.prepare("SELECT add2(10, 20)").unwrap();
        stmt.run_with_row_callback(|row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 30);
            Ok(())
        })
        .unwrap();
        assert_eq!(stmt.metrics().wasm_cache_misses, 1);
        assert_eq!(stmt.metrics().wasm_cache_hits, 0);
        assert_eq!(stmt.metrics().wasm_instances_created, 1);
        assert_eq!(stmt.metrics().wasm_cache_evictions, 0);

        // Statement 2: also a miss (different ProgramState, fresh cache)
        let mut stmt = conn.prepare("SELECT add2(30, 40)").unwrap();
        stmt.run_with_row_callback(|row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 70);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            stmt.metrics().wasm_cache_misses,
            1,
            "new statement = new cache = miss"
        );
        assert_eq!(stmt.metrics().wasm_cache_hits, 0);
        assert_eq!(stmt.metrics().wasm_instances_created, 1);

        // Statement 3: same func on multiple rows = 1 miss + (N-1) hits
        conn.execute("CREATE TABLE t(a INT, b INT)").unwrap();
        for i in 0..5 {
            conn.execute(format!("INSERT INTO t VALUES ({}, {})", i, i + 10))
                .unwrap();
        }
        let mut stmt = conn.prepare("SELECT mul2(a, b) FROM t").unwrap();
        let mut results = Vec::new();
        stmt.run_with_row_callback(|row| {
            results.push(row.get::<i64>(0).unwrap());
            Ok(())
        })
        .unwrap();
        assert_eq!(results, vec![0, 11, 24, 39, 56]);
        assert_eq!(stmt.metrics().wasm_cache_misses, 1, "1 miss on first row");
        assert_eq!(stmt.metrics().wasm_cache_hits, 4, "4 hits on rows 2-5");
        assert_eq!(stmt.metrics().wasm_cache_evictions, 0);
    }

    #[test]
    fn test_zero_cache_size_prevents_wasm_calls() {
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let add_hex = wasm_to_hex(&wat_to_wasm(ADD_WAT));
        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
        ))
        .unwrap();

        // Set cache to 0 — no WASM instances can be created
        conn.execute("PRAGMA wasm_cache_size = 0").unwrap();

        let result = conn.execute("SELECT add2(1, 2)");
        assert!(
            result.is_err(),
            "WASM UDF call should fail with zero cache budget"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("budget exhausted"),
            "error should mention budget exhausted, got: {err_msg}"
        );
    }

    #[test]
    fn test_pragma_used_is_zero_between_statements() {
        // Budget is released when ProgramState drops (statement ends).
        // PRAGMA wasm_cache_size should show used=0 between statements.
        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        let add_hex = wasm_to_hex(&wat_to_wasm(ADD_WAT));
        conn.execute(format!(
            "CREATE FUNCTION add2 LANGUAGE wasm AS X'{add_hex}' EXPORT 'add'"
        ))
        .unwrap();

        // Call a WASM UDF
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT add2(1, 2)");
        assert_eq!(rows, vec![(3,)]);

        // After the statement completes, used should be 0 (cache dropped)
        let rows: Vec<(i64, i64)> = conn.exec_rows("PRAGMA wasm_cache_size");
        assert_eq!(
            rows[0].0, 0,
            "used should be 0 after statement ends (cache dropped)"
        );
    }

    /// CREATE FUNCTION must fail if the name shadows a built-in function.
    #[test]
    fn test_create_function_builtin_name_rejected() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn = db.connect_limbo();

        // "abs" is a builtin scalar function
        let result = conn.execute(format!(
            "CREATE FUNCTION abs LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
        ));
        assert!(
            result.is_err(),
            "CREATE FUNCTION should fail when the name shadows a builtin"
        );

        // "count" is a builtin aggregate function
        let result = conn.execute(format!(
            "CREATE FUNCTION count LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
        ));
        assert!(
            result.is_err(),
            "CREATE FUNCTION should fail when the name shadows a builtin aggregate"
        );
    }

    /// A new connection opened after CREATE FUNCTION should see the function
    /// without any special WASM loading in refresh_schema.
    #[test]
    fn test_new_connection_sees_function_created_by_another() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn1 = db.connect_limbo();
        conn1
            .execute(format!(
                "CREATE FUNCTION myadd LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
            ))
            .unwrap();

        // Verify conn1 can call it
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT myadd(10, 20)");
        assert_eq!(rows, vec![(30,)]);

        // Open a second connection on the same database — should see myadd
        let conn2 = db.connect_limbo();
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT myadd(3, 4)");
        assert_eq!(
            rows,
            vec![(7,)],
            "new connection must see function created by another connection"
        );
    }

    /// An already-open connection should see a function created by a sibling
    /// connection after the schema version changes (triggers reparse).
    #[test]
    fn test_existing_connection_sees_function_after_schema_change() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();

        // Open both connections before CREATE FUNCTION
        let conn1 = db.connect_limbo();
        let conn2 = db.connect_limbo();

        conn1
            .execute(format!(
                "CREATE FUNCTION myadd LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
            ))
            .unwrap();

        // conn2 was already open — it should see myadd after schema refresh
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT myadd(5, 6)");
        assert_eq!(
            rows,
            vec![(11,)],
            "already-open connection must see function after schema change"
        );
    }

    /// Exercises the maybe_reparse_schema → reparse_schema path.
    /// Connection 2 creates a table, bumping the schema cookie on disk.
    /// Connection 1 then runs a query — the VDBE detects the stale cookie and
    /// triggers maybe_reparse_schema, which internally calls prepare() →
    /// maybe_update_schema(). This must not deadlock (syms.write() inside
    /// maybe_update_schema while reparse_schema is in progress).
    #[test]
    fn test_schema_reparse_with_wasm_function_does_not_deadlock() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let db = TempDatabase::builder()
            .with_opts(opts_with_wasm_runtime())
            .build();
        let conn1 = db.connect_limbo();
        let conn2 = db.connect_limbo();

        // Create a WASM function on conn1
        conn1
            .execute(format!(
                "CREATE FUNCTION myadd LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
            ))
            .unwrap();

        // Verify it works on conn1
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT myadd(1, 2)");
        assert_eq!(rows, vec![(3,)]);

        // conn2 modifies the schema, bumping the on-disk schema cookie
        conn2
            .execute("CREATE TABLE reparse_trigger (id INTEGER PRIMARY KEY)")
            .unwrap();

        // conn1 now has a stale schema cookie. The next query will detect the
        // mismatch and trigger maybe_reparse_schema. This must not deadlock.
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT myadd(10, 20)");
        assert_eq!(rows, vec![(30,)], "function must work after schema reparse");
    }

    /// Functions survive database close + reopen (persisted in __turso_internal_wasm).
    #[test]
    fn test_function_survives_database_reopen() {
        let wasm_bytes = wat_to_wasm(ADD_WAT);
        let hex = wasm_to_hex(&wasm_bytes);

        let dir = TempDir::new().unwrap().keep();
        let db_path = dir.join("reopen_test.db");

        // First open: create the function
        {
            let db = TempDatabase::builder()
                .with_db_path(&db_path)
                .with_opts(opts_with_wasm_runtime())
                .build();
            let conn = db.connect_limbo();
            conn.execute(format!(
                "CREATE FUNCTION myadd LANGUAGE wasm AS X'{hex}' EXPORT 'add'"
            ))
            .unwrap();
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT myadd(1, 2)");
            assert_eq!(rows, vec![(3,)]);
        }

        // Second open: function should still work
        {
            let db = TempDatabase::builder()
                .with_db_path(&db_path)
                .with_opts(opts_with_wasm_runtime())
                .build();
            let conn = db.connect_limbo();
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT myadd(10, 20)");
            assert_eq!(rows, vec![(30,)], "function must survive database reopen");
        }
    }
}
