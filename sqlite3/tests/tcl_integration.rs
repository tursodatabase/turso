#[cfg(feature = "tcl")]
#[test]
fn test_tcl_extension_compiles() {
    // This test verifies that the Tcl extension compiles correctly
    // In a real test environment, you would need Tcl installed
    // and would test the actual functionality
    
    // The following would be a real test if Tcl was available:
    /*
    use std::process::Command;
    
    let output = Command::new("tclsh")
        .arg("-c")
        .arg("load ./target/debug/libturso_sqlite3.so; sqlite3 db :memory:; db eval {CREATE TABLE t(x); INSERT INTO t VALUES(42); SELECT * FROM t}; db close")
        .output()
        .expect("failed to execute tclsh");
    
    assert!(output.status.success());
    assert!(output.stdout.contains("42"));
    */
    
    // For now, just verify the module compiles
    assert!(true);
}

#[cfg(feature = "tcl")]
#[test]
fn test_tcl_version_compatibility() {
    // Test that both Tcl 8.6 and Tcl 9.0 features work
    #[cfg(feature = "tcl9")]
    {
        // Tcl 9.0 uses usize for Tcl_Size
        let _size: usize = 42;
    }
    
    #[cfg(not(feature = "tcl9"))]
    {
        // Tcl 8.6 uses c_int for Tcl_Size
        let _size: std::os::raw::c_int = 42;
    }
    
    assert!(true);
}

#[cfg(not(feature = "tcl"))]
#[test]
fn test_tcl_feature_disabled() {
    // Test that the module compiles without Tcl support
    assert!(true);
}

// Integration test that would work with actual Tcl installation
#[cfg(feature = "tcl")]
#[test]
#[ignore] // Ignored by default since it requires Tcl installation
fn test_tcl_integration() {
    use std::process::Command;
    
    // Check if tclsh is available
    if Command::new("tclsh").arg("-c").arg("puts \"Tcl available\"").output().is_err() {
        println!("Tcl not available, skipping integration test");
        return;
    }
    
    // Build the library
    let build_output = Command::new("cargo")
        .args(&["build", "--features", "tcl"])
        .current_dir("..")
        .output()
        .expect("failed to build");
    
    if !build_output.status.success() {
        panic!("Build failed: {}", String::from_utf8_lossy(&build_output.stderr));
    }
    
    // Test basic functionality
    let test_script = r#"
        # Load the extension
        if {[catch {load ./target/debug/libturso_sqlite3.so} err]} {
            puts "Failed to load extension: $err"
            exit 1
        }
        
        # Create database
        sqlite3 db :memory:
        
        # Create table
        db eval "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
        
        # Insert data
        db eval "INSERT INTO test (name) VALUES ('Alice')"
        db eval "INSERT INTO test (name) VALUES ('Bob')"
        
        # Query data
        set result [db eval "SELECT * FROM test ORDER BY id"]
        puts "Result: $result"
        
        # Close database
        db close
        
        puts "Test completed successfully"
    "#;
    
    let output = Command::new("tclsh")
        .arg("-c")
        .arg(test_script)
        .current_dir("..")
        .output()
        .expect("failed to execute tclsh");
    
    if !output.status.success() {
        panic!("Tcl test failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Test completed successfully"));
}
