# SQLite Test Framework - Simplified Version
# Based on the official SQLite tester.tcl
#
# Requires the native TCL extension (libturso_tcl) to be built.
# Build with: cargo build -p turso-tcl

# Global variables for test execution (safe to re-initialize)
if {![info exists TC(errors)]} {
  set TC(errors) 0
}
if {![info exists TC(count)]} {
  set TC(count) 0
}
if {![info exists TC(fail_list)]} {
  set TC(fail_list) [list]
}
if {![info exists testprefix]} {
  set testprefix ""
}

set script_dir [file dirname [file dirname [file dirname [file normalize [info script]]]]]
set test_db "test.db"

# Load the native TCL extension (libturso_tcl).
# This provides a real in-process sqlite3 command backed by the Turso engine.
set _native_loaded 0
foreach _native_candidate [list \
    [file join $script_dir "bindings" "tcl" "libturso_tcl.so"] \
    [file join $script_dir "bindings" "tcl" "libturso_tcl.dylib"]] {
  if {[file exists $_native_candidate]} {
    if {![catch {load $_native_candidate Tursotcl} _native_load_err]} {
      set _native_loaded 1
      break
    } else {
      puts stderr "Failed to load $_native_candidate: $_native_load_err"
    }
  }
}
if {!$_native_loaded} {
  puts stderr "FATAL: Could not load native TCL extension (libturso_tcl)."
  puts stderr "Build it with: cargo build -p turso-tcl"
  puts stderr "Searched:"
  puts stderr "  [file join $script_dir bindings tcl libturso_tcl.so]"
  puts stderr "  [file join $script_dir bindings tcl libturso_tcl.dylib]"
  exit 1
}
catch {unset _native_candidate}
catch {unset _native_load_err}
catch {unset _native_loaded}

# Create or reset test database
proc reset_db {} {
  global test_db
  file delete -force $test_db
  file delete -force "${test_db}-journal"
  file delete -force "${test_db}-wal"

  if {[llength [info commands db]] > 0} {
    catch {db close}
  }
  sqlite3 db $test_db
}

# Execute SQL and return results
proc execsql {sql {db db}} {
  return [$db eval $sql]
}

# Execute SQL and return first value only (similar to db one)
proc db_one {sql {db db}} {
  set result [execsql $sql $db]
  if {[llength $result] > 0} {
    return [lindex $result 0]
  } else {
    return ""
  }
}

# Execute SQL and return results with column names
# Format: column1 value1 column2 value2 ... (alternating for each row)
proc execsql2 {sql {db db}} {
  set result {}
  $db eval $sql row {
    foreach col $row(*) {
      lappend result $col $row($col)
    }
  }
  return $result
}

# Execute SQL and catch errors
proc catchsql {sql {db db}} {
  if {[catch {execsql $sql $db} result]} {
    return [list 1 $result]
  } else {
    return [list 0 $result]
  }
}

# Main test execution function
proc do_test {name cmd expected} {
  global TC testprefix

  # Add prefix if it exists
  if {$testprefix ne ""} {
    set name "${testprefix}-$name"
  }

  incr TC(count)
  puts -nonewline "$name... "
  flush stdout

  if {[catch {uplevel #0 $cmd} result]} {
    puts "ERROR: $result"
    lappend TC(fail_list) $name
    incr TC(errors)
    return
  }

  # Compare result with expected
  set ok 0
  if {[regexp {^/.*/$} $expected]} {
    # Regular expression match
    set pattern [string range $expected 1 end-1]
    set ok [regexp $pattern $result]
  } elseif {[string match "*" $expected]} {
    # Glob pattern match
    set ok [string match $expected $result]
  } else {
    # Exact match - handle both list and string formats
    if {[llength $expected] > 1 || [llength $result] > 1} {
      # List comparison
      set ok [expr {$result eq $expected}]
    } else {
      # String comparison
      set ok [expr {[string trim $result] eq [string trim $expected]}]
    }
  }

  if {$ok} {
    puts "Ok"
  } else {
    puts "FAILED"
    puts "  Expected: $expected"
    puts "  Got:      $result"
    lappend TC(fail_list) $name
    incr TC(errors)
  }
}

# Execute SQL test with expected results
proc do_execsql_test {name sql {expected {}}} {
  do_test $name [list execsql $sql] $expected
}

# Execute SQL test expecting an error
proc do_catchsql_test {name sql expected} {
  do_test $name [list catchsql $sql] $expected
}

# Placeholder for virtual table conditional tests
proc do_execsql_test_if_vtab {name sql expected} {
  # For now, just run the test (assume vtab support)
  do_execsql_test $name $sql $expected
}

# Database integrity check
proc integrity_check {name} {
  do_execsql_test $name {PRAGMA integrity_check} {ok}
}

# Query execution plan test (simplified)
proc do_eqp_test {name sql expected} {
  do_execsql_test $name "EXPLAIN QUERY PLAN $sql" $expected
}

# Capability checking (simplified - assume all features available)
proc ifcapable {expr code {else_keyword ""} {elsecode ""}} {
  # Check capabilities and execute appropriate code
  set capable 1

  # Simple capability checking for common features
  foreach capability [split $expr {&|}] {
    set capability [string trim $capability]
    set negate 0
    if {[string index $capability 0] eq "!"} {
      set negate 1
      set capability [string range $capability 1 end]
    }

    # Check specific capabilities
    set has_capability 1
    switch -- $capability {
      "autovacuum" { set has_capability [expr {$::AUTOVACUUM != 0}] }
      "vacuum" { set has_capability [expr {$::OMIT_VACUUM == 0}] }
      "tempdb" { set has_capability 1 }
      "attach" { set has_capability 1 }
      "compound" { set has_capability 1 }
      "subquery" { set has_capability 1 }
      "view" { set has_capability 1 }
      "trigger" { set has_capability 1 }
      "foreignkey" { set has_capability 1 }
      "check" { set has_capability 1 }
      "vtab" { set has_capability 1 }
      "rtree" { set has_capability 0 }
      "fts3" { set has_capability 0 }
      "fts4" { set has_capability 0 }
      "fts5" { set has_capability 0 }
      "json1" { set has_capability 1 }
      "windowfunc" { set has_capability 1 }
      "altertable" { set has_capability 1 }
      "analyze" { set has_capability 1 }
      "cte" { set has_capability 1 }
      "with" { set has_capability 1 }
      "upsert" { set has_capability 1 }
      "gencol" { set has_capability 1 }
      "generated_always" { set has_capability 1 }
      default { set has_capability 1 }
    }

    if {$negate} {
      set has_capability [expr {!$has_capability}]
    }

    # Handle AND/OR logic (simplified - just use AND for now)
    if {!$has_capability} {
      set capable 0
      break
    }
  }

  if {$capable} {
    uplevel 1 $code
  } elseif {$else_keyword eq "else" && $elsecode ne ""} {
    uplevel 1 $elsecode
  }
}

# Capability test (simplified)
proc capable {expr} {
  # For simplicity, assume all capabilities are available
  return 1
}

# Sanitizer detection (simplified - assume no sanitizers)
proc clang_sanitize_address {} {
  return 0
}

# SQLite configuration constants (set to reasonable defaults)
# These are typically set based on compile-time options
set SQLITE_MAX_COMPOUND_SELECT 500
set SQLITE_MAX_VDBE_OP 25000
set SQLITE_MAX_FUNCTION_ARG 127
set SQLITE_MAX_ATTACHED 10
set SQLITE_MAX_VARIABLE_NUMBER 999
set SQLITE_MAX_COLUMN 2000
set SQLITE_MAX_SQL_LENGTH 1000000
set SQLITE_MAX_EXPR_DEPTH 1000
set SQLITE_MAX_LIKE_PATTERN_LENGTH 50000
set SQLITE_MAX_TRIGGER_DEPTH 1000

# SQLite compile-time option variables
set AUTOVACUUM 1      ;# Whether AUTOVACUUM is enabled
set OMIT_VACUUM 0     ;# Whether VACUUM is omitted
set TEMP_STORE 1      ;# Where temp tables are stored (0=disk, 1=file, 2=memory)
set DEFAULT_AUTOVACUUM 0  ;# Default autovacuum setting

# Support for sqlite3_limit command at the global level
# This is called as sqlite3_limit db LIMIT_TYPE ?VALUE?
proc sqlite3_limit {db limit_type {value {}}} {
  # If a value is provided, we're setting the limit
  if {$value ne ""} {
    return $value
  } else {
    switch -- $limit_type {
      SQLITE_LIMIT_COMPOUND_SELECT { return 500 }
      SQLITE_LIMIT_VDBE_OP { return 25000 }
      SQLITE_LIMIT_FUNCTION_ARG { return 127 }
      SQLITE_LIMIT_ATTACHED { return 10 }
      SQLITE_LIMIT_VARIABLE_NUMBER { return 999 }
      SQLITE_LIMIT_COLUMN { return 2000 }
      SQLITE_LIMIT_SQL_LENGTH { return 1000000 }
      SQLITE_LIMIT_EXPR_DEPTH { return 1000 }
      SQLITE_LIMIT_LIKE_PATTERN_LENGTH { return 50000 }
      SQLITE_LIMIT_TRIGGER_DEPTH { return 1000 }
      default { return 1000000 }
    }
  }
}

# Support for sqlite3_db_config command
proc sqlite3_db_config {db option {value {}}} {
  if {$value ne ""} {
    return 0
  } else {
    switch -- $option {
      SQLITE_DBCONFIG_DQS_DML { return 0 }
      SQLITE_DBCONFIG_DQS_DDL { return 0 }
      SQLITE_DBCONFIG_LOOKASIDE { return {1 1200 100} }
      SQLITE_DBCONFIG_ENABLE_FKEY { return 0 }
      SQLITE_DBCONFIG_ENABLE_TRIGGER { return 1 }
      SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER { return 0 }
      SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION { return 0 }
      SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE { return 0 }
      SQLITE_DBCONFIG_ENABLE_QPSG { return 0 }
      SQLITE_DBCONFIG_TRIGGER_EQP { return 0 }
      SQLITE_DBCONFIG_RESET_DATABASE { return 0 }
      SQLITE_DBCONFIG_DEFENSIVE { return 0 }
      SQLITE_DBCONFIG_WRITABLE_SCHEMA { return 0 }
      SQLITE_DBCONFIG_LEGACY_ALTER_TABLE { return 0 }
      SQLITE_DBCONFIG_ENABLE_VIEW { return 1 }
      SQLITE_DBCONFIG_LEGACY_FILE_FORMAT { return 0 }
      SQLITE_DBCONFIG_TRUSTED_SCHEMA { return 1 }
      default { return 0 }
    }
  }
}

# Support for optimization_control command
proc optimization_control {db optimization setting} {
  return ""
}

# File operation utilities
proc forcedelete {args} {
  foreach filename $args {
    catch {file delete -force $filename}
  }
}

proc delete_file {args} {
  foreach filename $args {
    file delete $filename
  }
}

proc forcecopy {from to} {
  catch {file delete -force $to}
  file copy -force $from $to
}

proc copy_file {from to} {
  file copy $from $to
}

# Finish test execution and report results
proc finish_test {} {
  global TC

  # Check if we're running as part of all.test - if so, don't exit
  if {[info exists ::ALL_TESTS]} {
    return
  }

  puts ""
  puts "=========================================="
  if {$TC(errors) == 0} {
    puts "All $TC(count) tests passed!"
  } else {
    puts "$TC(errors) errors out of $TC(count) tests"
    puts "Failed tests: $TC(fail_list)"
  }
  puts "=========================================="
}

reset_db
