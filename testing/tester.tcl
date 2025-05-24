set sqlite_exec [expr {[info exists env(SQLITE_EXEC)] ? $env(SQLITE_EXEC) : "sqlite3"}]
set test_dbs [list "testing/testing.db" "testing/testing_norowidalias.db"]
set test_small_dbs [list "testing/testing_small.db" ]

proc error_put {sql} {
    puts [format "\033\[1;31mTest FAILED:\033\[0m %s" $sql ]
}

proc test_put {msg db test_name} {
    puts [format "\033\[1;34m(%s)\033\[0m %s $msg: \033\[1;32m%s\033\[0m" $db [string repeat " " [expr {40 - [string length $db]}]] $test_name]
}

proc evaluate_sql {sqlite_exec db_name sql} {
    set command [list $sqlite_exec $db_name $sql]
    set output [exec {*}$command]
    return $output
}

proc run_test {sqlite_exec db_name sql expected_output} {
    set actual_output [evaluate_sql $sqlite_exec $db_name $sql]
    if {$actual_output ne $expected_output} {
        error_put $sql
        puts "returned '$actual_output'"
        puts "expected '$expected_output'"
        exit 1
    }
}

proc do_execsql_test {test_name sql_statements expected_outputs} {
    foreach db $::test_dbs {
        test_put "Running test" $db $test_name
        set combined_sql [string trim $sql_statements]
        set combined_expected_output [join $expected_outputs "\n"]
        run_test $::sqlite_exec $db $combined_sql $combined_expected_output
    }
}

proc do_execsql_test_small {test_name sql_statements expected_outputs} {
    foreach db $::test_small_dbs {
        test_put "Running test" $db $test_name
        set combined_sql [string trim $sql_statements]
        set combined_expected_output [join $expected_outputs "\n"]
        run_test $::sqlite_exec $db $combined_sql $combined_expected_output
    }
}


proc do_execsql_test_regex {test_name sql_statements expected_regex} {
    foreach db $::test_dbs {
        test_put "Running test" $db $test_name
        set combined_sql [string trim $sql_statements]
        set actual_output [evaluate_sql $::sqlite_exec $db $combined_sql]

        # Validate the actual output against the regular expression
        if {![regexp $expected_regex $actual_output]} {
            error_put $sql_statements
            puts "returned '$actual_output'"
            puts "expected to match regex '$expected_regex'"
            exit 1
        }
    }
}


proc do_execsql_test_on_specific_db {db_name test_name sql_statements expected_outputs} {
    test_put "Running test" $db_name $test_name
    set combined_sql [string trim $sql_statements]
    set combined_expected_output [join $expected_outputs "\n"]
    run_test $::sqlite_exec $db_name $combined_sql $combined_expected_output
}

proc within_tolerance {actual expected tolerance} {
    expr {abs($actual - $expected) <= $tolerance}
}

# This function is used to test floating point values within a tolerance
# FIXME: When Limbo's floating point presentation matches to SQLite, this could/should be removed
proc do_execsql_test_tolerance {test_name sql_statements expected_outputs tolerance} {
    foreach db $::test_dbs {
        test_put "Running test" $db $test_name
        set combined_sql [string trim $sql_statements]
        set actual_output [evaluate_sql $::sqlite_exec $db $combined_sql]
        set actual_values [split $actual_output "\n"]
        set expected_values [split $expected_outputs "\n"]

        if {[llength $actual_values] != [llength $expected_values]} {
            error_put $sql_statements
            puts "returned '$actual_output'"
            puts "expected '$expected_outputs'"
            exit 1
        }

        for {set i 0} {$i < [llength $actual_values]} {incr i} {
            set actual [lindex $actual_values $i]
            set expected [lindex $expected_values $i]

            if {![within_tolerance $actual $expected $tolerance]} {
                set lower_bound [expr {$expected - $tolerance}]
                set upper_bound [expr {$expected + $tolerance}]
                error_put $sql_statements
                puts "returned '$actual'"
                puts "expected a value within the range \[$lower_bound, $upper_bound\]"
                exit 1
            }
        }
    }
}
# This procedure passes the test if the output contains error messages
proc run_test_expecting_any_error {sqlite_exec db_name sql} {
    # Execute the SQL command and capture output
    set command [list $sqlite_exec $db_name $sql]

    # Use catch to handle both successful and error cases
    catch {exec {*}$command} result options

    # Check if the output contains error indicators (×, error, syntax error, etc.)
    if {[regexp {(error|ERROR|Error|×|syntax error|failed)} $result]} {
        # Error found in output - test passed
        puts "\033\[1;32mTest PASSED:\033\[0m Got expected error"
        return 1
    }

    # No error indicators in output
    error_put $sql
    puts "Expected an error but command output didn't indicate any error: '$result'"
    exit 1
}

# This procedure passes if error matches a specific pattern
proc run_test_expecting_error {sqlite_exec db_name sql expected_error_pattern} {
    # Execute the SQL command and capture output
    set command [list $sqlite_exec $db_name $sql]

    # Capture output whether command succeeds or fails
    catch {exec {*}$command} result options

    # Check if the output contains error indicators first
    if {![regexp {(error|ERROR|Error|×|syntax error|failed)} $result]} {
        error_put $sql
        puts "Expected an error matching '$expected_error_pattern'"
        puts "But command output didn't indicate any error: '$result'"
        exit 1
    }

    # Now check if the error message matches the expected pattern
    if {![regexp $expected_error_pattern $result]} {
        error_put $sql
        puts "Error occurred but didn't match expected pattern."
        puts "Output was: '$result'"
        puts "Expected pattern: '$expected_error_pattern'"
        exit 1
    }

    # If we get here, the test passed - got expected error matching pattern
    return 1
}

# This version accepts exact error text, ignoring formatting
proc run_test_expecting_error_content {sqlite_exec db_name sql expected_error_text} {
    # Execute the SQL command and capture output
    set command [list $sqlite_exec $db_name $sql]

    # Capture output whether command succeeds or fails
    catch {exec {*}$command} result options

    # Check if the output contains error indicators first
    if {![regexp {(error|ERROR|Error|×|syntax error|failed)} $result]} {
        error_put $sql
        puts "Expected an error with text: '$expected_error_text'"
        puts "But command output didn't indicate any error: '$result'"
        exit 1
    }

    # Normalize both the actual and expected error messages
    # Remove all whitespace, newlines, and special characters for comparison
    set normalized_actual [regsub -all {[[:space:]]|[[:punct:]]} $result ""]
    set normalized_expected [regsub -all {[[:space:]]|[[:punct:]]} $expected_error_text ""]

    # Convert to lowercase for case-insensitive comparison
    set normalized_actual [string tolower $normalized_actual]
    set normalized_expected [string tolower $normalized_expected]

    # Check if the normalized strings contain the same text
    if {[string first $normalized_expected $normalized_actual] == -1} {
        error_put $sql
        puts "Error occurred but content didn't match."
        puts "Output was: '$result'"
        puts "Expected text: '$expected_error_text'"
        exit 1
    }

    # If we get here, the test passed - got error with expected content
    return 1
}

proc do_execsql_test_error {test_name sql_statements expected_error_pattern} {
    foreach db $::test_dbs {
        test_put "Running error test" $db $test_name
        set combined_sql [string trim $sql_statements]
        run_test_expecting_error $::sqlite_exec $db $combined_sql $expected_error_pattern
    }
}

proc do_execsql_test_error_content {test_name sql_statements expected_error_text} {
    foreach db $::test_dbs {
        test_put "Running error content test" $db $test_name
        set combined_sql [string trim $sql_statements]
        run_test_expecting_error_content $::sqlite_exec $db $combined_sql $expected_error_text
    }
}

proc do_execsql_test_any_error {test_name sql_statements} {
    foreach db $::test_dbs {
        test_put "Running any-error test" $db $test_name
        set combined_sql [string trim $sql_statements]
        run_test_expecting_any_error $::sqlite_exec $db $combined_sql
    }
}

proc do_execsql_test_in_memory_any_error {test_name sql_statements} {
    test_put "Running any-error test" in-memory $test_name

    # Use ":memory:" special filename for in-memory database
    set db_name ":memory:"

    set combined_sql [string trim $sql_statements]
    run_test_expecting_any_error $::sqlite_exec $db_name $combined_sql
}

proc do_execsql_test_in_memory_error_content {test_name sql_statements expected_error_text} {
    test_put "Running error content test" in-memory $test_name

    # Use ":memory:" special filename for in-memory database
    set db_name ":memory:"

    set combined_sql [string trim $sql_statements]
    run_test_expecting_error_content $::sqlite_exec $db_name $combined_sql $expected_error_text
}
