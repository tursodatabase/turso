# Probe tests for the native Turso TCL extension (bindings/tcl/turso_tcl.c).
#
# Validates the three capabilities that the subprocess shim cannot provide:
#   1. Real engine error codes via [db errorcode].
#   2. Accurate DML change counters via [db changes] / [db total_changes].
#   3. In-process Tcl function registration via [db func].
#
# Run via:
#   LD_LIBRARY_PATH=target/debug tclsh bindings/tcl/test_probes.tcl
#
# Exit code: 0 on success, 1 on any failure.

set pass 0
set fail 0

proc ok {label} {
    puts "PASS  $label"
    incr ::pass
}

proc fail {label want got} {
    puts "FAIL  $label"
    puts "      want: $want"
    puts "      got:  $got"
    incr ::fail
}

proc assert_eq {label want got} {
    if {$got eq $want} { ok $label } else { fail $label $want $got }
}

proc assert_ne {label not_want got} {
    if {$got ne $not_want} {
        ok $label
    } else {
        fail $label "anything other than $not_want" $got
    }
}

# ---------------------------------------------------------------------------
# Load the native extension.
# ---------------------------------------------------------------------------

set here [file dirname [file normalize [info script]]]
set lib  [file join $here libturso_tcl.so]

if {![file exists $lib]} {
    puts "ERROR: $lib not found — run 'make docker-build' first"
    exit 1
}

if {[catch {load $lib Tursotcl} err]} {
    puts "ERROR: failed to load $lib: $err"
    exit 1
}

# Use an in-memory database so tests leave no files on disk.
sqlite3 db :memory:

# ---------------------------------------------------------------------------
# Probe 1: error code fidelity.
# The subprocess shim always returned 0; the native module returns the real
# SQLite result code from the engine.
# ---------------------------------------------------------------------------

catch {db eval {SELECT * FROM no_such_table;}}
assert_ne "errorcode is non-zero after bad query" 0 [db errorcode]

# ---------------------------------------------------------------------------
# Probe 2: DML change counters.
# The subprocess shim always returned 0; the native module tracks them via
# sqlite3_changes() and sqlite3_total_changes().
# ---------------------------------------------------------------------------

db eval {CREATE TABLE tc(x);}
db eval {INSERT INTO tc VALUES(1);}
assert_eq "changes is 1 after single INSERT"   1 [db changes]
db eval {INSERT INTO tc VALUES(2);}
assert_ne "total_changes accumulates across stmts" 0 [db total_changes]

# ---------------------------------------------------------------------------
# Probe 3: in-process Tcl function registration.
# The subprocess shim accepted [db func] but did not wire the callback into
# the SQL engine; the native module routes calls through sqlite3_create_function.
# ---------------------------------------------------------------------------

db func my_echo {x} { return $x }
set result [db eval {SELECT my_echo(42);}]
assert_eq "db func result echoed back" 42 $result

db func add2 {a b} { expr {$a + $b} }
set result [db eval {SELECT add2(3, 7);}]
assert_eq "db func with two args" 10 $result

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

db close
puts ""
puts "$pass passed, $fail failed"

if {$fail > 0} { exit 1 }
