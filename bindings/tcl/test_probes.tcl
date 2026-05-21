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

proc assert_gt {label got minimum} {
    if {$got > $minimum} { ok $label } else { fail $label "greater than $minimum" $got }
}

proc assert_lt {label got maximum} {
    if {$got < $maximum} { ok $label } else { fail $label "less than $maximum" $got }
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
# Probe 4: multi-statement `db eval` accumulates rows from every statement.
# Regression test for the select3-8.100 failure where the per-statement reset
# of the result list dropped rows from earlier statements (e.g. a SELECT
# followed by `PRAGMA integrity_check`). Upstream tclsqlite appends rows from
# every statement in the script to a single result list.
# ---------------------------------------------------------------------------

db eval {DROP TABLE IF EXISTS tm;}
set multi_sql {
    CREATE TABLE tm(a, b);
    INSERT INTO tm VALUES (1, 'one'), (2, 'two');
    SELECT a, b FROM tm ORDER BY a;
    SELECT count(*) FROM tm;
}
assert_eq "multi-statement db eval keeps rows from every statement" \
    {1 one 2 two 2} [db eval $multi_sql]

# ---------------------------------------------------------------------------
# Probe 5: VACUUM is enabled for the native binding and preserves data.
# ---------------------------------------------------------------------------

set vacuum_db [file join $here vacuum-probe-[pid].db]
file delete -force $vacuum_db ${vacuum_db}-wal ${vacuum_db}-shm
sqlite3 vac $vacuum_db
vac eval {
    CREATE TABLE tv(id INTEGER PRIMARY KEY, payload BLOB);
    INSERT INTO tv SELECT value, randomblob(4096) FROM generate_series(1, 64);
    DELETE FROM tv WHERE id > 8;
}
vac eval {PRAGMA wal_checkpoint(TRUNCATE);}
set before_page_count [lindex [vac eval {PRAGMA page_count;}] 0]
set before_freelist_count [lindex [vac eval {PRAGMA freelist_count;}] 0]
set before_size [file size $vacuum_db]
assert_gt "vacuum workload creates free pages" $before_freelist_count 0

vac eval {VACUUM;}
vac eval {PRAGMA wal_checkpoint(TRUNCATE);}
set after_page_count [lindex [vac eval {PRAGMA page_count;}] 0]
set after_freelist_count [lindex [vac eval {PRAGMA freelist_count;}] 0]
set after_size [file size $vacuum_db]
assert_lt "vacuum reduces page count" $after_page_count $before_page_count
assert_eq "vacuum clears freelist" 0 $after_freelist_count
assert_lt "vacuum shrinks database file" $after_size $before_size

assert_eq "vacuum keeps data and integrity" \
    {8 ok} [vac eval {SELECT count(*) FROM tv; PRAGMA integrity_check;}]
vac close
file delete -force $vacuum_db ${vacuum_db}-wal ${vacuum_db}-shm

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

db close
puts ""
puts "$pass passed, $fail failed"

if {$fail > 0} { exit 1 }
