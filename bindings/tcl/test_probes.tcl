# Probe tests for the native Turso TCL extension (bindings/tcl/turso_tcl.c).
#
# Validates the capabilities that the subprocess shim cannot provide:
#   1. Real engine error codes via [db errorcode].
#   2. Accurate DML change counters via [db changes] / [db total_changes].
#   3. In-process Tcl function registration via [db func].
#   4. Rows from every statement of a multi-statement [db eval].
#   5. ATTACH / DETACH, which the engine gates behind its experimental flag.
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
# Probe 5: ATTACH and DETACH.
# The engine refuses ATTACH unless the database was opened with the attach
# option, which the tursodb CLI spells --experimental-attach. The module turns
# that option on for every database it opens, so the upstream attach*.test
# files get a working ATTACH instead of an immediate "experimental feature"
# error.
# ---------------------------------------------------------------------------

assert_eq "ATTACH is accepted" "" [db eval {ATTACH ':memory:' AS aux}]
db eval {CREATE TABLE aux.t5(x); INSERT INTO aux.t5 VALUES (42);}
assert_eq "attached database is queryable" 42 [db eval {SELECT x FROM aux.t5}]
assert_eq "DETACH is accepted" "" [db eval {DETACH aux}]

# ---------------------------------------------------------------------------
# Probe 6: TCL variable binding in [db one] and [db exists].
# Both prepare their statement directly (bypassing the eval path), so they
# must bind TCL variables themselves; without that, $var silently binds NULL
# and the query returns an empty result.
# ---------------------------------------------------------------------------

db eval {CREATE TABLE tv(x);}
db eval {INSERT INTO tv VALUES (7), (8);}

set want 7
assert_eq "db one binds \$var from the caller scope" 7 \
    [db one {SELECT x FROM tv WHERE x = $want}]
assert_eq "db exists binds \$var from the caller scope" 1 \
    [db exists {SELECT 1 FROM tv WHERE x = $want}]
assert_eq "db exists is false for a non-matching \$var" 0 \
    [db exists {SELECT 1 FROM tv WHERE x = $want + 100}]

# ---------------------------------------------------------------------------
# Probe 7: [db transaction].
# The script runs inside a transaction: committed on success, rolled back
# when the script raises an error. Nested transactions use a savepoint, so
# an inner failure undoes only the inner work.
# ---------------------------------------------------------------------------

db eval {CREATE TABLE tt(x);}

db transaction {
    db eval {INSERT INTO tt VALUES (1);}
}
assert_eq "transaction commits on success" 1 [db one {SELECT count(*) FROM tt}]

set txerr [catch {
    db transaction {
        db eval {INSERT INTO tt VALUES (2);}
        error "boom"
    }
} txmsg]
assert_eq "transaction propagates the script error" 1 $txerr
assert_eq "transaction error message survives" "boom" $txmsg
assert_eq "transaction rolls back on error" 1 [db one {SELECT count(*) FROM tt}]

db transaction immediate {
    db eval {INSERT INTO tt VALUES (3);}
}
assert_eq "transaction accepts a type argument" 2 [db one {SELECT count(*) FROM tt}]

db transaction {
    db eval {INSERT INTO tt VALUES (4);}
    catch {
        db transaction {
            db eval {INSERT INTO tt VALUES (5);}
            error "inner boom"
        }
    }
}
assert_eq "inner transaction rolls back to its savepoint only" \
    {1 3 4} [db eval {SELECT x FROM tt ORDER BY x}]

# ---------------------------------------------------------------------------
# Probe 8: the [sqlite3_exec] test-harness command.
# Returns {rc results} where results is column names followed by row values
# (first row supplies the names), or {rc errmsg} on failure. %HH escapes in
# the SQL decode to raw bytes, as in upstream test1.c.
# ---------------------------------------------------------------------------

db eval {CREATE TABLE te(a, b); INSERT INTO te VALUES (1, 2), (3, 4);}

assert_eq "sqlite3_exec returns rc 0 plus names and rows" \
    {0 {a b 1 2 3 4}} [sqlite3_exec db {SELECT * FROM te ORDER BY a}]
assert_eq "sqlite3_exec renders NULL as the string NULL" \
    {0 {x NULL}} [sqlite3_exec db {SELECT NULL AS x}]
assert_eq "sqlite3_exec decodes %HH escapes" \
    {0 {x 41}} [sqlite3_exec db {SELECT hex('%41') AS x}]
assert_eq "sqlite3_exec reports SQL errors via rc" \
    1 [lindex [sqlite3_exec db {SELECT * FROM no_such_table}] 0]
set execerr [catch {sqlite3_exec nosuchdb {SELECT 1}} execmsg]
assert_eq "sqlite3_exec errors on an unknown handle" 1 $execerr

# ---------------------------------------------------------------------------
# Probe 9: [sqlite3_connection_pointer].
# Returns a value that identifies the database to C-API-level harness
# commands (here, the handle name itself), and errors on a non-database.
# ---------------------------------------------------------------------------

set DB [sqlite3_connection_pointer db]
assert_eq "sqlite3_connection_pointer result works as a DB argument" \
    {0 {one 1}} [sqlite3_exec $DB {SELECT 1 AS one}]
set ptrerr [catch {sqlite3_connection_pointer nosuchdb} ptrmsg]
assert_eq "sqlite3_connection_pointer errors on an unknown handle" 1 $ptrerr

# ---------------------------------------------------------------------------
# Probe 10: the [sqlite3_mprintf_*] / [sqlite3_snprintf_*] commands.
# Each formats through the real sqlite3_mprintf/sqlite3_snprintf C API, so
# these expectations (taken from upstream printf.test) exercise the engine's
# printf implementation.
# ---------------------------------------------------------------------------

assert_eq "mprintf_int formats three ints" \
    {This is a test 1,2,3} \
    [sqlite3_mprintf_int {This is a test %d,%d,%d} 1 2 3]
assert_eq "mprintf_int honors width and zero-pad flags" \
    {abc: (000012) (00000d) (000016) :xyz} \
    [sqlite3_mprintf_int {abc: (%06d) (%06x) (%06o) :xyz} 12 13 14]
assert_eq "mprintf_int64 formats past 32 bits" \
    {2147483647 2147483648 4294967296} \
    [sqlite3_mprintf_int64 {%lld %lld %lld} 2147483647 2147483648 4294967296]
assert_eq "mprintf_long truncates each argument to 32 bits" \
    {2147483647 2147483648 4294967295} \
    [sqlite3_mprintf_long {%lu %lu %lu} 0x7fffffff 0x80000000 0xffffffff]
assert_eq "mprintf_str formats two ints and a string" \
    {1 2 A String: (This is the string)} \
    [sqlite3_mprintf_str {%d %d A String: (%s)} 1 2 {This is the string}]
assert_eq "mprintf_str passes NULL when the string is omitted" \
    {1 2 A NULL pointer in %q: '(NULL)'} \
    [sqlite3_mprintf_str {%d %d A NULL pointer in %%q: '%q'} 1 2]
assert_eq "mprintf_stronly quotes via %q" \
    {Hi Y''all} [sqlite3_mprintf_stronly %q {Hi Y'all}]
assert_eq "mprintf_double formats two ints and a double" \
    {1 2 3.5} [sqlite3_mprintf_double {%d %d %g} 1 2 3.5]
assert_eq "mprintf_scaled formats the product of its doubles" \
    {A double: 1e+308} [sqlite3_mprintf_scaled {A double: %g} 1.0e307 10.0]
assert_eq "mprintf_hexdouble decodes an IEEE-754 bit pattern" \
    {10.00000000000000000000} [sqlite3_mprintf_hexdouble %.20f 4024000000000000]
assert_eq "mprintf_z_test joins arguments via %z" \
    {,one,two,three} [sqlite3_mprintf_z_test , one two three]
assert_eq "snprintf_int truncates to SIZE-1 characters" \
    {1234} [sqlite3_snprintf_int 5 {12345} 0]
assert_eq "snprintf_int leaves the buffer alone when SIZE is 0" \
    {abcdefghijklmnopqrstuvwxyz} [sqlite3_snprintf_int 0 {} 0]
assert_eq "snprintf_str truncates to SIZE-1 characters" \
    {x10 1} [sqlite3_snprintf_str 6 {x%d %d %s} 10 10 {This is the string}]

# ---------------------------------------------------------------------------
# Probe 11: [btree_varint_test].
# Round-trips values through core's varint encoder/decoder; returns "" on
# success. The ranges cover 1-byte, mid-size, and 9-byte encodings.
# ---------------------------------------------------------------------------

assert_eq "btree_varint_test round-trips small values" "" \
    [btree_varint_test 0 1 5000 1]
assert_eq "btree_varint_test round-trips large steps" "" \
    [btree_varint_test 100 1000000 5000 50000000]
assert_eq "btree_varint_test round-trips 9-byte encodings" "" \
    [btree_varint_test 0x10000000 0x10000000 5000 50000000]

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

db close
puts ""
puts "$pass passed, $fail failed"

if {$fail > 0} { exit 1 }
