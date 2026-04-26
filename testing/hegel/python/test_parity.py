"""Hypothesis property-based parity tests for turso (local) vs turso_serverless.

Generates random sequences of database operations and asserts that both
drivers produce structurally identical results: same success/failure,
same column counts/names, same row counts, same value types, and same
actual cell values.

For the remote driver, set TURSO_DATABASE_URL (default http://localhost:8080).
Start a server with throttling disabled (so property tests don't hit 429s):
    docker run -d -p 8080:8080 \
      -e SQLD_MAX_CONCURRENT_REQUESTS=1024 \
      -e SQLD_MAX_CONCURRENT_CONNECTIONS=1024 \
      -e SQLD_DISABLE_INTELLIGENT_THROTTLING=true \
      ghcr.io/tursodatabase/libsql-server:latest
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import hypothesis.strategies as st
import turso
from hypothesis import HealthCheck, given, settings
from turso_serverless import connect as remote_connect

SERVER_URL = os.environ.get("TURSO_DATABASE_URL", "http://localhost:8080")
AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

# ---------------------------------------------------------------------------
# Load spec from ops.json
# ---------------------------------------------------------------------------

_SPEC_PATH = Path(__file__).parent.parent / "spec" / "ops.json"
with open(_SPEC_PATH) as _f:
    _SPEC = json.load(_f)

COL_TYPES = _SPEC["constants"]["col_types"]


# ---------------------------------------------------------------------------
# Strategies (operation generators)
# ---------------------------------------------------------------------------

table_names = st.integers(0, 5).map(lambda i: f"t_{i}")

# Build value strategy dynamically from spec
def _build_value_strategy(v):  # noqa: C901
    """Convert a value spec entry into a Hypothesis strategy."""
    vid = v["id"]
    if vid == "null":
        return st.none()
    if "random" in v:
        return st.integers(min_value=v["random"]["min"], max_value=v["random"]["max"])
    if "random_scaled" in v:
        r = v["random_scaled"]
        return st.floats(
            min_value=r["min"] / r["divisor"],
            max_value=r["max"] / r["divisor"],
            allow_nan=False,
            allow_infinity=False,
        )
    if "random_string" in v:
        return st.text(
            alphabet=st.characters(whitelist_categories=("L", "N", "P", "S", "Z")),
            max_size=v["random_string"]["max_len"],
        )
    if "random_bytes" in v:
        return st.binary(max_size=v["random_bytes"]["max_len"])
    if "oneof" in v:
        return st.sampled_from(v["oneof"])
    if "oneof_float" in v:
        return st.sampled_from(v["oneof_float"])
    if vid == "large_or_unicode":
        return st.one_of(
            st.just(b"\xab" * 256),
            st.sampled_from(_SPEC["unicode_options"]),
        )
    if "literal" in v:
        return st.just(v["literal"])
    if "literal_float" in v:
        return st.just(v["literal_float"])
    if "literal_bytes" in v:
        return st.just(b"")
    if "fill_bytes" in v:
        fb = v["fill_bytes"]
        return st.just(bytes([fb["byte"]]) * fb["len"])
    if "repeat_char" in v:
        rc = v["repeat_char"]
        return st.just(rc["char"] * rc["len"])
    raise ValueError(f"unknown value spec: {vid}")


values = st.one_of(*[_build_value_strategy(v) for v in _SPEC["values"]])

dynamic_cols = st.integers(1, _SPEC["constants"]["max_dynamic_cols"]).flatmap(
    lambda n: st.tuples(
        *[st.sampled_from(COL_TYPES).map(lambda t, i=i: (f"c{i}", t)) for i in range(n)]
    ).map(list)
)

batch_sql = st.tuples(
    st.integers(0, _SPEC["constants"]["num_tables"] - 1),
    st.integers(-1000, 1000),
).map(
    lambda args: (
        f"CREATE TABLE IF NOT EXISTS t_{args[0]} (a INTEGER, b TEXT); "
        f"INSERT INTO t_{args[0]} VALUES ({args[1]}, 'batch')"
    )
)


# Build op strategy dynamically from spec
def _build_op_strategy(op_spec):  # noqa: C901
    """Convert an op spec entry into a Hypothesis strategy."""
    oid = op_spec["id"]
    fields = op_spec.get("fields", {})

    if oid in ("begin", "commit", "rollback"):
        return st.just({"kind": oid})

    if oid == "invalid":
        return st.just({"kind": "invalid", "sql": op_spec["sql"]})

    d = {"kind": st.just(oid)}

    if "table" in fields:
        d["table"] = table_names

    if fields.get("values") == "table_values":
        d["values"] = st.tuples(values, values)

    if fields.get("value") == "one_value":
        d["value"] = values

    if fields.get("params") == "two_values":
        # Some ops with params also have a static SQL template (e.g. "param")
        if "sql" not in fields and "sql" in op_spec:
            d["sql"] = st.just(op_spec["sql"])
        d["params"] = st.tuples(values, values)

    if fields.get("expr") == "random_int_str":
        d["expr"] = st.integers(-1000, 1000).map(str)

    if fields.get("cols") == "dynamic_cols":
        d["cols"] = dynamic_cols

    if fields.get("sql") == "batch_sql":
        d["sql"] = batch_sql

    if fields.get("sql") == "error_sql":
        d["sql"] = st.sampled_from(_SPEC["constants"]["error_sqls"])

    if fields.get("named") == "named_values":
        names = op_spec.get("named_params", [])
        d["named"] = st.fixed_dictionaries({n: values for n in names})

    if fields.get("params_sets") == "three_param_pairs":
        d["params_sets"] = st.tuples(
            st.tuples(values, values),
            st.tuples(values, values),
            st.tuples(values, values),
        )

    return st.fixed_dictionaries(d)


ops = st.one_of(*[_build_op_strategy(op) for op in _SPEC["ops"]])


# ---------------------------------------------------------------------------
# Value comparison with float epsilon tolerance
# ---------------------------------------------------------------------------


def cell_equal(a, b):
    """Compare two cell values with float epsilon tolerance and int/real crossover."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False

    # Both float
    if isinstance(a, float) and isinstance(b, float):
        mx = max(abs(a), abs(b))
        if mx == 0:
            return True
        return abs(a - b) / mx < 1e-12

    # Int/float crossover
    if isinstance(a, int) and isinstance(b, float):
        return abs(float(a) - b) < 1e-12
    if isinstance(a, float) and isinstance(b, int):
        return abs(a - float(b)) < 1e-12

    # Bytes
    if isinstance(a, (bytes, bytearray, memoryview)) and isinstance(
        b, (bytes, bytearray, memoryview)
    ):
        return bytes(a) == bytes(b)

    return a == b


def results_equal(a, b):  # noqa: C901
    """Compare two result dicts with float epsilon tolerance on cell values."""
    # Compare all keys except 'values'
    for key in set(list(a.keys()) + list(b.keys())):
        if key == "values":
            continue
        if a.get(key) != b.get(key):
            return False

    # Compare values with epsilon
    a_vals = a.get("values")
    b_vals = b.get("values")
    if a_vals is None and b_vals is None:
        return True
    if a_vals is None or b_vals is None:
        return False
    if len(a_vals) != len(b_vals):
        return False
    for row_a, row_b in zip(a_vals, b_vals):
        if len(row_a) != len(row_b):
            return False
        for ca, cb in zip(row_a, row_b):
            if not cell_equal(ca, cb):
                return False
    return True


# ---------------------------------------------------------------------------
# Result structure
# ---------------------------------------------------------------------------


def value_type_tag(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "integer"
    if isinstance(v, int):
        return "integer"
    if isinstance(v, float):
        return "real"
    if isinstance(v, str):
        return "text"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "blob"
    return f"unknown({type(v).__name__})"


def _query_result(cur):
    """Extract a standard result dict from a cursor after a query."""
    rows = cur.fetchall()
    col_count = len(cur.description) if cur.description else 0
    col_names = [d[0] for d in cur.description] if cur.description else []
    types = [[value_type_tag(v) for v in row] for row in rows]
    vals = [list(row) for row in rows]
    return {
        "success": True,
        "column_count": col_count,
        "column_names": col_names,
        "row_count": len(rows),
        "value_types": types,
        "values": vals,
    }


def execute_op(conn, op):  # noqa: C901
    """Execute an operation against a DB-API 2.0 connection, return result dict."""
    try:
        kind = op["kind"]

        if kind == "create":
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {op['table']} (a INTEGER, b TEXT)"
            )
            return {"success": True, "column_count": 0, "row_count": 0}

        elif kind == "create_dynamic":
            col_defs = ", ".join(f"{n} {t}" for n, t in op["cols"])
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {op['table']} ({col_defs})"
            )
            return {"success": True, "column_count": 0, "row_count": 0}

        elif kind == "insert":
            placeholders = ", ".join("?" for _ in op["values"])
            conn.execute(
                f"INSERT INTO {op['table']} VALUES ({placeholders})", op["values"]
            )
            return {"success": True, "column_count": 0, "row_count": 1}

        elif kind == "insert_returning":
            placeholders = ", ".join("?" for _ in op["values"])
            cur = conn.execute(
                f"INSERT INTO {op['table']} VALUES ({placeholders}) RETURNING *",
                op["values"],
            )
            return _query_result(cur)

        elif kind == "delete_returning":
            cur = conn.execute(f"DELETE FROM {op['table']} RETURNING *")
            return _query_result(cur)

        elif kind == "update_returning":
            cur = conn.execute(
                f"UPDATE {op['table']} SET a = ? RETURNING *",
                (op["value"],),
            )
            return _query_result(cur)

        elif kind == "select":
            cur = conn.execute(f"SELECT * FROM {op['table']}")
            return _query_result(cur)

        elif kind == "select_value":
            cur = conn.execute(f"SELECT {op['expr']}")
            return _query_result(cur)

        elif kind == "begin":
            conn.execute("BEGIN")
            return {"success": True, "column_count": 0, "row_count": 0}

        elif kind == "commit":
            conn.commit()
            return {"success": True, "column_count": 0, "row_count": 0}

        elif kind == "rollback":
            conn.rollback()
            return {"success": True, "column_count": 0, "row_count": 0}

        elif kind == "invalid":
            cur = conn.execute(op["sql"])
            cur.fetchall()
            return {"success": True, "column_count": 0, "row_count": 0}

        elif kind == "param":
            cur = conn.execute(op["sql"], op["params"])
            return _query_result(cur)

        elif kind == "insert_affected":
            placeholders = ", ".join("?" for _ in op["values"])
            cur = conn.execute(
                f"INSERT INTO {op['table']} VALUES ({placeholders})", op["values"]
            )
            return {"success": True, "affected_rows": cur.rowcount}

        elif kind == "delete_affected":
            cur = conn.execute(f"DELETE FROM {op['table']}")
            return {"success": True, "affected_rows": cur.rowcount}

        elif kind == "update_affected":
            cur = conn.execute(
                f"UPDATE {op['table']} SET a = ?", (op["value"],)
            )
            return {"success": True, "affected_rows": cur.rowcount}

        elif kind == "insert_rowid":
            placeholders = ", ".join("?" for _ in op["values"])
            cur = conn.execute(
                f"INSERT INTO {op['table']} VALUES ({placeholders})", op["values"]
            )
            return {"success": True, "last_insert_rowid": cur.lastrowid}

        elif kind == "batch":
            conn.executescript(op["sql"])
            return {"success": True, "column_count": 0, "row_count": 0}

        elif kind == "select_limit":
            cur = conn.execute(f"SELECT * FROM {op['table']} LIMIT 1")
            return _query_result(cur)

        elif kind == "select_count":
            cur = conn.execute(f"SELECT COUNT(*), SUM(a) FROM {op['table']}")
            return _query_result(cur)

        elif kind == "select_expr":
            cur = conn.execute(
                "SELECT 1+1, 'hello'||'world', NULL, CAST(3.14 AS INTEGER), typeof(?)",
                (op["value"],),
            )
            return _query_result(cur)

        elif kind == "error_check":
            try:
                conn.execute(op["sql"])
                return {"success": True}
            except Exception:
                return {"success": False}

        elif kind == "prepared_reuse":
            sql = "SELECT ?, ?"
            all_results = []
            for params in op["params_sets"]:
                cur = conn.execute(sql, params)
                result = _query_result(cur)
                if result.get("values"):
                    all_results.extend(result["values"])
            col_names = ["?", "?"]
            types = [[value_type_tag(v) for v in row] for row in all_results]
            return {
                "success": True,
                "column_count": 2,
                "column_names": col_names,
                "row_count": len(all_results),
                "value_types": types,
                "values": all_results,
            }

        elif kind == "named_param":
            named = op["named"]
            cols = ", ".join(f":{k}" for k in named)
            sql = f"SELECT {cols}"
            cur = conn.execute(sql, named)
            return _query_result(cur)

        elif kind == "numbered_param":
            params = op["params"]
            cols = ", ".join(f"?{i+1}" for i in range(len(params)))
            sql = f"SELECT {cols}"
            cur = conn.execute(sql, params)
            return _query_result(cur)

        elif kind == "create_trigger":
            table = op["table"]
            audit = f"{table}_audit"
            conn.execute(f"CREATE TABLE IF NOT EXISTS {audit} (src TEXT, val)")
            trigger_sql = (
                f"CREATE TRIGGER IF NOT EXISTS tr_{table}_ins AFTER INSERT ON {table} "
                f"BEGIN "
                f"INSERT INTO {audit} VALUES ('{table}', NEW.a); "
                f"INSERT INTO {audit} VALUES ('{table}', NEW.a * 2); "
                f"END"
            )
            conn.execute(trigger_sql)
            conn.execute(
                f"INSERT INTO {table} VALUES (42, 'trigger_test')"
            )
            cur = conn.execute(f"SELECT * FROM {audit} ORDER BY rowid")
            return _query_result(cur)

        else:
            return {"success": False}

    except Exception:
        return {"success": False}


# ---------------------------------------------------------------------------
# Prefix helper — makes table names unique per test case so parallel tests
# don't interfere. Transforms "t_N" → "t_{prefix}_N" in table fields and SQL.
# ---------------------------------------------------------------------------

_TABLE_RE = re.compile(r"\bt_(\d+)\b")


def _apply_prefix(op, prefix):
    op = dict(op)
    if "table" in op:
        op["table"] = _TABLE_RE.sub(f"t_{prefix}_\\1", op["table"])
    if op.get("kind") in ("batch", "error_check"):
        op["sql"] = _TABLE_RE.sub(f"t_{prefix}_\\1", op["sql"])
    return op


# ---------------------------------------------------------------------------
# Composite strategy: op sequence with table column tracking
# ---------------------------------------------------------------------------


@st.composite
def op_sequence(draw):
    prefix = draw(st.integers(0, 65535))
    table_cols = {}
    count = draw(st.integers(1, 20))
    result = []
    for _ in range(count):
        op = draw(ops)
        op = _apply_prefix(op, prefix)
        # Track table schemas
        if op["kind"] == "create":
            table_cols[op["table"]] = 2
        elif op["kind"] == "create_dynamic":
            table_cols[op["table"]] = len(op["cols"])
        # Adjust insert value counts to match table column count
        elif op["kind"] in ("insert", "insert_returning", "insert_affected", "insert_rowid"):
            n = table_cols.get(op["table"], 2)
            op = dict(op)  # copy
            op["values"] = tuple(draw(values) for _ in range(n))
        result.append(op)
    return (prefix, result)


# ---------------------------------------------------------------------------
# The property test
# ---------------------------------------------------------------------------


@given(data=op_sequence())
@settings(
    max_examples=200,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
    database=None,
)
def test_api_parity(data):
    prefix, op_seq = data
    local = turso.connect(":memory:")
    kwargs = {}
    if AUTH_TOKEN:
        kwargs["auth_token"] = AUTH_TOKEN
    remote = remote_connect(SERVER_URL, **kwargs)

    try:
        # Drop any leftover tables for this prefix (from prior runs or replays).
        for i in range(_SPEC["constants"]["num_tables"]):
            try:
                remote.execute(f"DROP TABLE IF EXISTS t_{prefix}_{i}")
            except Exception:
                pass
            try:
                remote.execute(f"DROP TABLE IF EXISTS t_{prefix}_{i}_audit")
            except Exception:
                pass
            try:
                remote.execute(f"DROP TRIGGER IF EXISTS tr_t_{prefix}_{i}_ins")
            except Exception:
                pass

        # Accumulate an operation trace so failures show the full history.
        trace = []

        for i, op in enumerate(op_seq):
            local_result = execute_op(local, op)
            remote_result = execute_op(remote, op)

            trace.append(
                f"  op[{i}]: kind={op['kind']} table={op.get('table', '')!r}\n"
                f"    local:  ok={local_result.get('success')} rows={local_result.get('row_count')}\n"
                f"    remote: ok={remote_result.get('success')} rows={remote_result.get('row_count')}"
            )
            trace_dump = "\n".join(trace)

            # ErrorCheck only compares success/failure — error messages differ.
            if op["kind"] == "error_check":
                assert local_result.get("success") == remote_result.get("success"), (
                    f"Parity violation on op #{i} {op}:\n"
                    f"  local:  {local_result}\n"
                    f"  remote: {remote_result}\n\n"
                    f"Full trace (prefix={prefix}):\n{trace_dump}"
                )
                continue

            assert results_equal(local_result, remote_result), (
                f"Parity violation on op #{i} {op}:\n"
                f"  local:  {local_result}\n"
                f"  remote: {remote_result}\n\n"
                f"Full trace (prefix={prefix}):\n{trace_dump}"
            )

            # If both failed, stop — continuing with diverged implicit
            # transaction state leads to false positives.
            if not local_result.get("success"):
                break
    finally:
        local.close()
        remote.close()


# ---------------------------------------------------------------------------
# Error recovery property: errors must never prevent subsequent commands
# ---------------------------------------------------------------------------

ERROR_SQLS = st.sampled_from([
    "SELECT * FROM nonexistent_table_xyz",
    "SELECT * FROM nonexistent_table_abc",
    "SELECT * FROM nonexistent_table_zzz",
    "INSERT INTO nonexistent_table_xyz VALUES (1)",
    "INSERT INTO t_0 VALUES (1, 2, 3)",
    "SELECT length(1, 2, 3)",
])


@given(error_sql=ERROR_SQLS)
@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
    database=None,
)
def test_error_recovery(error_sql):
    kwargs = {}
    if AUTH_TOKEN:
        kwargs["auth_token"] = AUTH_TOKEN
    remote = remote_connect(SERVER_URL, **kwargs)

    try:
        # Send the error-inducing SQL (expected to fail)
        try:
            remote.execute(error_sql)
        except Exception:
            pass

        # The critical assertion: SELECT 1 must succeed afterward
        result = remote.execute("SELECT 1")
        rows = result.fetchall()
        assert len(rows) == 1, (
            f"SELECT 1 returned {len(rows)} rows after error SQL {error_sql!r}"
        )
        assert rows[0][0] == 1, (
            f"SELECT 1 returned {rows[0][0]!r} after error SQL {error_sql!r}"
        )
    finally:
        remote.close()


# ---------------------------------------------------------------------------
# DDL visibility in transactions: CREATE TABLE must be visible to subsequent
# statements within the same transaction.
# ---------------------------------------------------------------------------

DDL_PREFIXES = st.integers(min_value=200000, max_value=265535)
DDL_TABLE_INDICES = st.integers(min_value=0, max_value=5)
DDL_VALUES = st.integers(min_value=-1000, max_value=1000)


@given(prefix=DDL_PREFIXES, table_idx=DDL_TABLE_INDICES, val=DDL_VALUES)
@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
    database=None,
)
def test_ddl_in_transaction(prefix, table_idx, val):
    table = f"t_{prefix}_{table_idx}"
    local = turso.connect(":memory:")
    kwargs = {}
    if AUTH_TOKEN:
        kwargs["auth_token"] = AUTH_TOKEN
    remote = remote_connect(SERVER_URL, **kwargs)

    try:
        # Drop any leftover from prior runs so the INSERT produces exactly 1 row.
        try:
            remote.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass

        create_sql = f"CREATE TABLE IF NOT EXISTS {table} (a INTEGER, b TEXT)"
        insert_sql = f"INSERT INTO {table} VALUES (?, 'txn_ddl')"
        select_sql = f"SELECT a FROM {table}"

        # Local: BEGIN → CREATE → INSERT → SELECT → COMMIT
        local.execute("BEGIN")
        local.execute(create_sql)
        local.execute(insert_sql, [val])
        local_rows = local.execute(select_sql).fetchall()
        assert len(local_rows) == 1, (
            f"local: expected 1 row inside txn, got {len(local_rows)}"
        )
        local.execute("COMMIT")

        # Remote: BEGIN → CREATE → INSERT → SELECT → COMMIT
        remote.execute("BEGIN")
        remote.execute(create_sql)
        remote.execute(insert_sql, [val])
        remote_rows = remote.execute(select_sql).fetchall()
        assert len(remote_rows) == 1, (
            f"remote: expected 1 row inside txn, got {len(remote_rows)}"
        )
        remote.execute("COMMIT")
    finally:
        local.close()
        remote.close()


# Note: no ddl_prepare_in_transaction test for Python — DB-API2 has no
# separate prepare() step; cursor.execute() is the only path and doesn't
# call describe. The execute-based ddl_in_transaction test above covers
# the Python driver.


# ---------------------------------------------------------------------------
# API surface parity: local public members must exist on remote
# ---------------------------------------------------------------------------

# Members that only make sense on the local driver (FFI, async I/O hook).
_LOCAL_ONLY_CONN = {"extra_io"}
_LOCAL_ONLY_CURSOR = set()


def _public_members(obj):
    """Return set of public (non-underscore) member names."""
    return {m for m in dir(obj) if not m.startswith("_")}


def test_api_surface_parity():
    local = turso.connect(":memory:")
    kwargs = {}
    if AUTH_TOKEN:
        kwargs["auth_token"] = AUTH_TOKEN
    remote = remote_connect(SERVER_URL, **kwargs)

    try:
        # Connection
        local_conn_api = _public_members(local)
        remote_conn_api = _public_members(remote)
        missing_conn = sorted(local_conn_api - remote_conn_api - _LOCAL_ONLY_CONN)

        # Cursor
        local_cursor = local.execute("SELECT 1")
        remote_cursor = remote.execute("SELECT 1")
        local_cursor_api = _public_members(local_cursor)
        remote_cursor_api = _public_members(remote_cursor)
        missing_cursor = sorted(local_cursor_api - remote_cursor_api - _LOCAL_ONLY_CURSOR)

        errors = []
        if missing_conn:
            errors.append(f"Remote Connection missing: {missing_conn}")
        if missing_cursor:
            errors.append(f"Remote Cursor missing: {missing_cursor}")

        assert not errors, "\n".join(errors)
    finally:
        local.close()
        remote.close()
