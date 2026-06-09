//! Tests for Turso-specific statement result-column metadata APIs.
//!
//! `Statement::get_column_type_info` is one entry point for "what is the type
//! of this column?", covering both direct table-column references (declared
//! name, array depth, custom-type kind, resolved primitive) and computed
//! expressions (inferred affinity primitive). Gated behind the experimental
//! custom-types feature.

#[cfg(test)]
mod tests {
    use crate::common::TempDatabase;
    use tempfile::TempDir;
    use turso_core::{ColumnTypeInfo, ColumnTypeKind, Statement};

    /// Helper: open a fresh DB with custom types + STRICT support enabled.
    /// Array, struct, union and domain columns require STRICT mode and the
    /// `custom_types` opt, and `get_column_type_info` itself requires the
    /// same opt — every test that exercises the rich type-info path uses
    /// this constructor.
    fn fresh_db_with_custom_types(name: &str) -> TempDatabase {
        let path = TempDir::new().unwrap().keep().join(name);
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        TempDatabase::new_with_existent_with_opts(&path, opts)
    }

    /// Unwrap `Result<Option<ColumnTypeInfo>>` down to `ColumnTypeInfo`,
    /// panicking with a useful message at each layer. Used by the happy-path
    /// tests where neither the experimental-feature error nor the "no info"
    /// signal applies.
    fn expect_info(stmt: &Statement, idx: usize) -> ColumnTypeInfo {
        stmt.get_column_type_info(idx)
            .unwrap_or_else(|e| panic!("get_column_type_info({idx}) errored: {e}"))
            .unwrap_or_else(|| panic!("get_column_type_info({idx}) returned None"))
    }

    /// Built-in scalar columns: `array_dimensions == 0`, no `base_type`
    /// (declared name is itself the primitive), `kind == Builtin`.
    #[test]
    fn type_info_for_builtin_scalar_columns() {
        let db = fresh_db_with_custom_types("type_info_builtin_scalar.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT) STRICT")
            .unwrap();

        let stmt = conn.prepare("SELECT id, label FROM t").unwrap();

        let id_info = expect_info(&stmt, 0);
        assert_eq!(id_info.declared_name, "INTEGER");
        assert_eq!(id_info.array_dimensions, 0);
        assert_eq!(id_info.base_type, None);
        assert_eq!(id_info.kind, ColumnTypeKind::Builtin);

        let label_info = expect_info(&stmt, 1);
        assert_eq!(label_info.declared_name, "TEXT");
        assert_eq!(label_info.array_dimensions, 0);
        assert_eq!(label_info.base_type, None);
        assert_eq!(label_info.kind, ColumnTypeKind::Builtin);
    }

    /// Array columns report `array_dimensions` matching the bracket depth in
    /// CREATE TABLE. The declared name is still the element type (no
    /// brackets), preserving compatibility with how `get_column_decltype`
    /// already names the type.
    #[test]
    fn type_info_for_array_columns() {
        let db = fresh_db_with_custom_types("type_info_arrays.db");
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE t(\
                 arr_1d INTEGER[],\
                 arr_2d TEXT[][]\
             ) STRICT",
        )
        .unwrap();

        let stmt = conn.prepare("SELECT arr_1d, arr_2d FROM t").unwrap();

        let one_d = expect_info(&stmt, 0);
        assert_eq!(one_d.declared_name, "INTEGER");
        assert_eq!(one_d.array_dimensions, 1);
        assert_eq!(one_d.base_type, None);
        assert_eq!(one_d.kind, ColumnTypeKind::Builtin);

        let two_d = expect_info(&stmt, 1);
        assert_eq!(two_d.declared_name, "TEXT");
        assert_eq!(two_d.array_dimensions, 2);
        assert_eq!(two_d.base_type, None);
        assert_eq!(two_d.kind, ColumnTypeKind::Builtin);
    }

    /// A column whose declared type is a `CREATE TYPE ... BASE ...` reports
    /// the declared name verbatim, resolves `base_type` to the underlying
    /// primitive, and reports `kind == Custom`.
    #[test]
    fn type_info_for_custom_type_columns() {
        let db = fresh_db_with_custom_types("type_info_custom.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute("CREATE TABLE accounts(id INTEGER PRIMARY KEY, amount cents) STRICT")
            .unwrap();

        let stmt = conn.prepare("SELECT id, amount FROM accounts").unwrap();

        let id_info = expect_info(&stmt, 0);
        assert_eq!(id_info.declared_name, "INTEGER");
        assert_eq!(id_info.base_type, None);
        assert_eq!(id_info.kind, ColumnTypeKind::Builtin);

        let amount_info = expect_info(&stmt, 1);
        assert_eq!(amount_info.declared_name, "cents");
        assert_eq!(amount_info.array_dimensions, 0);
        assert_eq!(amount_info.base_type, Some("INTEGER".to_string()));
        assert_eq!(amount_info.kind, ColumnTypeKind::Custom);
    }

    /// `CREATE DOMAIN` columns share the same underlying primitive as their
    /// base type but report `kind == Domain` — letting consumers know there
    /// are CHECK constraints attached even though the on-disk representation
    /// is identical to the base.
    #[test]
    fn type_info_for_domain_columns() {
        let db = fresh_db_with_custom_types("type_info_domain.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score positive_int) STRICT")
            .unwrap();

        let stmt = conn.prepare("SELECT score FROM t").unwrap();
        let score_info = expect_info(&stmt, 0);
        assert_eq!(score_info.declared_name, "positive_int");
        assert_eq!(score_info.base_type, Some("INTEGER".to_string()));
        assert_eq!(score_info.kind, ColumnTypeKind::Domain);
    }

    /// `CREATE TYPE ... AS STRUCT(...)` columns are stored as BLOBs but
    /// report `kind == Struct` so wire-protocol layers can map them to
    /// composite / JSON types instead of raw BYTEA.
    #[test]
    fn type_info_for_struct_columns() {
        let db = fresh_db_with_custom_types("type_info_struct.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE point AS STRUCT(x INT, y INT)")
            .unwrap();
        conn.execute("CREATE TABLE t(id INT, pos point) STRICT")
            .unwrap();

        let stmt = conn.prepare("SELECT pos FROM t").unwrap();
        let pos_info = expect_info(&stmt, 0);
        assert_eq!(pos_info.declared_name, "point");
        assert_eq!(pos_info.array_dimensions, 0);
        // Structs flatten to BLOB on disk, but `kind` exposes the original
        // shape so callers can tell apart a struct column from a plain BLOB.
        assert_eq!(pos_info.base_type, Some("BLOB".to_string()));
        assert_eq!(pos_info.kind, ColumnTypeKind::Struct);
    }

    /// `CREATE TYPE ... AS UNION(...)` columns are stored as BLOBs but
    /// report `kind == Union`, distinguishing them from struct columns and
    /// plain BLOBs.
    #[test]
    fn type_info_for_union_columns() {
        let db = fresh_db_with_custom_types("type_info_union.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE shape AS UNION(circle INT, square INT)")
            .unwrap();
        conn.execute("CREATE TABLE t(id INT, s shape) STRICT")
            .unwrap();

        let stmt = conn.prepare("SELECT s FROM t").unwrap();
        let s_info = expect_info(&stmt, 0);
        assert_eq!(s_info.declared_name, "shape");
        assert_eq!(s_info.array_dimensions, 0);
        assert_eq!(s_info.base_type, Some("BLOB".to_string()));
        assert_eq!(s_info.kind, ColumnTypeKind::Union);
    }

    /// Computed expressions whose primitive type is statically inferable —
    /// `CAST(... AS type)` casts and `ROWID`-typed references — report that
    /// inferred primitive in `declared_name` with `kind == Builtin`, giving
    /// wire-protocol layers a usable type without a schema column behind it.
    #[test]
    fn type_info_for_typed_expressions() {
        let db = fresh_db_with_custom_types("type_info_typed_exprs.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY) STRICT")
            .unwrap();

        let stmt = conn
            .prepare(
                "SELECT \
                     CAST(? AS INTEGER), \
                     CAST(? AS TEXT), \
                     CAST(? AS REAL), \
                     rowid \
                 FROM t",
            )
            .unwrap();

        let cast_int = expect_info(&stmt, 0);
        assert_eq!(cast_int.declared_name, "INTEGER");
        assert_eq!(cast_int.kind, ColumnTypeKind::Builtin);
        assert_eq!(cast_int.base_type, None);
        assert_eq!(cast_int.array_dimensions, 0);

        let cast_text = expect_info(&stmt, 1);
        assert_eq!(cast_text.declared_name, "TEXT");
        assert_eq!(cast_text.kind, ColumnTypeKind::Builtin);

        let cast_real = expect_info(&stmt, 2);
        assert_eq!(cast_real.declared_name, "REAL");
        assert_eq!(cast_real.kind, ColumnTypeKind::Builtin);

        let rowid_col = expect_info(&stmt, 3);
        assert_eq!(rowid_col.declared_name, "INTEGER");
        assert_eq!(rowid_col.kind, ColumnTypeKind::Builtin);
    }

    /// Bare literals carry a concrete primitive that the wire-protocol layer
    /// must report (a PostgreSQL client running `SELECT 42` expects INT4,
    /// not TEXT). The API deliberately departs from SQLite's "literals have
    /// no affinity" rule here: SQLite's affinity machinery is about how
    /// columns coerce values at runtime, but a literal already IS a typed
    /// value — we can classify it directly from its parsed form.
    #[test]
    fn type_info_for_literal_expressions() {
        let db = fresh_db_with_custom_types("type_info_literals.db");
        let conn = db.connect_limbo();

        let stmt = conn.prepare("SELECT 42, 'literal', 3.14").unwrap();

        let int_lit = expect_info(&stmt, 0);
        assert_eq!(int_lit.declared_name, "INTEGER");
        assert_eq!(int_lit.kind, ColumnTypeKind::Builtin);
        assert_eq!(int_lit.base_type, None);
        assert_eq!(int_lit.array_dimensions, 0);

        let text_lit = expect_info(&stmt, 1);
        assert_eq!(text_lit.declared_name, "TEXT");
        assert_eq!(text_lit.kind, ColumnTypeKind::Builtin);

        let real_lit = expect_info(&stmt, 2);
        assert_eq!(real_lit.declared_name, "REAL");
        assert_eq!(real_lit.kind, ColumnTypeKind::Builtin);
    }

    /// Arithmetic over typed operands propagates: `42 + 1` reports INTEGER,
    /// matching what PostgreSQL itself reports. Mixed-precision arithmetic
    /// widens to REAL; mixing in a non-numeric column collapses to NUMERIC.
    /// This goes beyond SQLite's column-affinity machinery (which deliberately
    /// stops at binary operators) because wire-protocol clients need a usable
    /// type per result column.
    #[test]
    fn type_info_propagates_through_arithmetic() {
        let db = fresh_db_with_custom_types("type_info_arith.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY) STRICT")
            .unwrap();

        // Pure integer arithmetic → INTEGER (the PG-compatible answer).
        let int_arith = conn
            .prepare("SELECT 42 + 1, id - 1, 7 * 8, 10 % 3 FROM t")
            .unwrap();
        for idx in 0..4 {
            let info = expect_info(&int_arith, idx);
            assert_eq!(
                info.declared_name, "INTEGER",
                "column {idx} should be INTEGER, got {:?}",
                info.declared_name
            );
            assert_eq!(info.kind, ColumnTypeKind::Builtin);
        }

        // Mixed INTEGER + REAL → REAL.
        let mixed = conn.prepare("SELECT 42 + 1.5, 3.14 * id FROM t").unwrap();
        assert_eq!(expect_info(&mixed, 0).declared_name, "REAL");
        assert_eq!(expect_info(&mixed, 1).declared_name, "REAL");

        // Bitwise ops always return INTEGER.
        let bitwise = conn.prepare("SELECT 1 << 4, 0xFF & 0x0F, 1 | 2").unwrap();
        for idx in 0..3 {
            assert_eq!(expect_info(&bitwise, idx).declared_name, "INTEGER");
        }

        // Comparison and logical ops return INTEGER (SQLite's 0/1).
        let bools = conn.prepare("SELECT 1 = 1, 2 < 3, 1 AND 0, NOT 1").unwrap();
        for idx in 0..4 {
            assert_eq!(expect_info(&bools, idx).declared_name, "INTEGER");
        }

        // Concat is always TEXT.
        let concat = conn.prepare("SELECT 'hello' || ' world'").unwrap();
        assert_eq!(expect_info(&concat, 0).declared_name, "TEXT");
    }

    /// Expressions whose primitive can't be determined statically — function
    /// calls without a declared return affinity, BLOB literals, NULL literals
    /// — yield `Ok(None)`. Callers fall through to their own default (TEXT
    /// is the typical wire-protocol choice).
    #[test]
    fn type_info_none_for_indeterminate_expressions() {
        let db = fresh_db_with_custom_types("type_info_indet.db");
        let conn = db.connect_limbo();

        // `randomblob(N)` returns a BLOB — no useful wire primitive to
        // report. Use any function call with no declared return affinity.
        let blob = conn.prepare("SELECT randomblob(8)").unwrap();
        assert!(blob.get_column_type_info(0).unwrap().is_none());

        // Blob literals carry a concrete value type but no useful primitive
        // to expose in wire-protocol terms (clients consuming BYTEA already
        // get an exact byte stream; there's nothing to "report").
        let blob_lit = conn.prepare("SELECT x'AB'").unwrap();
        assert!(blob_lit.get_column_type_info(0).unwrap().is_none());

        // NULL literal: type is genuinely indeterminate at compile time.
        let null_lit = conn.prepare("SELECT NULL").unwrap();
        assert!(null_lit.get_column_type_info(0).unwrap().is_none());
    }

    /// `get_column_type_info` is the public surface of the custom-types
    /// system, so it errors when the connection does not have the
    /// `--experimental-custom-types` flag enabled. Callers that want plain
    /// type metadata regardless of feature flags can use
    /// `get_column_decltype` (mirrors SQLite's stable contract).
    #[test]
    fn type_info_errors_without_experimental_custom_types() {
        let db = TempDatabase::new_empty(); // custom_types off by default
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER)").unwrap();

        let stmt = conn.prepare("SELECT id FROM t").unwrap();
        let err = stmt
            .get_column_type_info(0)
            .expect_err("expected experimental-feature error");
        let msg = err.to_string();
        assert!(
            msg.contains("experimental-custom-types"),
            "unexpected error message: {msg}"
        );
    }

    /// Array depth survives `SELECT *` expansion — the planner preserves the
    /// table-column refs even when the projection is implicit.
    #[test]
    fn type_info_survives_star_expansion() {
        let db = fresh_db_with_custom_types("type_info_star.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a INTEGER[], b INTEGER) STRICT")
            .unwrap();

        let stmt = conn.prepare("SELECT * FROM t").unwrap();

        let a_info = expect_info(&stmt, 0);
        assert_eq!(a_info.declared_name, "INTEGER");
        assert_eq!(a_info.array_dimensions, 1);
        assert_eq!(a_info.kind, ColumnTypeKind::Builtin);

        let b_info = expect_info(&stmt, 1);
        assert_eq!(b_info.declared_name, "INTEGER");
        assert_eq!(b_info.array_dimensions, 0);
        assert_eq!(b_info.kind, ColumnTypeKind::Builtin);
    }
}
