//! Tests for Turso-specific statement result-column metadata APIs and
//! function-name aliases.
//!
//! Two pieces of public surface bundled because they share the same testing
//! pattern: prepare a statement, inspect column metadata or invoke aliased
//! functions, assert the result.
//!
//! - `Statement::get_column_type_info`: one entry point for "what is the type
//!   of this column?", covering both direct table-column references (declared
//!   name, array depth, custom-type kind, resolved primitive) and computed
//!   expressions (inferred affinity primitive). Gated behind the experimental
//!   custom-types feature.
//! - `Statement::get_column_origin`: the table column a result column reads,
//!   as SQLite's `sqlite3_column_origin_name()` and its siblings give it.
//! - Function-name aliases: `chr` resolves to the same scalar function as
//!   `char`, and `strpos` resolves to the same scalar function as `instr`.

#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use std::sync::Arc;
    use turso_core::{ast, ColumnOrigin, ColumnTypeInfo, ColumnTypeKind, Dialect, Statement};
    use turso_sqlite::SqliteDialect;

    /// Helper: open a fresh DB with custom types + STRICT support enabled.
    /// Array, struct, union and domain columns require STRICT mode and the
    /// `custom_types` opt, and `get_column_type_info` itself requires the
    /// same opt — every test that exercises the rich type-info path uses
    /// this constructor.
    fn fresh_db_with_custom_types(name: &str) -> TempDatabase {
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        TempDatabase::builder()
            .with_db_name(name)
            .with_opts(opts)
            .build()
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

    /// A dialect with identifiers for its types: 1 for INTEGER, 2 for TEXT and
    /// 9 for the literals `true` and `false`. It parses as SQLite does.
    struct AnnotatingDialect;

    const INTEGER_ID: u32 = 1;
    const TEXT_ID: u32 = 2;
    const BOOLEAN_ID: u32 = 9;

    impl Dialect for AnnotatingDialect {
        fn name(&self) -> &'static str {
            SqliteDialect.name()
        }
        fn parse(&self, sql: &str) -> turso_core::Result<(Option<ast::Cmd>, usize)> {
            SqliteDialect.parse(sql)
        }
        fn parse_internal(&self, sql: &str) -> turso_core::Result<(Option<ast::Cmd>, usize)> {
            SqliteDialect.parse_internal(sql)
        }
        fn parse_internal_column_definition(
            &self,
            sql: &str,
        ) -> turso_core::Result<ast::ColumnDefinition> {
            SqliteDialect.parse_internal_column_definition(sql)
        }
        fn parameter_marker_ranges(
            &self,
            sql: &str,
        ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
            SqliteDialect.parameter_marker_ranges(sql)
        }
        fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(SqliteDialect)
        }
        fn type_annotation(&self, ty: &ast::Type) -> Option<ast::TypeAnnotation> {
            match ty.name.to_ascii_uppercase().as_str() {
                "INTEGER" => Some(ast::TypeAnnotation::new(INTEGER_ID)),
                "TEXT" => Some(ast::TypeAnnotation::new(TEXT_ID)),
                _ => None,
            }
        }
        fn literal_type_annotation(&self, literal: &ast::Literal) -> Option<ast::TypeAnnotation> {
            match literal {
                ast::Literal::True | ast::Literal::False => {
                    Some(ast::TypeAnnotation::new(BOOLEAN_ID))
                }
                _ => None,
            }
        }
    }

    fn annotating_db(name: &str) -> TempDatabase {
        let db = TempDatabase::builder()
            .with_db_name(name)
            .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
            .with_dialect(Arc::new(AnnotatingDialect))
            .build();
        db.connect_limbo()
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT) STRICT")
            .unwrap();
        db
    }

    /// The identifier that the dialect has for the type of each result column.
    fn type_ids(stmt: &Statement) -> Vec<Option<u32>> {
        (0..stmt.num_columns())
            .map(|idx| {
                stmt.get_column_type_info(idx)
                    .unwrap()
                    .and_then(|info| info.ty_annotation)
                    .map(|annotation| annotation.type_id)
            })
            .collect()
    }

    /// The SELECT statement `sql`, with `annotate` applied to its first result
    /// column.
    fn select_with_first_column(
        sql: &str,
        annotate: impl FnOnce(ast::Expr) -> ast::Expr,
    ) -> ast::Stmt {
        let Some(ast::Cmd::Stmt(ast::Stmt::Select(mut select))) =
            turso_core::dialect::parse_statement(&SqliteDialect, sql).unwrap()
        else {
            panic!("not a SELECT: {sql}");
        };
        let ast::OneSelect::Select { columns, .. } = &mut select.body.select else {
            panic!("not a SELECT: {sql}");
        };
        let ast::ResultColumn::Expr(expr, _) = &mut columns[0] else {
            panic!("not an expression: {sql}");
        };
        **expr = annotate(std::mem::take(expr.as_mut()));
        ast::Stmt::Select(select)
    }

    /// `expr` with `type_id` stored on the type of its casts: of `expr` when
    /// it is a cast, and of the arguments that are casts when it is a call to
    /// a function.
    fn with_annotated_casts(expr: ast::Expr, type_id: u32) -> ast::Expr {
        match expr {
            ast::Expr::Cast {
                expr,
                type_name: Some(mut ty),
            } => {
                ty.ty_annotation = Some(ast::TypeAnnotation::new(type_id));
                ast::Expr::Cast {
                    expr,
                    type_name: Some(ty),
                }
            }
            ast::Expr::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                within_group,
                filter_over,
            } => ast::Expr::FunctionCall {
                name,
                distinctness,
                args: args
                    .into_iter()
                    .map(|arg| Box::new(with_annotated_casts(*arg, type_id)))
                    .collect(),
                order_by,
                within_group,
                filter_over,
            },
            expr => expr,
        }
    }

    /// SQLite has no identifiers for its types, so neither a column nor a
    /// cast reports an annotation.
    #[test]
    fn type_info_has_no_annotation_in_sqlite() {
        let db = fresh_db_with_custom_types("type_info_annotation.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT) STRICT")
            .unwrap();

        let stmt = conn
            .prepare("SELECT id, CAST(label AS INTEGER), coalesce(id, 1), true FROM t")
            .unwrap();

        assert_eq!(type_ids(&stmt), [None, None, None, None]);
    }

    /// Where the frontend stored no annotation, an expression has the one
    /// derived from it: the one the dialect gives for the declared type
    /// of a column, for the type of a cast and for a literal, and the one that
    /// the values of `coalesce`, `nullif`, `iif` and CASE all have.
    #[test]
    fn type_annotation_is_derived_for_an_expression_without_one() {
        let db = annotating_db("type_annotation_derived.db");
        let conn = db.connect_limbo();
        for (expr, expected) in [
            ("id", Some(INTEGER_ID)),
            ("label", Some(TEXT_ID)),
            ("(id)", Some(INTEGER_ID)),
            ("CAST(label AS INTEGER)", Some(INTEGER_ID)),
            ("true", Some(BOOLEAN_ID)),
            ("coalesce(id, id)", Some(INTEGER_ID)),
            ("coalesce(true, false)", Some(BOOLEAN_ID)),
            ("coalesce(CAST(label AS INTEGER), id)", Some(INTEGER_ID)),
            ("nullif(id, 0)", Some(INTEGER_ID)),
            ("iif(id > 0, label, label)", Some(TEXT_ID)),
            ("CASE WHEN id > 0 THEN id ELSE id END", Some(INTEGER_ID)),
            // NULL and a string literal take the type of the other values.
            ("coalesce(id, NULL)", Some(INTEGER_ID)),
            ("CASE WHEN id > 0 THEN label ELSE 'none' END", Some(TEXT_ID)),
            // Values of different types have no common annotation.
            ("coalesce(id, label)", None),
            // Nothing tells the type of an operator's result.
            ("id + 1", None),
            ("1", None),
        ] {
            let stmt = conn.prepare(format!("SELECT {expr} FROM t")).unwrap();
            assert_eq!(type_ids(&stmt), [expected], "{expr}");
        }
    }

    /// The annotation that the frontend stored on the type of a cast is the
    /// answer: nothing is derived for it. The annotation does not change
    /// the value.
    #[test]
    fn type_annotation_of_the_frontend_is_used_before_a_derived_one() {
        const FRONTEND_ID: u32 = 42;
        let db = annotating_db("type_annotation_frontend.db");
        let conn = db.connect_limbo();
        conn.execute("INSERT INTO t VALUES (7, '7')").unwrap();

        // A cast to an annotated type, alone and as the value of a function.
        // Without the annotation the dialect gives INTEGER_ID for the cast.
        for sql in [
            "SELECT CAST(label AS INTEGER) FROM t",
            "SELECT coalesce(CAST(label AS INTEGER), NULL) FROM t",
        ] {
            let stmt =
                select_with_first_column(sql, |expr| with_annotated_casts(expr, FRONTEND_ID));
            let mut stmt = conn.prepare_ast_stmt(stmt).unwrap();
            assert_eq!(type_ids(&stmt), [Some(FRONTEND_ID)], "{sql}");
            assert_eq!(expect_info(&stmt, 0).declared_name, "INTEGER", "{sql}");
            let rows = stmt.run_collect_rows().unwrap();
            assert_eq!(rows, [[turso_core::Value::from_i64(7)]], "{sql}");
        }
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

    /// Each unary operator, checked against what SQLite's `typeof()` actually
    /// reports. The three behaviours are distinct and none of them is "return
    /// the operand's type unchanged":
    ///
    /// * `NOT` / `~` are INTEGER for every operand — `OP_Not` and `OP_BitNot`
    ///   unconditionally write an integer register.
    /// * `-` is always numeric: SQLite compiles it to `0 - x` (`OP_Subtract`),
    ///   so a TEXT operand comes back numeric, not TEXT. INTEGER and REAL
    ///   operands keep their type; anything else is numeric but not statically
    ///   resolvable to one or the other (`typeof(-'abc')` is `integer` while
    ///   `typeof(-'1.5')` is `real`), so NUMERIC is the honest answer.
    /// * `+` alone is a true no-op — `typeof(+'abc')` is `text`.
    #[test]
    fn type_info_for_unary_operators() {
        let db = fresh_db_with_custom_types("type_info_unary.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, ratio REAL) STRICT")
            .unwrap();

        // Every assertion below is `(expression, expected primitive)` with the
        // SQLite `typeof()` it mirrors noted alongside.
        let cases: &[(&str, &str)] = &[
            // NOT is INTEGER regardless of operand type.
            ("NOT ('a' || 'b')", "INTEGER"), // typeof -> integer
            ("NOT 1.5", "INTEGER"),          // typeof -> integer
            ("NOT id", "INTEGER"),           // typeof -> integer
            ("NOT label", "INTEGER"),        // typeof -> integer
            // Bitwise NOT is INTEGER regardless of operand type.
            ("~1.5", "INTEGER"),          // typeof -> integer
            ("~label", "INTEGER"),        // typeof -> integer
            ("~('a' || 'b')", "INTEGER"), // typeof -> integer
            // Unary minus is numeric, never the operand's non-numeric type.
            ("-42", "INTEGER"),           // typeof -> integer
            ("-1.5", "REAL"),             // typeof -> real
            ("-id", "INTEGER"),           // typeof -> integer
            ("-ratio", "REAL"),           // typeof -> real
            ("-label", "NUMERIC"),        // typeof -> integer or real, by value
            ("-('a' || 'b')", "NUMERIC"), // typeof -> integer
            // Unary plus preserves the operand exactly.
            ("+id", "INTEGER"),        // typeof -> integer
            ("+1.5", "REAL"),          // typeof -> real
            ("+label", "TEXT"),        // typeof -> text
            ("+('a' || 'b')", "TEXT"), // typeof -> text
            // Nesting composes the rules above.
            ("- -1.5", "REAL"),           // typeof -> real
            ("NOT NOT label", "INTEGER"), // typeof -> integer
            ("-(~1.5)", "INTEGER"),       // typeof -> integer
        ];

        // Collect every mismatch rather than tripping on the first, so a
        // regression reports the full picture in one run.
        let mut mismatches = Vec::new();
        for (expr, expected) in cases {
            let stmt = conn
                .prepare(format!("SELECT {expr} FROM t"))
                .unwrap_or_else(|e| panic!("failed to prepare `SELECT {expr} FROM t`: {e}"));
            let info = expect_info(&stmt, 0);
            if info.declared_name != *expected {
                mismatches.push(format!(
                    "  {expr:<16} expected {expected}, got {}",
                    info.declared_name
                ));
            }
            assert_eq!(info.kind, ColumnTypeKind::Builtin, "`{expr}` kind");
        }
        assert!(
            mismatches.is_empty(),
            "unary operator inference diverges from SQLite:\n{}",
            mismatches.join("\n")
        );
    }

    /// CASE takes the common type of its branches, as do COALESCE, IFNULL and
    /// the multi-argument MIN/MAX. NULLIF takes the type of its first argument
    /// and ABS follows unary minus. NULL branches do not affect the result, and
    /// branches with no common type give no result type.
    #[test]
    fn type_info_for_case_and_functions_that_return_their_argument_type() {
        let db = fresh_db_with_custom_types("type_info_case_functions.db");
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, label TEXT, ratio REAL) STRICT",
        )
        .unwrap();

        let cases: &[(&str, Option<&str>)] = &[
            ("coalesce(x, 2)", Some("INTEGER")),
            ("coalesce(x, 2) * coalesce(x, 3)", Some("INTEGER")),
            ("ifnull(x, NULL)", Some("INTEGER")),
            ("coalesce(x, ratio)", Some("REAL")),
            ("coalesce(label, 'none')", Some("TEXT")),
            ("coalesce(x, label)", None),
            ("max(x, 1, 2)", Some("INTEGER")),
            ("min(x, 1.5)", Some("REAL")),
            ("nullif(x, 0)", Some("INTEGER")),
            ("abs(x)", Some("INTEGER")),
            ("abs(x) * 2", Some("INTEGER")),
            ("abs(ratio)", Some("REAL")),
            ("abs(label)", Some("REAL")),
            ("CASE WHEN x = 1 THEN x ELSE 0 END - 1", Some("INTEGER")),
            ("CASE WHEN x = 1 THEN x END", Some("INTEGER")),
            ("CASE x WHEN 1 THEN NULL ELSE ratio END", Some("REAL")),
            ("CASE WHEN x = 1 THEN label ELSE 'other' END", Some("TEXT")),
            ("CASE WHEN x = 1 THEN label ELSE 0 END", None),
            ("CASE WHEN x = 1 THEN NULL END", None),
            ("coalesce(x, randomblob(8))", None),
            ("iif(x = 1, x, 2)", Some("INTEGER")),
            ("iif(x = 1, label, 'other')", Some("TEXT")),
        ];

        let mut mismatches = Vec::new();
        for (expr, expected) in cases {
            let stmt = conn
                .prepare(format!("SELECT {expr} FROM t"))
                .unwrap_or_else(|e| panic!("failed to prepare `SELECT {expr} FROM t`: {e}"));
            let actual = stmt
                .get_column_type_info(0)
                .unwrap()
                .map(|info| info.declared_name);
            if actual.as_deref() != *expected {
                mismatches.push(format!(
                    "  {expr:<44} expected {expected:?}, got {actual:?}"
                ));
            }
        }
        assert!(
            mismatches.is_empty(),
            "CASE and function result types are wrong:\n{}",
            mismatches.join("\n")
        );
    }

    /// Aggregates that return one of their input values have that value's type.
    #[test]
    fn type_info_for_aggregates_that_return_an_input_value() {
        let db = fresh_db_with_custom_types("type_info_aggregates.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, ratio REAL) STRICT")
            .unwrap();

        let stmt = conn
            .prepare("SELECT max(x), min(ratio), array_agg(x) FROM t")
            .unwrap();
        assert_eq!(expect_info(&stmt, 0).declared_name, "INTEGER");
        assert_eq!(expect_info(&stmt, 1).declared_name, "REAL");
        let array_agg = expect_info(&stmt, 2);
        assert_eq!(array_agg.declared_name, "INTEGER");
        assert_eq!(array_agg.array_dimensions, 1);
    }

    /// An expression of a custom type that isn't a column reports itself like a
    /// column of that type: the type's name, with the primitive it is stored as
    /// in `base_type` (not one guessed from the type's name).
    #[test]
    fn type_info_for_custom_type_expressions() {
        let db = fresh_db_with_custom_types("type_info_custom_expressions.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute("CREATE TYPE rev BASE text ENCODE value DECODE value")
            .unwrap();
        conn.execute(
            "CREATE TABLE accounts(id INTEGER PRIMARY KEY, amount cents, code rev) STRICT",
        )
        .unwrap();

        let stmt = conn
            .prepare(
                "SELECT CAST(id AS cents), CAST(id AS rev), coalesce(code, code), \
                 coalesce(amount, CAST(0 AS cents)), CAST(id AS cents[]) FROM accounts",
            )
            .unwrap();
        let column_of_cents = conn.prepare("SELECT amount FROM accounts").unwrap();
        let expected_cents = expect_info(&column_of_cents, 0);

        let cast_to_cents = expect_info(&stmt, 0);
        assert_eq!(cast_to_cents, expected_cents);
        assert_eq!(cast_to_cents.declared_name, "cents");
        assert_eq!(cast_to_cents.base_type.as_deref(), Some("INTEGER"));
        assert_eq!(cast_to_cents.kind, ColumnTypeKind::Custom);

        let cast_to_rev = expect_info(&stmt, 1);
        assert_eq!(cast_to_rev.declared_name, "rev");
        assert_eq!(cast_to_rev.base_type.as_deref(), Some("TEXT"));
        assert_eq!(cast_to_rev.kind, ColumnTypeKind::Custom);

        let coalesced = expect_info(&stmt, 2);
        assert_eq!(coalesced.declared_name, "rev");
        assert_eq!(coalesced.base_type.as_deref(), Some("TEXT"));

        assert_eq!(expect_info(&stmt, 3), expected_cents);

        let cents_array = expect_info(&stmt, 4);
        assert_eq!(cents_array.declared_name, "cents");
        assert_eq!(cents_array.array_dimensions, 1);
    }

    /// A custom type with no function for an operator goes to SQLite's operator as
    /// the type it is stored as, so `si * 2` on a smallint is an integer. A value
    /// that comes from two different custom types, as in `coalesce(si, bi)`, is a
    /// value of their common stored type.
    #[test]
    fn type_info_for_sqlite_operators_on_custom_types() {
        let db = fresh_db_with_custom_types("type_info_sqlite_operators.db");
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, si smallint, bi bigint, b boolean) STRICT",
        )
        .unwrap();

        let stmt = conn
            .prepare(
                "SELECT si * 2, bi + 1, -si, abs(bi), coalesce(si, 0), coalesce(b, 0), \
                 coalesce(si, bi), CASE WHEN id = 1 THEN si ELSE bi END, max(si, bi) FROM t",
            )
            .unwrap();
        for idx in 0..9 {
            assert_eq!(
                expect_info(&stmt, idx).declared_name,
                "INTEGER",
                "column {idx}"
            );
        }
    }

    /// Date arithmetic declares its result: `date + n` is a date, which is stored
    /// as text (it reports as a date), and `date - date` is a number of days.
    #[test]
    fn type_info_for_date_arithmetic() {
        let db = fresh_db_with_custom_types("type_info_date_arithmetic.db");
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, d date) STRICT")
            .unwrap();

        let stmt = conn.prepare("SELECT d + 1, d - d FROM t").unwrap();
        let date_plus_days = expect_info(&stmt, 0);
        assert_eq!(date_plus_days.declared_name, "date");
        assert_eq!(date_plus_days.base_type.as_deref(), Some("TEXT"));
        assert_eq!(expect_info(&stmt, 1).declared_name, "INTEGER");
    }

    /// An expression that always gives a blob reports BLOB.
    #[test]
    fn type_info_blob_for_blob_expressions() {
        let db = fresh_db_with_custom_types("type_info_blob.db");
        let conn = db.connect_limbo();
        for sql in ["SELECT randomblob(8)", "SELECT x'AB'"] {
            let stmt = conn.prepare(sql).unwrap();
            assert_eq!(expect_info(&stmt, 0).declared_name, "BLOB", "{sql}");
        }
    }

    /// A NULL literal's type can't be determined statically, so it yields
    /// `Ok(None)`. Callers fall through to their own default (TEXT is the
    /// typical wire-protocol choice).
    #[test]
    fn type_info_none_for_indeterminate_expressions() {
        let db = fresh_db_with_custom_types("type_info_indet.db");
        let conn = db.connect_limbo();

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

    /// `chr(n)` is the SQL standard / PostgreSQL spelling of SQLite's
    /// `char(n)`. Both must produce identical results.
    #[test]
    fn chr_alias_matches_char() {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        let via_char: Vec<(String,)> = conn.exec_rows("SELECT char(65, 66, 67)");
        let via_chr: Vec<(String,)> = conn.exec_rows("SELECT chr(65, 66, 67)");

        assert_eq!(via_char, vec![("ABC".to_string(),)]);
        assert_eq!(via_chr, vec![("ABC".to_string(),)]);
    }

    /// `strpos(haystack, needle)` is the PostgreSQL spelling of SQLite's
    /// `instr(haystack, needle)`. Argument order is identical, so the alias
    /// is unambiguous.
    #[test]
    fn strpos_alias_matches_instr() {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        let via_instr: Vec<(i64,)> = conn.exec_rows("SELECT instr('hello world', 'world')");
        let via_strpos: Vec<(i64,)> = conn.exec_rows("SELECT strpos('hello world', 'world')");
        assert_eq!(via_instr, vec![(7,)]);
        assert_eq!(via_strpos, vec![(7,)]);

        // Needle not found → 0 from both spellings.
        let miss_instr: Vec<(i64,)> = conn.exec_rows("SELECT instr('hello', 'xyz')");
        let miss_strpos: Vec<(i64,)> = conn.exec_rows("SELECT strpos('hello', 'xyz')");
        assert_eq!(miss_instr, vec![(0,)]);
        assert_eq!(miss_strpos, vec![(0,)]);
    }

    /// `btrim(X[, chars])` is the PG/Oracle spelling of SQLite's
    /// `trim(X[, chars])`. Both trim the listed characters (or whitespace by
    /// default) from both sides of the string with identical semantics.
    #[test]
    fn btrim_alias_matches_trim() {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        // Default: trim whitespace.
        let trim_ws: Vec<(String,)> = conn.exec_rows("SELECT trim('  hello  ')");
        let btrim_ws: Vec<(String,)> = conn.exec_rows("SELECT btrim('  hello  ')");
        assert_eq!(trim_ws, vec![("hello".to_string(),)]);
        assert_eq!(btrim_ws, vec![("hello".to_string(),)]);

        // Explicit character set.
        let trim_x: Vec<(String,)> = conn.exec_rows("SELECT trim('xxhelloxx', 'x')");
        let btrim_x: Vec<(String,)> = conn.exec_rows("SELECT btrim('xxhelloxx', 'x')");
        assert_eq!(trim_x, vec![("hello".to_string(),)]);
        assert_eq!(btrim_x, vec![("hello".to_string(),)]);
    }

    /// `char_length(X)` and `character_length(X)` are the SQL standard
    /// spellings of SQLite's `length(X)`. All three return the character
    /// count for TEXT input.
    #[test]
    fn char_length_aliases_match_length() {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        let via_length: Vec<(i64,)> = conn.exec_rows("SELECT length('hello world')");
        let via_char_length: Vec<(i64,)> = conn.exec_rows("SELECT char_length('hello world')");
        let via_character_length: Vec<(i64,)> =
            conn.exec_rows("SELECT character_length('hello world')");

        assert_eq!(via_length, vec![(11,)]);
        assert_eq!(via_char_length, vec![(11,)]);
        assert_eq!(via_character_length, vec![(11,)]);

        // Multi-byte input: SQLite reports character count, not byte count.
        let via_length_unicode: Vec<(i64,)> = conn.exec_rows("SELECT length('héllo')");
        let via_char_length_unicode: Vec<(i64,)> = conn.exec_rows("SELECT char_length('héllo')");
        assert_eq!(via_length_unicode, vec![(5,)]);
        assert_eq!(via_char_length_unicode, vec![(5,)]);
    }

    /// `reverse(X)` is the spelling PG, MySQL, and Oracle all use; upstream
    /// Turso registered the function under the more explicit `string_reverse`
    /// name. Both must produce identical results.
    #[test]
    fn reverse_alias_matches_string_reverse() {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        let via_string_reverse: Vec<(String,)> = conn.exec_rows("SELECT string_reverse('hello')");
        let via_reverse: Vec<(String,)> = conn.exec_rows("SELECT reverse('hello')");
        assert_eq!(via_string_reverse, vec![("olleh".to_string(),)]);
        assert_eq!(via_reverse, vec![("olleh".to_string(),)]);
    }

    /// `CREATE TABLE ... AS SELECT` returns no rows, so the prepared
    /// statement must report zero result columns (sqlite3_column_count is 0
    /// for CTAS). The source SELECT feeds the insert coroutine internally
    /// and must not leak its columns into statement metadata.
    #[test]
    fn ctas_statement_reports_zero_columns() {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE src(a INTEGER, b TEXT)").unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'x'), (2, 'y')")
            .unwrap();

        let mut stmt = conn
            .prepare("CREATE TABLE dst AS SELECT a, b FROM src")
            .unwrap();
        assert_eq!(
            stmt.num_columns(),
            0,
            "CTAS must report zero result columns"
        );

        stmt.run_ignore_rows().unwrap();
        let copied: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM dst ORDER BY a");
        assert_eq!(copied, vec![(1, "x".to_string()), (2, "y".to_string())]);
    }

    fn origins(stmt: &Statement) -> Vec<Option<ColumnOrigin>> {
        (0..stmt.num_columns())
            .map(|i| stmt.get_column_origin(i))
            .collect()
    }

    fn origin(database: &str, table: &str, column: &str, ty: Option<&str>) -> Option<ColumnOrigin> {
        Some(ColumnOrigin {
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            declared_type: ty.map(str::to_string),
        })
    }

    /// The origins SQLite gives for the same statements.
    #[test]
    fn column_origin_follows_subqueries_views_and_ctes() {
        let db = TempDatabase::builder()
            .with_opts(
                turso_core::DatabaseOpts::new()
                    .with_attach(true)
                    .with_views(true),
            )
            .build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(40), note)")
            .unwrap();
        conn.execute("CREATE TABLE posts (title text)").unwrap();
        conn.execute("CREATE VIEW v AS SELECT name AS n, id * 2 AS dbl FROM users")
            .unwrap();
        conn.execute("ATTACH ':memory:' AS aux").unwrap();
        conn.execute("CREATE TABLE aux.t (a DECIMAL(10,2))")
            .unwrap();
        conn.execute("CREATE TEMP TABLE tt (z INT)").unwrap();

        let users_name = origin("main", "users", "name", Some("VARCHAR(40)"));
        let cases = [
            (
                "SELECT id, name AS display, note, id + 1 FROM users",
                vec![
                    origin("main", "users", "id", Some("INTEGER")),
                    users_name.clone(),
                    origin("main", "users", "note", None),
                    None,
                ],
            ),
            (
                "SELECT x FROM (SELECT name AS x FROM users)",
                vec![users_name.clone()],
            ),
            ("SELECT (name) FROM users", vec![users_name.clone()]),
            ("SELECT n, dbl FROM v", vec![users_name.clone(), None]),
            (
                "WITH c AS (SELECT name AS n FROM users) SELECT n FROM c",
                vec![users_name.clone()],
            ),
            (
                "SELECT name FROM users UNION SELECT title FROM posts",
                vec![users_name.clone()],
            ),
            (
                "SELECT x FROM (SELECT name AS x FROM users UNION SELECT title FROM posts)",
                vec![origin("main", "posts", "title", Some("TEXT"))],
            ),
            (
                "SELECT a FROM aux.t",
                vec![origin("aux", "t", "a", Some("DECIMAL(10,2)"))],
            ),
            (
                "SELECT rowid FROM posts",
                vec![origin("main", "posts", "rowid", Some("INTEGER"))],
            ),
            (
                "SELECT z FROM tt",
                vec![origin("temp", "tt", "z", Some("INT"))],
            ),
            (
                "SELECT value FROM json_each('[1]')",
                vec![origin("main", "json_each", "value", None)],
            ),
            ("EXPLAIN SELECT name FROM users", vec![None; 8]),
        ];
        for (sql, expected) in cases {
            let stmt = conn.prepare(sql).unwrap();
            assert_eq!(origins(&stmt), expected, "{sql}");
            for (i, origin) in expected.iter().enumerate() {
                assert_eq!(
                    stmt.get_column_table_name(i).as_deref(),
                    origin.as_ref().map(|o| o.table.as_str()),
                    "{sql}"
                );
                if origin.is_some() {
                    assert_eq!(
                        stmt.get_column_decltype(i),
                        origin.as_ref().and_then(|o| o.declared_type.clone()),
                        "{sql}"
                    );
                }
            }
        }
    }
}
