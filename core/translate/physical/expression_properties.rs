//! Properties for direct, catalog-free HIR expression lowering.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Schema, Type},
    sync::Arc,
    translate::{
        collate::CollationSeq,
        semantic::{
            analyze,
            context::SemanticContext,
            hir::{
                BoundCastPrograms, BoundSchemaCall, BoundSchemaProgram, CatalogSnapshot,
                ColumnReadExpression, Expr, From, HirDocument, HirRoot, IndexCoverage, IndexHint,
                Output, OutputId, OutputNameKind, Query, QueryBlock, QueryBlockBody, QueryBlockId,
                QueryId, QueryRoot, SchemaProgramId, Source, SourceColumn, SourceId, SourceKind,
                SourceOwner, TypeFact, TypeName,
            },
            AnalyzeInput,
        },
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder, ProgramBuilderOpts},
        insn::Insn,
    },
    QueryMode, SymbolTable,
};

fn parse_statement(sql: &str) -> ast::Stmt {
    let command = Parser::new(sql.as_bytes())
        .next_cmd()
        .expect("generated SQL parses")
        .expect("generated SQL contains a statement");
    let ast::Cmd::Stmt(statement) = command else {
        panic!("generated SQL contains a statement");
    };
    statement
}

fn program() -> ProgramBuilder {
    ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 8))
}

fn source_document(width: usize, expression: Expr) -> (HirDocument, QueryId, SourceId) {
    let query = QueryId::new(0);
    let block = QueryBlockId::new(query, 0);
    let source = SourceId::new(0);
    let output = OutputId::query(block, 0);
    let columns = (0..width)
        .map(|position| SourceColumn {
            name: format!("c{position}"),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            hidden: false,
            rowid_alias: false,
        })
        .collect::<Vec<_>>();
    (
        HirDocument {
            snapshot: CatalogSnapshot::from_id(17),
            databases: Vec::new(),
            root: HirRoot::Query(QueryRoot {
                query,
                trigger: None,
            }),
            queries: vec![Query {
                id: query,
                parent: None,
                captures: Vec::new(),
                reachable_ctes: Vec::new(),
                blocks: vec![QueryBlock {
                    id: block,
                    from: Some(From {
                        first: source,
                        joins: Vec::new(),
                    }),
                    outputs: vec![Output {
                        id: output,
                        name: "value".to_string(),
                        expr: expression,
                        type_fact: TypeFact::known(Type::Integer),
                        affinity: Affinity::Integer,
                        schema_affinity: Affinity::Integer,
                        has_affinity: true,
                        collation: None,
                        collation_is_explicit: false,
                        name_kind: OutputNameKind::Inferred,
                    }],
                    aggregate_count: 0,
                    window_function_count: 0,
                    body: QueryBlockBody::Select {
                        distinctness: None,
                        filter: None,
                        grouping: None,
                        windows: Vec::new(),
                    },
                }],
                first: block,
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output: vec![output],
            }],
            sources: vec![Source {
                id: source,
                owner: SourceOwner::QueryBlock(block),
                database: None,
                name: "items".to_string(),
                alias: None,
                kind: SourceKind::SchemaExpression,
                generated_expressions: vec![ColumnReadExpression::Absent; width],
                default_expressions: vec![ColumnReadExpression::Absent; width],
                column_type_programs: vec![None; width],
                check_constraints: None,
                columns,
                rowid_available: true,
                index_hint: IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            }],
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        },
        query,
        source,
    )
}

fn root_filter(document: &HirDocument) -> &Expr {
    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let query = document.query(root.query).expect("root query exists");
    let QueryBlockBody::Select {
        filter: Some(filter),
        ..
    } = &query.blocks[query.first.index].body
    else {
        panic!("fixture has a filter");
    };
    filter
}

fn root_output(document: &HirDocument, position: usize) -> &Expr {
    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let query = document.query(root.query).expect("root query exists");
    &query.blocks[query.first.index].outputs[position].expr
}

fn btree_source(document: &HirDocument, position: usize) -> Arc<BTreeTable> {
    let SourceKind::Table(table) = &document.sources[position].kind else {
        panic!("source is a catalog table");
    };
    let crate::schema::Table::BTree(table) = table.value() else {
        panic!("source is a B-tree table");
    };
    table.clone()
}

// Example: `SELECT c7, rowid FROM items` must read column position 7 from
// exactly the runtime bound to the HIR `SourceId`. A register-backed OLD/NEW
// row reads `base + 7`, while rowid uses its separate register, never slot 7.
#[hegel::test]
fn columns_and_rowids_use_exact_hir_runtime_positions(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let (document, query, source) =
        source_document(width, Expr::column(SourceId::new(0), position));
    let cursor_backed = tc.draw(generators::booleans());
    let runtime = if cursor_backed {
        SourceRuntime::Cursor(CursorId(9))
    } else {
        SourceRuntime::Registers {
            columns: RegisterRange::new(40, width),
            rowid: Some(RegisterId(39)),
        }
    };
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("generated document is closed");
    bindings.enter_query(query).expect("q0 is a root query");
    bindings
        .bind_source(source, runtime)
        .expect("source belongs to q0");
    let mut program = program();

    let column_target = {
        let mut emitter = ExpressionEmitter::new(&mut program, &mut bindings);
        emitter
            .emit_new(&Expr::column(source, position))
            .expect("resolved column lowers")
            .first
            .0
    };
    if cursor_backed {
        assert!(matches!(
            program.insns.last(),
            Some((Insn::Column { cursor_id: 9, column, dest, .. }, _))
                if *column == position && *dest == column_target
        ));
    } else {
        assert!(matches!(
            program.insns.last(),
            Some((Insn::Copy { src_reg, dst_reg, extra_amount: 0 }, _))
                if *src_reg == 40 + position && *dst_reg == column_target
        ));
    }

    let rowid_target = {
        let mut emitter = ExpressionEmitter::new(&mut program, &mut bindings);
        emitter
            .emit_new(&Expr::rowid(source))
            .expect("resolved rowid lowers")
            .first
            .0
    };
    if cursor_backed {
        assert!(matches!(
            program.insns.last(),
            Some((Insn::RowId { cursor_id: 9, dest }, _)) if *dest == rowid_target
        ));
    } else {
        assert!(matches!(
            program.insns.last(),
            Some((Insn::Copy { src_reg: 39, dst_reg, extra_amount: 0 }, _))
                if *dst_reg == rowid_target
        ));
    }
}

// Example: after analyzing
// `SELECT 1 FROM items WHERE c0 COLLATE NOCASE = ?1`, lowering still emits
// NOCASE and c0's frozen affinity after the live `Schema` has been dropped.
// No name lookup or Resolver is available to repair missing HIR facts.
#[hegel::test]
fn comparisons_lower_from_frozen_hir_after_catalog_is_gone(tc: hegel::TestCase) {
    let types = [
        ("BLOB", Affinity::Blob),
        ("TEXT", Affinity::Text),
        ("NUMERIC", Affinity::Numeric),
        ("INTEGER", Affinity::Integer),
        ("REAL", Affinity::Real),
    ];
    let type_index = tc.draw(generators::integers::<usize>().max_value(types.len() - 1));
    let explicit_collation = tc.draw(generators::booleans());
    let table = BTreeTable::from_sql(
        &format!("CREATE TABLE items(c0 {})", types[type_index].0),
        2,
    )
    .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let collation = if explicit_collation {
        " COLLATE NOCASE"
    } else {
        ""
    };
    let statement = parse_statement(&format!("SELECT 1 FROM items WHERE c0{collation} = ?1"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("comparison has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);
    PhysicalPlan::new(&document).expect("planning needs only the closed HIR document");

    let expression = root_filter(&document);
    let Expr::Binary {
        comparison: Some(comparison),
        ..
    } = expression
    else {
        panic!("filter is a binary comparison");
    };
    let expected = &comparison.components[0];
    assert_eq!(expected.affinity, types[type_index].1);
    assert_eq!(
        expected.collation.as_ref().map(|value| *value.value()),
        explicit_collation.then_some(CollationSeq::NoCase)
    );

    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let source = document.sources[0].id;
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("analyzed document is closed");
    bindings
        .enter_query(root.query)
        .expect("root query enters from root scope");
    let mut program = program();
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(btree_source(&document, 0)));
    bindings
        .bind_source(source, SourceRuntime::Cursor(CursorId(cursor)))
        .expect("items belongs to the root query");
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(expression)
        .expect("closed comparison lowers without a catalog");

    let emitted = program.insns.iter().find_map(|(instruction, _)| {
        let Insn::Eq {
            flags, collation, ..
        } = instruction
        else {
            return None;
        };
        Some((flags, collation))
    });
    let Some((flags, collation)) = emitted else {
        panic!("comparison lowering emits Eq");
    };
    assert_eq!(flags.get_affinity(), expected.affinity);
    assert_eq!(
        *collation,
        expected.collation.as_ref().map(|value| *value.value())
    );
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Column {
            cursor_id,
            column: 0,
            ..
        } if *cursor_id == cursor
    )));
    assert!(program.insns.iter().any(
        |(instruction, _)| matches!(instruction, Insn::Variable { index, .. } if index.get() == 1)
    ));
}

// Examples: `SELECT c0 || '-tail' FROM items` for `c0 TEXT` must emit text
// concatenation, while `SELECT c0 || c0 FROM items` for `c0 INTEGER[]` must
// emit array concatenation. The choice comes from the bound HIR type facts and
// remains valid after the catalog that declared `c0` has been dropped.
#[hegel::test]
fn concatenation_kind_lowers_from_frozen_operand_types(tc: hegel::TestCase) {
    let array = tc.draw(generators::booleans());
    let declaration = if array { "INTEGER[]" } else { "TEXT" };
    let rhs = if array { "c0" } else { "'-tail'" };
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items(c0 {declaration})"), 2)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("SELECT c0 || {rhs} FROM items"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated concatenation has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let Expr::Binary { array_concat, .. } = root_output(&document, 0) else {
        panic!("output is a binary concatenation");
    };
    assert_eq!(*array_concat, array);

    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let source = document.sources[0].id;
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("analyzed document is closed");
    bindings
        .enter_query(root.query)
        .expect("root query enters from root scope");
    let mut program = program();
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(btree_source(&document, 0)));
    bindings
        .bind_source(source, SourceRuntime::Cursor(CursorId(cursor)))
        .expect("items belongs to the root query");
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(root_output(&document, 0))
        .expect("closed concatenation lowers without a catalog");

    assert!(program.insns.iter().any(|(instruction, _)| if array {
        matches!(instruction, Insn::ArrayConcat { .. })
    } else {
        matches!(instruction, Insn::Concat { .. })
    }));
}

// Example: each of `c0 BETWEEN ?1 AND ?2`,
// `CASE c0 WHEN ?1 THEN 10 ELSE 20 END`, and `c0 IN (?1, ?2, NULL)`
// carries all comparison facts out of binding. Direct lowering must accept any
// of them after the schema is gone, without rebuilding their SQL rules.
#[hegel::test]
fn comparison_forms_lower_as_closed_hir(tc: hegel::TestCase) {
    let form = tc.draw(generators::integers::<u8>().max_value(2));
    let table = BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER)", 2)
        .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = match form {
        0 => "SELECT 1 FROM items WHERE c0 BETWEEN ?1 AND ?2",
        1 => "SELECT CASE c0 WHEN ?1 THEN 10 ELSE 20 END FROM items",
        _ => "SELECT 1 FROM items WHERE c0 IN (?1, ?2, NULL)",
    };
    let statement = parse_statement(sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("fixture has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);
    PhysicalPlan::new(&document).expect("planning needs only the closed HIR document");

    let expression = if form == 1 {
        root_output(&document, 0)
    } else {
        root_filter(&document)
    };
    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("analyzed document is closed");
    bindings
        .enter_query(root.query)
        .expect("q0 is a root query");
    let mut program = program();
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(btree_source(&document, 0)));
    bindings
        .bind_source(
            document.sources[0].id,
            SourceRuntime::Cursor(CursorId(cursor)),
        )
        .expect("items belongs to q0");
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(expression)
        .expect("closed comparison form lowers without a catalog");

    let comparison_count = program
        .insns
        .iter()
        .filter(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Eq { .. }
                    | Insn::Ne { .. }
                    | Insn::Lt { .. }
                    | Insn::Le { .. }
                    | Insn::Gt { .. }
                    | Insn::Ge { .. }
            )
        })
        .count();
    assert_eq!(
        comparison_count,
        if form == 1 {
            1
        } else if form == 0 {
            2
        } else {
            3
        }
    );
}

// Example: after analyzing
// `SELECT c1, c2 FROM items` for
// `c1 GENERATED ALWAYS AS (c0 + 7) VIRTUAL, c2 DEFAULT 11`, reading c1 must
// execute the stored `c0 + 7` HIR instead of loading physical field 1. Reading
// c2 must use `ColumnHasField`: old short records compute 11, while newer
// records load physical field 1. Both paths must still lower after the live
// schema and resolver context have been dropped.
#[hegel::test]
fn stored_column_reads_use_frozen_hir_and_physical_positions(tc: hegel::TestCase) {
    let generated_offset =
        i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let default_value =
        i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31))) + 40;
    let table = BTreeTable::from_sql(
        &format!(
            "CREATE TABLE items(\
             c0 INTEGER, \
             c1 INTEGER GENERATED ALWAYS AS (c0 + {generated_offset}) VIRTUAL, \
             c2 INTEGER DEFAULT {default_value})"
        ),
        2,
    )
    .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement("SELECT c1, c2 FROM items");
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("stored-expression fixture has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    assert!(matches!(
        document.sources[0].generated_expressions[1],
        ColumnReadExpression::Planned(_)
    ));
    assert!(matches!(
        document.sources[0].default_expressions[2],
        ColumnReadExpression::Planned(_)
    ));
    let SourceKind::Table(resolved_table) = &document.sources[0].kind else {
        panic!("items is a table source");
    };
    let crate::schema::Table::BTree(table) = resolved_table.value() else {
        panic!("items is a B-tree table");
    };
    let table = table.clone();
    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let source = document.sources[0].id;
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("analyzed document is closed");
    bindings
        .enter_query(root.query)
        .expect("q0 is a root query");
    let mut program = program();
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table));
    bindings
        .bind_source(source, SourceRuntime::Cursor(CursorId(cursor)))
        .expect("items belongs to q0");

    let generated_start = program.insns.len();
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(root_output(&document, 0))
        .expect("virtual generated column lowers from HIR");
    let generated_insns = &program.insns[generated_start..];
    assert!(generated_insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Column {
            cursor_id,
            column: 0,
            ..
        } if *cursor_id == cursor
    )));
    assert!(generated_insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Add { .. })));
    assert!(!generated_insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Column { column: 1, .. })));

    let default_start = program.insns.len();
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(root_output(&document, 1))
        .expect("added-column default lowers from HIR");
    program
        .resolve_labels()
        .expect("stored-column branches are closed");
    let default_insns = &program.insns[default_start..];
    assert!(default_insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::ColumnHasField {
            cursor_id,
            column: 1,
            ..
        } if *cursor_id == cursor
    )));
    assert!(default_insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Integer { value, .. } if *value == default_value
    )));
    assert!(default_insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Column {
            cursor_id,
            column: 1,
            default: None,
            ..
        } if *cursor_id == cursor
    )));
}

// Example: `SELECT 'hello' NOT LIKE 'h%'` resolves the exact two-argument
// LIKE function during analysis. After the catalog is dropped, lowering calls
// that stored function handle and applies NOT; it never resolves `like` again.
#[hegel::test]
fn like_uses_the_resolved_function_handle_after_catalog_is_gone(tc: hegel::TestCase) {
    let negated = tc.draw(generators::booleans());
    let operator = if tc.draw(generators::booleans()) {
        "LIKE"
    } else {
        "GLOB"
    };
    let not = if negated { "NOT " } else { "" };
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("SELECT 'hello' {not}{operator} 'h*'"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("LIKE-family fixture has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let expression = root_output(&document, 0);
    let Expr::Like {
        function,
        argument_count,
        ..
    } = expression
    else {
        panic!("output is a resolved LIKE-family call");
    };
    let expected_kind = std::mem::discriminant(function.value());
    let expected_name = function.value().to_string();
    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("analyzed document is closed");
    bindings
        .enter_query(root.query)
        .expect("q0 is a root query");
    let mut program = program();
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(expression)
        .expect("resolved LIKE-family call lowers without a catalog");

    let emitted = program.insns.iter().find_map(|(instruction, _)| {
        let Insn::Function { func, .. } = instruction else {
            return None;
        };
        Some(func)
    });
    let Some(emitted) = emitted else {
        panic!("LIKE-family lowering emits Function");
    };
    assert_eq!(emitted.arg_count, *argument_count);
    assert_eq!(std::mem::discriminant(&emitted.func), expected_kind);
    assert_eq!(emitted.func.to_string(), expected_name);
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Not { .. }))
            .count(),
        usize::from(negated)
    );
}

// Example: each of `SELECT abs(?1)`, `SELECT lower(?1)`, and
// `SELECT length(?1)` stores its exact scalar function during analysis.
// Direct lowering after the schema is dropped must emit that handle and the
// already-numbered parameter; it must never search for the function by name.
#[hegel::test]
fn scalar_functions_lower_from_their_frozen_handles(tc: hegel::TestCase) {
    let names = ["abs", "lower", "length"];
    let selected = tc.draw(generators::integers::<usize>().max_value(names.len() - 1));
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("SELECT {}(?1)", names[selected]));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("scalar function fixture has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let expression = root_output(&document, 0);
    let Expr::Function(call) = expression else {
        panic!("output is a resolved scalar function");
    };
    let expected_name = call.function.value().to_string();
    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("analyzed document is closed");
    bindings
        .enter_query(root.query)
        .expect("q0 is a root query");
    let mut program = program();
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(expression)
        .expect("resolved scalar call lowers without a catalog");

    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Variable { index, .. } if index.get() == 1
    )));
    let emitted = program.insns.iter().find_map(|(instruction, _)| {
        let Insn::Function { func, .. } = instruction else {
            return None;
        };
        Some(func)
    });
    let Some(emitted) = emitted else {
        panic!("scalar lowering emits Function");
    };
    assert_eq!(emitted.arg_count, 1);
    assert_eq!(emitted.func.to_string(), expected_name);
}

// Example: in `SELECT coalesce(NULL, ?1, random())`, a non-NULL `?1` must
// jump past `random()`; in `SELECT iif(?1, ?2, random())`, a true condition
// must do the same. The stored function handle is not enough here: lowering
// must preserve SQL's lazy branch evaluation and exact jump targets.
#[hegel::test]
fn lazy_scalar_functions_do_not_evaluate_unselected_branches(tc: hegel::TestCase) {
    let coalesce = tc.draw(generators::booleans());
    let sql = if coalesce {
        "SELECT coalesce(NULL, ?1, random())"
    } else {
        "SELECT iif(?1, ?2, random())"
    };
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("lazy scalar fixture has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("fixture is a query");
    };
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("analyzed document is closed");
    bindings
        .enter_query(root.query)
        .expect("q0 is a root query");
    let mut program = program();
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(root_output(&document, 0))
        .expect("lazy scalar lowers without a catalog");
    program
        .resolve_labels()
        .expect("lazy function branches are all closed");

    let random = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Function { func, .. } if func.func.to_string() == "random"
            )
        })
        .expect("unselected branch still has runtime bytecode");
    if coalesce {
        assert!(program.insns[..random]
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::NotNull {
                    target_pc: crate::vdbe::BranchOffset::Offset(target),
                    ..
                } if *target as usize == random + 1
            )));
    } else {
        assert!(program.insns[..random]
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::Goto {
                    target_pc: crate::vdbe::BranchOffset::Offset(target),
                } if *target as usize == random + 1
            )));
    }
}

// Example: `ENCODE (precision)` for `custom_value(6)` receives the exact
// synthetic row `[value, 6]`. If the stored body reads position 1, lowering
// copies the precision register—not the outer table's c1—and removes that
// synthetic source as soon as the schema call finishes.
#[hegel::test]
fn schema_programs_lower_with_exact_temporary_inputs(tc: hegel::TestCase) {
    let argument_count = usize::from(tc.draw(generators::integers::<u8>().max_value(5))) + 1;
    let selected = tc.draw(generators::integers::<usize>().max_value(argument_count));
    let outer_source = SourceId::new(0);
    let (mut document, query, _) = source_document(2, Expr::column(outer_source, 0));
    let input_source = SourceId::new(1);
    let program_id = SchemaProgramId::new(0);
    let arguments = (0..argument_count)
        .map(|position| Expr::Literal(ast::Literal::Numeric((100 + position).to_string())))
        .collect::<Vec<_>>();
    let mut schema_source = source_document(argument_count + 1, Expr::column(SourceId::new(0), 0))
        .0
        .sources
        .remove(0);
    schema_source.id = input_source;
    schema_source.owner = SourceOwner::Root;
    schema_source.name = "schema_inputs".to_string();
    document.sources.push(schema_source);
    document.schema_programs.push(BoundSchemaProgram {
        input_source,
        body: Expr::column(input_source, selected),
    });
    document.queries[0].blocks[0].outputs[0].expr = Expr::Cast {
        expr: Box::new(Expr::column(outer_source, 0)),
        target: TypeName {
            name: "property_type".to_string(),
            parameters: arguments.clone(),
            array_dimensions: 0,
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            programs: BoundCastPrograms {
                encode: vec![BoundSchemaCall {
                    program: program_id,
                    arguments,
                }],
                domain: None,
                apply_builtin_affinity: false,
            },
        },
    };
    document
        .validate()
        .expect("generated schema-program document is closed");

    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("generated document is closed");
    bindings.enter_query(query).expect("q0 is a root query");
    let outer_runtime = SourceRuntime::Registers {
        columns: RegisterRange::new(40, 2),
        rowid: Some(RegisterId(39)),
    };
    bindings
        .bind_source(outer_source, outer_runtime)
        .expect("outer source belongs to q0");
    let mut program = program();
    ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(root_output(&document, 0))
        .expect("stored program lowers directly from HIR");

    // r1 is the CAST result. The temporary schema row begins at r2:
    // r2=value and r3.. are its user arguments.
    assert!(matches!(
        program.insns.last(),
        Some((Insn::Copy { src_reg, dst_reg: 1, extra_amount: 0 }, _))
            if *src_reg == 2 + selected
    ));
    assert_eq!(bindings.source(outer_source), Ok(outer_runtime));
    assert_eq!(
        bindings.source(input_source),
        Err(RuntimeBindingError::SourceNotCaptured(input_source))
    );
}

// Example: `SELECT c2 FROM items` with no runtime binding for `items` is a
// physical-planning error. Lowering must not search a schema for another table
// named `items` or guess a cursor from column position 2.
#[hegel::test]
fn an_unbound_hir_source_is_an_explicit_lowering_error(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let (document, query, source) =
        source_document(width, Expr::column(SourceId::new(0), position));
    let mut bindings =
        RuntimeBindings::new(&document, document.snapshot).expect("generated document is closed");
    bindings.enter_query(query).expect("q0 is a root query");
    let mut program = program();
    let error = ExpressionEmitter::new(&mut program, &mut bindings)
        .emit_new(&Expr::column(source, position))
        .expect_err("unbound source cannot lower");

    assert!(matches!(
        error,
        PhysicalExpressionError::Runtime(RuntimeBindingError::WrongScope("unbound source"))
    ));
    assert!(program.insns.is_empty());
}
