//! Properties for the closed stored-expression physical boundary.

use hegel::generators;

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Schema, Table},
    schema_expr::{
        BuiltinSchemaExprResolver, ResolutionMode, SchemaColumn, SchemaExpr, SchemaExprContext,
        SchemaExprProfile, SchemaTable,
    },
    sync::Arc,
    translate::semantic::{
        context::SemanticContext,
        schema_expr::{
            analyze_positional_scalar_syntax, analyze_schema_expr, analyze_table_schema_syntax,
            SchemaExprInput, SchemaSyntaxInput,
        },
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts},
        insn::Insn,
    },
    QueryMode, SymbolTable,
};
use turso_parser::ast;

// Examples:
// - Input `c2` over `[c0, c1, c2]` reads register 42 when the caller binds the
//   input range at register 40.
// - Binding the same closed HIR at register 90 makes `c2` read register 92;
//   the parser expression remains the identifier `c2` in both cases.
// For every generated width, position, and register base, semantic analysis
// turns the input name into one stable HIR position. Runtime placement is a
// separate physical concern and cannot leak register nodes into parser syntax.
#[hegel::test]
fn positional_scalar_inputs_bind_by_hir_position_not_parser_mutation(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)));
    let position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let register_base =
        usize::from(tc.draw(generators::integers::<u16>().min_value(1).max_value(1000)));
    let inputs = (0..width)
        .map(|column| SchemaExprInput {
            name: format!("c{column}"),
            declared_type: None,
            array_dimensions: 0,
            type_fact: None,
        })
        .collect::<Vec<_>>();
    let expected_name = format!("c{position}");
    let syntax = ast::Expr::Id(ast::Name::exact(expected_name.clone()));
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let analyzed = analyze_positional_scalar_syntax(&context, 0, &syntax, &inputs)
        .expect("positional expression closes into HIR");
    let root = match &analyzed.document.root {
        crate::translate::semantic::hir::HirRoot::SchemaExpressions(root) => root,
        _ => panic!("positional analysis returns a schema-expression root"),
    };
    let plan = PhysicalPlan::new(&analyzed.document).expect("closed HIR plans");
    let mut runtime = RootRuntimeInputs::default();
    runtime.bind_source(
        root.source,
        SourceRuntime::Registers {
            columns: RegisterRange::new(register_base, width),
            rowid: None,
        },
    );
    let mut program =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 8));

    emit_root_schema_expression_into(&plan, &mut program, &runtime, 0, 2000)
        .expect("positional expression emits");

    assert!(matches!(
        &syntax,
        ast::Expr::Id(name) if name.as_str() == expected_name
    ));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Copy { src_reg, dst_reg, extra_amount: 0 }
            if *src_reg == register_base + position && *dst_reg == 2000
    )));
}

// Examples:
// - Stored index key `c2` over `[c0, c1, c2]` reads runtime register 102
//   when the caller binds the positional row at register 100.
// - Stored key `c0` over a one-column table reads register 100.
// For every generated width and column position, semantic analysis closes the
// stored expression into HIR and physical lowering reads exactly that slot
// without a Resolver, table name, or parser-column rewrite.
#[hegel::test]
fn stored_column_positions_lower_from_a_closed_hir_root(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)));
    let position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let columns = (0..width)
        .map(|column| SchemaColumn::new(format!("c{column}"), false, None))
        .collect::<Vec<_>>();
    let schema_table = SchemaTable::new("items", columns, true);
    let syntax = ast::Expr::Id(ast::Name::exact(format!("c{position}")));
    let expression = SchemaExpr::resolve(
        &syntax,
        SchemaExprProfile::IndexKey,
        SchemaExprContext::for_table(&schema_table),
        &BuiltinSchemaExprResolver,
        ResolutionMode::Strict,
    )
    .expect("generated stored expression resolves");

    let definitions = (0..width)
        .map(|column| format!("c{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE items({definitions})"), 2)
            .expect("generated table is valid"),
    );
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("fixture table is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let analyzed = analyze_schema_expr(
        &context,
        0,
        Arc::new(Table::BTree(table)),
        expression.as_valid().expect("expression is valid"),
    )
    .expect("stored expression analysis closes the HIR document");
    let root = match &analyzed.document.root {
        crate::translate::semantic::hir::HirRoot::SchemaExpressions(root) => root,
        _ => panic!("stored analysis returns a schema-expression root"),
    };
    let plan = PhysicalPlan::new(&analyzed.document).expect("closed HIR plans");
    let mut inputs = RootRuntimeInputs::default();
    inputs.bind_source(
        root.source,
        SourceRuntime::Registers {
            columns: RegisterRange::new(100, width),
            rowid: Some(RegisterId(200)),
        },
    );
    let mut program =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 8));

    let outputs = emit_root_schema_expressions(&plan, &mut program, &inputs)
        .expect("closed stored expression emits");

    assert_eq!(outputs.len(), 1);
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Copy { src_reg, dst_reg, extra_amount: 0 }
            if *src_reg == 100 + position && *dst_reg == outputs[0].first.0
    )));
}

// Examples:
// - For `CREATE INDEX ... ON items(c0) WHERE c1`, the c1 predicate is emitted
//   and tested before the c0 key is read.
// - For `CREATE INDEX ... ON items(c3) WHERE c0`, selecting expression zero
//   cannot accidentally emit expression one from the same closed batch.
// For every generated pair of distinct positions, indexed emission preserves
// the semantic batch order and reads only the requested positional column.
#[hegel::test]
fn partial_index_predicate_can_be_emitted_before_its_key(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(2).max_value(16)));
    let key_position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let predicate_position = (key_position + 1) % width;
    let definitions = (0..width)
        .map(|column| format!("c{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE items({definitions})"), 2)
            .expect("generated table is valid"),
    );
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("fixture table is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let predicate = ast::Expr::Id(ast::Name::exact(format!("c{predicate_position}")));
    let key = ast::Expr::Id(ast::Name::exact(format!("c{key_position}")));
    let analyzed = analyze_table_schema_syntax(
        &context,
        0,
        Arc::new(Table::BTree(table)),
        &[
            SchemaSyntaxInput {
                syntax: &predicate,
                profile: SchemaExprProfile::PartialIndexPredicate,
                owner_column: None,
            },
            SchemaSyntaxInput {
                syntax: &key,
                profile: SchemaExprProfile::IndexKey,
                owner_column: None,
            },
        ],
    )
    .expect("catalog syntax closes into one HIR batch");
    let root = match &analyzed.document.root {
        crate::translate::semantic::hir::HirRoot::SchemaExpressions(root) => root,
        _ => panic!("stored analysis returns a schema-expression root"),
    };
    let plan = PhysicalPlan::new(&analyzed.document).expect("closed HIR plans");
    let mut inputs = RootRuntimeInputs::default();
    inputs.bind_source(
        root.source,
        SourceRuntime::Registers {
            columns: RegisterRange::new(100, width),
            rowid: Some(RegisterId(200)),
        },
    );
    let mut program =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 8));

    emit_root_schema_expression_into(&plan, &mut program, &inputs, 0, 300)
        .expect("predicate emits independently");
    let predicate_end = program.insns.len();
    emit_root_schema_expression_into(&plan, &mut program, &inputs, 1, 301)
        .expect("key emits independently");

    assert!(program.insns[..predicate_end]
        .iter()
        .any(|(instruction, _)| {
            matches!(instruction, Insn::Copy { src_reg, dst_reg, extra_amount: 0 }
            if *src_reg == 100 + predicate_position && *dst_reg == 300)
        }));
    assert!(!program.insns[..predicate_end]
        .iter()
        .any(|(instruction, _)| {
            matches!(instruction, Insn::Copy { src_reg, .. } if *src_reg == 100 + key_position)
        }));
    assert!(program.insns[predicate_end..]
        .iter()
        .any(|(instruction, _)| {
            matches!(instruction, Insn::Copy { src_reg, dst_reg, extra_amount: 0 }
                if *src_reg == 100 + key_position && *dst_reg == 301)
        }));
}

// Example: for `CREATE TABLE t(a, g AS (a), h AS (g))` followed by
// `CREATE INDEX i ON t(h)`, the stored key program for `h` must read physical
// column `a`. It must not emit `Column(g)`, because `g` is virtual and has no
// field in the table record. The same rule holds for every generated depth.
#[hegel::test]
fn cursor_schema_expressions_close_transitive_generated_reads(tc: hegel::TestCase) {
    let depth = usize::from(tc.draw(generators::integers::<u8>().min_value(2).max_value(8)));
    let mut columns = vec!["base INTEGER".to_string()];
    for generated in 0..depth {
        let dependency = if generated == 0 {
            "base".to_string()
        } else {
            format!("g{}", generated - 1)
        };
        columns.push(format!("g{generated} INTEGER AS ({dependency}) VIRTUAL"));
    }
    let table = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE items({})", columns.join(", ")), 2)
            .expect("the generated-column chain is valid"),
    );
    let owner = depth;
    let expression = table.columns()[owner]
        .generated_expr()
        .expect("the final column is generated")
        .clone();
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("the fixture table is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let analyzed = analyze_table_schema_syntax(
        &context,
        0,
        Arc::new(Table::BTree(table)),
        &[SchemaSyntaxInput {
            syntax: &expression,
            profile: SchemaExprProfile::IndexKey,
            owner_column: Some(owner),
        }],
    )
    .expect("the generated index key closes into HIR");
    let root = match &analyzed.document.root {
        crate::translate::semantic::hir::HirRoot::SchemaExpressions(root) => root,
        _ => panic!("stored analysis returns a schema-expression root"),
    };
    let source = analyzed
        .document
        .source(root.source)
        .expect("the schema-expression source exists");
    for dependency in 1..owner {
        assert!(matches!(
            source.generated_expressions[dependency],
            crate::translate::semantic::hir::ColumnReadExpression::Planned(_)
        ));
    }

    let plan = PhysicalPlan::new(&analyzed.document).expect("closed HIR plans");
    let cursor = CursorId(7);
    let mut inputs = RootRuntimeInputs::default();
    inputs.bind_source(root.source, SourceRuntime::Cursor(cursor));
    let mut program =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 8));
    emit_root_schema_expression_into(&plan, &mut program, &inputs, 0, 300)
        .expect("the transitive generated key emits");

    let physical_reads = program.insns.iter().filter_map(|(instruction, _)| {
        let Insn::Column {
            cursor_id, column, ..
        } = instruction
        else {
            return None;
        };
        (*cursor_id == cursor.0).then_some(*column)
    });
    assert_eq!(physical_reads.collect::<Vec<_>>(), vec![0]);
}
