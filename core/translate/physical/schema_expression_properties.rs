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
    translate::semantic::{context::SemanticContext, schema_expr::analyze_schema_expr},
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts},
        insn::Insn,
    },
    QueryMode, SymbolTable,
};
use turso_parser::ast;

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
