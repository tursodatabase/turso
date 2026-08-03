//! Properties for lowering trigger OLD and NEW inputs without AST rewrites.

use hegel::generators;

use super::*;
use crate::{
    schema::{BTreeTable, Table, Type},
    sync::Arc,
    translate::semantic::hir::{
        CatalogObject, CatalogObjectId, CatalogSnapshot, DatabaseId, DatabaseSnapshot, Expr,
        HirDocument, HirRoot, IndexCoverage, IndexHint, PseudoSource, Source, SourceColumn,
        SourceId, SourceKind, SourceOwner, TriggerEnvironment, TriggerPredicate, TypeFact,
    },
    vdbe::{
        affinity::Affinity,
        builder::{ProgramBuilder, ProgramBuilderOpts},
        insn::Insn,
    },
    QueryMode, MAIN_DB_ID,
};

fn trigger_document(position: usize) -> HirDocument {
    let snapshot = CatalogSnapshot::from_id(71);
    let table = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(a INTEGER, b INTEGER, c INTEGER)", 2)
            .expect("fixed trigger table is valid"),
    );
    let resolved = CatalogObject::new(
        CatalogObjectId::new(1),
        snapshot,
        Some(DatabaseId::new(MAIN_DB_ID)),
        Arc::new(Table::BTree(table)),
    );
    let source = SourceId::new(0);
    let columns = (0..3)
        .map(|column| SourceColumn {
            name: format!("c{column}"),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            has_affinity: false,
            collation: None,
            hidden: false,
            rowid_alias: false,
        })
        .collect::<Vec<_>>();
    HirDocument {
        snapshot,
        databases: vec![DatabaseSnapshot {
            database: DatabaseId::new(MAIN_DB_ID),
            schema_version: 0,
        }],
        root: HirRoot::TriggerPredicate(TriggerPredicate {
            expression: Expr::column(source, position),
            environment: TriggerEnvironment {
                table: resolved.clone(),
                new_source: Some(source),
                old_source: None,
            },
        }),
        queries: Vec::new(),
        sources: vec![Source {
            id: source,
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(MAIN_DB_ID)),
            name: "new".to_string(),
            alias: None,
            kind: SourceKind::Pseudo {
                kind: PseudoSource::New,
                table: resolved,
            },
            generated_expressions:
                vec![crate::translate::semantic::hir::ColumnReadExpression::Absent; 3],
            default_expressions: vec![
                crate::translate::semantic::hir::ColumnReadExpression::Absent;
                3
            ],
            column_type_programs: vec![None; 3],
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
    }
}

// Example: `WHEN NEW.c2` must read `new_base + 2` and branch on that value;
// it must never discover NEW by name or accidentally read the OLD row range.
#[hegel::test]
fn trigger_predicates_read_the_exact_supplied_row_image_position(tc: hegel::TestCase) {
    let position = usize::from(tc.draw(generators::integers::<u8>().max_value(2)));
    let base = usize::from(tc.draw(generators::integers::<u16>().min_value(20))) + 1;
    let document = trigger_document(position);
    let plan = PhysicalPlan::new(&document).expect("generated trigger HIR is closed");
    let mut program =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 16, 4));
    let false_target = program.allocate_label();
    let mut inputs = RootRuntimeInputs::default();
    inputs.bind_source(
        SourceId::new(0),
        SourceRuntime::Registers {
            columns: RegisterRange::new(base, 3),
            rowid: Some(RegisterId(base + 3)),
        },
    );

    emit_trigger_predicate(&plan, &mut program, &inputs, false_target)
        .expect("a supplied NEW row lowers without catalog access");

    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Copy { src_reg, .. } if *src_reg == base + position
    )));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::IfNot { target_pc, jump_if_null: true, .. } if *target_pc == false_target
    )));
}

// Example: a trigger body that reads `NEW.c2, OLD.rowid, NEW.rowid, OLD.c0`
// must receive `[new_base + 2, old_rowid, new_rowid, old_base]` in that exact order.
#[hegel::test]
fn trigger_parameters_preserve_row_kind_position_and_reference_order(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)));
    let new_base = usize::from(tc.draw(generators::integers::<u16>().min_value(1))) + 1;
    let old_base = new_base + width + 5;
    let new_rowid = old_base + width + 7;
    let old_rowid = new_rowid + 3;
    let first_position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let second_position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let parameters = [
        TriggerParameter::NewColumn(first_position),
        TriggerParameter::OldRowId,
        TriggerParameter::NewRowId,
        TriggerParameter::OldColumn(second_position),
    ];

    let resolved = resolve_trigger_parameters(
        &parameters,
        TriggerRows {
            new: Some(TriggerRow {
                columns: RegisterRange::new(new_base, width),
                rowid: RegisterId(new_rowid),
            }),
            old: Some(TriggerRow {
                columns: RegisterRange::new(old_base, width),
                rowid: RegisterId(old_rowid),
            }),
        },
    )
    .expect("both generated trigger row images are available");

    assert_eq!(
        resolved,
        vec![
            new_base + first_position,
            old_rowid,
            new_rowid,
            old_base + second_position,
        ]
    );
}
