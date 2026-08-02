//! Property tests for DML destination and expression-namespace rules.

use hegel::generators;
use turso_parser::ast;

use super::{
    binding_outcome,
    dml_rules::{
        add_schema_named_target, configure_target_read_scope, configure_upsert_scope,
        resolve_assignment_columns, resolve_insert_columns, resolve_target_column, DmlOperation,
    },
    hir::{self, DatabaseId, TargetColumn},
    scope::{NamePrecedence, Scope},
    source, source_columns, BindingOutcome,
};
use crate::{
    schema::{BTreeTable, Table},
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
enum GeneratedDml {
    Insert,
    Update,
    Delete,
}

fn generated_count(tc: &hegel::TestCase) -> usize {
    usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1
}

fn generated_position(tc: &hegel::TestCase, len: usize) -> usize {
    tc.draw(generators::integers::<usize>().max_value(len - 1))
}

fn name(value: impl Into<String>) -> ast::Name {
    ast::Name::exact(value.into())
}

fn table_from_sql(sql: &str) -> Table {
    Table::BTree(Arc::new(
        BTreeTable::from_sql(sql, 2).expect("generated table definition is valid"),
    ))
}

fn generated_table(tc: &hegel::TestCase) -> (Table, Vec<bool>, Vec<bool>) {
    let count = generated_count(tc);
    let mut generated = vec![false; count];
    let mut definitions = vec!["c0 INTEGER".to_string()];
    for (position, generated_at_position) in generated.iter_mut().enumerate().skip(1) {
        *generated_at_position = tc.draw(generators::booleans());
        if *generated_at_position {
            definitions.push(format!("c{position} INTEGER AS (c0) VIRTUAL"));
        } else {
            definitions.push(format!("c{position} INTEGER"));
        }
    }

    let mut table = BTreeTable::from_sql(
        &format!("CREATE TABLE items ({})", definitions.join(", ")),
        2,
    )
    .expect("generated table definition is valid");
    let hidden = (0..count)
        .map(|_| tc.draw(generators::booleans()))
        .collect::<Vec<_>>();
    for (column, hidden) in table.columns_mut().iter_mut().zip(&hidden) {
        column.set_hidden(*hidden);
    }
    (Table::BTree(Arc::new(table)), generated, hidden)
}

// Example: `INSERT INTO items VALUES (...)` writes only ordinary, visible
// columns, preserving their schema positions even around hidden/generated fields.
#[hegel::test]
fn omitted_insert_targets_are_visible_writable_columns_in_schema_order(tc: hegel::TestCase) {
    let (table, generated, hidden) = generated_table(&tc);
    let expected = generated
        .iter()
        .zip(&hidden)
        .enumerate()
        .filter(|(_, (generated, hidden))| !**generated && !**hidden)
        .map(|(position, _)| TargetColumn::Column(position))
        .collect::<Vec<_>>();

    let actual = resolve_insert_columns(&table, &[]).expect("omitted target list is valid");

    assert_eq!(actual, expected);
}

// Example: `INSERT INTO items(C3, c0, C3) ...` preserves the written order and
// duplicate destinations while binding names without ASCII case sensitivity.
#[hegel::test]
fn explicit_insert_targets_bind_exact_positions_in_syntax_order(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let table = table_from_sql(&format!(
        "CREATE TABLE items ({})",
        (0..count)
            .map(|position| format!("c{position} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    let target_count = generated_count(&tc);
    let positions = (0..target_count)
        .map(|_| generated_position(&tc, count))
        .collect::<Vec<_>>();
    let names = positions
        .iter()
        .map(|position| {
            let value = format!("c{position}");
            name(if tc.draw(generators::booleans()) {
                value.to_ascii_uppercase()
            } else {
                value
            })
        })
        .collect::<Vec<_>>();

    let actual = resolve_insert_columns(&table, &names).expect("generated targets exist");
    let expected = positions
        .into_iter()
        .map(TargetColumn::Column)
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
}

// Example: `UPDATE items SET (c2, C0, c2) = (...)` binds destination columns
// by schema position and does not sort or deduplicate the assignment list.
#[hegel::test]
fn update_assignment_targets_preserve_written_positions(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let table = table_from_sql(&format!(
        "CREATE TABLE items ({})",
        (0..count)
            .map(|position| format!("c{position} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    let assignment_count = generated_count(&tc);
    let positions = (0..assignment_count)
        .map(|_| generated_position(&tc, count))
        .collect::<Vec<_>>();
    let names = positions
        .iter()
        .map(|position| name(format!("C{position}")))
        .collect::<Vec<_>>();

    let actual =
        resolve_assignment_columns(&table, &names).expect("generated assignment targets exist");
    let expected = positions
        .into_iter()
        .map(TargetColumn::Column)
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
}

// Example: `INSERT INTO items(generated_value) VALUES (1)` and
// `UPDATE items SET generated_value = 1` both reject the generated destination.
#[hegel::test]
fn generated_columns_are_never_writable_dml_destinations(tc: hegel::TestCase) {
    let suffix = tc.draw(generators::integers::<u16>());
    let column = format!("generated_{suffix}");
    let table = table_from_sql(&format!(
        "CREATE TABLE items (base INTEGER, {column} INTEGER AS (base) VIRTUAL)"
    ));
    let column = name(column);

    assert!(resolve_target_column(&table, &column, DmlOperation::Insert).is_err());
    assert!(resolve_target_column(&table, &column, DmlOperation::Update).is_err());
}

// Example: `INSERT INTO items(rowid) ...` targets the hidden rowid for an
// ordinary table, its INTEGER PRIMARY KEY alias when present, and is rejected
// for a WITHOUT ROWID table. `_rowid_` and `oid` follow the same rule.
#[hegel::test]
fn pseudo_rowid_names_follow_the_table_rowid_model(tc: hegel::TestCase) {
    let spellings = ["rowid", "_rowid_", "oid"];
    let spelling = spellings[generated_position(&tc, spellings.len())];
    let spelling = name(if tc.draw(generators::booleans()) {
        spelling.to_ascii_uppercase()
    } else {
        spelling.to_string()
    });
    let ordinary = table_from_sql("CREATE TABLE ordinary (value INTEGER)");
    let aliased = table_from_sql("CREATE TABLE aliased (id INTEGER PRIMARY KEY, value INTEGER)");
    let without_rowid = table_from_sql(
        "CREATE TABLE without_rowid (id TEXT PRIMARY KEY, value INTEGER) WITHOUT ROWID",
    );

    assert_eq!(
        resolve_target_column(&ordinary, &spelling, DmlOperation::Insert)
            .expect("ordinary tables expose rowid"),
        TargetColumn::RowId
    );
    assert_eq!(
        resolve_target_column(&aliased, &spelling, DmlOperation::Insert)
            .expect("INTEGER PRIMARY KEY aliases rowid"),
        TargetColumn::Column(0)
    );
    assert!(resolve_target_column(&without_rowid, &spelling, DmlOperation::Insert).is_err());
}

// Example: `CREATE TABLE items(rowid, oid, _rowid_)` makes each written name
// refer to that real column before the hidden-rowid spellings are considered.
#[hegel::test]
fn real_columns_shadow_pseudo_rowid_spellings(tc: hegel::TestCase) {
    let spellings = ["rowid", "_rowid_", "oid"];
    let position = generated_position(&tc, spellings.len());
    let table = table_from_sql("CREATE TABLE items (rowid INTEGER, _rowid_ TEXT, oid BLOB)");

    let actual = resolve_target_column(&table, &name(spellings[position]), DmlOperation::Update)
        .expect("the real column exists");

    assert_eq!(actual, TargetColumn::Column(position));
}

// Example: `... ON CONFLICT DO UPDATE SET c2 = excluded.c2 + c2` binds the
// qualified term to the proposed INSERT row and the unqualified term to target.
#[hegel::test]
fn upsert_scope_separates_target_and_excluded_row_images(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let position = generated_position(&tc, count);
    let target = source(
        7,
        "items",
        Some("dst"),
        Some(4),
        source_columns(count, None, None),
        true,
    );
    let excluded = source(
        11,
        "excluded",
        None,
        None,
        source_columns(count, None, None),
        true,
    );
    let mut scope = Scope::default();
    configure_upsert_scope(&mut scope, &target, &excluded);

    assert_eq!(
        binding_outcome(
            scope.resolve_unqualified(&format!("c{position}"), NamePrecedence::SourcesOnly)
        ),
        BindingOutcome::Column(target.id, position)
    );
    assert_eq!(
        binding_outcome(scope.resolve_qualified("excluded", &format!("c{position}"))),
        BindingOutcome::Column(excluded.id, position)
    );
    assert_eq!(
        binding_outcome(scope.resolve_qualified("dst", &format!("c{position}"))),
        BindingOutcome::Column(target.id, position)
    );
    assert!(scope.missing_qualified_name_is_column());
}

// Example: `UPDATE items AS dst SET ... RETURNING items.c1` uses the schema
// name `items`; the write alias `dst` is not part of RETURNING's namespace.
#[hegel::test]
fn returning_and_conflict_targets_use_the_schema_table_name(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let position = generated_position(&tc, count);
    let target = source(
        3,
        "items",
        Some("dst"),
        Some(4),
        source_columns(count, None, None),
        true,
    );
    let mut scope = Scope::default();
    add_schema_named_target(&mut scope, &target);

    assert_eq!(
        binding_outcome(scope.resolve_qualified("items", &format!("c{position}"))),
        BindingOutcome::Column(target.id, position)
    );
    assert_eq!(
        binding_outcome(scope.resolve_qualified("dst", &format!("c{position}"))),
        BindingOutcome::Missing
    );
    assert_eq!(
        binding_outcome(scope.resolve_database_qualified(
            DatabaseId::new(4),
            "items",
            &format!("c{position}")
        )),
        BindingOutcome::Column(target.id, position)
    );
}

// Example: `UPDATE items SET c0 = c1, c1 = c0` resolves both right-hand sides
// against the same pre-update row, regardless of assignment traversal order.
#[hegel::test]
fn assignment_lookup_order_cannot_change_pre_update_bindings(tc: hegel::TestCase) {
    let count = generated_count(&tc).max(2);
    let target = source(
        19,
        "items",
        None,
        None,
        source_columns(count, None, None),
        true,
    );
    let mut scope = Scope::default();
    configure_target_read_scope(&mut scope, &target);
    let lookup_count = generated_count(&tc);
    let positions = (0..lookup_count)
        .map(|_| generated_position(&tc, count))
        .collect::<Vec<_>>();
    let resolve = |position| {
        binding_outcome(
            scope.resolve_unqualified(&format!("c{position}"), NamePrecedence::SourcesOnly),
        )
    };
    let forward = positions.iter().copied().map(resolve).collect::<Vec<_>>();
    let mut reverse = positions
        .iter()
        .rev()
        .copied()
        .map(resolve)
        .collect::<Vec<_>>();
    reverse.reverse();

    assert_eq!(forward, reverse);
    for (binding, position) in forward.into_iter().zip(positions) {
        assert_eq!(binding, BindingOutcome::Column(target.id, position));
    }
}

// Example: `INSERT INTO items(c2) VALUES (1)`, `UPDATE items SET c2 = c2`,
// and `DELETE FROM items WHERE c2` produce closed HIR with target position two;
// changing that position to the table width must make validation fail.
#[hegel::test]
fn dml_hir_closes_over_target_and_expression_positions(tc: hegel::TestCase) {
    let width = generated_count(&tc);
    let position = generated_position(&tc, width);
    let operation = match tc.draw(generators::integers::<u8>().max_value(2)) {
        0 => GeneratedDml::Insert,
        1 => GeneratedDml::Update,
        _ => GeneratedDml::Delete,
    };
    let mut target = source(
        0,
        "items",
        None,
        Some(0),
        source_columns(width, None, None),
        true,
    );
    target.index_coverage = hir::IndexCoverage::Complete {
        indexes: Vec::new(),
    };
    let target_id = target.id;
    let root = match operation {
        GeneratedDml::Insert => hir::HirRoot::Insert(hir::Insert {
            target: target_id,
            columns: vec![TargetColumn::Column(position)],
            defaults: Vec::new(),
            source: hir::InsertSource::Values(vec![vec![hir::Expr::Literal(ast::Literal::Null)]]),
            conflict: None,
            upserts: Vec::new(),
            excluded_source: None,
            returning: None,
            trigger: None,
            triggers: Vec::new(),
            foreign_keys: hir::DmlForeignKeys::default(),
        }),
        GeneratedDml::Update => hir::HirRoot::Update(hir::Update {
            target: target_id,
            defaults: Vec::new(),
            from: None,
            assignments: vec![hir::Assignment {
                columns: vec![TargetColumn::Column(position)],
                value: hir::Expr::column(target_id, position),
            }],
            predicate: None,
            order_by: Vec::new(),
            limit: None,
            conflict: None,
            returning: None,
            trigger: None,
            triggers: Vec::new(),
            foreign_keys: hir::DmlForeignKeys::default(),
        }),
        GeneratedDml::Delete => hir::HirRoot::Delete(hir::Delete {
            target: target_id,
            predicate: Some(hir::Expr::column(target_id, position)),
            order_by: Vec::new(),
            limit: None,
            returning: None,
            trigger: None,
            triggers: Vec::new(),
            foreign_keys: hir::DmlForeignKeys::default(),
        }),
    };
    let mut document = hir::HirDocument {
        snapshot: hir::CatalogSnapshot::from_id(1),
        databases: vec![hir::DatabaseSnapshot {
            database: hir::DatabaseId::new(0),
            schema_version: 0,
        }],
        root,
        queries: Vec::new(),
        sources: vec![target],
        ctes: Vec::new(),
        schema_programs: Vec::new(),
    };

    document.validate().expect("generated DML HIR is closed");

    match &mut document.root {
        hir::HirRoot::Insert(insert) => insert.columns[0] = TargetColumn::Column(width),
        hir::HirRoot::Update(update) => {
            update.assignments[0].columns[0] = TargetColumn::Column(width)
        }
        hir::HirRoot::Delete(delete) => {
            delete.predicate = Some(hir::Expr::column(target_id, width))
        }
        _ => unreachable!("the generator creates a DML root"),
    }
    assert!(document.validate().is_err());
}
