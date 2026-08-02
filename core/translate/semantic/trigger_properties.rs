//! Property tests for trigger row images, conflict policy, and database rules.

use hegel::generators;
use turso_parser::ast::ResolveType;

use super::{
    binding_outcome,
    hir::{
        self, CatalogObject, CatalogObjectId, CatalogSnapshot, DatabaseId, PseudoSource, Source,
        SourceId, SourceKind, TriggerEnvironment,
    },
    scope::{PseudoSourceVisibility, QueryEnvironment, Scope},
    source, source_columns,
    trigger_rules::{
        apply_pseudo_column_affinity, database_reference_allowed, default_database,
        effective_conflict_policy, query_environment, restricts_database_references,
    },
    BindingOutcome,
};
use crate::{
    schema::{BTreeTable, Table},
    sync::Arc,
    MAIN_DB_ID, TEMP_DB_ID,
};

fn generated_count(tc: &hegel::TestCase) -> usize {
    usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1
}

fn generated_position(tc: &hegel::TestCase, len: usize) -> usize {
    tc.draw(generators::integers::<usize>().max_value(len - 1))
}

fn resolved_table() -> super::hir::ResolvedTable {
    resolved_table_with_id(41)
}

fn resolved_table_with_id(object_id: u64) -> super::hir::ResolvedTable {
    let table = BTreeTable::from_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", 2)
        .expect("fixed trigger table is valid");
    CatalogObject::new(
        CatalogObjectId::new(object_id),
        CatalogSnapshot::from_id(9),
        Some(DatabaseId::new(MAIN_DB_ID)),
        Arc::new(Table::BTree(Arc::new(table))),
    )
}

fn trigger_environment(new_visible: bool, old_visible: bool) -> TriggerEnvironment {
    TriggerEnvironment {
        table: resolved_table(),
        new_source: new_visible.then(|| SourceId::new(7)),
        old_source: old_visible.then(|| SourceId::new(13)),
    }
}

fn pseudo_source(id: usize, kind: PseudoSource, count: usize) -> Source {
    let mut source = source(
        id,
        match kind {
            PseudoSource::New => "new",
            PseudoSource::Old => "old",
            PseudoSource::Excluded => "excluded",
        },
        None,
        Some(MAIN_DB_ID),
        source_columns(count, None, None),
        true,
    );
    source.kind = SourceKind::Pseudo {
        kind,
        table: resolved_table(),
    };
    source
}

fn trigger_document(new_visible: bool, old_visible: bool) -> hir::HirDocument {
    let table = resolved_table();
    let mut sources = Vec::new();
    let new_source = new_visible.then(|| {
        let id = sources.len();
        sources.push(pseudo_source(id, PseudoSource::New, 2));
        SourceId::new(id)
    });
    let old_source = old_visible.then(|| {
        let id = sources.len();
        sources.push(pseudo_source(id, PseudoSource::Old, 2));
        SourceId::new(id)
    });
    hir::HirDocument {
        snapshot: CatalogSnapshot::from_id(9),
        root: hir::HirRoot::TriggerPredicate(hir::TriggerPredicate {
            expression: hir::Expr::Literal(turso_parser::ast::Literal::Null),
            environment: TriggerEnvironment {
                table,
                new_source,
                old_source,
            },
        }),
        queries: Vec::new(),
        sources,
        ctes: Vec::new(),
        schema_programs: Vec::new(),
    }
}

fn expected_visibility(visible: bool, source: SourceId) -> PseudoSourceVisibility {
    if visible {
        PseudoSourceVisibility::Visible(source)
    } else {
        PseudoSourceVisibility::Forbidden("not visible")
    }
}

fn assert_visibility(
    actual: &PseudoSourceVisibility,
    expected: PseudoSourceVisibility,
    forbidden_fragment: &str,
) {
    match (actual, expected) {
        (PseudoSourceVisibility::Visible(actual), PseudoSourceVisibility::Visible(expected)) => {
            assert_eq!(*actual, expected);
        }
        (PseudoSourceVisibility::Forbidden(message), PseudoSourceVisibility::Forbidden(_)) => {
            assert!(message.contains(forbidden_fragment));
        }
        (actual, expected) => {
            panic!("visibility differs: actual={actual:?}, expected={expected:?}")
        }
    }
}

fn generated_conflict_policy(tc: &hegel::TestCase) -> ResolveType {
    const POLICIES: [ResolveType; 5] = [
        ResolveType::Rollback,
        ResolveType::Abort,
        ResolveType::Fail,
        ResolveType::Ignore,
        ResolveType::Replace,
    ];
    POLICIES[generated_position(tc, POLICIES.len())]
}

// Example: an INSERT trigger permits `NEW.c2` and rejects `OLD.c2`; DELETE
// does the inverse, while UPDATE exposes both exact row-image source IDs.
#[hegel::test]
fn trigger_event_environment_records_exact_new_and_old_visibility(tc: hegel::TestCase) {
    let new_visible = tc.draw(generators::booleans());
    let old_visible = tc.draw(generators::booleans());
    let trigger = trigger_environment(new_visible, old_visible);
    let environment = query_environment(&trigger);

    assert_visibility(
        environment.pseudo_sources.state(PseudoSource::New),
        expected_visibility(new_visible, SourceId::new(7)),
        "NEW references",
    );
    assert_visibility(
        environment.pseudo_sources.state(PseudoSource::Old),
        expected_visibility(old_visible, SourceId::new(13)),
        "OLD references",
    );
    assert!(environment.allow_raise);
}

// Example: in `UPDATE items SET value = NEW.c3` inside a trigger, `NEW.c3`
// binds column position three of NEW, never the matching OLD position.
#[hegel::test]
fn trigger_pseudo_names_bind_the_chosen_row_image_and_position(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let position = generated_position(&tc, count);
    let new_visible = tc.draw(generators::booleans());
    let old_visible = tc.draw(generators::booleans());
    let trigger = trigger_environment(new_visible, old_visible);
    let environment = query_environment(&trigger);
    let new_source = pseudo_source(7, PseudoSource::New, count);
    let old_source = pseudo_source(13, PseudoSource::Old, count);
    let mut scope = Scope::default();
    scope
        .add_environment_pseudo_sources(&environment, |id| match id {
            id if id == new_source.id => Some(&new_source),
            id if id == old_source.id => Some(&old_source),
            _ => None,
        })
        .expect("all visible trigger sources exist");

    let new_binding = binding_outcome(scope.resolve_qualified("new", &format!("c{position}")));
    let old_binding = binding_outcome(scope.resolve_qualified("old", &format!("c{position}")));
    assert_eq!(
        new_binding,
        if new_visible {
            BindingOutcome::Column(new_source.id, position)
        } else {
            BindingOutcome::Error
        }
    );
    assert_eq!(
        old_binding,
        if old_visible {
            BindingOutcome::Column(old_source.id, position)
        } else {
            BindingOutcome::Error
        }
    );
}

// Example: `(SELECT NEW.c1)` inside a trigger inherits the outer NEW binding;
// it does not add a second NEW occurrence that would make the name ambiguous.
#[hegel::test]
fn nested_trigger_queries_inherit_one_pseudo_source_identity(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let position = generated_position(&tc, count);
    let trigger = trigger_environment(true, false);
    let environment = query_environment(&trigger);
    let new_source = pseudo_source(7, PseudoSource::New, count);
    let mut outer = Scope::default();
    outer
        .add_environment_pseudo_sources(&environment, |id| {
            (id == new_source.id).then_some(&new_source)
        })
        .expect("NEW source exists");
    let nested_environment = QueryEnvironment::for_subquery(&outer);
    let mut nested = Scope::new(nested_environment.outer.clone());
    nested
        .add_environment_pseudo_sources(&nested_environment, |_| Some(&new_source))
        .expect("nested environment inherits NEW through its outer scope");

    assert_eq!(
        binding_outcome(nested.resolve_qualified("new", &format!("c{position}"))),
        BindingOutcome::Column(new_source.id, position)
    );
}

// Example: `NEW.value` and `OLD.value` behave like register values with no
// column affinity, while an `INTEGER PRIMARY KEY` field keeps rowid affinity.
#[hegel::test]
fn trigger_pseudo_columns_keep_affinity_only_for_rowid_aliases(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let mut columns = source_columns(count, None, None);
    let expected = (0..count)
        .map(|_| tc.draw(generators::booleans()))
        .collect::<Vec<_>>();
    for (column, rowid_alias) in columns.iter_mut().zip(&expected) {
        column.rowid_alias = *rowid_alias;
        column.has_affinity = tc.draw(generators::booleans());
    }

    apply_pseudo_column_affinity(&mut columns);

    assert_eq!(
        columns
            .iter()
            .map(|column| column.has_affinity)
            .collect::<Vec<_>>(),
        expected
    );
}

// Example: an outer `INSERT OR IGNORE` makes a trigger body's `INSERT OR
// REPLACE` use IGNORE; without an outer policy, the local REPLACE is retained.
#[hegel::test]
fn inherited_trigger_conflict_policy_wins_over_the_local_clause(tc: hegel::TestCase) {
    let inherited = tc
        .draw(generators::booleans())
        .then(|| generated_conflict_policy(&tc));
    let local = tc
        .draw(generators::booleans())
        .then(|| generated_conflict_policy(&tc));

    let actual = effective_conflict_policy(inherited, local);

    assert_eq!(actual, inherited.or(local));
}

// Example: a trigger stored in `main` may reference only `main.items`; a TEMP
// trigger may reference `main.items`, `temp.items`, or an attached database.
#[hegel::test]
fn trigger_database_restrictions_match_sqlite_ownership_rules(tc: hegel::TestCase) {
    let attached = usize::from(tc.draw(generators::integers::<u8>())) + 2;
    let trigger_database = if tc.draw(generators::booleans()) {
        TEMP_DB_ID
    } else if tc.draw(generators::booleans()) {
        MAIN_DB_ID
    } else {
        attached
    };
    let referenced_database = match tc.draw(generators::integers::<u8>().max_value(2)) {
        0 => MAIN_DB_ID,
        1 => TEMP_DB_ID,
        _ => attached,
    };

    assert_eq!(
        restricts_database_references(trigger_database),
        trigger_database != TEMP_DB_ID
    );
    assert_eq!(
        database_reference_allowed(trigger_database, referenced_database),
        trigger_database == TEMP_DB_ID || trigger_database == referenced_database
    );
    assert_eq!(
        default_database(trigger_database),
        if trigger_database == TEMP_DB_ID {
            MAIN_DB_ID
        } else {
            trigger_database
        }
    );
}

// Example: HIR for `CREATE TRIGGER ... UPDATE ... SET value = NEW.value`
// closes only when its NEW/OLD IDs point to the matching pseudo row images.
#[hegel::test]
fn trigger_hir_closes_over_visible_row_image_identities(tc: hegel::TestCase) {
    let document = trigger_document(
        tc.draw(generators::booleans()),
        tc.draw(generators::booleans()),
    );

    document
        .validate()
        .expect("generated trigger HIR has matching pseudo-source identities");
}

// Example: `NEW.value` cannot point at an OLD row image or at NEW for a
// different target table, even if the source and column positions are valid.
#[hegel::test]
fn trigger_hir_rejects_wrong_pseudo_kind_or_target_table(tc: hegel::TestCase) {
    let mut document = trigger_document(true, tc.draw(generators::booleans()));
    let SourceKind::Pseudo { kind, table } = &mut document.sources[0].kind else {
        unreachable!("the first generated source is NEW");
    };
    if tc.draw(generators::booleans()) {
        *kind = PseudoSource::Old;
    } else {
        *table = resolved_table_with_id(99);
    }

    assert!(document.validate().is_err());
}
