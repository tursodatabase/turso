//! Property tests for the SQL name and position rules implemented by `Scope`.

use super::{cte_bindings, cte_rules, dml_rules, hir, scope, trigger_rules};

#[path = "cte_properties.rs"]
mod cte_properties;

#[path = "dml_properties.rs"]
mod dml_properties;

#[path = "trigger_properties.rs"]
mod trigger_properties;

use hegel::generators;
use turso_parser::ast::Literal;

use self::{
    hir::{
        ColumnReadExpression, ColumnRef, ComparisonComponent, ComparisonSemantics, DatabaseId,
        Expr, IndexCoverage, IndexHint, MergedColumnValue, Output, OutputId, OutputNameKind,
        QueryBlockId, QueryId, Source, SourceColumn, SourceId, SourceKind, SourceOwner, TypeFact,
        UsingColumn,
    },
    scope::{NamePrecedence, Scope},
};
use crate::{schema::Type, vdbe::affinity::Affinity};

fn generated_column_count(tc: &hegel::TestCase) -> usize {
    usize::from(tc.draw(generators::integers::<u8>())) + 1
}

fn generated_position(tc: &hegel::TestCase, len: usize) -> usize {
    tc.draw(generators::integers::<usize>().max_value(len - 1))
}

fn column_name(position: usize) -> String {
    format!("c{position}")
}

fn source_columns(
    count: usize,
    renamed: Option<(usize, &str)>,
    hidden: Option<&[bool]>,
) -> Vec<SourceColumn> {
    (0..count)
        .map(|position| SourceColumn {
            name: renamed
                .filter(|(renamed_position, _)| *renamed_position == position)
                .map_or_else(|| column_name(position), |(_, name)| name.to_string()),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            hidden: hidden.is_some_and(|hidden| hidden[position]),
            rowid_alias: false,
        })
        .collect()
}

fn source(
    id: usize,
    name: &str,
    alias: Option<&str>,
    database: Option<usize>,
    columns: Vec<SourceColumn>,
    rowid_available: bool,
) -> Source {
    let width = columns.len();
    Source {
        id: SourceId::new(id),
        owner: SourceOwner::Root,
        database: database.map(DatabaseId::new),
        name: name.to_string(),
        alias: alias.map(str::to_string),
        kind: SourceKind::SchemaExpression,
        columns,
        generated_expressions: vec![ColumnReadExpression::Absent; width],
        default_expressions: vec![ColumnReadExpression::Absent; width],
        column_type_programs: vec![None; width],
        check_constraints: None,
        rowid_available,
        index_hint: IndexHint::None,
        index_expressions: Vec::new(),
        index_coverage: IndexCoverage::Selective,
        index_method_patterns: Vec::new(),
    }
}

fn output(position: usize, name: String) -> Output {
    output_with_kind(position, name, OutputNameKind::ExplicitAlias)
}

fn output_with_kind(position: usize, name: String, name_kind: OutputNameKind) -> Output {
    let block = QueryBlockId::new(QueryId::new(0), 0);
    Output {
        id: OutputId::query(block, position),
        name,
        expr: Expr::Literal(Literal::Null),
        type_fact: TypeFact::known(Type::Integer),
        affinity: Affinity::Integer,
        schema_affinity: Affinity::Integer,
        has_affinity: true,
        collation: None,
        collation_is_explicit: false,
        name_kind,
    }
}

fn expect_column(expr: Expr, expected_source: SourceId, expected_position: usize) {
    match expr {
        Expr::Column(ColumnRef { source, column }) => {
            assert_eq!(source, expected_source);
            assert_eq!(column, expected_position);
        }
        other => panic!("expected a bound column, got {other:?}"),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BindingOutcome {
    Missing,
    Column(SourceId, usize),
    RowId(SourceId),
    Error,
}

fn binding_outcome(result: crate::Result<Option<scope::ResolvedScopeExpr>>) -> BindingOutcome {
    match result {
        Ok(None) => BindingOutcome::Missing,
        Ok(Some(resolved)) => match resolved.expr {
            Expr::Column(ColumnRef { source, column }) => BindingOutcome::Column(source, column),
            Expr::RowId(source) => BindingOutcome::RowId(source),
            other => panic!("expected a source binding, got {other:?}"),
        },
        Err(_) => BindingOutcome::Error,
    }
}

// Example: `SELECT c2 FROM items` binds `c2` to the third schema column.
#[hegel::test]
fn unqualified_names_bind_to_schema_positions(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let source = source(
        0,
        "items",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let resolved = scope
        .resolve_unqualified(&column_name(position), NamePrecedence::SourcesOnly)
        .expect("generated source namespace is valid")
        .expect("generated column is visible");

    expect_column(resolved.expr, source.id, position);
}

// Example: `SELECT key FROM a, b` is missing, unique, or ambiguous solely from
// the visible `key` columns, independent of source insertion order.
#[hegel::test]
fn unqualified_lookup_agrees_with_a_reference_model(tc: hegel::TestCase) {
    let source_count = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let mut expected_columns = Vec::new();
    let mut sources = Vec::with_capacity(source_count);

    for id in 0..source_count {
        let column_count = generated_column_count(&tc);
        let position = generated_position(&tc, column_count);
        let has_name = tc.draw(generators::booleans());
        let unqualified = tc.draw(generators::booleans());
        let columns = source_columns(column_count, has_name.then_some((position, "needle")), None);
        let source = source(id, &format!("table_{id}"), None, None, columns, false);
        if has_name && unqualified {
            expected_columns.push((source.id, position));
        }
        sources.push((source, unqualified));
    }

    let mut scope = Scope::default();
    for (source, unqualified) in &sources {
        scope.add_source(source, *unqualified);
    }
    let actual = binding_outcome(scope.resolve_unqualified("needle", NamePrecedence::SourcesOnly));
    let expected = match expected_columns.as_slice() {
        [] => BindingOutcome::Missing,
        [(source, position)] => BindingOutcome::Column(*source, *position),
        _ => BindingOutcome::Error,
    };

    assert_eq!(actual, expected);
}

// Example: `SELECT rhs.c2 FROM lhs, rhs` binds position two from `rhs`.
#[hegel::test]
fn qualified_names_bind_to_the_chosen_source_and_position(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let left = source(
        0,
        "left_table",
        Some("left_alias"),
        None,
        source_columns(count, None, None),
        false,
    );
    let right = source(
        1,
        "right_table",
        Some("right_alias"),
        None,
        source_columns(count, None, None),
        false,
    );
    let choose_right = tc.draw(generators::booleans());
    let (qualifier, expected_source) = if choose_right {
        ("right_alias", right.id)
    } else {
        ("left_alias", left.id)
    };
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);

    let resolved = scope
        .resolve_qualified(qualifier, &column_name(position))
        .expect("generated source namespace is valid")
        .expect("qualified column is visible");

    expect_column(resolved.expr, expected_source, position);
}

// Example: `SELECT chosen.key FROM a AS chosen, b` considers only occurrences
// named `chosen`, then reports a missing, unique, or ambiguous column.
#[hegel::test]
fn qualified_lookup_agrees_with_a_reference_model(tc: hegel::TestCase) {
    let source_count = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let mut matching_sources = 0;
    let mut expected_columns = Vec::new();
    let mut sources = Vec::with_capacity(source_count);

    for id in 0..source_count {
        let column_count = generated_column_count(&tc);
        let position = generated_position(&tc, column_count);
        let has_qualifier = tc.draw(generators::booleans());
        let has_column = tc.draw(generators::booleans());
        let alias = if has_qualifier {
            "chosen".to_string()
        } else {
            format!("other_{id}")
        };
        let columns = source_columns(
            column_count,
            has_column.then_some((position, "needle")),
            None,
        );
        let source = source(
            id,
            &format!("table_{id}"),
            Some(&alias),
            None,
            columns,
            false,
        );
        if has_qualifier {
            matching_sources += 1;
            if has_column {
                expected_columns.push((source.id, position));
            }
        }
        sources.push(source);
    }

    let mut scope = Scope::default();
    for source in &sources {
        scope.add_source(source, true);
    }
    let actual = binding_outcome(scope.resolve_qualified("chosen", "needle"));
    let expected = if matching_sources == 0 {
        BindingOutcome::Missing
    } else {
        match expected_columns.as_slice() {
            [(source, position)] => BindingOutcome::Column(*source, *position),
            _ => BindingOutcome::Error,
        }
    };

    assert_eq!(actual, expected);
}

// Example: `SELECT MIXED_ALIAS.C2 FROM items AS MiXeD_Alias` still resolves.
#[hegel::test]
fn identifier_case_does_not_change_binding(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let source = source(
        0,
        "MiXeD_Table",
        Some("MiXeD_Alias"),
        None,
        source_columns(count, None, None),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let resolved = scope
        .resolve_qualified("MIXED_ALIAS", &column_name(position).to_ascii_uppercase())
        .expect("generated source namespace is valid")
        .expect("ASCII case does not hide an identifier");

    expect_column(resolved.expr, source.id, position);
}

// Example: `SELECT id FROM left_table, right_table` is ambiguous when both
// tables expose `id`.
#[hegel::test]
fn duplicate_unqualified_names_are_ambiguous(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let left = source(
        0,
        "left_table",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let right = source(
        1,
        "right_table",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);

    let result = scope.resolve_unqualified(&column_name(position), NamePrecedence::SourcesOnly);

    assert!(
        result.is_err(),
        "duplicate visible columns must be ambiguous"
    );
}

// Example: `SELECT * FROM left_table, right_table` returns visible left columns
// first, then visible right columns, while omitting hidden columns.
#[hegel::test]
fn star_expansion_preserves_source_and_column_order(tc: hegel::TestCase) {
    let left_count = generated_column_count(&tc);
    let right_count = generated_column_count(&tc);
    let left_hidden: Vec<bool> = tc.draw(
        generators::vecs(generators::booleans())
            .min_size(left_count)
            .max_size(left_count),
    );
    let right_hidden: Vec<bool> = tc.draw(
        generators::vecs(generators::booleans())
            .min_size(right_count)
            .max_size(right_count),
    );
    let left = source(
        0,
        "left_table",
        None,
        None,
        source_columns(left_count, None, Some(&left_hidden)),
        false,
    );
    let right = source(
        1,
        "right_table",
        None,
        None,
        source_columns(right_count, None, Some(&right_hidden)),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);

    let expanded = scope
        .expand_star()
        .expect("different table names are not ambiguous");
    let expected = left_hidden
        .iter()
        .enumerate()
        .filter(|(_, hidden)| !**hidden)
        .map(|(position, _)| (left.id, position))
        .chain(
            right_hidden
                .iter()
                .enumerate()
                .filter(|(_, hidden)| !**hidden)
                .map(|(position, _)| (right.id, position)),
        )
        .collect::<Vec<_>>();

    assert_eq!(expanded.len(), expected.len());
    for ((_, expr, ..), (source, position)) in expanded.into_iter().zip(expected) {
        expect_column(expr, source, position);
    }
}

// Example: `SELECT a, b FROM items ORDER BY 2` refers to output `b`.
#[hegel::test]
fn output_ordinals_are_one_based_positions(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let outputs = (0..count)
        .map(|position| output(position, format!("output_{position}")))
        .collect::<Vec<_>>();
    let mut scope = Scope::default();
    scope.set_outputs(&outputs);

    let resolved = scope
        .resolve_output_ordinal(position + 1, "ORDER BY")
        .expect("generated ordinal is in range");

    match resolved.expr {
        Expr::Output(id) => assert_eq!(id, outputs[position].id),
        other => panic!("expected an output reference, got {other:?}"),
    }
}

// Example: when `name` is both a source column and an output alias,
// `GROUP BY name` uses source-first policy while `ORDER BY name` uses output-first policy.
#[hegel::test]
fn source_and_output_precedence_follow_the_clause_policy(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let shared_name = "shared_name";
    let source = source(
        0,
        "items",
        None,
        None,
        source_columns(count, Some((position, shared_name)), None),
        false,
    );
    let outputs = vec![output(0, shared_name.to_string())];
    let mut scope = Scope::default();
    scope.add_source(&source, true);
    scope.set_outputs(&outputs);

    let source_first = scope
        .resolve_unqualified(shared_name, NamePrecedence::SourceThenOutput)
        .expect("one source column is unambiguous")
        .expect("shared name is visible");
    expect_column(source_first.expr, source.id, position);

    let output_first = scope
        .resolve_unqualified(shared_name, NamePrecedence::OutputThenSource)
        .expect("one source column is unambiguous")
        .expect("shared name is visible");
    match output_first.expr {
        Expr::Output(id) => assert_eq!(id, outputs[0].id),
        other => panic!("expected the output alias to win, got {other:?}"),
    }
}

// Example: inside `EXISTS (SELECT c0 FROM inner_table)`, `c0` binds to the
// inner table even when the outer query also has a `c0`.
#[hegel::test]
fn inner_sources_shadow_outer_sources_by_name(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let outer_source = source(
        0,
        "outer_table",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let inner_source = source(
        1,
        "inner_table",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let mut outer = Scope::default();
    outer.add_source(&outer_source, true);
    let mut inner = Scope::new(Some(outer));
    inner.add_source(&inner_source, true);

    let resolved = inner
        .resolve_unqualified(&column_name(position), NamePrecedence::SourcesOnly)
        .expect("inner source is unambiguous")
        .expect("inner column is visible");

    expect_column(resolved.expr, inner_source.id, position);
}

// Example: a correlated subquery with no local `outer_only` column may bind
// `outer_only` from its containing query.
#[hegel::test]
fn missing_inner_names_fall_back_to_the_outer_scope(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let outer_source = source(
        0,
        "outer_table",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let inner_source = source(
        1,
        "inner_table",
        None,
        None,
        source_columns(count, Some((position, "inner_only")), None),
        false,
    );
    let mut outer = Scope::default();
    outer.add_source(&outer_source, true);
    let mut inner = Scope::new(Some(outer));
    inner.add_source(&inner_source, true);

    let resolved = inner
        .resolve_unqualified(&column_name(position), NamePrecedence::SourcesOnly)
        .expect("outer source is unambiguous")
        .expect("outer column is visible to the correlated scope");

    expect_column(resolved.expr, outer_source.id, position);
}

// Example: `SELECT aux.items.c0 FROM main.items, aux.items` selects the `aux`
// occurrence even though both tables have the same name and column positions.
#[hegel::test]
fn database_qualification_selects_the_database_occurrence(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let main = source(
        0,
        "items",
        None,
        Some(0),
        source_columns(count, None, None),
        false,
    );
    let attached = source(
        1,
        "items",
        None,
        Some(2),
        source_columns(count, None, None),
        false,
    );
    let choose_attached = tc.draw(generators::booleans());
    let (database, expected_source) = if choose_attached {
        (DatabaseId::new(2), attached.id)
    } else {
        (DatabaseId::new(0), main.id)
    };
    let mut scope = Scope::default();
    scope.add_source(&main, true);
    scope.add_source(&attached, true);

    let resolved = scope
        .resolve_database_qualified(database, "items", &column_name(position))
        .expect("database and table identity is unambiguous")
        .expect("database-qualified column is visible");

    expect_column(resolved.expr, expected_source, position);
}

// Example: after `FROM items AS renamed`, `renamed.c0` resolves but `items.c0`
// does not.
#[hegel::test]
fn aliases_hide_the_original_table_qualifier(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let source = source(
        0,
        "items",
        Some("renamed"),
        Some(0),
        source_columns(count, None, None),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let original = scope
        .resolve_qualified("items", &column_name(position))
        .expect("missing original qualifier is not an internal error");
    assert!(
        original.is_none(),
        "an alias must hide the original qualifier"
    );

    let aliased = scope
        .resolve_qualified("renamed", &column_name(position))
        .expect("alias is unambiguous")
        .expect("alias exposes the column");
    expect_column(aliased.expr, source.id, position);
}

// Example: `SELECT rowid FROM items` resolves only when `items` has an implicit
// rowid; the same name is absent for a `WITHOUT ROWID` table.
#[hegel::test]
fn rowid_names_bind_only_when_the_source_has_a_rowid(tc: hegel::TestCase) {
    let rowid_available = tc.draw(generators::booleans());
    let spelling = tc.draw(generators::sampled_from(vec!["rowid", "_rowid_", "oid"]));
    let source = source(
        0,
        "items",
        None,
        None,
        source_columns(generated_column_count(&tc), None, None),
        rowid_available,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let resolved = scope
        .resolve_unqualified(spelling, NamePrecedence::SourcesOnly)
        .expect("one source cannot make rowid ambiguous");

    match (rowid_available, resolved.map(|resolved| resolved.expr)) {
        (true, Some(Expr::RowId(id))) => assert_eq!(id, source.id),
        (false, None) => {}
        (_, other) => panic!("unexpected rowid binding: {other:?}"),
    }
}

// Example: `SELECT * FROM left_table JOIN right_table USING (key)` exposes one
// `key` value while retaining the left and right source-column positions.
#[hegel::test]
fn using_keeps_one_value_with_both_source_positions(tc: hegel::TestCase) {
    let left_count = generated_column_count(&tc);
    let right_count = generated_column_count(&tc);
    let left_position = generated_position(&tc, left_count);
    let right_position = generated_position(&tc, right_count);
    let value = tc.draw(generators::sampled_from(vec![
        MergedColumnValue::Left,
        MergedColumnValue::Right,
        MergedColumnValue::Coalesce,
    ]));
    let left = source(
        0,
        "left_table",
        None,
        None,
        source_columns(left_count, Some((left_position, "key")), None),
        false,
    );
    let right = source(
        1,
        "right_table",
        None,
        None,
        source_columns(right_count, Some((right_position, "key")), None),
        false,
    );
    let right_ref = ColumnRef {
        source: right.id,
        column: right_position,
    };
    let using = UsingColumn {
        name: "key".to_string(),
        left: Box::new(Expr::Column(ColumnRef {
            source: left.id,
            column: left_position,
        })),
        right: right_ref,
        value,
        type_fact: TypeFact::known(Type::Integer),
        affinity: Affinity::Integer,
        has_affinity: true,
        collation: None,
        comparison: ComparisonSemantics {
            components: vec![ComparisonComponent {
                affinity: Affinity::Integer,
                collation: None,
                array: false,
            }],
        },
    };
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);
    scope
        .apply_using(&[using])
        .expect("generated USING columns exist on both sides");

    let expanded = scope
        .expand_star()
        .expect("different table names are not ambiguous");
    let merged = expanded
        .iter()
        .filter(|(name, ..)| name == "key")
        .collect::<Vec<_>>();
    assert_eq!(merged.len(), 1, "USING must expose one merged column");
    match &merged[0].1 {
        Expr::MergedColumn(merged) => {
            assert_eq!(merged.right, right_ref);
            assert_eq!(merged.value, value);
            match merged.left.as_ref() {
                Expr::Column(reference) => {
                    assert_eq!(reference.source, left.id);
                    assert_eq!(reference.column, left_position);
                }
                other => panic!("expected the left USING position, got {other:?}"),
            }
        }
        other => panic!("expected one merged USING value, got {other:?}"),
    }
}

// Example: a virtual-table hidden column can be selected as `items.hidden`,
// but `SELECT * FROM items` must not include it.
#[hegel::test]
fn hidden_columns_bind_explicitly_but_do_not_expand(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let mut hidden = vec![false; count];
    hidden[position] = true;
    let source = source(
        0,
        "items",
        None,
        None,
        source_columns(count, None, Some(&hidden)),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let resolved = scope
        .resolve_unqualified(&column_name(position), NamePrecedence::SourcesOnly)
        .expect("one source is unambiguous")
        .expect("hidden columns remain explicitly addressable");
    expect_column(resolved.expr, source.id, position);

    let expanded = scope.expand_star().expect("one source is unambiguous");
    assert!(expanded
        .iter()
        .all(|(name, ..)| name != &column_name(position)));
}

// Example: in a qualifier-only namespace, `items.c0` resolves while bare `c0`
// does not enter unqualified lookup.
#[hegel::test]
fn qualified_only_sources_are_absent_from_unqualified_lookup(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let source = source(
        0,
        "items",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, false);

    let unqualified = scope
        .resolve_unqualified(&column_name(position), NamePrecedence::SourcesOnly)
        .expect("one source is unambiguous");
    assert!(unqualified.is_none());

    let qualified = scope
        .resolve_qualified("items", &column_name(position))
        .expect("one source is unambiguous")
        .expect("the source qualifier exposes its columns");
    expect_column(qualified.expr, source.id, position);
}

// Example: `CREATE TABLE items(rowid TEXT); SELECT rowid FROM items` binds the
// declared column rather than the table's implicit rowid.
#[hegel::test]
fn explicit_rowid_named_columns_shadow_the_implicit_rowid(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let spelling = tc.draw(generators::sampled_from(vec!["rowid", "_rowid_", "oid"]));
    let source = source(
        0,
        "items",
        None,
        None,
        source_columns(count, Some((position, spelling)), None),
        true,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let resolved = scope
        .resolve_unqualified(spelling, NamePrecedence::SourcesOnly)
        .expect("one explicit column is unambiguous")
        .expect("the explicit column is visible");

    expect_column(resolved.expr, source.id, position);
}

// Example: `SELECT rowid FROM left_table, right_table` is ambiguous when both
// tables provide an implicit rowid.
#[hegel::test]
fn multiple_visible_implicit_rowids_are_ambiguous(tc: hegel::TestCase) {
    let spelling = tc.draw(generators::sampled_from(vec!["rowid", "_rowid_", "oid"]));
    let left = source(
        0,
        "left_table",
        None,
        None,
        source_columns(generated_column_count(&tc), None, None),
        true,
    );
    let right = source(
        1,
        "right_table",
        None,
        None,
        source_columns(generated_column_count(&tc), None, None),
        true,
    );
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);

    let resolved = scope.resolve_unqualified(spelling, NamePrecedence::SourcesOnly);

    assert!(resolved.is_err());
}

// Example: with two outputs, `ORDER BY 0` and `ORDER BY 3` are both invalid.
#[hegel::test]
fn output_ordinals_outside_the_one_based_range_are_rejected(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let outputs = (0..count)
        .map(|position| output(position, format!("output_{position}")))
        .collect::<Vec<_>>();
    let ordinal = if tc.draw(generators::booleans()) {
        0
    } else {
        count + usize::from(tc.draw(generators::integers::<u8>())) + 1
    };
    let mut scope = Scope::default();
    scope.set_outputs(&outputs);

    let resolved = scope.resolve_output_ordinal(ordinal, "ORDER BY");

    assert!(resolved.is_err());
}

// Example: in `SELECT x, value AS x FROM items ORDER BY x`, the explicit `AS x`
// output wins over the inferred output name `x`.
#[hegel::test]
fn explicit_output_aliases_win_over_inferred_names(tc: hegel::TestCase) {
    let shared_name = "shared_name";
    let inferred_position = usize::from(tc.draw(generators::integers::<u8>()));
    let explicit_position = inferred_position + 256;
    let outputs = vec![
        output_with_kind(
            inferred_position,
            shared_name.to_string(),
            OutputNameKind::Inferred,
        ),
        output_with_kind(
            explicit_position,
            shared_name.to_string(),
            OutputNameKind::ExplicitAlias,
        ),
    ];
    let mut scope = Scope::default();
    scope.set_outputs(&outputs);

    let resolved = scope
        .resolve_unqualified(shared_name, NamePrecedence::OutputThenSource)
        .expect("outputs do not create source ambiguity")
        .expect("the shared output name is visible");

    match resolved.expr {
        Expr::Output(id) => assert_eq!(id, outputs[1].id),
        other => panic!("expected the explicit output alias, got {other:?}"),
    }
}

// Example: `SELECT left_table.id AS id FROM left_table, right_table ORDER BY id`
// remains ambiguous when both sources expose `id`.
#[hegel::test]
fn output_precedence_does_not_hide_source_ambiguity(tc: hegel::TestCase) {
    let left_count = generated_column_count(&tc);
    let right_count = generated_column_count(&tc);
    let left_position = generated_position(&tc, left_count);
    let right_position = generated_position(&tc, right_count);
    let left = source(
        0,
        "left_table",
        None,
        None,
        source_columns(left_count, Some((left_position, "shared_name")), None),
        false,
    );
    let right = source(
        1,
        "right_table",
        None,
        None,
        source_columns(right_count, Some((right_position, "shared_name")), None),
        false,
    );
    let outputs = vec![output(0, "shared_name".to_string())];
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);
    scope.set_outputs(&outputs);

    let resolved = scope.resolve_unqualified("shared_name", NamePrecedence::OutputThenSource);

    assert!(resolved.is_err());
}

// Example: `SELECT items.* FROM items` keeps the table's schema positions and
// order while omitting hidden columns.
#[hegel::test]
fn table_star_preserves_visible_column_positions(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let hidden: Vec<bool> = tc.draw(
        generators::vecs(generators::booleans())
            .min_size(count)
            .max_size(count),
    );
    let source = source(
        0,
        "MiXeD_Items",
        None,
        None,
        source_columns(count, None, Some(&hidden)),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let expanded = scope
        .expand_table_star("MIXED_ITEMS")
        .expect("one matching range variable is unambiguous");
    let expected_positions = hidden
        .iter()
        .enumerate()
        .filter(|(_, hidden)| !**hidden)
        .map(|(position, _)| position)
        .collect::<Vec<_>>();

    assert_eq!(expanded.len(), expected_positions.len());
    for ((_, expr, ..), position) in expanded.into_iter().zip(expected_positions) {
        expect_column(expr, source.id, position);
    }
}

// Example: `SELECT * FROM main.items, aux.items` expands both occurrences in
// FROM order because their database identities differ.
#[hegel::test]
fn star_allows_the_same_table_name_from_different_databases(tc: hegel::TestCase) {
    let left_count = generated_column_count(&tc);
    let right_count = generated_column_count(&tc);
    let left = source(
        0,
        "items",
        None,
        Some(0),
        source_columns(left_count, None, None),
        false,
    );
    let right = source(
        1,
        "items",
        None,
        Some(1),
        source_columns(right_count, None, None),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);

    let expanded = scope
        .expand_star()
        .expect("database identity distinguishes the table occurrences");
    let expected = (0..left_count)
        .map(|position| (left.id, position))
        .chain((0..right_count).map(|position| (right.id, position)))
        .collect::<Vec<_>>();

    assert_eq!(expanded.len(), expected.len());
    for ((_, expr, ..), (source, position)) in expanded.into_iter().zip(expected) {
        expect_column(expr, source, position);
    }
}

// Example: `SELECT * FROM items, items` rejects the repeated unaliased range
// variable when both occurrences contribute visible columns.
#[hegel::test]
fn star_rejects_repeated_table_occurrences_in_one_database(tc: hegel::TestCase) {
    let left = source(
        0,
        "items",
        None,
        Some(0),
        source_columns(generated_column_count(&tc), None, None),
        false,
    );
    let right = source(
        1,
        "items",
        None,
        Some(0),
        source_columns(generated_column_count(&tc), None, None),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&left, true);
    scope.add_source(&right, true);

    let expanded = scope.expand_star();

    assert!(expanded.is_err());
}

// Example: `left_table NATURAL JOIN right_table` merges names visible on both
// sides in right-table order and ignores hidden columns.
#[hegel::test]
fn natural_join_matches_only_columns_visible_on_both_sides(tc: hegel::TestCase) {
    let left_count = generated_column_count(&tc);
    let right_count = generated_column_count(&tc);
    let left_position = generated_position(&tc, left_count);
    let right_position = generated_position(&tc, right_count);
    let left_hidden = tc.draw(generators::booleans());
    let right_hidden = tc.draw(generators::booleans());
    let mut left_hidden_columns = vec![false; left_count];
    let mut right_hidden_columns = vec![false; right_count];
    left_hidden_columns[left_position] = left_hidden;
    right_hidden_columns[right_position] = right_hidden;
    let left = source(
        0,
        "left_table",
        None,
        None,
        source_columns(
            left_count,
            Some((left_position, "shared_name")),
            Some(&left_hidden_columns),
        ),
        false,
    );
    let right = source(
        1,
        "right_table",
        None,
        None,
        source_columns(
            right_count,
            Some((right_position, "shared_name")),
            Some(&right_hidden_columns),
        ),
        false,
    );
    let mut scope = Scope::default();
    scope.add_source(&left, true);

    let common = scope.natural_common_columns(&right);
    let expected = right
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .filter_map(|right_column| {
            left.columns
                .iter()
                .find(|left_column| {
                    !left_column.hidden
                        && crate::util::normalize_ident(&left_column.name)
                            == crate::util::normalize_ident(&right_column.name)
                })
                .map(|left_column| left_column.name.clone())
        })
        .collect::<Vec<_>>();

    assert_eq!(common, expected);
}

// Example: after entering a non-correlated binding scope, `outer_table.c0`
// must fail instead of leaking through to the removed outer query.
#[hegel::test]
fn removing_outer_scopes_prunes_their_qualifiers(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let outer_source = source(
        0,
        "outer_table",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let inner_source = source(
        1,
        "inner_table",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    let mut outer = Scope::default();
    outer.add_source(&outer_source, true);
    let mut inner = Scope::new(Some(outer));
    inner.add_source(&inner_source, true);

    let pruned = inner.without_outer();

    assert!(
        pruned
            .resolve_qualified("outer_table", &column_name(position))
            .is_err(),
        "a pruned qualifier must not fall through to another namespace"
    );
    let current = pruned
        .resolve_qualified("inner_table", &column_name(position))
        .expect("the current scope is unambiguous")
        .expect("the current scope is preserved");
    expect_column(current.expr, inner_source.id, position);
}

// Example: binding `SELECT c2 FROM items` carries `c2`'s type, affinity, and
// has-affinity flag together with source and column position two.
#[hegel::test]
fn resolved_columns_preserve_their_position_facts(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let storage = tc.draw(generators::sampled_from(vec![
        Type::Null,
        Type::Text,
        Type::Numeric,
        Type::Integer,
        Type::Real,
        Type::Blob,
    ]));
    let affinity = tc.draw(generators::sampled_from(vec![
        Affinity::Blob,
        Affinity::Text,
        Affinity::Numeric,
        Affinity::Integer,
        Affinity::Real,
    ]));
    let has_affinity = tc.draw(generators::booleans());
    let mut columns = source_columns(count, None, None);
    columns[position].type_fact = TypeFact::known(storage);
    columns[position].affinity = affinity;
    columns[position].has_affinity = has_affinity;
    let source = source(0, "items", None, None, columns, false);
    let mut scope = Scope::default();
    scope.add_source(&source, true);

    let resolved = scope
        .resolve_unqualified(&column_name(position), NamePrecedence::SourcesOnly)
        .expect("one source is unambiguous")
        .expect("the generated column is visible");

    expect_column(resolved.expr, source.id, position);
    assert_eq!(resolved.type_fact, TypeFact::known(storage));
    assert_eq!(resolved.affinity, affinity);
    assert_eq!(resolved.has_affinity, has_affinity);
}
