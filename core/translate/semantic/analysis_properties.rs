//! Properties for the public semantic-analysis boundary.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::{
    analyze,
    context::SemanticContext,
    hir::{Expr, HirDocument, HirRoot, JoinConstraint, QueryBlockBody, SourceOwner, SubqueryExpr},
    AnalyzeInput,
};
use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Index, Schema, Sequence, Trigger},
    sync::Arc,
    translate::collate::CollationSeq,
    vdbe::affinity::Affinity,
    LimboError, SymbolTable,
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

fn schema_with_items(width: usize) -> Schema {
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 2)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("generated table has a unique name");
    schema
}

fn semantic_context<'a>(schema: &'a Schema, symbols: &'a SymbolTable) -> SemanticContext<'a> {
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    SemanticContext::for_main_schema_object(schema, symbols, true, dialect)
}

fn root_select_filter(document: &HirDocument) -> &Expr {
    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a SELECT");
    };
    let query = &document.queries[root.query.index()];
    let QueryBlockBody::Select {
        filter: Some(filter),
        ..
    } = &query.blocks[query.first.index].body
    else {
        panic!("the fixture has a WHERE expression");
    };
    filter
}

fn root_select_filter_mut(document: &mut HirDocument) -> &mut Expr {
    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a SELECT");
    };
    let query = &mut document.queries[root.query.index()];
    let QueryBlockBody::Select {
        filter: Some(filter),
        ..
    } = &mut query.blocks[query.first.index].body
    else {
        panic!("the fixture has a WHERE expression");
    };
    filter
}

fn typed_items_schema(types: &[&str]) -> Schema {
    let columns = types
        .iter()
        .enumerate()
        .map(|(position, declared_type)| format!("c{position} {declared_type}"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 2)
        .expect("generated typed table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("generated table has a unique name");
    schema
}

// Example: `SELECT nextval('s')` and `SELECT setval('s', 42, false)` must
// carry `main.__turso_internal_seq_s`, the immutable sequence descriptor, and
// the main schema cookie in HIR. Dropping the prepare-time schema afterwards
// must not make physical lowering repeat a name or catalog lookup.
#[hegel::test]
fn sequence_calls_freeze_every_catalog_fact_needed_by_physical_lowering(tc: hegel::TestCase) {
    let increments = [-7, -1, 1, 7];
    let increment =
        increments[tc.draw(generators::integers::<usize>().max_value(increments.len() - 1))];
    let start = tc.draw(generators::integers::<i64>().min_value(-50).max_value(50));
    let cycle = tc.draw(generators::booleans());
    let is_setval = tc.draw(generators::booleans());
    let sequence = Arc::new(
        Sequence::new(
            "s".to_string(),
            Some(start),
            Some(increment),
            Some(-100),
            Some(100),
            cycle,
        )
        .expect("generated sequence bounds are valid"),
    );
    let backing_name = crate::translate::sequence::sequence_backing_table_name("s");
    let backing = BTreeTable::from_sql(
        &crate::translate::sequence::sequence_backing_table_sql("s"),
        2,
    )
    .expect("the sequence backing-table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(backing))
        .expect("the backing table name is unique");
    schema.sequences.insert("s".to_string(), sequence.clone());
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let sql = if is_setval {
        "SELECT setval('s', 42, false)"
    } else {
        "SELECT nextval('s')"
    };
    let statement = parse_statement(sql);

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the generated sequence call has valid SQL meaning");
    let HirRoot::Query(root) = &document.root else {
        unreachable!("the fixture is a SELECT");
    };
    let query = document.query(root.query).expect("the root query exists");
    let Expr::Function(call) = &query.blocks[query.first.index].outputs[0].expr else {
        panic!("the SELECT output is a resolved function");
    };
    let operation = call
        .sequence_operation
        .as_ref()
        .expect("NEXTVAL and SETVAL carry a resolved sequence operation");
    assert_eq!(operation.database.index(), crate::MAIN_DB_ID);
    assert_eq!(operation.normalized_name, "s");
    let crate::schema::Table::BTree(backing_table) = operation.backing_table.value() else {
        panic!("the frozen sequence backing object is a B-tree table");
    };
    assert_eq!(backing_table.name, backing_name);
    assert!(operation.sqlite_sequence.is_none());
    assert_eq!(operation.sequence.start_value, start);
    assert_eq!(operation.sequence.increment_by, increment);
    assert_eq!(operation.sequence.cycle, cycle);
    assert_eq!(operation.schema_cookie, schema.schema_version);

    drop(context);
    drop(schema);
    document
        .validate()
        .expect("the sequence operation remains closed without the catalog");
}

// Example: with `c4 TEXT` and `c1 INTEGER`, `c4 = 7` uses TEXT affinity,
// while `c4 = c1` uses NUMERIC because both operands have declared affinity
// and one is numeric. The chosen source positions must not affect the rule.
#[hegel::test]
fn binary_comparisons_freeze_affinity_for_the_exact_bound_positions(tc: hegel::TestCase) {
    let declared_types = [
        ("BLOB", Affinity::Blob),
        ("TEXT", Affinity::Text),
        ("NUMERIC", Affinity::Numeric),
        ("INTEGER", Affinity::Integer),
        ("REAL", Affinity::Real),
    ];
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(14))) + 2;
    let lhs_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let rhs_offset = tc.draw(generators::integers::<usize>().max_value(width - 2)) + 1;
    let rhs_position = (lhs_position + rhs_offset) % width;
    let lhs_type = tc.draw(generators::integers::<usize>().max_value(declared_types.len() - 1));
    let rhs_type = tc.draw(generators::integers::<usize>().max_value(declared_types.len() - 1));
    let rhs_is_column = tc.draw(generators::booleans());
    let operator = ["=", "!=", "<", "<=", ">", ">=", "IS"]
        [tc.draw(generators::integers::<usize>().max_value(6))];
    let mut types = vec!["BLOB"; width];
    types[lhs_position] = declared_types[lhs_type].0;
    types[rhs_position] = declared_types[rhs_type].0;
    let schema = typed_items_schema(&types);
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let rhs = if rhs_is_column {
        format!("c{rhs_position}")
    } else {
        "7".to_string()
    };
    let statement = parse_statement(&format!(
        "SELECT 1 FROM items WHERE c{lhs_position} {operator} {rhs}"
    ));

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the generated comparison has valid SQL meaning");
    let Expr::Binary {
        lhs,
        rhs,
        comparison: Some(comparison),
        ..
    } = root_select_filter(&document)
    else {
        panic!("the WHERE expression is a resolved binary comparison");
    };
    assert!(matches!(lhs.as_ref(), Expr::Column(column) if column.column == lhs_position));
    if rhs_is_column {
        assert!(matches!(rhs.as_ref(), Expr::Column(column) if column.column == rhs_position));
    }
    let lhs_affinity = declared_types[lhs_type].1;
    let rhs_affinity = declared_types[rhs_type].1;
    let expected = if rhs_is_column {
        if lhs_affinity.is_numeric() || rhs_affinity.is_numeric() {
            Affinity::Numeric
        } else {
            Affinity::Blob
        }
    } else {
        lhs_affinity
    };
    assert_eq!(comparison.components.len(), 1);
    assert_eq!(comparison.components[0].affinity, expected);
    document.validate().expect("comparison metadata is closed");
}

// Example: `(c3, c0) IN ((1, 'one'), (2, 'two'))` stores two comparisons,
// each with component 0 bound to `c3` and component 1 bound to `c0`. IN uses
// the left component's affinity, rather than deriving it from each list item.
#[hegel::test]
fn row_in_lists_keep_per_position_lhs_affinity(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(14))) + 2;
    let integer_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let text_offset = tc.draw(generators::integers::<usize>().max_value(width - 2)) + 1;
    let text_position = (integer_position + text_offset) % width;
    let mut types = vec!["BLOB"; width];
    types[integer_position] = "INTEGER";
    types[text_position] = "TEXT";
    let schema = typed_items_schema(&types);
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let statement = parse_statement(&format!(
        "SELECT 1 FROM items WHERE (c{integer_position}, c{text_position}) \
         IN ((1, 'one'), (2, 'two'))"
    ));

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the generated row IN-list has valid SQL meaning");
    let Expr::InList {
        lhs,
        values,
        comparisons,
        ..
    } = root_select_filter(&document)
    else {
        panic!("the WHERE expression is a resolved IN list");
    };
    let Expr::Row(lhs_components) = lhs.as_ref() else {
        panic!("the IN left side is a row value");
    };
    assert!(
        matches!(&lhs_components[0], Expr::Column(column) if column.column == integer_position)
    );
    assert!(matches!(&lhs_components[1], Expr::Column(column) if column.column == text_position));
    assert_eq!(comparisons.len(), values.len());
    for comparison in comparisons {
        assert_eq!(comparison.components.len(), 2);
        assert_eq!(comparison.components[0].affinity, Affinity::Integer);
        assert_eq!(comparison.components[1].affinity, Affinity::Text);
    }
    document.validate().expect("row IN metadata is closed");
}

// Examples: `c0 = c1` uses c0's declared NOCASE; `c0 = c1 COLLATE BINARY`
// uses the right explicit BINARY; and `c0 IN (SELECT c1 COLLATE BINARY ...)`
// keeps that explicit origin across the subquery-output boundary.
#[hegel::test]
fn comparison_collation_obeys_explicit_then_left_precedence(tc: hegel::TestCase) {
    let table = BTreeTable::from_sql(
        "CREATE TABLE items(c0 TEXT COLLATE NOCASE, c1 TEXT COLLATE RTRIM)",
        2,
    )
    .expect("the collation fixture is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("the fixture table name is unique");
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let variant = tc.draw(generators::integers::<u8>().max_value(5));
    let (sql, expected) = match variant {
        0 => (
            "SELECT 1 FROM items WHERE c0 COLLATE RTRIM = c1 COLLATE NOCASE",
            CollationSeq::Rtrim,
        ),
        1 => (
            "SELECT 1 FROM items WHERE c0 = c1 COLLATE BINARY",
            CollationSeq::Binary,
        ),
        2 => ("SELECT 1 FROM items WHERE c0 = c1", CollationSeq::NoCase),
        3 => (
            "SELECT 1 FROM items WHERE c0 COLLATE BINARY = c1",
            CollationSeq::Binary,
        ),
        4 => (
            "SELECT 1 FROM items AS o WHERE o.c0 IN (SELECT i.c1 FROM items AS i)",
            CollationSeq::NoCase,
        ),
        _ => (
            "SELECT 1 FROM items AS o WHERE o.c0 IN \
             (SELECT i.c1 COLLATE BINARY FROM items AS i)",
            CollationSeq::Binary,
        ),
    };
    let statement = parse_statement(sql);

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the generated collation comparison has valid SQL meaning");
    let comparison = match root_select_filter(&document) {
        Expr::Binary {
            comparison: Some(comparison),
            ..
        } => comparison,
        Expr::Subquery(SubqueryExpr::In { comparison, .. }) => comparison,
        _ => panic!("the WHERE expression is a comparison"),
    };
    assert_eq!(comparison.components.len(), 1);
    assert_eq!(
        comparison.components[0]
            .collation
            .as_ref()
            .map(|collation| *collation.value()),
        Some(expected)
    );
    document
        .validate()
        .expect("collation origin remains closed in HIR");
}

// Examples: `c2 BETWEEN 1 AND 2`, `CASE c2 WHEN 1 THEN 1 END`, and
// `c2 IN (1, 2)` each carry one comparison record for every runtime compare.
// Clearing any record simulates a stale or partially produced HIR document.
#[hegel::test]
fn every_comparison_form_is_complete_or_hir_validation_rejects_it(tc: hegel::TestCase) {
    let schema = typed_items_schema(&["INTEGER"]);
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let variant = tc.draw(generators::integers::<u8>().max_value(4));
    let sql = match variant {
        0 => "SELECT 1 FROM items WHERE c0 = 1",
        1 => "SELECT 1 FROM items WHERE c0 BETWEEN 1 AND 2",
        2 => "SELECT 1 FROM items WHERE CASE c0 WHEN 1 THEN 1 ELSE 0 END",
        3 => "SELECT 1 FROM items WHERE c0 IN (1, 2)",
        _ => "SELECT 1 FROM items AS o WHERE o.c0 IN (SELECT i.c0 FROM items AS i)",
    };
    let statement = parse_statement(sql);
    let mut document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the generated comparison form has valid SQL meaning");
    document
        .validate()
        .expect("analysis produces complete comparison facts");

    let comparison = match root_select_filter_mut(&mut document) {
        Expr::Binary {
            comparison: Some(comparison),
            ..
        } => comparison,
        Expr::Between {
            start_comparison, ..
        } => start_comparison,
        Expr::Case {
            base_comparisons, ..
        } => &mut base_comparisons[0],
        Expr::InList { comparisons, .. } => &mut comparisons[0],
        Expr::Subquery(SubqueryExpr::In { comparison, .. }) => comparison,
        _ => panic!("the fixture is a comparison form"),
    };
    comparison.components.clear();
    assert!(
        document.validate().is_err(),
        "missing runtime comparison behavior must break HIR closure"
    );
}

// Example: `lhs_items NATURAL JOIN rhs_items` merges `key` at position zero.
// INTEGER on the left and TEXT on the right choose NUMERIC comparison affinity,
// while the left declared NOCASE wins over the right declared RTRIM.
#[hegel::test]
fn using_and_natural_joins_freeze_their_column_comparison(tc: hegel::TestCase) {
    let lhs = BTreeTable::from_sql(
        "CREATE TABLE lhs_items(key INTEGER COLLATE NOCASE, lhs_value TEXT)",
        2,
    )
    .expect("the left join table is valid");
    let rhs = BTreeTable::from_sql(
        "CREATE TABLE rhs_items(key TEXT COLLATE RTRIM, rhs_value TEXT)",
        3,
    )
    .expect("the right join table is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(lhs))
        .expect("the left table name is unique");
    schema
        .add_btree_table(Arc::new(rhs))
        .expect("the right table name is unique");
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let sql = if tc.draw(generators::booleans()) {
        "SELECT key FROM lhs_items JOIN rhs_items USING (key)"
    } else {
        "SELECT key FROM lhs_items NATURAL JOIN rhs_items"
    };
    let statement = parse_statement(sql);

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the generated merged-column join has valid SQL meaning");
    let HirRoot::Query(root) = &document.root else {
        unreachable!("the fixture is a SELECT");
    };
    let from = document.queries[root.query.index()].blocks[0]
        .from
        .as_ref()
        .expect("the query has a FROM clause");
    let columns = match &from.joins[0].constraint {
        JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => columns,
        _ => panic!("the fixture has a merged-column join"),
    };
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].right.column, 0);
    assert_eq!(columns[0].comparison.components.len(), 1);
    assert_eq!(
        columns[0].comparison.components[0].affinity,
        Affinity::Numeric
    );
    assert_eq!(
        columns[0].comparison.components[0]
            .collation
            .as_ref()
            .map(|collation| *collation.value()),
        Some(CollationSeq::NoCase)
    );
    document
        .validate()
        .expect("the merged-column comparison is closed");
}

// Example: `SELECT c3 AS picked, c1 FROM items WHERE c3 >= 0 ORDER BY picked`
// produces a closed document whose output columns still point at positions 3 and 1.
#[hegel::test]
fn successful_analysis_is_closed_positional_and_does_not_mutate_syntax(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let first = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let second = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let schema = schema_with_items(width);
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let statement = parse_statement(&format!(
        "SELECT c{first} AS picked, c{second} FROM items WHERE c{first} >= 0 ORDER BY picked"
    ));
    let original = statement.clone();

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated query has valid SQL meaning");

    assert_eq!(
        statement, original,
        "analysis must not mutate parser syntax"
    );
    document.validate().expect("successful analysis is closed");
    let HirRoot::Query(root) = &document.root else {
        panic!("SELECT analysis produces a query root");
    };
    let block = &document
        .query(root.query)
        .expect("root query exists")
        .blocks[0];
    let Expr::Column(first_output) = &block.outputs[0].expr else {
        panic!("first generated output is a source column");
    };
    let Expr::Column(second_output) = &block.outputs[1].expr else {
        panic!("second generated output is a source column");
    };
    assert_eq!(first_output.column, first);
    assert_eq!(second_output.column, second);
    assert_eq!(first_output.source, second_output.source);
}

// Example: neither an old `Expr::Column { .. }` nor a DBSP `Expr::Register(7)`
// may enter through `semantic::analyze`; both must be rejected at the boundary.
#[hegel::test]
fn bound_and_runtime_parser_nodes_are_rejected(tc: hegel::TestCase) {
    let mut statement = parse_statement("SELECT 1");
    let ast::Stmt::Select(select) = &mut statement else {
        unreachable!("the fixture is a SELECT");
    };
    let ast::OneSelect::Select { columns, .. } = &mut select.body.select else {
        unreachable!("the fixture is a SELECT core");
    };
    let forbidden = if tc.draw(generators::booleans()) {
        ast::Expr::Register(7)
    } else {
        ast::Expr::Column {
            database: None,
            table: 1usize.into(),
            column: 0,
            is_rowid_alias: false,
        }
    };
    columns[0] = ast::ResultColumn::Expr(Box::new(forbidden), None);
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);

    let error = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect_err("bound and runtime parser nodes are not semantic input");

    assert!(
        matches!(error, LimboError::InternalError(message) if message.contains("bound or runtime parser expression"))
    );
}

// Example: in `SELECT (SELECT i.c2 FROM items i WHERE i.c2 = o.c4)
// FROM items o`, the inner query has the outer query as its lexical parent and
// captures only `o`; it does not capture its own `i` source.
#[hegel::test]
fn correlated_queries_record_exact_parents_captures_and_schema_versions(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let inner_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let outer_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let schema_version = u32::from(tc.draw(generators::integers::<u16>()));
    let mut schema = schema_with_items(width);
    schema.schema_version = schema_version;
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let statement = parse_statement(&format!(
        "SELECT (SELECT i.c{inner_position} FROM items AS i \
         WHERE i.c{inner_position} = o.c{outer_position}) FROM items AS o"
    ));

    let mut document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated correlated query has valid SQL meaning");
    let HirRoot::Query(root) = &document.root else {
        panic!("SELECT analysis produces a query root");
    };
    let root_query = root.query;
    let nested = document
        .queries
        .iter()
        .find(|query| query.parent == Some(root_query))
        .expect("the scalar subquery records its lexical parent");
    assert_eq!(nested.captures.len(), 1);
    let captured = nested.captures[0];
    assert!(matches!(
        document.source(captured).map(|source| source.owner),
        Some(SourceOwner::QueryBlock(block)) if block.query == root_query
    ));
    assert_eq!(document.databases.len(), 1);
    assert_eq!(document.databases[0].schema_version, schema_version);

    let nested_index = nested.id.index();
    if tc.draw(generators::booleans()) {
        document.queries[nested_index].captures.clear();
    } else {
        document.queries[nested_index].parent = None;
    }
    assert!(
        document.validate().is_err(),
        "removing either the parent or exact capture must break closure"
    );
}

// Example: `CREATE TRIGGER tr UPDATE OF c3 ON items` is attached to
// `UPDATE items SET c3 = 1`, but not `SET c2 = 1`; INSERT and DELETE triggers
// likewise appear only on their matching DML root.
#[hegel::test]
fn dml_analysis_carries_only_triggers_matching_event_and_column_positions(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let trigger_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let written_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let operation = tc.draw(generators::integers::<u8>().max_value(2));
    let (event, sql, expected) = match operation {
        0 => (
            ast::TriggerEvent::Insert,
            "INSERT INTO items(c0) VALUES (1)".to_string(),
            true,
        ),
        1 => (
            ast::TriggerEvent::Delete,
            "DELETE FROM items".to_string(),
            true,
        ),
        _ => (
            ast::TriggerEvent::UpdateOf(vec![ast::Name::from_string(&format!(
                "c{trigger_position}"
            ))]),
            format!("UPDATE items SET c{written_position} = 1"),
            trigger_position == written_position,
        ),
    };
    let mut schema = schema_with_items(width);
    schema
        .add_trigger(
            Trigger::new(
                "tr".to_string(),
                "generated trigger".to_string(),
                "items".to_string(),
                Some(ast::TriggerTime::Before),
                event,
                true,
                None,
                Vec::new(),
                false,
                None,
            ),
            "items",
        )
        .expect("generated trigger name is unique");
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let statement = parse_statement(&sql);

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DML has valid SQL meaning");
    let trigger_count = match &document.root {
        HirRoot::Insert(insert) => insert.triggers.len(),
        HirRoot::Update(update) => update.triggers.len(),
        HirRoot::Delete(delete) => delete.triggers.len(),
        _ => unreachable!("the generator emits DML"),
    };
    assert_eq!(trigger_count, usize::from(expected));
}

// Example: `INSERT INTO children(c2) VALUES (1)` carries the resolved
// `children.c2 -> parents.p3` child check, while `DELETE FROM parents` carries
// the same constraint as an incoming parent check. Both sides use schema
// positions, and `p3 INTEGER PRIMARY KEY` is recorded as a rowid lookup.
#[hegel::test]
fn dml_analysis_freezes_foreign_key_direction_positions_and_rowid_lookup(tc: hegel::TestCase) {
    let child_width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let parent_width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let child_position = tc.draw(generators::integers::<usize>().max_value(child_width - 1));
    let parent_position = tc.draw(generators::integers::<usize>().max_value(parent_width - 1));
    let child_columns = (0..child_width)
        .map(|position| {
            if position == child_position {
                format!("c{position} INTEGER REFERENCES parents(p{parent_position})")
            } else {
                format!("c{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parent_columns = (0..parent_width)
        .map(|position| {
            if position == parent_position {
                format!("p{position} INTEGER PRIMARY KEY")
            } else {
                format!("p{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parent = BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 2)
        .expect("generated parent table is valid");
    let child = BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 3)
        .expect("generated child table is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(parent))
        .expect("parent table name is unique");
    schema
        .add_btree_table(Arc::new(child))
        .expect("child table name is unique");
    let target_is_child = tc.draw(generators::booleans());
    let sql = if target_is_child {
        format!("INSERT INTO children(c{child_position}) VALUES (1)")
    } else {
        "DELETE FROM parents".to_string()
    };
    let symbols = SymbolTable::new();
    let context = semantic_context(&schema, &symbols);
    let statement = parse_statement(&sql);

    let mut document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated foreign-key DML has valid SQL meaning");
    let foreign_keys = match &document.root {
        HirRoot::Insert(insert) => &insert.foreign_keys,
        HirRoot::Delete(delete) => &delete.foreign_keys,
        _ => unreachable!("the generator emits INSERT or DELETE"),
    };
    let foreign_key = if target_is_child {
        assert_eq!(foreign_keys.outgoing.len(), 1);
        assert!(foreign_keys.incoming.is_empty());
        &foreign_keys.outgoing[0]
    } else {
        assert!(foreign_keys.outgoing.is_empty());
        assert_eq!(foreign_keys.incoming.len(), 1);
        &foreign_keys.incoming[0]
    };
    assert_eq!(foreign_key.child_positions.as_ref(), [child_position]);
    assert_eq!(foreign_key.parent_positions.as_ref(), [parent_position]);
    assert_eq!(
        foreign_key.parent_columns.as_ref(),
        [format!("p{parent_position}")]
    );
    assert!(foreign_key.parent_uses_rowid);
    assert!(foreign_key.parent_unique_index.is_none());
    document
        .validate()
        .expect("resolved foreign-key facts close the HIR document");

    let foreign_keys = match &mut document.root {
        HirRoot::Insert(insert) => &mut insert.foreign_keys,
        HirRoot::Delete(delete) => &mut delete.foreign_keys,
        _ => unreachable!("the generator emits INSERT or DELETE"),
    };
    if target_is_child {
        foreign_keys.outgoing[0].child_positions[0] = child_width;
    } else {
        foreign_keys.incoming[0].parent_positions[0] = parent_width;
    }
    assert!(
        document.validate().is_err(),
        "an out-of-range resolved FK position must break HIR closure"
    );
}

// Example: `CREATE UNIQUE INDEX parent_key ON parents(p4)` makes
// `children.c1 REFERENCES parents(p4)` carry that exact index handle. Removing
// the handle from an outgoing child check must make the HIR invalid.
#[hegel::test]
fn outgoing_non_rowid_foreign_keys_carry_the_exact_parent_unique_index(tc: hegel::TestCase) {
    let child_width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let parent_width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let child_position = tc.draw(generators::integers::<usize>().max_value(child_width - 1));
    let parent_position = tc.draw(generators::integers::<usize>().max_value(parent_width - 1));
    let child_columns = (0..child_width)
        .map(|position| {
            if position == child_position {
                format!("c{position} TEXT REFERENCES parents(p{parent_position})")
            } else {
                format!("c{position} TEXT")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parent_columns = (0..parent_width)
        .map(|position| format!("p{position} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    let symbols = SymbolTable::new();
    let parent = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 2)
            .expect("generated parent table is valid"),
    );
    let parent_key = Index::from_sql(
        &symbols,
        &format!("CREATE UNIQUE INDEX parent_key ON parents(p{parent_position})"),
        4,
        &parent,
    )
    .expect("generated parent key index is valid");
    let child = BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 3)
        .expect("generated child table is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(parent)
        .expect("parent table name is unique");
    schema
        .add_btree_table(Arc::new(child))
        .expect("child table name is unique");
    schema
        .add_index(Arc::new(parent_key))
        .expect("parent index name is unique");
    let context = semantic_context(&schema, &symbols);
    let statement = parse_statement(&format!(
        "INSERT INTO children(c{child_position}) VALUES ('key')"
    ));

    let mut document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the generated parent key satisfies the foreign key");
    let HirRoot::Insert(insert) = &document.root else {
        unreachable!("the fixture is an INSERT");
    };
    let foreign_key = &insert.foreign_keys.outgoing[0];
    assert!(!foreign_key.parent_uses_rowid);
    let parent_index = foreign_key
        .parent_unique_index
        .as_ref()
        .expect("a non-rowid parent key carries its UNIQUE index");
    assert_eq!(parent_index.value().name, "parent_key");
    assert_eq!(
        parent_index.value().columns[0].pos_in_table,
        parent_position
    );
    document
        .validate()
        .expect("the exact parent index closes the HIR document");

    let HirRoot::Insert(insert) = &mut document.root else {
        unreachable!("the fixture is an INSERT");
    };
    insert.foreign_keys.outgoing[0].parent_unique_index = None;
    assert!(
        document.validate().is_err(),
        "an outgoing non-rowid FK without its parent index is incomplete"
    );
}

// Example: `INSERT INTO items(c0) VALUES (1)` on a table with generated `c1`,
// defaulted `c2`, `CHECK(c0 >= 0)`, and
// `CREATE INDEX items_expr ON items(c0 + 1) WHERE c0 > 0` closes every program
// needed to build and validate the new row. Removing any one fact is invalid.
#[hegel::test]
fn dml_analysis_closes_generated_default_check_and_index_programs(tc: hegel::TestCase) {
    let symbols = SymbolTable::new();
    let table = Arc::new(
        BTreeTable::from_sql(
            "CREATE TABLE items(\
             c0 INTEGER, \
             c1 INTEGER AS (c0 + 1) VIRTUAL, \
             c2 INTEGER DEFAULT 7, \
             CHECK(c0 >= 0))",
            2,
        )
        .expect("the stored-expression table is valid"),
    );
    let index = Index::from_sql(
        &symbols,
        "CREATE INDEX items_expr ON items(c0 + 1) WHERE c0 > 0",
        3,
        &table,
    )
    .expect("the expression index is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(table)
        .expect("the table name is unique");
    schema
        .add_index(Arc::new(index))
        .expect("the index name is unique");
    let context = semantic_context(&schema, &symbols);
    let statement = parse_statement("INSERT INTO items(c0) VALUES (1)");

    let mut document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("the DML stored programs have valid SQL meaning");
    let HirRoot::Insert(insert) = &document.root else {
        unreachable!("the fixture is an INSERT");
    };
    let source = document
        .source(insert.target)
        .expect("the INSERT target source exists");
    assert!(matches!(
        &source.index_coverage,
        super::hir::IndexCoverage::Complete { .. }
    ));
    assert_eq!(source.check_constraints.as_ref().map(Vec::len), Some(1));
    assert!(matches!(
        source.generated_expressions[1],
        super::hir::ColumnReadExpression::Planned(_)
    ));
    assert!(matches!(
        source.default_expressions[2],
        super::hir::ColumnReadExpression::Planned(_)
    ));
    assert_eq!(source.index_expressions.len(), 1);
    assert!(source.index_expressions[0].columns[0].is_some());
    assert!(source.index_expressions[0].predicate.is_some());
    document
        .validate()
        .expect("all required DML programs close the document");

    let target = match &document.root {
        HirRoot::Insert(insert) => insert.target,
        _ => unreachable!("the fixture is an INSERT"),
    };
    let source = &mut document.sources[target.index()];
    match tc.draw(generators::integers::<u8>().max_value(5)) {
        0 => source
            .check_constraints
            .as_mut()
            .expect("CHECK enforcement is active")
            .clear(),
        1 => source.index_coverage = super::hir::IndexCoverage::Selective,
        2 => source.index_expressions[0].columns[0] = None,
        3 => source.generated_expressions[1] = super::hir::ColumnReadExpression::NotRequired,
        4 => source.default_expressions[2] = super::hir::ColumnReadExpression::NotRequired,
        _ => source.index_expressions.clear(),
    }
    assert!(
        document.validate().is_err(),
        "removing one required stored program must break HIR closure"
    );
}
