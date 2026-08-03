//! Properties for stable-rowset UPDATE emission from closed HIR.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    error::SQLITE_CONSTRAINT_FOREIGNKEY,
    schema::{BTreeTable, Index, Schema},
    sync::Arc,
    translate::semantic::{
        analyze,
        context::{DmlPolicy, SemanticContext},
        hir::{Expr, HirRoot, TargetColumn},
        AnalyzeInput,
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts},
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

// Examples: changing `parents.p3` from `old` to `new` with
// `children.c1 REFERENCES parents(p3)` must compare the frozen OLD/NEW parent
// positions before scanning child position one. NO ACTION counts old matches
// and repairs deferred matches after the new row exists; RESTRICT halts before
// the write. Varying both positions proves no column name is rebound here.
#[hegel::test]
fn update_parent_checks_only_a_changed_frozen_key(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(10)));
    let child_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let parent_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let restrict = tc.draw(generators::booleans());
    let deferred = tc.draw(generators::booleans());
    let parent_columns = (0..width)
        .map(|position| format!("p{position} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    let child_columns = (0..width)
        .map(|position| {
            if position == child_position {
                format!(
                    "c{position} TEXT REFERENCES parents(p{parent_position}) ON UPDATE {}{}",
                    if restrict { "RESTRICT" } else { "NO ACTION" },
                    if deferred {
                        " DEFERRABLE INITIALLY DEFERRED"
                    } else {
                        ""
                    }
                )
            } else {
                format!("c{position} TEXT")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parent = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 23)
            .expect("parent table SQL is valid"),
    );
    let child = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 29)
            .expect("child table SQL is valid"),
    );
    let symbols = SymbolTable::new();
    let parent_index = Index::from_sql(
        &symbols,
        &format!("CREATE UNIQUE INDEX parents_key ON parents(p{parent_position})"),
        31,
        &parent,
    )
    .expect("parent unique index SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(parent).expect("parents is unique");
    schema.add_btree_table(child).expect("children is unique");
    schema
        .add_index(Arc::new(parent_index))
        .expect("parents_key is unique");
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect)
        .with_dml_policy(DmlPolicy::new(false, false, false, false, true));
    let statement = parse_statement(&format!("UPDATE parents SET p{parent_position} = 'new'"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated parent UPDATE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed FK UPDATE has a physical plan");
    let mut program = program();
    emit_root_update(&plan, &mut program).expect("parent FK UPDATE emits without a resolver");
    program
        .resolve_labels()
        .expect("all parent FK UPDATE branches are closed");

    let child_read = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::OpenRead {
                    root_page: 29,
                    db: 0,
                    ..
                }
            )
        })
        .expect("the frozen child table is scanned");
    let parent_delete = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Delete { table_name, .. } if table_name == "parents")
        })
        .expect("the old parent row is removed");
    assert!(child_read < parent_delete);
    if restrict {
        assert!(program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::Halt { err_code, .. } if *err_code == SQLITE_CONSTRAINT_FOREIGNKEY)
        }));
    } else {
        assert!(program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::FkCounter { increment_value: 1, deferred: actual } if *actual == deferred)
        }));
        assert_eq!(
            program.insns.iter().any(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::FkCounter {
                        increment_value: -1,
                        deferred: true
                    }
                )
            }),
            deferred
        );
    }
}

// Examples: `UPDATE children SET c3 = 8` removes any deferred violation for
// OLD c3, then checks NEW c3 against the frozen parent rowid; the immediate
// form performs only the NEW probe. Varying both positions proves that OLD and
// NEW row images use HIR offsets rather than rebinding the column names.
#[hegel::test]
fn update_child_foreign_keys_replace_old_counters_before_checking_new(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(10)));
    let child_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let parent_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let deferred = tc.draw(generators::integers::<u8>().max_value(1)) == 1;
    let parent_columns = (0..width)
        .map(|position| {
            if position == parent_position {
                format!("p{position} INTEGER PRIMARY KEY")
            } else {
                format!("p{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let child_columns = (0..width)
        .map(|position| {
            if position == child_position {
                format!(
                    "c{position} INTEGER REFERENCES parents(p{parent_position}){}",
                    if deferred {
                        " DEFERRABLE INITIALLY DEFERRED"
                    } else {
                        ""
                    }
                )
            } else {
                format!("c{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parent = BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 23)
        .expect("parent table SQL is valid");
    let child = BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 29)
        .expect("child table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(parent))
        .expect("parents is unique");
    schema
        .add_btree_table(Arc::new(child))
        .expect("children is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect)
        .with_dml_policy(DmlPolicy::new(false, false, false, false, true));
    let statement = parse_statement(&format!("UPDATE children SET c{child_position} = 8"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated FK update has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed FK UPDATE has a physical plan");
    let mut program = program();
    emit_root_update(&plan, &mut program).expect("child FK UPDATE emits without a resolver");
    program
        .resolve_labels()
        .expect("all FK UPDATE branches are closed");

    let counters = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::FkCounter {
                increment_value,
                deferred: actual,
            } => Some((*increment_value, *actual)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(counters.contains(&(1, deferred)));
    assert_eq!(counters.contains(&(-1, true)), deferred);
}

// Example: for `items(c0, c1, c2 AS (c0 + c1) VIRTUAL)`,
// `UPDATE items SET c0 = c1 + 7 WHERE c0` must first collect every matching
// rowid, then seek each stable rowid, evaluate the assignment against OLD c1,
// and recompute c2 from the NEW c0/c1 registers. No table write may happen
// while the WHERE scan is still adding rowids.
#[hegel::test]
fn update_uses_a_stable_rowset_and_recomputes_the_hir_row(tc: hegel::TestCase) {
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let table = BTreeTable::from_sql(
        "CREATE TABLE items(\
         c0 INTEGER, \
         c1 INTEGER, \
         c2 INTEGER GENERATED ALWAYS AS (c0 + c1) VIRTUAL)",
        12,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("UPDATE items SET c0 = c1 + {offset} WHERE c0"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UPDATE has valid SQL meaning");
    let HirRoot::Update(update) = &document.root else {
        panic!("UPDATE syntax produces an UPDATE HIR root");
    };
    assert!(matches!(
        update.assignments[0].columns.as_slice(),
        [TargetColumn::Column(0)]
    ));
    let Expr::Binary { lhs, .. } = &update.assignments[0].value else {
        panic!("assignment is the resolved addition");
    };
    let Expr::Column(reference) = lhs.as_ref() else {
        panic!("assignment reads one resolved source column");
    };
    assert_eq!(reference.source, update.target);
    assert_eq!(reference.column, 1);
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("UPDATE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all stable-rowset branches are closed");

    let position_of = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .position(|(instruction, _)| predicate(instruction))
            .expect("expected instruction exists")
    };
    let add_to_rowset = position_of(&|instruction| matches!(instruction, Insn::RowSetAdd { .. }));
    let read_rowset = position_of(&|instruction| matches!(instruction, Insn::RowSetRead { .. }));
    let seek_row = position_of(&|instruction| matches!(instruction, Insn::NotExists { .. }));
    let delete = position_of(&|instruction| {
        matches!(
            instruction,
            Insn::Delete {
                is_part_of_update: true,
                ..
            }
        )
    });
    let insert = position_of(
        &|instruction| matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items"),
    );
    assert!(add_to_rowset < read_rowset);
    assert!(read_rowset < seek_row && seek_row < delete && delete < insert);
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
            .count(),
        2,
        "one addition evaluates the assignment and one recomputes c2"
    );
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::MakeRecord { count: 2, .. })));
}

// Examples: `UPDATE items SET rowid = rowid + 7 RETURNING rowid` and
// `UPDATE OR IGNORE items SET id = id + 1` for an INTEGER PRIMARY KEY alias.
// The stable rowset and OLD index deletion keep the original key, while NEW
// constraints, index records, table insertion, triggers, and RETURNING use a
// separate assigned key. A collision is checked before any OLD row is deleted.
#[hegel::test]
fn update_rowid_assignment_keeps_old_and_new_keys_separate(tc: hegel::TestCase) {
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let alias = tc.draw(generators::booleans());
    let table_sql = if alias {
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)"
    } else {
        "CREATE TABLE items(value TEXT)"
    };
    let table = BTreeTable::from_sql(table_sql, 12).expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let key = if alias { "id" } else { "rowid" };
    let statement = parse_statement(&format!(
        "UPDATE OR IGNORE items SET {key} = {key} + {offset} RETURNING {key}"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated rowid UPDATE has valid SQL meaning");
    let HirRoot::Update(update) = &document.root else {
        panic!("fixture produces UPDATE HIR");
    };
    assert!(if alias {
        matches!(
            update.assignments[0].columns.as_slice(),
            [TargetColumn::Column(0)]
        )
    } else {
        matches!(
            update.assignments[0].columns.as_slice(),
            [TargetColumn::RowId]
        )
    });
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed rowid UPDATE has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("rowid assignment emits directly from HIR");
    program
        .resolve_labels()
        .expect("all rowid collision branches are closed");

    let old_rowid = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::RowSetRead { dest_reg, .. } => Some(*dest_reg),
            _ => None,
        })
        .expect("the stable OLD rowid is read from the rowset");
    let (insert_position, new_rowid) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::Insert {
                table_name,
                key_reg,
                ..
            } if table_name == "items" => Some((position, *key_reg)),
            _ => None,
        })
        .expect("the NEW row is inserted");
    assert_ne!(old_rowid, new_rowid);
    let collision_check = program.insns[..insert_position]
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::NotExists { rowid_reg, .. } if *rowid_reg == new_rowid)
        })
        .expect("the assigned key is checked for collision");
    let delete = program.insns[..insert_position]
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Delete {
                    is_part_of_update: true,
                    ..
                }
            )
        })
        .expect("the OLD row is deleted");
    assert!(collision_check < delete && delete < insert_position);
    assert!(program.insns[insert_position + 1..]
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::ResultRow { .. })));
}

// Examples:
// - `UPDATE items SET c1 = incoming.c1 + 7 FROM incoming
//   WHERE items.c0 = incoming.c0` must evaluate the SET value while both exact
//   SourceIds are bound, then save it under the target rowid before any write.
// - If several incoming rows match one item, inserting candidates by target
//   rowid leaves one stable value for the later write phase; FROM cursors are
//   no longer needed when the table and its indexes are changed.
// - RETURNING reads the rebuilt target NEW row, not the materialized FROM row.
#[hegel::test]
fn update_from_materializes_hir_assignment_values_before_writing(tc: hegel::TestCase) {
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let items = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER)", 12)
            .expect("fixture target SQL is valid"),
    );
    let incoming = Arc::new(
        BTreeTable::from_sql("CREATE TABLE incoming(c0 INTEGER, c1 INTEGER)", 13)
            .expect("fixture source SQL is valid"),
    );
    let mut schema = Schema::new();
    schema.add_btree_table(items).expect("items is unique");
    schema
        .add_btree_table(incoming)
        .expect("incoming is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "UPDATE items SET c1 = incoming.c1 + {offset} FROM incoming \
         WHERE items.c0 = incoming.c0 RETURNING c0, c1"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UPDATE FROM has valid SQL meaning");
    let HirRoot::Update(update) = &document.root else {
        panic!("fixture produces UPDATE HIR");
    };
    let from = update.from.as_ref().expect("UPDATE has FROM");
    let mut assignment_sources = Vec::new();
    update.assignments[0].value.walk(&mut |expression| {
        if let Expr::Column(column) = expression {
            assignment_sources.push(column.source);
        }
    });
    assert_eq!(assignment_sources, vec![from.first]);
    assert_ne!(from.first, update.target);
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("UPDATE FROM lowers without a catalog");
    program
        .resolve_labels()
        .expect("all UPDATE FROM branches are closed");

    let candidate_insert = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name.starts_with("update_from_"))
        })
        .expect("FROM values are materialized by target rowid");
    let delete = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Delete {
                    is_part_of_update: true,
                    ..
                }
            )
        })
        .expect("target OLD row is deleted");
    let target_insert = program
        .insns
        .iter()
        .enumerate()
        .skip(delete)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items")
                .then_some(position)
        })
        .expect("target NEW row is inserted");
    let returning = program
        .insns
        .iter()
        .enumerate()
        .skip(target_insert)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::ResultRow { .. }).then_some(position)
        })
        .expect("UPDATE FROM returns target NEW");
    let assignment = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
        .expect("SET expression is evaluated");
    assert!(assignment < candidate_insert && candidate_insert < delete);
    assert!(delete < target_insert && target_insert < returning);
    assert!(!program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::RowSetAdd { .. })));
}

// Example: `UPDATE items SET c1 = incoming.c1 FROM incoming
// WHERE items.c0 = incoming.c0 ORDER BY incoming.c2 DESC LIMIT 2 OFFSET 1`
// must freeze the assignment and sort value while `incoming` is still bound,
// choose the sliced target rowids, and only then write `items`. Changing the
// direction and bounds checks that no FROM expression is rebound in the write
// phase and that LIMIT never applies after a mutation.
#[hegel::test]
fn ordered_update_from_freezes_values_and_selection_before_writing(tc: hegel::TestCase) {
    let descending = tc.draw(generators::booleans());
    let limit = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(8)));
    let offset = usize::from(tc.draw(generators::integers::<u8>().max_value(4)));
    let items = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER)", 12)
            .expect("fixture target SQL is valid"),
    );
    let incoming = Arc::new(
        BTreeTable::from_sql(
            "CREATE TABLE incoming(c0 INTEGER, c1 INTEGER, c2 INTEGER)",
            13,
        )
        .expect("fixture source SQL is valid"),
    );
    let mut schema = Schema::new();
    schema.add_btree_table(items).expect("items is unique");
    schema
        .add_btree_table(incoming)
        .expect("incoming is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let direction = if descending { "DESC" } else { "ASC" };
    let statement = parse_statement(&format!(
        "UPDATE items SET c1 = incoming.c1 FROM incoming \
         WHERE items.c0 = incoming.c0 ORDER BY incoming.c2 {direction} \
         LIMIT {limit} OFFSET {offset}"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("ordered UPDATE FROM has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("ordered UPDATE FROM has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("ordered UPDATE FROM emits without a resolver");
    program
        .resolve_labels()
        .expect("all ordered UPDATE FROM branches are closed");

    let candidate = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name.starts_with("update_from_"))
        })
        .expect("FROM assignment and ordering values are frozen together");
    let sorter = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::SorterOpen { columns: 1, .. }))
        .expect("the frozen FROM ordering opens one sorter key");
    let selected = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name.starts_with("selected_update_from_"))
        })
        .expect("the sliced target rows are materialized");
    let write = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Delete { table_name, .. } if table_name == "items")
        })
        .expect("the target OLD row is removed during UPDATE");
    assert!(candidate < sorter && sorter < selected && selected < write);
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::DecrJumpZero { .. })));
}

// Examples:
// - `DELETE FROM items WHERE EXISTS (SELECT 1 FROM lookup WHERE
//   lookup.c0 = items.c0)` must keep the outer target SourceId live while the
//   nested query scans its own resolved source, freeze every matching rowid,
//   then finish all reads before deleting the first row.
// - `UPDATE items SET c1 = (SELECT lookup.c1 FROM lookup WHERE
//   lookup.c0 = items.c0 LIMIT 1) WHERE EXISTS (...)` must use the same query
//   layer for both the predicate and assignment; neither expression may fall
//   back to a DML-only binder or catalog lookup.
#[hegel::test]
fn dml_subqueries_share_the_closed_hir_query_layer(tc: hegel::TestCase) {
    let update = tc.draw(generators::booleans());
    let offset = i64::from(tc.draw(generators::integers::<u8>().max_value(31)));
    let items = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER)", 12)
            .expect("fixture target SQL is valid"),
    );
    let lookup = Arc::new(
        BTreeTable::from_sql("CREATE TABLE lookup(c0 INTEGER, c1 INTEGER)", 13)
            .expect("fixture lookup SQL is valid"),
    );
    let mut schema = Schema::new();
    schema.add_btree_table(items).expect("items is unique");
    schema.add_btree_table(lookup).expect("lookup is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if update {
        format!(
            "UPDATE items SET c1 = (SELECT lookup.c1 FROM lookup \
             WHERE lookup.c0 = items.c0 LIMIT 1) \
             WHERE EXISTS (SELECT 1 FROM lookup \
             WHERE lookup.c0 = items.c0 AND lookup.c1 > {offset})"
        )
    } else {
        format!(
            "DELETE FROM items WHERE EXISTS (SELECT 1 FROM lookup \
             WHERE lookup.c0 = items.c0 AND lookup.c1 > {offset})"
        )
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("correlated DML subqueries have valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed DML subqueries have a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("DML subqueries emit without a resolver");
    program
        .resolve_labels()
        .expect("all DML subquery branches are closed");

    let lookup_scans = program
        .insns
        .iter()
        .filter(|(instruction, _)| matches!(instruction, Insn::OpenRead { root_page: 13, .. }))
        .count();
    assert_eq!(lookup_scans, if update { 2 } else { 1 });
    if !update {
        let frozen = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Insert { table_name, .. } if table_name == "ordered_dml_rowids")
            })
            .expect("DELETE selection is frozen before mutation");
        let delete = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Delete { table_name, .. } if table_name == "items")
            })
            .expect("the target row is deleted");
        assert!(frozen < delete);
    }
    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(
            instruction,
            Insn::Delete {
                table_name,
                ..
            } if table_name == "items"
        )
    }));
    if update {
        assert!(program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items")
        }));
    }
}

// Examples:
// - `UPDATE items SET c1 = c1 + 7 WHERE c2 > 0 ORDER BY c0 DESC
//   LIMIT 2 OFFSET 1` must first freeze the two selected rowids, then change
//   those exact rows. The write cannot rescan values changed by the UPDATE.
// - `DELETE FROM items WHERE c2 > 0 ORDER BY c0 ASC LIMIT 3 OFFSET 0`
//   follows the same rule: sorting, filtering, and slicing finish before the
//   first target row is deleted.
// - Adding `INDEXED BY items_idx` makes the read phase walk that frozen index
//   and defer-seek the target table, while the write phase still consumes only
//   the materialized rowids.
// Varying statement kind, direction, limit, and offset checks that both DML
// roots use one closed-HIR selection layer rather than separate binders.
#[hegel::test]
fn ordered_dml_freezes_selected_rowids_before_writing(tc: hegel::TestCase) {
    let update = tc.draw(generators::booleans());
    let forced_index = tc.draw(generators::booleans());
    let descending = tc.draw(generators::booleans());
    let limit = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(8)));
    let offset = usize::from(tc.draw(generators::integers::<u8>().max_value(4)));
    let increment = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let items = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER, c2 INTEGER)", 12)
            .expect("fixture target SQL is valid"),
    );
    let symbols = SymbolTable::new();
    let index = Arc::new(
        Index::from_sql(&symbols, "CREATE INDEX items_idx ON items(c0)", 13, &items)
            .expect("fixture index SQL is valid"),
    );
    let mut schema = Schema::new();
    schema.add_btree_table(items).expect("items is unique");
    schema.add_index(index).expect("items_idx is unique");
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let direction = if descending { "DESC" } else { "ASC" };
    let access = if forced_index {
        " INDEXED BY items_idx"
    } else {
        ""
    };
    let sql = if update {
        format!(
            "UPDATE items{access} SET c1 = c1 + {increment} WHERE c2 > 0 \
             ORDER BY c0 {direction} LIMIT {limit} OFFSET {offset}"
        )
    } else {
        format!(
            "DELETE FROM items{access} WHERE c2 > 0 \
             ORDER BY c0 {direction} LIMIT {limit} OFFSET {offset}"
        )
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("ordered DML has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("ordered DML has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("ordered DML emits without a resolver");
    program
        .resolve_labels()
        .expect("all ordered DML branches are closed");

    let sorter = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::SorterOpen { columns: 1, .. }))
        .expect("ORDER BY opens one HIR sorter key");
    let (materialize, selected_cursor) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::Insert {
                cursor, table_name, ..
            } if table_name == "ordered_dml_rowids" => Some((position, *cursor)),
            _ => None,
        })
        .expect("selected rowids are materialized");
    let write = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Delete { table_name, .. } if table_name == "items")
        })
        .expect("the target is changed");
    let read_selected_rowid = program.insns[materialize + 1..write]
        .iter()
        .any(|(instruction, _)| {
            matches!(instruction, Insn::Column { cursor_id, column: 0, .. } if *cursor_id == selected_cursor)
        });
    assert!(sorter < materialize && materialize < write);
    assert!(read_selected_rowid);
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::DecrJumpZero { .. })));
    assert_eq!(
        program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::OpenRead { root_page: 13, .. })
        }),
        forced_index
    );
    assert_eq!(
        program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::DeferredSeek { .. })),
        forced_index
    );
}

// Example: `UPDATE items SET c1 = (SELECT SUM(c1) FROM items)` evaluates the
// closed, uncorrelated scalar query once. Every target row receives the same
// original sum even though earlier rows have already been written; a query
// such as `... WHERE inner.c0 = items.c0` has captures and must not use Once.
#[hegel::test]
fn uncorrelated_dml_subqueries_run_once_while_correlated_ones_run_per_row(tc: hegel::TestCase) {
    let correlated = tc.draw(generators::booleans());
    let items = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER)", 12)
            .expect("fixture target SQL is valid"),
    );
    let mut schema = Schema::new();
    schema.add_btree_table(items).expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if correlated {
        "UPDATE items SET c1 = (SELECT SUM(nested_items.c1) FROM items AS nested_items WHERE nested_items.c0 = items.c0)"
    } else {
        "UPDATE items SET c1 = (SELECT SUM(nested_items.c1) FROM items AS nested_items)"
    };
    let statement = parse_statement(sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated scalar UPDATE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed scalar UPDATE has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("scalar UPDATE emits without a resolver");
    program
        .resolve_labels()
        .expect("all scalar UPDATE branches are closed");

    assert_eq!(
        program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Once { .. })),
        !correlated
    );
}
