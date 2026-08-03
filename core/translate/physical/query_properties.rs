//! Properties for the catalog-free physical query boundary.

use hegel::generators;
use turso_parser::{
    ast::{self, CompoundOperator},
    parser::Parser,
};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Index, Schema},
    sync::{Arc, RwLock},
    translate::{
        collate::CollationSeq,
        semantic::{
            analyze,
            context::SemanticContext,
            hir::{Expr, FunctionEvaluation, HirRoot, QueryBlockBody},
            AnalyzeInput,
        },
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder, ProgramBuilderOpts},
        insn::Insn,
        BranchOffset,
    },
    vtab::{InternalVirtualTable, InternalVirtualTableCursor, VirtualTable},
    Connection, LimboError, QueryMode, SymbolTable, Value,
};

#[derive(Debug)]
struct PositionArgsTableFunction;

impl InternalVirtualTable for PositionArgsTableFunction {
    fn name(&self) -> String {
        "position_args".to_string()
    }

    fn open(
        &self,
        _conn: Arc<Connection>,
    ) -> crate::Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(EmptyTableFunctionCursor)))
    }

    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        _order_by: &[turso_ext::OrderByInfo],
    ) -> std::result::Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
        let idx_num = constraints.iter().fold(0, |mask, constraint| {
            mask | (1 << (constraint.column_index - 1))
        });
        Ok(turso_ext::IndexInfo {
            idx_num,
            idx_str: Some(idx_num.to_string()),
            constraint_usages: constraints
                .iter()
                .enumerate()
                .map(|(position, _)| turso_ext::ConstraintUsage {
                    argv_index: Some(position as u32 + 1),
                    omit: true,
                })
                .collect(),
            ..Default::default()
        })
    }

    fn sql(&self) -> String {
        "CREATE TABLE position_args(\
         value INTEGER, first INTEGER HIDDEN, second INTEGER HIDDEN, third INTEGER HIDDEN)"
            .to_string()
    }
}

struct EmptyTableFunctionCursor;

impl InternalVirtualTableCursor for EmptyTableFunctionCursor {
    fn next(&mut self) -> Result<bool, LimboError> {
        Ok(false)
    }

    fn rowid(&self) -> i64 {
        0
    }

    fn column(&self, _column: usize) -> Result<Value, LimboError> {
        Ok(Value::Null)
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<String>,
        _idx_num: i32,
    ) -> Result<bool, LimboError> {
        Ok(false)
    }
}

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

// Example: after binding
// `SELECT c7, c7 + 1 FROM items WHERE c2 >= ?1`, physical emission must open
// the resolved `items` table, read positions `[2, 7, 7]`, reject false or
// NULL filters, and loop that exact cursor. Dropping `Schema` first proves
// that no table, column, type, collation, or parameter name is resolved again.
#[hegel::test]
fn a_root_table_scan_emits_only_from_closed_hir(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let filter_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let output_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT c{output_position}, c{output_position} + 1 \
         FROM items WHERE c{filter_position} >= ?1"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("scan-first query lowers without a catalog");
    program
        .resolve_labels()
        .expect("all direct-emission branches are closed");

    let opened = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 7,
                db: 0,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("resolved items table is opened directly");
    let column_positions = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } if *cursor_id == opened => Some(*column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        column_positions,
        [filter_position, output_position, output_position]
    );

    let rewind = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Rewind { cursor_id, .. } if *cursor_id == opened)
        })
        .expect("B-tree scan starts with Rewind");
    let next = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Next { cursor_id, .. } if *cursor_id == opened)
        })
        .expect("B-tree scan advances with Next");
    let close = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Close { cursor_id } if *cursor_id == opened)
        })
        .expect("resolved cursor is closed");

    assert!(matches!(
        &program.insns[rewind].0,
        Insn::Rewind {
            pc_if_empty: BranchOffset::Offset(target),
            ..
        } if *target as usize == close
    ));
    assert!(matches!(
        &program.insns[next].0,
        Insn::Next {
            pc_if_next: BranchOffset::Offset(target),
            ..
        } if *target as usize == rewind + 1
    ));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::IfNot {
            target_pc: BranchOffset::Offset(target),
            jump_if_null: true,
            ..
        } if *target as usize == next
    )));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Ge { flags, .. } if flags.get_affinity() == Affinity::Integer
    )));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. })));
}

// Examples: `position_args(?1, ?2)` and `position_args WHERE second = ?2 AND
// first = ?1` both bind arguments to the hidden first and second columns in
// schema order. With `... AND first = ?3`, the first equality is passed to
// `VFilter` and the duplicate stays in the HIR filter. Physical lowering must
// keep the filter argument order and the residual predicate after the catalog
// is dropped.
#[hegel::test]
fn table_function_arguments_keep_their_bound_hidden_column_order(tc: hegel::TestCase) {
    let arity = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(3)));
    let arguments = (1..=arity)
        .map(|position| format!("?{position}"))
        .collect::<Vec<_>>()
        .join(", ");
    let via_where = tc.draw(generators::booleans());
    let duplicate = via_where && tc.draw(generators::booleans());
    let mut schema = Schema::new();
    schema
        .add_virtual_table(
            VirtualTable::wrap_internal_table(PositionArgsTableFunction)
                .expect("the test table function has valid schema SQL"),
        )
        .expect("the test table function name is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if via_where {
        let names = ["first", "second", "third"];
        let mut constraints = (1..=arity)
            .rev()
            .map(|position| format!("{} = ?{position}", names[position - 1]))
            .collect::<Vec<_>>();
        if duplicate {
            constraints.push(format!("first = ?{}", arity + 1));
        }
        format!(
            "SELECT value FROM position_args WHERE {}",
            constraints.join(" AND ")
        )
    } else {
        format!("SELECT value FROM position_args({arguments})")
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated table-function query has valid SQL meaning");
    let source = document
        .sources
        .iter()
        .find(|source| source.name == "position_args")
        .expect("the table-function source exists");
    let crate::translate::semantic::hir::SourceKind::TableFunction {
        arguments: bound_arguments,
        ..
    } = &source.kind
    else {
        panic!("both spellings bind to one table-function HIR source");
    };
    assert_eq!(bound_arguments.len(), arity);
    if via_where {
        let root = match &document.root {
            HirRoot::Query(root) => root,
            _ => unreachable!("the fixture is a query"),
        };
        let query = document.query(root.query).expect("the root query exists");
        assert_eq!(
            matches!(
                query.blocks[0].body,
                QueryBlockBody::Select {
                    filter: Some(_),
                    ..
                }
            ),
            duplicate
        );
    }
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed table-function HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("table function lowers without a catalog");
    program
        .resolve_labels()
        .expect("all table-function branches are closed");

    let (filter_position, cursor, args_reg, idx_str) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::VFilter {
                cursor_id,
                arg_count,
                args_reg,
                idx_str,
                idx_num,
                ..
            } if *arg_count == arity && *idx_num == (1 << arity) - 1 => {
                Some((position, *cursor_id, *args_reg, *idx_str))
            }
            _ => None,
        })
        .expect("the virtual filter receives every bound hidden-column argument");
    assert!(idx_str.is_some());
    assert!(program.insns[..filter_position].iter().any(
        |(instruction, _)| matches!(instruction, Insn::VOpen { cursor_id } if *cursor_id == cursor)
    ));

    let parameters = program.insns[..filter_position]
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Variable { index, dest } if *dest >= args_reg && *dest < args_reg + arity => {
                Some((index.get() as usize, *dest))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(parameters.len(), arity);
    for (position, (parameter, destination)) in parameters.into_iter().enumerate() {
        assert_eq!(parameter, position + 1);
        assert_eq!(destination, args_reg + position);
    }
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::VColumn {
            cursor_id,
            column: 0,
            ..
        } if *cursor_id == cursor
    )));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::VNext { cursor_id, .. } if *cursor_id == cursor
    )));
}

// Examples: `SELECT c7 FROM items INDEXED BY items_idx WHERE c2 >= ?1` and
// `... WHERE c4 IS NOT NULL AND c2 >= ?1` with a partial index declared as
// `items_idx ON items(c4) WHERE c4 IS NOT NULL` must
// iterate the exact resolved `items_idx`, seek the matching table row, and
// still read table positions `c2` and `c7` from the table cursor. Index-key
// positions are not table-column positions. Dropping `Schema` first proves
// that the index name and its table relationship are never resolved again.
#[hegel::test]
fn forced_indexes_iterate_the_index_but_bind_columns_to_the_table(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let index_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let filter_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let output_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let partial = tc.draw(generators::booleans());
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
            .expect("generated table SQL is valid"),
    );
    let symbols = SymbolTable::new();
    let partial_clause = if partial {
        format!(" WHERE c{index_position} IS NOT NULL")
    } else {
        String::new()
    };
    let index = Index::from_sql(
        &symbols,
        &format!("CREATE INDEX items_idx ON items(c{index_position}){partial_clause}"),
        13,
        &table,
    )
    .expect("generated index SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(table).expect("items is unique");
    schema
        .add_index(Arc::new(index))
        .expect("items_idx is unique");
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let partial_filter = partial
        .then(|| format!("c{index_position} IS NOT NULL AND "))
        .unwrap_or_default();
    let statement = parse_statement(&format!(
        "SELECT c{output_position} FROM items INDEXED BY items_idx \
         WHERE {partial_filter}c{filter_position} >= ?1"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated forced-index query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed forced-index HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("forced index lowers without a catalog");
    program
        .resolve_labels()
        .expect("forced-index branches are all closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    db: 0,
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved B-tree is opened")
    };
    let table_cursor = cursor_for_root(7);
    let index_cursor = cursor_for_root(13);
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::DeferredSeek {
            index_cursor_id,
            table_cursor_id,
        } if *index_cursor_id == index_cursor && *table_cursor_id == table_cursor
    )));

    let reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } => Some((*cursor_id, *column)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut expected_reads = Vec::new();
    if partial {
        expected_reads.push((table_cursor, index_position));
    }
    expected_reads.push((table_cursor, filter_position));
    expected_reads.push((table_cursor, output_position));
    assert_eq!(reads, expected_reads);
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Rewind { cursor_id, .. } if *cursor_id == index_cursor
    )));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Next { cursor_id, .. } if *cursor_id == index_cursor
    )));
    assert!(!program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Rewind { cursor_id, .. } | Insn::Next { cursor_id, .. }
            if *cursor_id == table_cursor
    )));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Close { cursor_id } if *cursor_id == index_cursor
    )));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Close { cursor_id } if *cursor_id == table_cursor
    )));
}

// Example: `SELECT d.value FROM (SELECT c7 AS value FROM items WHERE c2 >= ?1)
// AS d` must materialize the child query through its resolved QueryId, then
// bind the outer source to column zero of that materialization. The child
// still reads table positions `c2` and `c7`; the outer position is `d[0]`.
// These two position spaces must never be mixed or reconstructed by name.
#[hegel::test]
fn derived_sources_materialize_query_outputs_in_their_own_position_space(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let filter_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let output_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT d.value FROM (\
             SELECT c{output_position} AS value \
             FROM items WHERE c{filter_position} >= ?1\
         ) AS d"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated derived-table query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed derived-table HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("derived table lowers without a catalog");
    program
        .resolve_labels()
        .expect("derived-table branches are all closed");

    let table_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 7,
                db: 0,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("the child opens the resolved items table");
    let derived_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("the child output has one materialized source");
    let reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } => Some((*cursor_id, *column)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        reads,
        [
            (table_cursor, filter_position),
            (table_cursor, output_position),
            (derived_cursor, 0),
        ]
    );
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::MakeRecord { count: 1, .. })));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::NewRowid { cursor, .. } if *cursor == derived_cursor
    )));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Insert { cursor, .. } if *cursor == derived_cursor
    )));
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
            .count(),
        1
    );
}

// Example: `WITH chosen(value) AS (SELECT c7 FROM items WHERE c2 >= ?1)
// SELECT l.value, r.value FROM chosen AS l JOIN chosen AS r
// ON l.value = r.value` must evaluate `chosen` once, then open two independent
// cursors over the same ephemeral rows. Each CTE occurrence keeps its own
// SourceId and cursor position; neither reference may rerun or steal the
// other reference's cursor.
#[hegel::test]
fn repeated_cte_sources_share_rows_but_keep_independent_source_cursors(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let filter_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let output_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "WITH chosen(value) AS (\
             SELECT c{output_position} FROM items WHERE c{filter_position} >= ?1\
         ) \
         SELECT l.value, r.value \
         FROM chosen AS l JOIN chosen AS r ON l.value = r.value"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated repeated-CTE query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed repeated-CTE HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("repeated CTE lowers without a catalog");
    program
        .resolve_labels()
        .expect("repeated-CTE branches are all closed");

    let backing_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("the CTE is materialized once");
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { .. }))
            .count(),
        1
    );
    let duplicate_cursors = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::OpenDup {
                new_cursor_id,
                original_cursor_id,
            } if *original_cursor_id == backing_cursor => Some(*new_cursor_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(duplicate_cursors.len(), 2);
    assert_ne!(duplicate_cursors[0], duplicate_cursors[1]);
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Insert { cursor, .. } if *cursor == backing_cursor
    )));
    assert!(!program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Rewind { cursor_id, .. } if *cursor_id == backing_cursor
    )));
    assert!(duplicate_cursors
        .iter()
        .all(
            |cursor| program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Rewind { cursor_id, .. } if cursor_id == cursor
            ))
        ));

    let table_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 7,
                db: 0,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("the CTE body opens the resolved items table once");
    let table_reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } if *cursor_id == table_cursor => Some(*column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(table_reads, [filter_position, output_position]);
    assert!(duplicate_cursors
        .iter()
        .all(
            |cursor| program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Column { cursor_id, column: 0, .. } if cursor_id == cursor
            ))
        ));
}

// Example: `SELECT o.c7,
// (SELECT i.c4 + 1 FROM inner_items AS i WHERE i.c2 = o.c3),
// EXISTS(SELECT e.c4 + 99 FROM inner_items AS e WHERE e.c2 = o.c3)
// FROM outer_items AS o` gives both child queries the exact captured outer
// SourceId. Scalar evaluation keeps only the first row (or NULL when empty),
// while EXISTS stops at the first match without evaluating its SELECT output.
#[hegel::test]
fn correlated_scalar_and_exists_subqueries_use_exact_captures_and_stop_cleanly(
    tc: hegel::TestCase,
) {
    let outer_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let inner_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let outer_key = tc.draw(generators::integers::<usize>().max_value(outer_width - 1));
    let outer_output = tc.draw(generators::integers::<usize>().max_value(outer_width - 1));
    let inner_key = tc.draw(generators::integers::<usize>().max_value(inner_width - 1));
    let inner_value = tc.draw(generators::integers::<usize>().max_value(inner_width - 1));
    let table_sql = |name: &str, width: usize| {
        let columns = (0..width)
            .map(|position| format!("c{position} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE {name}({columns})")
    };
    let outer_table = BTreeTable::from_sql(&table_sql("outer_items", outer_width), 7)
        .expect("generated outer table is valid");
    let inner_table = BTreeTable::from_sql(&table_sql("inner_items", inner_width), 11)
        .expect("generated inner table is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(outer_table))
        .expect("outer_items is unique");
    schema
        .add_btree_table(Arc::new(inner_table))
        .expect("inner_items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT o.c{outer_output}, \
             (SELECT i.c{inner_value} + 1 FROM inner_items AS i \
              WHERE i.c{inner_key} = o.c{outer_key}), \
             EXISTS(SELECT e.c{inner_value} + 99 FROM inner_items AS e \
                    WHERE e.c{inner_key} = o.c{outer_key}) \
         FROM outer_items AS o"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated correlated subqueries have valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed correlated HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("correlated subqueries lower without a catalog");
    program
        .resolve_labels()
        .expect("correlated-subquery branches are all closed");

    let outer_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 7,
                db: 0,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("the resolved outer table is opened");
    let inner_cursors = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 11,
                db: 0,
            } => Some(*cursor_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(inner_cursors.len(), 2);
    assert_ne!(inner_cursors[0], inner_cursors[1]);

    let reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } => Some((*cursor_id, *column)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        reads,
        [
            (outer_cursor, outer_output),
            (inner_cursors[0], inner_key),
            (outer_cursor, outer_key),
            (inner_cursors[0], inner_value),
            (inner_cursors[1], inner_key),
            (outer_cursor, outer_key),
        ]
    );
    for inner in &inner_cursors {
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::Close { cursor_id } if cursor_id == inner
        )));
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::Goto { target_pc: BranchOffset::Offset(target) }
                if matches!(program.insns[*target as usize].0, Insn::Close { cursor_id } if cursor_id == *inner)
        )));
    }
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Null { .. })));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Integer { value: 0, .. })));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Integer { value: 1, .. })));
}

// Example: `WITH unused(x) AS (VALUES (99)), chosen(x) AS
// (SELECT c7 FROM items WHERE c2 >= ?1) SELECT (SELECT x FROM chosen)` uses a
// CTE only inside a scalar child query. `chosen` must be materialized before
// the root runtime frame is entered, while `unused` must not execute merely
// because it was declared in the same WITH clause.
#[hegel::test]
fn cte_reachability_follows_nested_queries_without_executing_unused_ctes(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let filter_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let output_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "WITH unused(x) AS (VALUES (99)), \
              chosen(x) AS (\
                  SELECT c{output_position} FROM items WHERE c{filter_position} >= ?1\
              ) \
         SELECT (SELECT x FROM chosen)"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated nested CTE query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed nested-CTE HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("nested CTE lowers without a catalog");
    program
        .resolve_labels()
        .expect("nested-CTE branches are all closed");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { .. }))
            .count(),
        1,
        "only chosen is reachable"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenDup { .. }))
            .count(),
        1
    );
    assert!(!program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Integer { value: 99, .. })));
    let table_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 7,
                db: 0,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("the reachable CTE opens items");
    let table_reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } if *cursor_id == table_cursor => Some(*column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(table_reads, [filter_position, output_position]);
}

// Example: `SELECT (WITH t(x) AS (VALUES (1), (2), (3)) SELECT SUM(x) FROM t)`
// gives the CTE body a lexical parent because its WITH clause belongs to the
// scalar child query. It has no captures, so that parent must not be mistaken
// for a runtime dependency when the physical layer materializes the CTE.
#[hegel::test]
fn a_cte_declared_inside_a_scalar_query_uses_captures_not_its_lexical_parent(tc: hegel::TestCase) {
    let row_count = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let values = (1..=row_count)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT (WITH t(x) AS (VALUES {values}) SELECT SUM(x) FROM t)"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("a local uncorrelated CTE has valid SQL meaning");
    let cte_query = document
        .ctes
        .iter()
        .find_map(|cte| match cte.body {
            crate::translate::semantic::hir::CteBody::Query(query) => Some(query),
            crate::translate::semantic::hir::CteBody::Recursive(_) => None,
        })
        .expect("the statement declares one ordinary CTE");
    let cte_query = document.query(cte_query).expect("the CTE query exists");
    assert!(
        cte_query.parent.is_some(),
        "the CTE keeps lexical ownership"
    );
    assert!(
        cte_query.captures.is_empty(),
        "the constant VALUES body has no runtime outer dependency"
    );
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed local-CTE HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program)
        .expect("an uncorrelated local CTE lowers using its frozen captures");
    program
        .resolve_labels()
        .expect("local-CTE branches are all closed");
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { .. })));
}

// Example: `SELECT l.c4, r.c6 FROM left_items AS l JOIN right_items AS r
// ON l.c2 = r.c3 WHERE r.c5 >= ?1` must form a nested loop over the two
// resolved source identities. Both ON and WHERE failures advance the inner
// cursor, and every column read uses the position bound for its own source.
#[hegel::test]
fn inner_joins_keep_source_identity_position_and_loop_scope(tc: hegel::TestCase) {
    let left_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let right_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let left_join = tc.draw(generators::integers::<usize>().max_value(left_width - 1));
    let left_output = tc.draw(generators::integers::<usize>().max_value(left_width - 1));
    let right_join = tc.draw(generators::integers::<usize>().max_value(right_width - 1));
    let right_filter = tc.draw(generators::integers::<usize>().max_value(right_width - 1));
    let right_output = tc.draw(generators::integers::<usize>().max_value(right_width - 1));
    let table_sql = |name: &str, width: usize| {
        let columns = (0..width)
            .map(|position| format!("c{position} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE {name}({columns})")
    };
    let left_table = BTreeTable::from_sql(&table_sql("left_items", left_width), 7)
        .expect("generated left table is valid");
    let right_table = BTreeTable::from_sql(&table_sql("right_items", right_width), 11)
        .expect("generated right table is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(left_table))
        .expect("left_items is unique");
    schema
        .add_btree_table(Arc::new(right_table))
        .expect("right_items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT l.c{left_output}, r.c{right_output} \
         FROM left_items AS l JOIN right_items AS r \
         ON l.c{left_join} = r.c{right_join} \
         WHERE r.c{right_filter} >= ?1"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated inner join has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed join HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("inner join lowers without a catalog");
    program
        .resolve_labels()
        .expect("nested loop branches are all closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    db: 0,
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved join table is opened")
    };
    let left_cursor = cursor_for_root(7);
    let right_cursor = cursor_for_root(11);
    let reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } => Some((*cursor_id, *column)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        reads,
        [
            (left_cursor, left_join),
            (right_cursor, right_join),
            (right_cursor, right_filter),
            (left_cursor, left_output),
            (right_cursor, right_output),
        ]
    );

    let inner_next = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Next { cursor_id, .. } if *cursor_id == right_cursor
            )
        })
        .expect("right source is the inner loop");
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::IfNot {
                    target_pc: BranchOffset::Offset(target),
                    jump_if_null: true,
                    ..
                } if *target as usize == inner_next
            ))
            .count(),
        2
    );
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Next { cursor_id, .. } if *cursor_id == left_cursor
    )));
}

// Example: `SELECT key FROM left_items JOIN right_items USING(key)` (and its
// NATURAL JOIN spelling) compares the exact two resolved key positions using
// SQLite's no-coercion BLOB comparison affinity and NOCASE, then reads the
// already-resolved merged value.
// Physical emission must not reconstruct `l.key = r.key` from column names.
#[hegel::test]
fn using_and_natural_joins_emit_their_stored_comparison(tc: hegel::TestCase) {
    let left_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let right_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let left_key = tc.draw(generators::integers::<usize>().max_value(left_width - 1));
    let right_key = tc.draw(generators::integers::<usize>().max_value(right_width - 1));
    let natural = tc.draw(generators::booleans());
    let columns = |width: usize, key: usize, prefix: &str| {
        (0..width)
            .map(|position| {
                if position == key {
                    "key TEXT COLLATE NOCASE".to_string()
                } else {
                    format!("{prefix}{position} INTEGER")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let left_table = BTreeTable::from_sql(
        &format!(
            "CREATE TABLE left_items({})",
            columns(left_width, left_key, "l")
        ),
        7,
    )
    .expect("generated left table is valid");
    let right_table = BTreeTable::from_sql(
        &format!(
            "CREATE TABLE right_items({})",
            columns(right_width, right_key, "r")
        ),
        11,
    )
    .expect("generated right table is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(left_table))
        .expect("left_items is unique");
    schema
        .add_btree_table(Arc::new(right_table))
        .expect("right_items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let join = if natural {
        "left_items NATURAL JOIN right_items"
    } else {
        "left_items JOIN right_items USING(key)"
    };
    let statement = parse_statement(&format!("SELECT key FROM {join}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated merged-column join has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed join HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("merged-column join lowers without a catalog");
    program
        .resolve_labels()
        .expect("merged-column branches are all closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    ..
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved join table is opened")
    };
    let left_cursor = cursor_for_root(7);
    let right_cursor = cursor_for_root(11);
    let reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } => Some((*cursor_id, *column)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(reads[0], (left_cursor, left_key));
    assert_eq!(reads[1], (right_cursor, right_key));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Eq {
            flags,
            collation: Some(CollationSeq::NoCase),
            ..
        } if flags.get_affinity() == Affinity::Blob
    )));
}

// Example: `VALUES (10, 11), (20, 21)` has no runtime source. Every generated
// row must reuse one exact result register range of width two and emit one
// `ResultRow`; physical emission must not invent or resolve a table cursor.
#[hegel::test]
fn values_rows_use_one_exact_source_free_result_range(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let row_count = usize::from(tc.draw(generators::integers::<u8>().max_value(4))) + 1;
    let rows = (0..row_count)
        .map(|row| {
            let values = (0..width)
                .map(|column| (row * 100 + column).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("({values})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("VALUES {rows}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated VALUES has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed VALUES HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("VALUES lowers without a catalog");

    assert!(!program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::OpenRead { .. } | Insn::VOpen { .. })));
    let result_rows = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::ResultRow { start_reg, count } => Some((*start_reg, *count)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(result_rows.len(), row_count);
    assert!(result_rows
        .iter()
        .all(|(start, count)| *start == result_rows[0].0 && *count == width));
}

// Example: `WITH rows(a, b) AS (
// VALUES (?1, ?2), (?3, ?4) UNION ALL VALUES (?5, ?6)
// ) SELECT a, b FROM rows` must materialize the first arm before the second,
// keep every arm at the resolved width two, and scan the combined rows through
// one CTE source. The parameter order proves emission follows HIR block order;
// no parser compound tree or catalog is available after semantic analysis.
#[hegel::test]
fn union_all_materializes_hir_arms_in_order_at_one_exact_width(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(3))) + 1;
    let first_rows = usize::from(tc.draw(generators::integers::<u8>().max_value(2))) + 1;
    let second_rows = usize::from(tc.draw(generators::integers::<u8>().max_value(2))) + 1;
    let mut next_parameter = 1;
    let mut arm = |row_count: usize| {
        (0..row_count)
            .map(|_| {
                let row = (0..width)
                    .map(|_| {
                        let parameter = next_parameter;
                        next_parameter += 1;
                        format!("?{parameter}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({row})")
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let first = arm(first_rows);
    let second = arm(second_rows);
    let total_parameters = next_parameter - 1;
    let names = (0..width)
        .map(|position| format!("c{position}"))
        .collect::<Vec<_>>();
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "WITH rows({}) AS (VALUES {first} UNION ALL VALUES {second}) SELECT {} FROM rows",
        names.join(", "),
        names.join(", ")
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UNION ALL CTE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed UNION ALL HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("UNION ALL lowers without a catalog");
    program
        .resolve_labels()
        .expect("UNION ALL branches are all closed");

    let parameters = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Variable { index, .. } => Some(index.get()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(parameters, (1..=total_parameters).collect::<Vec<_>>());

    let backing_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            } => Some(*cursor_id),
            _ => None,
        })
        .expect("the compound CTE has one backing table");
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::Insert { cursor, .. } if *cursor == backing_cursor
            ))
            .count(),
        first_rows + second_rows
    );
    let record_width = u32::try_from(width).expect("generated width fits in a VDBE operand");
    assert!(program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::MakeRecord { count, .. } => Some(*count),
            _ => None,
        })
        .all(|count| count == record_width));

    let scan_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenDup {
                new_cursor_id,
                original_cursor_id,
            } if *original_cursor_id == backing_cursor => Some(*new_cursor_id),
            _ => None,
        })
        .expect("the root scans one independent CTE cursor");
    let positions = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } if *cursor_id == scan_cursor => Some(*column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(positions, (0..width).collect::<Vec<_>>());
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::ResultRow { count, .. } if *count == width
    )));
}

// Examples: `WITH RECURSIVE walk(n) AS (VALUES(0) UNION ALL
// SELECT n + 2 FROM walk WHERE n + 2 <= 10) SELECT n FROM walk`, and the
// duplicate-removing UNION form with `ORDER BY 1 DESC LIMIT 6`. The seed enters
// a work queue, each dequeued row becomes the only recursive input row, UNION
// uses a collation-aware seen set, and the outward CTE reads a separate table.
#[hegel::test]
fn recursive_cte_uses_a_closed_hir_queue_and_input_binding(tc: hegel::TestCase) {
    let step = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(5)));
    let iterations = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(12)));
    let distinct = tc.draw(generators::integers::<u8>().max_value(1)) == 1;
    let ordered = tc.draw(generators::integers::<u8>().max_value(1)) == 1;
    let operator = if distinct { "UNION" } else { "UNION ALL" };
    let order = if ordered { " ORDER BY 1 DESC" } else { "" };
    let maximum = step * iterations;
    let sql = format!(
        "WITH RECURSIVE walk(n) AS (VALUES(0) {operator} \
         SELECT n + {step} FROM walk WHERE n + {step} <= {maximum}{order}) \
         SELECT n FROM walk"
    );
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated recursive CTE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed recursive HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("recursive CTE emits without a resolver");
    program
        .resolve_labels()
        .expect("all recursive queue branches are closed");

    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::OpenPseudo { .. })));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Sequence { .. })));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::IdxDelete { .. })));
    let ephemeral_indexes = program
        .insns
        .iter()
        .filter(|(instruction, _)| {
            matches!(
                instruction,
                Insn::OpenEphemeral {
                    is_table: false,
                    ..
                }
            )
        })
        .count();
    assert_eq!(ephemeral_indexes, 1 + usize::from(distinct));
}

// Example: `SELECT ?1 AS a, ?2 AS b UNION ALL SELECT ?3, ?4
// ORDER BY b COLLATE NOCASE DESC NULLS LAST` binds the ORDER BY output to
// position one, not permanently to the first arm's `?2` register. Each arm
// must copy its own position-one value into the sorter key, while the frozen
// NOCASE/direction/NULL facts configure the sorter without a resolver. The
// implicit sequence key also uses DESC, so equal keys follow SQLite's reverse
// insertion order; for ASC they retain insertion order.
#[hegel::test]
fn compound_order_by_remaps_each_hir_arm_and_keeps_frozen_sort_facts(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(3))) + 1;
    let order_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let aliases = (0..width)
        .map(|position| format!("c{position}"))
        .collect::<Vec<_>>();
    let first = aliases
        .iter()
        .enumerate()
        .map(|(position, alias)| format!("?{} AS {alias}", position + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let second = (0..width)
        .map(|position| format!("?{}", width + position + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT {first} UNION ALL SELECT {second} ORDER BY {} COLLATE NOCASE DESC NULLS LAST",
        aliases[order_position]
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated ordered compound query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("ordered compound HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("ordered compound lowers without a catalog");
    program
        .resolve_labels()
        .expect("ordered compound branches are all closed");

    let (sorter, sort_facts, comparators) = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::SorterOpen {
                cursor_id,
                order_collations_nulls,
                comparators,
                ..
            } => Some((*cursor_id, order_collations_nulls, comparators)),
            _ => None,
        })
        .expect("ORDER BY opens one HIR-configured sorter");
    assert_eq!(
        sort_facts,
        &[
            (
                ast::SortOrder::Desc,
                Some(CollationSeq::NoCase),
                Some(ast::NullsOrder::Last),
            ),
            (ast::SortOrder::Desc, Some(CollationSeq::Binary), None,)
        ]
    );
    assert_eq!(comparators, &[None, None]);

    let variables = program
        .insns
        .iter()
        .enumerate()
        .filter_map(
            |(instruction_position, (instruction, _))| match instruction {
                Insn::Variable { index, dest } => Some((instruction_position, index.get(), *dest)),
                _ => None,
            },
        )
        .collect::<Vec<_>>();
    assert_eq!(
        variables
            .iter()
            .map(|(_, index, _)| *index)
            .collect::<Vec<_>>(),
        (1..=width * 2).collect::<Vec<_>>()
    );
    for parameter in [order_position + 1, width + order_position + 1] {
        let (variable_position, _, variable_register) = variables
            .iter()
            .copied()
            .find(|(_, index, _)| *index == parameter)
            .expect("the selected arm output has a parameter register");
        let insert_position = program
            .insns
            .iter()
            .enumerate()
            .skip(variable_position + 1)
            .find_map(|(position, (instruction, _))| match instruction {
                Insn::SorterInsert { cursor_id, .. } if *cursor_id == sorter => Some(position),
                _ => None,
            })
            .expect("the selected arm is inserted into the sorter");
        assert!(program.insns[variable_position + 1..insert_position]
            .iter()
            .any(|(instruction, _)| matches!(
                instruction,
                Insn::Copy { src_reg, .. } if *src_reg == variable_register
            )));
    }
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::SorterInsert { cursor_id, .. } if *cursor_id == sorter
            ))
            .count(),
        2
    );

    let pseudo = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenPseudo { cursor_id, .. } => Some(*cursor_id),
            _ => None,
        })
        .expect("sorted result rows use one pseudo cursor");
    let output_positions = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } if *cursor_id == pseudo => Some(*column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(output_positions, (2..=width + 1).collect::<Vec<_>>());
}

// Example: `SELECT c7 FROM items ORDER BY c2 DESC LIMIT ?1 OFFSET ?2`
// must fill the sorter with every input row, then apply OFFSET and LIMIT while
// draining sorted rows. Both counters are bound and integer-checked once; an
// exhausted LIMIT must jump to sorter cleanup, never out past open cursors.
#[hegel::test]
fn sorted_limit_and_offset_control_final_hir_rows_and_reach_cleanup(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let output_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let order_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT c{output_position} FROM items ORDER BY c{order_position} DESC LIMIT ?1 OFFSET ?2"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated sorted LIMIT query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed LIMIT HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("sorted LIMIT lowers without a catalog");
    program
        .resolve_labels()
        .expect("sorted LIMIT branches are all closed");

    let variables = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| match instruction {
            Insn::Variable { index, dest } => Some((position, index.get(), *dest)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        variables
            .iter()
            .map(|(_, index, _)| *index)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let (_, _, limit_register) = variables[0];
    let (_, _, offset_register) = variables[1];
    assert!(variables.iter().all(|(variable_position, _, register)| {
        program.insns[variable_position + 1..]
            .iter()
            .any(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::MustBeInt { reg, .. } if reg == register
                )
            })
    }));

    let table_open = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::OpenRead { root_page: 7, .. }))
        .expect("the resolved table opens after counter setup");
    assert!(variables
        .iter()
        .all(|(position, _, _)| *position < table_open));

    let sorter_data = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::SorterData { .. }))
        .expect("sorted rows are drained");
    let offset = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::IfPos { reg, .. } if *reg == offset_register
            )
        })
        .expect("OFFSET controls sorted output");
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
        .expect("one selected column is returned");
    let decrement = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::DecrJumpZero { reg, .. } if *reg == limit_register
            )
        })
        .expect("LIMIT controls sorted output");
    let sorter_next = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::SorterNext { .. }))
        .expect("sorter advances after final-row control");
    assert!(
        sorter_data < offset && offset < result && result < decrement && decrement < sorter_next
    );
    assert!(matches!(
        &program.insns[offset].0,
        Insn::IfPos {
            target_pc: BranchOffset::Offset(target),
            ..
        } if *target as usize == sorter_next
    ));

    let cleanup = program
        .insns
        .iter()
        .enumerate()
        .skip(sorter_next + 1)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Close { .. }).then_some(position)
        })
        .expect("sorter output has a cleanup path");
    assert!(matches!(
        &program.insns[decrement].0,
        Insn::DecrJumpZero {
            target_pc: BranchOffset::Offset(target),
            ..
        } if *target as usize == cleanup
    ));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::IfNot {
            reg,
            target_pc: BranchOffset::Offset(target),
            jump_if_null: false,
        } if *reg == limit_register && *target as usize == cleanup
    )));
}

// Example: `SELECT ?1 UNION ALL SELECT ?2 LIMIT ?3 OFFSET ?4` must use one
// OFFSET and one LIMIT counter across both HIR arms. If the first arm exhausts
// LIMIT, it must leave that arm through its cleanup path and jump over the
// second arm; normal completion must keep going to the next arm.
#[hegel::test]
fn union_all_limit_uses_one_counter_and_stops_before_later_hir_arms(tc: hegel::TestCase) {
    let arm_count = usize::from(tc.draw(generators::integers::<u8>().max_value(3))) + 2;
    let mut sql = String::new();
    for arm in 1..=arm_count {
        if arm == 1 {
            sql.push_str("SELECT ");
        } else {
            sql.push_str(" UNION ALL SELECT ");
        }
        sql.push('?');
        sql.push_str(&arm.to_string());
    }
    let limit_parameter = arm_count + 1;
    let offset_parameter = arm_count + 2;
    sql.push_str(&format!(
        " LIMIT ?{limit_parameter} OFFSET ?{offset_parameter}"
    ));

    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated limited UNION ALL has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("limited compound HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("limited UNION ALL lowers without a catalog");
    program
        .resolve_labels()
        .expect("limited UNION ALL branches are all closed");

    let variables = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Variable { index, dest } => Some((index.get(), *dest)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(variables.len(), arm_count + 2);
    for parameter in 1..=arm_count + 2 {
        assert_eq!(
            variables
                .iter()
                .filter(|(index, _)| *index == parameter)
                .count(),
            1,
            "every SQL parameter is bound once"
        );
    }
    let register_for = |parameter| {
        variables
            .iter()
            .find_map(|(index, register)| (*index == parameter).then_some(*register))
            .expect("parameter has one runtime register")
    };
    let limit_register = register_for(limit_parameter);
    let offset_register = register_for(offset_parameter);
    let stopped_register = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::Integer { value: 0, dest } => Some(*dest),
            _ => None,
        })
        .expect("streaming compound LIMIT has one stop register");

    let results = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::ResultRow { count: 1, .. }).then_some(position)
        })
        .collect::<Vec<_>>();
    let decrements = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| match instruction {
            Insn::DecrJumpZero { reg, target_pc } if *reg == limit_register => {
                Some((position, *target_pc))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let offsets = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| match instruction {
            Insn::IfPos { reg, target_pc, .. } if *reg == offset_register => {
                Some((position, *target_pc))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let arm_exits = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| match instruction {
            Insn::If {
                reg,
                target_pc,
                jump_if_null: false,
            } if *reg == stopped_register => Some((position, *target_pc)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), arm_count);
    assert_eq!(decrements.len(), arm_count);
    assert_eq!(offsets.len(), arm_count);
    assert_eq!(arm_exits.len(), arm_count);

    let query_done = arm_exits[0].1;
    assert!(arm_exits.iter().all(|(_, target)| *target == query_done));
    let BranchOffset::Offset(query_done) = query_done else {
        panic!("query-wide compound exit is resolved");
    };
    assert!(query_done as usize > arm_exits.last().expect("at least two arms").0);
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::IfNot {
            reg,
            target_pc: BranchOffset::Offset(target),
            jump_if_null: false,
        } if *reg == limit_register && *target == query_done
    )));

    for arm in 0..arm_count {
        let (decrement, BranchOffset::Offset(exhausted)) = decrements[arm] else {
            panic!("arm LIMIT exit is resolved");
        };
        let (offset, BranchOffset::Offset(after_offset)) = offsets[arm] else {
            panic!("arm OFFSET exit is resolved");
        };
        let (arm_exit, _) = arm_exits[arm];
        assert!(offset < results[arm] && results[arm] < decrement && decrement < arm_exit);
        assert_eq!(after_offset as usize, arm_exit);
        assert!(matches!(
            &program.insns[exhausted as usize].0,
            Insn::Integer { value: 1, dest } if *dest == stopped_register
        ));
        if arm + 1 < arm_count {
            assert!(arm_exit < results[arm + 1]);
            assert!(query_done as usize > results[arm + 1]);
        }
    }
}

// Example: `SELECT DISTINCT c0 COLLATE NOCASE, c1 COLLATE RTRIM FROM items
// LIMIT ?1 OFFSET ?2` must deduplicate the fully produced HIR row with those
// exact frozen collations before OFFSET and LIMIT. A duplicate must jump to
// the scan's Next instruction, while LIMIT exits through both scan and hash
// cleanup.
#[hegel::test]
fn distinct_uses_frozen_output_collations_before_offset_and_limit(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(3))) + 1;
    let columns = (0..width)
        .map(|position| format!("c{position} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    let outputs = (0..width)
        .map(|position| {
            let collation = if position % 2 == 0 { "NOCASE" } else { "RTRIM" };
            format!("c{position} COLLATE {collation} AS out_{position}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let expected_collations = (0..width)
        .map(|position| {
            if position % 2 == 0 {
                CollationSeq::NoCase
            } else {
                CollationSeq::Rtrim
            }
        })
        .collect::<Vec<_>>();
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT DISTINCT {outputs} FROM items LIMIT ?1 OFFSET ?2"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DISTINCT query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("DISTINCT HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("DISTINCT lowers without a catalog");
    program
        .resolve_labels()
        .expect("DISTINCT branches are all closed");

    let variables = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Variable { index, dest } => Some((index.get(), *dest)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let limit_register = variables
        .iter()
        .find_map(|(index, register)| (*index == 1).then_some(*register))
        .expect("LIMIT has one register");
    let offset_register = variables
        .iter()
        .find_map(|(index, register)| (*index == 2).then_some(*register))
        .expect("OFFSET has one register");

    let (hash_clear, hash_table_id) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::HashClear { hash_table_id } => Some((position, *hash_table_id)),
            _ => None,
        })
        .expect("DISTINCT initializes one block-local hash table");
    let (distinct, key_start, duplicate_target) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::HashDistinct { data } if data.hash_table_id == hash_table_id => {
                assert_eq!(data.num_keys, width);
                assert_eq!(data.collations, expected_collations);
                Some((position, data.key_start_reg, data.target_pc))
            }
            _ => None,
        })
        .expect("DISTINCT probes the initialized hash table");
    let table_open = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::OpenRead { root_page: 7, .. }))
        .expect("resolved table is opened");
    let offset = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::IfPos { reg, .. } if *reg == offset_register)
        })
        .expect("OFFSET follows DISTINCT");
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::ResultRow { start_reg, count }
                    if *start_reg == key_start && *count == width
            )
        })
        .expect("the distinct key is the final output row");
    let decrement = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::DecrJumpZero { reg, .. } if *reg == limit_register)
        })
        .expect("LIMIT follows DISTINCT output");
    let next = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Next { .. }))
        .expect("duplicates continue the table scan");
    assert!(hash_clear < table_open && table_open < distinct);
    assert!(distinct < offset && offset < result && result < decrement && decrement < next);
    assert!(matches!(
        duplicate_target,
        BranchOffset::Offset(target) if target as usize == next
    ));

    let scan_close = program
        .insns
        .iter()
        .enumerate()
        .skip(next + 1)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Close { .. }).then_some(position)
        })
        .expect("scan cursor is closed");
    let hash_close = program
        .insns
        .iter()
        .enumerate()
        .skip(scan_close + 1)
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::HashClose {
                hash_table_id: actual,
            } if *actual == hash_table_id => Some(position),
            _ => None,
        })
        .expect("DISTINCT hash state is closed after the scan");
    assert!(matches!(
        &program.insns[decrement].0,
        Insn::DecrJumpZero {
            target_pc: BranchOffset::Offset(target),
            ..
        } if *target as usize == scan_close
    ));
    assert!(scan_close < hash_close);
}

// Example: `SELECT (SELECT d.v FROM
// (SELECT i.c4 AS v FROM inner_items AS i WHERE i.c2 = o.c3) AS d)
// FROM outer_items AS o` gives the derived query the exact `o` capture. Its
// ephemeral table is rebuilt inside the outer row's scalar-subquery path, so
// each outer row sees only the inner rows selected by its own `o.c3` value.
#[hegel::test]
fn correlated_derived_tables_materialize_with_their_exact_outer_capture(tc: hegel::TestCase) {
    let outer_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let inner_width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let outer_key = tc.draw(generators::integers::<usize>().max_value(outer_width - 1));
    let inner_key = tc.draw(generators::integers::<usize>().max_value(inner_width - 1));
    let inner_value = tc.draw(generators::integers::<usize>().max_value(inner_width - 1));
    let table_sql = |name: &str, width: usize| {
        let columns = (0..width)
            .map(|position| format!("c{position} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE {name}({columns})")
    };
    let outer_table = BTreeTable::from_sql(&table_sql("outer_items", outer_width), 7)
        .expect("generated outer table is valid");
    let inner_table = BTreeTable::from_sql(&table_sql("inner_items", inner_width), 11)
        .expect("generated inner table is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(outer_table))
        .expect("outer_items is unique");
    schema
        .add_btree_table(Arc::new(inner_table))
        .expect("inner_items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT (SELECT d.v FROM (\
             SELECT i.c{inner_value} AS v FROM inner_items AS i \
             WHERE i.c{inner_key} = o.c{outer_key}\
         ) AS d) FROM outer_items AS o"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated correlated derived table has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let root = match &document.root {
        HirRoot::Query(query) => query.query,
        _ => panic!("generated SELECT has a query root"),
    };
    let root_source = document.queries[root.index()].blocks[0]
        .from
        .as_ref()
        .expect("root query reads outer_items")
        .first;
    let derived = document
        .queries
        .iter()
        .find(|query| query.captures == [root_source])
        .expect("derived query captures exactly the outer source");
    let scalar = derived
        .parent
        .expect("derived query is nested inside the scalar query");
    assert_ne!(scalar, root);
    assert_eq!(document.queries[scalar.index()].parent, Some(root));

    let plan = PhysicalPlan::new(&document).expect("closed correlated HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program)
        .expect("correlated derived table lowers without reopening the catalog");
    program
        .resolve_labels()
        .expect("correlated-derived-table branches are all closed");

    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { is_table: true, .. })));
}

// Example: `SELECT outer_items.c0 COLLATE NOCASE IN
// (SELECT inner_items.c1 FROM inner_items
//  WHERE inner_items.c2 = outer_items.c3) FROM outer_items` must rebuild the
// correlated row set inside the outer loop, read the exact captured outer
// column, and compare membership with HIR-frozen NOCASE affinity rules. A
// TEXT RHS uses no coercion; an INTEGER RHS makes the comparison NUMERIC. The
// row-set scan must retain SQL's FALSE/TRUE/NULL outcomes.
#[hegel::test]
fn correlated_in_subqueries_use_captures_and_frozen_comparison_facts(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let lhs_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let rhs_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let inner_filter = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let outer_capture = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let outer_columns = (0..width)
        .map(|position| format!("c{position} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    let rhs_numeric = tc.draw(generators::booleans());
    let rhs_type = if rhs_numeric { "INTEGER" } else { "TEXT" };
    let inner_columns = (0..width)
        .map(|position| format!("c{position} {rhs_type}"))
        .collect::<Vec<_>>()
        .join(", ");
    let outer = BTreeTable::from_sql(&format!("CREATE TABLE outer_items({outer_columns})"), 7)
        .expect("generated outer table SQL is valid");
    let inner = BTreeTable::from_sql(&format!("CREATE TABLE inner_items({inner_columns})"), 13)
        .expect("generated inner table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(outer))
        .expect("outer_items is unique");
    schema
        .add_btree_table(Arc::new(inner))
        .expect("inner_items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT outer_items.c{lhs_position} COLLATE NOCASE IN (\
         SELECT inner_items.c{rhs_position} FROM inner_items \
         WHERE inner_items.c{inner_filter} = outer_items.c{outer_capture}\
         ) FROM outer_items"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated correlated IN query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("correlated IN HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("correlated IN lowers without a catalog");
    program
        .resolve_labels()
        .expect("correlated IN branches are all closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    ..
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved table cursor is opened")
    };
    let outer_cursor = cursor_for_root(7);
    let inner_cursor = cursor_for_root(13);
    let (row_set_open, row_set_cursor) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            } => Some((position, *cursor_id)),
            _ => None,
        })
        .expect("IN subquery opens one ephemeral row set");
    let outer_rewind = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Rewind { cursor_id, .. } if *cursor_id == outer_cursor)
        })
        .expect("outer scan starts");
    let outer_next = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Next { cursor_id, .. } if *cursor_id == outer_cursor)
        })
        .expect("outer scan advances");
    assert!(outer_rewind < row_set_open && row_set_open < outer_next);

    let inner_reads = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id, column, ..
            } if *cursor_id == inner_cursor => Some(*column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(inner_reads, [inner_filter, rhs_position]);
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Column {
            cursor_id,
            column,
            ..
        } if *cursor_id == outer_cursor && *column == outer_capture
    )));

    let (row_set_read, row_set_value) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::Column {
                cursor_id,
                column: 0,
                dest,
                ..
            } if *cursor_id == row_set_cursor => Some((position, *dest)),
            _ => None,
        })
        .expect("membership scans the materialized HIR output");
    let comparison = program
        .insns
        .iter()
        .enumerate()
        .skip(row_set_read + 1)
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::Eq {
                lhs,
                rhs,
                flags,
                collation: Some(CollationSeq::NoCase),
                ..
            } if *lhs == row_set_value || *rhs == row_set_value => Some((position, *flags)),
            _ => None,
        })
        .expect("membership uses the frozen explicit collation");
    assert_eq!(
        comparison.1.get_affinity(),
        if rhs_numeric {
            Affinity::Numeric
        } else {
            Affinity::Blob
        }
    );
    let row_set_next = program
        .insns
        .iter()
        .enumerate()
        .skip(comparison.0 + 1)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Next { cursor_id, .. } if *cursor_id == row_set_cursor)
                .then_some(position)
        })
        .expect("membership checks every candidate row");
    let result = program
        .insns
        .iter()
        .enumerate()
        .skip(row_set_next + 1)
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::ResultRow {
                start_reg,
                count: 1,
            } => Some((position, *start_reg)),
            _ => None,
        })
        .expect("membership produces one SQL value");
    assert!(program.insns[comparison.0..result.0]
        .iter()
        .any(|(instruction, _)| matches!(
            instruction,
            Insn::Null { dest, .. } if *dest == result.1
        )));
    assert!(program.insns[comparison.0..result.0]
        .iter()
        .any(|(instruction, _)| matches!(
            instruction,
            Insn::Integer { value: 1, dest } if *dest == result.1
        )));

    let row_set_close = program
        .insns
        .iter()
        .enumerate()
        .skip(outer_next + 1)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Close { cursor_id } if *cursor_id == row_set_cursor)
                .then_some(position)
        })
        .expect("root cleanup closes the IN row set");
    assert!(outer_next < row_set_close);
}

// Example: `SELECT c3, sum(c4), count(*), avg(c1) FROM items
// WHERE c2 >= 0 LIMIT 1` gives each aggregate a stable identity owned by this
// SELECT block. Physical planning must also close the LIMIT branch after the
// one aggregate result, even when bare columns retain values from the first row.
#[hegel::test]
fn ungrouped_aggregates_keep_hir_identity_through_physical_emission(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(11))) + 1;
    let aggregate_count = usize::from(tc.draw(generators::integers::<u8>().max_value(5))) + 1;
    let filter_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let bare_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let limit = tc.draw(generators::integers::<u8>().max_value(2));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 17)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let functions = (0..aggregate_count)
        .map(|position| {
            let column = tc.draw(generators::integers::<usize>().max_value(width - 1));
            match position % 5 {
                0 => format!("sum(c{column})"),
                1 => "count(*)".to_string(),
                2 => format!("avg(c{column})"),
                3 => format!("count(c{column})"),
                _ => format!("total(c{column})"),
            }
        })
        .collect::<Vec<_>>();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT c{bare_position}, {} FROM items WHERE c{filter_position} >= 0 LIMIT {limit}",
        functions.join(", ")
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated aggregate query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a SELECT");
    };
    let query = &document.queries[root.query.index()];
    let block = &query.blocks[query.first.index];
    assert_eq!(block.aggregate_count, aggregate_count);
    assert_eq!(block.window_function_count, 0);
    let mut hir_calls = vec![None; aggregate_count];
    for output in &block.outputs {
        output.expr.walk(&mut |expression| {
            let Expr::Function(call) = expression else {
                return;
            };
            if let FunctionEvaluation::Aggregate(id) = call.evaluation {
                assert_eq!(id.block, block.id);
                assert!(hir_calls[id.index].replace(call).is_none());
            }
        });
    }
    assert!(hir_calls.iter().all(Option::is_some));

    let plan = PhysicalPlan::new(&document).expect("closed aggregate HIR has a physical plan");
    let planned = &plan.queries[root.query.index()].blocks[query.first.index];
    assert_eq!(planned.aggregates.len(), aggregate_count);
    for (position, aggregate) in planned.aggregates.iter().enumerate() {
        assert_eq!(aggregate.id.index, position);
        assert!(std::ptr::eq(
            aggregate.call,
            hir_calls[position].expect("every HIR aggregate has a definition")
        ));
    }

    let mut program = program();
    emit_root_query(&plan, &mut program).expect("core ungrouped aggregates emit from HIR");
    program
        .resolve_labels()
        .expect("all aggregate-emission branches are closed");
    let steps = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::AggStep { acc_reg, .. } => Some(*acc_reg),
            _ => None,
        })
        .collect::<Vec<_>>();
    let finals = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::AggFinal { register, .. } => Some(*register),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(steps.len(), aggregate_count);
    assert_eq!(finals.len(), aggregate_count);
    assert_eq!(steps, finals);
    let first_step = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::AggStep { .. }))
        .expect("aggregate rows are stepped");
    let next = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Next { .. }))
        .expect("the input table is scanned");
    let first_final = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::AggFinal { .. }))
        .expect("aggregate state is finalized");
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { .. }))
        .expect("the aggregate query emits one result row");
    assert!(first_step < next && next < first_final && first_final < result);
}

// Example: `SELECT row_number() OVER (PARTITION BY c3 ORDER BY c1),
// sum(c4) OVER (ORDER BY c2) FROM items` assigns window identities 0 and 1
// to the calls, including the aggregate used in window mode. They must not be
// mixed with the ordinary aggregate slots for the same SELECT block.
#[hegel::test]
fn window_calls_have_separate_stable_hir_identity(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(11))) + 1;
    let window_count = usize::from(tc.draw(generators::integers::<u8>().max_value(5))) + 1;
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 19)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let functions = (0..window_count)
        .map(|position| {
            let partition = tc.draw(generators::integers::<usize>().max_value(width - 1));
            let order = tc.draw(generators::integers::<usize>().max_value(width - 1));
            if position % 2 == 0 {
                format!("row_number() OVER (PARTITION BY c{partition} ORDER BY c{order})")
            } else {
                let value = tc.draw(generators::integers::<usize>().max_value(width - 1));
                format!("sum(c{value}) OVER (PARTITION BY c{partition} ORDER BY c{order})")
            }
        })
        .collect::<Vec<_>>();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("SELECT {} FROM items", functions.join(", ")));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated window query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a SELECT");
    };
    let query = &document.queries[root.query.index()];
    let block = &query.blocks[query.first.index];
    assert_eq!(block.aggregate_count, 0);
    assert_eq!(block.window_function_count, window_count);
    let mut hir_calls = vec![None; window_count];
    for output in &block.outputs {
        output.expr.walk(&mut |expression| {
            let Expr::Function(call) = expression else {
                return;
            };
            if let FunctionEvaluation::Window(id) = call.evaluation {
                assert_eq!(id.block, block.id);
                assert!(hir_calls[id.index].replace(call).is_none());
            }
        });
    }
    assert!(hir_calls.iter().all(Option::is_some));

    let plan = PhysicalPlan::new(&document).expect("closed window HIR has a physical plan");
    let planned = &plan.queries[root.query.index()].blocks[query.first.index];
    assert!(planned.aggregates.is_empty());
    assert_eq!(planned.window_functions.len(), window_count);
    for (position, function) in planned.window_functions.iter().enumerate() {
        assert_eq!(function.id.index, position);
        assert!(std::ptr::eq(
            function.call,
            hir_calls[position].expect("every HIR window function has a definition")
        ));
    }
}

// Examples:
// - `row_number() OVER (PARTITION BY g ORDER BY value ASC)` counts equal
//   values separately, using rowid only to choose a stable order among peers.
// - `rank() OVER (PARTITION BY g ORDER BY value DESC)` gives peers the same
//   rank and leaves gaps after them.
// - `dense_rank() OVER (PARTITION BY g ORDER BY value)` gives peers the same
//   rank without gaps, so only distinct earlier HIR order keys are counted.
// The WHERE predicate must apply to both the output scan and every ranking
// rescan. Dropping the schema before emission proves the physical layer uses
// the bound SourceId, column positions, collation, and direction from HIR.
#[hegel::test]
fn ranking_windows_rescan_the_bound_hir_source(tc: hegel::TestCase) {
    let descending = tc.draw(generators::booleans());
    let filter_position = tc.draw(generators::integers::<usize>().max_value(2));
    let direction = if descending { "DESC" } else { "ASC" };
    let columns = ["g", "value", "keep"];
    let items = BTreeTable::from_sql(
        "CREATE TABLE items(g INTEGER, value INTEGER, keep INTEGER)",
        53,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(items))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT g, value, \
         row_number() OVER (PARTITION BY g ORDER BY value {direction}), \
         rank() OVER (PARTITION BY g ORDER BY value {direction}), \
         dense_rank() OVER (PARTITION BY g ORDER BY value {direction}) \
         FROM items WHERE {} >= ?1",
        columns[filter_position]
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated ranking query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed ranking HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("ranking windows emit from closed HIR");
    program
        .resolve_labels()
        .expect("all ranking-window branches are closed");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::OpenRead {
                        root_page: 53,
                        db: 0,
                        ..
                    }
                )
            })
            .count(),
        1,
        "the filtered catalog source is materialized once"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenDup { .. }))
            .count(),
        4,
        "the sorter and each ranking call scan the materialized window rows"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
            .count(),
        3,
        "each ranking function owns one counter"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. }))
            .count(),
        1,
        "only dense_rank counts distinct earlier order keys"
    );
    let first_compare = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Compare { .. }))
        .expect("partition and order keys are compared during rescans");
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { .. }))
        .expect("the ranked row is emitted");
    assert!(first_compare < result);
}

// Examples:
// - `percent_rank() OVER (PARTITION BY g ORDER BY value)` divides the number
//   of earlier rows by `partition_size - 1`, returning real `0.0` for one-row
//   groups instead of an integer zero.
// - `cume_dist() OVER (PARTITION BY g ORDER BY value DESC NULLS FIRST)` puts
//   the NULL peer group first and includes every peer in the numerator.
// - `ntile(4) OVER (PARTITION BY g ORDER BY value)` puts the extra rows in the
//   first buckets and returns consecutive bucket numbers when buckets outnumber
//   rows. The bucket expression, filter, and order are all frozen in HIR.
#[hegel::test]
fn distribution_windows_use_bound_partition_and_order_inputs(tc: hegel::TestCase) {
    let descending = tc.draw(generators::booleans());
    let nulls_first = tc.draw(generators::booleans());
    let bucket_count = i64::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let filter_position = tc.draw(generators::integers::<usize>().max_value(2));
    let direction = if descending { "DESC" } else { "ASC" };
    let nulls = if nulls_first { "FIRST" } else { "LAST" };
    let columns = ["g", "value", "keep"];
    let items = BTreeTable::from_sql(
        "CREATE TABLE items(g INTEGER, value INTEGER, keep INTEGER)",
        59,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(items))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT percent_rank() OVER (PARTITION BY g ORDER BY value {direction} NULLS {nulls}), \
         cume_dist() OVER (PARTITION BY g ORDER BY value {direction} NULLS {nulls}), \
         ntile({bucket_count}) OVER (PARTITION BY g ORDER BY value {direction} NULLS {nulls}) \
         FROM items WHERE {} >= ?1",
        columns[filter_position]
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated distribution query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed distribution HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("distribution windows emit from closed HIR");
    program
        .resolve_labels()
        .expect("all distribution-window branches are closed");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::OpenRead {
                        root_page: 59,
                        db: 0,
                        ..
                    }
                )
            })
            .count(),
        1,
        "the filtered catalog source is materialized once"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenDup { .. }))
            .count(),
        5,
        "the sorter and each function scan the rows, with one extra NTILE bucket scan"
    );
    assert!(
        program.insns.windows(3).any(|instructions| {
            matches!(
                (&instructions[0].0, &instructions[1].0, &instructions[2].0),
                (
                    Insn::AddImm { value: -1, .. },
                    Insn::RealAffinity { .. },
                    Insn::If { .. }
                )
            )
        }),
        "percent_rank becomes real before the singleton shortcut"
    );
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::RealAffinity { .. })));
    assert!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Divide { .. }))
            .count()
            >= 4
    );
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Cast {
            affinity: Affinity::Integer,
            ..
        }
    )));
    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(
            instruction,
            Insn::Compare { key_info, .. }
                if key_info.iter().any(|key| key.nulls_order
                    == Some(if nulls_first {
                        ast::NullsOrder::First
                    } else {
                        ast::NullsOrder::Last
                    }))
        )
    }));
    assert!(!program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
}

// Examples:
// - `lag(value) OVER (PARTITION BY g ORDER BY rank)` returns the previous row
//   in window order, not the previous physical table row.
// - `lead(value, 3, -1) OVER (PARTITION BY g ORDER BY rank DESC)` evaluates
//   offset and default against the current outer row and never crosses groups.
// - `lag(value, -3, -1) OVER (ORDER BY rank)` returns the default because the
//   streaming lag path has only buffered the single next row.
// Each filtered partition is sorted from HIR order terms into a private ordinal
// table, so duplicate order keys use rowid only as their deterministic tie-break.
#[hegel::test]
fn navigation_windows_materialize_bound_window_order(tc: hegel::TestCase) {
    let descending = tc.draw(generators::booleans());
    let offset = i64::from(tc.draw(generators::integers::<u8>().max_value(10))) - 5;
    let default = i64::from(tc.draw(generators::integers::<u8>().max_value(31))) - 15;
    let filter_position = tc.draw(generators::integers::<usize>().max_value(3));
    let direction = if descending { "DESC" } else { "ASC" };
    let columns = ["g", "value", "rank", "keep"];
    let items = BTreeTable::from_sql(
        "CREATE TABLE items(g INTEGER, value INTEGER, rank INTEGER, keep INTEGER)",
        61,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(items))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT lag(value, {offset}, {default}) \
           OVER (PARTITION BY g ORDER BY rank {direction}), \
         lead(value, {offset}, {default}) \
           OVER (PARTITION BY g ORDER BY rank {direction}) \
         FROM items WHERE {} >= ?1",
        columns[filter_position]
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated navigation query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed navigation HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("navigation windows emit from closed HIR");
    program
        .resolve_labels()
        .expect("all navigation-window branches are closed");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::OpenRead {
                        root_page: 61,
                        db: 0,
                        ..
                    }
                )
            })
            .count(),
        1,
        "the filtered catalog source is materialized once"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenDup { .. }))
            .count(),
        3,
        "the sorter, lag, and lead each scan the materialized window rows"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::SorterOpen { .. }))
            .count(),
        3,
        "one outer window-order sorter and one private ordinal sorter per function"
    );
    assert!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Lt { .. }))
            .count()
            >= 1,
        "lag guards lookups beyond SQLite's one-row lookahead"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
            .count(),
        2,
        "lag and lead each seek their bound ordinal target"
    );
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Subtract { .. })));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Add { .. })));
}

// Examples:
// - `row_number() OVER (), lag(value) OVER (ORDER BY rank DESC)` emits rows in
//   `rank DESC` order, and the empty outer window numbers that delivered order.
// - Adding a later `row_number() OVER (PARTITION BY g ORDER BY rank ASC)` must
//   not replace the earlier ordered stage as the final row stream.
// Varying the order column and direction proves this choice follows frozen HIR
// window order rather than a hard-coded output position.
#[hegel::test]
fn multiple_windows_keep_the_first_non_empty_hir_order(tc: hegel::TestCase) {
    let descending = tc.draw(generators::booleans());
    let order_position = tc.draw(generators::integers::<usize>().min_value(1).max_value(2));
    let other_position = if order_position == 1 { 2 } else { 1 };
    let direction = if descending { "DESC" } else { "ASC" };
    let other_direction = if descending { "ASC" } else { "DESC" };
    let items = BTreeTable::from_sql(
        "CREATE TABLE items(g INTEGER, first_value INTEGER, second_value INTEGER)",
        73,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(items))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT row_number() OVER (), \
         lag(c{order_position}) OVER (ORDER BY c{order_position} {direction}), \
         row_number() OVER (PARTITION BY g ORDER BY c{other_position} {other_direction}) \
         FROM (SELECT g, first_value AS c1, second_value AS c2 FROM items)"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated multi-window query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed multi-window HIR has a plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("multiple windows emit from closed HIR");
    program
        .resolve_labels()
        .expect("all multi-window branches are closed");

    let (columns, order_collations_nulls) = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::SorterOpen {
                columns,
                order_collations_nulls,
                ..
            } => Some((*columns, order_collations_nulls)),
            _ => None,
        })
        .expect("an ordered window opens the outer sorter");
    assert_eq!(columns, 2, "one HIR order key plus the stable row key");
    assert_eq!(
        order_collations_nulls[0].0,
        if descending {
            ast::SortOrder::Desc
        } else {
            ast::SortOrder::Asc
        },
        "the first non-empty HIR window chooses the outer order"
    );
}

// Examples under SQLite's default `RANGE UNBOUNDED PRECEDING .. CURRENT ROW`:
// - `first_value(value) OVER (PARTITION BY g ORDER BY rank)` reads ordinal 1.
// - `last_value(value) OVER (PARTITION BY g ORDER BY rank)` reads the last peer,
//   so duplicate rank values share the same answer.
// - `nth_value(value, 3) OVER (...)` is NULL until ordinal 3 enters the frame.
// The order keys and positive N are bound once in HIR; physical emission only
// materializes and seeks the resolved partition positions.
#[hegel::test]
fn positional_value_windows_use_the_bound_default_frame(tc: hegel::TestCase) {
    let descending = tc.draw(generators::booleans());
    let nth = i64::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let direction = if descending { "DESC" } else { "ASC" };
    let items = BTreeTable::from_sql(
        "CREATE TABLE items(g INTEGER, value INTEGER, rank INTEGER, keep INTEGER)",
        67,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(items))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT first_value(value) OVER (PARTITION BY g ORDER BY rank {direction}), \
         last_value(value) OVER (PARTITION BY g ORDER BY rank {direction}), \
         nth_value(value, {nth}) OVER (PARTITION BY g ORDER BY rank {direction}) \
         FROM items WHERE keep >= ?1"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated positional query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed positional HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("positional windows emit from closed HIR");
    program
        .resolve_labels()
        .expect("all positional-window branches are closed");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::OpenRead {
                        root_page: 67,
                        db: 0,
                        ..
                    }
                )
            })
            .count(),
        1,
        "the filtered catalog source is materialized once"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenDup { .. }))
            .count(),
        4,
        "the sorter and each positional function scan the materialized window rows"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::SorterOpen { .. }))
            .count(),
        4,
        "one outer window-order sorter and one private frame sorter per function"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
            .count(),
        3
    );
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::MustBeInt { .. })));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Compare { count, .. } if *count == 1)));
}

// Examples under the default peer frame:
// - `sum(value) OVER (PARTITION BY g ORDER BY rank)` includes all earlier rows
//   and every current peer, even when table rowid order differs from rank.
// - `count(*) FILTER (WHERE keep) OVER (...)` applies the SELECT filter first,
//   then the aggregate's independent FILTER only to rows inside the frame.
// - `group_concat(value, ':') OVER (...)` evaluates both bound arguments for
//   every included row and finalizes one accumulator per outer row.
#[hegel::test]
fn aggregate_windows_step_only_the_bound_default_frame(tc: hegel::TestCase) {
    let descending = tc.draw(generators::booleans());
    let filter_position = tc.draw(generators::integers::<usize>().max_value(3));
    let direction = if descending { "DESC" } else { "ASC" };
    let columns = ["g", "value", "rank", "keep"];
    let items = BTreeTable::from_sql(
        "CREATE TABLE items(g INTEGER, value INTEGER, rank INTEGER, keep INTEGER)",
        71,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(items))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT sum(value) OVER (PARTITION BY g ORDER BY rank {direction}), \
         count(*) FILTER (WHERE keep > 0) \
           OVER (PARTITION BY g ORDER BY rank {direction}), \
         group_concat(value, ':') \
           OVER (PARTITION BY g ORDER BY rank {direction}) \
         FROM items WHERE {} >= ?1",
        columns[filter_position]
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated aggregate-window query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed aggregate-window HIR has a plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("aggregate windows emit from closed HIR");
    program
        .resolve_labels()
        .expect("all aggregate-window branches are closed");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::OpenRead {
                        root_page: 71,
                        db: 0,
                        ..
                    }
                )
            })
            .count(),
        1,
        "the filtered catalog source is materialized once"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenDup { .. }))
            .count(),
        4,
        "the sorter and each aggregate window scan the materialized window rows"
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::AggStep { .. }))
            .count(),
        3
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::AggFinal { .. }))
            .count(),
        3
    );
    let first_step = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::AggStep { .. }))
        .expect("window rows step an accumulator");
    let first_final = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::AggFinal { .. }))
        .expect("window accumulators are finalized");
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { .. }))
        .expect("the outer row is emitted");
    assert!(first_step < first_final && first_final < result);
}

// Example: `SELECT c3, sum(c5), count(*) FROM items GROUP BY c3`, where
// `c3 TEXT COLLATE NOCASE`, sorts with the collation frozen in HIR, reloads
// each sorted source row under the same SourceId, steps one accumulator per
// group row, and emits only after that group's accumulators are finalized.
#[hegel::test]
fn grouped_aggregates_sort_and_rebind_from_frozen_hir(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(11))) + 2;
    let key_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let value_offset = tc.draw(generators::integers::<usize>().max_value(width - 2)) + 1;
    let value_position = (key_position + value_offset) % width;
    let aggregate_count = usize::from(tc.draw(generators::integers::<u8>().max_value(4))) + 1;
    let columns = (0..width)
        .map(|position| {
            if position == key_position {
                format!("c{position} TEXT COLLATE NOCASE")
            } else {
                format!("c{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 23)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let aggregates = (0..aggregate_count)
        .map(|position| match position % 5 {
            0 => format!("sum(c{value_position})"),
            1 => "count(*)".to_string(),
            2 => format!("avg(c{value_position})"),
            3 => format!("count(c{value_position})"),
            _ => format!("total(c{value_position})"),
        })
        .collect::<Vec<_>>();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT c{key_position}, {} FROM items GROUP BY c{key_position}",
        aggregates.join(", ")
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated grouped query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a SELECT");
    };
    let query = &document.queries[root.query.index()];
    let block = &query.blocks[query.first.index];
    let QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    else {
        panic!("the fixture has GROUP BY");
    };
    assert_eq!(block.aggregate_count, aggregate_count);
    assert_eq!(grouping.keys.len(), 1);
    assert_eq!(grouping.key_type_facts.len(), 1);
    assert_eq!(grouping.key_collations.len(), 1);
    assert_eq!(
        grouping.key_collations[0]
            .as_ref()
            .map(|collation| *collation.value()),
        Some(CollationSeq::NoCase)
    );

    let plan = PhysicalPlan::new(&document).expect("closed grouped HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("grouped aggregates emit from HIR");
    program
        .resolve_labels()
        .expect("all grouped-emission branches are closed");

    let (sorter_cursor, sorter_open) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::SorterOpen {
                cursor_id,
                columns: 1,
                order_collations_nulls,
                ..
            } if order_collations_nulls
                == &[(ast::SortOrder::Asc, Some(CollationSeq::NoCase), None)] =>
            {
                Some((*cursor_id, position))
            }
            _ => None,
        })
        .expect("GROUP BY opens a sorter with the frozen HIR collation");
    let sorter_insert = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::SorterInsert { cursor_id, .. } if *cursor_id == sorter_cursor
            )
        })
        .expect("source rows enter the group sorter");
    let sorter_sort = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::SorterSort { cursor_id, .. } if *cursor_id == sorter_cursor
            )
        })
        .expect("group rows are sorted");
    let pseudo_cursor = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::OpenPseudo { cursor_id, .. } => Some(*cursor_id),
            _ => None,
        })
        .expect("sorted rows are exposed through a pseudo cursor");
    let steps = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| match instruction {
            Insn::AggStep { acc_reg, col, .. } => Some((position, *acc_reg, *col)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let finals = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| match instruction {
            Insn::AggFinal { register, .. } => Some((position, *register)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(steps.len(), aggregate_count);
    assert_eq!(finals.len(), aggregate_count);
    assert_eq!(
        steps
            .iter()
            .map(|(_, register, _)| *register)
            .collect::<Vec<_>>(),
        finals
            .iter()
            .map(|(_, register)| *register)
            .collect::<Vec<_>>()
    );
    let rebound_source_registers = program.insns[..steps[0].0]
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::Column {
                cursor_id,
                column,
                dest,
                ..
            } if *cursor_id == pseudo_cursor && *column >= grouping.keys.len() => Some(*dest),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(rebound_source_registers.len() >= width);
    for (step, _, argument) in &steps {
        let argument_comes_from_sorted_source =
            program.insns[..*step]
                .iter()
                .any(|(instruction, _)| match instruction {
                    Insn::Copy {
                        src_reg,
                        dst_reg,
                        extra_amount: 0,
                    } => *dst_reg == *argument && rebound_source_registers.contains(src_reg),
                    _ => false,
                });
        let argument_is_count_star_one = program.insns[..*step].iter().any(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Integer { value: 1, dest } if *dest == *argument
            )
        });
        assert!(argument_comes_from_sorted_source || argument_is_count_star_one);
    }
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { .. }))
        .expect("each finished group can emit one row");
    assert!(sorter_open < sorter_insert && sorter_insert < sorter_sort);
    assert!(sorter_sort < steps[0].0 && steps[0].0 < finals[0].0 && finals[0].0 < result);
}

// Examples:
// - `lag(sum(value)) OVER (ORDER BY group_key)` must see one finalized `sum`
//   value per group, not the source rows that were fed into that sum.
// - `HAVING count(*) >= ?2` removes groups before the surviving group rows are
//   stored for the window pass.
// - Moving `group_key` and `value` to different column positions must not
//   change which SourceId columns and AggregateIds the window row carries.
#[hegel::test]
fn grouped_windows_consume_finalized_hir_groups(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(9))) + 2;
    let key_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let value_offset = tc.draw(generators::integers::<usize>().max_value(width - 2)) + 1;
    let value_position = (key_position + value_offset) % width;
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 71)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT c{key_position}, sum(c{value_position}), \
         lag(sum(c{value_position}), 1, -1) OVER (ORDER BY c{key_position}) \
         FROM items WHERE c{value_position} >= ?1 \
         GROUP BY c{key_position} HAVING count(*) >= ?2"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated grouped-window query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a SELECT");
    };
    let query = &document.queries[root.query.index()];
    let block = &query.blocks[query.first.index];
    assert_eq!(
        block.aggregate_count, 3,
        "each written sum call and the count keep separate stable IDs"
    );
    assert_eq!(block.window_function_count, 1);

    let plan = PhysicalPlan::new(&document).expect("closed grouped-window HIR has a plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("grouped windows emit from closed HIR");
    program
        .resolve_labels()
        .expect("all grouped-window branches are closed");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::OpenRead { root_page: 71, .. }))
            .count(),
        1,
        "the source is scanned once before grouping"
    );
    let last_final = program
        .insns
        .iter()
        .rposition(|(instruction, _)| matches!(instruction, Insn::AggFinal { .. }))
        .expect("each group finalizes its HIR aggregates");
    let stored_group = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Insert { table_name, .. } if table_name.as_str() == "window_rows"
            )
        })
        .expect("the finalized group is stored as a window row");
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { .. }))
        .expect("the window pass emits the final row");
    assert!(
        last_final < stored_group && stored_group < result,
        "aggregation and HAVING finish before the window pass"
    );
}

// Examples: `SELECT sum(DISTINCT n), min(s COLLATE NOCASE), group_concat(s),
// string_agg(s, '|') FROM items` and the same calls under `GROUP BY g`.
// Every DISTINCT aggregate owns a separate duplicate set, grouped execution
// clears it at each group boundary, and MIN uses the collation frozen in HIR.
#[hegel::test]
fn aggregate_runtime_state_follows_each_frozen_hir_call(tc: hegel::TestCase) {
    let grouped = tc.draw(generators::booleans());
    let table = BTreeTable::from_sql(
        "CREATE TABLE items(g INTEGER, n INTEGER, s TEXT COLLATE BINARY)",
        29,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let group = if grouped { " GROUP BY g" } else { "" };
    let statement = parse_statement(&format!(
        "SELECT sum(DISTINCT n), count(DISTINCT s), min(s COLLATE NOCASE), \
         group_concat(s), string_agg(s, '|') FROM items{group}"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated aggregate query has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed aggregate HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("aggregate families emit directly from HIR");
    program
        .resolve_labels()
        .expect("all aggregate runtime branches are closed");

    let hash_sets = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::HashDistinct { data } => Some(data.hash_table_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(hash_sets.len(), 2);
    assert_ne!(hash_sets[0], hash_sets[1]);
    for hash_table_id in hash_sets {
        let clears = program
            .insns
            .iter()
            .filter(|(instruction, _)| {
                matches!(instruction, Insn::HashClear { hash_table_id: id } if *id == hash_table_id)
            })
            .count();
        assert!(clears >= if grouped { 2 } else { 1 });
    }
    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(
            instruction,
            Insn::AggStep {
                func: crate::function::AccumulatorFunc::Agg(crate::function::AggFunc::Min),
                collation: Some(CollationSeq::NoCase),
                ..
            }
        )
    }));
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::AggStep { .. }))
            .count(),
        5
    );
}

// Examples: `SELECT 'A' COLLATE NOCASE, 1 UNION SELECT 'a', 1`, and the
// corresponding INTERSECT/EXCEPT forms. The left HIR output freezes NOCASE
// equality for the temporary set, UNION inserts both arms into one set,
// EXCEPT deletes the right arm from the left set, and INTERSECT probes a
// separately materialized right set before producing a row.
#[hegel::test]
fn binary_set_compounds_use_hir_output_equality(tc: hegel::TestCase) {
    let operator = match tc.draw(generators::integers::<u8>().max_value(2)) {
        0 => ("UNION", CompoundOperator::Union),
        1 => ("INTERSECT", CompoundOperator::Intersect),
        _ => ("EXCEPT", CompoundOperator::Except),
    };
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(5))) + 1;
    let left = std::iter::once("'A' COLLATE NOCASE".to_string())
        .chain((1..width).map(|position| position.to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    let right = std::iter::once("'a'".to_string())
        .chain((1..width).map(|position| position.to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("SELECT {left} {} SELECT {right}", operator.0));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated binary compound has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let query = &document.queries[root.query.index()];
    assert_eq!(query.compounds.len(), 1);
    assert_eq!(query.compounds[0].operator, operator.1);
    assert_eq!(query.blocks[0].outputs.len(), width);
    assert_eq!(
        query.blocks[0].outputs[0]
            .collation
            .as_ref()
            .map(|collation| *collation.value()),
        Some(CollationSeq::NoCase)
    );

    let plan = PhysicalPlan::new(&document).expect("closed compound HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("binary set compound emits from HIR");
    program
        .resolve_labels()
        .expect("all compound set branches are closed");

    let set_cursors = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::OpenEphemeral {
                cursor_id,
                is_table: false,
            } => Some(*cursor_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_sets = if operator.1 == CompoundOperator::Intersect {
        2
    } else {
        1
    };
    assert_eq!(set_cursors.len(), expected_sets);
    for cursor in &set_cursors {
        let CursorType::BTreeIndex(index) = program
            .get_cursor_type(*cursor)
            .expect("set cursor has a physical type")
        else {
            panic!("set storage is an ephemeral index");
        };
        assert_eq!(index.columns.len(), width);
        assert_eq!(index.columns[0].collation, Some(CollationSeq::NoCase));
        assert!(!index.has_rowid);
    }

    let inserts = program
        .insns
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::IdxInsert { cursor_id, .. } if set_cursors.contains(cursor_id) => {
                Some(*cursor_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    match operator.1 {
        CompoundOperator::Union => {
            assert_eq!(inserts, vec![set_cursors[0], set_cursors[0]]);
        }
        CompoundOperator::Except => {
            assert_eq!(inserts, vec![set_cursors[0]]);
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::IdxDelete { cursor_id, .. } if *cursor_id == set_cursors[0]
            )));
        }
        CompoundOperator::Intersect => {
            assert_eq!(inserts, set_cursors);
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::NotFound { cursor_id, .. } if *cursor_id == set_cursors[1]
            )));
        }
        CompoundOperator::UnionAll => unreachable!(),
    }
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Rewind { cursor_id, .. } if *cursor_id == set_cursors[0]
    )));
    assert!(program.insns.iter().any(
        |(instruction, _)| matches!(instruction, Insn::ResultRow { count, .. } if *count == width)
    ));
}

// Examples: `SELECT 1 UNION SELECT 2 EXCEPT SELECT 3`, and
// `SELECT 1 UNION ALL SELECT 1 INTERSECT SELECT 1 UNION ALL SELECT 2`.
// Every set operator consumes the complete result to its left. UNION ALL arms
// before the last set operator are therefore folded into that set, while arms
// after it stay streaming so their duplicates are preserved.
#[hegel::test]
fn mixed_multi_arm_compounds_keep_left_to_right_set_boundaries(tc: hegel::TestCase) {
    let arm_count = usize::from(tc.draw(generators::integers::<u8>().min_value(2).max_value(7)));
    let set_position = tc.draw(generators::integers::<usize>().max_value(arm_count - 1));
    let trailing_union_all = tc.draw(generators::integers::<u8>().max_value(1)) == 1;
    let mut sql = "SELECT 0 COLLATE NOCASE".to_string();
    let mut operators = Vec::with_capacity(arm_count);
    for position in 0..arm_count {
        let operator = if position == set_position {
            match tc.draw(generators::integers::<u8>().max_value(2)) {
                0 => CompoundOperator::Union,
                1 => CompoundOperator::Intersect,
                _ => CompoundOperator::Except,
            }
        } else if position > set_position && trailing_union_all {
            CompoundOperator::UnionAll
        } else {
            match tc.draw(generators::integers::<u8>().max_value(3)) {
                0 => CompoundOperator::Union,
                1 => CompoundOperator::Intersect,
                2 => CompoundOperator::Except,
                _ => CompoundOperator::UnionAll,
            }
        };
        let keyword = match operator {
            CompoundOperator::Union => "UNION",
            CompoundOperator::UnionAll => "UNION ALL",
            CompoundOperator::Intersect => "INTERSECT",
            CompoundOperator::Except => "EXCEPT",
        };
        sql.push_str(&format!(" {keyword} SELECT {}", position + 1));
        operators.push(operator);
    }

    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated mixed compound has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let query = &document.queries[root.query.index()];
    assert_eq!(query.compounds.len(), arm_count);
    assert_eq!(
        query
            .compounds
            .iter()
            .map(|arm| arm.operator)
            .collect::<Vec<_>>(),
        operators
    );

    let plan = PhysicalPlan::new(&document).expect("closed mixed compound has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("mixed compound emits from HIR");
    program
        .resolve_labels()
        .expect("all mixed compound branches are closed");

    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::OpenEphemeral {
            is_table: false,
            ..
        }
    )));
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. })));
}

// Examples: `l LEFT JOIN r ON l.k = r.k`, `LEFT JOIN r USING(k)`, and
// `NATURAL LEFT JOIN r`, all followed by `WHERE r.rv IS NULL`. A right row
// marks the join as matched only after its ON/USING rule passes. If no row
// matches, the right SourceId reads through NullRow and the separate WHERE
// rule is evaluated again against that NULL-extended row.
#[hegel::test]
fn left_join_keeps_join_matching_separate_from_where(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(8))) + 2;
    let key_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let value_offset = tc.draw(generators::integers::<usize>().max_value(width - 2)) + 1;
    let value_position = (key_position + value_offset) % width;
    let join_syntax = match tc.draw(generators::integers::<u8>().max_value(2)) {
        0 => "LEFT JOIN r ON l.k = r.k",
        1 => "LEFT JOIN r USING(k)",
        _ => "NATURAL LEFT JOIN r",
    };
    let left_columns = (0..width)
        .map(|position| {
            if position == key_position {
                "k INTEGER".to_string()
            } else {
                format!("l{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let right_columns = (0..width)
        .map(|position| {
            if position == key_position {
                "k INTEGER".to_string()
            } else {
                format!("r{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let left_table = BTreeTable::from_sql(&format!("CREATE TABLE l({left_columns})"), 31)
        .expect("generated left table SQL is valid");
    let right_table = BTreeTable::from_sql(&format!("CREATE TABLE r({right_columns})"), 37)
        .expect("generated right table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(left_table))
        .expect("l is unique");
    schema
        .add_btree_table(Arc::new(right_table))
        .expect("r is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT l.l{value_position}, r.r{value_position} FROM l {join_syntax} WHERE r.r{value_position} IS NULL"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated LEFT JOIN has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let query = &document.queries[root.query.index()];
    let block = &query.blocks[query.first.index];
    let from = block.from.as_ref().expect("the query has FROM");
    assert_eq!(from.joins.len(), 1);
    assert_eq!(
        from.joins[0].kind,
        crate::translate::semantic::hir::JoinKind::Left
    );
    let QueryBlockBody::Select {
        filter: Some(_), ..
    } = &block.body
    else {
        panic!("WHERE remains a separate HIR expression");
    };

    let plan = PhysicalPlan::new(&document).expect("closed LEFT JOIN HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("LEFT JOIN emits from HIR");
    program
        .resolve_labels()
        .expect("all LEFT JOIN branches are closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    ..
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved table cursor is opened")
    };
    let right_cursor = cursor_for_root(37);
    let right_rewind = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Rewind { cursor_id, .. } if *cursor_id == right_cursor
            )
        })
        .expect("right side is scanned for each left row");
    let (match_zero, match_register) = program.insns[..right_rewind]
        .iter()
        .enumerate()
        .rev()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::Integer { value: 0, dest } => Some((position, *dest)),
            _ => None,
        })
        .expect("LEFT JOIN clears one match flag before its right scan");
    let match_one = program.insns[right_rewind + 1..]
        .iter()
        .enumerate()
        .find_map(|(offset, (instruction, _))| match instruction {
            Insn::Integer { value: 1, dest } if *dest == match_register => {
                Some(right_rewind + 1 + offset)
            }
            _ => None,
        })
        .expect("a right row marks the LEFT JOIN only after its join rule passes");
    let null_row = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::NullRow { cursor_id } if *cursor_id == right_cursor
            )
        })
        .expect("an unmatched left row null-extends the right SourceId");
    assert!(match_zero < right_rewind && right_rewind < match_one && match_one < null_row);
    assert!(program.insns[right_rewind..match_one]
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::IfNot { .. })));
    assert!(program.insns[match_one..null_row]
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::IfNot { .. })));
    assert!(program.insns[..null_row]
        .iter()
        .any(|(instruction, _)| matches!(
            instruction,
            Insn::IfPos { reg, .. } if *reg == match_register
        )));
    assert!(program.insns[null_row + 1..]
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::IfNot { .. })));
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
            .count(),
        2
    );
}

// Examples: `l RIGHT JOIN r USING(k)` must null-extend `l` for an unmatched
// right row; `l FULL JOIN r ON l.k = r.k` must additionally null-extend `r`
// for an unmatched left row. A later WHERE predicate runs only after either
// null extension.
#[hegel::test]
fn two_source_right_and_full_joins_preserve_the_hir_sides(tc: hegel::TestCase) {
    let full = tc.draw(generators::integers::<u8>().max_value(1)) == 1;
    let keyword = if full { "FULL JOIN" } else { "RIGHT JOIN" };
    let join = match (full, tc.draw(generators::integers::<u8>().max_value(2))) {
        (true, _) | (false, 0) => format!("{keyword} r ON l.k = r.k"),
        (false, 1) => format!("{keyword} r USING(k)"),
        (false, _) => format!("NATURAL {keyword} r"),
    };
    let left = BTreeTable::from_sql("CREATE TABLE l(k INTEGER, lv TEXT)", 41)
        .expect("left table SQL is valid");
    let right = BTreeTable::from_sql("CREATE TABLE r(k INTEGER, rv TEXT)", 43)
        .expect("right table SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(Arc::new(left)).expect("l is unique");
    schema
        .add_btree_table(Arc::new(right))
        .expect("r is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT l.k, l.lv, r.k, r.rv FROM l {join} WHERE l.lv IS NULL OR r.rv IS NULL"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated outer join has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let query = &document.queries[root.query.index()];
    let block = &query.blocks[query.first.index];
    let from = block.from.as_ref().expect("outer join has FROM");
    assert_eq!(
        from.joins[0].kind,
        if full {
            crate::translate::semantic::hir::JoinKind::Full
        } else {
            crate::translate::semantic::hir::JoinKind::Right
        }
    );

    let plan = PhysicalPlan::new(&document).expect("closed outer-join HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("outer join emits from HIR");
    program
        .resolve_labels()
        .expect("all outer-join branches are closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    ..
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved table cursor is opened")
    };
    let left_cursor = cursor_for_root(41);
    let right_cursor = cursor_for_root(43);
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::NullRow { cursor_id } if *cursor_id == left_cursor
    )));
    assert_eq!(
        program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::NullRow { cursor_id } if *cursor_id == right_cursor
        )),
        full
    );
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 4, .. })));
}

// Examples: `l FULL JOIN r USING(k)` and `l NATURAL FULL JOIN r` both bind the
// shared `k` column into HIR. The current executor cannot use that frozen merge
// rule as its FULL JOIN key, so physical planning must return the established
// compatibility error instead of rejecting SQL during binding or changing it.
#[hegel::test]
fn full_join_using_binds_before_the_physical_limit(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let key_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let natural = tc.draw(generators::integers::<u8>().max_value(1)) == 1;
    let left_columns = (0..width)
        .map(|position| {
            if position == key_position {
                "k INTEGER".to_string()
            } else {
                format!("l{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let right_columns = (0..width)
        .map(|position| {
            if position == key_position {
                "k INTEGER".to_string()
            } else {
                format!("r{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let left = BTreeTable::from_sql(&format!("CREATE TABLE l({left_columns})"), 41)
        .expect("generated left table SQL is valid");
    let right = BTreeTable::from_sql(&format!("CREATE TABLE r({right_columns})"), 43)
        .expect("generated right table SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(Arc::new(left)).expect("l is unique");
    schema
        .add_btree_table(Arc::new(right))
        .expect("r is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let join = if natural {
        "NATURAL FULL JOIN r"
    } else {
        "FULL JOIN r USING(k)"
    };
    let statement = parse_statement(&format!("SELECT * FROM l {join}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("USING and NATURAL FULL JOIN bind into closed HIR");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let query = &document.queries[root.query.index()];
    let block = &query.blocks[query.first.index];
    let join = &block.from.as_ref().expect("FULL JOIN has FROM").joins[0];
    assert_eq!(join.kind, crate::translate::semantic::hir::JoinKind::Full);
    match (&join.constraint, natural) {
        (crate::translate::semantic::hir::JoinConstraint::Natural(columns), true)
        | (crate::translate::semantic::hir::JoinConstraint::Using(columns), false) => {
            assert_eq!(columns.len(), 1);
        }
        _ => panic!("binding preserves the generated join rule"),
    }

    let error = PhysicalPlan::new(&document).expect_err("the executor cannot plan this join rule");
    assert_eq!(
        error,
        PhysicalPlanError::UnsupportedQuery(
            "FULL OUTER JOIN requires an equality condition in the ON clause"
        )
    );
}

// Examples:
// - `l FULL JOIN r ON l.k = r.k LEFT JOIN t ON coalesce(l.k, r.k) = t.k`
//   sends matched rows and both null-extended sides through the `t` join.
// - `l RIGHT JOIN r ON l.k = r.k JOIN t ON r.k = t.k` sends an unmatched
//   `r` row through the inner join after null-extending `l`.
#[hegel::test]
fn a_first_outer_join_feeds_every_row_into_the_remaining_join_chain(tc: hegel::TestCase) {
    let full = tc.draw(generators::booleans());
    let tail_is_left = tc.draw(generators::booleans());
    let outer_keyword = if full { "FULL JOIN" } else { "RIGHT JOIN" };
    let tail_keyword = if tail_is_left { "LEFT JOIN" } else { "JOIN" };
    let left =
        BTreeTable::from_sql("CREATE TABLE l(k INTEGER)", 47).expect("left table SQL is valid");
    let right =
        BTreeTable::from_sql("CREATE TABLE r(k INTEGER)", 53).expect("right table SQL is valid");
    let tail =
        BTreeTable::from_sql("CREATE TABLE t(k INTEGER)", 59).expect("tail table SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(Arc::new(left)).expect("l is unique");
    schema
        .add_btree_table(Arc::new(right))
        .expect("r is unique");
    schema.add_btree_table(Arc::new(tail)).expect("t is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT l.k, r.k, t.k FROM l {outer_keyword} r ON l.k = r.k \
         {tail_keyword} t ON coalesce(l.k, r.k) = t.k"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated outer-join chain has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let query = &document.queries[root.query.index()];
    let from = query.blocks[query.first.index]
        .from
        .as_ref()
        .expect("outer-join chain has FROM");
    assert_eq!(
        from.joins.iter().map(|join| join.kind).collect::<Vec<_>>(),
        [
            if full {
                crate::translate::semantic::hir::JoinKind::Full
            } else {
                crate::translate::semantic::hir::JoinKind::Right
            },
            if tail_is_left {
                crate::translate::semantic::hir::JoinKind::Left
            } else {
                crate::translate::semantic::hir::JoinKind::Inner
            },
        ]
    );

    let plan = PhysicalPlan::new(&document).expect("closed outer-join HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("outer-join chain emits from HIR");
    program
        .resolve_labels()
        .expect("all outer-join-chain branches are closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    ..
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved table cursor is opened")
    };
    let left_cursor = cursor_for_root(47);
    let right_cursor = cursor_for_root(53);
    let tail_cursor = cursor_for_root(59);
    let is_nulled = |cursor| {
        program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::NullRow { cursor_id } if *cursor_id == cursor)
        })
    };
    assert!(is_nulled(left_cursor));
    assert_eq!(is_nulled(right_cursor), full);
    assert_eq!(is_nulled(tail_cursor), tail_is_left);
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 3, .. })));
}

// Examples:
// - `l JOIN p ON l.k = p.k FULL JOIN r ON p.k = r.k` null-extends both `l`
//   and `p` when an `r` row has no match in the completed inner-join prefix.
// - `l JOIN p JOIN q FULL JOIN r LEFT JOIN t` sends that unmatched `r` row
//   through the later `t` join only after null-extending all three prefix tables.
#[hegel::test]
fn a_full_join_after_an_inner_prefix_preserves_the_prefix_as_one_left_side(tc: hegel::TestCase) {
    let extra_prefix_table = tc.draw(generators::booleans());
    let tail_is_left = tc.draw(generators::booleans());
    let extra_output = if extra_prefix_table { ", q.k" } else { "" };
    let extra_join = if extra_prefix_table {
        "JOIN q ON p.k = q.k"
    } else {
        ""
    };
    let prefix_last = if extra_prefix_table { "q" } else { "p" };
    let tail_keyword = if tail_is_left { "LEFT JOIN" } else { "JOIN" };
    let mut schema = Schema::new();
    for (sql, root_page) in [
        ("CREATE TABLE l(k INTEGER)", 61),
        ("CREATE TABLE p(k INTEGER)", 67),
        ("CREATE TABLE q(k INTEGER)", 71),
        ("CREATE TABLE r(k INTEGER)", 73),
        ("CREATE TABLE t(k INTEGER)", 79),
    ] {
        let table = BTreeTable::from_sql(sql, root_page).expect("fixture table SQL is valid");
        schema
            .add_btree_table(Arc::new(table))
            .expect("fixture table name is unique");
    }
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT l.k, p.k{extra_output}, r.k, t.k \
         FROM l JOIN p ON l.k = p.k {extra_join} \
         FULL JOIN r ON {prefix_last}.k = r.k \
         {tail_keyword} t ON coalesce({prefix_last}.k, r.k) = t.k"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated inner-prefix FULL join has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let query = &document.queries[root.query.index()];
    let from = query.blocks[query.first.index]
        .from
        .as_ref()
        .expect("join chain has FROM");
    let mut expected_kinds = vec![crate::translate::semantic::hir::JoinKind::Inner];
    if extra_prefix_table {
        expected_kinds.push(crate::translate::semantic::hir::JoinKind::Inner);
    }
    expected_kinds.push(crate::translate::semantic::hir::JoinKind::Full);
    expected_kinds.push(if tail_is_left {
        crate::translate::semantic::hir::JoinKind::Left
    } else {
        crate::translate::semantic::hir::JoinKind::Inner
    });
    assert_eq!(
        from.joins.iter().map(|join| join.kind).collect::<Vec<_>>(),
        expected_kinds
    );

    let plan = PhysicalPlan::new(&document).expect("closed join HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("inner-prefix FULL join emits from HIR");
    program
        .resolve_labels()
        .expect("all inner-prefix FULL join branches are closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    ..
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("participating table cursor is opened")
    };
    let is_nulled = |cursor| {
        program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::NullRow { cursor_id } if *cursor_id == cursor)
        })
    };
    assert!(is_nulled(cursor_for_root(61)));
    assert!(is_nulled(cursor_for_root(67)));
    if extra_prefix_table {
        assert!(is_nulled(cursor_for_root(71)));
    }
    assert!(is_nulled(cursor_for_root(73)));
    assert_eq!(is_nulled(cursor_for_root(79)), tail_is_left);
    let output_width = if extra_prefix_table { 5 } else { 4 };
    assert!(program.insns.iter().any(
        |(instruction, _)| matches!(instruction, Insn::ResultRow { count, .. } if *count == output_width)
    ));
}

// Example: `SELECT l.c0, r.c1 FROM l FULL JOIN r ON l.c2 = r.c2
// WHERE r.c3 IN (SELECT v.c4 FROM v)` can enter the matched, unmatched-left,
// or unmatched-right filter first. The one uncorrelated `IN` row set must be
// open before all three paths, and every path must reuse that same cursor.
#[hegel::test]
fn uncorrelated_in_subqueries_are_ready_before_every_full_join_filter_path(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let left_output = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let right_output = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let join_column = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let filter_column = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let subquery_column = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut schema = Schema::new();
    for (name, root_page) in [("l", 83), ("r", 89), ("v", 97)] {
        let table = BTreeTable::from_sql(&format!("CREATE TABLE {name}({columns})"), root_page)
            .expect("generated table SQL is valid");
        schema
            .add_btree_table(Arc::new(table))
            .expect("fixture table name is unique");
    }
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT l.c{left_output}, r.c{right_output} \
         FROM l FULL JOIN r ON l.c{join_column} = r.c{join_column} \
         WHERE r.c{filter_column} IN (SELECT v.c{subquery_column} FROM v)"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated FULL join with an IN subquery has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed FULL-join HIR has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("FULL join with an IN subquery emits from HIR");
    program
        .resolve_labels()
        .expect("all FULL-join and IN-subquery branches are closed");

    let cursor_for_root = |root_page| {
        program
            .insns
            .iter()
            .find_map(|(instruction, _)| match instruction {
                Insn::OpenRead {
                    cursor_id,
                    root_page: actual,
                    ..
                } if *actual == root_page => Some(*cursor_id),
                _ => None,
            })
            .expect("resolved table cursor is opened")
    };
    let left_cursor = cursor_for_root(83);
    let right_cursor = cursor_for_root(89);
    let (row_set_open, row_set_cursor) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            } => Some((position, *cursor_id)),
            _ => None,
        })
        .expect("the IN query opens one row set");
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::OpenEphemeral { cursor_id, .. } if *cursor_id == row_set_cursor
            ))
            .count(),
        1
    );
    for outer_cursor in [left_cursor, right_cursor] {
        let outer_rewind = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(
                    instruction,
                    Insn::Rewind { cursor_id, .. } if *cursor_id == outer_cursor
                )
            })
            .expect("each FULL-join side starts a scan");
        assert!(row_set_open < outer_rewind);
    }
    assert!(program
        .insns
        .iter()
        .enumerate()
        .filter(|(_, (instruction, _))| matches!(
            instruction,
            Insn::Rewind { cursor_id, .. } if *cursor_id == row_set_cursor
        ))
        .all(|(position, _)| row_set_open < position));
}

// Examples:
// - `l FULL JOIN r ON l.c0 = r.c0 WHERE EXISTS
//   (SELECT 1 FROM v WHERE v.c1 = l.c2)` captures the left source.
// - `l FULL JOIN r ON l.c0 = r.c0 WHERE
//   (SELECT v.c1 FROM v WHERE v.c1 = r.c2) IS NOT NULL` captures the right.
// - `l.c2 IN (SELECT v.c1 FROM v WHERE v.c1 = r.c2)` captures through `IN`.
// Binding must preserve the exact captured SourceId. Physical planning must
// then return main's established FULL JOIN limit, while an uncorrelated child
// remains supported by the property above.
#[hegel::test]
fn correlated_subqueries_with_full_join_keep_their_binding_before_rejection(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let join_column = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let capture_column = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let inner_column = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let capture_left = tc.draw(generators::booleans());
    let captured_alias = if capture_left { "l" } else { "r" };
    let subquery_form = tc.draw(generators::integers::<u8>().max_value(2));
    let filter = match subquery_form {
        0 => format!(
            "EXISTS (SELECT 1 FROM v WHERE v.c{inner_column} = \
             {captured_alias}.c{capture_column})"
        ),
        1 => format!(
            "(SELECT v.c{inner_column} FROM v WHERE v.c{inner_column} = \
             {captured_alias}.c{capture_column}) IS NOT NULL"
        ),
        _ => format!(
            "{captured_alias}.c{capture_column} IN \
             (SELECT v.c{inner_column} FROM v WHERE v.c{inner_column} = \
             {captured_alias}.c{capture_column})"
        ),
    };
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut schema = Schema::new();
    for (name, root_page) in [("l", 101), ("r", 103), ("v", 107)] {
        let table = BTreeTable::from_sql(&format!("CREATE TABLE {name}({columns})"), root_page)
            .expect("generated table SQL is valid");
        schema
            .add_btree_table(Arc::new(table))
            .expect("fixture table name is unique");
    }
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "SELECT l.c{capture_column}, r.c{capture_column} \
         FROM l FULL JOIN r ON l.c{join_column} = r.c{join_column} \
         WHERE {filter}"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("correlated FULL-join subquery binds into closed HIR");
    drop(context);
    drop(schema);
    drop(symbols);

    let HirRoot::Query(root) = &document.root else {
        panic!("the fixture is a query");
    };
    let outer = &document.queries[root.query.index()];
    let from = outer.blocks[outer.first.index]
        .from
        .as_ref()
        .expect("FULL JOIN has FROM");
    let captured_source = if capture_left {
        from.first
    } else {
        from.joins[0].right
    };
    assert!(document
        .queries
        .iter()
        .filter(|query| query.parent == Some(outer.id))
        .any(|query| query.captures.contains(&captured_source)));

    assert!(matches!(
        PhysicalPlan::new(&document),
        Err(PhysicalPlanError::UnsupportedQuery(_))
    ));
}

// Examples:
// - `SELECT (SELECT 7 UNION ALL SELECT 9)` returns the first compound row, so
//   later arms must not overwrite the scalar result after an early jump.
// - `SELECT EXISTS(SELECT 1 WHERE 0 UNION ALL SELECT 2 WHERE 1)` is true even
//   when the first arm is empty. Materializing behind QueryId makes both rules
//   independent of arm-local cursor cleanup.
#[hegel::test]
fn compound_scalar_and_exists_subqueries_read_one_materialized_hir_result(tc: hegel::TestCase) {
    let first = i64::from(tc.draw(generators::integers::<u8>().max_value(63)));
    let second = first + 1;
    let first_exists = tc.draw(generators::booleans());
    let sql = format!(
        "SELECT (SELECT {first} UNION ALL SELECT {second}), \
         EXISTS(SELECT 1 WHERE {} UNION ALL SELECT 2 WHERE {})",
        i32::from(first_exists),
        i32::from(!first_exists),
    );
    let statement = parse_statement(&sql);
    let schema = Schema::new();
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("compound subqueries have valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed compounds have a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("compound subqueries emit without a resolver");
    program
        .resolve_labels()
        .expect("all compound-subquery branches are closed");

    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(instruction, Insn::Insert { table_name, .. } if table_name.starts_with("scalar_compound_"))
    }));
    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(instruction, Insn::Insert { table_name, .. } if table_name.starts_with("exists_compound_"))
    }));
}

// Examples:
// - `SELECT group_concat(x ORDER BY rank ASC) FROM items` must feed AggStep
//   only after the HIR ordering sorter is drained.
// - `SELECT g, group_concat(DISTINCT x ORDER BY rank DESC) FROM items GROUP BY g`
//   needs a fresh argument sorter and duplicate set for every group; the group
//   sorter must not replace the aggregate's independent ordering.
// Varying grouping, DISTINCT, and direction checks that the FunctionCall's
// stable AggregateId owns all of these runtime resources without rebinding.
#[hegel::test]
fn aggregate_argument_order_drains_before_hir_aggregate_steps(tc: hegel::TestCase) {
    let grouped = tc.draw(generators::booleans());
    let distinct = tc.draw(generators::booleans());
    let descending = tc.draw(generators::booleans());
    let items = BTreeTable::from_sql("CREATE TABLE items(g INTEGER, x TEXT, rank INTEGER)", 47)
        .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(items))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let distinct = if distinct { "DISTINCT " } else { "" };
    let direction = if descending { "DESC" } else { "ASC" };
    let sql = if grouped {
        format!(
            "SELECT g, group_concat({distinct}x ORDER BY rank {direction}) \
             FROM items GROUP BY g"
        )
    } else {
        format!(
            "SELECT group_concat({distinct}x ORDER BY rank {direction}) \
             FROM items"
        )
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("ordered aggregate has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("ordered aggregate has a physical plan");
    let mut program = program();
    emit_root_query(&plan, &mut program).expect("ordered aggregate emits from closed HIR");
    program
        .resolve_labels()
        .expect("all ordered aggregate branches are closed");

    let step = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::AggStep { .. }))
        .expect("ordered values eventually step the aggregate");
    let final_position = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::AggFinal { .. }))
        .expect("the ordered aggregate is finalized");
    let drain = program.insns[..step]
        .iter()
        .rposition(|(instruction, _)| matches!(instruction, Insn::SorterSort { .. }))
        .expect("an argument sorter is drained before AggStep");
    let insert = program.insns[..drain]
        .iter()
        .rposition(|(instruction, _)| matches!(instruction, Insn::SorterInsert { .. }))
        .expect("argument values enter a sorter before it is drained");
    assert!(insert < drain && drain < step && step < final_position);
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::SorterOpen { .. }))
            .count(),
        if grouped { 2 } else { 1 }
    );
    if distinct == "DISTINCT " {
        assert!(program.insns[drain..step]
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
    }
}
