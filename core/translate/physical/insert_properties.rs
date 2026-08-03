//! Properties for direct INSERT emission from closed HIR.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Schema, Sequence},
    sync::Arc,
    translate::semantic::{
        analyze,
        context::{DmlPolicy, SemanticContext},
        hir::{ColumnReadExpression, HirRoot, InsertSource, InsertTarget, TargetColumn},
        AnalyzeInput,
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts},
        insn::Insn,
    },
    QueryMode, SymbolTable,
};

use super::*;

// Example: `INSERT INTO t(a, c) VALUES (1, 3)` keeps the supplied `a` and `c`
// values and evaluates defaults only for omitted columns such as `b`.
#[hegel::test]
fn insert_defaults_fill_exactly_the_omitted_columns(tc: hegel::TestCase) {
    let supplied = tc.draw(
        generators::vecs(generators::booleans())
            .min_size(1)
            .max_size(16),
    );
    let columns = supplied
        .iter()
        .enumerate()
        .filter_map(|(position, supplied)| {
            supplied.then_some(InsertTarget {
                column: TargetColumn::Column(position),
                uses_value: true,
            })
        })
        .collect::<Vec<_>>();

    for (position, supplied) in supplied.into_iter().enumerate() {
        assert_eq!(column_needs_default(&columns, position), !supplied);
    }
}

// Examples:
// - `INSERT INTO t DEFAULT VALUES` evaluates every column default even though
//   the implicit target list names every writable column.
// - `INSERT INTO t(a) VALUES (1)` keeps `a` and evaluates only omitted fields.
// For every generated target position, DEFAULT VALUES must win over the
// implicit column list while VALUES must preserve supplied positions.
#[hegel::test]
fn default_values_evaluates_every_frozen_default(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)));
    let position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let supplied = tc.draw(generators::booleans());
    let default_values = tc.draw(generators::booleans());
    let columns = supplied
        .then_some(InsertTarget {
            column: TargetColumn::Column(position),
            uses_value: true,
        })
        .into_iter()
        .collect::<Vec<_>>();
    let source = if default_values {
        InsertSource::DefaultValues
    } else {
        InsertSource::Values(vec![Vec::new()])
    };

    assert_eq!(
        insert_column_needs_default(&source, &columns, position),
        default_values || !supplied
    );
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

// Examples: `INSERT INTO children(c2) VALUES (7)` probes the frozen
// `parents.p1 INTEGER PRIMARY KEY` rowid before writing the child; inserting
// NULL skips the probe, and `INSERT INTO node(id,parent) VALUES(4,4)` accepts
// the NEW row as its own parent. The generated position proves the emitter
// uses HIR offsets, not a new lookup of the SQL column names.
#[hegel::test]
fn insert_child_foreign_keys_probe_the_frozen_parent_position(tc: hegel::TestCase) {
    let child_width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(10)));
    let parent_width =
        usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(10)));
    let child_position = tc.draw(generators::integers::<usize>().max_value(child_width - 1));
    let parent_position = tc.draw(generators::integers::<usize>().max_value(parent_width - 1));
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
    let parent = BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 17)
        .expect("generated parent table is valid");
    let child = BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 19)
        .expect("generated child table is valid");
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
    let statement = parse_statement(&format!(
        "INSERT INTO children(c{child_position}) VALUES (7)"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated FK insert has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed FK INSERT has a physical plan");
    let mut program = program();
    emit_root_insert(&plan, &mut program).expect("child FK emits without a resolver");
    program
        .resolve_labels()
        .expect("all child FK branches are closed");

    let (parent_open, parent_cursor) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 17,
                ..
            } => Some((position, *cursor_id)),
            _ => None,
        })
        .expect("the frozen parent table is opened directly");
    let probe = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::NotExists { cursor, .. } if *cursor == parent_cursor
            )
        })
        .expect("the parent rowid is probed");
    let child_write = program
        .insns
        .iter()
        .rposition(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Insert { table_name, .. } if table_name == "children"
            )
        })
        .expect("the child row is written");
    assert!(parent_open < probe && probe < child_write);
    assert!(program.insns[probe..child_write]
        .iter()
        .any(|(instruction, _)| matches!(
            instruction,
            Insn::FkCounter {
                increment_value: 1,
                ..
            }
        )));
}

// Example: for
// `items(c0, c1 DEFAULT 11, c2, c3 AS (c0 + c1) VIRTUAL)`,
// `INSERT INTO items(c0, c2) VALUES (7, 9), (8, 10)` must build each complete
// logical row from the exact HIR positions: supplied c0/c2, frozen default c1,
// then frozen generated c3. Once the schema is dropped, emission must still
// compute the complete four-field logical row, then make one three-field
// stored record and one table insert for every VALUES row.
#[hegel::test]
fn values_insert_builds_complete_rows_from_hir(tc: hegel::TestCase) {
    let default_value =
        i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31))) + 40;
    let row_count = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(5)));
    let rows = (0..row_count)
        .map(|position| format!("({}, {})", 100 + position, 200 + position))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(
        &format!(
            "CREATE TABLE items(\
             c0 INTEGER, \
             c1 INTEGER DEFAULT {default_value}, \
             c2 INTEGER, \
             c3 INTEGER GENERATED ALWAYS AS (c0 + c1) VIRTUAL)"
        ),
        9,
    )
    .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("INSERT INTO items(c0, c2) VALUES {rows}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated INSERT has valid SQL meaning");
    let HirRoot::Insert(insert) = &document.root else {
        panic!("INSERT syntax produces an INSERT HIR root");
    };
    assert!(matches!(
        insert.columns.as_slice(),
        [
            InsertTarget {
                column: TargetColumn::Column(0),
                uses_value: true
            },
            InsertTarget {
                column: TargetColumn::Column(2),
                uses_value: true
            }
        ]
    ));
    assert!(insert.defaults.iter().any(|default| default.column == 1));
    assert!(matches!(&insert.source, InsertSource::Values(values) if values.len() == row_count));
    let target = document
        .source(insert.target)
        .expect("target source exists");
    assert!(matches!(
        target.generated_expressions[3],
        ColumnReadExpression::Planned(_)
    ));
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("VALUES INSERT lowers without a catalog");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::NewRowid { .. }))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::Integer { value, .. } if *value == default_value
            ))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::MakeRecord { count: 3, .. }))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::Insert { table_name, .. } if table_name == "items"
            ))
            .count(),
        row_count
    );
}

// Examples:
// - `INSERT INTO items(rowid, c0) VALUES (7, 1)` must validate rowid 7 as an
//   integer and reject an existing table key before any secondary-index write.
// - `INSERT INTO items(id, c0) VALUES (NULL, 1)` for
//   `id INTEGER PRIMARY KEY` must generate a rowid, copy it into logical id,
//   store NULL in id's record field, and use that rowid as the table key.
#[hegel::test]
fn explicit_rowid_and_integer_primary_key_share_one_key_path(tc: hegel::TestCase) {
    let alias = tc.draw(generators::booleans());
    let null_key = tc.draw(generators::booleans());
    let key = if null_key { "NULL" } else { "7" };
    let create = if alias {
        "CREATE TABLE items(id INTEGER PRIMARY KEY, c0 INTEGER)"
    } else {
        "CREATE TABLE items(c0 INTEGER)"
    };
    let insert = if alias {
        format!("INSERT INTO items(id, c0) VALUES ({key}, 1)")
    } else {
        format!("INSERT INTO items(rowid, c0) VALUES ({key}, 1)")
    };
    let table = BTreeTable::from_sql(create, 9).expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&insert);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated INSERT has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("explicit-rowid INSERT lowers without a catalog");
    program
        .resolve_labels()
        .expect("all rowid branches are closed");

    let positions = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (instruction, _))| predicate(instruction).then_some(position))
            .collect::<Vec<_>>()
    };
    let new_rowid = positions(&|instruction| matches!(instruction, Insn::NewRowid { .. }));
    let must_be_int = positions(&|instruction| matches!(instruction, Insn::MustBeInt { .. }));
    let uniqueness = positions(&|instruction| matches!(instruction, Insn::NotExists { .. }));
    let table_insert = positions(&|instruction| matches!(instruction, Insn::Insert { .. }));
    assert_eq!(new_rowid.len(), 1);
    assert_eq!(must_be_int.len(), 1);
    assert_eq!(uniqueness.len(), 1);
    assert_eq!(table_insert.len(), 1);
    assert!(uniqueness[0] < table_insert[0]);
    if alias {
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SoftNull { .. })));
    }
}

// Examples: for
// `CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, value)`, both
// `INSERT INTO items(value) VALUES (7)` and `INSERT INTO items DEFAULT VALUES`
// must allocate `id` through `__turso_internal_autoincrement_items`, update the
// frozen backing table and `sqlite_sequence`, and keep working after the
// prepare-time catalog is gone.
#[hegel::test]
fn mvcc_autoincrement_lowers_from_the_frozen_sequence_operation(tc: hegel::TestCase) {
    let value = i64::from(tc.draw(generators::integers::<u16>())) + 1;
    let default_values = tc.draw(generators::booleans());
    let target = BTreeTable::from_sql(
        "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, value INTEGER)",
        9,
    )
    .expect("fixture target SQL is valid");
    let sqlite_sequence = BTreeTable::from_sql("CREATE TABLE sqlite_sequence(name,seq)", 10)
        .expect("fixture sqlite_sequence SQL is valid");
    let sequence_name = crate::schema::autoincrement_sequence_name("items");
    let backing = BTreeTable::from_sql(
        &crate::translate::sequence::sequence_backing_table_sql(&sequence_name),
        11,
    )
    .expect("fixture sequence backing-table SQL is valid");
    let sequence = Arc::new(
        Sequence::new(sequence_name.clone(), Some(1), Some(1), None, None, false)
            .expect("AUTOINCREMENT sequence bounds are valid"),
    );
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(target))
        .expect("items is unique");
    schema
        .add_btree_table(Arc::new(sqlite_sequence))
        .expect("sqlite_sequence is unique");
    schema
        .add_btree_table(Arc::new(backing))
        .expect("the sequence backing table is unique");
    schema.sequences.insert(sequence_name.clone(), sequence);
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if default_values {
        "INSERT INTO items DEFAULT VALUES".to_string()
    } else {
        format!("INSERT INTO items(value) VALUES ({value})")
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated AUTOINCREMENT INSERT has valid SQL meaning");
    let HirRoot::Insert(insert) = &document.root else {
        panic!("INSERT syntax produces an INSERT HIR root");
    };
    let operation = insert
        .autoincrement_sequence
        .as_ref()
        .expect("MVCC AUTOINCREMENT carries its hidden sequence operation");
    assert_eq!(operation.normalized_name, sequence_name);
    assert_eq!(
        operation
            .backing_table
            .value()
            .get_root_page()
            .expect("the backing object is a B-tree table"),
        11
    );
    assert_eq!(
        operation
            .sqlite_sequence
            .as_ref()
            .expect("AUTOINCREMENT carries sqlite_sequence")
            .value()
            .get_root_page()
            .expect("sqlite_sequence is a B-tree table"),
        10
    );
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    program.set_mvcc_enabled(true);
    emit_root(&plan, &mut program).expect("AUTOINCREMENT INSERT lowers without a catalog");

    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::SequenceComputeNext { .. })));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::OpenWrite {
            root_page: crate::vdbe::insn::RegisterOrLiteral::Literal(11),
            ..
        }
    )));
}

// Examples: in the ordinary rollback-journal engine,
// `INSERT INTO items DEFAULT VALUES` reads and advances the frozen
// `sqlite_sequence` row, while `INSERT INTO items(id) VALUES (-7)` still
// creates the missing row at zero. Even when HIR also carries the MVCC hidden
// sequence, this mode must use `sqlite_sequence` so manual SQL updates to
// `sqlite_sequence.seq` control the next generated key.
#[hegel::test]
fn ordinary_autoincrement_uses_the_frozen_sqlite_sequence_table(tc: hegel::TestCase) {
    let explicit_negative = tc.draw(generators::booleans());
    let magnitude = i64::from(tc.draw(generators::integers::<u16>())) + 1;
    let target = BTreeTable::from_sql(
        "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, value INTEGER)",
        9,
    )
    .expect("fixture target SQL is valid");
    let sqlite_sequence = BTreeTable::from_sql("CREATE TABLE sqlite_sequence(name,seq)", 10)
        .expect("fixture sqlite_sequence SQL is valid");
    let sequence_name = crate::schema::autoincrement_sequence_name("items");
    let backing = BTreeTable::from_sql(
        &crate::translate::sequence::sequence_backing_table_sql(&sequence_name),
        11,
    )
    .expect("fixture sequence backing-table SQL is valid");
    let sequence = Arc::new(
        Sequence::new(sequence_name.clone(), Some(1), Some(1), None, None, false)
            .expect("AUTOINCREMENT sequence bounds are valid"),
    );
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(target))
        .expect("items is unique");
    schema
        .add_btree_table(Arc::new(sqlite_sequence))
        .expect("sqlite_sequence is unique");
    schema
        .add_btree_table(Arc::new(backing))
        .expect("the sequence backing table is unique");
    schema.sequences.insert(sequence_name, sequence);
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if explicit_negative {
        format!("INSERT INTO items(id) VALUES (-{magnitude})")
    } else {
        "INSERT INTO items DEFAULT VALUES".to_string()
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated AUTOINCREMENT INSERT has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("ordinary AUTOINCREMENT lowers without a catalog");
    program
        .resolve_labels()
        .expect("all AUTOINCREMENT branches are closed");

    assert!(!program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::SequenceComputeNext { .. })));
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::OpenWrite {
            root_page: crate::vdbe::insn::RegisterOrLiteral::Literal(10),
            ..
        }
    )));
    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(instruction, Insn::Insert { table_name, .. } if table_name == "sqlite_sequence")
    }));
}

// Example: `INSERT INTO items(c0, c1) SELECT c0 + 7, c1 FROM items WHERE c1`
// must finish the HIR SELECT into an ephemeral rowset before opening items for
// writing. The write loop then reads the exact two query positions, applies the
// ordinary INSERT row/constraint/index path, and cannot see rows it just added.
#[hegel::test]
fn insert_select_materializes_hir_rows_before_opening_the_target(tc: hegel::TestCase) {
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let table = BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER)", 9)
        .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "INSERT INTO items(c0, c1) SELECT c0 + {offset}, c1 FROM items WHERE c1"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated INSERT SELECT has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("INSERT SELECT lowers without a catalog");
    program
        .resolve_labels()
        .expect("all query and write-loop branches are closed");

    let target_open = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::OpenWrite {
                    root_page: crate::vdbe::insn::RegisterOrLiteral::Literal(9),
                    ..
                }
            )
        })
        .expect("target table is opened for writing");
    let materialized_insert = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name.starts_with("dml_query_"))
        })
        .expect("query rows are materialized");
    let target_insert = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items")
        })
        .expect("materialized rows enter the target");
    assert!(materialized_insert < target_open && target_open < target_insert);
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { is_table: true, .. })));
}
