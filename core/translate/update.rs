use std::collections::HashMap;
use std::sync::Arc;

use crate::schema::{BTreeTable, Column, Type};
use crate::translate::optimizer::optimize_select_plan;
use crate::translate::plan::{Operation, QueryDestination, Scan, Search, SelectPlan};
use crate::translate::planner::parse_limit;
use crate::vdbe::builder::CursorType;
use crate::{
    bail_parse_error,
    schema::{Schema, Table},
    util::normalize_ident,
    vdbe::builder::{ProgramBuilder, ProgramBuilderOpts},
    SymbolTable,
};
use turso_parser::ast::{self, Expr, Indexed, SortOrder};

use super::emitter::emit_program;
use super::expr::process_returning_clause;
use super::optimizer::optimize_plan;
use super::plan::{
    ColumnUsedMask, IterationDirection, JoinedTable, Plan, ResultSetColumn, TableReferences,
    UpdatePlan,
};
use super::planner::{bind_column_references, parse_where};
/*
* Update is simple. By default we scan the table, and for each row, we check the WHERE
* clause. If it evaluates to true, we build the new record with the updated value and insert.
*
* EXAMPLE:
*
sqlite> explain update t set a = 100 where b = 5;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     16    0                    0   Start at 16
1     Null           0     1     2                    0   r[1..2]=NULL
2     Noop           1     0     1                    0
3     OpenWrite      0     2     0     3              0   root=2 iDb=0; t
4     Rewind         0     15    0                    0
5       Column         0     1     6                    0   r[6]= cursor 0 column 1
6       Ne             7     14    6     BINARY-8       81  if r[6]!=r[7] goto 14
7       Rowid          0     2     0                    0   r[2]= rowid of 0
8       IsNull         2     15    0                    0   if r[2]==NULL goto 15
9       Integer        100   3     0                    0   r[3]=100
10      Column         0     1     4                    0   r[4]= cursor 0 column 1
11      Column         0     2     5                    0   r[5]= cursor 0 column 2
12      MakeRecord     3     3     1                    0   r[1]=mkrec(r[3..5])
13      Insert         0     1     2     t              7   intkey=r[2] data=r[1]
14    Next           0     5     0                    1
15    Halt           0     0     0                    0
16    Transaction    0     1     1     0              1   usesStmtJournal=0
17    Integer        5     7     0                    0   r[7]=5
18    Goto           0     1     0                    0
*/
pub fn translate_update(
    schema: &Schema,
    body: &mut ast::Update,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> crate::Result<ProgramBuilder> {
    let mut plan = prepare_update_plan(&mut program, schema, body, connection, false)?;
    optimize_plan(&mut plan, schema)?;
    // TODO: freestyling these numbers
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 20,
        approx_num_labels: 4,
    };
    program.extend(&opts);
    emit_program(&mut program, plan, schema, syms, |_| {})?;
    Ok(program)
}

pub fn translate_update_for_schema_change(
    schema: &Schema,
    body: &mut ast::Update,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
    ddl_query: &str,
    after: impl FnOnce(&mut ProgramBuilder),
) -> crate::Result<ProgramBuilder> {
    let mut plan = prepare_update_plan(&mut program, schema, body, connection, true)?;

    if let Plan::Update(plan) = &mut plan {
        if program.capture_data_changes_mode().has_updates() {
            plan.cdc_update_alter_statement = Some(ddl_query.to_string());
        }
    }

    optimize_plan(&mut plan, schema)?;
    // TODO: freestyling these numbers
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 20,
        approx_num_labels: 4,
    };
    program.extend(&opts);
    emit_program(&mut program, plan, schema, syms, after)?;
    Ok(program)
}

pub fn prepare_update_plan(
    program: &mut ProgramBuilder,
    schema: &Schema,
    body: &mut ast::Update,
    connection: &Arc<crate::Connection>,
    is_internal_schema_change: bool,
) -> crate::Result<Plan> {
    if body.with.is_some() {
        bail_parse_error!("WITH clause is not supported in UPDATE");
    }
    if body.or_conflict.is_some() {
        bail_parse_error!("ON CONFLICT clause is not supported in UPDATE");
    }
    if body
        .indexed
        .as_ref()
        .is_some_and(|i| matches!(i, Indexed::IndexedBy(_)))
    {
        bail_parse_error!("INDEXED BY clause is not supported in UPDATE");
    }
    let table_name = &body.tbl_name.name;

    // Check if this is a system table that should be protected from direct writes
    // Skip this check for internal schema change operations (like ALTER TABLE)
    if !is_internal_schema_change && crate::schema::is_system_table(table_name.as_str()) {
        bail_parse_error!("table {} may not be modified", table_name);
    }

    if schema.table_has_indexes(&table_name.to_string()) && !schema.indexes_enabled() {
        // Let's disable altering a table with indices altogether instead of checking column by
        // column to be extra safe.
        bail_parse_error!(
            "UPDATE table disabled for table with indexes is disabled. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }
    let table = match schema.get_table(table_name.as_str()) {
        Some(table) => table,
        None => bail_parse_error!("Parse error: no such table: {}", table_name),
    };

    // Check if this is a materialized view
    if schema.is_materialized_view(table_name.as_str()) {
        bail_parse_error!("cannot modify materialized view {}", table_name);
    }

    let table_name = table.get_name();
    let iter_dir = body
        .order_by
        .first()
        .and_then(|ob| {
            ob.order.map(|o| match o {
                SortOrder::Asc => IterationDirection::Forwards,
                SortOrder::Desc => IterationDirection::Backwards,
            })
        })
        .unwrap_or(IterationDirection::Forwards);

    let joined_tables = vec![JoinedTable {
        table: match table.as_ref() {
            Table::Virtual(vtab) => Table::Virtual(vtab.clone()),
            Table::BTree(btree_table) => Table::BTree(btree_table.clone()),
            _ => unreachable!(),
        },
        identifier: table_name.to_string(),
        internal_id: program.table_reference_counter.next(),
        op: build_scan_op(&table, iter_dir),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        database_id: 0,
    }];
    let mut table_references = TableReferences::new(joined_tables, vec![]);

    let column_lookup: HashMap<String, usize> = table
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, col)| col.name.as_ref().map(|name| (name.to_lowercase(), i)))
        .collect();

    let mut set_clauses = Vec::with_capacity(body.sets.len());

    // Process each SET assignment and map column names to expressions
    // e.g the statement `SET x = 1, y = 2, z = 3` has 3 set assigments
    for set in &mut body.sets {
        bind_column_references(&mut set.expr, &mut table_references, None, connection)?;

        let values = match set.expr.as_ref() {
            Expr::Parenthesized(vals) => vals.clone(),
            expr => vec![expr.clone().into()],
        };

        if set.col_names.len() != values.len() {
            bail_parse_error!(
                "{} columns assigned {} values",
                set.col_names.len(),
                values.len()
            );
        }

        // Map each column to its corresponding expression
        for (col_name, expr) in set.col_names.iter().zip(values.iter()) {
            let ident = normalize_ident(col_name.as_str());
            let col_index = match column_lookup.get(&ident) {
                Some(idx) => idx,
                None => bail_parse_error!("no such column: {}", ident),
            };

            // Update existing entry or add new one
            match set_clauses.iter_mut().find(|(idx, _)| idx == col_index) {
                Some((_, existing_expr)) => *existing_expr = expr.clone(),
                None => set_clauses.push((*col_index, expr.clone())),
            }
        }
    }

    let (result_columns, _table_references) = process_returning_clause(
        &mut body.returning,
        &table,
        body.tbl_name.name.as_str(),
        program,
        connection,
    )?;

    let order_by = body
        .order_by
        .iter()
        .map(|o| (o.expr.clone(), o.order.unwrap_or(SortOrder::Asc)))
        .collect();

    // Sqlite determines we should create an ephemeral table if we do not have a FROM clause
    // Difficult to say what items from the plan can be checked for this so currently just checking if a RowId Alias is referenced
    // https://github.com/sqlite/sqlite/blob/master/src/update.c#L395
    // https://github.com/sqlite/sqlite/blob/master/src/update.c#L670
    let columns = table.columns();

    let rowid_alias_used = set_clauses.iter().fold(false, |accum, (idx, _)| {
        accum || columns[*idx].is_rowid_alias
    });

    let (ephemeral_plan, mut where_clause) = if rowid_alias_used {
        let mut where_clause = vec![];
        let internal_id = program.table_reference_counter.next();

        let joined_tables = vec![JoinedTable {
            table: match table.as_ref() {
                Table::Virtual(vtab) => Table::Virtual(vtab.clone()),
                Table::BTree(btree_table) => Table::BTree(btree_table.clone()),
                _ => unreachable!(),
            },
            identifier: table_name.to_string(),
            internal_id,
            op: build_scan_op(&table, iter_dir),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
        }];
        let mut table_references = TableReferences::new(joined_tables, vec![]);

        // Parse the WHERE clause
        parse_where(
            body.where_clause.as_deref(),
            &mut table_references,
            Some(&result_columns),
            &mut where_clause,
            connection,
        )?;

        let table = Arc::new(BTreeTable {
            root_page: 0, // Not relevant for ephemeral table definition
            name: "ephemeral_scratch".to_string(),
            has_rowid: true,
            primary_key_columns: vec![],
            columns: vec![Column {
                name: Some("rowid".to_string()),
                ty: Type::Integer,
                ty_str: "INTEGER".to_string(),
                primary_key: true,
                is_rowid_alias: false,
                notnull: true,
                default: None,
                unique: false,
                collation: None,
                hidden: false,
            }],
            is_strict: false,
            unique_sets: None,
        });

        let temp_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));

        let mut ephemeral_plan = SelectPlan {
            table_references,
            result_columns: vec![ResultSetColumn {
                expr: Expr::RowId {
                    database: None,
                    table: internal_id,
                },
                alias: None,
                contains_aggregates: false,
            }],
            where_clause,       // original WHERE terms from the UPDATE clause
            group_by: None,     // N/A
            order_by: vec![],   // N/A
            aggregates: vec![], // N/A
            limit: None,        // N/A
            query_destination: QueryDestination::EphemeralTable {
                cursor_id: temp_cursor_id,
                table,
            },
            join_order: vec![],
            offset: None,
            contains_constant_false_condition: false,
            distinctness: super::plan::Distinctness::NonDistinct,
            values: vec![],
        };

        optimize_select_plan(&mut ephemeral_plan, schema)?;
        let table = ephemeral_plan
            .table_references
            .joined_tables()
            .first()
            .unwrap();
        // We do not need to emit an ephemeral plan if we are not going to loop over the table values
        if matches!(table.op, Operation::Search(Search::RowidEq { .. })) {
            (None, vec![])
        } else {
            (Some(ephemeral_plan), vec![])
        }
    } else {
        (None, vec![])
    };

    if ephemeral_plan.is_none() {
        // Parse the WHERE clause
        parse_where(
            body.where_clause.as_deref(),
            &mut table_references,
            Some(&result_columns),
            &mut where_clause,
            connection,
        )?;
    };

    // Parse the LIMIT/OFFSET clause
    let (limit, offset) = body
        .limit
        .as_mut()
        .map_or(Ok((None, None)), |l| parse_limit(l, connection))?;

    // Check what indexes will need to be updated by checking set_clauses and see
    // if a column is contained in an index.
    let indexes = schema.get_indices(table_name);
    let rowid_alias_used = set_clauses
        .iter()
        .any(|(idx, _)| columns[*idx].is_rowid_alias);
    let indexes_to_update = if rowid_alias_used {
        // If the rowid alias is used in the SET clause, we need to update all indexes
        indexes.to_vec()
    } else {
        // otherwise we need to update the indexes whose columns are set in the SET clause
        indexes
            .iter()
            .filter(|index| {
                index.columns.iter().any(|index_column| {
                    set_clauses
                        .iter()
                        .any(|(set_index_column, _)| index_column.pos_in_table == *set_index_column)
                })
            })
            .cloned()
            .collect()
    };

    Ok(Plan::Update(UpdatePlan {
        table_references,
        set_clauses,
        where_clause,
        returning: if result_columns.is_empty() {
            None
        } else {
            Some(result_columns)
        },
        order_by,
        limit,
        offset,
        contains_constant_false_condition: false,
        indexes_to_update,
        ephemeral_plan,
        cdc_update_alter_statement: None,
    }))
}

fn build_scan_op(table: &Table, iter_dir: IterationDirection) -> Operation {
    match table {
        Table::BTree(_) => Operation::Scan(Scan::BTreeTable {
            iter_dir,
            index: None,
        }),
        Table::Virtual(_) => Operation::default_scan_for(table),
        _ => unreachable!(),
    }
}
