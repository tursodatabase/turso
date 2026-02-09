use crate::generation::{
    gen_random_text, pick_index, pick_n_unique, pick_unique, Arbitrary, ArbitraryFrom,
    ArbitrarySized, GenerationContext, InsertOpts,
};
use crate::model::query::alter_table::{AlterTable, AlterTableType, AlterTableTypeDiscriminants};
use crate::model::query::predicate::Predicate;
use crate::model::query::select::{
    CompoundOperator, CompoundSelect, Distinctness, FromClause, OrderBy, ResultColumn, SelectBody,
    SelectInner, SelectTable,
};
use crate::model::query::update::{SetValue, Update};
use crate::model::query::{
    Create, CreateIndex, Delete, Drop, DropIndex, Insert, OnConflict, Select, UpdateSetItem,
};
use crate::model::table::{
    Column, ColumnType, Index, JoinType, JoinedTable, Name, SimValue, Table, TableContext,
};
use indexmap::IndexSet;
use rand::seq::IndexedRandom;
use rand::Rng;
use turso_parser::ast::{ColumnConstraint, Expr, SortOrder};

use super::{backtrack, pick};

impl Arbitrary for Create {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        Create {
            table: Table::arbitrary(rng, context),
        }
    }
}

impl Arbitrary for FromClause {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let opts = &context.opts().query.from_clause;
        let weights = opts.as_weighted_index();
        let num_joins = opts.joins[rng.sample(weights)].num_joins;

        let mut tables = context.tables().clone();
        let mut table = pick(&tables, rng).clone();

        tables.retain(|t| t.name != table.name);

        let joins: Vec<_> = (0..num_joins)
            .filter_map(|_| {
                if tables.is_empty() {
                    return None;
                }
                let join_table = pick(&tables, rng).clone();
                let joined_table_name = join_table.name.clone();

                tables.retain(|t| t.name != join_table.name);
                for row in &mut table.rows {
                    assert_eq!(
                        row.len(),
                        table.columns.len(),
                        "Row length does not match column length after join"
                    );
                }

                let predicate = Predicate::arbitrary_from(rng, context, &table);
                Some(JoinedTable {
                    table: joined_table_name,
                    join_type: JoinType::Inner,
                    on: predicate,
                })
            })
            .collect();
        FromClause {
            table: SelectTable::Table(table.name.clone()),
            joins,
        }
    }
}

impl Arbitrary for SelectInner {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let from = FromClause::arbitrary(rng, env);
        let tables = env.tables().clone();
        let join_table = from.into_join_table(&tables);
        let cuml_col_count = join_table.columns().count();

        let order_by = rng
            .random_bool(env.opts().query.select.order_by_prob)
            .then(|| {
                let dependencies = &from.table.dependencies();
                let order_by_table_candidates = from
                    .joins
                    .iter()
                    .map(|j| &j.table)
                    .chain(dependencies)
                    .collect::<Vec<_>>();
                let order_by_col_count =
                    (rng.random::<f64>() * rng.random::<f64>() * (cuml_col_count as f64)) as usize; // skew towards 0
                if order_by_col_count == 0 {
                    return None;
                }
                let mut col_names = IndexSet::new();
                let mut order_by_cols = Vec::new();
                while order_by_cols.len() < order_by_col_count {
                    let table = pick(&order_by_table_candidates, rng);
                    let table = tables.iter().find(|t| t.name == table.as_str()).unwrap();
                    let col = pick(&table.columns, rng);
                    let col_name = format!("{}.{}", table.name, col.name);
                    if col_names.insert(col_name.clone()) {
                        order_by_cols.push((
                            col_name,
                            if rng.random_bool(0.5) {
                                SortOrder::Asc
                            } else {
                                SortOrder::Desc
                            },
                        ));
                    }
                }
                Some(OrderBy {
                    columns: order_by_cols,
                })
            })
            .flatten();

        SelectInner {
            distinctness: Distinctness::arbitrary(rng, env),
            columns: vec![ResultColumn::Star],
            from: Some(from),
            where_clause: Predicate::arbitrary_from(rng, env, &join_table),
            order_by,
        }
    }
}

impl ArbitrarySized for SelectInner {
    //FIXME this can generate SELECT statements containing fewer columns than the num_result_columns parameter.
    fn arbitrary_sized<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        env: &C,
        num_result_columns: usize,
    ) -> Self {
        let mut select_inner = SelectInner::arbitrary(rng, env);
        let select_from = &select_inner.from.as_ref().unwrap();
        let dependencies = select_from.table.dependencies();
        let table_names = select_from
            .joins
            .iter()
            .map(|j| &j.table)
            .chain(&dependencies);

        let flat_columns_names = table_names
            .flat_map(|t| {
                env.tables()
                    .iter()
                    .find(|table| table.name == *t)
                    .unwrap()
                    .columns
                    .iter()
                    .map(move |c| format!("{}.{}", t, c.name))
            })
            .collect::<Vec<_>>();
        let selected_columns = pick_unique(&flat_columns_names, num_result_columns, rng);
        let columns = selected_columns
            .map(|col_name| ResultColumn::Column(col_name.clone()))
            .collect();

        select_inner.columns = columns;
        select_inner
    }
}

impl Arbitrary for Distinctness {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        match rng.random_range(0..=5) {
            0..4 => Distinctness::All,
            _ => Distinctness::Distinct,
        }
    }
}

impl Arbitrary for CompoundOperator {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        match rng.random_range(0..=1) {
            0 => CompoundOperator::Union,
            1 => CompoundOperator::UnionAll,
            _ => unreachable!(),
        }
    }
}

/// SelectFree is a wrapper around Select that allows for arbitrary generation
/// of selects without requiring a specific environment, which is useful for generating
/// arbitrary expressions without referring to the tables.
pub struct SelectFree(pub Select);

impl Arbitrary for SelectFree {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let expr = Predicate(Expr::arbitrary_sized(rng, env, 8));
        let select = Select::expr(expr);
        Self(select)
    }
}

impl Arbitrary for Select {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        // Generate a number of selects based on the query size
        let opts = &env.opts().query.select;
        let num_compound_selects = opts.compound_selects
            [rng.sample(opts.compound_select_weighted_index())]
        .num_compound_selects;

        let min_column_count_across_tables =
            env.tables().iter().map(|t| t.columns.len()).min().unwrap();

        let num_result_columns = rng.random_range(1..=min_column_count_across_tables);

        let mut first = SelectInner::arbitrary_sized(rng, env, num_result_columns);

        let mut rest: Vec<SelectInner> = (0..num_compound_selects)
            .map(|_| SelectInner::arbitrary_sized(rng, env, num_result_columns))
            .collect();

        if !rest.is_empty() {
            // ORDER BY is not supported in compound selects yet
            first.order_by = None;
            for s in &mut rest {
                s.order_by = None;
            }
        }

        Self {
            body: SelectBody {
                select: Box::new(first),
                compounds: rest
                    .into_iter()
                    .map(|s| CompoundSelect {
                        operator: CompoundOperator::arbitrary(rng, env),
                        select: Box::new(s),
                    })
                    .collect(),
            },
            limit: None,
        }
    }
}

impl Arbitrary for Insert {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let insert_opts = &env.opts().query.insert;
        let gen_values = |rng: &mut R| gen_insert_values(rng, env, insert_opts);
        let gen_upsert_values = |rng: &mut R| gen_insert_upsert_values(rng, env, insert_opts);

        // we keep this here for now, because gen_select does not generate subqueries, and they're
        // important for surfacing bugs.
        let gen_nested_self_insert = |rng: &mut R| {
            // Skip tables that are already large to prevent exponential row growth from
            // repeated self-inserts (INSERT INTO t SELECT * FROM t doubles the table).
            let max_rows = insert_opts.max_rows.get() as usize;
            let non_unique: Vec<_> = env
                .tables()
                .iter()
                .filter(|t| !t.has_any_unique_column() && t.rows.len() <= max_rows)
                .collect();
            non_unique.first()?;
            let table = *pick(&non_unique, rng);

            const MAX_SELF_INSERT_DEPTH: i32 = 5;
            let nesting_depth = rng.random_range(1..=MAX_SELF_INSERT_DEPTH);

            let mut select = Select::simple(
                table.name.clone(),
                Predicate::arbitrary_from(rng, env, table),
            );

            for _ in 1..nesting_depth {
                select = Select {
                    body: SelectBody {
                        select: Box::new(SelectInner {
                            distinctness: Distinctness::All,
                            columns: vec![ResultColumn::Star],
                            from: Some(FromClause {
                                table: SelectTable::Select(select),
                                joins: Vec::new(),
                            }),
                            where_clause: Predicate::true_(),
                            order_by: None,
                        }),
                        compounds: Vec::new(),
                    },
                    limit: None,
                };
            }

            Some(Insert::Select {
                table: table.name.clone(),
                select: Box::new(select),
            })
        };

        let gen_select = |rng: &mut R| {
            // Find a non-empty, not-too-large table without UNIQUE constraints
            let max_rows = insert_opts.max_rows.get() as usize;
            let select_table = env.tables().iter().find(|t| {
                !t.rows.is_empty() && !t.has_any_unique_column() && t.rows.len() <= max_rows
            })?;
            let row = pick(&select_table.rows, rng);
            let predicate = Predicate::arbitrary_from(rng, env, (select_table, row));
            // TODO change for arbitrary_sized and insert from arbitrary tables
            // Build a SELECT from the same table to ensure schema matches for INSERT INTO ... SELECT
            let select = Select::simple(select_table.name.clone(), predicate);
            Some(Insert::Select {
                table: select_table.name.clone(),
                select: Box::new(select),
            })
        };

        let mut choices = vec![
            (
                1,
                Box::new(gen_values) as Box<dyn Fn(&mut R) -> Option<Insert>>,
            ),
            (
                1,
                Box::new(gen_upsert_values) as Box<dyn Fn(&mut R) -> Option<Insert>>,
            ),
            (
                1,
                Box::new(gen_nested_self_insert) as Box<dyn Fn(&mut R) -> Option<Insert>>,
            ),
        ];

        if env.opts().arbitrary_insert_into_select {
            choices.push((1, Box::new(gen_select)));
        }

        backtrack(choices, rng).expect("backtrack should with these arguments not return None")
    }
}

fn gen_insert_values<R: Rng + ?Sized, C: GenerationContext>(
    rng: &mut R,
    env: &C,
    insert_opts: &InsertOpts,
) -> Option<Insert> {
    const UNIQUE_BASE_OFFSET_RANGE: std::ops::Range<i64> = 1_000_000_000..2_000_000_000;
    const UNIQUE_COL_STRIDE: i64 = 10_000_000;

    let table = pick(env.tables(), rng);

    const INTEGER_PK_NULL_PROB: f64 = 0.05;
    let integer_pk_idx = table
        .columns
        .iter()
        .position(|c| matches!(c.column_type, ColumnType::Integer) && c.is_primary_key());

    let num_rows = rng.random_range(insert_opts.min_rows.get()..insert_opts.max_rows.get());
    let base_offset: i64 = rng.random_range(UNIQUE_BASE_OFFSET_RANGE);

    let values: Vec<Vec<SimValue>> = (0..num_rows)
        .map(|row_idx| {
            table
                .columns
                .iter()
                .enumerate()
                .map(|(col_idx, c)| {
                    if integer_pk_idx == Some(col_idx) && rng.random_bool(INTEGER_PK_NULL_PROB) {
                        return SimValue::NULL;
                    }
                    if c.has_unique_or_pk() {
                        let offset =
                            base_offset + (col_idx as i64 * UNIQUE_COL_STRIDE) + row_idx as i64;
                        SimValue::unique_for_type(&c.column_type, offset)
                    } else {
                        SimValue::arbitrary_from(rng, env, &c.column_type)
                    }
                })
                .collect()
        })
        .collect();

    Some(Insert::Values {
        table: table.name.clone(),
        values,
        on_conflict: None,
    })
}

fn gen_insert_upsert_values<R: Rng + ?Sized, C: GenerationContext>(
    rng: &mut R,
    env: &C,
    insert_opts: &InsertOpts,
) -> Option<Insert> {
    const UNIQUE_BASE_OFFSET_RANGE: std::ops::Range<i64> = 2_000_000_000..3_000_000_000;
    const UNIQUE_COL_STRIDE: i64 = 10_000_000;
    const FRESH_ID_JITTER: i64 = 5;
    const PAYLOAD_OFFSET_BUMP: i64 = 42;
    const UPSERT_INTEGER_PK_NULL_PROB: f64 = 0.25;

    if !rng.random_bool(insert_opts.upsert_prob) {
        return None;
    }

    let is_unique_only = |c: &Column| {
        c.constraints
            .iter()
            .any(|cc| matches!(cc, ColumnConstraint::Unique(_)))
    };

    let candidates: Vec<_> = env
        .tables()
        .iter()
        .filter(|t| {
            if t.rows.len() < 2 {
                return false;
            }
            let has_integer_pk = t
                .columns
                .iter()
                .any(|c| matches!(c.column_type, ColumnType::Integer) && c.is_primary_key());
            let has_unique_non_pk = t
                .columns
                .iter()
                .any(|c| is_unique_only(c) && !c.is_primary_key());
            has_integer_pk && has_unique_non_pk
        })
        .collect();

    let table = *candidates.choose(rng)?;

    let (pk_idx, pk_col) = table
        .columns
        .iter()
        .enumerate()
        .find(|(_, c)| matches!(c.column_type, ColumnType::Integer) && c.is_primary_key())?;

    let unique_cols: Vec<(usize, &Column)> = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| is_unique_only(c) && !c.is_primary_key())
        .collect();
    let (target_idx, target_col) = *pick(&unique_cols, rng);

    let existing_rows = &table.rows;
    let conflict_rows: Vec<_> = existing_rows
        .iter()
        .filter(|r| {
            r.get(target_idx)
                .is_some_and(|v| v.0 != turso_core::Value::Null)
        })
        .collect();
    let conflict_row = *conflict_rows.choose(rng)?;

    let payload_cols: Vec<(usize, &Column)> = table
        .columns
        .iter()
        .enumerate()
        .filter(|(i, c)| *i != pk_idx && *i != target_idx && !c.has_unique_or_pk())
        .collect();
    let payload = payload_cols
        .iter()
        .find(|(_, c)| matches!(c.column_type, ColumnType::Text))
        .copied()
        .or_else(|| payload_cols.first().copied());

    let base_offset: i64 = rng.random_range(UNIQUE_BASE_OFFSET_RANGE);

    // Build a single-row insert and override the conflict target.
    let mut row: Vec<SimValue> = table
        .columns
        .iter()
        .enumerate()
        .map(|(col_idx, c)| {
            if c.has_unique_or_pk() {
                let offset = base_offset + (col_idx as i64 * UNIQUE_COL_STRIDE);
                SimValue::unique_for_type(&c.column_type, offset)
            } else {
                SimValue::arbitrary_from(rng, env, &c.column_type)
            }
        })
        .collect();

    row[target_idx] = conflict_row[target_idx].clone();

    // Pick an id that does not currently exist.
    let mut ids: Vec<i64> = existing_rows
        .iter()
        .filter_map(|r| match r[pk_idx].0 {
            turso_core::Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();

    let mut maybe_new_id = None;
    if ids.len() >= 2 && rng.random_bool(0.7) {
        for _ in 0..8 {
            let i = rng.random_range(0..(ids.len() - 1));
            let lo = ids[i];
            let hi = ids[i + 1];
            let gap = hi.saturating_sub(lo);
            if gap > 1 {
                let offset = rng.random_range(1..gap);
                maybe_new_id = Some(lo.saturating_add(offset));
                break;
            }
        }
    }

    let mut new_id = maybe_new_id.unwrap_or_else(|| {
        ids.last()
            .copied()
            .unwrap_or(0)
            .saturating_add(1 + rng.random_range(0i64..=FRESH_ID_JITTER))
    });

    let mut tries = 0;
    while existing_rows
        .iter()
        .any(|r| matches!(r[pk_idx].0, turso_core::Value::Integer(i) if i == new_id))
    {
        new_id = new_id.saturating_add(1);
        tries += 1;
        assert!(
            tries < 1024,
            "failed to find fresh INTEGER PRIMARY KEY value"
        );
    }
    row[pk_idx] = SimValue::unique_for_type(&pk_col.column_type, new_id);
    if rng.random_bool(UPSERT_INTEGER_PK_NULL_PROB) {
        row[pk_idx] = SimValue::NULL;
    }

    if let Some((payload_idx, payload_col)) = payload {
        let offset = base_offset + PAYLOAD_OFFSET_BUMP;
        row[payload_idx] = SimValue::unique_for_type(&payload_col.column_type, offset);
    }

    let mut assignments = Vec::new();
    assignments.push(UpdateSetItem {
        column: pk_col.name.clone(),
        excluded_column: pk_col.name.clone(),
    });
    if let Some((_, payload_col)) = payload {
        assignments.push(UpdateSetItem {
            column: payload_col.name.clone(),
            excluded_column: payload_col.name.clone(),
        });
    }

    Some(Insert::Values {
        table: table.name.clone(),
        values: vec![row],
        on_conflict: Some(OnConflict {
            target_column: target_col.name.clone(),
            assignments,
        }),
    })
}

impl Arbitrary for Delete {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let table = pick(env.tables(), rng);
        Self {
            table: table.name.clone(),
            predicate: Predicate::arbitrary_from(rng, env, table),
        }
    }
}

impl Arbitrary for Drop {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let table = pick(env.tables(), rng);
        Self {
            table: table.name.clone(),
        }
    }
}

impl Arbitrary for CreateIndex {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        assert!(
            !env.tables().is_empty(),
            "Cannot create an index when no tables exist in the environment."
        );

        let table = pick(env.tables(), rng);

        if table.columns.is_empty() {
            panic!(
                "Cannot create an index on table '{}' as it has no columns.",
                table.name
            );
        }

        let num_columns_to_pick = rng.random_range(1..=table.columns.len());
        let picked_column_indices = pick_n_unique(0..table.columns.len(), num_columns_to_pick, rng);

        let columns = picked_column_indices
            .map(|i| {
                let column = &table.columns[i];
                (
                    column.name.clone(),
                    if rng.random_bool(0.5) {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    },
                )
            })
            .collect::<Vec<(String, SortOrder)>>();

        let index_name = format!(
            "idx_{}_{}_{}",
            table.name,
            gen_random_text(rng).chars().take(8).collect::<String>(),
            rng.random_range(0..1000000)
        );

        CreateIndex {
            index: Index {
                index_name,
                table_name: table.name.clone(),
                columns,
            },
        }
    }
}

impl Arbitrary for Update {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let table = pick(env.tables(), rng);
        let update_opts = &env.opts().query.update;

        let unique_columns: Vec<(usize, &Column)> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.has_unique_or_pk() && !matches!(c.column_type, ColumnType::Blob))
            .collect();

        let non_unique_columns: Vec<&Column> = table
            .columns
            .iter()
            .filter(|c| !c.has_unique_or_pk())
            .collect();

        // (first row and last row have different values)
        let last_row_idx = table.rows.len().saturating_sub(1);
        let conflict_capable_columns: Vec<(usize, &Column)> = if table.rows.len() >= 2 {
            unique_columns
                .iter()
                .filter(|(col_idx, _)| {
                    table.rows[0][*col_idx] != table.rows[last_row_idx][*col_idx]
                })
                .copied()
                .collect()
        } else {
            vec![]
        };

        // CASE WHEN:
        // 1. Profile explicitly enables force_late_failure
        // 2. Table has conflict-capable UNIQUE columns
        // 3. Table has at least one non-UNIQUE column
        let use_case_when = update_opts.force_late_failure
            && !conflict_capable_columns.is_empty()
            && !non_unique_columns.is_empty();

        let (set_values, predicate) = if use_case_when {
            let (col_idx, unique_col) = *pick(&conflict_capable_columns, rng);
            let marker_col = *pick(&non_unique_columns, rng);
            let first_val = table.rows[0][col_idx].clone();
            let last_val = table.rows[last_row_idx][col_idx].clone();

            let marker_value = match update_opts.padding_size {
                Some(size) => {
                    let p = "X".repeat(size);
                    if matches!(marker_col.column_type, ColumnType::Blob) {
                        SimValue(turso_core::Value::Blob(p.into_bytes()))
                    } else {
                        SimValue(turso_core::Value::Text(p.into()))
                    }
                }
                None => SimValue::arbitrary_from(rng, env, &marker_col.column_type),
            };

            let set_values = vec![
                (marker_col.name.clone(), SetValue::Simple(marker_value)),
                (
                    unique_col.name.clone(),
                    SetValue::CaseWhen {
                        condition: Box::new(Predicate::eq(
                            Predicate::column(unique_col.name.clone()),
                            Predicate::value(last_val),
                        )),
                        then_value: first_val,
                        else_column: unique_col.name.clone(),
                    },
                ),
            ];
            (set_values, Predicate::true_())
        } else if non_unique_columns.is_empty() {
            let column = pick(&table.columns, rng);
            let base_offset: i64 = rng.random_range(2_000_000_000i64..3_000_000_000i64);

            let unique_value = SimValue::unique_for_type(&column.column_type, base_offset);
            let set_values = vec![(column.name.clone(), SetValue::Simple(unique_value))];

            let predicate = if !table.rows.is_empty() {
                let row = pick(&table.rows, rng);
                Predicate::arbitrary_from(rng, env, (table, row))
            } else {
                Predicate::arbitrary_from(rng, env, table)
            };

            (set_values, predicate)
        } else {
            // Normal case: pick from non-UNIQUE columns
            let num_cols = rng.random_range(1..=non_unique_columns.len());
            let set_values: Vec<(String, SetValue)> =
                pick_unique(&non_unique_columns, num_cols, rng)
                    .map(|column| {
                        (
                            column.name.clone(),
                            SetValue::Simple(SimValue::arbitrary_from(
                                rng,
                                env,
                                &column.column_type,
                            )),
                        )
                    })
                    .collect();

            let predicate = Predicate::arbitrary_from(rng, env, table);
            (set_values, predicate)
        };

        Update {
            table: table.name.clone(),
            set_values,
            predicate,
        }
    }
}

const ALTER_TABLE_ALL: &[AlterTableTypeDiscriminants] = &[
    AlterTableTypeDiscriminants::RenameTo,
    AlterTableTypeDiscriminants::AddColumn,
    AlterTableTypeDiscriminants::AlterColumn,
    AlterTableTypeDiscriminants::RenameColumn,
    AlterTableTypeDiscriminants::DropColumn,
];
const ALTER_TABLE_NO_DROP: &[AlterTableTypeDiscriminants] = &[
    AlterTableTypeDiscriminants::RenameTo,
    AlterTableTypeDiscriminants::AddColumn,
    AlterTableTypeDiscriminants::AlterColumn,
    AlterTableTypeDiscriminants::RenameColumn,
];
const ALTER_TABLE_NO_ALTER_COL: &[AlterTableTypeDiscriminants] = &[
    AlterTableTypeDiscriminants::RenameTo,
    AlterTableTypeDiscriminants::AddColumn,
    AlterTableTypeDiscriminants::RenameColumn,
    AlterTableTypeDiscriminants::DropColumn,
];
const ALTER_TABLE_NO_ALTER_COL_NO_DROP: &[AlterTableTypeDiscriminants] = &[
    AlterTableTypeDiscriminants::RenameTo,
    AlterTableTypeDiscriminants::AddColumn,
    AlterTableTypeDiscriminants::RenameColumn,
];

fn get_column_diff(table: &Table) -> IndexSet<&str> {
    let mut undropable: IndexSet<&str> = table
        .indexes
        .iter()
        .flat_map(|idx| idx.columns.iter().map(|(name, _)| name.as_str()))
        .collect();

    undropable.extend(
        table
            .columns
            .iter()
            .filter(|c| c.has_unique_or_pk())
            .map(|c| c.name.as_str()),
    );

    table
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .filter(|name| !undropable.contains(name))
        .collect()
}

impl ArbitraryFrom<(&Table, &[AlterTableTypeDiscriminants])> for AlterTableType {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        (table, choices): (&Table, &[AlterTableTypeDiscriminants]),
    ) -> Self {
        match choices.choose(rng).unwrap() {
            AlterTableTypeDiscriminants::RenameTo => AlterTableType::RenameTo {
                new_name: Name::arbitrary(rng, context).0,
            },
            AlterTableTypeDiscriminants::AddColumn => {
                use turso_parser::ast::ColumnConstraint;

                let mut column = Column::arbitrary(rng, context);
                // can't be dropped acc to sqlite
                column
                    .constraints
                    .retain(|c| !matches!(c, ColumnConstraint::Unique(_)));
                AlterTableType::AddColumn { column }
            }
            AlterTableTypeDiscriminants::AlterColumn => {
                use turso_parser::ast::ColumnConstraint;

                let col_diff = get_column_diff(table);

                if col_diff.is_empty() {
                    // Generate a DropColumn if we can drop a column
                    return AlterTableType::arbitrary_from(
                        rng,
                        context,
                        (
                            table,
                            if choices.contains(&AlterTableTypeDiscriminants::DropColumn) {
                                ALTER_TABLE_NO_ALTER_COL
                            } else {
                                ALTER_TABLE_NO_ALTER_COL_NO_DROP
                            },
                        ),
                    );
                }

                let col_idx = pick_index(col_diff.len(), rng);
                let col_name = col_diff.get_index(col_idx).unwrap();

                let mut column = Column::arbitrary(rng, context);
                column
                    .constraints
                    .retain(|c| !matches!(c, ColumnConstraint::Unique(_)));

                AlterTableType::AlterColumn {
                    old: col_name.to_string(),
                    new: column,
                }
            }
            AlterTableTypeDiscriminants::RenameColumn => AlterTableType::RenameColumn {
                old: pick(&table.columns, rng).name.clone(),
                new: Name::arbitrary(rng, context).0,
            },
            AlterTableTypeDiscriminants::DropColumn => {
                let col_diff = get_column_diff(table);

                if col_diff.is_empty() {
                    // Generate a DropColumn if we can drop a column
                    return AlterTableType::arbitrary_from(
                        rng,
                        context,
                        (
                            table,
                            if context.opts().query.alter_table.alter_column {
                                ALTER_TABLE_NO_DROP
                            } else {
                                ALTER_TABLE_NO_ALTER_COL_NO_DROP
                            },
                        ),
                    );
                }

                let col_idx = pick_index(col_diff.len(), rng);
                let col_name = col_diff.get_index(col_idx).unwrap();

                AlterTableType::DropColumn {
                    column_name: col_name.to_string(),
                }
            }
        }
    }
}

impl Arbitrary for AlterTable {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let table = pick(context.tables(), rng);
        let choices = match (
            table.columns.len() > 1,
            context.opts().query.alter_table.alter_column,
        ) {
            (true, true) => ALTER_TABLE_ALL,
            (true, false) => ALTER_TABLE_NO_ALTER_COL,
            (false, true) | (false, false) => ALTER_TABLE_NO_ALTER_COL_NO_DROP,
        };

        let alter_table_type = AlterTableType::arbitrary_from(rng, context, (table, choices));
        Self {
            table_name: table.name.clone(),
            alter_table_type,
        }
    }
}

impl Arbitrary for DropIndex {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let tables_with_indexes = context
            .tables()
            .iter()
            .filter(|table| !table.indexes.is_empty())
            .collect::<Vec<_>>();

        // Cannot DROP INDEX if there is no index to drop
        assert!(!tables_with_indexes.is_empty());
        let table = tables_with_indexes.choose(rng).unwrap();
        let index = table.indexes.choose(rng).unwrap();
        Self {
            index_name: index.index_name.clone(),
            table_name: table.name.clone(),
        }
    }
}
