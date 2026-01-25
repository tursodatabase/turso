use crate::generation::{
    gen_random_text, pick_index, pick_n_unique, pick_unique, Arbitrary, ArbitraryFrom,
    ArbitrarySized, GenerationContext,
};
use crate::model::query::alter_table::{AlterTable, AlterTableType, AlterTableTypeDiscriminants};
use crate::model::query::predicate::Predicate;
use crate::model::query::select::{
    CompoundOperator, CompoundSelect, Distinctness, FromClause, OrderBy, ResultColumn, SelectBody,
    SelectInner, SelectTable,
};
use crate::model::query::update::Update;
use crate::model::query::{Create, CreateIndex, Delete, Drop, DropIndex, Insert, Select};
use crate::model::table::{
    Column, Index, JoinType, JoinedTable, Name, SimValue, Table, TableContext,
};
use indexmap::IndexSet;
use rand::seq::IndexedRandom;
use rand::Rng;
use turso_parser::ast::{Expr, SortOrder};

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
        let opts = &env.opts().query.insert;
        let gen_values = |rng: &mut R| {
            let table = pick(env.tables(), rng);
            let num_rows = rng.random_range(opts.min_rows.get()..opts.max_rows.get());
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(rng, env, &c.column_type))
                        .collect()
                })
                .collect();
            Some(Insert::Values {
                table: table.name.clone(),
                values,
            })
        };

        // we keep this here for now, because gen_select does not generate subqueries, and they're
        // important for surfacing bugs.
        let gen_nested_self_insert = |rng: &mut R| {
            let table = pick(env.tables(), rng);

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
            // Find a non-empty table
            let select_table = env.tables().iter().find(|t| !t.rows.is_empty())?;
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
                Box::new(gen_nested_self_insert) as Box<dyn Fn(&mut R) -> Option<Insert>>,
            ),
        ];

        if env.opts().arbitrary_insert_into_select {
            choices.push((1, Box::new(gen_select)));
        }

        backtrack(choices, rng).expect("backtrack should with these arguments not return None")
    }
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
        let num_cols = rng.random_range(1..=table.columns.len());
        let columns = pick_unique(&table.columns, num_cols, rng);
        let set_values: Vec<(String, SimValue)> = columns
            .map(|column| {
                (
                    column.name.clone(),
                    SimValue::arbitrary_from(rng, env, &column.column_type),
                )
            })
            .collect();
        Update {
            table: table.name.clone(),
            set_values,
            predicate: Predicate::arbitrary_from(rng, env, table),
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

// TODO: Unfortunately this diff strategy allocates a couple of IndexSet's
// in the future maybe change this to be more efficient. This is currently acceptable because this function
// is only called for `DropColumn`
fn get_column_diff(table: &Table) -> IndexSet<&str> {
    // Columns that are referenced in INDEXES cannot be dropped
    let column_cannot_drop = table
        .indexes
        .iter()
        .flat_map(|index| index.columns.iter().map(|(col_name, _)| col_name.as_str()))
        .collect::<IndexSet<_>>();
    if column_cannot_drop.len() == table.columns.len() {
        // Optimization: all columns are present in indexes so we do not need to but the table column set
        return IndexSet::new();
    }

    let column_set: IndexSet<_, std::hash::RandomState> =
        IndexSet::from_iter(table.columns.iter().map(|col| col.name.as_str()));

    let diff = column_set
        .difference(&column_cannot_drop)
        .copied()
        .collect::<IndexSet<_, std::hash::RandomState>>();
    diff
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
            AlterTableTypeDiscriminants::AddColumn => AlterTableType::AddColumn {
                column: Column::arbitrary(rng, context),
            },
            AlterTableTypeDiscriminants::AlterColumn => {
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

                AlterTableType::AlterColumn {
                    old: col_name.to_string(),
                    new: Column::arbitrary(rng, context),
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
