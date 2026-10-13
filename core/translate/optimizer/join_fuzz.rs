//! Random join orders for the join optimizer, checked with the rules of
//! "On the Correct and Complete Enumeration of the Core Search Space"
//! (Moerkotte, Fender, Eich, SIGMOD 2013).
//!
//! Each round builds a random table list with join types and join terms, runs
//! the join optimizer on it, and checks the chosen order with the conflict
//! detector CD-C from the paper. For a small table list, an order that the
//! detector rejects is still valid when a search over the transformation rules
//! of the paper reaches it, because the detector is conservative for
//! degenerate terms. A small table list also gets its rows compared with the
//! rows of the `FROM` list on random table contents.
//!
//! The optimizer evaluates inner join terms and `WHERE` terms at the first loop
//! that has all of their tables, after the outer join match check of that
//! loop. So every inner join is a cross product followed by a filter, and the
//! checks model it as a cross product.
//!
//! Two more tests check the detector itself: it must accept every order the
//! optimizer allows, and every order it accepts must give the same rows as the
//! `FROM` list on small random tables.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_parser::ast::{self, Expr, Operator, TableInternalId};

use super::{
    constraints::constraints_from_where_clause,
    cost::RowCountEstimate,
    cost_params::DEFAULT_PARAMS,
    join::{compute_best_join_order, required_prefix_by_table, GREEDY_JOIN_THRESHOLD},
    AvailableIndexes,
};
use crate::{
    alloc::{TursoIteratorExt, TursoSliceExt},
    schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table, Type,
    },
    stats::AnalyzeStats,
    translate::{
        plan::{
            ColumnUsedMask, JoinInfo, JoinOrigin, JoinType, JoinedTable, Operation,
            TableReferences, WhereTerm, WhereTermOrigin,
        },
        planner::TableMask,
    },
    vdbe::builder::TableRefIdCounter,
    MAIN_DB_ID,
};

#[test]
fn optimizer_join_order_is_a_valid_reordering() {
    let (mut rng, seed) = fuzz_rng();
    let rounds = rounds_from_env(300);
    let mut reordered = 0;
    for round in 0..rounds {
        let shape = QueryShape::random(&mut rng);
        let order = plan_join_order(&shape);
        if order
            .iter()
            .enumerate()
            .any(|(position, &table)| position != table)
        {
            reordered += 1;
        }
        if let Err(reason) = check_join_order(&mut rng, &shape, &order) {
            panic!(
                "seed {seed} round {round}: the optimizer chose an invalid join order\n  query: {shape}\n  order: {}\n  {reason}",
                order_names(&order)
            );
        }
    }
    println!("join fuzz: {rounds} rounds, {reordered} plans changed the table order");
}

#[test]
fn conflict_detector_accepts_every_order_the_optimizer_allows() {
    let (mut rng, seed) = fuzz_rng();
    let rounds = rounds_from_env(200);
    for round in 0..rounds {
        let table_count = rng.random_range(2..=5);
        let shape = QueryShape::random_with_table_count(&mut rng, table_count);
        let required = required_prefix_by_table(&joined_tables(&shape))
            .expect("a small table list fits in a table mask");
        let tree = InitialTree::from_shape(&shape);
        let search_space = core_search_space(&tree);
        for order in permutations(table_count) {
            if !optimizer_allows(&required, &order) {
                continue;
            }
            let detector = check_join_order_with_conflict_detector(&tree, &order);
            assert!(
                detector.is_ok(),
                "seed {seed} round {round}: the detector rejects an order the optimizer allows: {detector:?}\n  query: {shape}\n  order: {}",
                order_names(&order)
            );
            assert!(
                search_space.contains(&left_deep_tree(&tree, &order)),
                "seed {seed} round {round}: the transformation search rejects an order the optimizer allows\n  query: {shape}\n  order: {}",
                order_names(&order)
            );
        }
    }
}

#[test]
fn conflict_detector_accepts_only_orders_that_keep_results() {
    let (mut rng, seed) = fuzz_rng();
    let rounds = rounds_from_env(200);
    for round in 0..rounds {
        let table_count = rng.random_range(2..=5);
        let shape = QueryShape::random_with_table_count(&mut rng, table_count);
        let instances: Vec<Instance> = (0..INSTANCES_PER_SHAPE)
            .map(|_| Instance::random(&mut rng, table_count))
            .collect();
        let expected: Vec<Vec<Tuple>> = instances
            .iter()
            .map(|instance| sql_rows(&shape, instance))
            .collect();
        let from_order: Vec<usize> = (0..table_count).collect();
        for (instance, rows) in instances.iter().zip(&expected) {
            assert_eq!(
                &plan_rows(&shape, instance, &from_order),
                rows,
                "seed {seed} round {round}: the plan evaluator differs from the SQL evaluator on the FROM order\n  query: {shape}"
            );
        }
        let tree = InitialTree::from_shape(&shape);
        for order in permutations(table_count) {
            if check_join_order_with_conflict_detector(&tree, &order).is_err() {
                continue;
            }
            for (instance, rows) in instances.iter().zip(&expected) {
                assert_eq!(
                    &plan_rows(&shape, instance, &order),
                    rows,
                    "seed {seed} round {round}: the detector accepts an order that changes the result\n  query: {shape}\n  order: {}\n  tables: {instance}",
                    order_names(&order)
                );
            }
        }
    }
}

fn fuzz_rng() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or_else(
        |_| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("the clock is after 1970")
                .as_millis() as u64
        },
        |value| value.parse().expect("SEED must be a u64"),
    );
    println!("join fuzz seed: {seed}");
    (ChaCha8Rng::seed_from_u64(seed), seed)
}

fn rounds_from_env(default: usize) -> usize {
    std::env::var("JOIN_FUZZ_ROUNDS").map_or(default, |value| {
        value.parse().expect("JOIN_FUZZ_ROUNDS must be a usize")
    })
}

/// `JOIN_FUZZ_TABLES` fixes the table count of every round.
fn table_count_from_env() -> Option<usize> {
    let count: usize = std::env::var("JOIN_FUZZ_TABLES")
        .ok()?
        .parse()
        .expect("JOIN_FUZZ_TABLES must be a usize");
    assert!(
        (2..=TableReferences::MAX_JOINED_TABLES).contains(&count),
        "JOIN_FUZZ_TABLES must be between 2 and {}",
        TableReferences::MAX_JOINED_TABLES
    );
    Some(count)
}

const COLUMN_NAMES: [&str; 3] = ["a", "b", "c"];
const ROW_COUNTS: [f64; 5] = [5.0, 50.0, 500.0, 5_000.0, 1_000_000.0];
/// The transformation search enumerates every plan, so it and the row
/// comparison only run for small table lists.
const SMALL_TABLE_LIMIT: usize = 6;
/// How many random table contents each row comparison uses.
const INSTANCES_PER_SHAPE: usize = 6;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Join {
    Inner,
    Cross,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

impl Join {
    fn random(rng: &mut ChaCha8Rng) -> Self {
        match rng.random_range(0..20) {
            0..=7 => Join::Inner,
            8..=9 => Join::Cross,
            10..=13 => Join::Left,
            14..=15 => Join::Right,
            16 => Join::Full,
            17..=18 => Join::Semi,
            _ => Join::Anti,
        }
    }

    fn keyword(self) -> &'static str {
        match self {
            Join::Inner => "JOIN",
            Join::Cross => "CROSS JOIN",
            Join::Left => "LEFT JOIN",
            Join::Right => "RIGHT JOIN",
            Join::Full => "FULL JOIN",
            Join::Semi => "SEMI JOIN",
            Join::Anti => "ANTI JOIN",
        }
    }

    fn hides_columns(self) -> bool {
        matches!(self, Join::Semi | Join::Anti)
    }

    fn null_extends_right(self) -> bool {
        matches!(self, Join::Left | Join::Full)
    }

    fn null_extends_left(self) -> bool {
        matches!(self, Join::Right | Join::Full)
    }

    fn is_outer(self) -> bool {
        self.null_extends_left() || self.null_extends_right()
    }

    fn join_info(self) -> JoinInfo {
        let join_type = match self {
            Join::Inner | Join::Cross => JoinType::Inner,
            Join::Left => JoinType::LeftOuter,
            Join::Right => JoinType::RightOuter,
            Join::Full => JoinType::FullOuter,
            Join::Semi => JoinType::Semi,
            Join::Anti => JoinType::Anti,
        };
        JoinInfo {
            join_type,
            using: vec![],
            no_reorder: self == Join::Cross,
        }
    }

    fn on_term_origin(self, table: TableInternalId) -> WhereTermOrigin {
        match self {
            Join::Inner | Join::Cross => WhereTermOrigin::Join(JoinOrigin::Inner(table)),
            Join::Left | Join::Right | Join::Full => {
                WhereTermOrigin::Join(JoinOrigin::Outer(table))
            }
            Join::Semi | Join::Anti => WhereTermOrigin::Where,
        }
    }

    fn operator_kind(self) -> OpKind {
        match self {
            Join::Inner | Join::Cross => OpKind::Cross,
            Join::Left | Join::Right => OpKind::LeftOuter,
            Join::Full => OpKind::FullOuter,
            Join::Semi => OpKind::Semi,
            Join::Anti => OpKind::Anti,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ColumnRef {
    table: usize,
    column: usize,
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{}.{}", self.table, COLUMN_NAMES[self.column])
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Compare {
    Equal,
    Is,
}

impl Compare {
    fn random(rng: &mut ChaCha8Rng) -> Self {
        if rng.random_ratio(1, 7) {
            Compare::Is
        } else {
            Compare::Equal
        }
    }

    fn operator(self) -> Operator {
        match self {
            Compare::Equal => Operator::Equals,
            Compare::Is => Operator::Is,
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Compare::Equal => "=",
            Compare::Is => "IS",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Term {
    Columns {
        left: ColumnRef,
        compare: Compare,
        right: ColumnRef,
    },
    Constant {
        column: ColumnRef,
        value: i64,
    },
    IsNull {
        column: ColumnRef,
    },
}

impl Term {
    fn tables(self) -> u64 {
        match self {
            Term::Columns { left, right, .. } => bit(left.table) | bit(right.table),
            Term::Constant { column, .. } | Term::IsNull { column } => bit(column.table),
        }
    }

    fn null_rejecting_tables(self) -> u64 {
        match self {
            Term::Columns {
                compare: Compare::Equal,
                ..
            }
            | Term::Constant { .. } => self.tables(),
            Term::Columns {
                compare: Compare::Is,
                ..
            }
            | Term::IsNull { .. } => 0,
        }
    }

    fn expr(self, table_ids: &[TableInternalId]) -> Expr {
        let column_expr = |column: ColumnRef| Expr::Column {
            database: None,
            table: table_ids[column.table],
            column: column.column,
            is_rowid_alias: false,
        };
        match self {
            Term::Columns {
                left,
                compare,
                right,
            } => Expr::Binary(
                Box::new(column_expr(left)),
                compare.operator(),
                Box::new(column_expr(right)),
            ),
            Term::Constant { column, value } => Expr::Binary(
                Box::new(column_expr(column)),
                Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric(value.to_string()))),
            ),
            Term::IsNull { column } => Expr::Binary(
                Box::new(column_expr(column)),
                Operator::Is,
                Box::new(Expr::Literal(ast::Literal::Null)),
            ),
        }
    }

    fn holds(self, binding: &Binding) -> bool {
        let value = |column: ColumnRef| binding[column.table].and_then(|row| row[column.column]);
        match self {
            Term::Columns {
                left,
                compare: Compare::Equal,
                right,
            } => matches!((value(left), value(right)), (Some(l), Some(r)) if l == r),
            Term::Columns {
                left,
                compare: Compare::Is,
                right,
            } => value(left) == value(right),
            Term::Constant {
                column,
                value: expected,
            } => value(column) == Some(expected),
            Term::IsNull { column } => value(column).is_none(),
        }
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Term::Columns {
                left,
                compare,
                right,
            } => write!(f, "{left} {} {right}", compare.symbol()),
            Term::Constant { column, value } => write!(f, "{column} = {value}"),
            Term::IsNull { column } => write!(f, "{column} IS NULL"),
        }
    }
}

fn terms_text(terms: &[Term]) -> String {
    terms
        .iter()
        .map(Term::to_string)
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[derive(Clone, Debug)]
struct TableShape {
    join: Option<Join>,
    on_terms: Vec<Term>,
    rows: f64,
    indexes: Vec<(usize, bool)>,
}

#[derive(Clone, Debug)]
struct QueryShape {
    tables: Vec<TableShape>,
    where_terms: Vec<Term>,
}

impl QueryShape {
    /// A random table list. A third of the lists are small enough for the
    /// row comparison. The rest cover the dynamic programming search up to
    /// its table limit and the greedy search up to the table limit of Turso.
    fn random(rng: &mut ChaCha8Rng) -> Self {
        let table_count = table_count_from_env().unwrap_or_else(|| match rng.random_range(0..6) {
            0..=1 => rng.random_range(2..=SMALL_TABLE_LIMIT),
            2 => rng.random_range(SMALL_TABLE_LIMIT + 1..=GREEDY_JOIN_THRESHOLD),
            3 => rng.random_range(GREEDY_JOIN_THRESHOLD + 1..=24),
            4 => rng.random_range(25..=44),
            _ => rng.random_range(45..=TableReferences::MAX_JOINED_TABLES),
        });
        Self::random_with_table_count(rng, table_count)
    }

    fn random_with_table_count(rng: &mut ChaCha8Rng, table_count: usize) -> Self {
        let swarm = Swarm::random(rng);
        let mut tables: Vec<TableShape> = Vec::with_capacity(table_count);
        for index in 0..table_count {
            let join = (index > 0).then(|| swarm.random_join(rng));
            let visible: Vec<usize> = (0..index)
                .filter(|&earlier| !tables[earlier].join.is_some_and(Join::hides_columns))
                .collect();
            let on_terms = match join {
                None => Vec::new(),
                Some(join) => random_on_terms(rng, &swarm, index, join, &visible),
            };
            tables.push(TableShape {
                join,
                on_terms,
                rows: swarm.row_counts[rng.random_range(0..swarm.row_counts.len())],
                indexes: random_indexes(rng, &swarm),
            });
        }
        let where_terms = random_where_terms(rng, &swarm, &tables);
        Self {
            tables,
            where_terms,
        }
    }
}

/// The features that one table list may use. Each list draws a random
/// subset, as in swarm testing, so that a rare combination such as a chain
/// of FULL JOINs with `IS` terms and no indexes gets many rounds.
#[derive(Clone, Debug)]
struct Swarm {
    joins: Vec<Join>,
    links: Links,
    is_compare: bool,
    local_on_terms: bool,
    max_on_terms: usize,
    where_terms: bool,
    indexes: bool,
    unique_indexes: bool,
    row_counts: Vec<f64>,
}

/// The earlier table that a join term reads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Links {
    /// Any earlier table that shows its columns.
    Any,
    /// The nearest earlier table that shows its columns, which makes a chain.
    Previous,
    /// The first table, which makes a star.
    First,
}

impl Swarm {
    const ALL_JOINS: [Join; 7] = [
        Join::Inner,
        Join::Cross,
        Join::Left,
        Join::Right,
        Join::Full,
        Join::Semi,
        Join::Anti,
    ];

    fn random(rng: &mut ChaCha8Rng) -> Self {
        if rng.random_ratio(1, 4) {
            return Self::everything();
        }
        Self {
            joins: random_subset(rng, &Self::ALL_JOINS),
            links: match rng.random_range(0..3) {
                0 => Links::Any,
                1 => Links::Previous,
                _ => Links::First,
            },
            is_compare: rng.random_bool(0.5),
            local_on_terms: rng.random_bool(0.5),
            max_on_terms: rng.random_range(1..=3),
            where_terms: rng.random_bool(0.5),
            indexes: rng.random_bool(0.5),
            unique_indexes: rng.random_bool(0.5),
            row_counts: random_subset(rng, &ROW_COUNTS),
        }
    }

    fn everything() -> Self {
        Self {
            joins: Self::ALL_JOINS.to_vec(),
            links: Links::Any,
            is_compare: true,
            local_on_terms: true,
            max_on_terms: 2,
            where_terms: true,
            indexes: true,
            unique_indexes: true,
            row_counts: ROW_COUNTS.to_vec(),
        }
    }

    fn random_join(&self, rng: &mut ChaCha8Rng) -> Join {
        if self.joins.len() == Self::ALL_JOINS.len() {
            Join::random(rng)
        } else {
            self.joins[rng.random_range(0..self.joins.len())]
        }
    }

    fn link_target(&self, rng: &mut ChaCha8Rng, visible: &[usize]) -> usize {
        match self.links {
            Links::Any => visible[rng.random_range(0..visible.len())],
            Links::Previous => *visible
                .last()
                .expect("the caller checks for a visible table"),
            Links::First => visible[0],
        }
    }
}

fn random_subset<T: Copy>(rng: &mut ChaCha8Rng, items: &[T]) -> Vec<T> {
    loop {
        let subset: Vec<T> = items
            .iter()
            .copied()
            .filter(|_| rng.random_bool(0.5))
            .collect();
        if !subset.is_empty() {
            return subset;
        }
    }
}

fn random_on_terms(
    rng: &mut ChaCha8Rng,
    swarm: &Swarm,
    table: usize,
    join: Join,
    visible: &[usize],
) -> Vec<Term> {
    let count = match join {
        Join::Inner | Join::Cross => rng.random_range(0..=swarm.max_on_terms),
        _ if rng.random_ratio(1, 20) => 0,
        _ => rng.random_range(1..=swarm.max_on_terms),
    };
    (0..count)
        .map(|_| {
            let column = ColumnRef {
                table,
                column: random_column(rng),
            };
            if visible.is_empty() || (swarm.local_on_terms && rng.random_ratio(1, 8)) {
                if rng.random_ratio(1, 4) {
                    Term::IsNull { column }
                } else {
                    Term::Constant {
                        column,
                        value: rng.random_range(1..=2),
                    }
                }
            } else {
                let compare = if swarm.is_compare {
                    Compare::random(rng)
                } else {
                    Compare::Equal
                };
                Term::Columns {
                    left: column,
                    compare,
                    right: ColumnRef {
                        table: swarm.link_target(rng, visible),
                        column: random_column(rng),
                    },
                }
            }
        })
        .collect()
}

fn random_where_terms(rng: &mut ChaCha8Rng, swarm: &Swarm, tables: &[TableShape]) -> Vec<Term> {
    if !swarm.where_terms {
        return Vec::new();
    }
    let visible: Vec<usize> = (0..tables.len())
        .filter(|&table| !tables[table].join.is_some_and(Join::hides_columns))
        .collect();
    let null_extended = |table: usize| {
        tables[table].join.is_some_and(Join::null_extends_right)
            || tables[table + 1..]
                .iter()
                .any(|later| later.join.is_some_and(Join::null_extends_left))
    };
    let count = rng.random_range(0..=(tables.len() / 2).max(3));
    (0..count)
        .map(|_| {
            let first_position = rng.random_range(0..visible.len());
            let first = ColumnRef {
                table: visible[first_position],
                column: random_column(rng),
            };
            if visible.len() >= 2 && rng.random_ratio(3, 5) {
                let second_position = match swarm.links {
                    Links::Any => {
                        (first_position + 1 + rng.random_range(0..visible.len() - 1))
                            % visible.len()
                    }
                    Links::Previous => first_position.checked_sub(1).unwrap_or(1),
                    Links::First => {
                        if first_position == 0 {
                            1
                        } else {
                            0
                        }
                    }
                };
                let second = ColumnRef {
                    table: visible[second_position],
                    column: random_column(rng),
                };
                let compare = if null_extended(first.table) || null_extended(second.table) {
                    Compare::Is
                } else {
                    Compare::Equal
                };
                Term::Columns {
                    left: first,
                    compare,
                    right: second,
                }
            } else if null_extended(first.table) {
                Term::IsNull { column: first }
            } else {
                Term::Constant {
                    column: first,
                    value: rng.random_range(1..=2),
                }
            }
        })
        .collect()
}

fn random_column(rng: &mut ChaCha8Rng) -> usize {
    rng.random_range(0..COLUMN_NAMES.len())
}

fn random_indexes(rng: &mut ChaCha8Rng, swarm: &Swarm) -> Vec<(usize, bool)> {
    let mut indexes = Vec::new();
    if !swarm.indexes {
        return indexes;
    }
    for column in 0..COLUMN_NAMES.len() {
        if rng.random_ratio(1, 3) {
            indexes.push((column, swarm.unique_indexes && rng.random_ratio(1, 3)));
        }
    }
    indexes
}

impl fmt::Display for QueryShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FROM t0")?;
        for (index, table) in self.tables.iter().enumerate().skip(1) {
            let join = table.join.expect("every table after the first has a join");
            write!(f, " {} t{index}", join.keyword())?;
            if !table.on_terms.is_empty() {
                write!(f, " ON {}", terms_text(&table.on_terms))?;
            }
        }
        if !self.where_terms.is_empty() {
            write!(f, " WHERE {}", terms_text(&self.where_terms))?;
        }
        write!(f, "; rows:")?;
        for (index, table) in self.tables.iter().enumerate() {
            write!(f, " t{index}={}", table.rows)?;
        }
        let indexes: Vec<String> = self
            .tables
            .iter()
            .enumerate()
            .flat_map(|(index, table)| {
                table.indexes.iter().map(move |&(column, unique)| {
                    let unique = if unique { " unique" } else { "" };
                    format!("t{index}({}{unique})", COLUMN_NAMES[column])
                })
            })
            .collect();
        if !indexes.is_empty() {
            write!(f, "; indexes: {}", indexes.join(" "))?;
        }
        Ok(())
    }
}

fn plan_join_order(shape: &QueryShape) -> Vec<usize> {
    let joined_tables = joined_tables(shape);
    let table_ids: Vec<TableInternalId> = joined_tables
        .iter()
        .map(|table| table.internal_id)
        .collect();
    let mut available_indexes = AvailableIndexes::default();
    for (index, table) in shape.tables.iter().enumerate() {
        if table.indexes.is_empty() {
            continue;
        }
        let indexes = table
            .indexes
            .iter()
            .map(|&(column, unique)| {
                Arc::new(Index {
                    name: format!("t{index}_{}", COLUMN_NAMES[column]),
                    table_name: format!("t{index}"),
                    where_clause: None,
                    columns: std::iter::once(IndexColumn::new(COLUMN_NAMES[column], column))
                        .try_collect()
                        .expect(crate::alloc::ALLOC_ERR_MSG),
                    unique,
                    ephemeral: false,
                    root_page: 1,
                    has_rowid: true,
                    index_method: None,
                    on_conflict: None,
                })
            })
            .collect();
        available_indexes.insert_for_table_name(&joined_tables, &format!("t{index}"), indexes);
    }
    let table_ids = &table_ids;
    let mut where_clause: Vec<WhereTerm> = shape
        .tables
        .iter()
        .enumerate()
        .flat_map(|(index, table)| {
            let origin = table.join.map_or(WhereTermOrigin::Where, |join| {
                join.on_term_origin(table_ids[index])
            });
            table.on_terms.iter().map(move |term| WhereTerm {
                expr: term.expr(table_ids),
                origin,
                consumed: false,
            })
        })
        .chain(shape.where_terms.iter().map(|term| WhereTerm {
            expr: term.expr(table_ids),
            origin: WhereTermOrigin::Where,
            consumed: false,
        }))
        .collect();
    let table_references = TableReferences::new(joined_tables, vec![]);
    let schema = Schema::default();
    let constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &schema,
        &DEFAULT_PARAMS,
    )
    .expect("the generated terms are well formed");
    let base_rows: Vec<RowCountEstimate> = shape
        .tables
        .iter()
        .map(|table| RowCountEstimate::AnalyzeStats(table.rows))
        .collect();
    let mut access_methods = Vec::new();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &constraints,
        &base_rows,
        &mut access_methods,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .expect("join planning must not fail")
    .expect("a non-empty table list always has a plan");
    result.best_plan.table_numbers().collect()
}

fn joined_tables(shape: &QueryShape) -> Vec<JoinedTable> {
    let mut id_counter = TableRefIdCounter::new();
    shape
        .tables
        .iter()
        .enumerate()
        .map(|(index, table)| joined_table(index, table, id_counter.next()))
        .collect()
}

fn joined_table(index: usize, table: &TableShape, internal_id: TableInternalId) -> JoinedTable {
    let columns: Vec<Column> = COLUMN_NAMES
        .iter()
        .map(|name| {
            Column::new(
                Some(name.to_string()),
                Type::Integer.to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            )
        })
        .collect();
    let btree = Arc::new(BTreeTable::new(
        1,
        format!("t{index}"),
        crate::alloc::vec![],
        columns.try_to_vec().expect(crate::alloc::ALLOC_ERR_MSG),
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ));
    let table_object = Table::BTree(btree);
    JoinedTable {
        op: Operation::default_scan_for(&table_object),
        unmatched_right_rows_plan: None,
        table: table_object,
        identifier: format!("t{index}"),
        internal_id,
        join_info: table.join.map(Join::join_info),
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        indexed: None,
        plan_estimate: None,
    }
}

fn optimizer_allows(required: &[TableMask], order: &[usize]) -> bool {
    let mut prefix = TableMask::default();
    for &table in order {
        if !prefix.contains_all_set_bits_of(&required[table]) {
            return false;
        }
        prefix
            .set(table)
            .expect("a small table list fits in a table mask");
    }
    true
}

/// Check one join order of the optimizer.
///
/// The conflict detector decides for large table lists. For a small table
/// list, an order that the detector rejects is still valid when the
/// transformation search reaches it, and every order must give the same rows
/// as the `FROM` list on random table contents.
fn check_join_order(
    rng: &mut ChaCha8Rng,
    shape: &QueryShape,
    order: &[usize],
) -> Result<(), String> {
    let table_count = shape.tables.len();
    let mut seen = 0u64;
    for &table in order {
        assert!(
            table < table_count,
            "the plan names a table that does not exist"
        );
        seen |= bit(table);
    }
    if order.len() != table_count || seen != all_tables(table_count) {
        return Err("the order is not a permutation of the tables".to_string());
    }
    check_cross_join_positions(shape, order)?;
    let tree = InitialTree::from_shape(shape);
    let detector = check_join_order_with_conflict_detector(&tree, order);
    if table_count > SMALL_TABLE_LIMIT {
        return detector;
    }
    if let Err(reason) = detector {
        if !core_search_space(&tree).contains(&left_deep_tree(&tree, order)) {
            return Err(reason);
        }
    }
    for _ in 0..INSTANCES_PER_SHAPE {
        let instance = Instance::random(rng, table_count);
        let expected = sql_rows(shape, &instance);
        let actual = plan_rows(shape, &instance, order);
        if actual != expected {
            return Err(format!(
                "the order changes the result\n  tables:{instance}\n  expected: {}\n  got: {}",
                rows_text(&expected),
                rows_text(&actual)
            ));
        }
    }
    Ok(())
}

fn check_cross_join_positions(shape: &QueryShape, order: &[usize]) -> Result<(), String> {
    for (position, &table) in order.iter().enumerate() {
        if shape.tables[table].join != Some(Join::Cross) {
            continue;
        }
        let moved_after: Vec<String> = order[position + 1..]
            .iter()
            .filter(|&&later| later < table)
            .map(|later| format!("t{later}"))
            .collect();
        if !moved_after.is_empty() {
            return Err(format!(
                "CROSS JOIN t{table} runs before {}",
                moved_after.join(", ")
            ));
        }
    }
    Ok(())
}

fn check_join_order_with_conflict_detector(
    tree: &InitialTree,
    order: &[usize],
) -> Result<(), String> {
    let rules: Vec<Vec<ConflictRule>> = (0..tree.table_count())
        .map(|table| tree.conflict_rules(table))
        .collect();
    let first = order[0];
    if tree.operators[first].is_some_and(|op| op.kind != OpKind::Cross) {
        return Err(format!(
            "t{first} is the right side of a non-inner join and cannot be the first table"
        ));
    }
    let mut prefix = bit(first);
    for (position, &table) in order.iter().enumerate().skip(1) {
        if !step_is_applicable(tree, &rules, prefix, table) {
            return Err(format!(
                "step {position}: no operator can join t{table} to {}",
                mask_names(prefix)
            ));
        }
        prefix |= bit(table);
    }
    Ok(())
}

/// One step of a left-deep plan adds `table` to `prefix`. The step uses the
/// operator of `table`, or for a cross product any cross product that has
/// `table` on its left side and one prefix table on its right side.
fn step_is_applicable(
    tree: &InitialTree,
    rules: &[Vec<ConflictRule>],
    prefix: u64,
    table: usize,
) -> bool {
    let added = bit(table);
    match tree.operators[table] {
        Some(op) if op.kind != OpKind::Cross => {
            if op.left == added {
                applicable(op, &rules[table], added, prefix)
            } else {
                applicable(op, &rules[table], prefix, added)
            }
        }
        own => {
            let direct = own.is_some_and(|op| applicable(op, &rules[table], prefix, added));
            let commuted = (table + 1..tree.table_count())
                .filter(|&later| prefix & bit(later) != 0)
                .any(|later| {
                    tree.operators[later].is_some_and(|op| {
                        op.kind == OpKind::Cross && applicable(op, &rules[later], added, prefix)
                    })
                });
            direct || commuted
        }
    }
}

/// `APPLICABLE` from Figure 9 of the paper, with the cross product check from
/// Section 6.2.
///
/// The terms of an inner join run as a filter above the join, so they do not
/// make tables required. They only limit the conflict rules to the tables
/// they read.
fn applicable(op: OperatorNode, rules: &[ConflictRule], s1: u64, s2: u64) -> bool {
    let s = s1 | s2;
    let tes = if op.kind == OpKind::Cross {
        0
    } else {
        op.predicate.tables
    };
    (tes & op.left) & !s1 == 0
        && (tes & op.right) & !s2 == 0
        && op.left & s1 != 0
        && op.right & s2 != 0
        && rules
            .iter()
            .all(|rule| rule.from & s == 0 || rule.to & !s == 0)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum OpKind {
    Cross,
    LeftOuter,
    FullOuter,
    Semi,
    Anti,
}

#[derive(Clone, Copy, Default, Debug)]
struct Predicate {
    tables: u64,
    null_rejecting: u64,
}

impl Predicate {
    fn from_terms(terms: &[Term]) -> Self {
        terms
            .iter()
            .fold(Predicate::default(), |predicate, term| Predicate {
                tables: predicate.tables | term.tables(),
                null_rejecting: predicate.null_rejecting | term.null_rejecting_tables(),
            })
    }

    fn rejects_nulls_on(self, tables: u64) -> bool {
        self.null_rejecting & tables != 0
    }
}

/// A binary operator of the initial operator tree. A `RIGHT JOIN` is a left
/// outer join with its right table as the left input. An inner join is a
/// cross product whose terms only narrow the conflict rules.
#[derive(Clone, Copy, Debug)]
struct OperatorNode {
    kind: OpKind,
    predicate: Predicate,
    left: u64,
    right: u64,
}

/// A conflict rule `from -> to`: when a plan has a table of `from`, it must
/// have every table of `to`.
#[derive(Clone, Copy, Debug)]
struct ConflictRule {
    from: u64,
    to: u64,
}

/// The left-deep operator tree of the `FROM` list. Table `i` has operator
/// `operators[i]`, and the first table has none.
struct InitialTree {
    operators: Vec<Option<OperatorNode>>,
}

impl InitialTree {
    fn from_shape(shape: &QueryShape) -> Self {
        let operators = shape
            .tables
            .iter()
            .enumerate()
            .map(|(index, table)| {
                let join = table.join?;
                let own = bit(index);
                let earlier = own - 1;
                let (left, right) = if join == Join::Right {
                    (own, earlier)
                } else {
                    (earlier, own)
                };
                Some(OperatorNode {
                    kind: join.operator_kind(),
                    predicate: Predicate::from_terms(&table.on_terms),
                    left,
                    right,
                })
            })
            .collect();
        Self { operators }
    }

    fn table_count(&self) -> usize {
        self.operators.len()
    }

    /// CD-C from Figure 11 of the paper.
    fn conflict_rules(&self, table: usize) -> Vec<ConflictRule> {
        let Some(b) = self.operators[table] else {
            return Vec::new();
        };
        let mut rules = Vec::new();
        for a in self.operators[1..table].iter().flatten().copied() {
            let a_tables = a.left | a.right;
            if a_tables & b.left == a_tables {
                if !assoc(a.kind, b.kind, a.predicate, b.predicate, a.right) {
                    rules.push(ConflictRule {
                        from: a.right,
                        to: referenced_or_all(a.left, a.predicate),
                    });
                }
                if !l_asscom(a.kind, b.kind, a.predicate, b.predicate, a.left) {
                    rules.push(ConflictRule {
                        from: a.left,
                        to: referenced_or_all(a.right, a.predicate),
                    });
                }
            } else {
                if !assoc(b.kind, a.kind, b.predicate, a.predicate, a.left) {
                    rules.push(ConflictRule {
                        from: a.left,
                        to: referenced_or_all(a.right, a.predicate),
                    });
                }
                if !r_asscom(b.kind, a.kind, b.predicate, a.predicate, a.right) {
                    rules.push(ConflictRule {
                        from: a.right,
                        to: referenced_or_all(a.left, a.predicate),
                    });
                }
            }
        }
        rules
    }
}

fn referenced_or_all(side: u64, predicate: Predicate) -> u64 {
    let referenced = side & predicate.tables;
    if referenced == 0 {
        side
    } else {
        referenced
    }
}

/// Table 1 of the paper.
fn comm(kind: OpKind) -> bool {
    matches!(kind, OpKind::Cross | OpKind::FullOuter)
}

/// Table 2 of the paper: `(e1 a_12 e2) b_23 e3 = e1 a_12 (e2 b_23 e3)`.
fn assoc(a: OpKind, b: OpKind, p12: Predicate, p23: Predicate, e2: u64) -> bool {
    match (a, b) {
        (OpKind::Cross, OpKind::FullOuter) => false,
        (OpKind::Cross, _) => true,
        (OpKind::Semi | OpKind::Anti, _) => false,
        (OpKind::LeftOuter, OpKind::LeftOuter) => p23.rejects_nulls_on(e2),
        (OpKind::LeftOuter, _) => false,
        (OpKind::FullOuter, OpKind::LeftOuter) => p23.rejects_nulls_on(e2),
        (OpKind::FullOuter, OpKind::FullOuter) => {
            p12.rejects_nulls_on(e2) && p23.rejects_nulls_on(e2)
        }
        (OpKind::FullOuter, _) => false,
    }
}

/// Table 3 of the paper, left entries: `(e1 a_12 e2) b_13 e3 = (e1 b_13 e3) a_12 e2`.
///
/// The paper gives the condition of the full outer join row as "p13 rejects
/// nulls on A(e3)". The property is symmetric, so the condition is the one of
/// the left outer join row with the roles swapped: p13 rejects nulls on A(e1).
fn l_asscom(a: OpKind, b: OpKind, p12: Predicate, p13: Predicate, e1: u64) -> bool {
    match (a, b) {
        (OpKind::Cross | OpKind::Semi | OpKind::Anti, OpKind::FullOuter) => false,
        (OpKind::Cross | OpKind::Semi | OpKind::Anti, _) => true,
        (OpKind::LeftOuter, OpKind::FullOuter) => p12.rejects_nulls_on(e1),
        (OpKind::LeftOuter, _) => true,
        (OpKind::FullOuter, OpKind::LeftOuter) => p13.rejects_nulls_on(e1),
        (OpKind::FullOuter, OpKind::FullOuter) => {
            p12.rejects_nulls_on(e1) && p13.rejects_nulls_on(e1)
        }
        (OpKind::FullOuter, _) => false,
    }
}

/// Table 3 of the paper, right entries: `e1 a_13 (e2 b_23 e3) = e2 b_23 (e1 a_13 e3)`.
fn r_asscom(a: OpKind, b: OpKind, p13: Predicate, p23: Predicate, e3: u64) -> bool {
    match (a, b) {
        (OpKind::Cross, OpKind::Cross) => true,
        (OpKind::FullOuter, OpKind::FullOuter) => {
            p13.rejects_nulls_on(e3) && p23.rejects_nulls_on(e3)
        }
        _ => false,
    }
}

/// An operator tree of a plan. Every cross product is the same operator, so
/// `operator` is `CROSS_PRODUCT` for all of them.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Tree {
    Leaf(usize),
    Node {
        kind: OpKind,
        operator: usize,
        left: Box<Tree>,
        right: Box<Tree>,
    },
}

const CROSS_PRODUCT: usize = usize::MAX;

impl Tree {
    fn tables(&self) -> u64 {
        match self {
            Tree::Leaf(table) => bit(*table),
            Tree::Node { left, right, .. } => left.tables() | right.tables(),
        }
    }
}

fn node(kind: OpKind, operator: usize, left: Tree, right: Tree) -> Tree {
    Tree::Node {
        kind,
        operator,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn left_deep_tree(tree: &InitialTree, order: &[usize]) -> Tree {
    let mut plan = Tree::Leaf(order[0]);
    for &table in &order[1..] {
        let leaf = Tree::Leaf(table);
        plan = match tree.operators[table] {
            Some(op) if op.kind != OpKind::Cross && op.left == bit(table) => {
                node(op.kind, table, leaf, plan)
            }
            Some(op) if op.kind != OpKind::Cross => node(op.kind, table, plan, leaf),
            _ => node(OpKind::Cross, CROSS_PRODUCT, plan, leaf),
        };
    }
    plan
}

/// All plans that the four transformation rules of the paper reach from the
/// initial operator tree.
fn core_search_space(tree: &InitialTree) -> HashSet<Tree> {
    let from_list: Vec<usize> = (0..tree.table_count()).collect();
    let initial = left_deep_tree(tree, &from_list);
    let mut seen = HashSet::new();
    let mut queue = VecDeque::new();
    seen.insert(initial.clone());
    queue.push_back(initial);
    while let Some(plan) = queue.pop_front() {
        for next in transformations(tree, &plan) {
            if seen.insert(next.clone()) {
                queue.push_back(next);
            }
        }
    }
    seen
}

fn transformations(tree: &InitialTree, plan: &Tree) -> Vec<Tree> {
    let Tree::Node {
        kind,
        operator,
        left,
        right,
    } = plan
    else {
        return Vec::new();
    };
    let (kind, operator) = (*kind, *operator);
    let mut out = Vec::new();
    for new_left in transformations(tree, left) {
        out.push(node(kind, operator, new_left, (**right).clone()));
    }
    for new_right in transformations(tree, right) {
        out.push(node(kind, operator, (**left).clone(), new_right));
    }
    if comm(kind) {
        out.push(node(kind, operator, (**right).clone(), (**left).clone()));
    }
    let p_b = predicate_of(tree, operator);
    if let Tree::Node {
        kind: a_kind,
        operator: a_operator,
        left: e1,
        right: e2,
    } = &**left
    {
        let (a_kind, a_operator) = (*a_kind, *a_operator);
        let p_a = predicate_of(tree, a_operator);
        let e3 = right;
        let p_a_avoids_e3 = p_a.tables & e3.tables() == 0;
        if assoc(a_kind, kind, p_a, p_b, e2.tables())
            && p_a_avoids_e3
            && p_b.tables & e1.tables() == 0
        {
            out.push(node(
                a_kind,
                a_operator,
                (**e1).clone(),
                node(kind, operator, (**e2).clone(), (**e3).clone()),
            ));
        }
        if l_asscom(a_kind, kind, p_a, p_b, e1.tables())
            && p_a_avoids_e3
            && p_b.tables & e2.tables() == 0
        {
            out.push(node(
                a_kind,
                a_operator,
                node(kind, operator, (**e1).clone(), (**e3).clone()),
                (**e2).clone(),
            ));
        }
    }
    if let Tree::Node {
        kind: a_kind,
        operator: a_operator,
        left: e2,
        right: e3,
    } = &**right
    {
        let (a_kind, a_operator) = (*a_kind, *a_operator);
        let p_a = predicate_of(tree, a_operator);
        let e1 = left;
        let p_a_avoids_e1 = p_a.tables & e1.tables() == 0;
        if assoc(kind, a_kind, p_b, p_a, e2.tables())
            && p_b.tables & e3.tables() == 0
            && p_a_avoids_e1
        {
            out.push(node(
                a_kind,
                a_operator,
                node(kind, operator, (**e1).clone(), (**e2).clone()),
                (**e3).clone(),
            ));
        }
        if r_asscom(kind, a_kind, p_b, p_a, e3.tables())
            && p_b.tables & e2.tables() == 0
            && p_a_avoids_e1
        {
            out.push(node(
                a_kind,
                a_operator,
                (**e2).clone(),
                node(kind, operator, (**e1).clone(), (**e3).clone()),
            ));
        }
    }
    out
}

fn predicate_of(tree: &InitialTree, operator: usize) -> Predicate {
    if operator == CROSS_PRODUCT {
        Predicate::default()
    } else {
        tree.operators[operator]
            .expect("only the first table has no operator")
            .predicate
    }
}

type Row = [Option<i64>; COLUMN_NAMES.len()];
/// The current row of each table. `None` is a NULL row or a table that is
/// not read yet.
type Binding = Vec<Option<Row>>;
type Tuple = Vec<Option<i64>>;

/// Small random tables for one table list.
struct Instance {
    rows: Vec<Vec<Row>>,
}

impl Instance {
    fn random(rng: &mut ChaCha8Rng, table_count: usize) -> Self {
        let rows = (0..table_count)
            .map(|_| {
                (0..rng.random_range(0..=3))
                    .map(|_| {
                        std::array::from_fn(|_| match rng.random_range(0..3) {
                            0 => None,
                            value => Some(value),
                        })
                    })
                    .collect()
            })
            .collect();
        Self { rows }
    }
}

impl fmt::Display for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (table, rows) in self.rows.iter().enumerate() {
            let rows: Vec<String> = rows.iter().map(|row| tuple_text(row)).collect();
            write!(f, " t{table}=[{}]", rows.join(" "))?;
        }
        Ok(())
    }
}

fn rows_text(rows: &[Tuple]) -> String {
    rows.iter()
        .map(|row| tuple_text(row))
        .collect::<Vec<_>>()
        .join(" ")
}

fn tuple_text(tuple: &[Option<i64>]) -> String {
    let values: Vec<String> = tuple
        .iter()
        .map(|value| value.map_or("NULL".to_string(), |value| value.to_string()))
        .collect();
    format!("({})", values.join(","))
}

/// The rows of the `FROM` list under SQL semantics, as a sorted bag.
fn sql_rows(shape: &QueryShape, instance: &Instance) -> Vec<Tuple> {
    let table_count = shape.tables.len();
    let mut bindings: Vec<Binding> = instance.rows[0]
        .iter()
        .map(|&row| {
            let mut binding = vec![None; table_count];
            binding[0] = Some(row);
            binding
        })
        .collect();
    for (index, table) in shape.tables.iter().enumerate().skip(1) {
        let join = table.join.expect("every table after the first has a join");
        let mut next = Vec::new();
        let mut matched_right = vec![false; instance.rows[index].len()];
        for binding in &bindings {
            let mut matched_left = false;
            for (row_index, &row) in instance.rows[index].iter().enumerate() {
                let mut joined = binding.clone();
                joined[index] = Some(row);
                if !table.on_terms.iter().all(|term| term.holds(&joined)) {
                    continue;
                }
                matched_left = true;
                matched_right[row_index] = true;
                match join {
                    Join::Semi => {
                        next.push(binding.clone());
                        break;
                    }
                    Join::Anti => break,
                    Join::Inner | Join::Cross | Join::Left | Join::Right | Join::Full => {
                        next.push(joined)
                    }
                }
            }
            if !matched_left && matches!(join, Join::Left | Join::Full | Join::Anti) {
                next.push(binding.clone());
            }
        }
        if join.null_extends_left() {
            for (row_index, &row) in instance.rows[index].iter().enumerate() {
                if !matched_right[row_index] {
                    let mut binding = vec![None; table_count];
                    binding[index] = Some(row);
                    next.push(binding);
                }
            }
        }
        bindings = next;
    }
    let mut result: Vec<Tuple> = bindings
        .iter()
        .filter(|binding| shape.where_terms.iter().all(|term| term.holds(binding)))
        .map(|binding| output_tuple(shape, binding))
        .collect();
    result.sort();
    result
}

fn output_tuple(shape: &QueryShape, binding: &Binding) -> Tuple {
    shape
        .tables
        .iter()
        .enumerate()
        .filter(|(_, table)| !table.join.is_some_and(Join::hides_columns))
        .flat_map(|(index, _)| {
            (0..COLUMN_NAMES.len()).map(move |column| binding[index].and_then(|row| row[column]))
        })
        .collect()
}

/// The rows of a join order under the loop semantics of the emitter, as a
/// sorted bag.
///
/// Each loop checks the outer join terms of its table, sets the match flag,
/// then checks the other terms whose tables are all read. A LEFT or FULL JOIN
/// loop adds a NULL row when no row matched. The unmatched right rows of a
/// RIGHT or FULL JOIN run through the loop body after the main loop, with the
/// tables before that loop set to NULL.
///
/// A `WHERE` term waits for every RIGHT or FULL JOIN loop, so the unmatched
/// right rows see it. The emitter only waits for the joins whose left tables
/// the term reads. Both rules give the same loop for the orders that the
/// optimizer allows, because such an order keeps every table on the side of
/// the join where the `FROM` list has it.
fn plan_rows(shape: &QueryShape, instance: &Instance, order: &[usize]) -> Vec<Tuple> {
    let table_count = shape.tables.len();
    let mut loop_of = vec![0; table_count];
    for (position, &table) in order.iter().enumerate() {
        loop_of[table] = position;
    }
    let mut run = PlanRun {
        shape,
        instance,
        order,
        outer_terms_at: vec![Vec::new(); table_count],
        other_terms_at: vec![Vec::new(); table_count],
        binding: vec![None; table_count],
        matched_right: instance
            .rows
            .iter()
            .map(|rows| vec![false; rows.len()])
            .collect(),
        output: Vec::new(),
    };
    for (table, table_shape) in shape.tables.iter().enumerate() {
        let Some(join) = table_shape.join else {
            continue;
        };
        for &term in &table_shape.on_terms {
            let at = (0..table_count)
                .filter(|&read| term.tables() & bit(read) != 0)
                .map(|read| loop_of[read])
                .max()
                .expect("a term reads a table")
                .max(loop_of[table]);
            if join.is_outer() {
                run.outer_terms_at[at].push(term);
            } else {
                run.other_terms_at[at].push(term);
            }
        }
    }
    let right_join_loops: Vec<usize> = shape
        .tables
        .iter()
        .enumerate()
        .filter(|(_, table_shape)| table_shape.join.is_some_and(Join::null_extends_left))
        .map(|(table, _)| loop_of[table])
        .collect();
    for &term in &shape.where_terms {
        let read_tables = term.tables();
        let at = (0..table_count)
            .filter(|&read| read_tables & bit(read) != 0)
            .map(|read| loop_of[read])
            .chain(right_join_loops.iter().copied())
            .max()
            .expect("a term reads a table");
        run.other_terms_at[at].push(term);
    }
    run.run_loop(0);
    for (table, &position) in loop_of.iter().enumerate() {
        if !shape.tables[table]
            .join
            .is_some_and(Join::null_extends_left)
        {
            continue;
        }
        for row_index in 0..instance.rows[table].len() {
            if run.matched_right[table][row_index] {
                continue;
            }
            run.binding = vec![None; table_count];
            run.binding[table] = Some(instance.rows[table][row_index]);
            if run.terms_hold(position, false) {
                run.run_loop(position + 1);
            }
        }
    }
    run.output.sort();
    run.output
}

struct PlanRun<'a> {
    shape: &'a QueryShape,
    instance: &'a Instance,
    order: &'a [usize],
    outer_terms_at: Vec<Vec<Term>>,
    other_terms_at: Vec<Vec<Term>>,
    binding: Binding,
    matched_right: Vec<Vec<bool>>,
    output: Vec<Tuple>,
}

impl PlanRun<'_> {
    fn run_loop(&mut self, position: usize) {
        if position == self.order.len() {
            self.output.push(output_tuple(self.shape, &self.binding));
            return;
        }
        let table = self.order[position];
        let row_count = self.instance.rows[table].len();
        match self.shape.tables[table].join {
            None | Some(Join::Inner | Join::Cross) => {
                for row_index in 0..row_count {
                    self.binding[table] = Some(self.instance.rows[table][row_index]);
                    if self.terms_hold(position, true) && self.terms_hold(position, false) {
                        self.run_loop(position + 1);
                    }
                }
            }
            Some(Join::Left) => {
                let mut matched = false;
                for row_index in 0..row_count {
                    self.binding[table] = Some(self.instance.rows[table][row_index]);
                    if !self.terms_hold(position, true) {
                        continue;
                    }
                    matched = true;
                    if self.terms_hold(position, false) {
                        self.run_loop(position + 1);
                    }
                }
                if !matched {
                    self.binding[table] = None;
                    if self.terms_hold(position, false) {
                        self.run_loop(position + 1);
                    }
                }
            }
            Some(Join::Right) => {
                for row_index in 0..row_count {
                    self.binding[table] = Some(self.instance.rows[table][row_index]);
                    if !self.terms_hold(position, true) {
                        continue;
                    }
                    self.matched_right[table][row_index] = true;
                    if self.terms_hold(position, false) {
                        self.run_loop(position + 1);
                    }
                }
            }
            Some(Join::Full) => {
                let mut matched = false;
                for row_index in 0..row_count {
                    self.binding[table] = Some(self.instance.rows[table][row_index]);
                    if !self.terms_hold(position, true) {
                        continue;
                    }
                    matched = true;
                    self.matched_right[table][row_index] = true;
                    if self.terms_hold(position, false) {
                        self.run_loop(position + 1);
                    }
                }
                if !matched {
                    self.binding[table] = None;
                    if self.terms_hold(position, false) {
                        self.run_loop(position + 1);
                    }
                }
            }
            Some(Join::Semi) => {
                for row_index in 0..row_count {
                    self.binding[table] = Some(self.instance.rows[table][row_index]);
                    if self.terms_hold(position, true) && self.terms_hold(position, false) {
                        self.run_loop(position + 1);
                        break;
                    }
                }
            }
            Some(Join::Anti) => {
                let mut matched = false;
                for row_index in 0..row_count {
                    self.binding[table] = Some(self.instance.rows[table][row_index]);
                    if self.terms_hold(position, true) && self.terms_hold(position, false) {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    self.binding[table] = None;
                    self.run_loop(position + 1);
                }
            }
        }
        self.binding[table] = None;
    }

    fn terms_hold(&self, position: usize, outer: bool) -> bool {
        let terms = if outer {
            &self.outer_terms_at[position]
        } else {
            &self.other_terms_at[position]
        };
        terms.iter().all(|term| term.holds(&self.binding))
    }
}

fn permutations(table_count: usize) -> Vec<Vec<usize>> {
    fn extend(prefix: &mut Vec<usize>, remaining: u64, out: &mut Vec<Vec<usize>>) {
        if remaining == 0 {
            out.push(prefix.clone());
            return;
        }
        for table in 0..u64::BITS as usize {
            if remaining & bit(table) == 0 {
                continue;
            }
            prefix.push(table);
            extend(prefix, remaining & !bit(table), out);
            prefix.pop();
        }
    }
    let mut out = Vec::new();
    extend(&mut Vec::new(), all_tables(table_count), &mut out);
    out
}

fn bit(table: usize) -> u64 {
    1 << table
}

fn all_tables(table_count: usize) -> u64 {
    (1 << table_count) - 1
}

fn mask_names(mask: u64) -> String {
    let names: Vec<String> = (0..u64::BITS as usize)
        .filter(|&table| mask & bit(table) != 0)
        .map(|table| format!("t{table}"))
        .collect();
    format!("{{{}}}", names.join(", "))
}

fn order_names(order: &[usize]) -> String {
    order
        .iter()
        .map(|table| format!("t{table}"))
        .collect::<Vec<_>>()
        .join(", ")
}
