use std::cell::Cell;

use super::{
    expr::walk_expr,
    plan::{
        Aggregate, ColumnUsedMask, Distinctness, EvalAt, IterationDirection, JoinInfo,
        JoinOrderMember, JoinedTable, Operation, OuterQueryReference, Plan, QueryDestination,
        ResultSetColumn, TableReferences, WhereTerm,
    },
    select::prepare_select_plan,
    SymbolTable,
};
use crate::{
    function::Func,
    schema::{Schema, Table},
    translate::expr::walk_expr_mut,
    util::{exprs_are_equivalent, normalize_ident},
    vdbe::{builder::TableRefIdCounter, BranchOffset},
    Result,
};
use limbo_sqlite3_parser::ast::{
    self, Expr, FromClause, JoinType, Limit, Materialized, TableInternalId, UnaryOperator, With,
};

pub const ROWID: &str = "rowid";

pub fn resolve_aggregates(top_level_expr: &Expr, aggs: &mut Vec<Aggregate>) -> Result<bool> {
    let mut contains_aggregates = false;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<()> {
        if aggs
            .iter()
            .any(|a| exprs_are_equivalent(&a.original_expr, expr))
        {
            contains_aggregates = true;
            return Ok(());
        }
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinctness,
                ..
            } => {
                let args_count = if let Some(args) = &args {
                    args.len()
                } else {
                    0
                };
                match Func::resolve_function(normalize_ident(name.0.as_str()).as_str(), args_count)
                {
                    Ok(Func::Agg(f)) => {
                        let distinctness = Distinctness::from_ast(distinctness.as_ref());
                        let num_args = args.as_ref().map_or(0, |args| args.len());
                        if distinctness.is_distinct() && num_args != 1 {
                            crate::bail_parse_error!(
                                "DISTINCT aggregate functions must have exactly one argument"
                            );
                        }
                        aggs.push(Aggregate {
                            func: f,
                            args: args.clone().unwrap_or_default(),
                            original_expr: expr.clone(),
                            distinctness,
                        });
                        contains_aggregates = true;
                    }
                    _ => {
                        if let Some(args) = args {
                            for arg in args.iter() {
                                contains_aggregates |= resolve_aggregates(arg, aggs)?;
                            }
                        }
                    }
                }
            }
            Expr::FunctionCallStar { name, .. } => {
                if let Ok(Func::Agg(f)) =
                    Func::resolve_function(normalize_ident(name.0.as_str()).as_str(), 0)
                {
                    aggs.push(Aggregate {
                        func: f,
                        args: vec![],
                        original_expr: expr.clone(),
                        distinctness: Distinctness::NonDistinct,
                    });
                    contains_aggregates = true;
                }
            }
            _ => {}
        }

        Ok(())
    })?;

    Ok(contains_aggregates)
}

pub fn bind_column_references(
    top_level_expr: &mut Expr,
    referenced_tables: &mut TableReferences,
    result_columns: Option<&[ResultSetColumn]>,
) -> Result<()> {
    walk_expr_mut(top_level_expr, &mut |expr: &mut Expr| -> Result<()> {
        match expr {
            Expr::Id(id) => {
                // true and false are special constants that are effectively aliases for 1 and 0
                // and not identifiers of columns
                if id.0.eq_ignore_ascii_case("true") || id.0.eq_ignore_ascii_case("false") {
                    return Ok(());
                }
                let normalized_id = normalize_ident(id.0.as_str());

                if !referenced_tables.joined_tables().is_empty() {
                    if let Some(row_id_expr) = parse_row_id(
                        &normalized_id,
                        referenced_tables.joined_tables()[0].internal_id,
                        || referenced_tables.joined_tables().len() != 1,
                    )? {
                        *expr = row_id_expr;

                        return Ok(());
                    }
                }
                let mut match_result = None;

                // First check joined tables
                for joined_table in referenced_tables.joined_tables().iter() {
                    let col_idx = joined_table.table.columns().iter().position(|c| {
                        c.name
                            .as_ref()
                            .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
                    });
                    if col_idx.is_some() {
                        if match_result.is_some() {
                            crate::bail_parse_error!("Column {} is ambiguous", id.0);
                        }
                        let col = joined_table.table.columns().get(col_idx.unwrap()).unwrap();
                        match_result = Some((
                            joined_table.internal_id,
                            col_idx.unwrap(),
                            col.is_rowid_alias,
                        ));
                    }
                }

                // Then check outer query references, if we still didn't find something.
                // Normally finding multiple matches for a non-qualified column is an error (column x is ambiguous)
                // but in the case of subqueries, the inner query takes precedence.
                // For example:
                // SELECT * FROM t WHERE x = (SELECT x FROM t2)
                // In this case, there is no ambiguity:
                // - x in the outer query refers to t.x,
                // - x in the inner query refers to t2.x.
                if match_result.is_none() {
                    for outer_ref in referenced_tables.outer_query_refs().iter() {
                        let col_idx = outer_ref.table.columns().iter().position(|c| {
                            c.name
                                .as_ref()
                                .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
                        });
                        if col_idx.is_some() {
                            if match_result.is_some() {
                                crate::bail_parse_error!("Column {} is ambiguous", id.0);
                            }
                            let col = outer_ref.table.columns().get(col_idx.unwrap()).unwrap();
                            match_result =
                                Some((outer_ref.internal_id, col_idx.unwrap(), col.is_rowid_alias));
                        }
                    }
                }

                if let Some((table_id, col_idx, is_rowid_alias)) = match_result {
                    *expr = Expr::Column {
                        database: None, // TODO: support different databases
                        table: table_id,
                        column: col_idx,
                        is_rowid_alias,
                    };
                    referenced_tables.mark_column_used(table_id, col_idx);
                    return Ok(());
                }

                if let Some(result_columns) = result_columns {
                    for result_column in result_columns.iter() {
                        if result_column
                            .name(referenced_tables)
                            .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
                        {
                            *expr = result_column.expr.clone();
                            return Ok(());
                        }
                    }
                }
                crate::bail_parse_error!("Column {} not found", id.0);
            }
            Expr::Qualified(tbl, id) => {
                let normalized_table_name = normalize_ident(tbl.0.as_str());
                let matching_tbl = referenced_tables
                    .find_table_and_internal_id_by_identifier(&normalized_table_name);
                if matching_tbl.is_none() {
                    crate::bail_parse_error!("Table {} not found", normalized_table_name);
                }
                let (tbl_id, tbl) = matching_tbl.unwrap();
                let normalized_id = normalize_ident(id.0.as_str());

                if let Some(row_id_expr) = parse_row_id(&normalized_id, tbl_id, || false)? {
                    *expr = row_id_expr;

                    return Ok(());
                }
                let col_idx = tbl.columns().iter().position(|c| {
                    c.name
                        .as_ref()
                        .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
                });
                let Some(col_idx) = col_idx else {
                    crate::bail_parse_error!("Column {} not found", normalized_id);
                };
                let col = tbl.columns().get(col_idx).unwrap();
                *expr = Expr::Column {
                    database: None, // TODO: support different databases
                    table: tbl_id,
                    column: col_idx,
                    is_rowid_alias: col.is_rowid_alias,
                };
                referenced_tables.mark_column_used(tbl_id, col_idx);
                Ok(())
            }
            _ => Ok(()),
        }
    })
}

fn parse_from_clause_table<'a>(
    schema: &Schema,
    table: ast::SelectTable,
    table_references: &mut TableReferences,
    ctes: &mut Vec<JoinedTable>,
    syms: &SymbolTable,
    table_ref_counter: &mut TableRefIdCounter,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => {
            let normalized_qualified_name = normalize_ident(qualified_name.name.0.as_str());
            // Check if the FROM clause table is referring to a CTE in the current scope.
            if let Some(cte_idx) = ctes
                .iter()
                .position(|cte| cte.identifier == normalized_qualified_name)
            {
                // TODO: what if the CTE is referenced multiple times?
                let cte_table = ctes.remove(cte_idx);
                table_references.add_joined_table(cte_table);
                return Ok(());
            };

            // Check if our top level schema has this table.
            if let Some(table) = schema.get_table(&normalized_qualified_name) {
                let alias = maybe_alias
                    .map(|a| match a {
                        ast::As::As(id) => id,
                        ast::As::Elided(id) => id,
                    })
                    .map(|a| a.0);
                let tbl_ref = if let Table::Virtual(tbl) = table.as_ref() {
                    Table::Virtual(tbl.clone())
                } else if let Table::BTree(table) = table.as_ref() {
                    Table::BTree(table.clone())
                } else {
                    return Err(crate::LimboError::InvalidArgument(
                        "Table type not supported".to_string(),
                    ));
                };
                table_references.add_joined_table(JoinedTable {
                    op: Operation::Scan {
                        iter_dir: IterationDirection::Forwards,
                        index: None,
                    },
                    table: tbl_ref,
                    identifier: alias.unwrap_or(normalized_qualified_name),
                    internal_id: table_ref_counter.next(),
                    join_info: None,
                    col_used_mask: ColumnUsedMask::new(),
                });
                return Ok(());
            };

            // CTEs are transformed into FROM clause subqueries.
            // If we find a CTE with this name in our outer query references,
            // we can use it as a joined table, but we must clone it since it's not MATERIALIZED.
            //
            // For other types of tables in the outer query references, we do not add them as joined tables,
            // because the query can simply _reference_ them in e.g. the SELECT columns or the WHERE clause,
            // but it's not part of the join order.
            if let Some(outer_ref) =
                table_references.find_outer_query_ref_by_identifier(&normalized_qualified_name)
            {
                if matches!(outer_ref.table, Table::FromClauseSubquery(_)) {
                    table_references.add_joined_table(JoinedTable {
                        op: Operation::Scan {
                            iter_dir: IterationDirection::Forwards,
                            index: None,
                        },
                        table: outer_ref.table.clone(),
                        identifier: outer_ref.identifier.clone(),
                        internal_id: table_ref_counter.next(),
                        join_info: None,
                        col_used_mask: ColumnUsedMask::new(),
                    });
                    return Ok(());
                }
            }

            crate::bail_parse_error!("Table {} not found", normalized_qualified_name);
        }
        ast::SelectTable::Select(subselect, maybe_alias) => {
            let Plan::Select(subplan) = prepare_select_plan(
                schema,
                *subselect,
                syms,
                table_references.outer_query_refs(),
                table_ref_counter,
                QueryDestination::CoroutineYield {
                    yield_reg: usize::MAX, // will be set later in bytecode emission
                    coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
                },
            )?
            else {
                crate::bail_parse_error!("Only non-compound SELECT queries are currently supported in FROM clause subqueries");
            };
            let cur_table_index = table_references.joined_tables().len();
            let identifier = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id.0.clone(),
                    ast::As::Elided(id) => id.0.clone(),
                })
                .unwrap_or(format!("subquery_{}", cur_table_index));
            table_references.add_joined_table(JoinedTable::new_subquery(
                identifier,
                subplan,
                None,
                table_ref_counter.next(),
            ));
            Ok(())
        }
        ast::SelectTable::TableCall(qualified_name, maybe_args, maybe_alias) => {
            let normalized_name = &normalize_ident(qualified_name.name.0.as_str());
            let vtab = crate::VirtualTable::function(normalized_name, maybe_args, syms)?;
            let alias = maybe_alias
                .as_ref()
                .map(|a| match a {
                    ast::As::As(id) => id.0.clone(),
                    ast::As::Elided(id) => id.0.clone(),
                })
                .unwrap_or(normalized_name.to_string());

            table_references.add_joined_table(JoinedTable {
                op: Operation::Scan {
                    iter_dir: IterationDirection::Forwards,
                    index: None,
                },
                join_info: None,
                table: Table::Virtual(vtab),
                identifier: alias,
                internal_id: table_ref_counter.next(),
                col_used_mask: ColumnUsedMask::new(),
            });

            Ok(())
        }
        _ => todo!(),
    }
}

pub fn parse_from<'a>(
    schema: &Schema,
    mut from: Option<FromClause>,
    syms: &SymbolTable,
    with: Option<With>,
    out_where_clause: &mut Vec<WhereTerm>,
    table_references: &mut TableReferences,
    table_ref_counter: &mut TableRefIdCounter,
) -> Result<()> {
    if from.as_ref().and_then(|f| f.select.as_ref()).is_none() {
        return Ok(());
    }

    let mut ctes_as_subqueries = vec![];

    if let Some(with) = with {
        if with.recursive {
            crate::bail_parse_error!("Recursive CTEs are not yet supported");
        }
        for cte in with.ctes {
            if cte.materialized == Materialized::Yes {
                crate::bail_parse_error!("Materialized CTEs are not yet supported");
            }
            if cte.columns.is_some() {
                crate::bail_parse_error!("CTE columns are not yet supported");
            }

            // Check if normalized name conflicts with catalog tables or other CTEs
            // TODO: sqlite actually allows overriding a catalog table with a CTE.
            // We should carry over the 'Scope' struct to all of our identifier resolution.
            let cte_name_normalized = normalize_ident(&cte.tbl_name.0);
            if schema.get_table(&cte_name_normalized).is_some() {
                crate::bail_parse_error!(
                    "CTE name {} conflicts with catalog table name",
                    cte.tbl_name.0
                );
            }
            if table_references
                .outer_query_refs()
                .iter()
                .any(|t| t.identifier == cte_name_normalized)
            {
                crate::bail_parse_error!(
                    "CTE name {} conflicts with WITH table name {}",
                    cte.tbl_name.0,
                    cte_name_normalized
                );
            }

            let mut outer_query_refs_for_cte = table_references.outer_query_refs().to_vec();
            outer_query_refs_for_cte.extend(ctes_as_subqueries.iter().map(|t: &JoinedTable| {
                OuterQueryReference {
                    identifier: t.identifier.clone(),
                    internal_id: t.internal_id,
                    table: t.table.clone(),
                    col_used_mask: ColumnUsedMask::new(),
                }
            }));

            // CTE can refer to other CTEs that came before it, plus any schema tables or tables in the outer scope.
            let cte_plan = prepare_select_plan(
                schema,
                *cte.select,
                syms,
                &outer_query_refs_for_cte,
                table_ref_counter,
                QueryDestination::CoroutineYield {
                    yield_reg: usize::MAX, // will be set later in bytecode emission
                    coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
                },
            )?;
            let Plan::Select(cte_plan) = cte_plan else {
                crate::bail_parse_error!("Only SELECT queries are currently supported in CTEs");
            };
            ctes_as_subqueries.push(JoinedTable::new_subquery(
                cte_name_normalized,
                cte_plan,
                None,
                table_ref_counter.next(),
            ));
        }
    }

    let mut from_owned = std::mem::take(&mut from).unwrap();
    let select_owned = *std::mem::take(&mut from_owned.select).unwrap();
    let joins_owned = std::mem::take(&mut from_owned.joins).unwrap_or_default();
    parse_from_clause_table(
        schema,
        select_owned,
        table_references,
        &mut ctes_as_subqueries,
        syms,
        table_ref_counter,
    )?;

    for join in joins_owned.into_iter() {
        parse_join(
            schema,
            join,
            syms,
            &mut ctes_as_subqueries,
            out_where_clause,
            table_references,
            table_ref_counter,
        )?;
    }

    Ok(())
}

pub fn parse_where(
    where_clause: Option<Expr>,
    table_references: &mut TableReferences,
    result_columns: Option<&[ResultSetColumn]>,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    if let Some(where_expr) = where_clause {
        let mut predicates = vec![];
        break_predicate_at_and_boundaries(where_expr, &mut predicates);
        for expr in predicates.iter_mut() {
            bind_column_references(expr, table_references, result_columns)?;
        }
        for expr in predicates {
            out_where_clause.push(WhereTerm {
                expr,
                from_outer_join: None,
                consumed: Cell::new(false),
            });
        }
        Ok(())
    } else {
        Ok(())
    }
}

/**
  Returns the earliest point at which a WHERE term can be evaluated.
  For expressions referencing tables, this is the innermost loop that contains a row for each
  table referenced in the expression.
  For expressions not referencing any tables (e.g. constants), this is before the main loop is
  opened, because they do not need any table data.
*/
pub fn determine_where_to_eval_term(
    term: &WhereTerm,
    join_order: &[JoinOrderMember],
) -> Result<EvalAt> {
    if let Some(table_id) = term.from_outer_join {
        return Ok(EvalAt::Loop(
            join_order
                .iter()
                .position(|t| t.table_id == table_id)
                .unwrap_or(usize::MAX),
        ));
    }

    return determine_where_to_eval_expr(&term.expr, join_order);
}

/// A bitmask representing a set of tables in a query plan.
/// Tables are numbered by their index in [SelectPlan::joined_tables].
/// In the bitmask, the first bit is unused so that a mask with all zeros
/// can represent "no tables".
///
/// E.g. table 0 is represented by bit index 1, table 1 by bit index 2, etc.
///
/// Usage in Join Optimization
///
/// In join optimization, [TableMask] is used to:
/// - Generate subsets of tables for dynamic programming in join optimization
/// - Ensure tables are joined in valid orders (e.g., respecting LEFT JOIN order)
///
/// Usage with constraints (WHERE clause)
///
/// [TableMask] helps determine:
/// - Which tables are referenced in a constraint
/// - When a constraint can be applied as a join condition (all referenced tables must be on the left side of the table being joined)
///
/// Note that although [TableReference]s contain an internal ID as well, in join order optimization
/// the [TableMask] refers to the index of the table in the original join order, not the internal ID.
/// This is simply because we want to represent the tables as a contiguous set of bits, and the internal ID
/// might not be contiguous after e.g. subquery unnesting or other transformations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableMask(pub u128);

impl std::ops::BitOrAssign for TableMask {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl TableMask {
    /// Creates a new empty table mask.
    ///
    /// The initial mask represents an empty set of tables.
    pub fn new() -> Self {
        Self(0)
    }

    /// Returns true if the mask represents an empty set of tables.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Creates a new mask that is the same as this one but without the specified table.
    pub fn without_table(&self, table_no: usize) -> Self {
        assert!(table_no < 127, "table_no must be less than 127");
        Self(self.0 ^ (1 << (table_no + 1)))
    }

    /// Creates a table mask from raw bits.
    ///
    /// The bits are shifted left by 1 to maintain the convention that table 0 is at bit 1.
    pub fn from_bits(bits: u128) -> Self {
        Self(bits << 1)
    }

    /// Creates a table mask from an iterator of table numbers.
    pub fn from_table_number_iter(iter: impl Iterator<Item = usize>) -> Self {
        iter.fold(Self::new(), |mut mask, table_no| {
            assert!(table_no < 127, "table_no must be less than 127");
            mask.add_table(table_no);
            mask
        })
    }

    /// Adds a table to the mask.
    pub fn add_table(&mut self, table_no: usize) {
        assert!(table_no < 127, "table_no must be less than 127");
        self.0 |= 1 << (table_no + 1);
    }

    /// Returns true if the mask contains the specified table.
    pub fn contains_table(&self, table_no: usize) -> bool {
        assert!(table_no < 127, "table_no must be less than 127");
        self.0 & (1 << (table_no + 1)) != 0
    }

    /// Returns true if this mask contains all tables in the other mask.
    pub fn contains_all(&self, other: &TableMask) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the number of tables in the mask.
    pub fn table_count(&self) -> usize {
        self.0.count_ones() as usize
    }

    /// Returns true if this mask shares any tables with the other mask.
    pub fn intersects(&self, other: &TableMask) -> bool {
        self.0 & other.0 != 0
    }
}

/// Returns a [TableMask] representing the tables referenced in the given expression.
/// Used in the optimizer for constraint analysis.
pub fn table_mask_from_expr(
    top_level_expr: &Expr,
    table_references: &TableReferences,
) -> Result<TableMask> {
    let mut mask = TableMask::new();
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<()> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if let Some(table_idx) = table_references
                    .joined_tables()
                    .iter()
                    .position(|t| t.internal_id == *table)
                {
                    mask.add_table(table_idx);
                } else if table_references
                    .find_outer_query_ref_by_internal_id(*table)
                    .is_none()
                {
                    // Tables from outer query scopes are guaranteed to be 'in scope' for this query,
                    // so they don't need to be added to the table mask. However, if the table is not found
                    // in the outer scope either, then it's an invalid reference.
                    crate::bail_parse_error!("table not found in joined_tables");
                }
            }
            _ => {}
        }
        Ok(())
    })?;

    Ok(mask)
}

pub fn determine_where_to_eval_expr<'a>(
    top_level_expr: &'a Expr,
    join_order: &[JoinOrderMember],
) -> Result<EvalAt> {
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<()> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                let join_idx = join_order
                    .iter()
                    .position(|t| t.table_id == *table)
                    .unwrap_or(usize::MAX);
                eval_at = eval_at.max(EvalAt::Loop(join_idx));
            }
            _ => {}
        }
        Ok(())
    })?;

    Ok(eval_at)
}

fn parse_join<'a>(
    schema: &Schema,
    join: ast::JoinedSelectTable,
    syms: &SymbolTable,
    ctes: &mut Vec<JoinedTable>,
    out_where_clause: &mut Vec<WhereTerm>,
    table_references: &mut TableReferences,
    table_ref_counter: &mut TableRefIdCounter,
) -> Result<()> {
    let ast::JoinedSelectTable {
        operator: join_operator,
        table,
        constraint,
    } = join;

    parse_from_clause_table(
        schema,
        table,
        table_references,
        ctes,
        syms,
        table_ref_counter,
    )?;

    let (outer, natural) = match join_operator {
        ast::JoinOperator::TypedJoin(Some(join_type)) => {
            let is_outer = join_type.contains(JoinType::OUTER);
            let is_natural = join_type.contains(JoinType::NATURAL);
            (is_outer, is_natural)
        }
        _ => (false, false),
    };

    let mut using = None;

    if natural && constraint.is_some() {
        crate::bail_parse_error!("NATURAL JOIN cannot be combined with ON or USING clause");
    }

    let constraint = if natural {
        assert!(table_references.joined_tables().len() >= 2);
        let rightmost_table = table_references.joined_tables().last().unwrap();
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let right_cols = rightmost_table.columns();
        let mut distinct_names: Option<ast::DistinctNames> = None;
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        for right_col in right_cols.iter() {
            let mut found_match = false;
            for left_table in table_references
                .joined_tables()
                .iter()
                .take(table_references.joined_tables().len() - 1)
            {
                for left_col in left_table.columns().iter() {
                    if left_col.name == right_col.name {
                        if let Some(distinct_names) = distinct_names.as_mut() {
                            distinct_names
                                .insert(ast::Name(
                                    left_col.name.clone().expect("column name is None"),
                                ))
                                .unwrap();
                        } else {
                            distinct_names = Some(ast::DistinctNames::new(ast::Name(
                                left_col.name.clone().expect("column name is None"),
                            )));
                        }
                        found_match = true;
                        break;
                    }
                }
                if found_match {
                    break;
                }
            }
        }
        if let Some(distinct_names) = distinct_names {
            Some(ast::JoinConstraint::Using(distinct_names))
        } else {
            crate::bail_parse_error!("No columns found to NATURAL join on");
        }
    } else {
        constraint
    };

    if let Some(constraint) = constraint {
        match constraint {
            ast::JoinConstraint::On(expr) => {
                let mut preds = vec![];
                break_predicate_at_and_boundaries(expr, &mut preds);
                for predicate in preds.iter_mut() {
                    bind_column_references(predicate, table_references, None)?;
                }
                for pred in preds {
                    out_where_clause.push(WhereTerm {
                        expr: pred,
                        from_outer_join: if outer {
                            Some(table_references.joined_tables().last().unwrap().internal_id)
                        } else {
                            None
                        },
                        consumed: Cell::new(false),
                    });
                }
            }
            ast::JoinConstraint::Using(distinct_names) => {
                // USING join is replaced with a list of equality predicates
                for distinct_name in distinct_names.iter() {
                    let name_normalized = normalize_ident(distinct_name.0.as_str());
                    let cur_table_idx = table_references.joined_tables().len() - 1;
                    let left_tables = &table_references.joined_tables()[..cur_table_idx];
                    assert!(!left_tables.is_empty());
                    let right_table = table_references.joined_tables().last().unwrap();
                    let mut left_col = None;
                    for (left_table_idx, left_table) in left_tables.iter().enumerate() {
                        left_col = left_table
                            .columns()
                            .iter()
                            .enumerate()
                            .find(|(_, col)| {
                                col.name
                                    .as_ref()
                                    .map_or(false, |name| *name == name_normalized)
                            })
                            .map(|(idx, col)| (left_table_idx, left_table.internal_id, idx, col));
                        if left_col.is_some() {
                            break;
                        }
                    }
                    if left_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.0
                        );
                    }
                    let right_col = right_table.columns().iter().enumerate().find(|(_, col)| {
                        col.name
                            .as_ref()
                            .map_or(false, |name| *name == name_normalized)
                    });
                    if right_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.0
                        );
                    }
                    let (left_table_idx, left_table_id, left_col_idx, left_col) = left_col.unwrap();
                    let (right_col_idx, right_col) = right_col.unwrap();
                    let expr = Expr::Binary(
                        Box::new(Expr::Column {
                            database: None,
                            table: left_table_id,
                            column: left_col_idx,
                            is_rowid_alias: left_col.is_rowid_alias,
                        }),
                        ast::Operator::Equals,
                        Box::new(Expr::Column {
                            database: None,
                            table: right_table.internal_id,
                            column: right_col_idx,
                            is_rowid_alias: right_col.is_rowid_alias,
                        }),
                    );

                    let left_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(left_table_idx)
                        .unwrap();
                    left_table.mark_column_used(left_col_idx);
                    let right_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(cur_table_idx)
                        .unwrap();
                    right_table.mark_column_used(right_col_idx);
                    out_where_clause.push(WhereTerm {
                        expr,
                        from_outer_join: if outer {
                            Some(right_table.internal_id)
                        } else {
                            None
                        },
                        consumed: Cell::new(false),
                    });
                }
                using = Some(distinct_names);
            }
        }
    }

    assert!(table_references.joined_tables().len() >= 2);
    let last_idx = table_references.joined_tables().len() - 1;
    let rightmost_table = table_references
        .joined_tables_mut()
        .get_mut(last_idx)
        .unwrap();
    rightmost_table.join_info = Some(JoinInfo { outer, using });

    Ok(())
}

pub fn parse_limit(limit: &Limit) -> Result<(Option<isize>, Option<isize>)> {
    let offset_val = match &limit.offset {
        Some(offset_expr) => match offset_expr {
            Expr::Literal(ast::Literal::Numeric(n)) => n.parse().ok(),
            // If OFFSET is negative, the result is as if OFFSET is zero
            Expr::Unary(UnaryOperator::Negative, expr) => {
                if let Expr::Literal(ast::Literal::Numeric(ref n)) = &**expr {
                    n.parse::<isize>().ok().map(|num| -num)
                } else {
                    crate::bail_parse_error!("Invalid OFFSET clause");
                }
            }
            _ => crate::bail_parse_error!("Invalid OFFSET clause"),
        },
        None => Some(0),
    };

    if let Expr::Literal(ast::Literal::Numeric(n)) = &limit.expr {
        Ok((n.parse().ok(), offset_val))
    } else if let Expr::Unary(UnaryOperator::Negative, expr) = &limit.expr {
        if let Expr::Literal(ast::Literal::Numeric(n)) = &**expr {
            let limit_val = n.parse::<isize>().ok().map(|num| -num);
            Ok((limit_val, offset_val))
        } else {
            crate::bail_parse_error!("Invalid LIMIT clause");
        }
    } else if let Expr::Id(id) = &limit.expr {
        if id.0.eq_ignore_ascii_case("true") {
            Ok((Some(1), offset_val))
        } else if id.0.eq_ignore_ascii_case("false") {
            Ok((Some(0), offset_val))
        } else {
            crate::bail_parse_error!("Invalid LIMIT clause");
        }
    } else {
        crate::bail_parse_error!("Invalid LIMIT clause");
    }
}

pub fn break_predicate_at_and_boundaries(predicate: Expr, out_predicates: &mut Vec<Expr>) {
    match predicate {
        Expr::Binary(left, ast::Operator::And, right) => {
            break_predicate_at_and_boundaries(*left, out_predicates);
            break_predicate_at_and_boundaries(*right, out_predicates);
        }
        _ => {
            out_predicates.push(predicate);
        }
    }
}

fn parse_row_id<F>(
    column_name: &str,
    table_id: TableInternalId,
    fn_check: F,
) -> Result<Option<Expr>>
where
    F: FnOnce() -> bool,
{
    if column_name.eq_ignore_ascii_case(ROWID) {
        if fn_check() {
            crate::bail_parse_error!("ROWID is ambiguous");
        }

        return Ok(Some(Expr::RowId {
            database: None, // TODO: support different databases
            table: table_id,
        }));
    }
    Ok(None)
}
