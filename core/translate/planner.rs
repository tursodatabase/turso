use std::cmp::PartialEq;
use std::sync::Arc;

use super::{
    expr::walk_expr,
    plan::{
        Aggregate, ColumnUsedMask, Distinctness, EvalAt, IterationDirection, JoinInfo,
        JoinOrderMember, JoinedTable, Operation, OuterQueryReference, Plan, QueryDestination,
        ResultSetColumn, Scan, TableReferences, WhereTerm,
    },
    select::prepare_select_plan,
};
use crate::translate::{
    emitter::Resolver,
    expr::{BindingBehavior, WalkControl},
};
use crate::{
    ast::Limit,
    function::Func,
    schema::Table,
    util::{exprs_are_equivalent, normalize_ident},
    Result,
};
use crate::{
    function::{AggFunc, ExtFunc},
    translate::expr::{bind_and_rewrite_expr, ParamState},
};
use crate::{
    translate::plan::{Window, WindowFunction},
    vdbe::builder::ProgramBuilder,
};
use turso_parser::ast::Literal::Null;
use turso_parser::ast::{
    self, As, Expr, FromClause, JoinType, Materialized, Over, QualifiedName, TableInternalId, With,
};

/// Valid ways to refer to the rowid of a btree table.
pub const ROWID_STRS: [&str; 3] = ["rowid", "_rowid_", "oid"];

/// This function walks the expression tree and identifies aggregate
/// and window functions.
///
/// # Window functions
/// - If `windows` is `Some`, window functions will be resolved against the
///   provided set of windows or added to it if not present.
/// - If `windows` is `None`, any encountered window function is treated
///   as a misuse and results in a parse error.
///
/// # Aggregates
/// Aggregate functions are always allowed. They are collected in `aggs`.
///
/// # Returns
/// - `Ok(true)` if at least one aggregate function was found.
/// - `Ok(false)` if no aggregates were found.
/// - `Err(..)` if an invalid function usage is detected (e.g., window
///   function encountered while `windows` is `None`).
pub fn resolve_window_and_aggregate_functions(
    top_level_expr: &Expr,
    resolver: &Resolver,
    aggs: &mut Vec<Aggregate>,
    mut windows: Option<&mut Vec<Window>>,
) -> Result<bool> {
    let mut contains_aggregates = false;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinctness,
                filter_over,
                order_by,
            } => {
                if filter_over.filter_clause.is_some() {
                    crate::bail_parse_error!(
                        "FILTER clause is not supported yet in aggregate functions"
                    );
                }
                if !order_by.is_empty() {
                    crate::bail_parse_error!(
                        "ORDER BY clause is not supported yet in aggregate functions"
                    );
                }
                let args_count = args.len();
                let distinctness = Distinctness::from_ast(distinctness.as_ref());

                if !resolver.schema.indexes_enabled() && distinctness.is_distinct() {
                    crate::bail_parse_error!(
                        "SELECT with DISTINCT is not allowed without indexes enabled"
                    );
                }
                match Func::resolve_function(name.as_str(), args_count) {
                    Ok(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                expr,
                                f,
                                over_clause,
                                distinctness,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(aggs, expr, args, distinctness, f)?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Err(e) => {
                        if let Some(f) = resolver
                            .symbol_table
                            .resolve_function(name.as_str(), args_count)
                        {
                            let func = AggFunc::External(f.func.clone().into());
                            if let ExtFunc::Aggregate { .. } = f.as_ref().func {
                                if let Some(over_clause) = filter_over.over_clause.as_ref() {
                                    link_with_window(
                                        windows.as_deref_mut(),
                                        expr,
                                        func,
                                        over_clause,
                                        distinctness,
                                    )?;
                                } else {
                                    add_aggregate_if_not_exists(
                                        aggs,
                                        expr,
                                        args,
                                        distinctness,
                                        func,
                                    )?;
                                    contains_aggregates = true;
                                }
                                return Ok(WalkControl::SkipChildren);
                            }
                        } else {
                            return Err(e);
                        }
                    }
                    _ => {
                        if filter_over.over_clause.is_some() {
                            crate::bail_parse_error!(
                                "{} may not be used as a window function",
                                name.as_str()
                            );
                        }
                    }
                }
            }
            Expr::FunctionCallStar { name, filter_over } => {
                if filter_over.filter_clause.is_some() {
                    crate::bail_parse_error!(
                        "FILTER clause is not supported yet in aggregate functions"
                    );
                }
                match Func::resolve_function(name.as_str(), 0) {
                    Ok(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                expr,
                                f,
                                over_clause,
                                Distinctness::NonDistinct,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(
                                aggs,
                                expr,
                                &[],
                                Distinctness::NonDistinct,
                                f,
                            )?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Ok(_) => {
                        if filter_over.over_clause.is_some() {
                            crate::bail_parse_error!(
                                "{} may not be used as a window function",
                                name.as_str()
                            );
                        }
                        crate::bail_parse_error!("Invalid aggregate function: {}", name.as_str());
                    }
                    Err(e) => match e {
                        crate::LimboError::ParseError(e) => {
                            crate::bail_parse_error!("{}", e);
                        }
                        _ => {
                            crate::bail_parse_error!(
                                "Invalid aggregate function: {}",
                                name.as_str()
                            );
                        }
                    },
                }
            }
            _ => {}
        }

        Ok(WalkControl::Continue)
    })?;

    Ok(contains_aggregates)
}

fn link_with_window(
    windows: Option<&mut Vec<Window>>,
    expr: &Expr,
    func: AggFunc,
    over_clause: &Over,
    distinctness: Distinctness,
) -> Result<()> {
    if distinctness.is_distinct() {
        crate::bail_parse_error!("DISTINCT is not supported for window functions");
    }
    if let Some(windows) = windows {
        let window = resolve_window(windows, over_clause)?;
        window.functions.push(WindowFunction {
            func,
            original_expr: expr.clone(),
        });
    } else {
        crate::bail_parse_error!("misuse of window function: {}()", func.to_string());
    }
    Ok(())
}

fn resolve_window<'a>(windows: &'a mut Vec<Window>, over_clause: &Over) -> Result<&'a mut Window> {
    match over_clause {
        Over::Window(window) => {
            if let Some(idx) = windows.iter().position(|w| w.is_equivalent(window)) {
                return Ok(&mut windows[idx]);
            }

            windows.push(Window::new(None, window)?);
            Ok(windows.last_mut().expect("just pushed, so must exist"))
        }
        Over::Name(name) => {
            let window_name = normalize_ident(name.as_str());
            // When multiple windows share the same name, SQLite uses the most recent
            // definition. Iterate in reverse so we find the last definition first.
            for window in windows.iter_mut().rev() {
                if window.name.as_ref() == Some(&window_name) {
                    return Ok(window);
                }
            }
            crate::bail_parse_error!("no such window: {}", window_name);
        }
    }
}

fn add_aggregate_if_not_exists(
    aggs: &mut Vec<Aggregate>,
    expr: &Expr,
    args: &[Box<Expr>],
    distinctness: Distinctness,
    func: AggFunc,
) -> Result<()> {
    if distinctness.is_distinct() && args.len() != 1 {
        crate::bail_parse_error!("DISTINCT aggregate functions must have exactly one argument");
    }
    if aggs
        .iter()
        .all(|a| !exprs_are_equivalent(&a.original_expr, expr))
    {
        aggs.push(Aggregate::new(func, args, expr, distinctness));
    }
    Ok(())
}

fn parse_from_clause_table(
    table: ast::SelectTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    vtab_predicates: &mut Vec<Expr>,
    ctes: &mut Vec<JoinedTable>,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => parse_table(
            table_references,
            resolver,
            program,
            ctes,
            vtab_predicates,
            &qualified_name,
            maybe_alias.as_ref(),
            &[],
            connection,
        ),
        ast::SelectTable::Select(subselect, maybe_alias) => {
            let Plan::Select(subplan) = prepare_select_plan(
                subselect,
                resolver,
                program,
                table_references.outer_query_refs(),
                QueryDestination::placeholder_for_subquery(),
                connection,
            )?
            else {
                crate::bail_parse_error!("Only non-compound SELECT queries are currently supported in FROM clause subqueries");
            };
            let cur_table_index = table_references.joined_tables().len();
            let identifier = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id,
                    ast::As::Elided(id) => id,
                })
                .map(|id| normalize_ident(id.as_str()))
                .unwrap_or(format!("subquery_{cur_table_index}"));
            table_references.add_joined_table(JoinedTable::new_subquery(
                identifier,
                subplan,
                None,
                program.table_reference_counter.next(),
            ));
            Ok(())
        }
        ast::SelectTable::TableCall(qualified_name, args, maybe_alias) => parse_table(
            table_references,
            resolver,
            program,
            ctes,
            vtab_predicates,
            &qualified_name,
            maybe_alias.as_ref(),
            &args,
            connection,
        ),
        _ => todo!(),
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_table(
    table_references: &mut TableReferences,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    ctes: &mut Vec<JoinedTable>,
    vtab_predicates: &mut Vec<Expr>,
    qualified_name: &QualifiedName,
    maybe_alias: Option<&As>,
    args: &[Box<Expr>],
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let normalized_qualified_name = normalize_ident(qualified_name.name.as_str());
    let database_id = connection.resolve_database_id(qualified_name)?;
    let table_name = &qualified_name.name;

    // Check if the FROM clause table is referring to a CTE in the current scope.
    if let Some(cte_idx) = ctes
        .iter()
        .position(|cte| cte.identifier == normalized_qualified_name)
    {
        // TODO: what if the CTE is referenced multiple times?
        let mut cte_table = ctes.remove(cte_idx);

        // If there's an alias provided, update the identifier to use that alias
        if let Some(a) = maybe_alias {
            let alias = match a {
                ast::As::As(id) => id,
                ast::As::Elided(id) => id,
            };
            cte_table.identifier = normalize_ident(alias.as_str());
        }

        table_references.add_joined_table(cte_table);
        return Ok(());
    };

    // Resolve table using connection's with_schema method
    let table = connection.with_schema(database_id, |schema| schema.get_table(table_name.as_str()));

    if let Some(table) = table {
        let alias = maybe_alias
            .map(|a| match a {
                ast::As::As(id) => id,
                ast::As::Elided(id) => id,
            })
            .map(|a| normalize_ident(a.as_str()));
        let internal_id = program.table_reference_counter.next();
        let tbl_ref = if let Table::Virtual(tbl) = table.as_ref() {
            transform_args_into_where_terms(args, internal_id, vtab_predicates, table.as_ref())?;
            Table::Virtual(tbl.clone())
        } else if let Table::BTree(table) = table.as_ref() {
            Table::BTree(table.clone())
        } else {
            return Err(crate::LimboError::InvalidArgument(
                "Table type not supported".to_string(),
            ));
        };
        table_references.add_joined_table(JoinedTable {
            op: Operation::default_scan_for(&tbl_ref),
            table: tbl_ref,
            identifier: alias.unwrap_or(normalized_qualified_name),
            internal_id,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            database_id,
        });
        return Ok(());
    };

    let regular_view = connection.with_schema(database_id, |schema| {
        schema.get_view(table_name.as_str()).cloned()
    });
    if let Some(view) = regular_view {
        // Views are essentially query aliases, so just Expand the view as a subquery
        let view_select = view.select_stmt.clone();
        let subselect = Box::new(view_select);

        // Use the view name as alias if no explicit alias was provided
        let view_alias = maybe_alias
            .cloned()
            .or_else(|| Some(ast::As::As(table_name.clone())));

        // Recursively call parse_from_clause_table with the view as a SELECT
        return parse_from_clause_table(
            ast::SelectTable::Select(*subselect.clone(), view_alias),
            resolver,
            program,
            table_references,
            vtab_predicates,
            ctes,
            connection,
        );
    }

    let view = connection.with_schema(database_id, |schema| {
        schema.get_materialized_view(table_name.as_str())
    });
    if let Some(view) = view {
        // First check if the DBSP state table exists with the correct version
        let has_compatible_state = connection.with_schema(database_id, |schema| {
            schema.has_compatible_dbsp_state_table(table_name.as_str())
        });

        if !has_compatible_state {
            use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
            return Err(crate::LimboError::InternalError(format!(
                "Materialized view '{table_name}' has an incompatible version. \n\
                 The current version is {DBSP_CIRCUIT_VERSION}, but the view was created with a different version. \n\
                 Please DROP and recreate the view to use it."
            )));
        }

        // Check if this materialized view has persistent storage
        let view_guard = view.lock().unwrap();
        let root_page = view_guard.get_root_page();

        if root_page == 0 {
            drop(view_guard);
            return Err(crate::LimboError::InternalError(
                "Materialized view has no storage allocated".to_string(),
            ));
        }

        // This is a materialized view with storage - treat it as a regular BTree table
        // Create a BTreeTable from the view's metadata
        let btree_table = Arc::new(crate::schema::BTreeTable {
            name: view_guard.name().to_string(),
            root_page,
            columns: view_guard.column_schema.flat_columns(),
            primary_key_columns: Vec::new(),
            has_rowid: true,
            is_strict: false,
            has_autoincrement: false,

            unique_sets: vec![],
        });
        drop(view_guard);

        let alias = maybe_alias
            .map(|a| match a {
                ast::As::As(id) => id,
                ast::As::Elided(id) => id,
            })
            .map(|a| normalize_ident(a.as_str()));

        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            table: Table::BTree(btree_table),
            identifier: alias.unwrap_or(normalized_qualified_name),
            internal_id: program.table_reference_counter.next(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            database_id,
        });
        return Ok(());
    }

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
                op: Operation::default_scan_for(&outer_ref.table),
                table: outer_ref.table.clone(),
                identifier: outer_ref.identifier.clone(),
                internal_id: program.table_reference_counter.next(),
                join_info: None,
                col_used_mask: ColumnUsedMask::default(),
                database_id,
            });
            return Ok(());
        }
    }

    // Check if this is an incompatible view
    let is_incompatible = connection.with_schema(database_id, |schema| {
        schema
            .incompatible_views
            .contains(&normalized_qualified_name)
    });

    if is_incompatible {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Materialized view '{}' has an incompatible version. \n\
             The view was created with a different DBSP version than the current version ({}). \n\
             Please DROP and recreate the view to use it.",
            normalized_qualified_name,
            DBSP_CIRCUIT_VERSION
        );
    }

    crate::bail_parse_error!("no such table: {}", normalized_qualified_name);
}

fn transform_args_into_where_terms(
    args: &[Box<Expr>],
    internal_id: TableInternalId,
    predicates: &mut Vec<Expr>,
    table: &Table,
) -> Result<()> {
    let mut args_iter = args.iter();
    let mut hidden_count = 0;
    for (i, col) in table.columns().iter().enumerate() {
        if !col.hidden {
            continue;
        }
        hidden_count += 1;

        if let Some(arg_expr) = args_iter.next() {
            let column_expr = Expr::Column {
                database: None,
                table: internal_id,
                column: i,
                is_rowid_alias: col.is_rowid_alias,
            };
            let expr = match arg_expr.as_ref() {
                Expr::Literal(Null) => Expr::IsNull(Box::new(column_expr)),
                other => Expr::Binary(
                    column_expr.into(),
                    ast::Operator::Equals,
                    other.clone().into(),
                ),
            };
            predicates.push(expr);
        }
    }

    if args_iter.next().is_some() {
        return Err(crate::LimboError::ParseError(format!(
            "Too many arguments for {}: expected at most {}, got {}",
            table.get_name(),
            hidden_count,
            hidden_count + 1 + args_iter.count()
        )));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn parse_from(
    mut from: Option<FromClause>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    with: Option<With>,
    out_where_clause: &mut Vec<WhereTerm>,
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    if from.is_none() {
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
            if !cte.columns.is_empty() {
                crate::bail_parse_error!("CTE columns are not yet supported");
            }

            // Check if normalized name conflicts with catalog tables or other CTEs
            // TODO: sqlite actually allows overriding a catalog table with a CTE.
            // We should carry over the 'Scope' struct to all of our identifier resolution.
            let cte_name_normalized = normalize_ident(cte.tbl_name.as_str());
            if resolver.schema.get_table(&cte_name_normalized).is_some() {
                crate::bail_parse_error!(
                    "CTE name {} conflicts with catalog table name",
                    cte.tbl_name.as_str()
                );
            }
            if table_references
                .outer_query_refs()
                .iter()
                .any(|t| t.identifier == cte_name_normalized)
            {
                crate::bail_parse_error!(
                    "CTE name {} conflicts with WITH table name {}",
                    cte.tbl_name.as_str(),
                    cte_name_normalized
                );
            }

            let mut outer_query_refs_for_cte = table_references.outer_query_refs().to_vec();
            outer_query_refs_for_cte.extend(ctes_as_subqueries.iter().map(|t: &JoinedTable| {
                OuterQueryReference {
                    identifier: t.identifier.clone(),
                    internal_id: t.internal_id,
                    table: t.table.clone(),
                    col_used_mask: ColumnUsedMask::default(),
                }
            }));

            // CTE can refer to other CTEs that came before it, plus any schema tables or tables in the outer scope.
            let cte_plan = prepare_select_plan(
                cte.select,
                resolver,
                program,
                &outer_query_refs_for_cte,
                QueryDestination::placeholder_for_subquery(),
                connection,
            )?;
            let Plan::Select(cte_plan) = cte_plan else {
                crate::bail_parse_error!("Only SELECT queries are currently supported in CTEs");
            };
            ctes_as_subqueries.push(JoinedTable::new_subquery(
                cte_name_normalized,
                cte_plan,
                None,
                program.table_reference_counter.next(),
            ));
        }
    }

    let from_owned = std::mem::take(&mut from).unwrap();
    let select_owned = from_owned.select;
    let joins_owned = from_owned.joins;
    parse_from_clause_table(
        *select_owned,
        resolver,
        program,
        table_references,
        vtab_predicates,
        &mut ctes_as_subqueries,
        connection,
    )?;

    for join in joins_owned.into_iter() {
        parse_join(
            join,
            resolver,
            program,
            &mut ctes_as_subqueries,
            out_where_clause,
            vtab_predicates,
            table_references,
            connection,
        )?;
    }

    Ok(())
}

pub fn parse_where(
    where_clause: Option<&Expr>,
    table_references: &mut TableReferences,
    result_columns: Option<&[ResultSetColumn]>,
    out_where_clause: &mut Vec<WhereTerm>,
    connection: &Arc<crate::Connection>,
    param_ctx: &mut ParamState,
) -> Result<()> {
    if let Some(where_expr) = where_clause {
        let start_idx = out_where_clause.len();
        break_predicate_at_and_boundaries(where_expr, out_where_clause);
        for expr in out_where_clause[start_idx..].iter_mut() {
            bind_and_rewrite_expr(
                &mut expr.expr,
                Some(table_references),
                result_columns,
                connection,
                param_ctx,
                BindingBehavior::TryCanonicalColumnsFirst,
            )?;
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

    determine_where_to_eval_expr(&term.expr, join_order)
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
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
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
        Ok(WalkControl::Continue)
    })?;

    Ok(mask)
}

pub fn determine_where_to_eval_expr(
    top_level_expr: &Expr,
    join_order: &[JoinOrderMember],
) -> Result<EvalAt> {
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
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
        Ok(WalkControl::Continue)
    })?;

    Ok(eval_at)
}

#[allow(clippy::too_many_arguments)]
fn parse_join(
    join: ast::JoinedSelectTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    ctes: &mut Vec<JoinedTable>,
    out_where_clause: &mut Vec<WhereTerm>,
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let ast::JoinedSelectTable {
        operator: join_operator,
        table,
        constraint,
    } = join;

    parse_from_clause_table(
        table.as_ref().clone(),
        resolver,
        program,
        table_references,
        vtab_predicates,
        ctes,
        connection,
    )?;

    let (outer, natural) = match join_operator {
        ast::JoinOperator::TypedJoin(Some(join_type)) => {
            if join_type.contains(JoinType::RIGHT) {
                crate::bail_parse_error!("RIGHT JOIN is not supported");
            }
            if join_type.contains(JoinType::CROSS) {
                crate::bail_parse_error!("CROSS JOIN is not supported");
            }
            let is_outer = join_type.contains(JoinType::OUTER);
            let is_natural = join_type.contains(JoinType::NATURAL);
            (is_outer, is_natural)
        }
        _ => (false, false),
    };

    if natural && constraint.is_some() {
        crate::bail_parse_error!("NATURAL JOIN cannot be combined with ON or USING clause");
    }

    let constraint = if natural {
        assert!(table_references.joined_tables().len() >= 2);
        let rightmost_table = table_references.joined_tables().last().unwrap();
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let mut distinct_names: Vec<ast::Name> = vec![];
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        // SQLite doesn't use HIDDEN columns for NATURAL joins: https://www3.sqlite.org/src/info/ab09ef427181130b
        for right_col in rightmost_table.columns().iter().filter(|col| !col.hidden) {
            let mut found_match = false;
            for left_table in table_references
                .joined_tables()
                .iter()
                .take(table_references.joined_tables().len() - 1)
            {
                for left_col in left_table.columns().iter().filter(|col| !col.hidden) {
                    if left_col.name == right_col.name {
                        distinct_names.push(ast::Name::exact(
                            left_col.name.clone().expect("column name is None"),
                        ));
                        found_match = true;
                        break;
                    }
                }
                if found_match {
                    break;
                }
            }
        }
        if distinct_names.is_empty() {
            crate::bail_parse_error!("No columns found to NATURAL join on");
        } else {
            Some(ast::JoinConstraint::Using(distinct_names))
        }
    } else {
        constraint
    };

    let mut using = vec![];

    if let Some(constraint) = constraint {
        match constraint {
            ast::JoinConstraint::On(ref expr) => {
                let start_idx = out_where_clause.len();
                break_predicate_at_and_boundaries(expr, out_where_clause);
                for predicate in out_where_clause[start_idx..].iter_mut() {
                    predicate.from_outer_join = if outer {
                        Some(table_references.joined_tables().last().unwrap().internal_id)
                    } else {
                        None
                    };
                    bind_and_rewrite_expr(
                        &mut predicate.expr,
                        Some(table_references),
                        None,
                        connection,
                        &mut program.param_ctx,
                        BindingBehavior::TryResultColumnsFirst,
                    )?;
                }
            }
            ast::JoinConstraint::Using(distinct_names) => {
                // USING join is replaced with a list of equality predicates
                for distinct_name in distinct_names.iter() {
                    let name_normalized = normalize_ident(distinct_name.as_str());
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
                            .filter(|(_, col)| !natural || !col.hidden)
                            .find(|(_, col)| {
                                col.name
                                    .as_ref()
                                    .is_some_and(|name| *name == name_normalized)
                            })
                            .map(|(idx, col)| (left_table_idx, left_table.internal_id, idx, col));
                        if left_col.is_some() {
                            break;
                        }
                    }
                    if left_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.as_str()
                        );
                    }
                    let right_col = right_table.columns().iter().enumerate().find(|(_, col)| {
                        col.name
                            .as_ref()
                            .is_some_and(|name| *name == name_normalized)
                    });
                    if right_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.as_str()
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
                        consumed: false,
                    });
                }
                using = distinct_names;
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

pub fn break_predicate_at_and_boundaries<T: From<Expr>>(
    predicate: &Expr,
    out_predicates: &mut Vec<T>,
) {
    match predicate {
        Expr::Binary(left, ast::Operator::And, right) => {
            break_predicate_at_and_boundaries(left, out_predicates);
            break_predicate_at_and_boundaries(right, out_predicates);
        }
        _ => {
            out_predicates.push(predicate.clone().into());
        }
    }
}

pub fn parse_row_id<F>(
    column_name: &str,
    table_id: TableInternalId,
    fn_check: F,
) -> Result<Option<Expr>>
where
    F: FnOnce() -> bool,
{
    if ROWID_STRS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(column_name))
    {
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

#[allow(clippy::type_complexity)]
pub fn parse_limit(
    limit: &mut Limit,
    connection: &std::sync::Arc<crate::Connection>,
    param_ctx: &mut ParamState,
) -> Result<(Option<Box<Expr>>, Option<Box<Expr>>)> {
    bind_and_rewrite_expr(
        &mut limit.expr,
        None,
        None,
        connection,
        param_ctx,
        BindingBehavior::TryResultColumnsFirst,
    )?;
    if let Some(ref mut off_expr) = limit.offset {
        bind_and_rewrite_expr(
            off_expr,
            None,
            None,
            connection,
            param_ctx,
            BindingBehavior::TryResultColumnsFirst,
        )?;
    }
    Ok((Some(limit.expr.clone()), limit.offset.clone()))
}
