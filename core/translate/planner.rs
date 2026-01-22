use crate::sync::Arc;
use std::cmp::PartialEq;

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
    plan::{NonFromClauseSubquery, SubqueryState},
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
    translate::expr::bind_and_rewrite_expr,
};
use crate::{
    translate::plan::{Window, WindowFunction},
    vdbe::builder::ProgramBuilder,
};
use smallvec::SmallVec;
use turso_parser::ast::Literal::Null;
use turso_parser::ast::{
    self, As, Expr, FromClause, JoinType, Materialized, Over, QualifiedName, Select,
    TableInternalId, With,
};

/// A CTE definition stored for deferred planning.
/// Instead of planning CTEs once and cloning the result, we store the AST and
/// re-plan each time the CTE is referenced. This ensures each reference gets
/// truly unique internal_ids and cursor IDs.
struct CteDefinition {
    /// Normalized CTE name
    name: String,
    /// The original AST SELECT statement (cloned for each reference)
    select: Select,
    /// Explicit column names from WITH t(a, b) AS (...) syntax
    explicit_columns: Vec<String>,
    /// Indexes of CTEs that this CTE directly references.
    /// Only includes CTEs that appear in this CTE's FROM clause,
    /// avoiding exponential re-planning when CTEs have transitive dependencies.
    referenced_cte_indices: SmallVec<[usize; 2]>,
}

/// Collect all table names referenced in a SELECT's FROM clause.
/// Used to determine which earlier CTEs a CTE directly depends on.
fn collect_from_clause_table_refs(select: &Select, out: &mut Vec<String>) {
    collect_from_select_body(&select.body, out);
}

fn collect_from_select_body(body: &ast::SelectBody, out: &mut Vec<String>) {
    collect_from_one_select(&body.select, out);
    for compound in &body.compounds {
        collect_from_one_select(&compound.select, out);
    }
}

fn collect_from_one_select(one: &ast::OneSelect, out: &mut Vec<String>) {
    match one {
        ast::OneSelect::Select { from, .. } => {
            if let Some(from_clause) = from {
                collect_from_select_table(&from_clause.select, out);
                for join in &from_clause.joins {
                    collect_from_select_table(&join.table, out);
                }
            }
        }
        ast::OneSelect::Values(_) => {}
    }
}

fn collect_from_select_table(table: &ast::SelectTable, out: &mut Vec<String>) {
    match table {
        ast::SelectTable::Table(qualified_name, _, _)
        | ast::SelectTable::TableCall(qualified_name, _, _) => {
            out.push(normalize_ident(qualified_name.name.as_str()));
        }
        ast::SelectTable::Select(subselect, _) => {
            collect_from_clause_table_refs(subselect, out);
        }
        ast::SelectTable::Sub(from_clause, _) => {
            collect_from_select_table(&from_clause.select, out);
            for join in &from_clause.joins {
                collect_from_select_table(&join.table, out);
            }
        }
    }
}

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

                        // Check if the function supports (*) syntax using centralized logic
                        match crate::function::Func::resolve_function(name.as_str(), 0) {
                            Ok(func) => {
                                if func.supports_star_syntax() {
                                    return Ok(WalkControl::Continue);
                                } else {
                                    crate::bail_parse_error!(
                                        "wrong number of arguments to function {}()",
                                        name.as_str()
                                    );
                                }
                            }
                            Err(_) => {
                                crate::bail_parse_error!(
                                    "wrong number of arguments to function {}()",
                                    name.as_str()
                                );
                            }
                        }
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
        crate::bail_parse_error!("misuse of window function: {}()", func.as_str());
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

/// Plan a CTE when it's referenced in a query.
/// Each call produces a fresh plan with unique internal_ids, ensuring that
/// multiple references to the same CTE get independent cursor IDs,
/// yield registers and so on.
#[allow(clippy::too_many_arguments)]
fn plan_cte(
    cte_idx: usize,
    cte_definitions: &[CteDefinition],
    base_outer_query_refs: &[OuterQueryReference],
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<JoinedTable> {
    let cte_def = &cte_definitions[cte_idx];

    // Build outer_query_refs including only the CTEs this one directly references.
    // By tracking direct dependencies instead of all preceding CTEs, we avoid
    // exponential re-planning when CTEs have transitive dependencies.
    let mut outer_query_refs = base_outer_query_refs.to_vec();
    for &ref_idx in &cte_def.referenced_cte_indices {
        // Recursively plan the referenced CTE
        let referenced_table = plan_cte(
            ref_idx,
            cte_definitions,
            base_outer_query_refs,
            resolver,
            program,
            connection,
        )?;
        outer_query_refs.push(OuterQueryReference {
            identifier: referenced_table.identifier.clone(),
            internal_id: referenced_table.internal_id,
            table: referenced_table.table.clone(),
            col_used_mask: ColumnUsedMask::default(),
        });
    }

    // Plan this CTE with fresh IDs
    let cte_plan = prepare_select_plan(
        cte_def.select.clone(),
        resolver,
        program,
        &outer_query_refs,
        QueryDestination::placeholder_for_subquery(),
        connection,
    )?;

    // CTEs can be either simple SELECT or compound SELECT (UNION/INTERSECT/EXCEPT)
    let explicit_cols = if cte_def.explicit_columns.is_empty() {
        None
    } else {
        Some(cte_def.explicit_columns.as_slice())
    };
    match cte_plan {
        Plan::Select(_) | Plan::CompoundSelect { .. } => JoinedTable::new_subquery_from_plan(
            cte_def.name.clone(),
            cte_plan,
            None,
            program.table_reference_counter.next(),
            explicit_cols,
        ),
        Plan::Delete(_) | Plan::Update(_) => {
            crate::bail_parse_error!("DELETE/UPDATE queries are not supported in CTEs")
        }
    }
}

/// Plan CTEs from a WITH clause and add them as outer query references.
/// This is used by DML statements (DELETE, UPDATE) to make CTEs available
/// for subqueries in WHERE and SET clauses.
pub fn plan_ctes_as_outer_refs(
    with: Option<With>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let Some(with) = with else {
        return Ok(());
    };

    if with.recursive {
        crate::bail_parse_error!("Recursive CTEs are not yet supported");
    }

    for cte in with.ctes {
        if cte.materialized == Materialized::Yes {
            crate::bail_parse_error!("Materialized CTEs are not yet supported");
        }

        // Normalize explicit column names
        let explicit_columns: Vec<String> = cte
            .columns
            .iter()
            .map(|c| normalize_ident(c.col_name.as_str()))
            .collect();

        let cte_name = normalize_ident(cte.tbl_name.as_str());

        // Check for duplicate CTE names
        if table_references
            .outer_query_refs()
            .iter()
            .any(|r| r.identifier == cte_name)
        {
            crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
        }

        // Check if CTE name conflicts with catalog table
        if resolver.schema.get_table(&cte_name).is_some() {
            crate::bail_parse_error!(
                "CTE name {} conflicts with catalog table name",
                cte.tbl_name.as_str()
            );
        }

        // Plan the CTE SELECT
        let cte_plan = prepare_select_plan(
            cte.select,
            resolver,
            program,
            table_references.outer_query_refs(),
            QueryDestination::placeholder_for_subquery(),
            connection,
        )?;

        // Convert plan to JoinedTable to extract column info
        let explicit_cols = if explicit_columns.is_empty() {
            None
        } else {
            Some(explicit_columns.as_slice())
        };
        let joined_table = match cte_plan {
            Plan::Select(_) | Plan::CompoundSelect { .. } => JoinedTable::new_subquery_from_plan(
                cte_name.clone(),
                cte_plan,
                None,
                program.table_reference_counter.next(),
                explicit_cols,
            )?,
            Plan::Delete(_) | Plan::Update(_) => {
                crate::bail_parse_error!("Only SELECT queries are supported in CTEs")
            }
        };

        // Add CTE as outer query reference so it's available to subqueries
        table_references.add_outer_query_reference(OuterQueryReference {
            identifier: cte_name,
            internal_id: joined_table.internal_id,
            table: joined_table.table,
            col_used_mask: ColumnUsedMask::default(),
        });
    }

    Ok(())
}

fn parse_from_clause_table(
    table: ast::SelectTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    vtab_predicates: &mut Vec<Expr>,
    cte_definitions: &[CteDefinition],
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, indexed) => {
            if indexed.is_some() {
                crate::bail_parse_error!(
                    "INDEXED BY / NOT INDEXED clauses are not supported yet in FROM clause"
                );
            }
            parse_table(
                table_references,
                resolver,
                program,
                cte_definitions,
                vtab_predicates,
                &qualified_name,
                maybe_alias.as_ref(),
                &[],
                connection,
            )
        }
        ast::SelectTable::Select(subselect, maybe_alias) => {
            // For inline subqueries, we plan all CTEs once and pass them as outer_query_refs.
            // This allows the subquery to reference CTEs defined in the parent's WITH clause.
            let mut outer_query_refs_for_subquery = table_references.outer_query_refs().to_vec();
            for (idx, cte_def) in cte_definitions.iter().enumerate() {
                // Plan each CTE so it can be used by the subquery
                let cte_table = plan_cte(
                    idx,
                    cte_definitions,
                    table_references.outer_query_refs(),
                    resolver,
                    program,
                    connection,
                )?;
                outer_query_refs_for_subquery.push(OuterQueryReference {
                    identifier: cte_def.name.clone(),
                    internal_id: cte_table.internal_id,
                    table: cte_table.table,
                    col_used_mask: ColumnUsedMask::default(),
                });
            }

            let subplan = prepare_select_plan(
                subselect,
                resolver,
                program,
                &outer_query_refs_for_subquery,
                QueryDestination::placeholder_for_subquery(),
                connection,
            )?;
            match &subplan {
                Plan::Select(_) | Plan::CompoundSelect { .. } => {}
                Plan::Delete(_) | Plan::Update(_) => {
                    crate::bail_parse_error!(
                        "DELETE/UPDATE queries are not supported in FROM clause subqueries"
                    );
                }
            }
            let cur_table_index = table_references.joined_tables().len();
            let identifier = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id,
                    ast::As::Elided(id) => id,
                })
                .map(|id| normalize_ident(id.as_str()))
                .unwrap_or_else(|| format!("subquery_{cur_table_index}"));
            table_references.add_joined_table(JoinedTable::new_subquery_from_plan(
                identifier,
                subplan,
                None,
                program.table_reference_counter.next(),
                None, // No explicit columns for regular subqueries
            )?);
            Ok(())
        }
        ast::SelectTable::TableCall(qualified_name, args, maybe_alias) => parse_table(
            table_references,
            resolver,
            program,
            cte_definitions,
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
    cte_definitions: &[CteDefinition],
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
    // Each reference gets a freshly planned CTE to ensure unique internal_ids and cursor IDs.
    if let Some(cte_idx) = cte_definitions
        .iter()
        .position(|cte| cte.name == normalized_qualified_name)
    {
        let mut cte_table = plan_cte(
            cte_idx,
            cte_definitions,
            table_references.outer_query_refs(),
            resolver,
            program,
            connection,
        )?;

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
    }

    // Check if the table is a CTE from an outer scope (e.g., a CTE referencing another CTE).
    // This handles cases like: WITH a AS (...), b AS (SELECT ... FROM a) SELECT * FROM b;
    // When planning b's body, 'a' is in outer_query_refs.
    if let Some(outer_ref) =
        table_references.find_outer_query_ref_by_identifier(&normalized_qualified_name)
    {
        let alias = maybe_alias
            .map(|a| match a {
                ast::As::As(id) => id,
                ast::As::Elided(id) => id,
            })
            .map(|a| normalize_ident(a.as_str()));
        let internal_id = program.table_reference_counter.next();
        table_references.add_joined_table(JoinedTable {
            op: Operation::default_scan_for(&outer_ref.table),
            table: outer_ref.table.clone(),
            identifier: alias.unwrap_or(normalized_qualified_name),
            internal_id,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id,
        });
        return Ok(());
    }

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
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id,
        });
        return Ok(());
    };

    let regular_view =
        connection.with_schema(database_id, |schema| schema.get_view(table_name.as_str()));
    if let Some(view) = regular_view {
        // Views are essentially query aliases, so just Expand the view as a subquery
        view.process()?;
        let view_select = view.select_stmt.clone();
        let subselect = Box::new(view_select);

        // Use the view name as alias if no explicit alias was provided
        let view_alias = maybe_alias
            .cloned()
            .or_else(|| Some(ast::As::As(table_name.clone())));

        // Recursively call parse_from_clause_table with the view as a SELECT
        let result = parse_from_clause_table(
            ast::SelectTable::Select(*subselect.clone(), view_alias),
            resolver,
            program,
            table_references,
            vtab_predicates,
            cte_definitions,
            connection,
        );
        view.done();
        return result;
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
        let view_guard = view.lock();
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
            foreign_keys: vec![],
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
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
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
                column_use_counts: Vec::new(),
                expression_index_usages: Vec::new(),
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
        if !col.hidden() {
            continue;
        }
        hidden_count += 1;

        if let Some(arg_expr) = args_iter.next() {
            let column_expr = Expr::Column {
                database: None,
                table: internal_id,
                column: i,
                is_rowid_alias: col.is_rowid_alias(),
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
    from: Option<FromClause>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    with: Option<With>,
    out_where_clause: &mut Vec<WhereTerm>,
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    // Collect CTE definitions instead of planning them immediately.
    // Each CTE reference will be planned fresh when encountered, ensuring unique internal_ids.
    let mut cte_definitions: Vec<CteDefinition> = vec![];

    if let Some(with) = with {
        if with.recursive {
            crate::bail_parse_error!("Recursive CTEs are not yet supported");
        }

        for (idx, cte) in with.ctes.into_iter().enumerate() {
            if cte.materialized == Materialized::Yes {
                crate::bail_parse_error!("Materialized CTEs are not yet supported");
            }
            // Normalize explicit column names
            let explicit_columns: Vec<String> = cte
                .columns
                .iter()
                .map(|c| normalize_ident(c.col_name.as_str()))
                .collect();

            // Check if normalized name conflicts with catalog tables or other CTEs
            // TODO: sqlite actually allows overriding a catalog table with a CTE.
            // We should carry over the 'Scope' struct to all of our identifier resolution.
            let cte_name_normalized = normalize_ident(cte.tbl_name.as_str());
            if cte_definitions
                .iter()
                .any(|d| d.name == cte_name_normalized)
            {
                crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
            }

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

            // Collect table names referenced in this CTE's FROM clause.
            let mut referenced_tables = Vec::new();
            collect_from_clause_table_refs(&cte.select, &mut referenced_tables);

            // Find which preceding CTEs are directly referenced by this CTE.
            // This avoids exponential re-planning when CTEs have transitive dependencies.
            let referenced_cte_indices: SmallVec<[usize; 2]> = (0..idx)
                .filter(|&i| referenced_tables.contains(&cte_definitions[i].name))
                .collect();

            cte_definitions.push(CteDefinition {
                name: cte_name_normalized,
                select: cte.select,
                explicit_columns,
                referenced_cte_indices,
            });
        }

        // Add CTEs to outer_query_refs so they're visible to WHERE/HAVING clause subqueries.
        // Each CTE is planned here and added to outer_query_refs, then subsequent references
        // from the FROM clause can still get fresh plans via cte_definitions lookup.
        for (idx, cte_def) in cte_definitions.iter().enumerate() {
            let cte_table = plan_cte(
                idx,
                &cte_definitions,
                table_references.outer_query_refs(),
                resolver,
                program,
                connection,
            )?;
            table_references.add_outer_query_reference(OuterQueryReference {
                identifier: cte_def.name.clone(),
                internal_id: cte_table.internal_id,
                table: cte_table.table,
                col_used_mask: ColumnUsedMask::default(),
            });
        }
    }

    // Process FROM clause if present
    if let Some(from_owned) = from {
        let select_owned = from_owned.select;
        let joins_owned = from_owned.joins;
        parse_from_clause_table(
            *select_owned,
            resolver,
            program,
            table_references,
            vtab_predicates,
            &cte_definitions,
            connection,
        )?;

        for join in joins_owned.into_iter() {
            parse_join(
                join,
                resolver,
                program,
                &cte_definitions,
                out_where_clause,
                vtab_predicates,
                table_references,
                connection,
            )?;
        }
    }

    Ok(())
}

pub fn parse_where(
    where_clause: Option<&Expr>,
    table_references: &mut TableReferences,
    result_columns: Option<&[ResultSetColumn]>,
    out_where_clause: &mut Vec<WhereTerm>,
    connection: &Arc<crate::Connection>,
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
                BindingBehavior::TryCanonicalColumnsFirst,
            )?;
        }
        // BETWEEN is rewritten to (lhs >= start) AND (lhs <= end) by bind_and_rewrite_expr.
        // Re-break any ANDs that were created so they become separate WhereTerms for
        // constraint extraction.
        let mut i = start_idx;
        while i < out_where_clause.len() {
            if matches!(
                &out_where_clause[i].expr,
                Expr::Binary(_, ast::Operator::And, _)
            ) {
                let term = out_where_clause.remove(i);
                let mut new_terms: Vec<WhereTerm> = Vec::new();
                break_predicate_at_and_boundaries(&term.expr, &mut new_terms);
                // Preserve from_outer_join from the original term
                for new_term in new_terms.iter_mut() {
                    new_term.from_outer_join = term.from_outer_join;
                }
                let count = new_terms.len();
                for (j, new_term) in new_terms.into_iter().enumerate() {
                    out_where_clause.insert(i + j, new_term);
                }
                i += count;
            } else {
                i += 1;
            }
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
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    if let Some(table_id) = term.from_outer_join {
        return Ok(EvalAt::Loop(
            join_order
                .iter()
                .position(|t| t.table_id == table_id)
                .unwrap_or(usize::MAX),
        ));
    }

    determine_where_to_eval_expr(&term.expr, join_order, subqueries, table_references)
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

    /// Iterate the table indices present in this mask.
    pub fn tables_iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..127).filter(move |table_no| self.contains_table(*table_no))
    }
}

/// Returns a [TableMask] representing the tables referenced in the given expression.
///
/// This includes outer references from subqueries, even if the subquery plan has
/// already been consumed, by relying on the cached outer reference ids.
/// Used in the optimizer for constraint analysis.
pub fn table_mask_from_expr(
    top_level_expr: &Expr,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
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
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            // Hence, the tables referenced in subqueries must be added to the table mask.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Unevaluated { plan } => {
                        let used_outer_query_refs = plan
                            .as_ref()
                            .unwrap()
                            .table_references
                            .outer_query_refs()
                            .iter()
                            .filter(|t| t.is_used());
                        for outer_query_ref in used_outer_query_refs {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == outer_query_ref.internal_id)
                            {
                                mask.add_table(table_idx);
                            }
                        }
                    }
                    SubqueryState::Evaluated { outer_ref_ids, .. } => {
                        // Now hash-join plans can now translate some correlated subqueries early, we
                        // still revisit those predicates even though the plan has already been consumed.
                        // Without this cache we'd panic or lose the knowledge that an outer table was required.
                        //
                        // Example: `SELECT t.a FROM t WHERE t.a = (SELECT MAX(x.a) FROM x WHERE x.b = t.b)`.
                        // The outer expression `x.b = t.b` is visited after the subquery is translated,
                        // so we need cached `outer_ref_ids` to realize that `t` must already be in scope.
                        for outer_ref_id in outer_ref_ids {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == *outer_ref_id)
                            {
                                mask.add_table(table_idx);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(mask)
}

/// Determines the earliest loop where an expression can be safely evaluated.
///
/// When a referenced table is not found in `join_order`, we check if it's a hash-join
/// build table and map the condition to the probe loop where its rows are produced.
/// Subquery references are also respected, even after their plans are consumed.
pub fn determine_where_to_eval_expr(
    top_level_expr: &Expr,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    // If the expression references no tables, it can be evaluated before any table loops are opened.
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                let Some(join_idx) = join_order.iter().position(|t| t.table_id == *table) else {
                    // Table not found in join_order. Check if it's a hash join build table.
                    // If so, we need to evaluate the condition at the probe table's loop position.
                    if let Some(tables) = table_references {
                        for (probe_idx, member) in join_order.iter().enumerate() {
                            let probe_table = &tables.joined_tables()[member.original_idx];
                            if let Operation::HashJoin(ref hj) = probe_table.op {
                                let build_table = &tables.joined_tables()[hj.build_table_idx];
                                if build_table.internal_id == *table {
                                    // This table is the build side of a hash join.
                                    // Evaluate the condition at the probe table's loop position.
                                    eval_at = eval_at.max(EvalAt::Loop(probe_idx));
                                    return Ok(WalkControl::Continue);
                                }
                            }
                        }
                    }
                    // Must be an outer query reference; in that case, the table is already in scope.
                    return Ok(WalkControl::Continue);
                };
                eval_at = eval_at.max(EvalAt::Loop(join_idx));
            }
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Evaluated { evaluated_at, .. } => {
                        eval_at = eval_at.max(*evaluated_at);
                    }
                    SubqueryState::Unevaluated { plan } => {
                        let used_outer_refs = plan
                            .as_ref()
                            .unwrap()
                            .table_references
                            .outer_query_refs()
                            .iter()
                            .filter(|t| t.is_used());
                        for outer_ref in used_outer_refs {
                            let join_idx = join_order
                                .iter()
                                .position(|t| t.table_id == outer_ref.internal_id)
                                .or_else(|| {
                                    let tables = table_references?;
                                    for (probe_idx, member) in join_order.iter().enumerate() {
                                        let probe_table =
                                            &tables.joined_tables()[member.original_idx];
                                        if let Operation::HashJoin(ref hj) = probe_table.op {
                                            let build_table =
                                                &tables.joined_tables()[hj.build_table_idx];
                                            if build_table.internal_id == outer_ref.internal_id {
                                                return Some(probe_idx);
                                            }
                                        }
                                    }
                                    None
                                });
                            if let Some(join_idx) = join_idx {
                                eval_at = eval_at.max(EvalAt::Loop(join_idx));
                            }
                        }
                        return Ok(WalkControl::Continue);
                    }
                }
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
    cte_definitions: &[CteDefinition],
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
        cte_definitions,
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

    // this is called once for each join, so we only need to check the rightmost table
    // against all previous tables for duplicates
    let rightmost_table = table_references.joined_tables().last().unwrap();
    let has_duplicate = table_references
        .joined_tables()
        .iter()
        .take(table_references.joined_tables().len() - 1)
        .any(|t| t.identifier == rightmost_table.identifier);

    if has_duplicate
        && !natural
        && constraint
            .as_ref()
            .is_none_or(|c| !matches!(c, ast::JoinConstraint::Using(_)))
    {
        // Duplicate table names are only allowed for NATURAL or USING joins
        crate::bail_parse_error!(
            "table name {} specified more than once - use an alias to disambiguate",
            rightmost_table.identifier
        );
    }
    let constraint = if natural {
        assert!(table_references.joined_tables().len() >= 2);
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let mut distinct_names: Vec<ast::Name> = vec![];
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        // SQLite doesn't use HIDDEN columns for NATURAL joins: https://www3.sqlite.org/src/info/ab09ef427181130b
        for right_col in rightmost_table.columns().iter().filter(|col| !col.hidden()) {
            let mut found_match = false;
            for left_table in table_references
                .joined_tables()
                .iter()
                .take(table_references.joined_tables().len() - 1)
            {
                for left_col in left_table.columns().iter().filter(|col| !col.hidden()) {
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
                            .filter(|(_, col)| !natural || !col.hidden())
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
                            is_rowid_alias: left_col.is_rowid_alias(),
                        }),
                        ast::Operator::Equals,
                        Box::new(Expr::Column {
                            database: None,
                            table: right_table.internal_id,
                            column: right_col_idx,
                            is_rowid_alias: right_col.is_rowid_alias(),
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
    mut limit: Limit,
    connection: &crate::sync::Arc<crate::Connection>,
) -> Result<(Option<Box<Expr>>, Option<Box<Expr>>)> {
    bind_and_rewrite_expr(
        &mut limit.expr,
        None,
        None,
        connection,
        BindingBehavior::TryResultColumnsFirst,
    )?;
    if let Some(ref mut off_expr) = limit.offset {
        bind_and_rewrite_expr(
            off_expr,
            None,
            None,
            connection,
            BindingBehavior::TryResultColumnsFirst,
        )?;
    }
    Ok((Some(limit.expr), limit.offset))
}
