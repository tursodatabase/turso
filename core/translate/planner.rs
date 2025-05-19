use super::{
    plan::{
        Aggregate, ColumnUsedMask, EvalAt, IterationDirection, JoinInfo, JoinOrderMember,
        Operation, Plan, ResultSetColumn, SelectPlan, SelectQueryType, TableReference, WhereTerm,
    },
    select::prepare_select_plan,
    SymbolTable,
};
use crate::{
    function::Func,
    schema::{Schema, Table},
    util::{exprs_are_equivalent, normalize_ident, vtable_args},
    vdbe::BranchOffset,
    Result,
};
use limbo_sqlite3_parser::ast::{
    self, Expr, FromClause, JoinType, Limit, Materialized, UnaryOperator, With,
};

pub const ROWID: &str = "rowid";

pub fn resolve_aggregates(expr: &Expr, aggs: &mut Vec<Aggregate>) -> bool {
    if aggs
        .iter()
        .any(|a| exprs_are_equivalent(&a.original_expr, expr))
    {
        return true;
    }
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let args_count = if let Some(args) = &args {
                args.len()
            } else {
                0
            };
            match Func::resolve_function(normalize_ident(name.0.as_str()).as_str(), args_count) {
                Ok(Func::Agg(f)) => {
                    aggs.push(Aggregate {
                        func: f,
                        args: args.clone().unwrap_or_default(),
                        original_expr: expr.clone(),
                    });
                    true
                }
                _ => {
                    let mut contains_aggregates = false;
                    if let Some(args) = args {
                        for arg in args.iter() {
                            contains_aggregates |= resolve_aggregates(arg, aggs);
                        }
                    }
                    contains_aggregates
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
                });
                true
            } else {
                false
            }
        }
        Expr::Binary(lhs, _, rhs) => {
            let mut contains_aggregates = false;
            contains_aggregates |= resolve_aggregates(lhs, aggs);
            contains_aggregates |= resolve_aggregates(rhs, aggs);
            contains_aggregates
        }
        Expr::Unary(_, expr) => {
            let mut contains_aggregates = false;
            contains_aggregates |= resolve_aggregates(expr, aggs);
            contains_aggregates
        }
        // TODO: handle other expressions that may contain aggregates
        _ => false,
    }
}

pub fn bind_column_references(
    expr: &mut Expr,
    referenced_tables: &mut [TableReference],
    result_columns: Option<&[ResultSetColumn]>,
) -> Result<()> {
    match expr {
        Expr::Id(id) => {
            // true and false are special constants that are effectively aliases for 1 and 0
            // and not identifiers of columns
            if id.0.eq_ignore_ascii_case("true") || id.0.eq_ignore_ascii_case("false") {
                return Ok(());
            }
            let normalized_id = normalize_ident(id.0.as_str());

            if !referenced_tables.is_empty() {
                if let Some(row_id_expr) =
                    parse_row_id(&normalized_id, 0, || referenced_tables.len() != 1)?
                {
                    *expr = row_id_expr;

                    return Ok(());
                }
            }
            let mut match_result = None;
            for (tbl_idx, table) in referenced_tables.iter().enumerate() {
                let col_idx = table.columns().iter().position(|c| {
                    c.name
                        .as_ref()
                        .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
                });
                if col_idx.is_some() {
                    if match_result.is_some() {
                        crate::bail_parse_error!("Column {} is ambiguous", id.0);
                    }
                    let col = table.columns().get(col_idx.unwrap()).unwrap();
                    match_result = Some((tbl_idx, col_idx.unwrap(), col.is_rowid_alias));
                }
            }
            if let Some((tbl_idx, col_idx, is_rowid_alias)) = match_result {
                *expr = Expr::Column {
                    database: None, // TODO: support different databases
                    table: tbl_idx,
                    column: col_idx,
                    is_rowid_alias,
                };
                referenced_tables[tbl_idx].mark_column_used(col_idx);
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
            let matching_tbl_idx = referenced_tables
                .iter()
                .position(|t| t.identifier.eq_ignore_ascii_case(&normalized_table_name));
            if matching_tbl_idx.is_none() {
                crate::bail_parse_error!("Table {} not found", normalized_table_name);
            }
            let tbl_idx = matching_tbl_idx.unwrap();
            let normalized_id = normalize_ident(id.0.as_str());

            if let Some(row_id_expr) = parse_row_id(&normalized_id, tbl_idx, || false)? {
                *expr = row_id_expr;

                return Ok(());
            }
            let col_idx = referenced_tables[tbl_idx].columns().iter().position(|c| {
                c.name
                    .as_ref()
                    .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
            });
            if col_idx.is_none() {
                crate::bail_parse_error!("Column {} not found", normalized_id);
            }
            let col = referenced_tables[tbl_idx]
                .columns()
                .get(col_idx.unwrap())
                .unwrap();
            *expr = Expr::Column {
                database: None, // TODO: support different databases
                table: tbl_idx,
                column: col_idx.unwrap(),
                is_rowid_alias: col.is_rowid_alias,
            };
            referenced_tables[tbl_idx].mark_column_used(col_idx.unwrap());
            Ok(())
        }
        Expr::Between {
            lhs,
            not: _,
            start,
            end,
        } => {
            bind_column_references(lhs, referenced_tables, result_columns)?;
            bind_column_references(start, referenced_tables, result_columns)?;
            bind_column_references(end, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Binary(expr, _operator, expr1) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            bind_column_references(expr1, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                bind_column_references(base, referenced_tables, result_columns)?;
            }
            for (when, then) in when_then_pairs {
                bind_column_references(when, referenced_tables, result_columns)?;
                bind_column_references(then, referenced_tables, result_columns)?;
            }
            if let Some(else_expr) = else_expr {
                bind_column_references(else_expr, referenced_tables, result_columns)?;
            }
            Ok(())
        }
        Expr::Cast { expr, type_name: _ } => {
            bind_column_references(expr, referenced_tables, result_columns)
        }
        Expr::Collate(expr, _string) => {
            bind_column_references(expr, referenced_tables, result_columns)
        }
        Expr::FunctionCall {
            name: _,
            distinctness: _,
            args,
            order_by: _,
            filter_over: _,
        } => {
            if let Some(args) = args {
                for arg in args {
                    bind_column_references(arg, referenced_tables, result_columns)?;
                }
            }
            Ok(())
        }
        // Already bound earlier
        Expr::Column { .. } | Expr::RowId { .. } => Ok(()),
        Expr::DoublyQualified(_, _, _) => todo!(),
        Expr::Exists(_) => todo!(),
        Expr::FunctionCallStar { .. } => Ok(()),
        Expr::InList { lhs, not: _, rhs } => {
            bind_column_references(lhs, referenced_tables, result_columns)?;
            if let Some(rhs) = rhs {
                for arg in rhs {
                    bind_column_references(arg, referenced_tables, result_columns)?;
                }
            }
            Ok(())
        }
        Expr::InSelect { .. } => todo!(),
        Expr::InTable { .. } => todo!(),
        Expr::IsNull(expr) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Like { lhs, rhs, .. } => {
            bind_column_references(lhs, referenced_tables, result_columns)?;
            bind_column_references(rhs, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Literal(_) => Ok(()),
        Expr::Name(_) => todo!(),
        Expr::NotNull(expr) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Parenthesized(expr) => {
            for e in expr.iter_mut() {
                bind_column_references(e, referenced_tables, result_columns)?;
            }
            Ok(())
        }
        Expr::Raise(_, _) => todo!(),
        Expr::Subquery(_) => todo!(),
        Expr::Unary(_, expr) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Variable(_) => Ok(()),
    }
}

fn parse_from_clause_table<'a>(
    schema: &Schema,
    table: ast::SelectTable,
    scope: &mut Scope<'a>,
    syms: &SymbolTable,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => {
            let normalized_qualified_name = normalize_ident(qualified_name.name.0.as_str());
            // Check if the FROM clause table is referring to a CTE in the current scope.
            if let Some(cte) = scope
                .ctes
                .iter()
                .find(|cte| cte.name == normalized_qualified_name)
            {
                // CTE can be rewritten as a subquery.
                // TODO: find a way not to clone the CTE plan here.
                let cte_table =
                    TableReference::new_subquery(cte.name.clone(), cte.plan.clone(), None);
                scope.tables.push(cte_table);
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
                scope.tables.push(TableReference {
                    op: Operation::Scan {
                        iter_dir: IterationDirection::Forwards,
                        index: None,
                    },
                    table: tbl_ref,
                    identifier: alias.unwrap_or(normalized_qualified_name),
                    join_info: None,
                    col_used_mask: ColumnUsedMask::new(),
                });
                return Ok(());
            };

            // Check if the outer query scope has this table.
            if let Some(outer_scope) = scope.parent {
                if let Some(table_ref_idx) = outer_scope
                    .tables
                    .iter()
                    .position(|t| t.identifier == normalized_qualified_name)
                {
                    // TODO: avoid cloning the table reference here.
                    scope.tables.push(outer_scope.tables[table_ref_idx].clone());
                    return Ok(());
                }
                if let Some(cte) = outer_scope
                    .ctes
                    .iter()
                    .find(|cte| cte.name == normalized_qualified_name)
                {
                    // TODO: avoid cloning the CTE plan here.
                    let cte_table =
                        TableReference::new_subquery(cte.name.clone(), cte.plan.clone(), None);
                    scope.tables.push(cte_table);
                    return Ok(());
                }
            }

            crate::bail_parse_error!("Table {} not found", normalized_qualified_name);
        }
        ast::SelectTable::Select(subselect, maybe_alias) => {
            let Plan::Select(mut subplan) =
                prepare_select_plan(schema, *subselect, syms, Some(scope))?
            else {
                unreachable!();
            };
            subplan.query_type = SelectQueryType::Subquery {
                yield_reg: usize::MAX, // will be set later in bytecode emission
                coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
            };
            let cur_table_index = scope.tables.len();
            let identifier = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id.0.clone(),
                    ast::As::Elided(id) => id.0.clone(),
                })
                .unwrap_or(format!("subquery_{}", cur_table_index));
            scope
                .tables
                .push(TableReference::new_subquery(identifier, subplan, None));
            Ok(())
        }
        ast::SelectTable::TableCall(qualified_name, maybe_args, maybe_alias) => {
            let normalized_name = &normalize_ident(qualified_name.name.0.as_str());
            let args = match maybe_args {
                Some(ref args) => vtable_args(args),
                None => vec![],
            };
            let vtab = crate::VirtualTable::from_args(
                None,
                normalized_name,
                args,
                syms,
                limbo_ext::VTabKind::TableValuedFunction,
                maybe_args,
            )?;
            let alias = maybe_alias
                .as_ref()
                .map(|a| match a {
                    ast::As::As(id) => id.0.clone(),
                    ast::As::Elided(id) => id.0.clone(),
                })
                .unwrap_or(normalized_name.to_string());

            scope.tables.push(TableReference {
                op: Operation::Scan {
                    iter_dir: IterationDirection::Forwards,
                    index: None,
                },
                join_info: None,
                table: Table::Virtual(vtab),
                identifier: alias,
                col_used_mask: ColumnUsedMask::new(),
            });

            Ok(())
        }
        _ => todo!(),
    }
}

/// A scope is a list of tables that are visible to the current query.
/// It is used to resolve table references in the FROM clause.
/// To resolve table references that are potentially ambiguous, the resolution
/// first looks at schema tables and tables in the current scope (which currently just means CTEs in the current query),
/// and only after that looks at whether a table from an outer (upper) query level matches.
///
/// For example:
///
/// WITH nested AS (SELECT foo FROM bar)
/// WITH sub AS (SELECT foo FROM bar)
/// SELECT * FROM sub
///
/// 'sub' would preferentially refer to the 'foo' column from the 'bar' table in the catalog.
/// With an explicit reference like:
///
/// SELECT nested.foo FROM sub
///
/// 'nested.foo' would refer to the 'foo' column from the 'nested' CTE.
///
/// TODO: we should probably use Scope in all of our identifier resolution, because it allows for e.g.
/// WITH users AS (SELECT * FROM products) SELECT * FROM users  <-- returns products, even if there is a table named 'users' in the catalog!
///
/// Currently we are treating Schema as a first-class object in identifier resolution, when in reality
/// be part of the 'Scope' struct.
pub struct Scope<'a> {
    /// The tables that are explicitly present in the current query, including catalog tables and CTEs.
    tables: Vec<TableReference>,
    ctes: Vec<Cte>,
    /// The parent scope, if any. For example, a second CTE has access to the first CTE via the parent scope.
    parent: Option<&'a Scope<'a>>,
}

pub struct Cte {
    /// The name of the CTE.
    name: String,
    /// The query plan for the CTE.
    /// Currently we only support SELECT queries in CTEs.
    plan: SelectPlan,
}

pub fn parse_from<'a>(
    schema: &Schema,
    mut from: Option<FromClause>,
    syms: &SymbolTable,
    with: Option<With>,
    out_where_clause: &mut Vec<WhereTerm>,
    outer_scope: Option<&'a Scope<'a>>,
) -> Result<Vec<TableReference>> {
    if from.as_ref().and_then(|f| f.select.as_ref()).is_none() {
        return Ok(vec![]);
    }

    let mut scope = Scope {
        tables: vec![],
        ctes: vec![],
        parent: outer_scope,
    };

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
            if scope
                .tables
                .iter()
                .any(|t| t.identifier == cte_name_normalized)
            {
                crate::bail_parse_error!("CTE name {} conflicts with table name", cte.tbl_name.0);
            }
            if scope.ctes.iter().any(|c| c.name == cte_name_normalized) {
                crate::bail_parse_error!("duplicate WITH table name {}", cte.tbl_name.0);
            }

            // CTE can refer to other CTEs that came before it, plus any schema tables or tables in the outer scope.
            let cte_plan = prepare_select_plan(schema, *cte.select, syms, Some(&scope))?;
            let Plan::Select(mut cte_plan) = cte_plan else {
                crate::bail_parse_error!("Only SELECT queries are currently supported in CTEs");
            };
            // CTE can be rewritten as a subquery.
            cte_plan.query_type = SelectQueryType::Subquery {
                yield_reg: usize::MAX, // will be set later in bytecode emission
                coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
            };
            scope.ctes.push(Cte {
                name: cte_name_normalized,
                plan: cte_plan,
            });
        }
    }

    let mut from_owned = std::mem::take(&mut from).unwrap();
    let select_owned = *std::mem::take(&mut from_owned.select).unwrap();
    let joins_owned = std::mem::take(&mut from_owned.joins).unwrap_or_default();
    parse_from_clause_table(schema, select_owned, &mut scope, syms)?;

    for join in joins_owned.into_iter() {
        parse_join(schema, join, syms, &mut scope, out_where_clause)?;
    }

    Ok(scope.tables)
}

pub fn parse_where(
    where_clause: Option<Expr>,
    table_references: &mut [TableReference],
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
                consumed: false,
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
    if let Some(table_no) = term.from_outer_join {
        return Ok(EvalAt::Loop(
            join_order
                .iter()
                .position(|t| t.table_no == table_no)
                .unwrap_or(usize::MAX),
        ));
    }

    return determine_where_to_eval_expr(&term.expr, join_order);
}

/// A bitmask representing a set of tables in a query plan.
/// Tables are numbered by their index in [SelectPlan::table_references].
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
pub fn table_mask_from_expr(expr: &Expr) -> Result<TableMask> {
    let mut mask = TableMask::new();
    match expr {
        Expr::Binary(e1, _, e2) => {
            mask |= table_mask_from_expr(e1)?;
            mask |= table_mask_from_expr(e2)?;
        }
        Expr::Column { table, .. } | Expr::RowId { table, .. } => {
            mask.add_table(*table);
        }
        Expr::Between {
            lhs,
            not: _,
            start,
            end,
        } => {
            mask |= table_mask_from_expr(lhs)?;
            mask |= table_mask_from_expr(start)?;
            mask |= table_mask_from_expr(end)?;
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                mask |= table_mask_from_expr(base)?;
            }
            for (when, then) in when_then_pairs {
                mask |= table_mask_from_expr(when)?;
                mask |= table_mask_from_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                mask |= table_mask_from_expr(else_expr)?;
            }
        }
        Expr::Cast { expr, .. } => {
            mask |= table_mask_from_expr(expr)?;
        }
        Expr::Collate(expr, _) => {
            mask |= table_mask_from_expr(expr)?;
        }
        Expr::DoublyQualified(_, _, _) => {
            crate::bail_parse_error!(
                "DoublyQualified should be resolved to a Column before resolving table mask"
            );
        }
        Expr::Exists(_) => {
            todo!();
        }
        Expr::FunctionCall {
            args,
            order_by,
            filter_over: _,
            ..
        } => {
            if let Some(args) = args {
                for arg in args.iter() {
                    mask |= table_mask_from_expr(arg)?;
                }
            }
            if let Some(order_by) = order_by {
                for term in order_by.iter() {
                    mask |= table_mask_from_expr(&term.expr)?;
                }
            }
        }
        Expr::FunctionCallStar { .. } => {}
        Expr::Id(_) => panic!("Id should be resolved to a Column before resolving table mask"),
        Expr::InList { lhs, not: _, rhs } => {
            mask |= table_mask_from_expr(lhs)?;
            if let Some(rhs) = rhs {
                for rhs_expr in rhs.iter() {
                    mask |= table_mask_from_expr(rhs_expr)?;
                }
            }
        }
        Expr::InSelect { .. } => todo!(),
        Expr::InTable {
            lhs,
            not: _,
            rhs: _,
            args,
        } => {
            mask |= table_mask_from_expr(lhs)?;
            if let Some(args) = args {
                for arg in args.iter() {
                    mask |= table_mask_from_expr(arg)?;
                }
            }
        }
        Expr::IsNull(expr) => {
            mask |= table_mask_from_expr(expr)?;
        }
        Expr::Like {
            lhs,
            not: _,
            op: _,
            rhs,
            escape,
        } => {
            mask |= table_mask_from_expr(lhs)?;
            mask |= table_mask_from_expr(rhs)?;
            if let Some(escape) = escape {
                mask |= table_mask_from_expr(escape)?;
            }
        }
        Expr::Literal(_) => {}
        Expr::Name(_) => {}
        Expr::NotNull(expr) => {
            mask |= table_mask_from_expr(expr)?;
        }
        Expr::Parenthesized(exprs) => {
            for expr in exprs.iter() {
                mask |= table_mask_from_expr(expr)?;
            }
        }
        Expr::Qualified(_, _) => {
            panic!("Qualified should be resolved to a Column before resolving table mask");
        }
        Expr::Raise(_, _) => todo!(),
        Expr::Subquery(_) => todo!(),
        Expr::Unary(_, expr) => {
            mask |= table_mask_from_expr(expr)?;
        }
        Expr::Variable(_) => {}
    }

    Ok(mask)
}

pub fn determine_where_to_eval_expr<'a>(
    expr: &'a Expr,
    join_order: &[JoinOrderMember],
) -> Result<EvalAt> {
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    match expr {
        ast::Expr::Binary(e1, _, e2) => {
            eval_at = eval_at.max(determine_where_to_eval_expr(e1, join_order)?);
            eval_at = eval_at.max(determine_where_to_eval_expr(e2, join_order)?);
        }
        ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => {
            let join_idx = join_order
                .iter()
                .position(|t| t.table_no == *table)
                .unwrap_or(usize::MAX);
            eval_at = eval_at.max(EvalAt::Loop(join_idx));
        }
        ast::Expr::Id(_) => {
            /* Id referring to column will already have been rewritten as an Expr::Column */
            /* we only get here with literal 'true' or 'false' etc  */
        }
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before resolving eval_at")
        }
        ast::Expr::Literal(_) => {}
        ast::Expr::Like { lhs, rhs, .. } => {
            eval_at = eval_at.max(determine_where_to_eval_expr(lhs, join_order)?);
            eval_at = eval_at.max(determine_where_to_eval_expr(rhs, join_order)?);
        }
        ast::Expr::FunctionCall {
            args: Some(args), ..
        } => {
            for arg in args {
                eval_at = eval_at.max(determine_where_to_eval_expr(arg, join_order)?);
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            eval_at = eval_at.max(determine_where_to_eval_expr(lhs, join_order)?);
            if let Some(rhs_list) = rhs {
                for rhs_expr in rhs_list {
                    eval_at = eval_at.max(determine_where_to_eval_expr(rhs_expr, join_order)?);
                }
            }
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            eval_at = eval_at.max(determine_where_to_eval_expr(lhs, join_order)?);
            eval_at = eval_at.max(determine_where_to_eval_expr(start, join_order)?);
            eval_at = eval_at.max(determine_where_to_eval_expr(end, join_order)?);
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                eval_at = eval_at.max(determine_where_to_eval_expr(base, join_order)?);
            }
            for (when, then) in when_then_pairs {
                eval_at = eval_at.max(determine_where_to_eval_expr(when, join_order)?);
                eval_at = eval_at.max(determine_where_to_eval_expr(then, join_order)?);
            }
            if let Some(else_expr) = else_expr {
                eval_at = eval_at.max(determine_where_to_eval_expr(else_expr, join_order)?);
            }
        }
        Expr::Cast { expr, .. } => {
            eval_at = eval_at.max(determine_where_to_eval_expr(expr, join_order)?);
        }
        Expr::Collate(expr, _) => {
            eval_at = eval_at.max(determine_where_to_eval_expr(expr, join_order)?);
        }
        Expr::DoublyQualified(_, _, _) => {
            unreachable!("DoublyQualified should be resolved to a Column before resolving eval_at")
        }
        Expr::Exists(_) => {
            todo!("exists not supported yet")
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args.as_ref().unwrap_or(&vec![]).iter() {
                eval_at = eval_at.max(determine_where_to_eval_expr(arg, join_order)?);
            }
        }
        Expr::FunctionCallStar { .. } => {}
        Expr::InSelect { .. } => {
            todo!("in select not supported yet")
        }
        Expr::InTable { .. } => {
            todo!("in table not supported yet")
        }
        Expr::IsNull(expr) => {
            eval_at = eval_at.max(determine_where_to_eval_expr(expr, join_order)?);
        }
        Expr::Name(_) => {}
        Expr::NotNull(expr) => {
            eval_at = eval_at.max(determine_where_to_eval_expr(expr, join_order)?);
        }
        Expr::Parenthesized(exprs) => {
            for expr in exprs.iter() {
                eval_at = eval_at.max(determine_where_to_eval_expr(expr, join_order)?);
            }
        }
        Expr::Raise(_, _) => {
            todo!("raise not supported yet")
        }
        Expr::Subquery(_) => {
            todo!("subquery not supported yet")
        }
        Expr::Unary(_, expr) => {
            eval_at = eval_at.max(determine_where_to_eval_expr(expr, join_order)?);
        }
        Expr::Variable(_) => {}
    }

    Ok(eval_at)
}

fn parse_join<'a>(
    schema: &Schema,
    join: ast::JoinedSelectTable,
    syms: &SymbolTable,
    scope: &mut Scope<'a>,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    let ast::JoinedSelectTable {
        operator: join_operator,
        table,
        constraint,
    } = join;

    parse_from_clause_table(schema, table, scope, syms)?;

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
        assert!(scope.tables.len() >= 2);
        let rightmost_table = scope.tables.last().unwrap();
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let right_cols = rightmost_table.columns();
        let mut distinct_names: Option<ast::DistinctNames> = None;
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        for right_col in right_cols.iter() {
            let mut found_match = false;
            for left_table in scope.tables.iter().take(scope.tables.len() - 1) {
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
                    bind_column_references(predicate, &mut scope.tables, None)?;
                }
                for pred in preds {
                    out_where_clause.push(WhereTerm {
                        expr: pred,
                        from_outer_join: if outer {
                            Some(scope.tables.len() - 1)
                        } else {
                            None
                        },
                        consumed: false,
                    });
                }
            }
            ast::JoinConstraint::Using(distinct_names) => {
                // USING join is replaced with a list of equality predicates
                for distinct_name in distinct_names.iter() {
                    let name_normalized = normalize_ident(distinct_name.0.as_str());
                    let cur_table_idx = scope.tables.len() - 1;
                    let left_tables = &scope.tables[..cur_table_idx];
                    assert!(!left_tables.is_empty());
                    let right_table = scope.tables.last().unwrap();
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
                            .map(|(idx, col)| (left_table_idx, idx, col));
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
                    let (left_table_idx, left_col_idx, left_col) = left_col.unwrap();
                    let (right_col_idx, right_col) = right_col.unwrap();
                    let expr = Expr::Binary(
                        Box::new(Expr::Column {
                            database: None,
                            table: left_table_idx,
                            column: left_col_idx,
                            is_rowid_alias: left_col.is_rowid_alias,
                        }),
                        ast::Operator::Equals,
                        Box::new(Expr::Column {
                            database: None,
                            table: cur_table_idx,
                            column: right_col_idx,
                            is_rowid_alias: right_col.is_rowid_alias,
                        }),
                    );

                    let left_table = scope.tables.get_mut(left_table_idx).unwrap();
                    left_table.mark_column_used(left_col_idx);
                    let right_table = scope.tables.get_mut(cur_table_idx).unwrap();
                    right_table.mark_column_used(right_col_idx);
                    out_where_clause.push(WhereTerm {
                        expr,
                        from_outer_join: if outer { Some(cur_table_idx) } else { None },
                        consumed: false,
                    });
                }
                using = Some(distinct_names);
            }
        }
    }

    assert!(scope.tables.len() >= 2);
    let last_idx = scope.tables.len() - 1;
    let rightmost_table = scope.tables.get_mut(last_idx).unwrap();
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

fn parse_row_id<F>(column_name: &str, table_id: usize, fn_check: F) -> Result<Option<Expr>>
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
