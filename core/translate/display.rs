use core::fmt;
use std::fmt::{Display, Formatter};
use turso_parser::{
    ast::{
        self,
        fmt::{BlankContext, ToSqlContext, ToTokens, TokenStream},
        SortOrder,
    },
    token::TokenType,
};

use crate::{
    schema::Table,
    translate::{
        plan::{SeekKeyComponent, TableReferences},
        plan_expr::{
            PlanColumnRef, PlanExpr, PlanFrameBound, PlanMergedColumnValue, PlanOrderTerm,
            PlanSourceId, PlanSubqueryExpr, PlanWindowSpec,
        },
        semantic::hir::OutputNameKind,
    },
    types::SeekOp,
};

use super::plan::{
    Aggregate, DeletePlan, JoinedTable, Operation, Plan, Scan, Search, SeekDef, SelectPlan,
    SetOperation, UpdatePlan,
};

/// SQL rendering for a resolved plan expression.
///
/// `PlanExpr` deliberately contains stable source identities instead of parser
/// table identities. Keeping this small wrapper local to plan display lets us
/// recover user-facing table and column names without putting presentation
/// concerns back into the semantic expression type.
#[derive(Clone, Copy)]
struct PlanExprSql<'a> {
    expr: &'a PlanExpr,
    tables: &'a [&'a TableReferences],
}

impl<'a> PlanExprSql<'a> {
    fn new(expr: &'a PlanExpr, tables: &'a [&'a TableReferences]) -> Self {
        Self { expr, tables }
    }

    fn nested<'b>(&'b self, expr: &'b PlanExpr) -> PlanExprSql<'b> {
        PlanExprSql::new(expr, self.tables)
    }

    fn source(&self, source: PlanSourceId) -> Option<(&'a str, &'a Table)> {
        self.tables.iter().find_map(|tables| {
            if let Some(joined) = tables.find_joined_table_by_internal_id(source) {
                return Some((joined.identifier.as_str(), &joined.table));
            }
            tables
                .find_outer_query_ref_by_internal_id(source)
                .map(|outer| (outer.identifier.as_str(), &outer.table))
        })
    }

    fn column_name(&self, column: &PlanColumnRef) -> (String, String) {
        let Some((table_name, table)) = self.source(column.source) else {
            return (
                column.source.to_string(),
                format!("column{}", column.column),
            );
        };
        let column_name = if column.rowid_alias {
            "rowid".to_string()
        } else {
            table
                .columns()
                .get(column.column)
                .and_then(|column| column.name.as_deref())
                .map(str::to_string)
                .unwrap_or_else(|| format!("column{}", column.column))
        };
        (table_name.to_string(), column_name)
    }
}

impl Display for PlanExprSql<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.displayer(&BlankContext).fmt(f)
    }
}

impl Display for PlanExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        PlanExprSql::new(self, &[]).fmt(f)
    }
}

fn append_plan_order_terms<S: TokenStream + ?Sized>(
    s: &mut S,
    terms: &[PlanOrderTerm],
    tables: &[&TableReferences],
) -> Result<(), S::Error> {
    for (index, term) in terms.iter().enumerate() {
        if index != 0 {
            s.append(TokenType::TK_COMMA, None)?;
        }
        PlanExprSql::new(&term.expr, tables).to_tokens(s, &BlankContext)?;
        term.order.to_tokens(s, &BlankContext)?;
        if let Some(nulls) = term.nulls {
            nulls.to_tokens(s, &BlankContext)?;
        }
    }
    Ok(())
}

fn append_plan_window<S: TokenStream + ?Sized>(
    s: &mut S,
    window: &PlanWindowSpec,
    tables: &[&TableReferences],
) -> Result<(), S::Error> {
    s.append(TokenType::TK_LP, None)?;
    if !window.partition_by.is_empty() {
        s.append(TokenType::TK_PARTITION, None)?;
        s.append(TokenType::TK_BY, None)?;
        for (index, expr) in window.partition_by.iter().enumerate() {
            if index != 0 {
                s.append(TokenType::TK_COMMA, None)?;
            }
            PlanExprSql::new(expr, tables).to_tokens(s, &BlankContext)?;
        }
    }
    if !window.order_by.is_empty() {
        s.append(TokenType::TK_ORDER, None)?;
        s.append(TokenType::TK_BY, None)?;
        append_plan_order_terms(s, &window.order_by, tables)?;
    }
    if let Some(frame) = &window.frame {
        frame.mode.to_tokens(s, &BlankContext)?;
        if let Some(end) = &frame.end {
            s.append(TokenType::TK_BETWEEN, None)?;
            append_plan_frame_bound(s, &frame.start, tables)?;
            s.append(TokenType::TK_AND, None)?;
            append_plan_frame_bound(s, end, tables)?;
        } else {
            append_plan_frame_bound(s, &frame.start, tables)?;
        }
        if let Some(exclude) = &frame.exclude {
            s.append(TokenType::TK_EXCLUDE, None)?;
            exclude.to_tokens(s, &BlankContext)?;
        }
    }
    s.append(TokenType::TK_RP, None)
}

fn append_plan_frame_bound<S: TokenStream + ?Sized>(
    s: &mut S,
    bound: &PlanFrameBound,
    tables: &[&TableReferences],
) -> Result<(), S::Error> {
    match bound {
        PlanFrameBound::CurrentRow => {
            s.append(TokenType::TK_CURRENT, None)?;
            s.append(TokenType::TK_ROW, None)
        }
        PlanFrameBound::Following(expr) => {
            PlanExprSql::new(expr, tables).to_tokens(s, &BlankContext)?;
            s.append(TokenType::TK_FOLLOWING, None)
        }
        PlanFrameBound::Preceding(expr) => {
            PlanExprSql::new(expr, tables).to_tokens(s, &BlankContext)?;
            s.append(TokenType::TK_PRECEDING, None)
        }
        PlanFrameBound::UnboundedFollowing => {
            s.append(TokenType::TK_UNBOUNDED, None)?;
            s.append(TokenType::TK_FOLLOWING, None)
        }
        PlanFrameBound::UnboundedPreceding => {
            s.append(TokenType::TK_UNBOUNDED, None)?;
            s.append(TokenType::TK_PRECEDING, None)
        }
    }
}

impl ToTokens for PlanExprSql<'_> {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _context: &C,
    ) -> Result<(), S::Error> {
        match self.expr {
            PlanExpr::Literal(literal) => literal.to_tokens(s, &BlankContext),
            PlanExpr::Parameter(parameter) => {
                if let Some(name) = parameter.name.as_deref() {
                    s.append(TokenType::TK_VARIABLE, Some(name))
                } else {
                    let name = format!("?{}", parameter.index);
                    s.append(TokenType::TK_VARIABLE, Some(&name))
                }
            }
            PlanExpr::Column(column) => {
                let (table, column) = self.column_name(column);
                s.append(TokenType::TK_ID, Some(&table))?;
                s.append(TokenType::TK_DOT, None)?;
                s.append(TokenType::TK_ID, Some(&column))
            }
            PlanExpr::MergedColumn(column) => match column.value {
                PlanMergedColumnValue::Left => {
                    self.nested(&column.left).to_tokens(s, &BlankContext)
                }
                PlanMergedColumnValue::Right => {
                    let right = PlanExpr::Column(column.right.clone());
                    self.nested(&right).to_tokens(s, &BlankContext)
                }
                PlanMergedColumnValue::Coalesce => {
                    s.append(TokenType::TK_ID, Some("coalesce"))?;
                    s.append(TokenType::TK_LP, None)?;
                    self.nested(&column.left).to_tokens(s, &BlankContext)?;
                    s.append(TokenType::TK_COMMA, None)?;
                    let right = PlanExpr::Column(column.right.clone());
                    self.nested(&right).to_tokens(s, &BlankContext)?;
                    s.append(TokenType::TK_RP, None)
                }
            },
            PlanExpr::RowId(source) => {
                let table = self
                    .source(*source)
                    .map(|(name, _)| name.to_string())
                    .unwrap_or_else(|| source.to_string());
                s.append(TokenType::TK_ID, Some(&table))?;
                s.append(TokenType::TK_DOT, None)?;
                s.append(TokenType::TK_ID, Some("rowid"))
            }
            PlanExpr::Output(output) => {
                let name = format!("${output}");
                s.append(TokenType::TK_VARIABLE, Some(&name))
            }
            PlanExpr::Unary { operator, expr } => {
                operator.to_tokens(s, &BlankContext)?;
                self.nested(expr).to_tokens(s, &BlankContext)
            }
            PlanExpr::Binary {
                lhs, operator, rhs, ..
            } => {
                self.nested(lhs).to_tokens(s, &BlankContext)?;
                operator.to_tokens(s, &BlankContext)?;
                self.nested(rhs).to_tokens(s, &BlankContext)
            }
            PlanExpr::Between {
                expr,
                negated,
                start,
                end,
            } => {
                self.nested(expr).to_tokens(s, &BlankContext)?;
                if *negated {
                    s.append(TokenType::TK_NOT, None)?;
                }
                s.append(TokenType::TK_BETWEEN, None)?;
                self.nested(start).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_AND, None)?;
                self.nested(end).to_tokens(s, &BlankContext)
            }
            PlanExpr::Case {
                base,
                when_then,
                else_expr,
            } => {
                s.append(TokenType::TK_CASE, None)?;
                if let Some(base) = base {
                    self.nested(base).to_tokens(s, &BlankContext)?;
                }
                for (when, then) in when_then {
                    s.append(TokenType::TK_WHEN, None)?;
                    self.nested(when).to_tokens(s, &BlankContext)?;
                    s.append(TokenType::TK_THEN, None)?;
                    self.nested(then).to_tokens(s, &BlankContext)?;
                }
                if let Some(else_expr) = else_expr {
                    s.append(TokenType::TK_ELSE, None)?;
                    self.nested(else_expr).to_tokens(s, &BlankContext)?;
                }
                s.append(TokenType::TK_END, None)
            }
            PlanExpr::Cast { expr, target } => {
                s.append(TokenType::TK_CAST, None)?;
                s.append(TokenType::TK_LP, None)?;
                self.nested(expr).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_AS, None)?;
                s.append(TokenType::TK_ID, Some(&target.name))?;
                if !target.parameters.is_empty() {
                    s.append(TokenType::TK_LP, None)?;
                    for (index, parameter) in target.parameters.iter().enumerate() {
                        if index != 0 {
                            s.append(TokenType::TK_COMMA, None)?;
                        }
                        self.nested(parameter).to_tokens(s, &BlankContext)?;
                    }
                    s.append(TokenType::TK_RP, None)?;
                }
                for _ in 0..target.array_dimensions {
                    s.append(TokenType::TK_LBRACKET, None)?;
                    s.append(TokenType::TK_RBRACKET, None)?;
                }
                s.append(TokenType::TK_RP, None)
            }
            PlanExpr::Collate { expr, collation } => {
                self.nested(expr).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_COLLATE, None)?;
                let name = collation.value().to_string();
                s.append(TokenType::TK_ID, Some(&name))
            }
            PlanExpr::Function(call) => {
                let name = call.function.value().to_string();
                s.append(TokenType::TK_ID, Some(&name))?;
                s.append(TokenType::TK_LP, None)?;
                if let Some(distinctness) = call.distinctness {
                    distinctness.to_tokens(s, &BlankContext)?;
                }
                if call.star {
                    s.append(TokenType::TK_STAR, None)?;
                } else {
                    for (index, argument) in call.arguments.iter().enumerate() {
                        if index != 0 {
                            s.append(TokenType::TK_COMMA, None)?;
                        }
                        self.nested(argument).to_tokens(s, &BlankContext)?;
                    }
                }
                if !call.argument_order.is_empty() {
                    s.append(TokenType::TK_ORDER, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    append_plan_order_terms(s, &call.argument_order, self.tables)?;
                }
                s.append(TokenType::TK_RP, None)?;
                if !call.within_group.is_empty() {
                    s.append(TokenType::TK_WITHIN, None)?;
                    s.append(TokenType::TK_GROUP, None)?;
                    s.append(TokenType::TK_LP, None)?;
                    s.append(TokenType::TK_ORDER, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    append_plan_order_terms(s, &call.within_group, self.tables)?;
                    s.append(TokenType::TK_RP, None)?;
                }
                if let Some(filter) = &call.filter {
                    s.append(TokenType::TK_FILTER, None)?;
                    s.append(TokenType::TK_LP, None)?;
                    s.append(TokenType::TK_WHERE, None)?;
                    self.nested(filter).to_tokens(s, &BlankContext)?;
                    s.append(TokenType::TK_RP, None)?;
                }
                if let Some(window) = &call.window {
                    s.append(TokenType::TK_OVER, None)?;
                    append_plan_window(s, window, self.tables)?;
                }
                Ok(())
            }
            PlanExpr::IsNull(expr) => {
                self.nested(expr).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_ISNULL, None)
            }
            PlanExpr::NotNull(expr) => {
                self.nested(expr).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_NOTNULL, None)
            }
            PlanExpr::InList {
                lhs,
                negated,
                values,
            } => {
                self.nested(lhs).to_tokens(s, &BlankContext)?;
                if *negated {
                    s.append(TokenType::TK_NOT, None)?;
                }
                s.append(TokenType::TK_IN, None)?;
                s.append(TokenType::TK_LP, None)?;
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        s.append(TokenType::TK_COMMA, None)?;
                    }
                    self.nested(value).to_tokens(s, &BlankContext)?;
                }
                s.append(TokenType::TK_RP, None)
            }
            PlanExpr::Subquery(subquery) => match subquery {
                PlanSubqueryExpr::Scalar { query, output } => {
                    let name = format!("${query}_{output}");
                    s.append(TokenType::TK_VARIABLE, Some(&name))
                }
                PlanSubqueryExpr::Exists(query) => {
                    s.append(TokenType::TK_EXISTS, None)?;
                    s.append(TokenType::TK_LP, None)?;
                    let name = format!("${query}");
                    s.append(TokenType::TK_VARIABLE, Some(&name))?;
                    s.append(TokenType::TK_RP, None)
                }
                PlanSubqueryExpr::In {
                    lhs,
                    query,
                    negated,
                } => {
                    self.nested(lhs).to_tokens(s, &BlankContext)?;
                    if *negated {
                        s.append(TokenType::TK_NOT, None)?;
                    }
                    s.append(TokenType::TK_IN, None)?;
                    s.append(TokenType::TK_LP, None)?;
                    let name = format!("${query}");
                    s.append(TokenType::TK_VARIABLE, Some(&name))?;
                    s.append(TokenType::TK_RP, None)
                }
            },
            PlanExpr::Like {
                lhs,
                negated,
                operator,
                rhs,
                escape,
                ..
            } => {
                self.nested(lhs).to_tokens(s, &BlankContext)?;
                if *negated {
                    s.append(TokenType::TK_NOT, None)?;
                }
                operator.to_tokens(s, &BlankContext)?;
                self.nested(rhs).to_tokens(s, &BlankContext)?;
                if let Some(escape) = escape {
                    s.append(TokenType::TK_ESCAPE, None)?;
                    self.nested(escape).to_tokens(s, &BlankContext)?;
                }
                Ok(())
            }
            PlanExpr::Row(values) => {
                s.append(TokenType::TK_LP, None)?;
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        s.append(TokenType::TK_COMMA, None)?;
                    }
                    self.nested(value).to_tokens(s, &BlankContext)?;
                }
                s.append(TokenType::TK_RP, None)
            }
            PlanExpr::Array(values) => {
                s.append(TokenType::TK_ID, Some("ARRAY"))?;
                s.append(TokenType::TK_LBRACKET, None)?;
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        s.append(TokenType::TK_COMMA, None)?;
                    }
                    self.nested(value).to_tokens(s, &BlankContext)?;
                }
                s.append(TokenType::TK_RBRACKET, None)
            }
            PlanExpr::Subscript { base, index } => {
                self.nested(base).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_LBRACKET, None)?;
                self.nested(index).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_RBRACKET, None)
            }
            PlanExpr::FieldAccess(access) => {
                self.nested(&access.base).to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_DOT, None)?;
                s.append(TokenType::TK_ID, Some(&access.field_name))
            }
            PlanExpr::Raise { action, message } => {
                s.append(TokenType::TK_RAISE, None)?;
                s.append(TokenType::TK_LP, None)?;
                action.to_tokens(s, &BlankContext)?;
                if let Some(message) = message {
                    s.append(TokenType::TK_COMMA, None)?;
                    self.nested(message).to_tokens(s, &BlankContext)?;
                }
                s.append(TokenType::TK_RP, None)
            }
        }
    }
}

fn fmt_order_by_item(
    f: &mut fmt::Formatter<'_>,
    expr: &impl fmt::Display,
    dir: SortOrder,
    nulls: Option<turso_parser::ast::NullsOrder>,
) -> fmt::Result {
    let dir_str = match dir {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    match nulls {
        Some(turso_parser::ast::NullsOrder::First) => {
            writeln!(f, "  - {expr} {dir_str} NULLS FIRST")
        }
        Some(turso_parser::ast::NullsOrder::Last) => writeln!(f, "  - {expr} {dir_str} NULLS LAST"),
        None => writeln!(f, "  - {expr} {dir_str}"),
    }
}

/// Format the EXPLAIN QUERY PLAN detail string for a table operation.
/// Used by DELETE/UPDATE emitters to emit EQP annotations.
pub(crate) fn format_eqp_detail(table: &JoinedTable) -> String {
    match &table.op {
        Operation::Scan(scan) => {
            let table_name = if table.table.get_name() == table.identifier {
                table.identifier.clone()
            } else {
                format!("{} AS {}", table.table.get_name(), table.identifier)
            };
            match scan {
                Scan::BTreeTable { index, .. } => {
                    if let Some(index) = index {
                        if table.utilizes_covering_index() {
                            format!("SCAN {table_name} USING COVERING INDEX {}", index.name)
                        } else {
                            format!("SCAN {table_name} USING INDEX {}", index.name)
                        }
                    } else {
                        format!("SCAN {table_name}")
                    }
                }
                Scan::VirtualTable { .. } | Scan::Subquery { .. } | Scan::RecursiveCteInput => {
                    format!("SCAN {table_name}")
                }
            }
        }
        Operation::Search(search) => match search {
            Search::RowidEq { .. }
            | Search::Seek { index: None, .. }
            | Search::InSeek { index: None, .. } => {
                format!(
                    "SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                    table.identifier
                )
            }
            Search::Seek {
                index: Some(index),
                seek_def,
            } => {
                let constraints = seek_constraint_annotation(index, seek_def);
                format!(
                    "SEARCH {} USING INDEX {}{}",
                    table.identifier, index.name, constraints
                )
            }
            Search::InSeek {
                index: Some(index), ..
            } => {
                let constraint = if let Some(col) = index.columns.first() {
                    format!(" ({}=?)", col.name)
                } else {
                    String::new()
                };
                format!(
                    "SEARCH {} USING INDEX {}{}",
                    table.identifier, index.name, constraint
                )
            }
        },
        Operation::MultiIndexScan(multi_idx) => {
            let index_names: Vec<&str> = multi_idx
                .branches
                .iter()
                .map(|b| {
                    b.index
                        .as_ref()
                        .map(|i| i.name.as_str())
                        .unwrap_or("PRIMARY KEY")
                })
                .collect();
            format!(
                "MULTI-INDEX {} {} ({})",
                match multi_idx.set_op {
                    SetOperation::Union => "OR",
                    SetOperation::Intersection { .. } => "AND",
                },
                table.identifier,
                index_names.join(", ")
            )
        }
        Operation::IndexMethodQuery(query) => {
            let index_method = query.index.index_method.as_ref().unwrap();
            format!(
                "QUERY INDEX METHOD {}",
                index_method.definition().method_name
            )
        }
        Operation::HashJoin(_) => {
            let table_name = if table.table.get_name() == table.identifier {
                table.identifier.clone()
            } else {
                format!("{} AS {}", table.table.get_name(), table.identifier)
            };
            format!("HASH JOIN {table_name}")
        }
    }
}

/// Build SQLite-style constraint annotation string for an index seek.
/// e.g. "(label=? AND fromId>?)"
pub(crate) fn seek_constraint_annotation(
    index: &crate::schema::Index,
    seek_def: &SeekDef,
) -> String {
    let mut parts = Vec::new();
    // Equality prefix constraints
    for (i, _constraint) in seek_def.prefix.iter().enumerate() {
        if let Some(col) = index.columns.get(i) {
            parts.push(format!("{}=?", col.name));
        }
    }
    // Range constraint from start key
    let range_col_idx = seek_def.prefix.len();
    if let SeekKeyComponent::Expr(_) = &seek_def.start.last_component {
        if let Some(col) = index.columns.get(range_col_idx) {
            let op_str = match seek_def.start.op {
                SeekOp::GE { .. } => ">=",
                SeekOp::GT => ">",
                SeekOp::LE { .. } => "<=",
                SeekOp::LT => "<",
            };
            parts.push(format!("{}{op_str}?", col.name));
        }
    }
    // Range constraint from end key.
    // The end key's SeekOp is the B-tree termination condition (the negation of the
    // user-facing SQL operator), so we reverse it for display.
    if let SeekKeyComponent::Expr(_) = &seek_def.end.last_component {
        if let Some(col) = index.columns.get(range_col_idx) {
            let op_str = match seek_def.end.op {
                SeekOp::GE { .. } => "<",
                SeekOp::GT => "<=",
                SeekOp::LE { .. } => ">",
                SeekOp::LT => ">=",
            };
            parts.push(format!("{}{op_str}?", col.name));
        }
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" AND "))
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let args_str = self
            .args
            .iter()
            .map(|arg| arg.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{:?}({})", self.func, args_str)
    }
}

/// For EXPLAIN QUERY PLAN
impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(select_plan) => select_plan.fmt(f),
            Self::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                for (plan, operator) in left {
                    plan.fmt(f)?;
                    writeln!(f, "{operator}")?;
                }
                right_most.fmt(f)?;
                if let Some(limit) = limit {
                    writeln!(f, "LIMIT: {limit}")?;
                }
                if let Some(offset) = offset {
                    writeln!(f, "OFFSET: {offset}")?;
                }
                if !order_by.is_empty() {
                    writeln!(f, "ORDER BY:")?;
                    for term in order_by {
                        fmt_order_by_item(f, &term.expr, term.order, term.nulls)?;
                    }
                }
                Ok(())
            }
            Self::RecursiveCte(plan) => {
                writeln!(f, "RECURSIVE CTE {}", plan.name)?;
                writeln!(f, "INITIAL QUERY:")?;
                plan.initial_query.fmt(f)?;
                writeln!(f, "RECURSIVE QUERY:")?;
                plan.recursive_query.fmt(f)
            }
            Self::Delete(delete_plan) => delete_plan.fmt(f),
            Self::Update(update_plan) => update_plan.fmt(f),
        }
    }
}

impl Display for SelectPlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Print each table reference with appropriate indentation based on join depth
        for (i, member) in self.join_order.iter().enumerate() {
            let reference = &self.table_references.joined_tables()[member.original_idx];
            let is_last = i == self.join_order.len() - 1;
            let indent = if i == 0 {
                if is_last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(i - 1),
                    if is_last { "`--" } else { "|--" }
                )
            };

            match &reference.op {
                Operation::Scan(scan) => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    match scan {
                        Scan::BTreeTable { index, .. } => {
                            if let Some(index) = index {
                                if reference.utilizes_covering_index() {
                                    writeln!(
                                        f,
                                        "{indent}SCAN {table_name} USING COVERING INDEX {}",
                                        index.name
                                    )?;
                                } else {
                                    writeln!(
                                        f,
                                        "{indent}SCAN {table_name} USING INDEX {}",
                                        index.name
                                    )?;
                                }
                            } else {
                                writeln!(f, "{indent}SCAN {table_name}")?;
                            }
                        }
                        Scan::VirtualTable { .. }
                        | Scan::Subquery { .. }
                        | Scan::RecursiveCteInput => {
                            writeln!(f, "{indent}SCAN {table_name}")?;
                        }
                    }
                }
                Operation::Search(search) => {
                    let left_join_suffix = if member.is_outer { " LEFT-JOIN" } else { "" };
                    match search {
                        Search::RowidEq { .. }
                        | Search::Seek { index: None, .. }
                        | Search::InSeek { index: None, .. } => {
                            writeln!(
                                f,
                                "{indent}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?){left_join_suffix}",
                                reference.identifier
                            )?;
                        }
                        Search::Seek {
                            index: Some(index),
                            seek_def,
                        } => {
                            let constraints = seek_constraint_annotation(index, seek_def);
                            writeln!(
                                f,
                                "{indent}SEARCH {} USING INDEX {}{constraints}{left_join_suffix}",
                                reference.identifier, index.name
                            )?;
                        }
                        Search::InSeek {
                            index: Some(index), ..
                        } => {
                            let constraint = if let Some(col) = index.columns.first() {
                                format!(" ({}=?)", col.name)
                            } else {
                                String::new()
                            };
                            writeln!(
                                f,
                                "{indent}SEARCH {} USING INDEX {}{constraint}{left_join_suffix}",
                                reference.identifier, index.name
                            )?;
                        }
                    }
                }
                Operation::IndexMethodQuery(query) => {
                    let index_method = query.index.index_method.as_ref().unwrap();
                    writeln!(
                        f,
                        "{}QUERY INDEX METHOD {}",
                        indent,
                        index_method.definition().method_name
                    )?;
                }
                Operation::HashJoin(_) => {
                    writeln!(f, "{indent}HASH JOIN")?;
                }
                Operation::MultiIndexScan(multi_idx) => {
                    let index_names: Vec<&str> = multi_idx
                        .branches
                        .iter()
                        .map(|b| {
                            b.index
                                .as_ref()
                                .map(|i| i.name.as_str())
                                .unwrap_or("PRIMARY KEY")
                        })
                        .collect();
                    let op_name = match multi_idx.set_op {
                        SetOperation::Union => "MULTI-INDEX OR",
                        SetOperation::Intersection { .. } => "MULTI-INDEX AND",
                    };
                    writeln!(
                        f,
                        "{indent}{op_name} {} ({}) ",
                        reference.identifier,
                        index_names.join(", ")
                    )?;
                }
            }
        }
        if self.distinctness.is_distinct() {
            writeln!(f, "USE HASH TABLE FOR DISTINCT")?;
        }
        Ok(())
    }
}

impl Display for DeletePlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Delete plan should only have one table reference
        if let Some(reference) = self.table_references.joined_tables().first() {
            let indent = "`--";

            match &reference.op {
                Operation::Scan(scan) => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    match scan {
                        Scan::BTreeTable { index, .. } => {
                            if let Some(index) = index {
                                if reference.utilizes_covering_index() {
                                    writeln!(
                                        f,
                                        "{indent}DELETE FROM {table_name} USING COVERING INDEX {}",
                                        index.name
                                    )?;
                                } else {
                                    writeln!(
                                        f,
                                        "{indent}DELETE FROM {table_name} USING INDEX {}",
                                        index.name
                                    )?;
                                }
                            } else {
                                writeln!(f, "{indent}DELETE FROM {table_name}")?;
                            }
                        }
                        Scan::VirtualTable { .. }
                        | Scan::Subquery { .. }
                        | Scan::RecursiveCteInput => {
                            writeln!(f, "{indent}DELETE FROM {table_name}")?;
                        }
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. }
                    | Search::Seek { index: None, .. }
                    | Search::InSeek { index: None, .. } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                    Search::InSeek {
                        index: Some(index), ..
                    } => {
                        let constraint = if let Some(col) = index.columns.first() {
                            format!(" ({}=?)", col.name)
                        } else {
                            String::new()
                        };
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}{constraint}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
                Operation::IndexMethodQuery(query) => {
                    let module = query.index.index_method.as_ref().unwrap();
                    writeln!(
                        f,
                        "{}QUERY MODULE {}",
                        indent,
                        module.definition().method_name
                    )?;
                }
                Operation::HashJoin(_) => {
                    unreachable!("Delete plan should not have hash joins");
                }
                Operation::MultiIndexScan(multi_idx) => {
                    let index_names: Vec<&str> = multi_idx
                        .branches
                        .iter()
                        .map(|b| {
                            b.index
                                .as_ref()
                                .map(|i| i.name.as_str())
                                .unwrap_or("PRIMARY KEY")
                        })
                        .collect();
                    let op_name = match multi_idx.set_op {
                        SetOperation::Union => "MULTI-INDEX OR",
                        SetOperation::Intersection { .. } => "MULTI-INDEX AND",
                    };
                    writeln!(
                        f,
                        "{indent}{op_name} {} ({})",
                        reference.identifier,
                        index_names.join(", ")
                    )?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for UpdatePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        let read_scope_tables = self.build_read_scope_tables();

        for (i, reference) in read_scope_tables.joined_tables().iter().enumerate() {
            let is_last = i == read_scope_tables.joined_tables().len() - 1;
            let indent = if i == 0 {
                if is_last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(i - 1),
                    if is_last { "`--" } else { "|--" }
                )
            };

            match &reference.op {
                Operation::Scan(scan) => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    match scan {
                        Scan::BTreeTable { index, .. } => {
                            let action = if i == 0 { "UPDATE" } else { "SCAN" };
                            if let Some(index) = index {
                                if reference.utilizes_covering_index() {
                                    writeln!(
                                        f,
                                        "{indent}{action} {table_name} USING COVERING INDEX {}",
                                        index.name
                                    )?;
                                } else {
                                    writeln!(
                                        f,
                                        "{indent}{action} {table_name} USING INDEX {}",
                                        index.name
                                    )?;
                                }
                            } else {
                                writeln!(f, "{indent}{action} {table_name}")?;
                            }
                        }
                        Scan::VirtualTable { .. }
                        | Scan::Subquery { .. }
                        | Scan::RecursiveCteInput => {
                            if i == 0 {
                                writeln!(f, "{indent}UPDATE {table_name}")?;
                            } else {
                                writeln!(f, "{indent}SCAN {table_name}")?;
                            }
                        }
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. }
                    | Search::Seek { index: None, .. }
                    | Search::InSeek { index: None, .. } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                    Search::InSeek {
                        index: Some(index), ..
                    } => {
                        let constraint = if let Some(col) = index.columns.first() {
                            format!(" ({}=?)", col.name)
                        } else {
                            String::new()
                        };
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}{constraint}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
                Operation::IndexMethodQuery(query) => {
                    let module = query.index.index_method.as_ref().unwrap();
                    writeln!(
                        f,
                        "{}QUERY MODULE {}",
                        indent,
                        module.definition().method_name
                    )?;
                }
                Operation::HashJoin(_) => {
                    unreachable!("Update plan should not have hash joins");
                }
                Operation::MultiIndexScan(_) => {
                    unreachable!("Update plan should not have multi-index scans");
                }
            }
        }
        if let Some(limit) = self.limit.as_ref() {
            writeln!(f, "LIMIT: {limit}")?;
        }
        if let Some(ret) = &self.returning {
            writeln!(f, "RETURNING:")?;
            for col in ret {
                writeln!(f, "  - {}", col.expr)?;
            }
        }

        Ok(())
    }
}

pub struct PlanContext<'a>(pub &'a [&'a TableReferences]);

impl ToSqlContext for PlanContext<'_> {}

impl ToTokens for Plan {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Select(select) => {
                select.to_tokens(s, &PlanContext(&[&select.table_references]))?;
            }
            Self::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                let all_refs = left
                    .iter()
                    .flat_map(|(plan, _)| std::iter::once(&plan.table_references))
                    .chain(std::iter::once(&right_most.table_references))
                    .collect::<Vec<_>>();
                let context = &PlanContext(all_refs.as_slice());

                for (plan, operator) in left {
                    plan.to_tokens(s, context)?;
                    operator.to_tokens(s, context)?;
                }

                right_most.to_tokens(s, context)?;

                if !order_by.is_empty() {
                    s.append(TokenType::TK_ORDER, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    append_plan_order_terms(s, order_by, all_refs.as_slice())?;
                }

                if let Some(limit) = &limit {
                    s.append(TokenType::TK_LIMIT, None)?;
                    PlanExprSql::new(limit, all_refs.as_slice()).to_tokens(s, &BlankContext)?;
                }

                if let Some(offset) = &offset {
                    s.append(TokenType::TK_OFFSET, None)?;
                    PlanExprSql::new(offset, all_refs.as_slice()).to_tokens(s, &BlankContext)?;
                }
            }
            Self::RecursiveCte(plan) => {
                plan.initial_query.to_tokens(s, context)?;
                if plan.union_all {
                    ast::CompoundOperator::UnionAll.to_tokens(s, context)?;
                } else {
                    ast::CompoundOperator::Union.to_tokens(s, context)?;
                }
                plan.recursive_query.to_tokens(s, context)?;
            }
            Self::Delete(delete) => delete.to_tokens(s, context)?,
            Self::Update(update) => update.to_tokens(s, context)?,
        }

        Ok(())
    }
}

impl Display for JoinedTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.displayer(&BlankContext).fmt(f)
    }
}

impl ToTokens for JoinedTable {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _context: &C,
    ) -> Result<(), S::Error> {
        match &self.table {
            Table::BTree(..) | Table::Virtual(..) | Table::RecursiveCteInput(..) => {
                let name = self.table.get_name();
                s.append(TokenType::TK_ID, Some(name))?;
                if self.identifier != name {
                    s.append(TokenType::TK_AS, None)?;
                    s.append(TokenType::TK_ID, Some(&self.identifier))?;
                }
            }
            Table::FromClauseSubquery(from_clause_subquery) => {
                s.append(TokenType::TK_LP, None)?;
                // Plan::to_tokens creates its own context internally, so we pass BlankContext here.
                from_clause_subquery.plan.to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_RP, None)?;

                s.append(TokenType::TK_AS, None)?;
                s.append(TokenType::TK_ID, Some(&self.identifier))?;
            }
        };

        Ok(())
    }
}

// TODO: currently cannot print the original CTE as it is optimized into a subquery
impl ToTokens for SelectPlan {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        let expr_tables = [&self.table_references];
        if !self.values.is_empty() {
            s.append(TokenType::TK_VALUES, None)?;
            for (row_index, row) in self.values.iter().enumerate() {
                if row_index != 0 {
                    s.append(TokenType::TK_COMMA, None)?;
                }
                s.append(TokenType::TK_LP, None)?;
                for (column_index, expr) in row.iter().enumerate() {
                    if column_index != 0 {
                        s.append(TokenType::TK_COMMA, None)?;
                    }
                    PlanExprSql::new(expr, &expr_tables).to_tokens(s, &BlankContext)?;
                }
                s.append(TokenType::TK_RP, None)?;
            }
        } else {
            s.append(TokenType::TK_SELECT, None)?;
            if self.distinctness.is_distinct() {
                s.append(TokenType::TK_DISTINCT, None)?;
            }

            for (i, column) in self.result_columns.iter().enumerate() {
                if i != 0 {
                    s.append(TokenType::TK_COMMA, None)?;
                }

                PlanExprSql::new(&column.expr, &expr_tables).to_tokens(s, &BlankContext)?;
                if column.name_kind == OutputNameKind::ExplicitAlias {
                    s.append(TokenType::TK_AS, None)?;
                    s.append(TokenType::TK_ID, Some(&column.name))?;
                }
            }

            if !self.join_order.is_empty() {
                s.append(TokenType::TK_FROM, None)?;
                for (i, order) in self.join_order.iter().enumerate() {
                    if i != 0 {
                        if order.is_outer {
                            s.append(TokenType::TK_ORDER, None)?;
                        }
                        s.append(TokenType::TK_JOIN, None)?;
                    }

                    let table_ref = self.joined_tables().get(order.original_idx).unwrap();
                    table_ref.to_tokens(s, context)?;
                }
            }

            if !self.where_clause.is_empty() {
                s.append(TokenType::TK_WHERE, None)?;

                for (i, expr) in self
                    .where_clause
                    .iter()
                    .map(|where_clause| &where_clause.expr)
                    .enumerate()
                {
                    if i != 0 {
                        s.append(TokenType::TK_AND, None)?;
                    }
                    PlanExprSql::new(expr, &expr_tables).to_tokens(s, &BlankContext)?;
                }
            }

            if let Some(group_by) = &self.group_by {
                if !group_by.exprs.is_empty() {
                    s.append(TokenType::TK_GROUP, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    for (index, expr) in group_by.exprs.iter().enumerate() {
                        if index != 0 {
                            s.append(TokenType::TK_COMMA, None)?;
                        }
                        PlanExprSql::new(expr, &expr_tables).to_tokens(s, &BlankContext)?;
                    }
                }

                // TODO: not sure where I need to place the group_by.sort_order
                if let Some(having) = &group_by.having {
                    s.append(TokenType::TK_HAVING, None)?;

                    for (i, expr) in having.iter().enumerate() {
                        if i != 0 {
                            s.append(TokenType::TK_AND, None)?;
                        }
                        PlanExprSql::new(expr, &expr_tables).to_tokens(s, &BlankContext)?;
                    }
                }
            }
        }

        if let Some(window) = &self.window {
            if let Some(window_name) = &window.name {
                s.append(TokenType::TK_WINDOW, None)?;
                s.append(TokenType::TK_ID, Some(window_name))?;
                s.append(TokenType::TK_AS, None)?;

                s.append(TokenType::TK_LP, None)?;

                if !window.partition_by.is_empty() {
                    s.append(TokenType::TK_PARTITION, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    for (index, expr) in window.partition_by.iter().enumerate() {
                        if index != 0 {
                            s.append(TokenType::TK_COMMA, None)?;
                        }
                        PlanExprSql::new(expr, &expr_tables).to_tokens(s, &BlankContext)?;
                    }
                }

                if !window.order_by.is_empty() {
                    s.append(TokenType::TK_ORDER, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    for (index, (expr, order, nulls)) in window.order_by.iter().enumerate() {
                        if index != 0 {
                            s.append(TokenType::TK_COMMA, None)?;
                        }
                        PlanExprSql::new(expr, &expr_tables).to_tokens(s, &BlankContext)?;
                        order.to_tokens(s, &BlankContext)?;
                        if let Some(nulls) = nulls {
                            nulls.to_tokens(s, &BlankContext)?;
                        }
                    }
                }

                s.append(TokenType::TK_RP, None)?;
            }
        }

        if !self.order_by.is_empty() {
            s.append(TokenType::TK_ORDER, None)?;
            s.append(TokenType::TK_BY, None)?;
            append_plan_order_terms(s, &self.order_by, &expr_tables)?;
        }

        if let Some(limit) = &self.limit {
            s.append(TokenType::TK_LIMIT, None)?;
            PlanExprSql::new(limit, &expr_tables).to_tokens(s, &BlankContext)?;
        }

        if let Some(offset) = &self.offset {
            s.append(TokenType::TK_OFFSET, None)?;
            PlanExprSql::new(offset, &expr_tables).to_tokens(s, &BlankContext)?;
        }

        Ok(())
    }
}

impl ToTokens for DeletePlan {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        let table = self
            .table_references
            .joined_tables()
            .first()
            .expect("Delete Plan should have only one table reference");
        let context = &[&self.table_references];

        s.append(TokenType::TK_DELETE, None)?;
        s.append(TokenType::TK_FROM, None)?;
        s.append(TokenType::TK_ID, Some(table.table.get_name()))?;

        if !self.where_clause.is_empty() {
            s.append(TokenType::TK_WHERE, None)?;

            for (i, expr) in self
                .where_clause
                .iter()
                .map(|where_clause| &where_clause.expr)
                .enumerate()
            {
                if i != 0 {
                    s.append(TokenType::TK_AND, None)?;
                }
                PlanExprSql::new(expr, context).to_tokens(s, &BlankContext)?;
            }
        }

        if !self.order_by.is_empty() {
            s.append(TokenType::TK_ORDER, None)?;
            s.append(TokenType::TK_BY, None)?;

            append_plan_order_terms(s, &self.order_by, context)?;
        }

        if let Some(limit) = &self.limit {
            s.append(TokenType::TK_LIMIT, None)?;
            PlanExprSql::new(limit, context).to_tokens(s, &BlankContext)?;
        }

        if let Some(offset) = &self.offset {
            s.append(TokenType::TK_OFFSET, None)?;
            PlanExprSql::new(offset, context).to_tokens(s, &BlankContext)?;
        }

        Ok(())
    }
}

impl ToTokens for UpdatePlan {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        let table = &self.target_table;
        let read_scope_tables = self.build_read_scope_tables();
        let context = [&read_scope_tables];

        s.append(TokenType::TK_UPDATE, None)?;
        s.append(TokenType::TK_ID, Some(table.table.get_name()))?;
        s.append(TokenType::TK_SET, None)?;

        for (index, set_clause) in self.set_clauses.iter().enumerate() {
            if index != 0 {
                s.append(TokenType::TK_COMMA, None)?;
            }
            let col_name = table
                .table
                .get_column_at(set_clause.column_index)
                .as_ref()
                .unwrap()
                .name
                .as_ref()
                .unwrap();
            s.append(TokenType::TK_ID, Some(col_name))?;
            s.append(TokenType::TK_EQ, None)?;
            PlanExprSql::new(&set_clause.expr, &context).to_tokens(s, &BlankContext)?;
        }

        if !self.where_clause.is_empty() {
            s.append(TokenType::TK_WHERE, None)?;

            let mut iter = self
                .where_clause
                .iter()
                .map(|where_clause| &where_clause.expr);
            let first = iter.next().expect("should not be empty");
            PlanExprSql::new(first, &context).to_tokens(s, &BlankContext)?;
            for expr in iter {
                s.append(TokenType::TK_AND, None)?;
                PlanExprSql::new(expr, &context).to_tokens(s, &BlankContext)?;
            }
        }

        if let Some(limit) = &self.limit {
            s.append(TokenType::TK_LIMIT, None)?;
            PlanExprSql::new(limit, &context).to_tokens(s, &BlankContext)?;
        }
        if let Some(offset) = &self.offset {
            s.append(TokenType::TK_OFFSET, None)?;
            PlanExprSql::new(offset, &context).to_tokens(s, &BlankContext)?;
        }

        Ok(())
    }
}
