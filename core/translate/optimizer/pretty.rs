//! Pretty-printing utilities for WHERETRACE output.
//!
//! Provides SQLite-style formatted output with optional ANSI color support.
//! Colors are automatically disabled when stderr is not a terminal.

use std::io::IsTerminal;

/// ANSI color codes for trace output.
/// Colors are organized by semantic meaning to make trace output scannable.
pub mod colors {
    /// Reset all formatting
    pub const RESET: &str = "\x1b[0m";
    /// Bold text
    pub const BOLD: &str = "\x1b[1m";
    /// Dim text (for less important info)
    pub const DIM: &str = "\x1b[2m";

    // Semantic color categories
    /// Cyan - SQL keywords and structure (FROM, WHERE, ORDERBY)
    pub const KEYWORD: &str = "\x1b[36m";
    /// Yellow - Table names
    pub const TABLE: &str = "\x1b[33m";
    /// Magenta - Index names
    pub const INDEX: &str = "\x1b[35m";
    /// Green - Column references
    pub const COLUMN: &str = "\x1b[32m";
    /// Blue - Operators (=, >, <, AND, OR)
    pub const OPERATOR: &str = "\x1b[34m";
    /// Light Red - Cost values (draws attention to performance metrics)
    pub const COST: &str = "\x1b[91m";
    /// Light Green - Good decisions (add, choose)
    pub const GOOD: &str = "\x1b[92m";
    /// Light Red - Bad decisions (skip, reject)
    pub const BAD: &str = "\x1b[91m";
    /// Bold White - Section headers
    pub const HEADER: &str = "\x1b[1;37m";
    /// Light Yellow - Literal values
    pub const LITERAL: &str = "\x1b[93m";
    /// Cyan - Tree structure characters
    pub const TREE: &str = "\x1b[36m";
}

/// Check if we should use colors (only when stderr is a terminal).
#[inline]
pub fn use_colors() -> bool {
    std::io::stderr().is_terminal()
}

/// Wrap text in color if colors are enabled.
#[inline]
pub fn color(text: &str, color_code: &str) -> String {
    if use_colors() {
        format!("{}{}{}", color_code, text, colors::RESET)
    } else {
        text.to_string()
    }
}

/// Format a header line (e.g., "*** Optimizer Start ***")
pub fn header(text: &str) -> String {
    color(text, colors::HEADER)
}

/// Format a table name
pub fn table(name: &str) -> String {
    color(name, colors::TABLE)
}

/// Format an index name
pub fn index(name: &str) -> String {
    color(name, colors::INDEX)
}

/// Format a column reference
pub fn column(name: &str) -> String {
    color(name, colors::COLUMN)
}

/// Format an operator
pub fn operator(op: &str) -> String {
    color(op, colors::OPERATOR)
}

/// Format a cost value
pub fn cost(value: f64) -> String {
    color(&format!("{:.1}", value), colors::COST)
}

/// Format a "good" decision (add, choose)
pub fn good(text: &str) -> String {
    color(text, colors::GOOD)
}

/// Format a "bad" decision (skip, reject)
pub fn bad(text: &str) -> String {
    color(text, colors::BAD)
}

/// Format a literal value
pub fn literal(text: &str) -> String {
    color(&format!("'{}'", text), colors::LITERAL)
}

/// Format tree structure characters
pub fn tree(text: &str) -> String {
    color(text, colors::TREE)
}

/// Tree drawing characters for AST printing
pub mod tree_chars {
    pub const BRANCH: &str = "|-- ";
    pub const LAST: &str = "'-- ";
    pub const PIPE: &str = "|   ";
    pub const SPACE: &str = "    ";
}

/// Build an indented tree prefix string.
///
/// # Arguments
/// * `depth` - Current depth in the tree
/// * `is_last` - Whether this is the last child at this level
/// * `parent_prefixes` - String of prefixes from parent levels
pub fn tree_prefix(depth: usize, is_last: bool, parent_prefixes: &str) -> String {
    if depth == 0 {
        return String::new();
    }
    let branch = if is_last {
        tree_chars::LAST
    } else {
        tree_chars::BRANCH
    };
    if use_colors() {
        format!(
            "{}{}{}{}",
            colors::TREE,
            parent_prefixes,
            branch,
            colors::RESET
        )
    } else {
        format!("{}{}", parent_prefixes, branch)
    }
}

/// Format an operator for display (matching SQLite style).
pub fn format_operator(op: &turso_parser::ast::Operator) -> &'static str {
    use turso_parser::ast::Operator;
    match op {
        Operator::Equals => "EQ",
        Operator::NotEquals => "NE",
        Operator::Less => "LT",
        Operator::LessEquals => "LE",
        Operator::Greater => "GT",
        Operator::GreaterEquals => "GE",
        Operator::And => "AND",
        Operator::Or => "OR",
        Operator::Is => "IS",
        Operator::IsNot => "ISNOT",
        Operator::Add => "ADD",
        Operator::Subtract => "SUB",
        Operator::Multiply => "MUL",
        Operator::Divide => "DIV",
        Operator::Modulus => "MOD",
        Operator::BitwiseAnd => "BITAND",
        Operator::BitwiseOr => "BITOR",
        Operator::BitwiseNot => "BITNOT",
        Operator::Concat => "CONCAT",
        Operator::LeftShift => "LSHIFT",
        Operator::RightShift => "RSHIFT",
        Operator::ArrowRight => "->",
        Operator::ArrowRightShift => "->>",
    }
}

/// Format a constraint for trace output (SQLite-compatible).
/// Output: `TERM-{idx} [TYPE] {flags} left={table:col} op={opcode} wtFlags={flags} prob={sel} prereq={mask}`
///
/// This matches SQLite's format with added constraint type badge.
pub fn format_constraint_sqlite(
    idx: usize,
    table_id: usize,
    table_col_pos: Option<usize>,
    op: &turso_parser::ast::Operator,
    selectivity: f64,
    usable: bool,
    consumed: bool,
    lhs_mask: u128,
    is_virtual: bool,
    is_equiv: bool,
    constraint_type: Option<super::constraints::ConstraintType>,
) -> String {
    // Type badge
    let type_badge = match constraint_type {
        Some(super::constraints::ConstraintType::Literal) => "[LIT] ",
        Some(super::constraints::ConstraintType::SelfConstraint) => "[SELF]",
        Some(super::constraints::ConstraintType::JoinConstraint) => "[JOIN]",
        None => "      ",
    };

    // Flags format: {V}.{E}{C}
    let flags = format!(
        "{}{}{}{}",
        if is_virtual { "V" } else { "." },
        if is_equiv { "E" } else { "." },
        ".",
        if consumed { "C" } else { "." }
    );

    // Column reference in SQLite format: {table:col}
    let left_str = match table_col_pos {
        Some(pos) => format!("left={{{}:{}}}", table_id, pos),
        None => format!("left={{{}:-1}}", table_id),
    };

    // Operator code (simplified to 3 digits)
    let op_code = match op {
        turso_parser::ast::Operator::Equals => "002",
        turso_parser::ast::Operator::NotEquals => "003",
        turso_parser::ast::Operator::Greater => "004",
        turso_parser::ast::Operator::GreaterEquals => "005",
        turso_parser::ast::Operator::Less => "006",
        turso_parser::ast::Operator::LessEquals => "007",
        turso_parser::ast::Operator::Is => "012",
        turso_parser::ast::Operator::IsNot => "013",
        turso_parser::ast::Operator::And => "020",
        turso_parser::ast::Operator::Or => "021",
        _ => "000",
    };

    // wtFlags bitmap (consumed = 0x0004)
    let wt_flags = if consumed { 0x0004 } else { 0x0000 } | if !usable { 0x0002 } else { 0 };

    // SQLite uses "1" for prob=1.0, otherwise .2float
    let prob_str = if selectivity >= 0.9999 {
        "1   ".to_string()
    } else {
        format!("{:<4.2}", selectivity)
    };

    // Comma-separated prerequisites
    let mut prereqs = Vec::new();
    for i in 0..128 {
        if (lhs_mask >> i) & 1 != 0 {
            prereqs.push(i.to_string());
        }
    }
    let prereq_str = if prereqs.is_empty() {
        "0".to_string()
    } else {
        prereqs.join(",")
    };

    format!(
        "TERM-{} {} {} {}  op={} wtFlags={:04x} prob={} prereq={}",
        idx,
        type_badge,
        flags,
        column(&left_str),
        op_code,
        wt_flags,
        prob_str,
        prereq_str
    )
}

/// Format a constraint for trace output (simple version for backward compat).
/// Output: `TERM-{idx} col={col} op={op} selectivity={sel} usable={usable}`
pub fn format_constraint(
    idx: usize,
    table_col_pos: Option<usize>,
    op: &turso_parser::ast::Operator,
    selectivity: f64,
    usable: bool,
    lhs_mask: u128,
) -> String {
    let col_str = match table_col_pos {
        Some(pos) => column(&format!("col={}", pos)),
        None => column("col=expr"),
    };
    let op_str = operator(format_operator(op));
    let sel_str = cost(selectivity);
    let usable_str = if usable {
        good("usable=true")
    } else {
        bad("usable=false")
    };

    format!(
        "TERM-{} {} op={} selectivity={} {} lhs_mask={:b}",
        idx, col_str, op_str, sel_str, usable_str, lhs_mask
    )
}

/// Format a table reference for trace output (SQLite style).
/// Output: `{idx:*} main.{name} tab='{name}' nCol={ncol}`
pub fn format_table_ref(idx: usize, name: &str, ncol: usize) -> String {
    format!(
        "{{{}:*}} main.{} tab='{}' nCol={}",
        idx,
        table(name),
        name,
        ncol
    )
}

/// Format an access method candidate for trace output.
/// Output: `add: * {table} idx={index} eq={eq_count} cost {cost}`
pub fn format_access_method_add(
    table_name: &str,
    index_name: Option<&str>,
    eq_count: usize,
    method_cost: f64,
) -> String {
    let idx_str = match index_name {
        Some(name) => index(name),
        None => index("ROWID"),
    };
    format!(
        "{}  {} idx={} eq={} cost {}",
        good("add:"),
        table(table_name),
        idx_str,
        eq_count,
        cost(method_cost)
    )
}

/// Format a skipped access method for trace output.
/// Output: `skip: {table} cost={cost} > bound={bound}`
pub fn format_access_method_skip(table_name: &str, method_cost: f64, bound: f64) -> String {
    format!(
        "{} {} cost={} > bound={}",
        bad("skip:"),
        table(table_name),
        cost(method_cost),
        cost(bound)
    )
}

/// Format solver iteration header.
pub fn format_solver_begin(row_estimate: f64, query_loop: usize) -> String {
    format!(
        "{} (nRowEst={:.0}, nQueryLoop={})",
        header("---- begin solver."),
        row_estimate,
        query_loop
    )
}

/// Format solver round summary.
pub fn format_solver_round(round: usize) -> String {
    header(&format!("---- after round {} ----", round))
}

/// Format solver solution.
pub fn format_solver_solution(total_cost: f64, total_rows: usize, order_satisfied: bool) -> String {
    format!(
        "{} cost={}, nRow={} ORDERBY={}",
        header("---- Solution"),
        cost(total_cost),
        total_rows,
        if order_satisfied { "1" } else { "0" }
    )
}

/// Format optimizer start message.
pub fn format_optimizer_start(
    num_tables: usize,
    wctrl_flags: u32,
    has_order_target: bool,
) -> String {
    format!(
        "{} tables={} wctrlFlags=0x{:x} order_target={}",
        header("*** Optimizer Start ***"),
        num_tables,
        wctrl_flags,
        has_order_target
    )
}

/// Format optimizer finish message.
pub fn format_optimizer_finish(
    total_cost: f64,
    total_rows: usize,
    table_order: &[usize],
) -> String {
    format!(
        "{} cost={} rows={} tables={:?}",
        header("*** Optimizer Finished ***"),
        cost(total_cost),
        total_rows,
        table_order
    )
}

/// Format a new best plan discovered during DP solver.
/// Output: `New    {mask} cost={cost},{rows},{total} order={order}`
pub fn format_solver_new(
    mask_bits: u128,
    run_cost: f64,
    row_count: usize,
    total_cost: f64,
    order_flag: bool,
) -> String {
    format!(
        "{}    {:0b} cost={},{},{} order={}",
        good("New"),
        mask_bits,
        cost(run_cost),
        row_count,
        cost(total_cost),
        if order_flag { "1" } else { "0" }
    )
}

/// Format a memo entry in the DP solver state.
/// Output: ` {mask} cost={cost} nrow={rows} order={order}`
pub fn format_solver_memo_entry(
    mask_bits: u128,
    plan_cost: f64,
    row_count: usize,
    order_flag: bool,
) -> String {
    format!(
        " {:0b} cost={} nrow={} order={}",
        mask_bits,
        cost(plan_cost),
        row_count,
        if order_flag { "1" } else { "0" }
    )
}

/// Format query structure begin message (shows the Turso function responsible).
pub fn format_where_begin() -> String {
    format!(
        "{}\n{}",
        header("'-- compute_best_join_order()"),
        tree("    |-- FROM")
    )
}

/// Format a FROM clause table entry.
/// Output: `    |   '-- {idx:*} main.{name} tab='{name}' nCol={ncol}`
pub fn format_from_table(idx: usize, name: &str, ncol: usize, is_last: bool) -> String {
    let branch = if is_last { "'-- " } else { "|-- " };
    format!(
        "{}{}{{{}:*}} main.{} tab='{}' nCol={}",
        tree("    |   "),
        tree(branch),
        idx,
        table(name),
        name,
        ncol
    )
}

/// Format the start of WHERE clause section.
pub fn format_where_section() -> String {
    format!("{}{}", tree("    '-- "), operator("WHERE"))
}

/// Format the start of ORDERBY clause section.
pub fn format_orderby_section() -> String {
    format!("{}{}", tree("    '-- "), operator("ORDERBY"))
}

/// Pretty-print an expression tree.
/// Recursively formats the expression as an ASCII tree.
pub fn format_expr_tree(expr: &turso_parser::ast::Expr, prefix: &str, is_last: bool) -> String {
    use turso_parser::ast::{Expr, Literal};

    let connector = if is_last { "'-- " } else { "|-- " };
    let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "|   " });

    match expr {
        Expr::Binary(lhs, op, rhs) => {
            let op_str = operator(format_operator(op));
            let mut result = format!("{}{}{}\n", tree(prefix), tree(connector), op_str);
            result.push_str(&format_expr_tree(lhs, &child_prefix, false));
            result.push_str(&format_expr_tree(rhs, &child_prefix, true));
            result
        }
        Expr::Column {
            table, column: col, ..
        } => {
            format!(
                "{}{}{}\n",
                tree(prefix),
                tree(connector),
                column(&format!("{{{}:{}}}", table, col))
            )
        }
        Expr::RowId { table, .. } => {
            format!(
                "{}{}{}\n",
                tree(prefix),
                tree(connector),
                column(&format!("{{{}:-1}}", table))
            )
        }
        Expr::Literal(lit) => {
            let lit_str = match lit {
                Literal::Numeric(n) => n.clone(),
                Literal::String(s) => format!("'{}'", s),
                Literal::Null => "NULL".to_string(),
                Literal::Blob(b) => format!("x'{}'", b),
                Literal::CurrentDate => "CURRENT_DATE".to_string(),
                Literal::CurrentTime => "CURRENT_TIME".to_string(),
                Literal::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
                Literal::Keyword(k) => k.clone(),
            };
            format!("{}{}{}\n", tree(prefix), tree(connector), literal(&lit_str))
        }
        Expr::Unary(op, inner) => {
            let op_str = match op {
                turso_parser::ast::UnaryOperator::Not => "NOT",
                turso_parser::ast::UnaryOperator::Negative => "-",
                turso_parser::ast::UnaryOperator::Positive => "+",
                turso_parser::ast::UnaryOperator::BitwiseNot => "~",
            };
            let mut result = format!("{}{}{}\n", tree(prefix), tree(connector), operator(op_str));
            result.push_str(&format_expr_tree(inner, &child_prefix, true));
            result
        }
        Expr::IsNull(inner) => {
            let mut result = format!(
                "{}{}{}\n",
                tree(prefix),
                tree(connector),
                operator("ISNULL")
            );
            result.push_str(&format_expr_tree(inner, &child_prefix, true));
            result
        }
        Expr::NotNull(inner) => {
            let mut result = format!(
                "{}{}{}\n",
                tree(prefix),
                tree(connector),
                operator("NOTNULL")
            );
            result.push_str(&format_expr_tree(inner, &child_prefix, true));
            result
        }
        Expr::Between {
            lhs,
            not,
            start,
            end,
        } => {
            let op_str = if *not { "NOT BETWEEN" } else { "BETWEEN" };
            let mut result = format!("{}{}{}\n", tree(prefix), tree(connector), operator(op_str));
            result.push_str(&format_expr_tree(lhs, &child_prefix, false));
            result.push_str(&format_expr_tree(start, &child_prefix, false));
            result.push_str(&format_expr_tree(end, &child_prefix, true));
            result
        }
        Expr::InList { lhs, not, rhs } => {
            let op_str = if *not { "NOT IN" } else { "IN" };
            let mut result = format!(
                "{}{}{} (LIST)\n",
                tree(prefix),
                tree(connector),
                operator(op_str)
            );
            result.push_str(&format_expr_tree(lhs, &child_prefix, rhs.is_empty()));
            for (i, item) in rhs.iter().enumerate() {
                result.push_str(&format_expr_tree(item, &child_prefix, i == rhs.len() - 1));
            }
            result
        }
        Expr::InSelect { lhs, not, .. } => {
            let op_str = if *not { "NOT IN" } else { "IN" };
            let mut result = format!(
                "{}{}{} (SELECT)\n",
                tree(prefix),
                tree(connector),
                operator(op_str)
            );
            result.push_str(&format_expr_tree(lhs, &child_prefix, true));
            result
        }
        Expr::Like {
            lhs, not, op, rhs, ..
        } => {
            let op_str = match op {
                turso_parser::ast::LikeOperator::Like => {
                    if *not {
                        "NOT LIKE"
                    } else {
                        "LIKE"
                    }
                }
                turso_parser::ast::LikeOperator::Glob => {
                    if *not {
                        "NOT GLOB"
                    } else {
                        "GLOB"
                    }
                }
                turso_parser::ast::LikeOperator::Match => {
                    if *not {
                        "NOT MATCH"
                    } else {
                        "MATCH"
                    }
                }
                turso_parser::ast::LikeOperator::Regexp => {
                    if *not {
                        "NOT REGEXP"
                    } else {
                        "REGEXP"
                    }
                }
            };
            let mut result = format!("{}{}{}\n", tree(prefix), tree(connector), operator(op_str));
            result.push_str(&format_expr_tree(lhs, &child_prefix, false));
            result.push_str(&format_expr_tree(rhs, &child_prefix, true));
            result
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            let mut result = format!("{}{}{}\n", tree(prefix), tree(connector), operator("CASE"));
            if let Some(base_expr) = base {
                result.push_str(&format_expr_tree(base_expr, &child_prefix, false));
            }
            for (cond, then_expr) in when_then_pairs {
                result.push_str(&format!("{}{}WHEN\n", tree(&child_prefix), tree("|-- ")));
                result.push_str(&format_expr_tree(
                    cond,
                    &format!("{}{}", child_prefix, "|   "),
                    true,
                ));
                result.push_str(&format!("{}{}THEN\n", tree(&child_prefix), tree("|-- ")));
                result.push_str(&format_expr_tree(
                    then_expr,
                    &format!("{}{}", child_prefix, "|   "),
                    true,
                ));
            }
            if let Some(else_e) = else_expr {
                result.push_str(&format!("{}{}ELSE\n", tree(&child_prefix), tree("'-- ")));
                result.push_str(&format_expr_tree(
                    else_e,
                    &format!("{}{}", child_prefix, "    "),
                    true,
                ));
            }
            result
        }
        Expr::Cast { expr, type_name } => {
            let type_str = type_name
                .as_ref()
                .map_or("?".to_string(), |t| format!("{:?}", t));
            let mut result = format!(
                "{}{}{} AS {}\n",
                tree(prefix),
                tree(connector),
                operator("CAST"),
                type_str
            );
            result.push_str(&format_expr_tree(expr, &child_prefix, true));
            result
        }
        Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                format_expr_tree(&exprs[0], prefix, is_last)
            } else {
                let mut result = format!("{}{}()\n", tree(prefix), tree(connector));
                for (i, e) in exprs.iter().enumerate() {
                    result.push_str(&format_expr_tree(e, &child_prefix, i == exprs.len() - 1));
                }
                result
            }
        }
        Expr::FunctionCall { name, args, .. } => {
            let func_name = name.as_str();
            let mut result = format!("{}{}{}()\n", tree(prefix), tree(connector), func_name);
            for (i, arg) in args.iter().enumerate() {
                result.push_str(&format_expr_tree(arg, &child_prefix, i == args.len() - 1));
            }
            result
        }
        Expr::FunctionCallStar { name, .. } => {
            format!("{}{}{}(*)\n", tree(prefix), tree(connector), name.as_str())
        }
        Expr::Id(name) => {
            format!("{}{}{}\n", tree(prefix), tree(connector), name.as_str())
        }
        Expr::Name(name) => {
            format!("{}{}{}\n", tree(prefix), tree(connector), name.as_str())
        }
        Expr::Qualified(table_name, col_name) => {
            format!(
                "{}{}{}.{}\n",
                tree(prefix),
                tree(connector),
                table_name.as_str(),
                column(col_name.as_str())
            )
        }
        Expr::SubqueryResult {
            query_type: _query_type,
            ..
        } => {
            format!("{}{}{}\n", tree(prefix), tree(connector), "(SUBQUERY)")
        }
        _ => {
            format!("{}{}(expr)\n", tree(prefix), tree(connector))
        }
    }
}

/// Format WHERE clause constraint entry (with expression tree).
/// Output matches SQLite's TERM format.
pub fn format_where_term(
    idx: usize,
    table_col: Option<(usize, i32)>, // (table_idx, col_idx) where -1 means rowid
    op_code: u32,
    wt_flags: u32,
    prob: f64,
    prereq_mask: u128,
    expr: Option<&turso_parser::ast::Expr>,
    is_equiv: bool,
    is_consumed: bool,
    is_virtual: bool,
) -> String {
    // Flags format: {V}.{E}{C}
    let flags = format!(
        "{}{}{}{}",
        if is_virtual { "V" } else { "." },
        if is_equiv { "E" } else { "." },
        ".",
        if is_consumed { "C" } else { "." }
    );

    // Column reference in SQLite format: {table:col}
    let left_str = match table_col {
        Some((t, c)) => format!("left={{{}:{}}}", t, c),
        None => "left=-1".to_string(),
    };

    // SQLite uses "1" for prob=1.0, otherwise .2float
    let prob_str = if prob >= 0.9999 {
        "1   ".to_string()
    } else {
        format!("{:<4.2}", prob)
    };

    // Comma-separated prerequisites
    let mut prereqs = Vec::new();
    for i in 0..128 {
        if (prereq_mask >> i) & 1 != 0 {
            prereqs.push(i.to_string());
        }
    }
    let prereq_str = if prereqs.is_empty() {
        "0".to_string()
    } else {
        prereqs.join(",")
    };

    let mut result = format!(
        "TERM-{}   {} {}  op={:03} wtFlags={:04x} prob={} prereq={}\n",
        idx,
        flags,
        column(&left_str),
        op_code,
        wt_flags,
        prob_str,
        prereq_str
    );

    if let Some(e) = expr {
        result.push_str(&format_expr_tree(e, "", true));
    }

    result
}

/// Format a WhereLoop entry (SQLite's internal loop candidate format).
/// Output: `{loop_idx} {mask}.{name} f {flags:06x} N {nEq} cost {setup},{run},{nOut}`
pub fn format_where_loop(
    loop_idx: usize,
    table_idx: usize,
    table_name: &str,
    flags: u32,
    n_eq: usize,
    setup_cost: f64,
    run_cost: f64,
    n_out: f64,
) -> String {
    format!(
        "{} {:02}.{:02}.{:02} {} f {:06x} N {} cost {},{},{}",
        loop_idx,
        0, // mask byte 0
        table_idx,
        0, // sub-index
        table(table_name),
        flags,
        n_eq,
        cost(setup_cost),
        cost(run_cost),
        cost(n_out)
    )
}

/// Format individual index candidate evaluation with badge-style.
/// Output:
/// ```text
///   [CANDIDATE 0] rowid
///     ├─ flags: UNIQUE
///     ├─ eq: 0 constraints
///     └─ cost: setup=0 | run=20K | rows=1M
/// ```
pub fn format_add_candidate(
    _loop_idx: usize,
    _table_idx: usize,
    sub_idx: usize,
    identifier: &str,
    flags: u32,
    usable_constraints: usize,
    setup_cost: f64,
    run_cost: f64,
    output_cardinality: f64,
) -> String {
    // Decode common flags for readability
    let is_unique = (flags & 0x000001) != 0;
    let is_covering = (flags & 0x000100) != 0;
    let is_rowid = (flags & 0x800000) != 0;

    let scan_type = if is_rowid { "rowid" } else { "index" };

    // Build flags list
    let mut flag_list = Vec::new();
    if is_unique {
        flag_list.push("UNIQUE");
    }
    if is_covering {
        flag_list.push("covering");
    }
    let flags_str = if flag_list.is_empty() {
        "none".to_string()
    } else {
        flag_list.join(", ")
    };

    // Badge-style output
    let candidate_badge = if use_colors() {
        format!(
            "{}[CANDIDATE {}]{}",
            colors::OPERATOR,
            sub_idx,
            colors::RESET
        )
    } else {
        format!("[CANDIDATE {}]", sub_idx)
    };

    format!(
        "  {} {} {}\n    ├─ flags: {}\n    ├─ eq: {} constraints\n    └─ cost: setup={} | run={} | rows={}",
        candidate_badge,
        scan_type,
        table(identifier),
        flags_str,
        usable_constraints,
        format_cost_human(setup_cost),
        format_cost_colored(run_cost),
        format_rows_human(output_cardinality)
    )
}

/// Format a SCAN-TERM message with badge-style.
pub fn format_scan_term(addr: &str, is_equiv: bool, table_idx: usize, col_idx: i32) -> String {
    let term_badge = if use_colors() {
        format!("{}[SCAN-TERM]{}", colors::GOOD, colors::RESET)
    } else {
        "[SCAN-TERM]".to_string()
    };
    format!(
        "  {} addr={} nEquiv={} {{{}:{}}}",
        term_badge,
        addr,
        if is_equiv { 2 } else { 1 },
        table_idx,
        col_idx
    )
}

/// Format addBtreeIdx BEGIN header with badge-style.
pub fn format_btree_idx_begin(
    table_name: &str,
    constraint_count: usize,
    candidate_count: usize,
) -> String {
    let table_badge = if use_colors() {
        format!("{}[TABLE]{}", colors::TABLE, colors::RESET)
    } else {
        "[TABLE]".to_string()
    };
    format!(
        "{} {}\n  ├─ [SCAN] constraints: {} | candidates: {}",
        table_badge,
        table(table_name),
        constraint_count,
        candidate_count
    )
}

/// Format addBtreeIdx END/result with badge-style.
pub fn format_btree_idx_end(table_name: &str, usable_constraints: usize, best_cost: f64) -> String {
    let winner_badge = if use_colors() {
        format!("{}[WINNER]{}", colors::GOOD, colors::RESET)
    } else {
        "[WINNER]".to_string()
    };
    format!(
        "  └─ {} {} cost={} matched={} constraints",
        winner_badge,
        table(table_name),
        format_cost_colored(best_cost),
        usable_constraints
    )
}

fn pretty_cost(c: f64) -> String {
    if c >= 1000000.0 {
        format!("{:.1e}", c)
    } else if c >= 1000.0 {
        format!("{:.0}", c)
    } else {
        format!("{:.1}", c)
    }
}

/// Format row count with human-readable suffixes (K, M).
fn format_rows(rows: f64) -> String {
    if rows >= 1_000_000.0 {
        format!("{:.1}M", rows / 1_000_000.0)
    } else if rows >= 1_000.0 {
        format!("{:.1}K", rows / 1_000.0)
    } else {
        format!("{:.0}", rows)
    }
}

/// Format coding level message.
/// Output: `Coding level {level} of {total}:  notReady={mask:016x}  iFrom={from_idx}`
pub fn format_coding_level(
    level: usize,
    total: usize,
    not_ready_mask: u64,
    from_idx: usize,
) -> String {
    format!(
        "{} level {} of {}:  notReady=0x{:016x}  iFrom={}",
        header("Coding"),
        level,
        total,
        not_ready_mask,
        from_idx
    )
}

/// Format term disable message.
/// Output: `DISABLE-TERM-{idx}`
pub fn format_disable_term(idx: usize) -> String {
    format!("{}", bad(&format!("DISABLE-TERM-{}", idx)))
}

/// Format WHERE clause end of analysis header.
/// Output: `---- WHERE clause at end of analysis:`
pub fn format_where_clause_end_of_analysis() -> String {
    header("---- WHERE clause at end of analysis:")
}

/// Format WHERE clause being coded header.
/// Output: `WHERE clause being coded:`
pub fn format_where_clause_being_coded() -> String {
    header("WHERE clause being coded:")
}

// ============================================================================
// Modern Badge-Style Formatters for Rejected Path Tracing
// ============================================================================

/// Badge types for trace output
#[derive(Clone, Copy, Debug)]
pub enum Badge {
    Add,    // Green [ADD]
    Skip,   // Red [SKIP]
    Hash,   // Cyan [HASH]
    Solver, // Blue [SOLVER]
    Winner, // Bold Green [WINNER]
}

/// Format a colored badge
pub fn badge(b: Badge) -> String {
    let (text, color_code) = match b {
        Badge::Add => ("ADD", colors::GOOD),
        Badge::Skip => ("SKIP", colors::BAD),
        Badge::Hash => ("HASH", colors::KEYWORD),
        Badge::Solver => ("SOLVER", colors::OPERATOR),
        Badge::Winner => ("WINNER", colors::GOOD),
    };
    if use_colors() {
        format!("{}[{}]{}", color_code, text, colors::RESET)
    } else {
        format!("[{}]", text)
    }
}

/// Rejection reason codes (for programmatic use + JSON output)
#[derive(Clone, Copy, Debug)]
pub enum RejectionReason {
    CostExceedsBound,
    NoUsableConstraints,
    HashJoinIndexExists,
    HashJoinOuterJoin,
    HashJoinCorrelatedSubquery,
    HashJoinSelfJoin,
    HashJoinOrderPreserved,
    HashJoinBuildUsesConstraints,
    HashJoinProbeIsPriorBuild,
    HashJoinBuildHasPriorConstraints,
    HashJoinBuildNotPlainScan,
    LeftJoinOrdering,
    NoViableLhsPlan,
}

impl RejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CostExceedsBound => "cost_exceeds_bound",
            Self::NoUsableConstraints => "no_usable_constraints",
            Self::HashJoinIndexExists => "index_exists",
            Self::HashJoinOuterJoin => "outer_join",
            Self::HashJoinCorrelatedSubquery => "correlated_subquery",
            Self::HashJoinSelfJoin => "self_join",
            Self::HashJoinOrderPreserved => "order_preserved_by_nested_loop",
            Self::HashJoinBuildUsesConstraints => "build_uses_constraints",
            Self::HashJoinProbeIsPriorBuild => "probe_is_prior_build",
            Self::HashJoinBuildHasPriorConstraints => "build_has_prior_constraints",
            Self::HashJoinBuildNotPlainScan => "build_not_plain_scan",
            Self::LeftJoinOrdering => "left_join_ordering",
            Self::NoViableLhsPlan => "no_viable_lhs_plan",
        }
    }
}

/// Format cost with human-readable K/M suffixes
pub fn format_cost_human(c: f64) -> String {
    if c >= 1_000_000.0 {
        format!("{:.1}M", c / 1_000_000.0)
    } else if c >= 1_000.0 {
        format!("{:.0}K", c / 1_000.0)
    } else {
        format!("{:.1}", c)
    }
}

/// Format a rejected join order (Inline Badge style)
/// Output: `  [SKIP] part→lineitem      cost=660K > bound=40K`
///         `         └─ reason: cost_exceeds_bound (16.5× over)`
pub fn format_rejected_join(
    table_chain: &str,
    plan_cost: f64,
    bound: f64,
    reason: RejectionReason,
) -> String {
    let ratio = plan_cost / bound;
    format!(
        "  {} {:<20} cost={} > bound={}\n         └─ reason: {} ({:.1}× over)",
        badge(Badge::Skip),
        table_chain,
        format_cost_human(plan_cost),
        format_cost_human(bound),
        reason.as_str(),
        ratio
    )
}

/// Format a rejected hash join candidate
/// Output: `  [SKIP] hash_join: build=t1 probe=t2  reason=index_exists`
///         `         └─ idx=pk_part(p_partkey)`
pub fn format_rejected_hash_join(
    build_table: &str,
    probe_table: &str,
    reason: RejectionReason,
    details: Option<&str>,
) -> String {
    let mut result = format!(
        "  {} hash_join: build={} probe={}  reason={}",
        badge(Badge::Skip),
        table(build_table),
        table(probe_table),
        reason.as_str()
    );
    if let Some(d) = details {
        result.push_str(&format!("\n         └─ {}", d));
    }
    result
}

/// Format an accepted candidate (for consistency)
/// Output: `  [ADD]  lineitem           cost=40K  rows=6,000`
pub fn format_accepted_join(table_name: &str, plan_cost: f64, rows: usize) -> String {
    format!(
        "  {} {:<20} cost={}  rows={}",
        badge(Badge::Add),
        table(table_name),
        format_cost_human(plan_cost),
        rows
    )
}

// ============================================================================
// JSON Formatters
// ============================================================================

/// JSON rejection event for join order
pub fn format_rejected_json(
    event_type: &str,
    table_name: &str,
    plan_cost: f64,
    bound: f64,
    reason: RejectionReason,
) -> String {
    format!(
        r#"{{"event":"{}","table":"{}","cost":{:.0},"bound":{:.0},"reason":"{}","ratio":{:.2}}}"#,
        event_type,
        table_name,
        plan_cost,
        bound,
        reason.as_str(),
        plan_cost / bound
    )
}

/// JSON rejection event for hash join
pub fn format_rejected_hash_json(build: &str, probe: &str, reason: RejectionReason) -> String {
    format!(
        r#"{{"event":"hash_join_rejected","build":"{}","probe":"{}","reason":"{}"}}"#,
        build,
        probe,
        reason.as_str()
    )
}

// ============================================================================
// Enhanced Formatting - Color-Coded Costs
// ============================================================================

/// Format cost with color based on magnitude (green=cheap, yellow=moderate, red=expensive)
pub fn format_cost_colored(cost_val: f64) -> String {
    let formatted = format_cost_human(cost_val);
    if !use_colors() {
        return formatted;
    }

    if cost_val < 10_000.0 {
        color(&formatted, colors::GOOD) // Green - cheap
    } else if cost_val < 1_000_000.0 {
        color(&formatted, colors::LITERAL) // Yellow - moderate
    } else {
        color(&formatted, colors::BAD) // Red - expensive
    }
}

/// Format row count with commas for readability
pub fn format_rows_human(rows: f64) -> String {
    if rows >= 1_000_000_000.0 {
        format!("{:.1}B", rows / 1_000_000_000.0)
    } else if rows >= 1_000_000.0 {
        format!("{:.1}M", rows / 1_000_000.0)
    } else if rows >= 1_000.0 {
        format!("{:.0}K", rows / 1_000.0)
    } else {
        format!("{:.0}", rows)
    }
}

// ============================================================================
// Structured Add Lines with Tree Characters
// ============================================================================

/// Unicode box-drawing characters for enhanced formatting
pub mod box_chars {
    pub const TOP_LEFT: &str = "┌";
    pub const TOP_RIGHT: &str = "┐";
    pub const BOTTOM_LEFT: &str = "└";
    pub const BOTTOM_RIGHT: &str = "┘";
    pub const HORIZONTAL: &str = "─";
    pub const VERTICAL: &str = "│";
    pub const T_RIGHT: &str = "├";
    pub const T_LEFT: &str = "┤";
    pub const T_DOWN: &str = "┬";
    pub const T_UP: &str = "┴";
    pub const CROSS: &str = "┼";
    pub const ARROW_DOWN: &str = "↓";
    pub const CHECK: &str = "✓";
    pub const CROSS_MARK: &str = "✗";
    pub const BULLET: &str = "•";
}

/// Format structured add line with tree visualization
/// Output:
/// ```text
/// [EVAL] Scan lineitem (table 0, idx 1)
///        ├─ scan_cost: 20K (1M rows)
///        └─ output: 100K rows
/// ```
pub fn format_add_structured(
    table_name: &str,
    scan_type: &str, // "Scan", "IndexSeek", "HashJoin"
    base_cost: f64,
    run_cost: f64,
    base_rows: f64,
    output_rows: f64,
) -> String {
    let eval_badge = if use_colors() {
        format!("{}[EVAL]{}", colors::OPERATOR, colors::RESET)
    } else {
        "[EVAL]".to_string()
    };

    format!(
        "{} {} {}\n\
         {}├─ cost: {} (base: {})\n\
         {}└─ rows: {} → {}",
        eval_badge,
        scan_type,
        table(table_name),
        "       ",
        format_cost_colored(run_cost),
        format_cost_colored(base_cost),
        "       ",
        format_rows_human(base_rows),
        format_rows_human(output_rows)
    )
}

// ============================================================================
// Join Order Decision Tables
// ============================================================================

/// Candidate entry for round summary table
pub struct RoundCandidate {
    pub mask_str: String,
    pub tables: String,
    pub cost: f64,
    pub rows: f64,
    pub status: CandidateStatus,
}

#[derive(Clone, Copy)]
pub enum CandidateStatus {
    Base,
    New,
    Best,
    Skipped,
}

impl CandidateStatus {
    fn symbol(&self) -> &'static str {
        match self {
            Self::Base => "○",
            Self::New => "+",
            Self::Best => "✓",
            Self::Skipped => "✗",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Base => "BASE",
            Self::New => "NEW",
            Self::Best => "BEST",
            Self::Skipped => "SKIP",
        }
    }
}

/// Format round summary as ASCII table
pub fn format_round_summary_table(round: usize, candidates: &[RoundCandidate]) -> String {
    let mut result = String::new();

    // Header
    result.push_str(&format!(
        "\n{}\n",
        header(&format!("Round {} Summary:", round))
    ));
    result.push_str("  ┌──────┬────────────────┬────────────┬───────────┬────────┐\n");
    result.push_str("  │ Mask │ Tables         │ Cost       │ Rows      │ Status │\n");
    result.push_str("  ├──────┼────────────────┼────────────┼───────────┼────────┤\n");

    for c in candidates {
        let status_str = match c.status {
            CandidateStatus::Best => good(&format!("{} {}", c.status.symbol(), c.status.label())),
            CandidateStatus::Skipped => bad(&format!("{} {}", c.status.symbol(), c.status.label())),
            _ => format!("{} {}", c.status.symbol(), c.status.label()),
        };

        result.push_str(&format!(
            "  │ {:>4} │ {:<14} │ {:>10} │ {:>9} │ {:>6} │\n",
            c.mask_str,
            c.tables,
            format_cost_human(c.cost),
            format_rows_human(c.rows),
            status_str
        ));
    }

    result.push_str("  └──────┴────────────────┴────────────┴───────────┴────────┘\n");
    result
}

// ============================================================================
// Extended Skip Explanations
// ============================================================================

/// Cost breakdown for extended skip explanation
pub struct CostBreakdown {
    pub outer_scan_cost: f64,
    pub outer_rows: f64,
    pub inner_seek_cost: f64,
    pub seeks_count: f64,
    pub cost_per_seek: f64,
}

/// Format extended rejection with cost breakdown
pub fn format_rejected_extended(
    table_chain: &str,
    total_cost: f64,
    bound: f64,
    breakdown: Option<&CostBreakdown>,
) -> String {
    let ratio = total_cost / bound;

    let mut result = format!(
        "  {} {}\n\
         {}├─ Reason: {}\n\
         {}├─ Total cost: {}\n\
         {}├─ Current best: {}\n",
        badge(Badge::Skip),
        table_chain,
        "  ",
        bad("cost_exceeds_bound"),
        "  ",
        format_cost_colored(total_cost),
        "  ",
        format_cost_colored(bound),
    );

    if let Some(b) = breakdown {
        result.push_str(&format!(
            "{}├─ Cost breakdown:\n\
             {}│   ├─ Outer scan: {} ({} rows)\n\
             {}│   └─ Inner seeks: {} ({:.0} × {:.1})\n",
            "  ",
            "  ",
            format_cost_human(b.outer_scan_cost),
            format_rows_human(b.outer_rows),
            "  ",
            format_cost_human(b.inner_seek_cost),
            b.seeks_count,
            b.cost_per_seek,
        ));
    }

    result.push_str(&format!("{}└─ Factor: {:.1}× more expensive", "  ", ratio));

    result
}

// ============================================================================
// Selectivity Breakdown
// ============================================================================

/// Single filter's selectivity info
pub struct FilterSelectivity {
    pub description: String,
    pub selectivity: f64,
    pub output_rows: f64,
}

/// Format selectivity analysis for a table
pub fn format_selectivity_analysis(
    table_name: &str,
    base_rows: f64,
    filters: &[FilterSelectivity],
) -> String {
    let mut result = format!(
        "\n{}Selectivity analysis for {}:{}\n\
         {}Base rows: {}\n",
        colors::HEADER,
        table(table_name),
        colors::RESET,
        "  ",
        format_rows_human(base_rows)
    );

    if filters.is_empty() {
        result.push_str("  └─ No filters applied\n");
        return result;
    }

    for (i, f) in filters.iter().enumerate() {
        let is_last = i == filters.len() - 1;
        let prefix = if is_last { "└─" } else { "├─" };

        result.push_str(&format!(
            "  {} {} (sel={:.2}) → {} rows\n",
            prefix,
            f.description,
            f.selectivity,
            format_rows_human(f.output_rows)
        ));
    }

    result
}

// ============================================================================
// Final Plan Visualization
// ============================================================================

/// Plan step for visualization
pub struct PlanStep {
    pub step_num: usize,
    pub operation: String, // "SCAN", "INDEX SEEK", "HASH JOIN"
    pub table_name: String,
    pub index_name: Option<String>,
    pub cost: f64,
    pub input_rows: f64,
    pub output_rows: f64,
    pub filters: Vec<String>,
}

/// Format final execution plan visualization
pub fn format_final_plan(steps: &[PlanStep], total_cost: f64, total_rows: f64) -> String {
    let mut result = String::new();

    // Header
    result.push_str("\n╔═══════════════════════════════════════╗\n");
    result.push_str("║         FINAL EXECUTION PLAN          ║\n");
    result.push_str("╚═══════════════════════════════════════╝\n\n");

    for (i, step) in steps.iter().enumerate() {
        // Step header
        result.push_str(&format!(
            "{}. {} {}\n",
            step.step_num,
            header(&step.operation),
            table(&step.table_name)
        ));

        // Index info if present
        if let Some(idx) = &step.index_name {
            result.push_str(&format!("   ├─ Index: {}\n", index(idx)));
        }

        // Cost and rows
        result.push_str(&format!(
            "   ├─ Cost: {} | Rows: {} → {}\n",
            format_cost_colored(step.cost),
            format_rows_human(step.input_rows),
            format_rows_human(step.output_rows)
        ));

        // Filters
        if !step.filters.is_empty() {
            result.push_str("   └─ Filters:\n");
            for (j, filter) in step.filters.iter().enumerate() {
                let is_last = j == step.filters.len() - 1;
                let prefix = if is_last {
                    "      └─"
                } else {
                    "      ├─"
                };
                result.push_str(&format!("{} {}\n", prefix, filter));
            }
        } else {
            result.push_str("   └─ (no filters)\n");
        }

        // Arrow to next step
        if i < steps.len() - 1 {
            result.push_str(&format!("       {}\n", box_chars::ARROW_DOWN));
        }
    }

    // Footer
    result.push_str(&format!(
        "\nTotal Cost: {} | Output Rows: {}\n",
        format_cost_colored(total_cost),
        format_rows_human(total_rows)
    ));

    result
}

// ============================================================================
// Optimizer Statistics Summary
// ============================================================================

/// Optimizer statistics for summary
pub struct OptimizerStats {
    pub tables: usize,
    pub join_orders_evaluated: usize,
    pub plans_considered: usize,
    pub plans_skipped: usize,
    pub optimization_time_ms: f64,
}

/// Format optimizer statistics summary
pub fn format_optimizer_stats(stats: &OptimizerStats) -> String {
    format!(
        "\n╔═══════════════════════════════════════╗\n\
         ║       OPTIMIZER STATISTICS            ║\n\
         ╚═══════════════════════════════════════╝\n\n\
         Search Space:\n\
         {}Tables: {}\n\
         {}Join orders evaluated: {}\n\
         {}Plans considered: {}\n\
         {}Plans skipped: {} (cost pruning)\n\n\
         Timing:\n\
         {}Optimization time: {:.2}ms\n",
        "  • ",
        stats.tables,
        "  • ",
        stats.join_orders_evaluated,
        "  • ",
        stats.plans_considered,
        "  • ",
        stats.plans_skipped,
        "  • ",
        stats.optimization_time_ms
    )
}

// ============================================================================
// Performance Warnings
// ============================================================================

/// Warning types for optimization suggestions
pub enum OptimizationWarning {
    LargeOrClause(usize),         // Number of OR branches
    FullTableScan(String),        // Table name
    MissingIndex(String, String), // Table, column
    HighCostRatio(f64),           // Ratio vs optimal
}

/// Format optimization warnings
pub fn format_warnings(warnings: &[OptimizationWarning]) -> String {
    if warnings.is_empty() {
        return String::new();
    }

    let mut result = format!(
        "\n{}⚠  Optimization Warnings:{}\n",
        colors::LITERAL,
        colors::RESET
    );

    for w in warnings {
        result.push_str(&format!("  {} ", box_chars::BULLET));
        match w {
            OptimizationWarning::LargeOrClause(n) => {
                result.push_str(&format!(
                    "Large OR clause ({} branches) - consider UNION ALL\n",
                    n
                ));
            }
            OptimizationWarning::FullTableScan(tbl) => {
                result.push_str(&format!(
                    "Full table scan on {} - consider adding index\n",
                    table(tbl)
                ));
            }
            OptimizationWarning::MissingIndex(tbl, col) => {
                result.push_str(&format!(
                    "Missing index on {}.{}\n",
                    table(tbl),
                    column(col)
                ));
            }
            OptimizationWarning::HighCostRatio(r) => {
                result.push_str(&format!(
                    "Cost {:.1}× higher than optimal - review query structure\n",
                    r
                ));
            }
        }
    }

    result
}

// ============================================================================
// Human-Readable Round Tables
// ============================================================================

/// Format solver round header with join count
pub fn format_round_header(round: usize, tables_joined: usize) -> String {
    format!(
        "\n╔══════════════════════════════════════╗\n\
         ║  Round {}: {} table{} joined            ║\n\
         ╚══════════════════════════════════════╝\n",
        round,
        tables_joined,
        if tables_joined == 1 { "" } else { "s" }
    )
}

/// Entry for readable round table
pub struct ReadableRoundEntry {
    pub id: u128,
    pub join_order: String,
    pub cost: f64,
    pub rows: f64,
    pub is_best: bool,
    pub is_skipped: bool,
}

/// Format readable round summary table
pub fn format_readable_round_table(entries: &[ReadableRoundEntry]) -> String {
    let mut result = String::new();
    result.push_str("  ┌─────┬─────────────────┬───────────┬──────────┬─────────┐\n");
    result.push_str("  │ ID  │ Join Order      │ Cost      │ Rows     │ Status  │\n");
    result.push_str("  ├─────┼─────────────────┼───────────┼──────────┼─────────┤\n");

    for e in entries {
        let status = if e.is_best {
            good("★")
        } else if e.is_skipped {
            bad("✗")
        } else {
            "○".to_string()
        };
        result.push_str(&format!(
            "  │{:>4} │ {:<15} │ {:>9} │ {:>8} │    {}    │\n",
            e.id,
            e.join_order,
            format_cost_human(e.cost),
            format_rows_human(e.rows),
            status
        ));
    }

    result.push_str("  └─────┴─────────────────┴───────────┴──────────┴─────────┘\n");
    result
}

// ============================================================================
// Detailed Join Evaluation
// ============================================================================

/// Join evaluation info
pub struct JoinEvaluation {
    pub outer_table: String,
    pub outer_access: String,
    pub outer_cost: f64,
    pub outer_rows: f64,
    pub inner_table: String,
    pub inner_access: String,
    pub index_name: Option<String>,
    pub seeks: f64,
    pub cost_per_seek: f64,
    pub output_rows: f64,
    pub total_cost: f64,
    pub is_new_best: bool,
}

/// Format join evaluation box
pub fn format_join_evaluation(eval: &JoinEvaluation) -> String {
    let mut result = format!(
        "\n  Evaluating: {} → {}\n\
         ┌─────────────────────────────────────────────────┐\n\
         │ Outer: {:<40} │\n\
         │   Access: {:<37} │\n\
         │   Cost: {:>9}    Rows: {:>15} │\n\
         │ Inner: {:<40} │\n\
         │   Access: {:<37} │\n",
        eval.outer_table,
        eval.inner_table,
        eval.outer_table,
        eval.outer_access,
        format_cost_human(eval.outer_cost),
        format_rows_human(eval.outer_rows),
        eval.inner_table,
        eval.inner_access,
    );
    if let Some(idx) = &eval.index_name {
        result.push_str(&format!("│   Index: {idx:<38} │\n"));
    }
    result.push_str(&format!(
        "│   Seeks: {:>6} × {:.1} cost/seek              │\n\
         │   Output: {:>38}│\n\
         │ Total: {:>42}│\n\
         └─────────────────────────────────────────────────┘\n",
        format_rows_human(eval.seeks),
        eval.cost_per_seek,
        format_rows_human(eval.output_rows),
        format_cost_colored(eval.total_cost)
    ));
    if eval.is_new_best {
        result.push_str(&format!("  {} NEW BEST\n", good("✓")));
    }
    result
}

// ============================================================================
// Index Selection Explanation
// ============================================================================

/// Index candidate info
pub struct IndexCandidate {
    pub name: String,
    pub access_type: String,
    pub usable_constraints: Vec<String>,
    pub total_cost: f64,
    pub output_rows: f64,
    pub selected: bool,
    pub rejection_reason: Option<String>,
}

/// Format index selection explanation
pub fn format_index_selection(table_name: &str, candidates: &[IndexCandidate]) -> String {
    let mut result = format!(
        "\n╔═══════════════════════════════════════════════╗\n\
         ║  Index Selection: {table_name:<27} ║\n\
         ╚═══════════════════════════════════════════════╝\n\n"
    );

    for (i, c) in candidates.iter().enumerate() {
        result.push_str(&format!("  Option {}: {}\n", i + 1, c.name));
        result.push_str(&format!("  ├─ Type: {}\n", c.access_type));
        if !c.usable_constraints.is_empty() {
            result.push_str("  ├─ Constraints:\n");
            for cstr in &c.usable_constraints {
                result.push_str(&format!("  │   • {cstr}\n"));
            }
        }
        result.push_str(&format!(
            "  ├─ Cost: {}\n",
            format_cost_colored(c.total_cost)
        ));
        result.push_str(&format!(
            "  ├─ Output: {} rows\n",
            format_rows_human(c.output_rows)
        ));
        if c.selected {
            result.push_str(&format!("  └─ {} SELECTED\n\n", good("✓")));
        } else {
            let reason = c.rejection_reason.as_deref().unwrap_or("not optimal");
            result.push_str(&format!("  └─ {} {}\n\n", bad("✗"), reason));
        }
    }
    result
}

/// Represents a constraint entry for selectivity breakdown display.
pub struct SelectivityEntry {
    pub display_name: String,
    pub selectivity: f64,
    pub applied: bool,
    pub constraint_type: super::constraints::ConstraintType,
}

/// Format a selectivity breakdown table showing applied and skipped constraints.
/// Output:
/// ```text
/// ┌─ Selectivity Breakdown (lineitem) ──────────────────────┐
/// │ Base rows: 1,000,000                                    │
/// ├─────────────────────────────────────────────────────────┤
/// │ ✓ [SELF] l_commitdate < l_receiptdate           × 0.40  │
/// │ ✓ [LIT]  l_receiptdate >= '1994-01-01'          × 0.40  │
/// │ ✗ [JOIN] o_orderkey = l_orderkey (prereq: orders)       │
/// ├─────────────────────────────────────────────────────────┤
/// │ Combined: 0.40 × 0.40 = 0.16                            │
/// │ Estimated rows: 1,000,000 × 0.16 = 160,000              │
/// └─────────────────────────────────────────────────────────┘
/// ```
pub fn format_selectivity_breakdown(
    table_name: &str,
    base_rows: f64,
    entries: &[SelectivityEntry],
    combined_selectivity: f64,
) -> String {
    let mut result = String::new();
    
    // Header
    result.push_str(&format!(
        "┌─ Selectivity Breakdown ({}) {}\n",
        table(table_name),
        "─".repeat(40_usize.saturating_sub(table_name.len()))
    ));
    
    // Base rows
    result.push_str(&format!("│ Base rows: {}\n", format_rows(base_rows)));
    result.push_str("├─────────────────────────────────────────────────────────┐\n");
    
    // Applied constraints
    let applied: Vec<_> = entries.iter().filter(|e| e.applied).collect();
    let skipped: Vec<_> = entries.iter().filter(|e| !e.applied).collect();
    
    for entry in &applied {
        let type_badge = match entry.constraint_type {
            super::constraints::ConstraintType::Literal => "[LIT] ",
            super::constraints::ConstraintType::SelfConstraint => "[SELF]",
            super::constraints::ConstraintType::JoinConstraint => "[JOIN]",
        };
        result.push_str(&format!(
            "│ {} {} {:40} × {:.2}\n",
            good("✓"),
            type_badge,
            entry.display_name,
            entry.selectivity
        ));
    }
    
    // Skipped constraints
    for entry in &skipped {
        let type_badge = match entry.constraint_type {
            super::constraints::ConstraintType::Literal => "[LIT] ",
            super::constraints::ConstraintType::SelfConstraint => "[SELF]",
            super::constraints::ConstraintType::JoinConstraint => "[JOIN]",
        };
        result.push_str(&format!(
            "│ {} {} {} (skipped - prereqs not ready)\n",
            bad("✗"),
            type_badge,
            entry.display_name
        ));
    }
    
    result.push_str("├─────────────────────────────────────────────────────────┤\n");
    
    // Combined selectivity formula
    if !applied.is_empty() {
        let formula: Vec<String> = applied.iter().map(|e| format!("{:.2}", e.selectivity)).collect();
        result.push_str(&format!(
            "│ Combined: {} = {:.4}\n",
            formula.join(" × "),
            combined_selectivity
        ));
    } else {
        result.push_str(&format!("│ Combined: 1.0 (no constraints applied)\n"));
    }
    
    // Estimated rows
    let estimated_rows = base_rows * combined_selectivity;
    result.push_str(&format!(
        "│ Estimated rows: {} × {:.4} = {}\n",
        format_rows(base_rows),
        combined_selectivity,
        format_rows(estimated_rows)
    ));
    result.push_str("└─────────────────────────────────────────────────────────┘\n");
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_operator() {
        assert_eq!(format_operator(&turso_parser::ast::Operator::Equals), "EQ");
        assert_eq!(format_operator(&turso_parser::ast::Operator::Greater), "GT");
    }

    #[test]
    fn test_tree_prefix() {
        // Without colors, should produce plain text
        let prefix = tree_prefix(1, false, "");
        assert!(prefix.contains(tree_chars::BRANCH) || prefix.contains("|--"));
    }
}
