//! Generic AST traversal for generated SQL statements.

use crate::ast;

#[derive(Clone, Copy)]
pub struct ExprContext {
    pub in_order_by: bool,
}

impl ExprContext {
    const DEFAULT: Self = Self { in_order_by: false };
    const ORDER_BY: Self = Self { in_order_by: true };
}

pub trait AstVisitor {
    fn table(&mut self, _table: &str) {}
    fn with_clause(&mut self, _with: &ast::WithClause) {}
    fn select(&mut self, _select: &ast::SelectStmt) {}
    fn compound_arm(&mut self, _arm: &ast::CompoundSelectArm) {}
    fn join(&mut self, _join: &ast::JoinClause) {}
    fn expr(&mut self, _expr: &ast::Expr, _context: ExprContext) {}
}

pub fn walk_stmt(stmt: &ast::Stmt, visitor: &mut impl AstVisitor) {
    match stmt {
        ast::Stmt::Select(select) => walk_select(select, visitor),
        ast::Stmt::Insert(insert) => walk_insert(insert, visitor),
        ast::Stmt::Update(update) => walk_update(update, visitor),
        ast::Stmt::Delete(delete) => walk_delete(delete, visitor),
        ast::Stmt::CreateTable(create_table) => {
            visitor.table(&create_table.table);
            for column in &create_table.columns {
                if let Some(default) = &column.default {
                    walk_expr(default, ExprContext::DEFAULT, visitor);
                }
                if let Some(check) = &column.check {
                    walk_expr(check, ExprContext::DEFAULT, visitor);
                }
            }
        }
        ast::Stmt::DropTable(drop_table) => visitor.table(&drop_table.table),
        ast::Stmt::AlterTable(alter_table) => {
            visitor.table(&alter_table.table);
            if let ast::AlterTableAction::AddColumn(column) = &alter_table.action {
                if let Some(default) = &column.default {
                    walk_expr(default, ExprContext::DEFAULT, visitor);
                }
                if let Some(check) = &column.check {
                    walk_expr(check, ExprContext::DEFAULT, visitor);
                }
            }
        }
        ast::Stmt::CreateIndex(create_index) => visitor.table(&create_index.table),
        ast::Stmt::PragmaForeignKeyList(pragma) => visitor.table(&pragma.table),
        ast::Stmt::CreateTrigger(create_trigger) => {
            visitor.table(&create_trigger.table);
            if let Some(when_clause) = &create_trigger.when_clause {
                walk_expr(when_clause, ExprContext::DEFAULT, visitor);
            }
            for stmt in &create_trigger.body {
                walk_trigger_stmt(stmt, visitor);
            }
        }
        ast::Stmt::DropIndex(_)
        | ast::Stmt::OptimizeIndex(_)
        | ast::Stmt::DropTrigger(_)
        | ast::Stmt::Begin
        | ast::Stmt::Commit
        | ast::Stmt::Rollback
        | ast::Stmt::CreateView
        | ast::Stmt::DropView
        | ast::Stmt::Vacuum
        | ast::Stmt::Reindex
        | ast::Stmt::Analyze
        | ast::Stmt::Savepoint(_)
        | ast::Stmt::Release(_) => {}
    }
}

fn walk_trigger_stmt(stmt: &ast::TriggerStmt, visitor: &mut impl AstVisitor) {
    match stmt {
        ast::TriggerStmt::Insert(insert) => walk_insert(insert, visitor),
        ast::TriggerStmt::Update(update) => walk_update(update, visitor),
        ast::TriggerStmt::Delete(delete) => walk_delete(delete, visitor),
        ast::TriggerStmt::Select(select) => walk_select(select, visitor),
    }
}

fn walk_insert(insert: &ast::InsertStmt, visitor: &mut impl AstVisitor) {
    visitor.table(&insert.table);
    if let Some(with) = &insert.with_clause {
        walk_with(with, visitor);
    }
    for row in &insert.values {
        for expr in row {
            walk_expr(expr, ExprContext::DEFAULT, visitor);
        }
    }
}

fn walk_update(update: &ast::UpdateStmt, visitor: &mut impl AstVisitor) {
    visitor.table(&update.table);
    if let Some(with) = &update.with_clause {
        walk_with(with, visitor);
    }
    if let Some(from) = &update.from {
        visitor.table(&from.table);
    }
    for join in &update.joins {
        walk_join(join, visitor);
    }
    for (_, expr) in &update.sets {
        walk_expr(expr, ExprContext::DEFAULT, visitor);
    }
    if let Some(where_clause) = &update.where_clause {
        walk_expr(where_clause, ExprContext::DEFAULT, visitor);
    }
    if let Some(returning) = &update.returning {
        for expr in returning {
            walk_expr(expr, ExprContext::DEFAULT, visitor);
        }
    }
}

fn walk_delete(delete: &ast::DeleteStmt, visitor: &mut impl AstVisitor) {
    visitor.table(&delete.table);
    if let Some(with) = &delete.with_clause {
        walk_with(with, visitor);
    }
    if let Some(where_clause) = &delete.where_clause {
        walk_expr(where_clause, ExprContext::DEFAULT, visitor);
    }
}

fn walk_with(with: &ast::WithClause, visitor: &mut impl AstVisitor) {
    visitor.with_clause(with);
    for cte in &with.ctes {
        walk_select(&cte.query, visitor);
    }
}

fn walk_select(select: &ast::SelectStmt, visitor: &mut impl AstVisitor) {
    visitor.select(select);
    if let Some(with) = &select.with_clause {
        walk_with(with, visitor);
    }
    if let Some(from) = &select.from {
        visitor.table(&from.table);
    }
    for join in &select.joins {
        walk_join(join, visitor);
    }
    for column in &select.columns {
        walk_expr(&column.expr, ExprContext::DEFAULT, visitor);
    }
    if let Some(where_clause) = &select.where_clause {
        walk_expr(where_clause, ExprContext::DEFAULT, visitor);
    }
    if let Some(group_by) = &select.group_by {
        for expr in &group_by.exprs {
            walk_expr(expr, ExprContext::DEFAULT, visitor);
        }
        if let Some(having) = &group_by.having {
            walk_expr(having, ExprContext::DEFAULT, visitor);
        }
    }
    for arm in &select.compounds {
        visitor.compound_arm(arm);
        if let Some(from) = &arm.from {
            visitor.table(&from.table);
        }
        for column in &arm.columns {
            walk_expr(&column.expr, ExprContext::DEFAULT, visitor);
        }
        if let Some(where_clause) = &arm.where_clause {
            walk_expr(where_clause, ExprContext::DEFAULT, visitor);
        }
    }
    for order_by in &select.order_by {
        walk_expr(&order_by.expr, ExprContext::ORDER_BY, visitor);
    }
}

fn walk_join(join: &ast::JoinClause, visitor: &mut impl AstVisitor) {
    visitor.join(join);
    visitor.table(&join.table);
    if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
        walk_expr(expr, ExprContext::DEFAULT, visitor);
    }
}

fn walk_expr(expr: &ast::Expr, context: ExprContext, visitor: &mut impl AstVisitor) {
    visitor.expr(expr, context);
    match expr {
        ast::Expr::ColumnRef(_) | ast::Expr::Literal(_) => {}
        ast::Expr::BinaryOp(binary) => {
            walk_expr(&binary.left, context, visitor);
            walk_expr(&binary.right, context, visitor);
        }
        ast::Expr::UnaryOp(unary) => {
            walk_expr(&unary.operand, context, visitor);
        }
        ast::Expr::FunctionCall(function) => {
            for arg in &function.args {
                walk_expr(arg, context, visitor);
            }
            if let Some(filter) = &function.filter {
                walk_expr(filter, context, visitor);
            }
        }
        ast::Expr::FtsMatch(fts) => {
            walk_expr(&fts.query, ExprContext::DEFAULT, visitor);
        }
        ast::Expr::Subquery(select) => walk_select(select, visitor),
        ast::Expr::Case(case_expr) => {
            if let Some(operand) = &case_expr.operand {
                walk_expr(operand, context, visitor);
            }
            for (when_expr, then_expr) in &case_expr.when_clauses {
                walk_expr(when_expr, context, visitor);
                walk_expr(then_expr, context, visitor);
            }
            if let Some(else_clause) = &case_expr.else_clause {
                walk_expr(else_clause, context, visitor);
            }
        }
        ast::Expr::Cast(cast) => {
            walk_expr(&cast.expr, context, visitor);
        }
        ast::Expr::Between(between) => {
            walk_expr(&between.expr, context, visitor);
            walk_expr(&between.low, context, visitor);
            walk_expr(&between.high, context, visitor);
        }
        ast::Expr::InList(in_list) => {
            walk_expr(&in_list.expr, context, visitor);
            for item in &in_list.list {
                walk_expr(item, context, visitor);
            }
        }
        ast::Expr::InSubquery(in_subquery) => {
            walk_expr(&in_subquery.expr, context, visitor);
            walk_select(&in_subquery.subquery, visitor);
        }
        ast::Expr::IsNull(is_null) => {
            walk_expr(&is_null.expr, context, visitor);
        }
        ast::Expr::Exists(exists) => {
            walk_select(&exists.subquery, visitor);
        }
        ast::Expr::Parenthesized(inner) => walk_expr(inner, context, visitor),
        ast::Expr::ArrayLiteral(array) => {
            for element in &array.elements {
                walk_expr(element, context, visitor);
            }
        }
        ast::Expr::ArraySubscript(subscript) => {
            walk_expr(&subscript.array, context, visitor);
            walk_expr(&subscript.index, context, visitor);
        }
        ast::Expr::WindowFunction(_) | ast::Expr::Collate(_) | ast::Expr::Raise(_) => {}
    }
}
