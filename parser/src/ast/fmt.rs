//! AST node format
use std::fmt::{self, Display, Formatter, Write};

use crate::ast::*;
use crate::token::TokenType;
use crate::token::TokenType::*;
use crate::Result;

use crate::ast::TableInternalId;

/// Context to be used in ToSqlString
pub trait ToSqlContext {
    /// Given an id, get the table name
    /// First Option indicates whether the table exists
    ///
    /// Currently not considering aliases
    fn get_table_name(&self, _id: TableInternalId) -> Option<&str> {
        None
    }

    /// Given a table id and a column index, get the column name
    /// First Option indicates whether the column exists
    /// Second Option indicates whether the column has a name
    fn get_column_name(&self, _table_id: TableInternalId, _col_idx: usize) -> Option<Option<&str>> {
        None
    }

    // help function to handle missing table/column names
    fn get_table_and_column_names(
        &self,
        table_id: TableInternalId,
        col_idx: usize,
    ) -> (String, String) {
        let table_name = self
            .get_table_name(table_id)
            .map(|s| s.to_owned())
            .unwrap_or_else(|| format!("t{}", table_id.0));

        let column_name = self
            .get_column_name(table_id, col_idx)
            .map(|opt| {
                opt.map(|s| s.to_owned())
                    .unwrap_or_else(|| format!("c{col_idx}"))
            })
            .unwrap_or_else(|| format!("c{col_idx}"));

        (table_name, column_name)
    }
}

pub struct WriteTokenStream<'a, T: Write> {
    write: &'a mut T,
    spaced: bool,
}

impl<'a, T: Write> WriteTokenStream<'a, T> {
    /// Create a new token stream writing to the specified writer
    pub fn new(write: &'a mut T) -> Self {
        Self {
            write,
            spaced: true,
        }
    }
}

impl<T: Write> TokenStream for WriteTokenStream<'_, T> {
    type Error = fmt::Error;

    fn append(&mut self, ty: TokenType, value: Option<&str>) -> fmt::Result {
        if !self.spaced {
            match ty {
                TK_COMMA | TK_SEMI | TK_RP | TK_DOT => {}
                _ => {
                    self.write.write_char(' ')?;
                    self.spaced = true;
                }
            };
        }

        match (ty, ty.as_str(), value) {
            (TK_BLOB, None, value) => {
                self.write.write_char('X')?;
                self.write.write_char('\'')?;
                if let Some(str) = value {
                    self.write.write_str(str)?;
                }
                self.write.write_char('\'')?;
                Ok(())
            }
            (_, ty_str, value) => {
                if let Some(str) = ty_str {
                    self.write.write_str(str)?;
                    self.spaced = ty == TK_LP || ty == TK_DOT; // str should not be whitespace
                }

                if let Some(str) = value {
                    // trick for pretty-print
                    self.spaced = str.bytes().all(|b| b.is_ascii_whitespace());
                    self.write.write_str(str)?;
                }

                Ok(())
            }
        }
    }
}

pub struct BlankContext;

impl ToSqlContext for BlankContext {}

/// Stream of token
pub trait TokenStream {
    /// Potential error raised
    type Error;

    /// Push token to this stream
    fn append(&mut self, ty: TokenType, value: Option<&str>) -> Result<(), Self::Error>;

    /// Interspace iterator with commas
    fn comma<I, C: ToSqlContext>(&mut self, items: I, context: &C) -> Result<(), Self::Error>
    where
        I: IntoIterator,
        I::Item: ToTokens,
    {
        let iter = items.into_iter();
        for (i, item) in iter.enumerate() {
            if i != 0 {
                self.append(TK_COMMA, None)?;
            }
            item.to_tokens(self, context)?;
        }
        Ok(())
    }
}

pub struct SqlDisplayer<'a, 'b, C: ToSqlContext, T: ToTokens> {
    ctx: &'a C,
    start_node: &'b T,
}

impl<'a, 'b, C: ToSqlContext, T: ToTokens> SqlDisplayer<'a, 'b, C, T> {
    /// Create a new displayer with the specified context and AST node
    pub fn new(ctx: &'a C, start_node: &'b T) -> Self {
        Self { ctx, start_node }
    }
}

impl<'a, 'b, C: ToSqlContext, T: ToTokens> Display for SqlDisplayer<'a, 'b, C, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut stream = WriteTokenStream::new(f);
        self.start_node.to_tokens(&mut stream, self.ctx)
    }
}

/// Generate token(s) from AST node
/// Also implements Display to make sure devs won't forget Display
pub trait ToTokens: Display {
    /// Send token(s) to the specified stream with context
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error>;

    // Return displayer representation with context
    fn displayer<'a, 'b, C: ToSqlContext>(&'b self, ctx: &'a C) -> SqlDisplayer<'a, 'b, C, Self>
    where
        Self: Sized,
    {
        SqlDisplayer::new(ctx, self)
    }
}

macro_rules! impl_display_for_to_tokens {
    ($struct:ty) => {
        impl Display for $struct {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                self.displayer(&BlankContext).fmt(f)
            }
        }
    };
}

impl<T: ?Sized + ToTokens> ToTokens for &T {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        ToTokens::to_tokens(&**self, s, context)
    }
}

impl<T: ?Sized + ToTokens> ToTokens for Box<T> {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        ToTokens::to_tokens(self.as_ref(), s, context)
    }
}

impl ToTokens for String {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_ANY, Some(self.as_ref()))
    }
}

impl_display_for_to_tokens!(Cmd);
impl ToTokens for Cmd {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Explain(stmt) => {
                s.append(TK_EXPLAIN, None)?;
                stmt.to_tokens(s, context)?;
            }
            Self::ExplainQueryPlan(stmt) => {
                s.append(TK_EXPLAIN, None)?;
                s.append(TK_QUERY, None)?;
                s.append(TK_PLAN, None)?;
                stmt.to_tokens(s, context)?;
            }
            Self::Stmt(stmt) => {
                stmt.to_tokens(s, context)?;
            }
        }
        s.append(TK_SEMI, None)
    }
}

impl_display_for_to_tokens!(Stmt);
impl ToTokens for Stmt {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::AlterTable(AlterTable { name, body }) => {
                s.append(TK_ALTER, None)?;
                s.append(TK_TABLE, None)?;
                name.to_tokens(s, context)?;
                body.to_tokens(s, context)
            }
            Self::Analyze { name } => {
                s.append(TK_ANALYZE, None)?;
                if let Some(name) = name {
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Attach { expr, db_name, key } => {
                s.append(TK_ATTACH, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_AS, None)?;
                db_name.to_tokens(s, context)?;
                if let Some(key) = key {
                    s.append(TK_KEY, None)?;
                    key.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Begin { typ, name } => {
                s.append(TK_BEGIN, None)?;
                if let Some(typ) = typ {
                    typ.to_tokens(s, context)?;
                }
                if let Some(name) = name {
                    s.append(TK_TRANSACTION, None)?;
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Commit { name } => {
                s.append(TK_COMMIT, None)?;
                if let Some(name) = name {
                    s.append(TK_TRANSACTION, None)?;
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::CreateIndex {
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                columns,
                where_clause,
            } => {
                s.append(TK_CREATE, None)?;
                if *unique {
                    s.append(TK_UNIQUE, None)?;
                }
                s.append(TK_INDEX, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                idx_name.to_tokens(s, context)?;
                s.append(TK_ON, None)?;
                tbl_name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                s.append(TK_RP, None)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_TABLE, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s, context)?;
                body.to_tokens(s, context)
            }
            Self::CreateTrigger {
                temporary,
                if_not_exists,
                trigger_name,
                time,
                event,
                tbl_name,
                for_each_row,
                when_clause,
                commands,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_TRIGGER, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                trigger_name.to_tokens(s, context)?;
                if let Some(time) = time {
                    time.to_tokens(s, context)?;
                }
                event.to_tokens(s, context)?;
                s.append(TK_ON, None)?;
                tbl_name.to_tokens(s, context)?;
                if *for_each_row {
                    s.append(TK_FOR, None)?;
                    s.append(TK_EACH, None)?;
                    s.append(TK_ROW, None)?;
                }
                if let Some(when_clause) = when_clause {
                    s.append(TK_WHEN, None)?;
                    when_clause.to_tokens(s, context)?;
                }
                s.append(TK_BEGIN, Some("\n"))?;
                for command in commands {
                    command.to_tokens(s, context)?;
                    s.append(TK_SEMI, Some("\n"))?;
                }
                s.append(TK_END, None)
            }
            Self::CreateView {
                temporary,
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_VIEW, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s, context)?;
                if !columns.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(columns, s, context)?;
                    s.append(TK_RP, None)?;
                }
                s.append(TK_AS, None)?;
                select.to_tokens(s, context)
            }
            Self::CreateMaterializedView {
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                s.append(TK_CREATE, None)?;
                s.append(TK_MATERIALIZED, None)?;
                s.append(TK_VIEW, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s, context)?;
                if !columns.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(columns, s, context)?;
                    s.append(TK_RP, None)?;
                }
                s.append(TK_AS, None)?;
                select.to_tokens(s, context)
            }
            Self::CreateVirtualTable(CreateVirtualTable {
                if_not_exists,
                tbl_name,
                module_name,
                args,
            }) => {
                s.append(TK_CREATE, None)?;
                s.append(TK_VIRTUAL, None)?;
                s.append(TK_TABLE, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s, context)?;
                s.append(TK_USING, None)?;
                module_name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                if !args.is_empty() {
                    comma(args, s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::Delete {
                with,
                tbl_name,
                indexed,
                where_clause,
                returning,
                order_by,
                limit,
            } => {
                if let Some(with) = with {
                    with.to_tokens(s, context)?;
                }
                s.append(TK_DELETE, None)?;
                s.append(TK_FROM, None)?;
                tbl_name.to_tokens(s, context)?;
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                if !order_by.is_empty() {
                    s.append(TK_ORDER, None)?;
                    s.append(TK_BY, None)?;
                    comma(order_by, s, context)?;
                }
                if let Some(limit) = limit {
                    limit.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Detach { name } => {
                s.append(TK_DETACH, None)?;
                name.to_tokens(s, context)
            }
            Self::DropIndex {
                if_exists,
                idx_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_INDEX, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                idx_name.to_tokens(s, context)
            }
            Self::DropTable {
                if_exists,
                tbl_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_TABLE, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s, context)
            }
            Self::DropTrigger {
                if_exists,
                trigger_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_TRIGGER, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                trigger_name.to_tokens(s, context)
            }
            Self::DropView {
                if_exists,
                view_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_VIEW, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s, context)
            }
            Self::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } => {
                if let Some(with) = with {
                    with.to_tokens(s, context)?;
                }
                if let Some(ResolveType::Replace) = or_conflict {
                    s.append(TK_REPLACE, None)?;
                } else {
                    s.append(TK_INSERT, None)?;
                    if let Some(or_conflict) = or_conflict {
                        s.append(TK_OR, None)?;
                        or_conflict.to_tokens(s, context)?;
                    }
                }
                s.append(TK_INTO, None)?;
                tbl_name.to_tokens(s, context)?;
                if !columns.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(columns, s, context)?;
                    s.append(TK_RP, None)?;
                }
                body.to_tokens(s, context)?;
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Pragma { name, body } => {
                s.append(TK_PRAGMA, None)?;
                name.to_tokens(s, context)?;
                if let Some(body) = body {
                    body.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Reindex { name } => {
                s.append(TK_REINDEX, None)?;
                if let Some(name) = name {
                    name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Release { name } => {
                s.append(TK_RELEASE, None)?;
                name.to_tokens(s, context)
            }
            Self::Rollback {
                tx_name,
                savepoint_name,
            } => {
                s.append(TK_ROLLBACK, None)?;
                if let Some(tx_name) = tx_name {
                    s.append(TK_TRANSACTION, None)?;
                    tx_name.to_tokens(s, context)?;
                }
                if let Some(savepoint_name) = savepoint_name {
                    s.append(TK_TO, None)?;
                    savepoint_name.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Savepoint { name } => {
                s.append(TK_SAVEPOINT, None)?;
                name.to_tokens(s, context)
            }
            Self::Select(select) => select.to_tokens(s, context),
            Self::Update(Update {
                with,
                or_conflict,
                tbl_name,
                indexed,
                sets,
                from,
                where_clause,
                returning,
                order_by,
                limit,
            }) => {
                if let Some(with) = with {
                    with.to_tokens(s, context)?;
                }
                s.append(TK_UPDATE, None)?;
                if let Some(or_conflict) = or_conflict {
                    s.append(TK_OR, None)?;
                    or_conflict.to_tokens(s, context)?;
                }
                tbl_name.to_tokens(s, context)?;
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s, context)?;
                }
                s.append(TK_SET, None)?;
                comma(sets, s, context)?;
                if let Some(from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                if !order_by.is_empty() {
                    s.append(TK_ORDER, None)?;
                    s.append(TK_BY, None)?;
                    comma(order_by, s, context)?;
                }
                if let Some(limit) = limit {
                    limit.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Vacuum { name, into } => {
                s.append(TK_VACUUM, None)?;
                if let Some(ref name) = name {
                    name.to_tokens(s, context)?;
                }
                if let Some(ref into) = into {
                    s.append(TK_INTO, None)?;
                    into.to_tokens(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(Expr);
impl ToTokens for Expr {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Between {
                lhs,
                not,
                start,
                end,
            } => {
                lhs.to_tokens(s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_BETWEEN, None)?;
                start.to_tokens(s, context)?;
                s.append(TK_AND, None)?;
                end.to_tokens(s, context)
            }
            Self::Binary(lhs, op, rhs) => {
                lhs.to_tokens(s, context)?;
                op.to_tokens(s, context)?;
                rhs.to_tokens(s, context)
            }
            Self::Register(reg) => {
                // This is for internal use only, not part of SQL syntax
                // Use a special notation that won't conflict with SQL
                s.append(TK_VARIABLE, Some(&format!("$r{reg}")))
            }
            Self::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                s.append(TK_CASE, None)?;
                if let Some(ref base) = base {
                    base.to_tokens(s, context)?;
                }
                for (when, then) in when_then_pairs {
                    s.append(TK_WHEN, None)?;
                    when.to_tokens(s, context)?;
                    s.append(TK_THEN, None)?;
                    then.to_tokens(s, context)?;
                }
                if let Some(ref else_expr) = else_expr {
                    s.append(TK_ELSE, None)?;
                    else_expr.to_tokens(s, context)?;
                }
                s.append(TK_END, None)
            }
            Self::Cast { expr, type_name } => {
                s.append(TK_CAST, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_AS, None)?;
                if let Some(ref type_name) = type_name {
                    type_name.to_tokens(s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::Collate(expr, collation) => {
                expr.to_tokens(s, context)?;
                s.append(TK_COLLATE, None)?;
                s.append(TK_ID, Some(&collation.as_ident()))
            }
            Self::DoublyQualified(db_name, tbl_name, col_name) => {
                db_name.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                tbl_name.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                col_name.to_tokens(s, context)
            }
            Self::Exists(subquery) => {
                s.append(TK_EXISTS, None)?;
                s.append(TK_LP, None)?;
                subquery.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                filter_over,
            } => {
                name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                if let Some(distinctness) = distinctness {
                    distinctness.to_tokens(s, context)?;
                }
                if !args.is_empty() {
                    comma(args, s, context)?;
                }
                if !order_by.is_empty() {
                    s.append(TK_ORDER, None)?;
                    s.append(TK_BY, None)?;
                    comma(order_by, s, context)?;
                }
                s.append(TK_RP, None)?;
                filter_over.to_tokens(s, context)?;
                Ok(())
            }
            Self::FunctionCallStar { name, filter_over } => {
                name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                s.append(TK_STAR, None)?;
                s.append(TK_RP, None)?;
                filter_over.to_tokens(s, context)?;
                Ok(())
            }
            Self::Id(id) => id.to_tokens(s, context),
            Self::Column { table, column, .. } => {
                let (tbl_name, col_name) = context.get_table_and_column_names(*table, *column);
                s.append(TK_ID, Some(tbl_name.as_ref()))?;
                s.append(TK_DOT, None)?;
                s.append(TK_ID, Some(col_name.as_ref()))
            }
            Self::InList { lhs, not, rhs } => {
                lhs.to_tokens(s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                s.append(TK_LP, None)?;
                if !rhs.is_empty() {
                    comma(rhs, s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::InSelect { lhs, not, rhs } => {
                lhs.to_tokens(s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                s.append(TK_LP, None)?;
                rhs.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::InTable {
                lhs,
                not,
                rhs,
                args,
            } => {
                lhs.to_tokens(s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                rhs.to_tokens(s, context)?;
                if !args.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(args, s, context)?;
                    s.append(TK_RP, None)?;
                }
                Ok(())
            }
            Self::IsNull(sub_expr) => {
                sub_expr.to_tokens(s, context)?;
                s.append(TK_ISNULL, None)
            }
            Self::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => {
                lhs.to_tokens(s, context)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                op.to_tokens(s, context)?;
                rhs.to_tokens(s, context)?;
                if let Some(escape) = escape {
                    s.append(TK_ESCAPE, None)?;
                    escape.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Literal(lit) => lit.to_tokens(s, context),
            Self::Name(name) => name.to_tokens(s, context),
            Self::NotNull(sub_expr) => {
                sub_expr.to_tokens(s, context)?;
                s.append(TK_NOTNULL, None)
            }
            Self::Parenthesized(exprs) => {
                s.append(TK_LP, None)?;
                comma(exprs, s, context)?;
                s.append(TK_RP, None)
            }
            Self::Qualified(qualifier, qualified) => {
                qualifier.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                qualified.to_tokens(s, context)
            }
            Self::Raise(rt, err) => {
                s.append(TK_RAISE, None)?;
                s.append(TK_LP, None)?;
                rt.to_tokens(s, context)?;
                if let Some(err) = err {
                    s.append(TK_COMMA, None)?;
                    err.to_tokens(s, context)?;
                }
                s.append(TK_RP, None)
            }
            Self::RowId { .. } => Ok(()),
            Self::Subquery(query) => {
                s.append(TK_LP, None)?;
                query.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::Unary(op, sub_expr) => {
                op.to_tokens(s, context)?;
                sub_expr.to_tokens(s, context)
            }
            Self::Variable(var) => match var.chars().next() {
                Some(c) if c == '$' || c == '@' || c == '#' || c == ':' => {
                    s.append(TK_VARIABLE, Some(var))
                }
                Some(_) => s.append(TK_VARIABLE, Some(&("?".to_owned() + var))),
                None => s.append(TK_VARIABLE, Some("?")),
            },
        }
    }
}

impl_display_for_to_tokens!(Literal);
impl ToTokens for Literal {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Numeric(ref num) => s.append(TK_FLOAT, Some(num)), // TODO Validate TK_FLOAT
            Self::String(ref str) => s.append(TK_STRING, Some(str)),
            Self::Blob(ref blob) => s.append(TK_BLOB, Some(blob)),
            Self::Keyword(ref str) => s.append(TK_ID, Some(str)), // TODO Validate TK_ID
            Self::Null => s.append(TK_NULL, None),
            Self::CurrentDate => s.append(TK_CTIME_KW, Some("CURRENT_DATE")),
            Self::CurrentTime => s.append(TK_CTIME_KW, Some("CURRENT_TIME")),
            Self::CurrentTimestamp => s.append(TK_CTIME_KW, Some("CURRENT_TIMESTAMP")),
        }
    }
}

impl_display_for_to_tokens!(LikeOperator);
impl ToTokens for LikeOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            TK_LIKE_KW,
            Some(match self {
                Self::Glob => "GLOB",
                Self::Like => "LIKE",
                Self::Match => "MATCH",
                Self::Regexp => "REGEXP",
            }),
        )
    }
}

impl_display_for_to_tokens!(Operator);
impl ToTokens for Operator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Add => s.append(TK_PLUS, None),
            Self::And => s.append(TK_AND, None),
            Self::ArrowRight => s.append(TK_PTR, Some("->")),
            Self::ArrowRightShift => s.append(TK_PTR, Some("->>")),
            Self::BitwiseAnd => s.append(TK_BITAND, None),
            Self::BitwiseOr => s.append(TK_BITOR, None),
            Self::BitwiseNot => s.append(TK_BITNOT, None),
            Self::Concat => s.append(TK_CONCAT, None),
            Self::Equals => s.append(TK_EQ, None),
            Self::Divide => s.append(TK_SLASH, None),
            Self::Greater => s.append(TK_GT, None),
            Self::GreaterEquals => s.append(TK_GE, None),
            Self::Is => s.append(TK_IS, None),
            Self::IsNot => {
                s.append(TK_IS, None)?;
                s.append(TK_NOT, None)
            }
            Self::LeftShift => s.append(TK_LSHIFT, None),
            Self::Less => s.append(TK_LT, None),
            Self::LessEquals => s.append(TK_LE, None),
            Self::Modulus => s.append(TK_REM, None),
            Self::Multiply => s.append(TK_STAR, None),
            Self::NotEquals => s.append(TK_NE, None),
            Self::Or => s.append(TK_OR, None),
            Self::RightShift => s.append(TK_RSHIFT, None),
            Self::Subtract => s.append(TK_MINUS, None),
        }
    }
}

impl_display_for_to_tokens!(UnaryOperator);
impl ToTokens for UnaryOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::BitwiseNot => TK_BITNOT,
                Self::Negative => TK_MINUS,
                Self::Not => TK_NOT,
                Self::Positive => TK_PLUS,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(Select);
impl ToTokens for Select {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref with) = self.with {
            with.to_tokens(s, context)?;
        }
        self.body.to_tokens(s, context)?;
        if !self.order_by.is_empty() {
            s.append(TK_ORDER, None)?;
            s.append(TK_BY, None)?;
            comma(&self.order_by, s, context)?;
        }
        if let Some(ref limit) = self.limit {
            limit.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(SelectBody);
impl ToTokens for SelectBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.select.to_tokens(s, context)?;
        for compound in &self.compounds {
            compound.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(CompoundSelect);
impl ToTokens for CompoundSelect {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.operator.to_tokens(s, context)?;
        self.select.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(CompoundOperator);
impl ToTokens for CompoundOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Union => s.append(TK_UNION, None),
            Self::UnionAll => {
                s.append(TK_UNION, None)?;
                s.append(TK_ALL, None)
            }
            Self::Except => s.append(TK_EXCEPT, None),
            Self::Intersect => s.append(TK_INTERSECT, None),
        }
    }
}

impl_display_for_to_tokens!(OneSelect);
impl ToTokens for OneSelect {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Select {
                distinctness,
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
            } => {
                s.append(TK_SELECT, None)?;
                if let Some(ref distinctness) = distinctness {
                    distinctness.to_tokens(s, context)?;
                }
                comma(columns, s, context)?;
                if let Some(ref from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s, context)?;
                }
                if let Some(ref where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                if let Some(ref group_by) = group_by {
                    group_by.to_tokens(s, context)?;
                }
                if !window_clause.is_empty() {
                    s.append(TK_WINDOW, None)?;
                    comma(window_clause, s, context)?;
                }
                Ok(())
            }
            Self::Values(values) => {
                for (i, vals) in values.iter().enumerate() {
                    if i == 0 {
                        s.append(TK_VALUES, None)?;
                    } else {
                        s.append(TK_COMMA, None)?;
                    }
                    s.append(TK_LP, None)?;
                    comma(vals, s, context)?;
                    s.append(TK_RP, None)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(FromClause);
impl ToTokens for FromClause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.select.as_ref().to_tokens(s, context)?;
        for join in &self.joins {
            join.to_tokens(s, context)?;
        }

        Ok(())
    }
}

impl_display_for_to_tokens!(Distinctness);
impl ToTokens for Distinctness {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Distinct => TK_DISTINCT,
                Self::All => TK_ALL,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(ResultColumn);
impl ToTokens for ResultColumn {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Expr(expr, alias) => {
                expr.to_tokens(s, context)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Star => s.append(TK_STAR, None),
            Self::TableStar(tbl_name) => {
                tbl_name.to_tokens(s, context)?;
                s.append(TK_DOT, None)?;
                s.append(TK_STAR, None)
            }
        }
    }
}

impl_display_for_to_tokens!(As);
impl ToTokens for As {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::As(ref name) => {
                s.append(TK_AS, None)?;
                name.to_tokens(s, context)
            }
            Self::Elided(ref name) => name.to_tokens(s, context),
        }
    }
}

impl_display_for_to_tokens!(JoinedSelectTable);
impl ToTokens for JoinedSelectTable {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.operator.to_tokens(s, context)?;
        self.table.to_tokens(s, context)?;
        if let Some(ref constraint) = self.constraint {
            constraint.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(SelectTable);
impl ToTokens for SelectTable {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Table(name, alias, indexed) => {
                name.to_tokens(s, context)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::TableCall(name, exprs, alias) => {
                name.to_tokens(s, context)?;
                s.append(TK_LP, None)?;
                comma(exprs, s, context)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Select(select, alias) => {
                s.append(TK_LP, None)?;
                select.to_tokens(s, context)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Sub(from, alias) => {
                s.append(TK_LP, None)?;
                from.to_tokens(s, context)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(JoinOperator);
impl ToTokens for JoinOperator {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Comma => s.append(TK_COMMA, None),
            Self::TypedJoin(join_type) => {
                if let Some(ref join_type) = join_type {
                    join_type.to_tokens(s, context)?;
                }
                s.append(TK_JOIN, None)
            }
        }
    }
}

impl_display_for_to_tokens!(JoinType);
impl ToTokens for JoinType {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        if self.contains(Self::NATURAL) {
            s.append(TK_JOIN_KW, Some("NATURAL"))?;
        }
        if self.contains(Self::INNER) {
            if self.contains(Self::CROSS) {
                s.append(TK_JOIN_KW, Some("CROSS"))?;
            }
            s.append(TK_JOIN_KW, Some("INNER"))?;
        } else {
            if self.contains(Self::LEFT) {
                if self.contains(Self::RIGHT) {
                    s.append(TK_JOIN_KW, Some("FULL"))?;
                } else {
                    s.append(TK_JOIN_KW, Some("LEFT"))?;
                }
            } else if self.contains(Self::RIGHT) {
                s.append(TK_JOIN_KW, Some("RIGHT"))?;
            }
            if self.contains(Self::OUTER) {
                s.append(TK_JOIN_KW, Some("OUTER"))?;
            }
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(JoinConstraint);
impl ToTokens for JoinConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::On(expr) => {
                s.append(TK_ON, None)?;
                expr.to_tokens(s, context)
            }
            Self::Using(col_names) => {
                s.append(TK_USING, None)?;
                s.append(TK_LP, None)?;
                comma(col_names, s, context)?;
                s.append(TK_RP, None)
            }
        }
    }
}

impl_display_for_to_tokens!(GroupBy);
impl ToTokens for GroupBy {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_GROUP, None)?;
        s.append(TK_BY, None)?;
        comma(&self.exprs, s, context)?;
        if let Some(ref having) = self.having {
            s.append(TK_HAVING, None)?;
            having.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Name);
impl ToTokens for Name {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_ID, Some(&self.as_ident()))
    }
}

impl_display_for_to_tokens!(QualifiedName);
impl ToTokens for QualifiedName {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref db_name) = self.db_name {
            db_name.to_tokens(s, context)?;
            s.append(TK_DOT, None)?;
        }
        self.name.to_tokens(s, context)?;
        if let Some(ref alias) = self.alias {
            s.append(TK_AS, None)?;
            alias.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(AlterTableBody);
impl ToTokens for AlterTableBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::RenameTo(name) => {
                s.append(TK_RENAME, None)?;
                s.append(TK_TO, None)?;
                name.to_tokens(s, context)
            }
            Self::AddColumn(def) => {
                s.append(TK_ADD, None)?;
                s.append(TK_COLUMNKW, None)?;
                def.to_tokens(s, context)
            }
            Self::AlterColumn { old, new } => {
                s.append(TK_ALTER, None)?;
                s.append(TK_COLUMNKW, None)?;
                old.to_tokens(s, context)?;
                s.append(TK_TO, None)?;
                new.to_tokens(s, context)
            }
            Self::RenameColumn { old, new } => {
                s.append(TK_RENAME, None)?;
                s.append(TK_COLUMNKW, None)?;
                old.to_tokens(s, context)?;
                s.append(TK_TO, None)?;
                new.to_tokens(s, context)
            }
            Self::DropColumn(name) => {
                s.append(TK_DROP, None)?;
                s.append(TK_COLUMNKW, None)?;
                name.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(CreateTableBody);
impl ToTokens for CreateTableBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::ColumnsAndConstraints {
                columns,
                constraints,
                options,
            } => {
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                if !constraints.is_empty() {
                    s.append(TK_COMMA, None)?;
                    comma(constraints, s, context)?;
                }
                s.append(TK_RP, None)?;
                if options.contains(TableOptions::WITHOUT_ROWID) {
                    s.append(TK_WITHOUT, None)?;
                    s.append(TK_ID, Some("ROWID"))?;
                }
                if options.contains(TableOptions::STRICT) {
                    s.append(TK_ID, Some("STRICT"))?;
                }
                Ok(())
            }
            Self::AsSelect(select) => {
                s.append(TK_AS, None)?;
                select.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(ColumnDefinition);
impl ToTokens for ColumnDefinition {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.col_name.to_tokens(s, context)?;
        if let Some(ref col_type) = self.col_type {
            col_type.to_tokens(s, context)?;
        }
        for constraint in &self.constraints {
            constraint.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(NamedColumnConstraint);
impl ToTokens for NamedColumnConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref name) = self.name {
            s.append(TK_CONSTRAINT, None)?;
            name.to_tokens(s, context)?;
        }
        self.constraint.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(ColumnConstraint);
impl ToTokens for ColumnConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::PrimaryKey {
                order,
                conflict_clause,
                auto_increment,
            } => {
                s.append(TK_PRIMARY, None)?;
                s.append(TK_KEY, None)?;
                if let Some(order) = order {
                    order.to_tokens(s, context)?;
                }
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                if *auto_increment {
                    s.append(TK_AUTOINCR, None)?;
                }
                Ok(())
            }
            Self::NotNull {
                nullable,
                conflict_clause,
            } => {
                if !nullable {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_NULL, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Unique(conflict_clause) => {
                s.append(TK_UNIQUE, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Check(expr) => {
                s.append(TK_CHECK, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::Default(expr) => {
                s.append(TK_DEFAULT, None)?;
                expr.to_tokens(s, context)
            }
            Self::Collate { collation_name } => {
                s.append(TK_COLLATE, None)?;
                collation_name.to_tokens(s, context)
            }
            Self::ForeignKey {
                clause,
                defer_clause,
            } => {
                s.append(TK_REFERENCES, None)?;
                clause.to_tokens(s, context)?;
                if let Some(defer_clause) = defer_clause {
                    defer_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Generated { expr, typ } => {
                s.append(TK_AS, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_RP, None)?;
                if let Some(typ) = typ {
                    typ.to_tokens(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(NamedTableConstraint);
impl ToTokens for NamedTableConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref name) = self.name {
            s.append(TK_CONSTRAINT, None)?;
            name.to_tokens(s, context)?;
        }
        self.constraint.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(TableConstraint);
impl ToTokens for TableConstraint {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::PrimaryKey {
                columns,
                auto_increment,
                conflict_clause,
            } => {
                s.append(TK_PRIMARY, None)?;
                s.append(TK_KEY, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                if *auto_increment {
                    s.append(TK_AUTOINCR, None)?;
                }
                s.append(TK_RP, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Unique {
                columns,
                conflict_clause,
            } => {
                s.append(TK_UNIQUE, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                s.append(TK_RP, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Check(expr) => {
                s.append(TK_CHECK, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
            Self::ForeignKey {
                columns,
                clause,
                defer_clause,
            } => {
                s.append(TK_FOREIGN, None)?;
                s.append(TK_KEY, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s, context)?;
                s.append(TK_RP, None)?;
                s.append(TK_REFERENCES, None)?;
                clause.to_tokens(s, context)?;
                if let Some(defer_clause) = defer_clause {
                    defer_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_to_tokens!(SortOrder);
impl ToTokens for SortOrder {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Asc => TK_ASC,
                Self::Desc => TK_DESC,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(NullsOrder);
impl ToTokens for NullsOrder {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_NULLS, None)?;
        s.append(
            match self {
                Self::First => TK_FIRST,
                Self::Last => TK_LAST,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(ForeignKeyClause);
impl ToTokens for ForeignKeyClause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.tbl_name.to_tokens(s, context)?;
        if !self.columns.is_empty() {
            s.append(TK_LP, None)?;
            comma(&self.columns, s, context)?;
            s.append(TK_RP, None)?;
        }
        for arg in &self.args {
            arg.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(RefArg);
impl ToTokens for RefArg {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::OnDelete(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_DELETE, None)?;
                action.to_tokens(s, context)
            }
            Self::OnInsert(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_INSERT, None)?;
                action.to_tokens(s, context)
            }
            Self::OnUpdate(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_UPDATE, None)?;
                action.to_tokens(s, context)
            }
            Self::Match(ref name) => {
                s.append(TK_MATCH, None)?;
                name.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(RefAct);
impl ToTokens for RefAct {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::SetNull => {
                s.append(TK_SET, None)?;
                s.append(TK_NULL, None)
            }
            Self::SetDefault => {
                s.append(TK_SET, None)?;
                s.append(TK_DEFAULT, None)
            }
            Self::Cascade => s.append(TK_CASCADE, None),
            Self::Restrict => s.append(TK_RESTRICT, None),
            Self::NoAction => {
                s.append(TK_NO, None)?;
                s.append(TK_ACTION, None)
            }
        }
    }
}

impl_display_for_to_tokens!(DeferSubclause);
impl ToTokens for DeferSubclause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if !self.deferrable {
            s.append(TK_NOT, None)?;
        }
        s.append(TK_DEFERRABLE, None)?;
        if let Some(init_deferred) = self.init_deferred {
            init_deferred.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(InitDeferredPred);
impl ToTokens for InitDeferredPred {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_INITIALLY, None)?;
        s.append(
            match self {
                Self::InitiallyDeferred => TK_DEFERRED,
                Self::InitiallyImmediate => TK_IMMEDIATE,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(IndexedColumn);
impl ToTokens for IndexedColumn {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.col_name.to_tokens(s, context)?;
        if let Some(ref collation_name) = self.collation_name {
            s.append(TK_COLLATE, None)?;
            collation_name.to_tokens(s, context)?;
        }
        if let Some(order) = self.order {
            order.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Indexed);
impl ToTokens for Indexed {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::IndexedBy(ref name) => {
                s.append(TK_INDEXED, None)?;
                s.append(TK_BY, None)?;
                name.to_tokens(s, context)
            }
            Self::NotIndexed => {
                s.append(TK_NOT, None)?;
                s.append(TK_INDEXED, None)
            }
        }
    }
}

impl_display_for_to_tokens!(SortedColumn);
impl ToTokens for SortedColumn {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.expr.to_tokens(s, context)?;
        if let Some(ref order) = self.order {
            order.to_tokens(s, context)?;
        }
        if let Some(ref nulls) = self.nulls {
            nulls.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Limit);
impl ToTokens for Limit {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_LIMIT, None)?;
        self.expr.to_tokens(s, context)?;
        if let Some(ref offset) = self.offset {
            s.append(TK_OFFSET, None)?;
            offset.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(InsertBody);
impl ToTokens for InsertBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Select(select, upsert) => {
                select.to_tokens(s, context)?;
                if let Some(upsert) = upsert {
                    upsert.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::DefaultValues => {
                s.append(TK_DEFAULT, None)?;
                s.append(TK_VALUES, None)
            }
        }
    }
}

impl_display_for_to_tokens!(Set);
impl ToTokens for Set {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if self.col_names.len() == 1 {
            comma(&self.col_names, s, context)?;
        } else {
            s.append(TK_LP, None)?;
            comma(&self.col_names, s, context)?;
            s.append(TK_RP, None)?;
        }
        s.append(TK_EQ, None)?;
        self.expr.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(PragmaBody);
impl ToTokens for PragmaBody {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Equals(value) => {
                s.append(TK_EQ, None)?;
                value.to_tokens(s, context)
            }
            Self::Call(value) => {
                s.append(TK_LP, None)?;
                value.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
        }
    }
}

impl_display_for_to_tokens!(TriggerTime);
impl ToTokens for TriggerTime {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Before => s.append(TK_BEFORE, None),
            Self::After => s.append(TK_AFTER, None),
            Self::InsteadOf => {
                s.append(TK_INSTEAD, None)?;
                s.append(TK_OF, None)
            }
        }
    }
}

impl_display_for_to_tokens!(TriggerEvent);
impl ToTokens for TriggerEvent {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Delete => s.append(TK_DELETE, None),
            Self::Insert => s.append(TK_INSERT, None),
            Self::Update => s.append(TK_UPDATE, None),
            Self::UpdateOf(ref col_names) => {
                s.append(TK_UPDATE, None)?;
                s.append(TK_OF, None)?;
                comma(col_names, s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(TriggerCmd);
impl ToTokens for TriggerCmd {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Update {
                or_conflict,
                tbl_name,
                sets,
                from,
                where_clause,
            } => {
                s.append(TK_UPDATE, None)?;
                if let Some(or_conflict) = or_conflict {
                    s.append(TK_OR, None)?;
                    or_conflict.to_tokens(s, context)?;
                }
                tbl_name.to_tokens(s, context)?;
                s.append(TK_SET, None)?;
                comma(sets, s, context)?;
                if let Some(from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Insert {
                or_conflict,
                tbl_name,
                col_names,
                select,
                upsert,
                returning,
            } => {
                if let Some(ResolveType::Replace) = or_conflict {
                    s.append(TK_REPLACE, None)?;
                } else {
                    s.append(TK_INSERT, None)?;
                    if let Some(or_conflict) = or_conflict {
                        s.append(TK_OR, None)?;
                        or_conflict.to_tokens(s, context)?;
                    }
                }
                s.append(TK_INTO, None)?;
                tbl_name.to_tokens(s, context)?;
                if !col_names.is_empty() {
                    s.append(TK_LP, None)?;
                    comma(col_names, s, context)?;
                    s.append(TK_RP, None)?;
                }
                select.to_tokens(s, context)?;
                if let Some(upsert) = upsert {
                    upsert.to_tokens(s, context)?;
                }
                if !returning.is_empty() {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Delete {
                tbl_name,
                where_clause,
            } => {
                s.append(TK_DELETE, None)?;
                s.append(TK_FROM, None)?;
                tbl_name.to_tokens(s, context)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Select(select) => select.to_tokens(s, context),
        }
    }
}

impl_display_for_to_tokens!(ResolveType);
impl ToTokens for ResolveType {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Rollback => TK_ROLLBACK,
                Self::Abort => TK_ABORT,
                Self::Fail => TK_FAIL,
                Self::Ignore => TK_IGNORE,
                Self::Replace => TK_REPLACE,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(With);
impl ToTokens for With {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_WITH, None)?;
        if self.recursive {
            s.append(TK_RECURSIVE, None)?;
        }
        comma(&self.ctes, s, context)
    }
}

impl_display_for_to_tokens!(CommonTableExpr);
impl ToTokens for CommonTableExpr {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.tbl_name.to_tokens(s, context)?;
        if !self.columns.is_empty() {
            s.append(TK_LP, None)?;
            comma(&self.columns, s, context)?;
            s.append(TK_RP, None)?;
        }
        s.append(TK_AS, None)?;
        match self.materialized {
            Materialized::Any => {}
            Materialized::Yes => {
                s.append(TK_MATERIALIZED, None)?;
            }
            Materialized::No => {
                s.append(TK_NOT, None)?;
                s.append(TK_MATERIALIZED, None)?;
            }
        };
        s.append(TK_LP, None)?;
        self.select.to_tokens(s, context)?;
        s.append(TK_RP, None)
    }
}

impl_display_for_to_tokens!(Type);
impl ToTokens for Type {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self.size {
            None => s.append(TK_ID, Some(&self.name)),
            Some(ref size) => {
                s.append(TK_ID, Some(&self.name))?; // TODO check there is no forbidden chars
                s.append(TK_LP, None)?;
                size.to_tokens(s, context)?;
                s.append(TK_RP, None)
            }
        }
    }
}

impl_display_for_to_tokens!(TypeSize);
impl ToTokens for TypeSize {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::MaxSize(size) => size.to_tokens(s, context),
            Self::TypeSize(size1, size2) => {
                size1.to_tokens(s, context)?;
                s.append(TK_COMMA, None)?;
                size2.to_tokens(s, context)
            }
        }
    }
}

impl_display_for_to_tokens!(TransactionType);
impl ToTokens for TransactionType {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Deferred => TK_DEFERRED,
                Self::Immediate => TK_IMMEDIATE,
                Self::Exclusive => TK_EXCLUSIVE,
                Self::Concurrent => TK_CONCURRENT,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(Upsert);
impl ToTokens for Upsert {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_ON, None)?;
        s.append(TK_CONFLICT, None)?;
        if let Some(ref index) = self.index {
            index.to_tokens(s, context)?;
        }
        self.do_clause.to_tokens(s, context)?;
        if let Some(ref next) = self.next {
            next.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(UpsertIndex);
impl ToTokens for UpsertIndex {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_LP, None)?;
        comma(&self.targets, s, context)?;
        s.append(TK_RP, None)?;
        if let Some(ref where_clause) = self.where_clause {
            s.append(TK_WHERE, None)?;
            where_clause.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(UpsertDo);
impl ToTokens for UpsertDo {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Set { sets, where_clause } => {
                s.append(TK_DO, None)?;
                s.append(TK_UPDATE, None)?;
                s.append(TK_SET, None)?;
                comma(sets, s, context)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s, context)?;
                }
                Ok(())
            }
            Self::Nothing => {
                s.append(TK_DO, None)?;
                s.append(TK_NOTHING, None)
            }
        }
    }
}

impl_display_for_to_tokens!(FunctionTail);
impl ToTokens for FunctionTail {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if let Some(ref filter_clause) = self.filter_clause {
            s.append(TK_FILTER, None)?;
            s.append(TK_LP, None)?;
            s.append(TK_WHERE, None)?;
            filter_clause.to_tokens(s, context)?;
            s.append(TK_RP, None)?;
        }
        if let Some(ref over_clause) = self.over_clause {
            s.append(TK_OVER, None)?;
            over_clause.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(Over);
impl ToTokens for Over {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Window(ref window) => window.to_tokens(s, context),
            Self::Name(ref name) => name.to_tokens(s, context),
        }
    }
}

impl_display_for_to_tokens!(WindowDef);
impl ToTokens for WindowDef {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.name.to_tokens(s, context)?;
        s.append(TK_AS, None)?;
        self.window.to_tokens(s, context)
    }
}

impl_display_for_to_tokens!(Window);
impl ToTokens for Window {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        s.append(TK_LP, None)?;
        if let Some(ref base) = self.base {
            base.to_tokens(s, context)?;
        }
        if !self.partition_by.is_empty() {
            s.append(TK_PARTITION, None)?;
            s.append(TK_BY, None)?;
            comma(&self.partition_by, s, context)?;
        }
        if !self.order_by.is_empty() {
            s.append(TK_ORDER, None)?;
            s.append(TK_BY, None)?;
            comma(&self.order_by, s, context)?;
        }
        if let Some(ref frame_clause) = self.frame_clause {
            frame_clause.to_tokens(s, context)?;
        }
        s.append(TK_RP, None)
    }
}

impl_display_for_to_tokens!(FrameClause);
impl ToTokens for FrameClause {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        self.mode.to_tokens(s, context)?;
        if let Some(ref end) = self.end {
            s.append(TK_BETWEEN, None)?;
            self.start.to_tokens(s, context)?;
            s.append(TK_AND, None)?;
            end.to_tokens(s, context)?;
        } else {
            self.start.to_tokens(s, context)?;
        }
        if let Some(ref exclude) = self.exclude {
            s.append(TK_EXCLUDE, None)?;
            exclude.to_tokens(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_to_tokens!(FrameMode);
impl ToTokens for FrameMode {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        s.append(
            match self {
                Self::Groups => TK_GROUPS,
                Self::Range => TK_RANGE,
                Self::Rows => TK_ROWS,
            },
            None,
        )
    }
}

impl_display_for_to_tokens!(FrameBound);
impl ToTokens for FrameBound {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::CurrentRow => {
                s.append(TK_CURRENT, None)?;
                s.append(TK_ROW, None)
            }
            Self::Following(value) => {
                value.to_tokens(s, context)?;
                s.append(TK_FOLLOWING, None)
            }
            Self::Preceding(value) => {
                value.to_tokens(s, context)?;
                s.append(TK_PRECEDING, None)
            }
            Self::UnboundedFollowing => {
                s.append(TK_UNBOUNDED, None)?;
                s.append(TK_FOLLOWING, None)
            }
            Self::UnboundedPreceding => {
                s.append(TK_UNBOUNDED, None)?;
                s.append(TK_PRECEDING, None)
            }
        }
    }
}

impl_display_for_to_tokens!(FrameExclude);
impl ToTokens for FrameExclude {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::NoOthers => {
                s.append(TK_NO, None)?;
                s.append(TK_OTHERS, None)
            }
            Self::CurrentRow => {
                s.append(TK_CURRENT, None)?;
                s.append(TK_ROW, None)
            }
            Self::Group => s.append(TK_GROUP, None),
            Self::Ties => s.append(TK_TIES, None),
        }
    }
}

fn comma<I, S: TokenStream + ?Sized, C: ToSqlContext>(
    items: I,
    s: &mut S,
    context: &C,
) -> Result<(), S::Error>
where
    I: IntoIterator,
    I::Item: ToTokens,
{
    s.comma(items, context)
}
