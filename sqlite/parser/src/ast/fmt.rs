//! AST node format
use std::fmt::{self, Display, Formatter, Write};

use crate::ast::*;

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

/// Writes SQL text piece by piece. It puts one space between two pieces,
/// except next to the punctuation marks that take none.
pub struct SqlWriter<'a> {
    write: &'a mut dyn Write,
    /// Whether the next piece is written without a space before it.
    spaced: bool,
}

impl<'a> SqlWriter<'a> {
    /// Creates a writer of SQL text to `write`.
    pub fn new(write: &'a mut dyn Write) -> Self {
        Self {
            write,
            spaced: true,
        }
    }

    /// Writes a keyword, such as `SELECT`.
    pub fn keyword(&mut self, keyword: &'static str) -> fmt::Result {
        if !self.spaced {
            self.write.write_char(' ')?;
        }
        self.spaced = false;
        self.write.write_str(keyword)
    }

    /// Writes an operator or a punctuation mark, such as `=` or `(`.
    pub fn punct(&mut self, punct: &'static str) -> fmt::Result {
        if !self.spaced && !matches!(punct, "," | ";" | ")" | "." | "[" | "]") {
            self.write.write_char(' ')?;
        }
        self.spaced = matches!(punct, "(" | "." | "[");
        self.write.write_str(punct)
    }

    /// Writes text that the caller made: a name, a literal or a parameter.
    pub fn text(&mut self, text: &str) -> fmt::Result {
        if !self.spaced {
            self.write.write_char(' ')?;
        }
        // Text that is only whitespace separates the pieces around it.
        self.spaced = text.bytes().all(|b| b.is_ascii_whitespace());
        self.write.write_str(text)
    }

    /// Writes a blob literal, such as `X'CAFE'`. The piece after it gets no
    /// space before it: `X'CAFE'AS b`.
    pub fn blob_literal(&mut self, literal: &str) -> fmt::Result {
        if !self.spaced {
            self.write.write_char(' ')?;
        }
        self.spaced = true;
        self.write.write_str(literal)
    }

    /// Ends the line. The next piece starts the new line.
    pub fn newline(&mut self) -> fmt::Result {
        self.spaced = true;
        self.write.write_char('\n')
    }

    /// Writes `items` with a comma between each two of them.
    pub fn comma<I, C: ToSqlContext>(&mut self, items: I, context: &C) -> fmt::Result
    where
        I: IntoIterator,
        I::Item: WriteSql,
    {
        for (i, item) in items.into_iter().enumerate() {
            if i != 0 {
                self.punct(",")?;
            }
            item.write_sql(self, context)?;
        }
        Ok(())
    }
}

pub struct BlankContext;

impl ToSqlContext for BlankContext {}

pub struct SqlDisplayer<'a, 'b, C: ToSqlContext, T: WriteSql> {
    ctx: &'a C,
    start_node: &'b T,
}

impl<'a, 'b, C: ToSqlContext, T: WriteSql> SqlDisplayer<'a, 'b, C, T> {
    /// Create a new displayer with the specified context and AST node
    pub fn new(ctx: &'a C, start_node: &'b T) -> Self {
        Self { ctx, start_node }
    }
}

impl<'a, 'b, C: ToSqlContext, T: WriteSql> Display for SqlDisplayer<'a, 'b, C, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut writer = SqlWriter::new(f);
        self.start_node.write_sql(&mut writer, self.ctx)
    }
}

/// An AST node that can be written as SQL text.
/// Also implements Display to make sure devs won't forget Display
pub trait WriteSql: Display {
    /// Writes the node to `s`, with the names that `context` gives
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result;

    // Return displayer representation with context
    fn displayer<'a, 'b, C: ToSqlContext>(&'b self, ctx: &'a C) -> SqlDisplayer<'a, 'b, C, Self>
    where
        Self: Sized,
    {
        SqlDisplayer::new(ctx, self)
    }
}

macro_rules! impl_display_for_write_sql {
    ($struct:ty) => {
        impl Display for $struct {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                self.displayer(&BlankContext).fmt(f)
            }
        }
    };
}

impl<T: ?Sized + WriteSql> WriteSql for &T {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        WriteSql::write_sql(&**self, s, context)
    }
}

impl<T: ?Sized + WriteSql> WriteSql for Box<T> {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        WriteSql::write_sql(self.as_ref(), s, context)
    }
}

impl WriteSql for String {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        s.text(self.as_ref())
    }
}

impl_display_for_write_sql!(Cmd);
impl WriteSql for Cmd {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Explain(stmt) => {
                s.keyword("EXPLAIN")?;
                stmt.write_sql(s, context)?;
            }
            Self::ExplainQueryPlan { stmt, format } => {
                s.keyword("EXPLAIN")?;
                s.keyword("QUERY")?;
                s.keyword("PLAN")?;
                if *format == EqpFormat::Json {
                    s.text("FORMAT")?;
                    s.punct("=")?;
                    s.text("JSON")?;
                }
                stmt.write_sql(s, context)?;
            }
            Self::Stmt(stmt) => {
                stmt.write_sql(s, context)?;
            }
        }
        s.punct(";")
    }
}

impl_display_for_write_sql!(Stmt);
impl WriteSql for Stmt {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::AlterTable(AlterTable { name, body }) => {
                s.keyword("ALTER")?;
                s.keyword("TABLE")?;
                name.write_sql(s, context)?;
                body.write_sql(s, context)
            }
            Self::Analyze { name } => {
                s.keyword("ANALYZE")?;
                if let Some(name) = name {
                    name.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Attach { expr, db_name, key } => {
                s.keyword("ATTACH")?;
                expr.write_sql(s, context)?;
                s.keyword("AS")?;
                db_name.write_sql(s, context)?;
                if let Some(key) = key {
                    s.keyword("KEY")?;
                    key.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Begin { typ, name, .. } => {
                s.keyword("BEGIN")?;
                if let Some(typ) = typ {
                    typ.write_sql(s, context)?;
                }
                if let Some(name) = name {
                    s.keyword("TRANSACTION")?;
                    name.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Commit { name } => {
                s.keyword("COMMIT")?;
                if let Some(name) = name {
                    s.keyword("TRANSACTION")?;
                    name.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::CreateIndex {
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                using,
                columns,
                with_clause,
                where_clause,
            } => {
                s.keyword("CREATE")?;
                if *unique {
                    s.keyword("UNIQUE")?;
                }
                s.keyword("INDEX")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                idx_name.write_sql(s, context)?;
                s.keyword("ON")?;
                tbl_name.write_sql(s, context)?;
                if let Some(using) = using {
                    s.keyword("USING")?;
                    using.write_sql(s, context)?;
                }
                s.punct("(")?;
                comma(columns, s, context)?;
                s.punct(")")?;
                if !with_clause.is_empty() {
                    s.keyword("WITH")?;
                    s.punct("(")?;
                    let mut first = true;
                    for (name, value) in with_clause.iter() {
                        if !first {
                            s.punct(",")?;
                        }
                        first = false;
                        name.write_sql(s, context)?;
                        s.punct("=")?;
                        value.write_sql(s, context)?;
                    }
                    s.punct(")")?;
                }
                if let Some(where_clause) = where_clause {
                    s.keyword("WHERE")?;
                    where_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } => {
                s.keyword("CREATE")?;
                if *temporary {
                    s.keyword("TEMP")?;
                }
                s.keyword("TABLE")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                tbl_name.write_sql(s, context)?;
                body.write_sql(s, context)
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
                s.keyword("CREATE")?;
                if *temporary {
                    s.keyword("TEMP")?;
                }
                s.keyword("TRIGGER")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                trigger_name.write_sql(s, context)?;
                if let Some(time) = time {
                    time.write_sql(s, context)?;
                }
                event.write_sql(s, context)?;
                s.keyword("ON")?;
                tbl_name.write_sql(s, context)?;
                if *for_each_row {
                    s.keyword("FOR")?;
                    s.keyword("EACH")?;
                    s.keyword("ROW")?;
                }
                if let Some(when_clause) = when_clause {
                    s.keyword("WHEN")?;
                    when_clause.write_sql(s, context)?;
                }
                s.keyword("BEGIN")?;
                s.newline()?;
                for command in commands {
                    command.write_sql(s, context)?;
                    s.punct(";")?;
                    s.newline()?;
                }
                s.keyword("END")
            }
            Self::CreateView {
                temporary,
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                s.keyword("CREATE")?;
                if *temporary {
                    s.keyword("TEMP")?;
                }
                s.keyword("VIEW")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                view_name.write_sql(s, context)?;
                if !columns.is_empty() {
                    s.punct("(")?;
                    comma(columns, s, context)?;
                    s.punct(")")?;
                }
                s.keyword("AS")?;
                select.write_sql(s, context)
            }
            Self::CreateMaterializedView {
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                s.keyword("CREATE")?;
                s.keyword("MATERIALIZED")?;
                s.keyword("VIEW")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                view_name.write_sql(s, context)?;
                if !columns.is_empty() {
                    s.punct("(")?;
                    comma(columns, s, context)?;
                    s.punct(")")?;
                }
                s.keyword("AS")?;
                select.write_sql(s, context)
            }
            Self::CreateVirtualTable(CreateVirtualTable {
                if_not_exists,
                tbl_name,
                module_name,
                args,
            }) => {
                s.keyword("CREATE")?;
                s.keyword("VIRTUAL")?;
                s.keyword("TABLE")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                tbl_name.write_sql(s, context)?;
                s.keyword("USING")?;
                module_name.write_sql(s, context)?;
                s.punct("(")?;
                if !args.is_empty() {
                    comma(args, s, context)?;
                }
                s.punct(")")
            }
            Self::Delete {
                with,
                tbl_name,
                indexed,
                where_clause,
                returning,
                // Written as the DELETE it runs as: this is SQLite's syntax,
                // which has no TRUNCATE.
                truncate: _,
            } => {
                if let Some(with) = with {
                    with.write_sql(s, context)?;
                }
                s.keyword("DELETE")?;
                s.keyword("FROM")?;
                tbl_name.write_sql(s, context)?;
                if let Some(indexed) = indexed {
                    indexed.write_sql(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.keyword("WHERE")?;
                    where_clause.write_sql(s, context)?;
                }
                if !returning.is_empty() {
                    s.keyword("RETURNING")?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Detach { name } => {
                s.keyword("DETACH")?;
                name.write_sql(s, context)
            }
            Self::DropIndex {
                if_exists,
                idx_name,
            } => {
                s.keyword("DROP")?;
                s.keyword("INDEX")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                idx_name.write_sql(s, context)
            }
            Self::DropTable {
                if_exists,
                tbl_name,
            } => {
                s.keyword("DROP")?;
                s.keyword("TABLE")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                tbl_name.write_sql(s, context)
            }
            Self::DropTrigger {
                if_exists,
                trigger_name,
            } => {
                s.keyword("DROP")?;
                s.keyword("TRIGGER")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                trigger_name.write_sql(s, context)
            }
            Self::DropView {
                if_exists,
                view_name,
            } => {
                s.keyword("DROP")?;
                s.keyword("VIEW")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                view_name.write_sql(s, context)
            }
            Self::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
                ..
            } => {
                if let Some(with) = with {
                    with.write_sql(s, context)?;
                }
                if let Some(ResolveType::Replace) = or_conflict {
                    s.keyword("REPLACE")?;
                } else {
                    s.keyword("INSERT")?;
                    if let Some(or_conflict) = or_conflict {
                        s.keyword("OR")?;
                        or_conflict.write_sql(s, context)?;
                    }
                }
                s.keyword("INTO")?;
                tbl_name.write_sql(s, context)?;
                if !columns.is_empty() {
                    s.punct("(")?;
                    comma(columns, s, context)?;
                    s.punct(")")?;
                }
                body.write_sql(s, context)?;
                if !returning.is_empty() {
                    s.keyword("RETURNING")?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Pragma { name, body } => {
                s.keyword("PRAGMA")?;
                name.write_sql(s, context)?;
                if let Some(body) = body {
                    body.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Reindex { name } => {
                s.keyword("REINDEX")?;
                if let Some(name) = name {
                    name.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Optimize { idx_name } => {
                s.keyword("OPTIMIZE")?;
                s.keyword("INDEX")?;
                if let Some(name) = idx_name {
                    name.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Release { name } => {
                s.keyword("RELEASE")?;
                name.write_sql(s, context)
            }
            Self::Rollback {
                tx_name,
                savepoint_name,
            } => {
                s.keyword("ROLLBACK")?;
                if let Some(tx_name) = tx_name {
                    s.keyword("TRANSACTION")?;
                    tx_name.write_sql(s, context)?;
                }
                if let Some(savepoint_name) = savepoint_name {
                    s.keyword("TO")?;
                    savepoint_name.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Savepoint { name } => {
                s.keyword("SAVEPOINT")?;
                name.write_sql(s, context)
            }
            Self::Select(select) => select.write_sql(s, context),
            Self::Update(Update {
                with,
                or_conflict,
                tbl_name,
                indexed,
                sets,
                from,
                where_clause,
                returning,
            }) => {
                if let Some(with) = with {
                    with.write_sql(s, context)?;
                }
                s.keyword("UPDATE")?;
                if let Some(or_conflict) = or_conflict {
                    s.keyword("OR")?;
                    or_conflict.write_sql(s, context)?;
                }
                tbl_name.write_sql(s, context)?;
                if let Some(indexed) = indexed {
                    indexed.write_sql(s, context)?;
                }
                s.keyword("SET")?;
                comma(sets, s, context)?;
                if let Some(from) = from {
                    s.keyword("FROM")?;
                    from.write_sql(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.keyword("WHERE")?;
                    where_clause.write_sql(s, context)?;
                }
                if !returning.is_empty() {
                    s.keyword("RETURNING")?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Vacuum { name, into } => {
                s.keyword("VACUUM")?;
                if let Some(ref name) = name {
                    name.write_sql(s, context)?;
                }
                if let Some(ref into) = into {
                    s.keyword("INTO")?;
                    into.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::CreateRole { role_name, options } => {
                s.keyword("CREATE")?;
                s.text("ROLE")?;
                s.text(role_name)?;
                if !options.inherit {
                    s.text("NOINHERIT")?;
                }
                if options.bypassrls {
                    s.text("BYPASSRLS")?;
                }
                Ok(())
            }
            Self::DropRole { if_exists, roles } => {
                s.keyword("DROP")?;
                s.text("ROLE")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                s.comma(roles, context)
            }
            Self::Grant(grant) => grant.write_sql(s, context),
            Self::CreateType {
                if_not_exists,
                type_name,
                body,
            } => {
                s.keyword("CREATE")?;
                s.keyword("TYPE")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                s.text(type_name)?;
                match body {
                    CreateTypeBody::Struct(fields) => {
                        s.keyword("AS")?;
                        s.text("STRUCT")?;
                        write_type_fields(fields, s, context)?;
                    }
                    CreateTypeBody::Union(fields) => {
                        s.keyword("AS")?;
                        s.text("UNION")?;
                        write_type_fields(fields, s, context)?;
                    }
                    CreateTypeBody::CustomType {
                        params,
                        base,
                        encode,
                        coerce,
                        decode,
                        operators,
                        default,
                    } => {
                        // Parameters
                        if !params.is_empty() {
                            s.punct("(")?;
                            for (i, param) in params.iter().enumerate() {
                                if i > 0 {
                                    s.punct(",")?;
                                }
                                s.text(&param.name)?;
                                if let Some(ref ty) = param.ty {
                                    s.text(ty)?;
                                }
                            }
                            s.punct(")")?;
                        }
                        // BASE
                        s.text("BASE")?;
                        s.text(base)?;
                        // ENCODE
                        if let Some(ref encode) = encode {
                            s.text("ENCODE")?;
                            encode.write_sql(s, context)?;
                        }
                        // COERCE
                        if let Some(ref coerce) = coerce {
                            s.text("COERCE")?;
                            coerce.write_sql(s, context)?;
                        }
                        // DECODE
                        if let Some(ref decode) = decode {
                            s.text("DECODE")?;
                            decode.write_sql(s, context)?;
                        }
                        // DEFAULT
                        if let Some(ref default) = default {
                            s.text("DEFAULT")?;
                            default.write_sql(s, context)?;
                        }
                        // OPERATOR clauses
                        for op in operators {
                            s.text("OPERATOR")?;
                            s.text(&format!("'{}'", op.op))?;
                            if let Some(ref func_name) = op.func_name {
                                s.text(func_name)?;
                            }
                        }
                    }
                }
                Ok(())
            }
            Self::CreateDomain {
                if_not_exists,
                domain_name,
                base_type,
                default,
                not_null,
                constraints,
            } => {
                s.keyword("CREATE")?;
                s.text("DOMAIN")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                s.text(domain_name)?;
                s.keyword("AS")?;
                s.text(base_type)?;
                if let Some(ref def) = default {
                    s.keyword("DEFAULT")?;
                    def.write_sql(s, context)?;
                }
                if *not_null {
                    s.keyword("NOT")?;
                    s.keyword("NULL")?;
                }
                for c in constraints {
                    if let Some(ref name) = c.name {
                        s.keyword("CONSTRAINT")?;
                        s.text(name)?;
                    }
                    s.keyword("CHECK")?;
                    s.punct("(")?;
                    c.check.write_sql(s, context)?;
                    s.punct(")")?;
                }
                Ok(())
            }
            Self::DropType {
                if_exists,
                type_name,
            } => {
                s.keyword("DROP")?;
                s.keyword("TYPE")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                s.text(type_name)?;
                Ok(())
            }
            Self::CreateSequence {
                if_not_exists,
                seq_name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
            } => {
                s.keyword("CREATE")?;
                s.text("SEQUENCE")?;
                if *if_not_exists {
                    s.keyword("IF")?;
                    s.keyword("NOT")?;
                    s.keyword("EXISTS")?;
                }
                seq_name.write_sql(s, context)?;
                if let Some(v) = start {
                    s.text("START")?;
                    s.text("WITH")?;
                    s.text(&v.to_string())?;
                }
                if let Some(v) = increment {
                    s.text("INCREMENT")?;
                    s.text("BY")?;
                    s.text(&v.to_string())?;
                }
                if let Some(v) = min_value {
                    s.text("MINVALUE")?;
                    s.text(&v.to_string())?;
                }
                if let Some(v) = max_value {
                    s.text("MAXVALUE")?;
                    s.text(&v.to_string())?;
                }
                if *cycle {
                    s.text("CYCLE")?;
                }
                Ok(())
            }
            Self::DropSequence {
                if_exists,
                seq_name,
            } => {
                s.keyword("DROP")?;
                s.text("SEQUENCE")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                seq_name.write_sql(s, context)?;
                Ok(())
            }
            Self::AlterSequenceOwnedBy { seq_name, owner } => {
                s.keyword("ALTER")?;
                s.text("SEQUENCE")?;
                seq_name.write_sql(s, context)?;
                s.text("OWNED")?;
                s.keyword("BY")?;
                match owner {
                    Some((table, column)) => {
                        table.write_sql(s, context)?;
                        s.punct(".")?;
                        column.write_sql(s, context)
                    }
                    None => s.text("NONE"),
                }
            }
            Self::CreateSchema { name } => {
                s.keyword("CREATE")?;
                s.text("SCHEMA")?;
                name.write_sql(s, context)?;
                Ok(())
            }
            Self::DropSchema { name } => {
                s.keyword("DROP")?;
                s.text("SCHEMA")?;
                name.write_sql(s, context)?;
                Ok(())
            }
            Self::CreatePolicy(policy) => policy.write_sql(s, context),
            Self::DropPolicy {
                if_exists,
                policy_name,
                tbl_name,
            } => {
                s.keyword("DROP")?;
                s.text("POLICY")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                policy_name.write_sql(s, context)?;
                s.keyword("ON")?;
                tbl_name.write_sql(s, context)
            }
            Self::DropDomain {
                if_exists,
                domain_name,
            } => {
                s.keyword("DROP")?;
                s.text("DOMAIN")?;
                if *if_exists {
                    s.keyword("IF")?;
                    s.keyword("EXISTS")?;
                }
                s.text(domain_name)?;
                Ok(())
            }
        }
    }
}

impl_display_for_write_sql!(Expr);
impl WriteSql for Expr {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::SubqueryResult { .. } => {
                // FIXME: what to put here? This is a highly "artificial" AST node that has no meaning when stringified.
                Ok(())
            }
            Self::Between {
                lhs,
                not,
                start,
                end,
            } => {
                lhs.write_sql(s, context)?;
                if *not {
                    s.keyword("NOT")?;
                }
                s.keyword("BETWEEN")?;
                start.write_sql(s, context)?;
                s.keyword("AND")?;
                end.write_sql(s, context)
            }
            Self::Binary(lhs, op, rhs) => {
                lhs.write_sql(s, context)?;
                op.write_sql(s, context)?;
                rhs.write_sql(s, context)
            }
            Self::Register { register: reg, .. } => {
                // This is for internal use only, not part of SQL syntax
                // Use a special notation that won't conflict with SQL
                s.text(&format!("$r{reg}"))
            }
            Self::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                s.keyword("CASE")?;
                if let Some(ref base) = base {
                    base.write_sql(s, context)?;
                }
                for (when, then) in when_then_pairs {
                    s.keyword("WHEN")?;
                    when.write_sql(s, context)?;
                    s.keyword("THEN")?;
                    then.write_sql(s, context)?;
                }
                if let Some(ref else_expr) = else_expr {
                    s.keyword("ELSE")?;
                    else_expr.write_sql(s, context)?;
                }
                s.keyword("END")
            }
            Self::Cast { expr, type_name } => {
                s.keyword("CAST")?;
                s.punct("(")?;
                expr.write_sql(s, context)?;
                s.keyword("AS")?;
                if let Some(ref type_name) = type_name {
                    type_name.write_sql(s, context)?;
                }
                s.punct(")")
            }
            Self::Collate(expr, collation) => {
                expr.write_sql(s, context)?;
                s.keyword("COLLATE")?;
                s.text(&collation.as_ident())
            }
            Self::DoublyQualified(db_name, tbl_name, col_name) => {
                db_name.write_sql(s, context)?;
                s.punct(".")?;
                tbl_name.write_sql(s, context)?;
                s.punct(".")?;
                col_name.write_sql(s, context)
            }
            Self::Exists(subquery) => {
                s.keyword("EXISTS")?;
                s.punct("(")?;
                subquery.write_sql(s, context)?;
                s.punct(")")
            }
            Self::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                within_group,
                filter_over,
            } => {
                name.write_sql(s, context)?;
                s.punct("(")?;
                if let Some(distinctness) = distinctness {
                    distinctness.write_sql(s, context)?;
                }
                if !args.is_empty() {
                    comma(args, s, context)?;
                }
                if !order_by.is_empty() {
                    s.keyword("ORDER")?;
                    s.keyword("BY")?;
                    comma(order_by, s, context)?;
                }
                s.punct(")")?;
                if !within_group.is_empty() {
                    s.keyword("WITHIN")?;
                    s.keyword("GROUP")?;
                    s.punct("(")?;
                    s.keyword("ORDER")?;
                    s.keyword("BY")?;
                    comma(within_group, s, context)?;
                    s.punct(")")?;
                }
                filter_over.write_sql(s, context)?;
                Ok(())
            }
            Self::FunctionCallStar { name, filter_over } => {
                name.write_sql(s, context)?;
                s.punct("(")?;
                s.punct("*")?;
                s.punct(")")?;
                filter_over.write_sql(s, context)?;
                Ok(())
            }
            Self::MergedColumn(columns) => {
                // EXPLAIN shows a merged USING column as `coalesce(t1.a, t2.a)`, as SQLite does.
                s.text("coalesce")?;
                s.punct("(")?;
                comma(columns, s, context)?;
                s.punct(")")
            }
            Self::Id(id) => id.write_sql(s, context),
            Self::Column { table, column, .. } => {
                let (tbl_name, col_name) = context.get_table_and_column_names(*table, *column);
                s.text(tbl_name.as_ref())?;
                s.punct(".")?;
                s.text(col_name.as_ref())
            }
            Self::InList { lhs, not, rhs } => {
                lhs.write_sql(s, context)?;
                if *not {
                    s.keyword("NOT")?;
                }
                s.keyword("IN")?;
                s.punct("(")?;
                if !rhs.is_empty() {
                    comma(rhs, s, context)?;
                }
                s.punct(")")
            }
            Self::InSelect { lhs, not, rhs } => {
                lhs.write_sql(s, context)?;
                if *not {
                    s.keyword("NOT")?;
                }
                s.keyword("IN")?;
                s.punct("(")?;
                rhs.write_sql(s, context)?;
                s.punct(")")
            }
            Self::InTable {
                lhs,
                not,
                rhs,
                args,
            } => {
                lhs.write_sql(s, context)?;
                if *not {
                    s.keyword("NOT")?;
                }
                s.keyword("IN")?;
                rhs.write_sql(s, context)?;
                if !args.is_empty() {
                    s.punct("(")?;
                    comma(args, s, context)?;
                    s.punct(")")?;
                }
                Ok(())
            }
            Self::IsNull(sub_expr) => {
                sub_expr.write_sql(s, context)?;
                s.keyword("ISNULL")
            }
            Self::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => {
                lhs.write_sql(s, context)?;
                if *not {
                    s.keyword("NOT")?;
                }
                op.write_sql(s, context)?;
                rhs.write_sql(s, context)?;
                if let Some(escape) = escape {
                    s.keyword("ESCAPE")?;
                    escape.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Literal(lit) => lit.write_sql(s, context),
            Self::Name(name) => name.write_sql(s, context),
            Self::NotNull(sub_expr) => {
                sub_expr.write_sql(s, context)?;
                s.keyword("NOTNULL")
            }
            Self::Parenthesized(exprs) => {
                s.punct("(")?;
                comma(exprs, s, context)?;
                s.punct(")")
            }
            Self::Qualified(qualifier, qualified) => {
                qualifier.write_sql(s, context)?;
                s.punct(".")?;
                qualified.write_sql(s, context)
            }
            Self::FieldAccess { base, field, .. } => {
                base.write_sql(s, context)?;
                s.punct(".")?;
                field.write_sql(s, context)
            }
            Self::Raise(rt, err) => {
                s.keyword("RAISE")?;
                s.punct("(")?;
                rt.write_sql(s, context)?;
                if let Some(err) = err {
                    s.punct(",")?;
                    err.write_sql(s, context)?;
                }
                s.punct(")")
            }
            Self::RowId { .. } => Ok(()),
            Self::Subquery(query) => {
                s.punct("(")?;
                query.write_sql(s, context)?;
                s.punct(")")
            }
            Self::Unary(op, sub_expr) => {
                op.write_sql(s, context)?;
                sub_expr.write_sql(s, context)
            }
            Self::Variable(var) => {
                if let Some(name) = var.name.as_deref() {
                    return s.text(name);
                }

                let indexed = format!("?{}", var.index.get());
                s.text(indexed.as_str())
            }
            Self::Default => s.keyword("DEFAULT"),
            Self::Array { elements } => {
                s.text("ARRAY")?;
                s.punct("[")?;
                comma(elements, s, context)?;
                s.punct("]")
            }
            Self::Subscript { base, index } => {
                base.write_sql(s, context)?;
                s.punct("[")?;
                index.write_sql(s, context)?;
                s.punct("]")
            }
        }
    }
}

impl_display_for_write_sql!(Literal);
impl WriteSql for Literal {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Numeric(ref num) => s.text(num), // TODO validate that this is a number
            Self::String(ref str) => s.text(str),
            Self::Blob(ref blob) => s.blob_literal(blob),
            Self::Keyword(ref str) => s.text(str), // TODO validate that this is a keyword
            Self::Null => s.keyword("NULL"),
            Self::True => s.text("TRUE"),
            Self::False => s.text("FALSE"),
            Self::CurrentDate => s.text("CURRENT_DATE"),
            Self::CurrentTime => s.text("CURRENT_TIME"),
            Self::CurrentTimestamp => s.text("CURRENT_TIMESTAMP"),
        }
    }
}

impl_display_for_write_sql!(LikeOperator);
impl WriteSql for LikeOperator {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        s.text(match self {
            Self::Glob => "GLOB",
            Self::Like => "LIKE",
            Self::Match => "MATCH",
            Self::Regexp => "REGEXP",
        })
    }
}

impl_display_for_write_sql!(Operator);
impl WriteSql for Operator {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Add => s.punct("+"),
            Self::And => s.keyword("AND"),
            Self::ArrowRight => s.text("->"),
            Self::ArrowRightShift => s.text("->>"),
            Self::BitwiseAnd => s.punct("&"),
            Self::BitwiseOr => s.punct("|"),
            Self::BitwiseNot => s.punct("~"),
            Self::Concat => s.punct("||"),
            Self::Equals => s.punct("="),
            Self::Divide => s.punct("/"),
            Self::Greater => s.punct(">"),
            Self::GreaterEquals => s.punct(">="),
            Self::Is => s.keyword("IS"),
            Self::IsNot => {
                s.keyword("IS")?;
                s.keyword("NOT")
            }
            Self::LeftShift => s.punct("<<"),
            Self::Less => s.punct("<"),
            Self::LessEquals => s.punct("<="),
            Self::Modulus => s.punct("%"),
            Self::Multiply => s.punct("*"),
            Self::NotEquals => s.punct("!="),
            Self::Or => s.keyword("OR"),
            Self::RightShift => s.punct(">>"),
            Self::Subtract => s.punct("-"),
            Self::ArrayContains => s.punct("@>"),
            Self::ArrayOverlap => s.punct("&&"),
        }
    }
}

impl_display_for_write_sql!(UnaryOperator);
impl WriteSql for UnaryOperator {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::BitwiseNot => s.punct("~"),
            Self::Negative => s.punct("-"),
            Self::Not => s.keyword("NOT"),
            Self::Positive => s.punct("+"),
        }
    }
}

impl_display_for_write_sql!(Select);
impl WriteSql for Select {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if let Some(ref with) = self.with {
            with.write_sql(s, context)?;
        }
        self.body.write_sql(s, context)?;
        if !self.order_by.is_empty() {
            s.keyword("ORDER")?;
            s.keyword("BY")?;
            comma(&self.order_by, s, context)?;
        }
        if let Some(ref limit) = self.limit {
            limit.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(SelectBody);
impl WriteSql for SelectBody {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.select.write_sql(s, context)?;
        for compound in &self.compounds {
            compound.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(CompoundSelect);
impl WriteSql for CompoundSelect {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.operator.write_sql(s, context)?;
        self.select.write_sql(s, context)
    }
}

impl_display_for_write_sql!(CompoundOperator);
impl WriteSql for CompoundOperator {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Union => s.keyword("UNION"),
            Self::UnionAll => {
                s.keyword("UNION")?;
                s.keyword("ALL")
            }
            Self::Except => s.keyword("EXCEPT"),
            Self::Intersect => s.keyword("INTERSECT"),
        }
    }
}

impl_display_for_write_sql!(OneSelect);
impl WriteSql for OneSelect {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Select {
                distinctness,
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
            } => {
                s.keyword("SELECT")?;
                if let Some(ref distinctness) = distinctness {
                    distinctness.write_sql(s, context)?;
                }
                comma(columns, s, context)?;
                if let Some(ref from) = from {
                    s.keyword("FROM")?;
                    from.write_sql(s, context)?;
                }
                if let Some(ref where_clause) = where_clause {
                    s.keyword("WHERE")?;
                    where_clause.write_sql(s, context)?;
                }
                if let Some(ref group_by) = group_by {
                    group_by.write_sql(s, context)?;
                }
                if !window_clause.is_empty() {
                    s.keyword("WINDOW")?;
                    comma(window_clause, s, context)?;
                }
                Ok(())
            }
            Self::Values(values) => {
                for (i, vals) in values.iter().enumerate() {
                    if i == 0 {
                        s.keyword("VALUES")?;
                    } else {
                        s.punct(",")?;
                    }
                    s.punct("(")?;
                    comma(vals, s, context)?;
                    s.punct(")")?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_write_sql!(FromClause);
impl WriteSql for FromClause {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.select.as_ref().write_sql(s, context)?;
        for join in &self.joins {
            join.write_sql(s, context)?;
        }

        Ok(())
    }
}

impl_display_for_write_sql!(SelectDistinctness);
impl WriteSql for SelectDistinctness {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Distinct => s.keyword("DISTINCT"),
            Self::All => s.keyword("ALL"),
            Self::DistinctOn(exprs) => {
                s.keyword("DISTINCT")?;
                s.keyword("ON")?;
                s.punct("(")?;
                comma(exprs, s, context)?;
                s.punct(")")
            }
        }
    }
}

impl_display_for_write_sql!(Distinctness);
impl WriteSql for Distinctness {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Distinct => s.keyword("DISTINCT"),
            Self::All => s.keyword("ALL"),
        }
    }
}

impl_display_for_write_sql!(ResultColumn);
impl WriteSql for ResultColumn {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Expr(expr, alias) => {
                expr.write_sql(s, context)?;
                if let Some(alias) = alias {
                    alias.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Star => s.punct("*"),
            Self::TableStar(tbl_name) => {
                tbl_name.write_sql(s, context)?;
                s.punct(".")?;
                s.punct("*")
            }
        }
    }
}

impl_display_for_write_sql!(As);
impl WriteSql for As {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::As(ref name) => {
                s.keyword("AS")?;
                name.write_sql(s, context)
            }
            Self::Elided(ref name) => name.write_sql(s, context),
            Self::ImplicitColumnName(_) => Ok(()),
        }
    }
}

impl_display_for_write_sql!(JoinedSelectTable);
impl WriteSql for JoinedSelectTable {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.operator.write_sql(s, context)?;
        self.table.write_sql(s, context)?;
        if let Some(ref constraint) = self.constraint {
            constraint.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(SelectTable);
impl WriteSql for SelectTable {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Table(name, alias, indexed) => {
                name.write_sql(s, context)?;
                if let Some(alias) = alias {
                    alias.write_sql(s, context)?;
                }
                if let Some(indexed) = indexed {
                    indexed.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::TableCall(name, exprs, alias) => {
                name.write_sql(s, context)?;
                s.punct("(")?;
                comma(exprs, s, context)?;
                s.punct(")")?;
                if let Some(alias) = alias {
                    alias.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Select {
                select,
                alias,
                lateral,
            } => {
                if *lateral {
                    s.keyword("LATERAL")?;
                }
                s.punct("(")?;
                select.write_sql(s, context)?;
                s.punct(")")?;
                if let Some(alias) = alias {
                    alias.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Sub(from, alias) => {
                s.punct("(")?;
                from.write_sql(s, context)?;
                s.punct(")")?;
                if let Some(alias) = alias {
                    alias.write_sql(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_write_sql!(JoinOperator);
impl WriteSql for JoinOperator {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Comma => s.punct(","),
            Self::TypedJoin(join_type) => {
                if let Some(ref join_type) = join_type {
                    join_type.write_sql(s, context)?;
                }
                s.keyword("JOIN")
            }
        }
    }
}

impl_display_for_write_sql!(JoinType);
impl WriteSql for JoinType {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        if self.contains(Self::NATURAL) {
            s.text("NATURAL")?;
        }
        if self.contains(Self::INNER) {
            if self.contains(Self::CROSS) {
                s.text("CROSS")?;
            }
            s.text("INNER")?;
        } else {
            if self.contains(Self::LEFT) {
                if self.contains(Self::RIGHT) {
                    s.text("FULL")?;
                } else {
                    s.text("LEFT")?;
                }
            } else if self.contains(Self::RIGHT) {
                s.text("RIGHT")?;
            }
            if self.contains(Self::OUTER) {
                s.text("OUTER")?;
            }
        }
        Ok(())
    }
}

impl_display_for_write_sql!(JoinConstraint);
impl WriteSql for JoinConstraint {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::On(expr) => {
                s.keyword("ON")?;
                expr.write_sql(s, context)
            }
            Self::Using(col_names) => {
                s.keyword("USING")?;
                s.punct("(")?;
                comma(col_names, s, context)?;
                s.punct(")")
            }
        }
    }
}

impl_display_for_write_sql!(GroupBy);
impl WriteSql for GroupBy {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if !self.exprs.is_empty() {
            s.keyword("GROUP")?;
            s.keyword("BY")?;
            comma(&self.exprs, s, context)?;
        }
        if let Some(ref having) = self.having {
            s.keyword("HAVING")?;
            having.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(Name);
impl WriteSql for Name {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        s.text(&self.as_ident())
    }
}

impl_display_for_write_sql!(QualifiedName);
impl WriteSql for QualifiedName {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if let Some(ref db_name) = self.namespace {
            db_name.write_sql(s, context)?;
            s.punct(".")?;
        }
        self.name.write_sql(s, context)?;
        if let Some(ref alias) = self.alias {
            s.keyword("AS")?;
            alias.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(AlterTableBody);
impl WriteSql for AlterTableBody {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::RenameTo(name) => {
                s.keyword("RENAME")?;
                s.keyword("TO")?;
                name.write_sql(s, context)
            }
            Self::AddColumn(def) => {
                s.keyword("ADD")?;
                s.keyword("COLUMN")?;
                def.write_sql(s, context)
            }
            Self::AlterColumn { old, new } => {
                s.keyword("ALTER")?;
                s.keyword("COLUMN")?;
                old.write_sql(s, context)?;
                s.keyword("TO")?;
                new.write_sql(s, context)
            }
            Self::RenameColumn { old, new } => {
                s.keyword("RENAME")?;
                s.keyword("COLUMN")?;
                old.write_sql(s, context)?;
                s.keyword("TO")?;
                new.write_sql(s, context)
            }
            Self::DropColumn(name) => {
                s.keyword("DROP")?;
                s.keyword("COLUMN")?;
                name.write_sql(s, context)
            }
            Self::OwnerTo { role, .. } => {
                s.text("OWNER")?;
                s.keyword("TO")?;
                role.write_sql(s, context)
            }
            Self::EnableRowSecurity => write_row_security(s, "ENABLE"),
            Self::DisableRowSecurity => write_row_security(s, "DISABLE"),
        }
    }
}

impl_display_for_write_sql!(RoleSpec);
impl WriteSql for RoleSpec {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _context: &C) -> fmt::Result {
        match self {
            Self::Name(name) => s.text(name),
            Self::CurrentRole => s.text("CURRENT_ROLE"),
            Self::SessionUser => s.text("SESSION_USER"),
        }
    }
}

impl_display_for_write_sql!(Grant);
impl WriteSql for Grant {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.text(if self.revoke { "REVOKE" } else { "GRANT" })?;
        match &self.objects {
            GrantObjects::Relations {
                privileges,
                on_sequences,
                relations,
            } => {
                s.comma(privileges, context)?;
                s.keyword("ON")?;
                if *on_sequences {
                    s.text("SEQUENCE")?;
                }
                s.comma(relations, context)?;
            }
            GrantObjects::Roles(roles) => {
                for (i, role) in roles.iter().enumerate() {
                    if i > 0 {
                        s.punct(",")?;
                    }
                    s.text(role)?;
                }
            }
        }
        if self.revoke {
            s.keyword("FROM")?;
        } else {
            s.keyword("TO")?;
        }
        s.comma(&self.grantees, context)
    }
}

impl_display_for_write_sql!(Privilege);
impl WriteSql for Privilege {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _context: &C) -> fmt::Result {
        match self {
            Self::Select => s.keyword("SELECT"),
            Self::Insert => s.keyword("INSERT"),
            Self::Update => s.keyword("UPDATE"),
            Self::Delete => s.keyword("DELETE"),
            Self::Truncate => s.text("TRUNCATE"),
            Self::References => s.keyword("REFERENCES"),
            Self::Trigger => s.keyword("TRIGGER"),
            Self::Maintain => s.text("MAINTAIN"),
            Self::Usage => s.text("USAGE"),
            Self::All => s.keyword("ALL"),
        }
    }
}

impl_display_for_write_sql!(Grantee);
impl WriteSql for Grantee {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Public => s.text("PUBLIC"),
            Self::Role(role) => role.write_sql(s, context),
        }
    }
}

impl_display_for_write_sql!(CreatePolicy);
impl WriteSql for CreatePolicy {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.keyword("CREATE")?;
        s.text("POLICY")?;
        self.policy_name.write_sql(s, context)?;
        s.keyword("ON")?;
        self.tbl_name.write_sql(s, context)?;
        s.keyword("AS")?;
        s.text(if self.permissive {
            "PERMISSIVE"
        } else {
            "RESTRICTIVE"
        })?;
        s.keyword("FOR")?;
        match self.command {
            PolicyCommand::All => s.keyword("ALL")?,
            PolicyCommand::Select => s.keyword("SELECT")?,
            PolicyCommand::Insert => s.keyword("INSERT")?,
            PolicyCommand::Update => s.keyword("UPDATE")?,
            PolicyCommand::Delete => s.keyword("DELETE")?,
        }
        s.keyword("TO")?;
        s.comma(&self.roles, context)?;
        if let Some(expr) = &self.using_expr {
            s.keyword("USING")?;
            s.punct("(")?;
            expr.write_sql(s, context)?;
            s.punct(")")?;
        }
        if let Some(expr) = &self.check_expr {
            s.keyword("WITH")?;
            s.keyword("CHECK")?;
            s.punct("(")?;
            expr.write_sql(s, context)?;
            s.punct(")")?;
        }
        Ok(())
    }
}

/// `ENABLE ROW LEVEL SECURITY` or `DISABLE ROW LEVEL SECURITY`.
fn write_row_security(s: &mut SqlWriter<'_>, enable_or_disable: &str) -> fmt::Result {
    for word in [enable_or_disable, "ROW", "LEVEL", "SECURITY"] {
        s.text(word)?;
    }
    Ok(())
}

impl_display_for_write_sql!(CreateTableBody);
impl WriteSql for CreateTableBody {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::ColumnsAndConstraints {
                columns,
                constraints,
                options,
            } => {
                s.punct("(")?;
                comma(columns, s, context)?;
                if !constraints.is_empty() {
                    s.punct(",")?;
                    comma(constraints, s, context)?;
                }
                s.punct(")")?;
                // Use the original text if available
                if let Some(ref without_rowid) = options.without_rowid_text {
                    // Split "WITHOUT ROWID" back into its two words
                    let parts: Vec<&str> = without_rowid.split_whitespace().collect();
                    if parts.len() == 2 {
                        s.keyword("WITHOUT")?;
                        s.text(parts[1])?;
                    }
                }
                if let Some(ref strict) = options.strict_text {
                    if options.contains_without_rowid() {
                        s.punct(",")?;
                    }
                    s.text(strict)?;
                }
                Ok(())
            }
            Self::AsSelect(select) => {
                s.keyword("AS")?;
                select.write_sql(s, context)
            }
        }
    }
}

impl_display_for_write_sql!(ColumnDefinition);
impl WriteSql for ColumnDefinition {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.col_name.write_sql(s, context)?;
        if let Some(ref col_type) = self.col_type {
            col_type.write_sql(s, context)?;
        }
        for constraint in &self.constraints {
            constraint.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(NamedColumnConstraint);
impl WriteSql for NamedColumnConstraint {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if let Some(ref name) = self.name {
            s.keyword("CONSTRAINT")?;
            name.write_sql(s, context)?;
        }
        self.constraint.write_sql(s, context)
    }
}

impl_display_for_write_sql!(ColumnConstraint);
impl WriteSql for ColumnConstraint {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::PrimaryKey {
                order,
                conflict_clause,
                auto_increment,
            } => {
                s.keyword("PRIMARY")?;
                s.keyword("KEY")?;
                if let Some(order) = order {
                    order.write_sql(s, context)?;
                }
                if let Some(conflict_clause) = conflict_clause {
                    s.keyword("ON")?;
                    s.keyword("CONFLICT")?;
                    conflict_clause.write_sql(s, context)?;
                }
                if *auto_increment {
                    s.keyword("AUTOINCREMENT")?;
                }
                Ok(())
            }
            Self::NotNull {
                nullable,
                conflict_clause,
            } => {
                if !nullable {
                    s.keyword("NOT")?;
                }
                s.keyword("NULL")?;
                if let Some(conflict_clause) = conflict_clause {
                    s.keyword("ON")?;
                    s.keyword("CONFLICT")?;
                    conflict_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Unique(conflict_clause) => {
                s.keyword("UNIQUE")?;
                if let Some(conflict_clause) = conflict_clause {
                    s.keyword("ON")?;
                    s.keyword("CONFLICT")?;
                    conflict_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Check { expr, source } => {
                s.keyword("CHECK")?;
                s.punct("(")?;
                match source {
                    // Emit the expression exactly as the user wrote it so the
                    // spelling survives the canonical re-render (SQLite
                    // reports failed CHECKs with this text). Put the closing
                    // paren on a new line when a line comment might otherwise
                    // swallow it. Closed block comments are safe as-is.
                    Some(src) if !src.is_empty() => {
                        s.text(src)?;
                        if src.contains("--") {
                            s.text("\n")?;
                        }
                    }
                    _ => expr.write_sql(s, context)?,
                }
                s.punct(")")
            }
            Self::Default(expr) => {
                s.keyword("DEFAULT")?;
                let parses_without_parentheses = match expr.as_ref() {
                    Expr::Literal(_) | Expr::Id(_) | Expr::Parenthesized(_) => true,
                    Expr::Unary(UnaryOperator::Positive | UnaryOperator::Negative, operand) => {
                        matches!(operand.as_ref(), Expr::Literal(_))
                    }
                    _ => false,
                };
                if parses_without_parentheses {
                    return expr.write_sql(s, context);
                }
                s.punct("(")?;
                expr.write_sql(s, context)?;
                s.punct(")")
            }
            Self::Collate { collation_name } => {
                s.keyword("COLLATE")?;
                collation_name.write_sql(s, context)
            }
            Self::ForeignKey {
                clause,
                defer_clause,
            } => {
                s.keyword("REFERENCES")?;
                clause.write_sql(s, context)?;
                if let Some(defer_clause) = defer_clause {
                    defer_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Generated {
                generated_always,
                expr,
                typ,
            } => {
                if *generated_always {
                    s.keyword("GENERATED")?;
                    s.keyword("ALWAYS")?;
                }
                s.keyword("AS")?;
                s.punct("(")?;
                expr.write_sql(s, context)?;
                s.punct(")")?;
                if let Some(typ) = typ {
                    match typ {
                        GeneratedColumnType::Virtual => s.keyword("VIRTUAL")?,
                        GeneratedColumnType::Stored => s.text("STORED")?,
                    }
                }
                Ok(())
            }
            Self::Identity(_) => Ok(()),
        }
    }
}

impl_display_for_write_sql!(NamedTableConstraint);
impl WriteSql for NamedTableConstraint {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if let Some(ref name) = self.name {
            s.keyword("CONSTRAINT")?;
            name.write_sql(s, context)?;
        }
        self.constraint.write_sql(s, context)
    }
}

impl_display_for_write_sql!(TableConstraint);
impl WriteSql for TableConstraint {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::PrimaryKey {
                columns,
                auto_increment,
                conflict_clause,
            } => {
                s.keyword("PRIMARY")?;
                s.keyword("KEY")?;
                s.punct("(")?;
                comma(columns, s, context)?;
                if *auto_increment {
                    s.keyword("AUTOINCREMENT")?;
                }
                s.punct(")")?;
                if let Some(conflict_clause) = conflict_clause {
                    s.keyword("ON")?;
                    s.keyword("CONFLICT")?;
                    conflict_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Unique {
                columns,
                conflict_clause,
            } => {
                s.keyword("UNIQUE")?;
                s.punct("(")?;
                comma(columns, s, context)?;
                s.punct(")")?;
                if let Some(conflict_clause) = conflict_clause {
                    s.keyword("ON")?;
                    s.keyword("CONFLICT")?;
                    conflict_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Check { expr, source } => {
                s.keyword("CHECK")?;
                s.punct("(")?;
                match source {
                    // Emit the expression exactly as the user wrote it so the
                    // spelling survives the canonical re-render (SQLite
                    // reports failed CHECKs with this text). Put the closing
                    // paren on a new line when a line comment might otherwise
                    // swallow it. Closed block comments are safe as-is.
                    Some(src) if !src.is_empty() => {
                        s.text(src)?;
                        if src.contains("--") {
                            s.text("\n")?;
                        }
                    }
                    _ => expr.write_sql(s, context)?,
                }
                s.punct(")")
            }
            Self::ForeignKey {
                columns,
                clause,
                defer_clause,
            } => {
                s.keyword("FOREIGN")?;
                s.keyword("KEY")?;
                s.punct("(")?;
                comma(columns, s, context)?;
                s.punct(")")?;
                s.keyword("REFERENCES")?;
                clause.write_sql(s, context)?;
                if let Some(defer_clause) = defer_clause {
                    defer_clause.write_sql(s, context)?;
                }
                Ok(())
            }
        }
    }
}

impl_display_for_write_sql!(SortOrder);
impl WriteSql for SortOrder {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Asc => s.keyword("ASC"),
            Self::Desc => s.keyword("DESC"),
        }
    }
}

impl_display_for_write_sql!(NullsOrder);
impl WriteSql for NullsOrder {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        s.keyword("NULLS")?;
        match self {
            Self::First => s.keyword("FIRST"),
            Self::Last => s.keyword("LAST"),
        }
    }
}

impl_display_for_write_sql!(ForeignKeyClause);
impl WriteSql for ForeignKeyClause {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.tbl_name.write_sql(s, context)?;
        if !self.columns.is_empty() {
            s.punct("(")?;
            comma(&self.columns, s, context)?;
            s.punct(")")?;
        }
        for arg in &self.args {
            arg.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(RefArg);
impl WriteSql for RefArg {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::OnDelete(ref action) => {
                s.keyword("ON")?;
                s.keyword("DELETE")?;
                action.write_sql(s, context)
            }
            Self::OnInsert(ref action) => {
                s.keyword("ON")?;
                s.keyword("INSERT")?;
                action.write_sql(s, context)
            }
            Self::OnUpdate(ref action) => {
                s.keyword("ON")?;
                s.keyword("UPDATE")?;
                action.write_sql(s, context)
            }
            Self::Match(ref name) => {
                s.keyword("MATCH")?;
                name.write_sql(s, context)
            }
        }
    }
}

impl_display_for_write_sql!(RefAct);
impl WriteSql for RefAct {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::SetNull => {
                s.keyword("SET")?;
                s.keyword("NULL")
            }
            Self::SetDefault => {
                s.keyword("SET")?;
                s.keyword("DEFAULT")
            }
            Self::Cascade => s.keyword("CASCADE"),
            Self::Restrict => s.keyword("RESTRICT"),
            Self::NoAction => {
                s.keyword("NO")?;
                s.keyword("ACTION")
            }
        }
    }
}

impl_display_for_write_sql!(DeferSubclause);
impl WriteSql for DeferSubclause {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if !self.deferrable {
            s.keyword("NOT")?;
        }
        s.keyword("DEFERRABLE")?;
        if let Some(init_deferred) = self.init_deferred {
            init_deferred.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(InitDeferredPred);
impl WriteSql for InitDeferredPred {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        s.keyword("INITIALLY")?;
        match self {
            Self::InitiallyDeferred => s.keyword("DEFERRED"),
            Self::InitiallyImmediate => s.keyword("IMMEDIATE"),
        }
    }
}

impl_display_for_write_sql!(IndexedColumn);
impl WriteSql for IndexedColumn {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.col_name.write_sql(s, context)?;
        if let Some(ref collation_name) = self.collation_name {
            s.keyword("COLLATE")?;
            collation_name.write_sql(s, context)?;
        }
        if let Some(order) = self.order {
            order.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(Indexed);
impl WriteSql for Indexed {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::IndexedBy(ref name) => {
                s.keyword("INDEXED")?;
                s.keyword("BY")?;
                name.write_sql(s, context)
            }
            Self::NotIndexed => {
                s.keyword("NOT")?;
                s.keyword("INDEXED")
            }
        }
    }
}

impl_display_for_write_sql!(SortedColumn);
impl WriteSql for SortedColumn {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.expr.write_sql(s, context)?;
        if let Some(ref order) = self.order {
            order.write_sql(s, context)?;
        }
        if let Some(ref nulls) = self.nulls {
            nulls.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(Limit);
impl WriteSql for Limit {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.keyword("LIMIT")?;
        self.expr.write_sql(s, context)?;
        if let Some(ref offset) = self.offset {
            s.keyword("OFFSET")?;
            offset.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(InsertBody);
impl WriteSql for InsertBody {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Select(select, upsert) => {
                select.write_sql(s, context)?;
                if let Some(upsert) = upsert {
                    upsert.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::DefaultValues => {
                s.keyword("DEFAULT")?;
                s.keyword("VALUES")
            }
        }
    }
}

impl_display_for_write_sql!(Set);
impl WriteSql for Set {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if self.col_names.len() == 1 {
            comma(&self.col_names, s, context)?;
        } else {
            s.punct("(")?;
            comma(&self.col_names, s, context)?;
            s.punct(")")?;
        }
        s.punct("=")?;
        self.expr.write_sql(s, context)
    }
}

impl_display_for_write_sql!(PragmaBody);
impl WriteSql for PragmaBody {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Equals(value) => {
                s.punct("=")?;
                value.write_sql(s, context)
            }
            Self::Call(value) => {
                s.punct("(")?;
                value.write_sql(s, context)?;
                s.punct(")")
            }
        }
    }
}

impl_display_for_write_sql!(TriggerTime);
impl WriteSql for TriggerTime {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Before => s.keyword("BEFORE"),
            Self::After => s.keyword("AFTER"),
            Self::InsteadOf => {
                s.keyword("INSTEAD")?;
                s.keyword("OF")
            }
        }
    }
}

impl_display_for_write_sql!(TriggerEvent);
impl WriteSql for TriggerEvent {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Delete => s.keyword("DELETE"),
            Self::Insert => s.keyword("INSERT"),
            Self::Update => s.keyword("UPDATE"),
            Self::UpdateOf(ref col_names) => {
                s.keyword("UPDATE")?;
                s.keyword("OF")?;
                comma(col_names, s, context)
            }
        }
    }
}

impl_display_for_write_sql!(TriggerCmd);
impl WriteSql for TriggerCmd {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Update {
                or_conflict,
                tbl_name,
                sets,
                from,
                where_clause,
            } => {
                s.keyword("UPDATE")?;
                if let Some(or_conflict) = or_conflict {
                    s.keyword("OR")?;
                    or_conflict.write_sql(s, context)?;
                }
                tbl_name.write_sql(s, context)?;
                s.keyword("SET")?;
                comma(sets, s, context)?;
                if let Some(from) = from {
                    s.keyword("FROM")?;
                    from.write_sql(s, context)?;
                }
                if let Some(where_clause) = where_clause {
                    s.keyword("WHERE")?;
                    where_clause.write_sql(s, context)?;
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
                    s.keyword("REPLACE")?;
                } else {
                    s.keyword("INSERT")?;
                    if let Some(or_conflict) = or_conflict {
                        s.keyword("OR")?;
                        or_conflict.write_sql(s, context)?;
                    }
                }
                s.keyword("INTO")?;
                tbl_name.write_sql(s, context)?;
                if !col_names.is_empty() {
                    s.punct("(")?;
                    comma(col_names, s, context)?;
                    s.punct(")")?;
                }
                select.write_sql(s, context)?;
                if let Some(upsert) = upsert {
                    upsert.write_sql(s, context)?;
                }
                if !returning.is_empty() {
                    s.keyword("RETURNING")?;
                    comma(returning, s, context)?;
                }
                Ok(())
            }
            Self::Delete {
                tbl_name,
                where_clause,
            } => {
                s.keyword("DELETE")?;
                s.keyword("FROM")?;
                tbl_name.write_sql(s, context)?;
                if let Some(where_clause) = where_clause {
                    s.keyword("WHERE")?;
                    where_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Select(select) => select.write_sql(s, context),
        }
    }
}

impl_display_for_write_sql!(ResolveType);
impl WriteSql for ResolveType {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Rollback => s.keyword("ROLLBACK"),
            Self::Abort => s.keyword("ABORT"),
            Self::Fail => s.keyword("FAIL"),
            Self::Ignore => s.keyword("IGNORE"),
            Self::Replace => s.keyword("REPLACE"),
        }
    }
}

impl_display_for_write_sql!(With);
impl WriteSql for With {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.keyword("WITH")?;
        if self.recursive {
            s.keyword("RECURSIVE")?;
        }
        comma(&self.ctes, s, context)
    }
}

impl_display_for_write_sql!(CommonTableExpr);
impl WriteSql for CommonTableExpr {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.tbl_name.write_sql(s, context)?;
        if !self.columns.is_empty() {
            s.punct("(")?;
            comma(&self.columns, s, context)?;
            s.punct(")")?;
        }
        s.keyword("AS")?;
        match self.materialized {
            Materialized::Any => {}
            Materialized::Yes => {
                s.keyword("MATERIALIZED")?;
            }
            Materialized::No => {
                s.keyword("NOT")?;
                s.keyword("MATERIALIZED")?;
            }
        };
        s.punct("(")?;
        self.select.write_sql(s, context)?;
        s.punct(")")
    }
}

impl_display_for_write_sql!(Type);
impl WriteSql for Type {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.text(&self.name)?;
        if let Some(ref size) = self.size {
            s.punct("(")?;
            size.write_sql(s, context)?;
            s.punct(")")?;
        }
        for _ in 0..self.array_dimensions {
            s.punct("[")?;
            s.punct("]")?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(TypeSize);
impl WriteSql for TypeSize {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::MaxSize(size) => size.write_sql(s, context),
            Self::TypeSize(size1, size2) => {
                size1.write_sql(s, context)?;
                s.punct(",")?;
                size2.write_sql(s, context)
            }
        }
    }
}

impl_display_for_write_sql!(TransactionType);
impl WriteSql for TransactionType {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Deferred => s.keyword("DEFERRED"),
            Self::Immediate => s.keyword("IMMEDIATE"),
            Self::Exclusive => s.keyword("EXCLUSIVE"),
            Self::Concurrent => s.keyword("CONCURRENT"),
        }
    }
}

impl_display_for_write_sql!(Upsert);
impl WriteSql for Upsert {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.keyword("ON")?;
        s.keyword("CONFLICT")?;
        if let Some(ref index) = self.index {
            index.write_sql(s, context)?;
        }
        self.do_clause.write_sql(s, context)?;
        if let Some(ref next) = self.next {
            next.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(UpsertIndex);
impl WriteSql for UpsertIndex {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.punct("(")?;
        comma(&self.targets, s, context)?;
        s.punct(")")?;
        if let Some(ref where_clause) = self.where_clause {
            s.keyword("WHERE")?;
            where_clause.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(UpsertDo);
impl WriteSql for UpsertDo {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Set { sets, where_clause } => {
                s.keyword("DO")?;
                s.keyword("UPDATE")?;
                s.keyword("SET")?;
                comma(sets, s, context)?;
                if let Some(where_clause) = where_clause {
                    s.keyword("WHERE")?;
                    where_clause.write_sql(s, context)?;
                }
                Ok(())
            }
            Self::Nothing => {
                s.keyword("DO")?;
                s.keyword("NOTHING")
            }
        }
    }
}

impl_display_for_write_sql!(FunctionTail);
impl WriteSql for FunctionTail {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if let Some(ref filter_clause) = self.filter_clause {
            s.keyword("FILTER")?;
            s.punct("(")?;
            s.keyword("WHERE")?;
            filter_clause.write_sql(s, context)?;
            s.punct(")")?;
        }
        if let Some(ref over_clause) = self.over_clause {
            s.keyword("OVER")?;
            over_clause.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(Over);
impl WriteSql for Over {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Window(ref window) => window.write_sql(s, context),
            Self::Name(ref name) => name.write_sql(s, context),
        }
    }
}

impl_display_for_write_sql!(WindowDef);
impl WriteSql for WindowDef {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.name.write_sql(s, context)?;
        s.keyword("AS")?;
        self.window.write_sql(s, context)
    }
}

impl_display_for_write_sql!(Window);
impl WriteSql for Window {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        s.punct("(")?;
        if let Some(ref base) = self.base {
            base.write_sql(s, context)?;
        }
        if !self.partition_by.is_empty() {
            s.keyword("PARTITION")?;
            s.keyword("BY")?;
            comma(&self.partition_by, s, context)?;
        }
        if !self.order_by.is_empty() {
            s.keyword("ORDER")?;
            s.keyword("BY")?;
            comma(&self.order_by, s, context)?;
        }
        if let Some(ref frame_clause) = self.frame_clause {
            frame_clause.write_sql(s, context)?;
        }
        s.punct(")")
    }
}

impl_display_for_write_sql!(FrameClause);
impl WriteSql for FrameClause {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        self.mode.write_sql(s, context)?;
        if let Some(ref end) = self.end {
            s.keyword("BETWEEN")?;
            self.start.write_sql(s, context)?;
            s.keyword("AND")?;
            end.write_sql(s, context)?;
        } else {
            self.start.write_sql(s, context)?;
        }
        if let Some(ref exclude) = self.exclude {
            s.keyword("EXCLUDE")?;
            exclude.write_sql(s, context)?;
        }
        Ok(())
    }
}

impl_display_for_write_sql!(FrameMode);
impl WriteSql for FrameMode {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::Groups => s.keyword("GROUPS"),
            Self::Range => s.keyword("RANGE"),
            Self::Rows => s.keyword("ROWS"),
        }
    }
}

impl_display_for_write_sql!(FrameBound);
impl WriteSql for FrameBound {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::CurrentRow => {
                s.keyword("CURRENT")?;
                s.keyword("ROW")
            }
            Self::Following(value) => {
                value.write_sql(s, context)?;
                s.keyword("FOLLOWING")
            }
            Self::Preceding(value) => {
                value.write_sql(s, context)?;
                s.keyword("PRECEDING")
            }
            Self::UnboundedFollowing => {
                s.keyword("UNBOUNDED")?;
                s.keyword("FOLLOWING")
            }
            Self::UnboundedPreceding => {
                s.keyword("UNBOUNDED")?;
                s.keyword("PRECEDING")
            }
        }
    }
}

impl_display_for_write_sql!(FrameExclude);
impl WriteSql for FrameExclude {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        match self {
            Self::NoOthers => {
                s.keyword("NO")?;
                s.keyword("OTHERS")
            }
            Self::CurrentRow => {
                s.keyword("CURRENT")?;
                s.keyword("ROW")
            }
            Self::Group => s.keyword("GROUP"),
            Self::Ties => s.keyword("TIES"),
        }
    }
}

/// Emit a parenthesized, comma-separated list of `name Type` pairs.
fn write_type_fields<C: ToSqlContext>(
    fields: &[TypeField],
    s: &mut SqlWriter<'_>,
    context: &C,
) -> fmt::Result {
    s.punct("(")?;
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            s.punct(",")?;
        }
        field.name.write_sql(s, context)?;
        field.field_type.write_sql(s, context)?;
    }
    s.punct(")")
}

fn comma<I, C: ToSqlContext>(items: I, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result
where
    I: IntoIterator,
    I::Item: WriteSql,
{
    s.comma(items, context)
}
