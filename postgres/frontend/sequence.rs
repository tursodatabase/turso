use std::sync::Arc;
use turso_core::{
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    types::IOResultOr,
    Connection, IOResult, LimboError, Result, Statement, Value,
};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

pub(crate) fn rewrite_dump_source(
    conn: &Arc<Connection>,
    cmd: &mut turso_parser::ast::Cmd,
) -> Result<bool> {
    use turso_parser::ast;
    let ast::Cmd::Stmt(ast::Stmt::Select(select)) = cmd else {
        return Ok(false);
    };
    if select.with.is_some() || !select.body.compounds.is_empty() {
        return Ok(false);
    }
    let ast::OneSelect::Select {
        from: Some(from), ..
    } = &mut select.body.select
    else {
        return Ok(false);
    };
    if !from.joins.is_empty() {
        return Ok(false);
    }
    let ast::SelectTable::Table(name, alias, None) = from.select.as_ref() else {
        return Ok(false);
    };
    let Some(schema) = &name.namespace else {
        return Ok(false);
    };
    let sequence_name = format!("{}.{}", schema.as_str(), name.name.as_str());
    conn.maybe_reparse_schema()?;
    conn.maybe_update_schema();
    if conn.find_sequence(&sequence_name).is_err() {
        return Ok(false);
    }
    let alias = alias
        .clone()
        .or_else(|| Some(ast::As::Elided(name.name.clone())));
    *from.select = ast::SelectTable::TableCall(
        ast::QualifiedName::fullname(
            ast::Name::exact("main".into()),
            ast::Name::exact("pg_turso_sequence_state".into()),
        ),
        vec![Box::new(ast::Expr::Literal(ast::Literal::String(format!(
            "'{}'",
            sequence_name.replace('\'', "''")
        ))))],
        alias,
    );
    Ok(true)
}

#[derive(Debug)]
pub(crate) struct SequenceState;

impl VirtualTableModule for SequenceState {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE pg_turso_sequence_state (last_value BIGINT, is_called BOOLEAN, sequence_name TEXT HIDDEN)".into())
    }

    fn create(&self, _args: &[Value]) -> Result<Self> {
        Ok(Self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for SequenceState {
    type Cursor = SequenceStateCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(SequenceStateCursor {
            conn,
            statement: None,
            row: None,
            name: Value::Null,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let input = constraints
            .iter()
            .position(|c| c.column_index == 2 && c.op == ConstraintOp::Eq && c.usable)
            .ok_or(ResultCode::ConstraintViolation)?;
        Ok(IndexInfo {
            constraint_usages: (0..constraints.len())
                .map(|i| ConstraintUsage {
                    argv_index: (i == input).then_some(1),
                    omit: i == input,
                })
                .collect(),
            estimated_cost: 1.0,
            estimated_rows: 1,
            ..Default::default()
        })
    }
}

pub(crate) struct SequenceStateCursor {
    conn: Arc<Connection>,
    statement: Option<Statement>,
    row: Option<[Value; 2]>,
    name: Value,
}

impl VirtualTableCursor for SequenceStateCursor {
    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        if self.statement.is_none() {
            let [Value::Text(name)] = args else {
                return Err(
                    LimboError::InvalidArgument("sequence name must be text".into()).into(),
                );
            };
            let (database_id, sequence) = self.conn.find_sequence(name.as_str())?;
            let database = self
                .conn
                .list_all_databases()
                .into_iter()
                .find(|(id, _, _)| *id == database_id)
                .expect("sequence database exists")
                .1
                .replace('"', "\"\"");
            let table = format!("__turso_internal_seq_{}", sequence.name).replace('"', "\"\"");
            let direction = if sequence.increment_by > 0 {
                "DESC"
            } else {
                "ASC"
            };
            self.row = None;
            self.name = args[0].clone();
            self.statement = Some(self.conn.prepare_internal(format!(
                "SELECT value, is_called FROM \"{database}\".\"{table}\" ORDER BY value {direction} LIMIT 1"
            ))?);
        }
        let row = &mut self.row;
        match self
            .statement
            .as_mut()
            .expect("sequence read prepared")
            .run_with_row_callback_nonblock(|values| {
                assert!(row.is_none());
                *row = Some([values.get_value(0).clone(), values.get_value(1).clone()]);
                Ok(())
            })? {
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
            IOResult::Done(()) => self.statement = None,
        }
        Ok(IOResult::Done(self.row.is_some()))
    }

    fn next(&mut self) -> IOResultOr<bool> {
        self.row = None;
        Ok(IOResult::Done(false))
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        Ok(IOResult::Done(match column {
            0 | 1 => self.row.as_ref().expect("sequence row available")[column].clone(),
            2 => self.name.clone(),
            _ => unreachable!("invalid sequence state column"),
        }))
    }

    fn rowid(&self) -> i64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_pg_parser::translator::PostgreSQLTranslator;

    #[test]
    fn sequence_dump_source_is_rewritten() {
        let db = crate::session::open_database_with_io(
            Arc::new(turso_core::MemoryIO::new()),
            "sequence-dump-source.db",
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = crate::PgConnection::connect(&db).unwrap();
        conn.execute("CREATE SEQUENCE dump_source START 7").unwrap();
        conn.execute("CREATE TABLE ordinary (last_value bigint, is_called boolean)")
            .unwrap();
        for (sql, expected) in [
            ("SELECT last_value, is_called FROM public.dump_source", true),
            ("SELECT last_value, is_called FROM public.ordinary", false),
            ("SELECT last_value, is_called FROM dump_source", false),
            (
                "WITH x AS (SELECT 1) SELECT last_value, is_called FROM public.dump_source",
                false,
            ),
            (
                "CREATE VIEW v AS SELECT last_value, is_called FROM public.dump_source",
                false,
            ),
        ] {
            let parsed = turso_pg_parser::parse(sql).unwrap();
            let mut translated = PostgreSQLTranslator::new()
                .translate_with_prereqs(&parsed)
                .unwrap();
            assert_eq!(
                rewrite_dump_source(conn.inner(), &mut translated.cmd).unwrap(),
                expected,
                "{sql}"
            );
            if expected {
                let rows = conn
                    .inner()
                    .prepare_translated_cmd(translated.cmd, sql)
                    .unwrap()
                    .run_collect_rows()
                    .unwrap();
                assert_eq!(rows, vec![vec![Value::from_i64(7), Value::from_i64(0)]]);
            }
        }
    }
}
