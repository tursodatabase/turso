//! Driver for traces of `MvccPassiveSchema.qnt`: spec object `o` is a table
//! `o(k INTEGER PRIMARY KEY, v INTEGER)`, and its one row has key 1 and the
//! spec commit timestamp of its insert as value. The engine picks root pages
//! and table ids itself, so the driver compares what recovery sees, not the
//! numbers.

use std::collections::BTreeMap;

use super::itf::Step;
use super::*;

/// The DDL transaction between DdlExec and DdlFinish.
struct Ddl {
    tx_id: u64,
    commit: Option<crate::Statement>,
}

pub(super) struct SchemaDriver {
    db: MvccTestDbNoConn,
    io: Arc<dyn IO>,
    ck: Checkpointer,
    ddl_conn: Arc<Connection>,
    ddl_stop: Arc<StopAt>,
    ddl: Option<Ddl>,
    writer: Arc<Connection>,
}

impl Driver for SchemaDriver {
    fn run_step(&mut self, _index: usize, step: &Step) -> StepOutcome {
        match step.action.as_str() {
            "DdlExec" => self.ddl_exec(step)?,
            "DdlPrepare" => self.ddl_prepare()?,
            "DdlFinish" => self.ddl_finish()?,
            "WriteRow" => self.write_row(step)?,
            "CheckpointStart" => self.ck.start(usize::MAX),
            "CheckpointObj" => self.checkpoint_obj(step)?,
            "CheckpointCommit" => self.checkpoint_commit(step)?,
            "CheckpointPublish" => self
                .ck
                .step_until(|state| state == CheckpointState::CheckpointWal),
            "CheckpointBackfill" => self
                .ck
                .step_until(|state| state == CheckpointState::TruncateLogicalLog),
            "CheckpointFinish" => self.ck.finish(),
            other => return refuse(format!("no engine operation for {other}")),
        }
        self.check_cookie()?;
        self.check_recovery(step)
    }
}

impl SchemaDriver {
    pub(super) fn new() -> Self {
        let db = MvccTestDbNoConn::new_with_random_db_passive();
        let setup = db.connect();
        setup
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        setup.execute("PRAGMA mvcc_gc_threshold = -1").unwrap();
        // The spec never gives a table a temporary id -P for a page P. Two
        // dropped tables leave free pages above the metadata table and move
        // the next temporary id below them, so the engine does not either.
        for sql in [
            "CREATE TABLE pad1(x)",
            "CREATE TABLE pad2(x)",
            "PRAGMA wal_checkpoint(TRUNCATE)",
            "DROP TABLE pad1",
            "DROP TABLE pad2",
            "PRAGMA wal_checkpoint(TRUNCATE)",
        ] {
            setup.execute(sql).unwrap();
        }
        setup.close().unwrap();
        let ck = Checkpointer::new(&db, db.get_mvcc_store());
        let (ddl_conn, ddl_stop) = connect_with_stop(&db);
        let writer = db.connect();
        Self {
            io: ck.io.clone(),
            db,
            ck,
            ddl_conn,
            ddl_stop,
            ddl: None,
            writer,
        }
    }

    fn ddl_exec(&mut self, step: &Step) -> StepOutcome {
        let table = step.pick("o").str();
        let sql = match step.pick("kind").str() {
            "create" => format!("CREATE TABLE {table}(k INTEGER PRIMARY KEY, v INTEGER)"),
            "drop" => format!("DROP TABLE {table}"),
            kind => return refuse(format!("no engine operation for DDL {kind}")),
        };
        self.ddl_conn.execute("BEGIN").unwrap();
        self.ddl_conn.execute(&sql).unwrap();
        self.ddl = Some(Ddl {
            tx_id: self.ddl_conn.get_mv_tx_id().unwrap(),
            commit: None,
        });
        Ok(())
    }

    fn ddl_prepare(&mut self) -> StepOutcome {
        let ddl = self.ddl.as_mut().unwrap();
        self.ddl_stop.set(
            CommitYieldPoint::CommitValidation.point(),
            Some(commit_yield_key(ddl.tx_id)),
        );
        let mut commit = self.ddl_conn.prepare("COMMIT").unwrap();
        assert!(
            step_statement(&mut commit, &self.ddl_stop, &self.io),
            "COMMIT of the DDL did not stop after it took its timestamp"
        );
        ddl.commit = Some(commit);
        Ok(())
    }

    fn ddl_finish(&mut self) -> StepOutcome {
        let mut commit = self.ddl.take().unwrap().commit.unwrap();
        self.ddl_stop.clear();
        assert!(
            !step_statement(&mut commit, &self.ddl_stop, &self.io),
            "COMMIT of the DDL must finish"
        );
        Ok(())
    }

    fn write_row(&mut self, step: &Step) -> StepOutcome {
        let table = step.pick("o").str();
        let ts = step.var("ins").get_str(table).field("ts").int();
        self.writer
            .execute(format!("INSERT INTO {table} VALUES (1, {ts})"))
            .unwrap();
        Ok(())
    }

    /// The engine collects every object in one pass over the dirty keys, so
    /// the last CheckpointObj of a pass runs the whole collection. A commit
    /// between two CheckpointObj steps is above the snapshot, so it changes
    /// nothing that the pass collects.
    fn checkpoint_obj(&mut self, step: &Step) -> StepOutcome {
        if step.var("ck").field("todo").items().is_empty() {
            self.ck
                .step_until(|state| state != CheckpointState::CollectTableRows);
        }
        Ok(())
    }

    fn checkpoint_commit(&mut self, step: &Step) -> StepOutcome {
        let spec_wrote = step.var("ck").field("frame").int() != 0;
        let wrote = self.ck.commit();
        if wrote != spec_wrote {
            return fail(
                Failure::Frame,
                format!("spec wrote a frame: {spec_wrote}, engine: {wrote}"),
            );
        }
        Ok(())
    }

    /// CookieNotBehind: the schema cookie in the global header is at least
    /// the version of the shared schema.
    fn check_cookie(&self) -> StepOutcome {
        let header = self.ck.mv.global_header.read().unwrap().schema_cookie.get();
        let schema = self.db.get_db().schema.lock().schema_version;
        if header < schema {
            return fail(
                Failure::CookieBehind,
                format!("header cookie {header}, shared schema version {schema}"),
            );
        }
        Ok(())
    }

    /// RecoveryCorrect of a state where it holds: after a crash, every table
    /// with a logged CREATE and no logged DROP exists, with its row if the
    /// row was written.
    fn check_recovery(&self, step: &Step) -> StepOutcome {
        let ddl_log = step.var("ddlLog").items();
        let logged = |table: &str, kind: &str| {
            ddl_log
                .iter()
                .any(|l| l.field("o").str() == table && l.field("kind").str() == kind)
        };
        let mut want = BTreeMap::new();
        for (table, ins) in step.var("ins").entries() {
            let table = table.str();
            if logged(table, "create") && !logged(table, "drop") {
                let ts = ins.field("ts").int();
                want.insert(table.to_string(), (ts != 0).then_some(ts));
            }
        }
        let tables: Vec<String> = step
            .var("ins")
            .entries()
            .iter()
            .map(|(table, _)| table.str().to_string())
            .collect();
        let path = self.db.path.as_ref().unwrap();
        let got = match crash_probe(path, |conn| recovered_tables(conn, &tables)) {
            Recovery::Read(got) => got,
            Recovery::Error(err) => return fail(Failure::RecoveryError, err),
            Recovery::Panic(msg) => return fail(Failure::RecoveryPanic, msg),
        };
        if got != want {
            return fail(
                Failure::RecoveryRows,
                format!("recovered {got:?}, spec logged {want:?}"),
            );
        }
        Ok(())
    }
}

/// The spec tables in `tables` that exist, each with the value of its row.
fn recovered_tables(
    conn: &Arc<Connection>,
    tables: &[String],
) -> crate::Result<BTreeMap<String, Option<i64>>> {
    let mut recovered = BTreeMap::new();
    for row in query_rows(conn, "SELECT name FROM sqlite_schema WHERE type = 'table'")? {
        let name = row[0].to_string();
        if tables.contains(&name) {
            let rows = query_rows(conn, &format!("SELECT v FROM {name}"))?;
            let value = rows.first().map(|row| row[0].as_int().unwrap());
            recovered.insert(name, value);
        }
    }
    Ok(recovered)
}

/// #11502: a row logged while the first checkpoint of its table runs keeps
/// the temporary table id. After a crash, recovery knows the table only by
/// its page, so the row is lost. When the fix lands, this trace stops
/// failing and the test must expect Conforms.
#[test]
fn issue_11502_trace_loses_row_on_recovery() {
    let replayed = replay(
        "MvccPassiveSchema-issue11502",
        include_str!("traces/MvccPassiveSchema-issue11502.itf.json"),
        Expect::Violation {
            step: 8,
            kind: Failure::RecoveryRows,
        },
        &mut SchemaDriver::new(),
    );
    assert_eq!(replayed, Replay::Done);
}

/// #11499: the checkpoint stages the header at its pager commit, a CREATE
/// commits with the next cookie, and the publish writes the staged header
/// with the old cookie. When the fix lands, this trace stops failing and the
/// test must expect Conforms.
#[test]
fn issue_11499_trace_puts_header_cookie_behind() {
    let replayed = replay(
        "MvccPassiveSchema-issue11499",
        include_str!("traces/MvccPassiveSchema-issue11499.itf.json"),
        Expect::Violation {
            step: 11,
            kind: Failure::CookieBehind,
        },
        &mut SchemaDriver::new(),
    );
    assert_eq!(replayed, Replay::Done);
}
