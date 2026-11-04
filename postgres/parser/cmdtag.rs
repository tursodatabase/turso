use pg_query::{
    protobuf::{DiscardMode, ObjectType, TransactionStmtKind, VariableSetKind},
    NodeEnum, ParseResult,
};

/// The line that reports what a statement did when it completes, such as
/// `INSERT 0 3` or `CREATE TABLE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandTag {
    Unknown,
    AlterDomain,
    AlterExtension,
    AlterFunction,
    AlterIndex,
    AlterMaterializedView,
    AlterProcedure,
    AlterRole,
    AlterSchema,
    AlterSequence,
    AlterTable,
    AlterTrigger,
    AlterType,
    AlterView,
    Analyze,
    Begin,
    Comment,
    Commit,
    CommitPrepared,
    Copy,
    CreateDomain,
    CreateExtension,
    CreateFunction,
    CreateIndex,
    CreateMaterializedView,
    CreateProcedure,
    CreateRole,
    CreateSchema,
    CreateSequence,
    CreateTable,
    CreateTableAs,
    CreateTrigger,
    CreateType,
    CreateView,
    Delete,
    DiscardAll,
    DiscardPlans,
    DiscardSequences,
    DiscardTemp,
    DropDomain,
    DropExtension,
    DropFunction,
    DropIndex,
    DropMaterializedView,
    DropProcedure,
    DropRole,
    DropSchema,
    DropSequence,
    DropTable,
    DropTrigger,
    DropType,
    DropView,
    Explain,
    Grant,
    GrantRole,
    Insert,
    LockTable,
    Merge,
    PrepareTransaction,
    RefreshMaterializedView,
    Release,
    Reset,
    Revoke,
    RevokeRole,
    Rollback,
    RollbackPrepared,
    Savepoint,
    Select,
    Set,
    Show,
    StartTransaction,
    TruncateTable,
    Update,
    Vacuum,
}

impl CommandTag {
    /// The tag of the first statement in `parsed`, as PostgreSQL reports it
    /// when the statement completes.
    pub fn of(parsed: &ParseResult) -> Self {
        let Some(stmt) = crate::first_statement(parsed) else {
            return Self::Unknown;
        };
        match stmt {
            NodeEnum::InsertStmt(_) => Self::Insert,
            NodeEnum::UpdateStmt(_) => Self::Update,
            NodeEnum::DeleteStmt(_) => Self::Delete,
            NodeEnum::MergeStmt(_) => Self::Merge,
            NodeEnum::SelectStmt(_) => Self::Select,
            NodeEnum::CopyStmt(_) => Self::Copy,
            // CREATE TABLE AS and CREATE MATERIALIZED VIEW report the rows they
            // inserted as SELECT, unless WITH NO DATA skips the insert.
            NodeEnum::CreateTableAsStmt(create) => {
                if !create.into.as_ref().is_some_and(|into| into.skip_data) {
                    Self::Select
                } else if create.objtype() == ObjectType::ObjectMatview {
                    Self::CreateMaterializedView
                } else {
                    Self::CreateTableAs
                }
            }
            NodeEnum::TransactionStmt(txn) => match txn.kind() {
                TransactionStmtKind::TransStmtBegin => Self::Begin,
                TransactionStmtKind::TransStmtStart => Self::StartTransaction,
                TransactionStmtKind::TransStmtCommit => Self::Commit,
                TransactionStmtKind::TransStmtRollback
                | TransactionStmtKind::TransStmtRollbackTo => Self::Rollback,
                TransactionStmtKind::TransStmtSavepoint => Self::Savepoint,
                TransactionStmtKind::TransStmtRelease => Self::Release,
                TransactionStmtKind::TransStmtPrepare => Self::PrepareTransaction,
                TransactionStmtKind::TransStmtCommitPrepared => Self::CommitPrepared,
                TransactionStmtKind::TransStmtRollbackPrepared => Self::RollbackPrepared,
                TransactionStmtKind::Undefined => Self::Unknown,
            },
            NodeEnum::CreateStmt(_) => Self::CreateTable,
            NodeEnum::CreateSchemaStmt(_) => Self::CreateSchema,
            NodeEnum::ViewStmt(_) => Self::CreateView,
            NodeEnum::IndexStmt(_) => Self::CreateIndex,
            NodeEnum::CreateSeqStmt(_) => Self::CreateSequence,
            NodeEnum::CreateEnumStmt(_) | NodeEnum::CompositeTypeStmt(_) => Self::CreateType,
            NodeEnum::CreateDomainStmt(_) => Self::CreateDomain,
            NodeEnum::CreateTrigStmt(_) => Self::CreateTrigger,
            NodeEnum::CreateFunctionStmt(function) if function.is_procedure => {
                Self::CreateProcedure
            }
            NodeEnum::CreateFunctionStmt(_) => Self::CreateFunction,
            NodeEnum::CreateExtensionStmt(_) => Self::CreateExtension,
            NodeEnum::CreateRoleStmt(_) => Self::CreateRole,
            NodeEnum::AlterRoleStmt(_) => Self::AlterRole,
            NodeEnum::DropRoleStmt(_) => Self::DropRole,
            NodeEnum::AlterSeqStmt(_) => Self::AlterSequence,
            NodeEnum::AlterEnumStmt(_) => Self::AlterType,
            NodeEnum::AlterDomainStmt(_) => Self::AlterDomain,
            NodeEnum::AlterTableStmt(alter) => Self::alter(alter.objtype()),
            // Renaming a column alters the relation that has it.
            NodeEnum::RenameStmt(rename) => Self::alter(match rename.rename_type() {
                ObjectType::ObjectColumn => rename.relation_type(),
                object => object,
            }),
            NodeEnum::DropStmt(drop) => Self::drop(drop.remove_type()),
            NodeEnum::TruncateStmt(_) => Self::TruncateTable,
            NodeEnum::CommentStmt(_) => Self::Comment,
            NodeEnum::GrantStmt(grant) if grant.is_grant => Self::Grant,
            NodeEnum::GrantStmt(_) => Self::Revoke,
            NodeEnum::GrantRoleStmt(grant) if grant.is_grant => Self::GrantRole,
            NodeEnum::GrantRoleStmt(_) => Self::RevokeRole,
            NodeEnum::VariableSetStmt(set) => match set.kind() {
                VariableSetKind::VarReset | VariableSetKind::VarResetAll => Self::Reset,
                _ => Self::Set,
            },
            NodeEnum::VariableShowStmt(_) => Self::Show,
            NodeEnum::DiscardStmt(discard) => match discard.target() {
                DiscardMode::DiscardAll => Self::DiscardAll,
                DiscardMode::DiscardPlans => Self::DiscardPlans,
                DiscardMode::DiscardSequences => Self::DiscardSequences,
                DiscardMode::DiscardTemp => Self::DiscardTemp,
                DiscardMode::Undefined => Self::Unknown,
            },
            NodeEnum::LockStmt(_) => Self::LockTable,
            NodeEnum::RefreshMatViewStmt(_) => Self::RefreshMaterializedView,
            NodeEnum::ExplainStmt(_) => Self::Explain,
            NodeEnum::VacuumStmt(vacuum) if vacuum.is_vacuumcmd => Self::Vacuum,
            NodeEnum::VacuumStmt(_) => Self::Analyze,
            _ => Self::Unknown,
        }
    }

    /// The text of the tag, without the row count.
    pub fn name(self) -> &'static str {
        match self {
            Self::Unknown => "???",
            Self::AlterDomain => "ALTER DOMAIN",
            Self::AlterExtension => "ALTER EXTENSION",
            Self::AlterFunction => "ALTER FUNCTION",
            Self::AlterIndex => "ALTER INDEX",
            Self::AlterMaterializedView => "ALTER MATERIALIZED VIEW",
            Self::AlterProcedure => "ALTER PROCEDURE",
            Self::AlterRole => "ALTER ROLE",
            Self::AlterSchema => "ALTER SCHEMA",
            Self::AlterSequence => "ALTER SEQUENCE",
            Self::AlterTable => "ALTER TABLE",
            Self::AlterTrigger => "ALTER TRIGGER",
            Self::AlterType => "ALTER TYPE",
            Self::AlterView => "ALTER VIEW",
            Self::Analyze => "ANALYZE",
            Self::Begin => "BEGIN",
            Self::Comment => "COMMENT",
            Self::Commit => "COMMIT",
            Self::CommitPrepared => "COMMIT PREPARED",
            Self::Copy => "COPY",
            Self::CreateDomain => "CREATE DOMAIN",
            Self::CreateExtension => "CREATE EXTENSION",
            Self::CreateFunction => "CREATE FUNCTION",
            Self::CreateIndex => "CREATE INDEX",
            Self::CreateMaterializedView => "CREATE MATERIALIZED VIEW",
            Self::CreateProcedure => "CREATE PROCEDURE",
            Self::CreateRole => "CREATE ROLE",
            Self::CreateSchema => "CREATE SCHEMA",
            Self::CreateSequence => "CREATE SEQUENCE",
            Self::CreateTable => "CREATE TABLE",
            Self::CreateTableAs => "CREATE TABLE AS",
            Self::CreateTrigger => "CREATE TRIGGER",
            Self::CreateType => "CREATE TYPE",
            Self::CreateView => "CREATE VIEW",
            Self::Delete => "DELETE",
            Self::DiscardAll => "DISCARD ALL",
            Self::DiscardPlans => "DISCARD PLANS",
            Self::DiscardSequences => "DISCARD SEQUENCES",
            Self::DiscardTemp => "DISCARD TEMP",
            Self::DropDomain => "DROP DOMAIN",
            Self::DropExtension => "DROP EXTENSION",
            Self::DropFunction => "DROP FUNCTION",
            Self::DropIndex => "DROP INDEX",
            Self::DropMaterializedView => "DROP MATERIALIZED VIEW",
            Self::DropProcedure => "DROP PROCEDURE",
            Self::DropRole => "DROP ROLE",
            Self::DropSchema => "DROP SCHEMA",
            Self::DropSequence => "DROP SEQUENCE",
            Self::DropTable => "DROP TABLE",
            Self::DropTrigger => "DROP TRIGGER",
            Self::DropType => "DROP TYPE",
            Self::DropView => "DROP VIEW",
            Self::Explain => "EXPLAIN",
            Self::Grant => "GRANT",
            Self::GrantRole => "GRANT ROLE",
            Self::Insert => "INSERT",
            Self::LockTable => "LOCK TABLE",
            Self::Merge => "MERGE",
            Self::PrepareTransaction => "PREPARE TRANSACTION",
            Self::RefreshMaterializedView => "REFRESH MATERIALIZED VIEW",
            Self::Release => "RELEASE",
            Self::Reset => "RESET",
            Self::Revoke => "REVOKE",
            Self::RevokeRole => "REVOKE ROLE",
            Self::Rollback => "ROLLBACK",
            Self::RollbackPrepared => "ROLLBACK PREPARED",
            Self::Savepoint => "SAVEPOINT",
            Self::Select => "SELECT",
            Self::Set => "SET",
            Self::Show => "SHOW",
            Self::StartTransaction => "START TRANSACTION",
            Self::TruncateTable => "TRUNCATE TABLE",
            Self::Update => "UPDATE",
            Self::Vacuum => "VACUUM",
        }
    }

    /// Whether the tag ends with the number of rows the statement processed,
    /// as in `UPDATE 3`.
    pub fn has_row_count(self) -> bool {
        matches!(
            self,
            Self::Copy | Self::Delete | Self::Insert | Self::Merge | Self::Select | Self::Update
        )
    }

    fn alter(object: ObjectType) -> Self {
        match object {
            ObjectType::ObjectTable
            | ObjectType::ObjectColumn
            | ObjectType::ObjectTabconstraint => Self::AlterTable,
            ObjectType::ObjectIndex => Self::AlterIndex,
            ObjectType::ObjectSequence => Self::AlterSequence,
            ObjectType::ObjectView => Self::AlterView,
            ObjectType::ObjectMatview => Self::AlterMaterializedView,
            ObjectType::ObjectType | ObjectType::ObjectAttribute => Self::AlterType,
            ObjectType::ObjectDomain | ObjectType::ObjectDomconstraint => Self::AlterDomain,
            ObjectType::ObjectSchema => Self::AlterSchema,
            ObjectType::ObjectTrigger => Self::AlterTrigger,
            ObjectType::ObjectFunction => Self::AlterFunction,
            ObjectType::ObjectProcedure => Self::AlterProcedure,
            ObjectType::ObjectRole => Self::AlterRole,
            ObjectType::ObjectExtension => Self::AlterExtension,
            _ => Self::Unknown,
        }
    }

    fn drop(object: ObjectType) -> Self {
        match object {
            ObjectType::ObjectTable => Self::DropTable,
            ObjectType::ObjectIndex => Self::DropIndex,
            ObjectType::ObjectSequence => Self::DropSequence,
            ObjectType::ObjectView => Self::DropView,
            ObjectType::ObjectMatview => Self::DropMaterializedView,
            ObjectType::ObjectType => Self::DropType,
            ObjectType::ObjectDomain => Self::DropDomain,
            ObjectType::ObjectSchema => Self::DropSchema,
            ObjectType::ObjectTrigger => Self::DropTrigger,
            ObjectType::ObjectFunction => Self::DropFunction,
            ObjectType::ObjectProcedure => Self::DropProcedure,
            ObjectType::ObjectExtension => Self::DropExtension,
            _ => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(sql: &str) -> CommandTag {
        CommandTag::of(&crate::parse(sql).unwrap())
    }

    #[test]
    fn tag_ignores_comments_and_line_breaks() {
        for (sql, expected) in [
            ("-- a comment\nINSERT INTO t VALUES (1)", CommandTag::Insert),
            (
                "/* a /* nested */ comment */ UPDATE t SET x = 1",
                CommandTag::Update,
            ),
            ("CREATE\nVIEW v AS SELECT 1", CommandTag::CreateView),
            ("DROP /* a comment */ INDEX i", CommandTag::DropIndex),
            (
                "CREATE TABLE t AS SELECT 1 AS x -- a comment\r WITH NO DATA",
                CommandTag::CreateTableAs,
            ),
            (
                "CREATE TABLE t AS SELECT 1 WITH NO DATA -- a comment",
                CommandTag::CreateTableAs,
            ),
            ("-- a comment\nSET search_path TO s", CommandTag::Set),
        ] {
            assert_eq!(tag(sql), expected, "{sql}");
        }
    }

    #[test]
    fn tag_names_the_kind_of_object() {
        for (sql, expected) in [
            ("CREATE UNIQUE INDEX i ON t (x)", CommandTag::CreateIndex),
            (
                "CREATE OR REPLACE VIEW v AS SELECT 1",
                CommandTag::CreateView,
            ),
            ("CREATE TEMP VIEW v AS SELECT 1", CommandTag::CreateView),
            ("CREATE SEQUENCE s", CommandTag::CreateSequence),
            (
                "CREATE TYPE mood AS ENUM ('sad', 'ok')",
                CommandTag::CreateType,
            ),
            ("CREATE DOMAIN d AS int", CommandTag::CreateDomain),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
                CommandTag::CreateTrigger,
            ),
            ("DROP SEQUENCE s", CommandTag::DropSequence),
            ("DROP TYPE mood", CommandTag::DropType),
            (
                "DROP MATERIALIZED VIEW mv",
                CommandTag::DropMaterializedView,
            ),
            ("ALTER INDEX i RENAME TO j", CommandTag::AlterIndex),
            ("ALTER TABLE t RENAME COLUMN a TO b", CommandTag::AlterTable),
            ("ALTER VIEW v RENAME COLUMN a TO b", CommandTag::AlterView),
            ("ALTER SEQUENCE s RESTART", CommandTag::AlterSequence),
            ("TRUNCATE t", CommandTag::TruncateTable),
            ("GRANT SELECT ON t TO alice", CommandTag::Grant),
            ("REVOKE SELECT ON t FROM alice", CommandTag::Revoke),
            ("START TRANSACTION", CommandTag::StartTransaction),
            ("ROLLBACK TO SAVEPOINT sp", CommandTag::Rollback),
            ("RESET ALL", CommandTag::Reset),
            ("RESET ROLE", CommandTag::Reset),
            ("DISCARD PLANS", CommandTag::DiscardPlans),
            (
                "REFRESH MATERIALIZED VIEW mv",
                CommandTag::RefreshMaterializedView,
            ),
            (
                "CREATE MATERIALIZED VIEW mv AS SELECT 1 WITH NO DATA",
                CommandTag::CreateMaterializedView,
            ),
        ] {
            assert_eq!(tag(sql), expected, "{sql}");
        }
    }

    #[test]
    fn statements_that_fill_a_new_table_report_select() {
        for sql in [
            "CREATE TABLE public.t AS SELECT 1",
            "CREATE MATERIALIZED VIEW mv AS SELECT 1",
            "SELECT 1 INTO t",
        ] {
            assert_eq!(tag(sql), CommandTag::Select, "{sql}");
        }
    }
}
