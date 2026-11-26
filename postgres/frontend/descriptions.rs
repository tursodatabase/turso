//! Comments, PostgreSQL's `pg_description` rows, in the `pg_description`
//! table of the main database: one row per comment that `COMMENT ON` stored.
//!
//! A row names its object as PostgreSQL does: `objoid` is the oid of the
//! object, `classoid` the oid of the catalog that lists it, and `objsubid`
//! the number of a column, 0 for the object itself. Relations, their columns
//! and schemas have comments. `COMMENT ON` any other kind of object is
//! refused.
//!
//! Nothing in memory holds these rows. `COMMENT ON` writes them with
//! statements of the engine in the user's transaction, and
//! `obj_description` and `col_description` are queries on the table.
//! A statement that drops a relation or a column deletes its comments in
//! its own bytecode, and one that drops a schema is followed by an engine
//! statement that does.

use std::sync::Arc;

use turso_core::namespace::NamespaceId;
use turso_core::schema::{RelationKind, Schema};
use turso_core::{
    to_u32, CmpInsFlags, CollationSeq, Connection, CursorType, IdxInsertFlags, InsertFlags, Insn,
    LimboError, ProgramBuilder, RegisterOrLiteral, Result, MAIN_DB_ID,
};
use turso_pg_catalog::PgDescription;
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INSUFFICIENT_PRIVILEGE, SQLSTATE_INVALID_SCHEMA_NAME,
    SQLSTATE_UNDEFINED_COLUMN, SQLSTATE_UNDEFINED_TABLE, SQLSTATE_WRONG_OBJECT_TYPE,
};
use turso_pg_parser::translator::{
    is_catalog_table_name, PgCommentObject, PgCommentOnStmt, PG_CLASS_OID, PG_NAMESPACE_OID,
};

use crate::catalog::{key_constraint_names, namespace_oid, PUBLIC_NAMESPACE_OID};
use crate::classes::{class_catalog, RelationClass};
use crate::error::sqlstate_error;
use crate::roles::schema_roles;
use crate::schema_init::TableInit;
use crate::session::relation_lookup_namespaces;

pub(crate) const DESCRIPTION_TABLE_NAME: &str = "pg_description";

const OBJOID_COLUMN: usize = 0;
const CLASSOID_COLUMN: usize = 1;
const OBJSUBID_COLUMN: usize = 2;
const DESCRIPTION_COLUMN: usize = 3;
const DESCRIPTION_COLUMN_COUNT: usize = 4;

/// The unique index of `pg_description` on `(objoid, classoid, objsubid)`,
/// as `postgres.bki` declares it.
pub(crate) const DESCRIPTION_INDEX_NAME: &str = "pg_description_o_c_o_index";

/// The number of columns of a key of the index: the three columns of the
/// index and the rowid.
const DESCRIPTION_INDEX_KEY_LEN: usize = 4;

/// The comment that PostgreSQL gives the public schema.
const INSERT_PUBLIC_SCHEMA_COMMENT_SQL: &str =
    "INSERT INTO pg_description (objoid, classoid, objsubid, description) \
     VALUES (2200, 2615, 0, 'standard public schema')";

// The statement above spells the oids so that it can be a constant.
const _: () = assert!(PUBLIC_NAMESPACE_OID == 2200 && PG_NAMESPACE_OID == 2615);

pub(crate) const TABLE_INIT: TableInit = TableInit {
    table: DESCRIPTION_TABLE_NAME,
    statements: &[
        PgDescription::CREATE_SQL,
        PgDescription::INDEX_SQL[0],
        INSERT_PUBLIC_SCHEMA_COMMENT_SQL,
    ],
};

/// The engine statements that `stmt` runs: they replace the comment of its
/// object, or delete it.
pub(crate) fn comment_statements(
    conn: &Arc<Connection>,
    stmt: &PgCommentOnStmt,
) -> Result<Vec<String>> {
    let object = described_object(conn, &stmt.object)?;
    let mut statements = vec![object.delete_statement()];
    if let Some(comment) = &stmt.comment {
        statements.push(object.insert_statement(comment));
    }
    Ok(statements)
}

/// The engine statement that deletes the comment of the schema `namespace`
/// of `schema`, which the running statement drops. None when the database
/// has no table of comments, which only happens outside the frontend's own
/// open path, such as a core-level open for tests.
pub(crate) fn delete_schema_comment_statement(
    schema: &Schema,
    namespace: NamespaceId,
) -> Option<String> {
    schema.get_btree_table(NamespaceId::ROOT, DESCRIPTION_TABLE_NAME)?;
    let comment = DescribedObject {
        objoid: namespace_oid(schema, namespace),
        classoid: PG_NAMESPACE_OID,
        objsubid: 0,
    };
    Some(comment.delete_statement())
}

/// What `pg_description` names an object by.
struct DescribedObject {
    objoid: i64,
    classoid: i64,
    objsubid: i64,
}

impl DescribedObject {
    fn delete_statement(&self) -> String {
        let Self {
            objoid,
            classoid,
            objsubid,
        } = self;
        format!(
            "DELETE FROM {DESCRIPTION_TABLE_NAME} \
             WHERE objoid = {objoid} AND classoid = {classoid} AND objsubid = {objsubid}"
        )
    }

    fn insert_statement(&self, comment: &str) -> String {
        let Self {
            objoid,
            classoid,
            objsubid,
        } = self;
        format!(
            "INSERT INTO {DESCRIPTION_TABLE_NAME} (objoid, classoid, objsubid, description) \
             VALUES ({objoid}, {classoid}, {objsubid}, '{}')",
            comment.replace('\'', "''")
        )
    }
}

/// Finds the object that a COMMENT ON statement names. Fails as PostgreSQL
/// does when the object does not exist, is of another kind than the
/// statement says, or belongs to a role whose privileges the current role
/// does not have.
fn described_object(conn: &Arc<Connection>, object: &PgCommentObject) -> Result<DescribedObject> {
    let schema = conn.current_schema();
    match object {
        PgCommentObject::Relation {
            kind,
            schema: schema_name,
            name,
        } => {
            let (_, class) = find_relation(conn, &schema, schema_name.as_deref(), name)?;
            if class.relkind != *kind {
                return Err(sqlstate_error(
                    SQLSTATE_WRONG_OBJECT_TYPE,
                    format!("\"{name}\" is not {}", kind.with_article()),
                    None,
                ));
            }
            check_owner(conn, &schema, class, kind.name())?;
            Ok(DescribedObject {
                objoid: class.oid,
                classoid: PG_CLASS_OID,
                objsubid: 0,
            })
        }
        PgCommentObject::Column {
            schema: schema_name,
            table,
            column,
        } => {
            let (namespace, class) = find_relation(conn, &schema, schema_name.as_deref(), table)?;
            let stored_table = match class.relkind {
                RelationKind::Table => schema.get_btree_table(namespace, &table.to_lowercase()),
                _ => None,
            };
            let Some(stored_table) = stored_table else {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "COMMENT ON COLUMN of {} is not supported",
                        class.relkind.with_article()
                    ),
                    None,
                ));
            };
            let Some((position, _)) = stored_table.get_column(column) else {
                return Err(sqlstate_error(
                    SQLSTATE_UNDEFINED_COLUMN,
                    format!("column \"{column}\" of relation \"{table}\" does not exist"),
                    None,
                ));
            };
            check_owner(conn, &schema, class, "relation")?;
            Ok(DescribedObject {
                objoid: class.oid,
                classoid: PG_CLASS_OID,
                objsubid: position as i64 + 1,
            })
        }
        PgCommentObject::Schema(name) => {
            let folded_name = name.to_lowercase();
            let Some(namespace) = conn
                .qualifier(&folded_name)
                .filter(|namespace| !namespace.is_database())
            else {
                if crate::catalog::namespace_exists(conn, &folded_name) {
                    return Err(sqlstate_error(
                        SQLSTATE_FEATURE_NOT_SUPPORTED,
                        format!("COMMENT ON SCHEMA {name} is not supported"),
                        None,
                    ));
                }
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_SCHEMA_NAME,
                    format!("schema \"{name}\" does not exist"),
                    None,
                ));
            };
            if !conn.current_role_is_superuser() {
                return Err(sqlstate_error(
                    SQLSTATE_INSUFFICIENT_PRIVILEGE,
                    format!("must be owner of schema {name}"),
                    None,
                ));
            }
            Ok(DescribedObject {
                objoid: namespace_oid(&schema, namespace.target.id),
                classoid: PG_NAMESPACE_OID,
                objsubid: 0,
            })
        }
        PgCommentObject::Unsupported(kind) => Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("COMMENT ON {kind} is not supported"),
            None,
        )),
    }
}

/// The relation `name` of the main database, in `schema_name` or else in
/// the first namespace of the search path that has it, with its namespace.
fn find_relation<'a>(
    conn: &Arc<Connection>,
    schema: &'a Schema,
    schema_name: Option<&str>,
    name: &str,
) -> Result<(NamespaceId, &'a RelationClass)> {
    if let Some(schema_name) = schema_name {
        if !crate::catalog::namespace_exists(conn, &schema_name.to_lowercase()) {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                format!("schema \"{schema_name}\" does not exist"),
                None,
            ));
        }
    }
    let classes = class_catalog(schema);
    let folded_name = name.to_lowercase();
    let namespaces: Vec<NamespaceId> = relation_lookup_namespaces(conn, schema_name)
        .into_iter()
        .filter(|namespace| namespace.database_id() == MAIN_DB_ID)
        .map(|namespace| namespace.target.id)
        .collect();
    namespaces
        .iter()
        .find_map(|namespace| Some((*namespace, classes?.get_by_name(*namespace, &folded_name)?)))
        .ok_or_else(|| {
            if is_catalog_relation(schema_name, name) {
                return sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "COMMENT ON a system catalog relation is not supported".to_string(),
                    None,
                );
            }
            if is_constraint_index(schema, &namespaces, &folded_name) {
                return sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "COMMENT ON the index of a constraint is not supported".to_string(),
                    None,
                );
            }
            let name = match schema_name {
                Some(schema_name) => format!("{schema_name}.{name}"),
                None => name.to_string(),
            };
            sqlstate_error(
                SQLSTATE_UNDEFINED_TABLE,
                format!("relation \"{name}\" does not exist"),
                None,
            )
        })
}

fn is_catalog_relation(schema_name: Option<&str>, name: &str) -> bool {
    match schema_name.map(str::to_lowercase).as_deref() {
        Some("information_schema") => true,
        Some("pg_catalog") | None => is_catalog_table_name(name),
        Some(_) => false,
    }
}

/// Whether `name` is the PostgreSQL name of the index of a PRIMARY KEY or
/// UNIQUE constraint of a table in one of `namespaces`.
fn is_constraint_index(schema: &Schema, namespaces: &[NamespaceId], name: &str) -> bool {
    let Some(classes) = class_catalog(schema) else {
        return false;
    };
    classes
        .classes()
        .filter(|class| {
            class.relkind == RelationKind::Table && namespaces.contains(&class.relnamespace)
        })
        .filter_map(|class| schema.get_btree_table(class.relnamespace, &class.relname))
        .flat_map(|table| key_constraint_names(&table))
        .any(|constraint| constraint.eq_ignore_ascii_case(name))
}

/// Fails unless the current role has the privileges of the owner of
/// `class`, which the error names as `kind_name` and its name.
fn check_owner(
    conn: &Connection,
    schema: &Schema,
    class: &RelationClass,
    kind_name: &str,
) -> Result<()> {
    if conn.current_role_is_superuser()
        || schema_roles(schema).has_privileges_of(conn.current_role(), class.relowner)
    {
        return Ok(());
    }
    Err(sqlstate_error(
        SQLSTATE_INSUFFICIENT_PRIVILEGE,
        format!("must be owner of {kind_name} {}", class.relname),
        None,
    ))
}

/// What the running statement removes from a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dropped {
    /// The relation itself: its comment and the comments of its columns go.
    Relation,
    /// The column with this number. Its comment goes, and the comments of
    /// the columns after it move to the numbers those columns now have.
    Column(i64),
}

/// Deletes the comments that go with what the running statement drops from
/// the relation with oid `oid`.
///
/// The bytecode writes the table through a cursor, which does not maintain
/// the index as a DELETE or an UPDATE does. So each row that it deletes or
/// changes also gets its index key deleted, and a changed row gets its new
/// key added.
pub(crate) fn emit_delete_comments(
    program: &mut ProgramBuilder,
    schema: &Schema,
    oid: i64,
    dropped: Dropped,
) -> Result<()> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, DESCRIPTION_TABLE_NAME) else {
        return Ok(());
    };
    let index = schema
        .get_index(
            NamespaceId::ROOT,
            DESCRIPTION_TABLE_NAME,
            DESCRIPTION_INDEX_NAME,
        )
        .cloned()
        .ok_or_else(|| {
            LimboError::Corrupt(format!(
                "{DESCRIPTION_TABLE_NAME} has no index {DESCRIPTION_INDEX_NAME}"
            ))
        })?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(Arc::clone(&table)));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    let index_cursor_id = program.alloc_cursor_index(None, &index)?;
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor_id,
        root_page: RegisterOrLiteral::Literal(index.root_page),
        db: MAIN_DB_ID,
    });
    let objoid_reg = program.alloc_register();
    program.emit_int(oid, objoid_reg);
    let classoid_reg = program.alloc_register();
    program.emit_int(PG_CLASS_OID, classoid_reg);

    let loop_end = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id,
        pc_if_empty: loop_end,
    });
    let loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);
    let next = program.allocate_label();

    // The index key of the current row: objoid, classoid, objsubid and the
    // rowid, in the order of the index.
    let key_reg = program.alloc_registers(DESCRIPTION_INDEX_KEY_LEN);
    for (column, expected_reg) in [(OBJOID_COLUMN, objoid_reg), (CLASSOID_COLUMN, classoid_reg)] {
        program.emit_column_or_rowid(cursor_id, column, key_reg + column);
        program.emit_insn(Insn::Ne {
            lhs: key_reg + column,
            rhs: expected_reg,
            target_pc: next,
            flags: CmpInsFlags::default().jump_if_null(),
            collation: CollationSeq::for_internal_values(),
        });
    }
    let objsubid_reg = key_reg + OBJSUBID_COLUMN;
    program.emit_column_or_rowid(cursor_id, OBJSUBID_COLUMN, objsubid_reg);
    let rowid_reg = key_reg + DESCRIPTION_INDEX_KEY_LEN - 1;
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: rowid_reg,
    });
    match dropped {
        Dropped::Relation => {
            emit_delete_index_key(program, index_cursor_id, key_reg);
            emit_delete_current_row(program, cursor_id);
        }
        Dropped::Column(number) => {
            let number_reg = program.alloc_register();
            program.emit_int(number, number_reg);
            program.emit_insn(Insn::Lt {
                lhs: objsubid_reg,
                rhs: number_reg,
                target_pc: next,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: CollationSeq::for_internal_values(),
            });
            let later_column = program.allocate_label();
            program.emit_insn(Insn::Gt {
                lhs: objsubid_reg,
                rhs: number_reg,
                target_pc: later_column,
                flags: CmpInsFlags::default(),
                collation: CollationSeq::for_internal_values(),
            });
            emit_delete_index_key(program, index_cursor_id, key_reg);
            emit_delete_current_row(program, cursor_id);
            program.emit_insn(Insn::Goto { target_pc: next });

            // A later column gets the number before its own. The index
            // loses the key with the old number and gets the key with the
            // new one. The new key can be equal, in its three columns, to
            // the key of a row that the loop has not reached. That row gets
            // a new key too, or is deleted, before the statement ends, so
            // the new key is added without a check for a duplicate.
            program.preassign_label_to_next_insn(later_column);
            emit_delete_index_key(program, index_cursor_id, key_reg);
            program.emit_insn(Insn::AddImm {
                register: objsubid_reg,
                value: -1,
            });
            let key_record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(key_reg),
                count: to_u32(DESCRIPTION_INDEX_KEY_LEN),
                dest_reg: to_u32(key_record_reg),
                index_name: Some(DESCRIPTION_INDEX_NAME.to_string()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: index_cursor_id,
                record_reg: key_record_reg,
                unpacked_start: Some(key_reg),
                unpacked_count: Some(to_u32(DESCRIPTION_INDEX_KEY_LEN)),
                flags: IdxInsertFlags::new(),
            });

            let row_reg = program.alloc_registers(DESCRIPTION_COLUMN_COUNT);
            for column in [OBJOID_COLUMN, CLASSOID_COLUMN, OBJSUBID_COLUMN] {
                program.emit_insn(Insn::Copy {
                    src_reg: key_reg + column,
                    dst_reg: row_reg + column,
                    extra_amount: 0,
                });
            }
            program.emit_column_or_rowid(
                cursor_id,
                DESCRIPTION_COLUMN,
                row_reg + DESCRIPTION_COLUMN,
            );
            emit_replace_current_row(program, cursor_id, row_reg, rowid_reg);
        }
    }

    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: loop_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(loop_end);
    Ok(())
}

/// Deletes the key in the registers from `key_reg` from the index.
fn emit_delete_index_key(program: &mut ProgramBuilder, index_cursor_id: usize, key_reg: usize) {
    program.emit_insn(Insn::IdxDelete {
        start_reg: key_reg,
        num_regs: DESCRIPTION_INDEX_KEY_LEN,
        cursor_id: index_cursor_id,
        raise_error_if_no_matching_entry: true,
    });
}

fn emit_delete_current_row(program: &mut ProgramBuilder, cursor_id: usize) {
    // Marked as part of an update so that changes() does not count it.
    program.emit_insn(Insn::Delete {
        cursor_id,
        table_name: DESCRIPTION_TABLE_NAME.to_string(),
        is_part_of_update: true,
    });
}

/// Writes the columns in the registers from `row_reg` over the row the
/// cursor is on, whose rowid is in `rowid_reg`.
fn emit_replace_current_row(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    row_reg: usize,
    rowid_reg: usize,
) {
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(row_reg),
        count: to_u32(DESCRIPTION_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_all_change_counts(),
        table_name: DESCRIPTION_TABLE_NAME.to_string(),
    });
}
