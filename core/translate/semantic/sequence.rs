//! Sequence catalog resolution shared by syntax and stored expressions.

use turso_parser::ast;

use super::{
    hir::{self, CatalogObject, DatabaseId},
    Analyzer, CatalogObjectKind,
};
use crate::{
    schema::{Table, AUTOINCREMENT_SEQ_PREFIX, SQLITE_SEQUENCE_TABLE_NAME},
    sync::Arc,
    LimboError, Result, MAIN_DB_ID,
};

impl Analyzer<'_, '_> {
    /// Freeze every catalog object needed to execute NEXTVAL or SETVAL.
    /// Callers only interpret their own expression representation far enough
    /// to supply the user-written sequence name.
    pub(super) fn resolve_sequence_catalog_operation(
        &mut self,
        kind: hir::SequenceOperationKind,
        user_name: String,
    ) -> Result<hir::SequenceOperation> {
        let (database_id, sequence_name) =
            if let Some((database_name, sequence_name)) = user_name.split_once('.') {
                let qualified = ast::QualifiedName {
                    db_name: Some(ast::Name::from_string(database_name)),
                    name: ast::Name::from_string(sequence_name),
                    alias: None,
                };
                (
                    self.context().resolve_database_id(&qualified)?,
                    sequence_name,
                )
            } else {
                (MAIN_DB_ID, user_name.as_str())
            };
        let normalized_name = crate::util::normalize_ident(sequence_name);
        let backing_table_name =
            crate::translate::sequence::sequence_backing_table_name(&normalized_name);
        let (backing_table, sqlite_sequence, sequence, schema_cookie) = {
            let schema = self.context().schema(database_id).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "resolved sequence database {database_id} is absent from the catalog snapshot"
                ))
            })?;
            let backing_table = schema.get_btree_table(&backing_table_name).ok_or_else(|| {
                LimboError::ParseError(format!("sequence \"{user_name}\" does not exist"))
            })?;
            let sequence = schema
                .get_sequence(&normalized_name)
                .cloned()
                .ok_or_else(|| {
                    LimboError::ParseError(format!("sequence \"{user_name}\" does not exist"))
                })?;
            let sqlite_sequence = normalized_name
                .strip_prefix(AUTOINCREMENT_SEQ_PREFIX)
                .map(|_| {
                    schema
                        .get_btree_table(SQLITE_SEQUENCE_TABLE_NAME)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "resolved AUTOINCREMENT sequence {normalized_name:?} has no sqlite_sequence table"
                            ))
                        })
                })
                .transpose()?;
            (
                backing_table,
                sqlite_sequence,
                sequence,
                schema.schema_version,
            )
        };
        let object_id = self.catalog_object_id(
            Some(database_id),
            CatalogObjectKind::Table,
            backing_table_name,
        );
        let backing_table = CatalogObject::new(
            object_id,
            self.context().snapshot(),
            Some(DatabaseId::new(database_id)),
            Arc::new(Table::BTree(backing_table)),
        );
        let sqlite_sequence = sqlite_sequence.map(|table| {
            let object_id = self.catalog_object_id(
                Some(database_id),
                CatalogObjectKind::Table,
                SQLITE_SEQUENCE_TABLE_NAME.to_string(),
            );
            CatalogObject::new(
                object_id,
                self.context().snapshot(),
                Some(DatabaseId::new(database_id)),
                Arc::new(Table::BTree(table)),
            )
        });

        Ok(hir::SequenceOperation {
            kind,
            database: DatabaseId::new(database_id),
            user_name,
            normalized_name,
            backing_table,
            sqlite_sequence,
            sequence,
            schema_cookie,
        })
    }
}
