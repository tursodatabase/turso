use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::Column;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AlterTable {
    pub table_name: String,
    pub alter_table_type: AlterTableType,
}

// TODO: in the future maybe use parser AST's when we test almost the entire SQL spectrum
// so we can repeat less code
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, strum::EnumDiscriminants)]
pub enum AlterTableType {
    /// `RENAME TO`: new table name
    RenameTo { new_name: String },
    /// `ADD COLUMN`
    AddColumn { column: Column },
    /// `ALTER COLUMN`
    AlterColumn { old: String, new: Column },
    /// `RENAME COLUMN`
    RenameColumn {
        /// old name
        old: String,
        /// new name
        new: String,
    },
    /// `DROP COLUMN`
    DropColumn { column_name: String },
}

impl Display for AlterTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ALTER TABLE {} {}",
            self.table_name, self.alter_table_type
        )
    }
}

impl Display for AlterTableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableType::RenameTo { new_name } => write!(f, "RENAME TO {new_name}"),
            AlterTableType::AddColumn { column } => write!(f, "ADD COLUMN {column}"),
            AlterTableType::AlterColumn { old, new } => write!(f, "ALTER COLUMN {old} TO {new}"),
            AlterTableType::RenameColumn { old, new } => write!(f, "RENAME COLUMN {old} TO {new}"),
            AlterTableType::DropColumn { column_name } => write!(f, "DROP COLUMN {column_name}"),
        }
    }
}
