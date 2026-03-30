use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DropIndex {
    pub index_name: String,
    pub table_name: String,
}

impl std::fmt::Display for DropIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // In SQLite, DROP INDEX syntax uses: DROP INDEX [db.]index_name
        // The database prefix goes on the index name.
        let db_prefix = self
            .table_name
            .split_once('.')
            .map(|(db, _)| format!("{db}."))
            .unwrap_or_default();
        write!(f, "DROP INDEX {}{}", db_prefix, self.index_name)
    }
}
