//! ATTACH and DETACH: SQLite's namespaces, one for each database.

use crate::sync::RwLock;
use crate::turso_assert;
use crate::NamespaceCatalog;
use crate::{Result, MAIN_DB_ID, TEMP_DB_ID};
use rustc_hash::FxHashMap as HashMap;

/// SQLite's namespaces: `main`, `temp` and the name of each attached
/// database.
#[derive(Default)]
pub(crate) struct AttachedNamespaces {
    name_to_index: RwLock<HashMap<String, usize>>,
}

impl NamespaceCatalog for AttachedNamespaces {
    fn database_id(&self, name: &str) -> Option<usize> {
        match name {
            "main" => Some(MAIN_DB_ID),
            "temp" => Some(TEMP_DB_ID),
            _ => self.name_to_index.read().get(name).copied(),
        }
    }

    fn name(&self, database_id: usize) -> Option<String> {
        match database_id {
            MAIN_DB_ID => Some("main".to_string()),
            TEMP_DB_ID => Some("temp".to_string()),
            _ => self
                .name_to_index
                .read()
                .iter()
                .find(|(_, &index)| index == database_id)
                .map(|(name, _)| name.clone()),
        }
    }

    fn others(&self) -> Vec<(usize, String)> {
        let mut namespaces: Vec<_> = self
            .name_to_index
            .read()
            .iter()
            .map(|(name, &index)| (index, name.clone()))
            .collect();
        namespaces.sort();
        namespaces
    }

    fn add(&self, name: &str, database_id: usize) {
        let previous = self
            .name_to_index
            .write()
            .insert(name.to_string(), database_id);
        turso_assert!(
            previous.is_none(),
            "attach: database name already exists",
            { "name": name }
        );
    }

    fn remove(&self, name: &str) -> Option<usize> {
        self.name_to_index.write().remove(name)
    }

    fn find_in_search_order(&self, contains: &mut dyn FnMut(usize) -> bool) -> Option<usize> {
        if contains(MAIN_DB_ID) {
            return Some(MAIN_DB_ID);
        }
        let mut attached: Vec<usize> = self.name_to_index.read().values().copied().collect();
        attached.sort_unstable();
        attached
            .into_iter()
            .find(|&database_id| contains(database_id))
    }

    fn database_for_create(&self) -> Result<usize> {
        Ok(MAIN_DB_ID)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_found_in_main_is_not_looked_up_in_attached_databases() {
        let namespaces = AttachedNamespaces::default();
        namespaces.add("aux", 2);
        let mut looked_up = Vec::new();
        let found = namespaces.find_in_search_order(&mut |database_id| {
            looked_up.push(database_id);
            database_id == MAIN_DB_ID
        });
        assert_eq!(found, Some(MAIN_DB_ID));
        assert_eq!(looked_up, vec![MAIN_DB_ID]);
    }

    #[test]
    fn name_is_looked_up_in_main_and_then_in_attached_databases_by_id() {
        let namespaces = AttachedNamespaces::default();
        namespaces.add("second", 3);
        namespaces.add("first", 2);
        let mut looked_up = Vec::new();
        let found = namespaces.find_in_search_order(&mut |database_id| {
            looked_up.push(database_id);
            false
        });
        assert_eq!(found, None);
        assert_eq!(looked_up, vec![MAIN_DB_ID, 2, 3]);
    }

    #[test]
    fn create_without_a_namespace_uses_main() {
        let namespaces = AttachedNamespaces::default();
        namespaces.add("aux", 2);
        assert_eq!(namespaces.database_for_create().unwrap(), MAIN_DB_ID);
    }
}
