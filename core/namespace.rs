//! The namespaces of a connection: the names that qualify object names in
//! SQL, like `aux` in `aux.t`.
//!
//! Every namespace refers to a database, the storage that holds its objects.
//! `main` and `temp` always exist. ATTACH adds a namespace for the attached
//! database, and a frontend can add more names for a database, like `public`
//! for main. A name without a namespace is looked up through the connection's
//! search path, an ordered list of namespace names.

use crate::sync::RwLock;
use crate::{turso_assert, MAIN_DB_ID, TEMP_DB_ID};
use rustc_hash::FxHashMap as HashMap;

/// A namespace of a database's schema. Every database has the root
/// namespace, which has the objects of the database itself; a schema can
/// have more namespaces, each with its own objects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(u32);

impl NamespaceId {
    pub const ROOT: NamespaceId = NamespaceId(0);

    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }

    pub const fn is_root(self) -> bool {
        self.0 == 0
    }
}

/// A namespace of one of the connection's databases: the place a name
/// resolves to, where the object is looked up by its name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NamespaceRef {
    pub database_id: usize,
    pub id: NamespaceId,
}

impl NamespaceRef {
    pub const fn root_of(database_id: usize) -> Self {
        Self {
            database_id,
            id: NamespaceId::ROOT,
        }
    }
}

#[derive(Default)]
pub struct Namespaces {
    name_to_database: RwLock<HashMap<String, usize>>,
}

impl Namespaces {
    /// The database that namespace `name` refers to. `name` is normalized.
    pub fn database_id(&self, name: &str) -> Option<usize> {
        match name {
            "main" => Some(MAIN_DB_ID),
            "temp" => Some(TEMP_DB_ID),
            _ => self.name_to_database.read().get(name).copied(),
        }
    }

    /// The name of the namespace that refers to database `database_id`.
    pub fn name(&self, database_id: usize) -> Option<String> {
        match database_id {
            MAIN_DB_ID => Some("main".to_string()),
            TEMP_DB_ID => Some("temp".to_string()),
            _ => self
                .name_to_database
                .read()
                .iter()
                .find(|(_, &id)| id == database_id)
                .map(|(name, _)| name.clone()),
        }
    }

    /// The namespaces of the databases other than main and temp, with the
    /// databases they refer to, ordered by database.
    pub fn others(&self) -> Vec<(usize, String)> {
        let mut namespaces: Vec<_> = self
            .name_to_database
            .read()
            .iter()
            .filter(|(_, &id)| id != MAIN_DB_ID && id != TEMP_DB_ID)
            .map(|(name, &id)| (id, name.clone()))
            .collect();
        namespaces.sort();
        namespaces
    }

    /// Adds namespace `name`, which refers to database `database_id`.
    pub fn add(&self, name: &str, database_id: usize) {
        let previous = self
            .name_to_database
            .write()
            .insert(name.to_string(), database_id);
        turso_assert!(
            previous.is_none(),
            "namespace name already exists",
            { "name": name }
        );
    }

    /// Removes namespace `name` and returns the database it referred to.
    pub fn remove(&self, name: &str) -> Option<usize> {
        self.name_to_database.write().remove(name)
    }

    /// The databases that a name without a namespace is looked up in, in
    /// order. The first one is where CREATE puts an object whose name has
    /// no namespace. Temp is not in the list: it comes before the list when
    /// the connection has a temp database.
    ///
    /// With no search path the order is main, then the other databases by
    /// id. With a search path it is the databases of the names that exist,
    /// each once. The names are normalized: the caller has already
    /// lowercased the unquoted ones and kept the quoted ones as written.
    pub fn search_order(&self, search_path: Option<&[String]>) -> Vec<usize> {
        let Some(search_path) = search_path else {
            let mut order = vec![MAIN_DB_ID];
            order.extend(self.others().into_iter().map(|(id, _)| id));
            return order;
        };
        let mut order = Vec::new();
        for name in search_path {
            let Some(database_id) = self.database_id(name) else {
                continue;
            };
            if database_id == TEMP_DB_ID || order.contains(&database_id) {
                continue;
            }
            order.push(database_id);
        }
        order
    }
}

#[cfg(test)]
mod tests {
    use super::{NamespaceId, NamespaceRef, Namespaces};
    use crate::{MAIN_DB_ID, TEMP_DB_ID};

    #[test]
    fn the_root_of_a_database_is_namespace_zero_of_that_database() {
        let namespace = NamespaceRef::root_of(3);
        assert_eq!(namespace.database_id, 3);
        assert_eq!(namespace.id, NamespaceId::ROOT);
        assert!(namespace.id.is_root());
        assert!(!NamespaceId::new(1).is_root());
        assert_ne!(NamespaceRef::root_of(3), NamespaceRef::root_of(MAIN_DB_ID));
    }

    fn namespaces_with_app_and_other() -> Namespaces {
        let namespaces = Namespaces::default();
        namespaces.add("app", 2);
        namespaces.add("other", 3);
        namespaces
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn no_search_path_orders_main_then_the_other_databases() {
        let namespaces = namespaces_with_app_and_other();
        assert_eq!(namespaces.search_order(None), vec![MAIN_DB_ID, 2, 3]);
    }

    #[test]
    fn search_path_orders_the_names_that_exist_once_each() {
        let namespaces = namespaces_with_app_and_other();
        let search_path = names(&["missing", "other", "main", "app", "other", "temp"]);
        assert_eq!(
            namespaces.search_order(Some(&search_path)),
            vec![3, MAIN_DB_ID, 2]
        );
    }

    #[test]
    fn search_path_names_are_matched_as_written() {
        let namespaces = namespaces_with_app_and_other();
        let search_path = names(&["MAIN", "App"]);
        assert_eq!(
            namespaces.search_order(Some(&search_path)),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn search_path_with_no_existing_name_is_empty() {
        let namespaces = namespaces_with_app_and_other();
        let search_path = names(&["missing", "temp"]);
        assert_eq!(
            namespaces.search_order(Some(&search_path)),
            Vec::<usize>::new()
        );
        assert_eq!(namespaces.search_order(Some(&[])), Vec::<usize>::new());
    }

    #[test]
    fn a_second_name_for_main_is_not_an_other_database() {
        let namespaces = namespaces_with_app_and_other();
        namespaces.add("public", MAIN_DB_ID);
        assert_eq!(namespaces.database_id("public"), Some(MAIN_DB_ID));
        assert_eq!(namespaces.name(MAIN_DB_ID).as_deref(), Some("main"));
        assert_eq!(
            namespaces.others(),
            vec![(2, "app".to_string()), (3, "other".to_string())]
        );
        let search_path = names(&["public", "app"]);
        assert_eq!(
            namespaces.search_order(Some(&search_path)),
            vec![MAIN_DB_ID, 2]
        );
    }

    #[test]
    fn main_and_temp_always_exist() {
        let namespaces = namespaces_with_app_and_other();
        assert_eq!(namespaces.database_id("main"), Some(MAIN_DB_ID));
        assert_eq!(namespaces.database_id("temp"), Some(TEMP_DB_ID));
        assert_eq!(namespaces.name(TEMP_DB_ID).as_deref(), Some("temp"));
        assert_eq!(namespaces.remove("other"), Some(3));
        assert_eq!(namespaces.name(3), None);
        assert_eq!(namespaces.others(), vec![(2, "app".to_string())]);
    }
}
