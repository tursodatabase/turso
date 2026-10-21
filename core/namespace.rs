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

/// What the names of the objects of a stored namespace start with, before
/// the namespace's id and an underscore.
pub const STORED_NAMESPACE_PREFIX: &str = "__turso_ns";

/// A namespace: the database that stores its objects, and the id it is
/// stored under when it is a namespace stored in that database rather than
/// a whole database, like `main` or an attached database. The objects of a
/// stored namespace are stored under names that start with
/// `__turso_ns<id>_`, so the names of the objects of different namespaces
/// never collide, whatever the namespaces are called.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Namespace {
    pub database_id: usize,
    pub id: Option<u32>,
}

impl Namespace {
    /// The namespace of a whole database: its objects are stored under their
    /// own names.
    pub fn database(database_id: usize) -> Self {
        Self {
            database_id,
            id: None,
        }
    }

    /// The namespace stored in the main database under id `id`.
    pub fn stored(id: u32) -> Self {
        Self {
            database_id: MAIN_DB_ID,
            id: Some(id),
        }
    }

    /// What is put in front of an object's name to get the name it is
    /// stored under.
    pub fn prefix(&self) -> String {
        self.id
            .map(|id| format!("{STORED_NAMESPACE_PREFIX}{id}_"))
            .unwrap_or_default()
    }

    /// The name that object `name` is stored under in the database.
    pub fn stored_name(&self, name: &str) -> String {
        format!("{}{name}", self.prefix())
    }
}

#[derive(Default)]
pub struct Namespaces {
    name_to_namespace: RwLock<HashMap<String, Namespace>>,
}

impl Namespaces {
    /// The namespace called `name`. `name` is normalized.
    pub fn get(&self, name: &str) -> Option<Namespace> {
        match name {
            "main" => Some(Namespace::database(MAIN_DB_ID)),
            "temp" => Some(Namespace::database(TEMP_DB_ID)),
            _ => self.name_to_namespace.read().get(name).cloned(),
        }
    }

    /// The database that namespace `name` refers to. `name` is normalized.
    pub fn database_id(&self, name: &str) -> Option<usize> {
        self.get(name).map(|namespace| namespace.database_id)
    }

    /// The name of the namespace that refers to database `database_id`.
    pub fn name(&self, database_id: usize) -> Option<String> {
        match database_id {
            MAIN_DB_ID => Some("main".to_string()),
            TEMP_DB_ID => Some("temp".to_string()),
            _ => self
                .name_to_namespace
                .read()
                .iter()
                .find(|(_, namespace)| namespace.database_id == database_id)
                .map(|(name, _)| name.clone()),
        }
    }

    /// The namespaces of the databases other than main and temp, with the
    /// databases they refer to, ordered by database.
    pub fn others(&self) -> Vec<(usize, String)> {
        let mut namespaces: Vec<_> = self
            .name_to_namespace
            .read()
            .iter()
            .filter(|(_, namespace)| {
                namespace.database_id != MAIN_DB_ID && namespace.database_id != TEMP_DB_ID
            })
            .map(|(name, namespace)| (namespace.database_id, name.clone()))
            .collect();
        namespaces.sort();
        namespaces
    }

    /// Adds `namespace` under `name`.
    pub fn add(&self, name: &str, namespace: Namespace) {
        let previous = self
            .name_to_namespace
            .write()
            .insert(name.to_string(), namespace);
        turso_assert!(
            previous.is_none(),
            "namespace name already exists",
            { "name": name }
        );
    }

    /// Removes namespace `name` and returns the database it referred to.
    pub fn remove(&self, name: &str) -> Option<usize> {
        self.name_to_namespace
            .write()
            .remove(name)
            .map(|namespace| namespace.database_id)
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
    use super::{Namespace, Namespaces};
    use crate::{MAIN_DB_ID, TEMP_DB_ID};

    fn namespaces_with_app_and_other() -> Namespaces {
        let namespaces = Namespaces::default();
        namespaces.add("app", Namespace::database(2));
        namespaces.add("other", Namespace::database(3));
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
        namespaces.add("public", Namespace::database(MAIN_DB_ID));
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
    fn a_stored_namespace_stores_its_objects_under_its_id() {
        let namespaces = namespaces_with_app_and_other();
        namespaces.add("app_in_main", Namespace::stored(17));
        let app = namespaces.get("app_in_main").unwrap();
        assert_eq!(app.database_id, MAIN_DB_ID);
        assert_eq!(app.stored_name("t"), "__turso_ns17_t");
        assert_eq!(namespaces.get("app").unwrap().stored_name("t"), "t");
        assert_eq!(namespaces.get("main").unwrap().stored_name("t"), "t");
        assert_eq!(namespaces.get("other").unwrap().stored_name("t"), "t");
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
