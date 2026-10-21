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

/// A namespace: its name, the database that stores its objects, and the id it is
/// stored under when it is a namespace stored in that database rather than
/// a whole database, like `main` or an attached database. The objects of a
/// stored namespace are stored under names that start with
/// `__turso_ns<id>_`, so the names of the objects of different namespaces
/// never collide, whatever the namespaces are called.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Namespace {
    pub name: String,
    pub database_id: usize,
    pub id: Option<u32>,
}

impl Namespace {
    /// The namespace of a whole database: its objects are stored under their
    /// own names.
    pub fn database(name: &str, database_id: usize) -> Self {
        Self {
            name: name.to_string(),
            database_id,
            id: None,
        }
    }

    /// The namespace stored in the main database under id `id`.
    pub fn stored(name: &str, id: u32) -> Self {
        Self {
            name: name.to_string(),
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

    /// Whether `self` and `other` hold the same objects: two names for the
    /// same database and stored namespace, like `main` and `public`.
    pub fn holds_the_same_objects_as(&self, other: &Namespace) -> bool {
        self.database_id == other.database_id && self.id == other.id
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
            "main" => Some(Namespace::database("main", MAIN_DB_ID)),
            "temp" => Some(Namespace::database("temp", TEMP_DB_ID)),
            _ => self.name_to_namespace.read().get(name).cloned(),
        }
    }

    /// The database that namespace `name` refers to. `name` is normalized.
    pub fn database_id(&self, name: &str) -> Option<usize> {
        self.get(name).map(|namespace| namespace.database_id)
    }

    /// The namespace that is the whole of database `database_id`: main,
    /// temp or an attached database.
    pub fn of_database(&self, database_id: usize) -> Option<Namespace> {
        match database_id {
            MAIN_DB_ID => Some(Namespace::database("main", MAIN_DB_ID)),
            TEMP_DB_ID => Some(Namespace::database("temp", TEMP_DB_ID)),
            _ => self
                .name_to_namespace
                .read()
                .values()
                .find(|namespace| namespace.database_id == database_id && namespace.id.is_none())
                .cloned(),
        }
    }

    /// The namespaces of the databases other than main and temp, ordered by
    /// database, then by name.
    pub fn others(&self) -> Vec<Namespace> {
        let mut namespaces: Vec<_> = self
            .name_to_namespace
            .read()
            .values()
            .filter(|namespace| {
                namespace.database_id != MAIN_DB_ID && namespace.database_id != TEMP_DB_ID
            })
            .cloned()
            .collect();
        namespaces.sort_by(|a, b| (a.database_id, &a.name).cmp(&(b.database_id, &b.name)));
        namespaces
    }

    /// Adds `namespace` under its name.
    pub fn add(&self, namespace: Namespace) {
        let name = namespace.name.clone();
        let previous = self
            .name_to_namespace
            .write()
            .insert(name.clone(), namespace);
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

    /// The namespaces that a name without a namespace is looked up in, in
    /// order. The first one is where CREATE puts an object whose name has
    /// no namespace. Temp is not in the list: it comes before the list when
    /// the connection has a temp database.
    ///
    /// With no search path the order is main, then the namespaces of the
    /// other databases by database. With a search path it is the namespaces
    /// of the names that exist, each once: a second name for the same
    /// objects, like `public` for `main`, is skipped. The names are
    /// normalized: the caller has already lowercased the unquoted ones and
    /// kept the quoted ones as written.
    pub fn search_order(&self, search_path: Option<&[String]>) -> Vec<Namespace> {
        let Some(search_path) = search_path else {
            let mut order = vec![Namespace::database("main", MAIN_DB_ID)];
            order.extend(self.others());
            return order;
        };
        let mut order: Vec<Namespace> = Vec::new();
        for name in search_path {
            let Some(namespace) = self.get(name) else {
                continue;
            };
            if namespace.database_id == TEMP_DB_ID
                || order
                    .iter()
                    .any(|seen| seen.holds_the_same_objects_as(&namespace))
            {
                continue;
            }
            order.push(namespace);
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
        namespaces.add(Namespace::database("app", 2));
        namespaces.add(Namespace::database("other", 3));
        namespaces
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn names_of(namespaces: &[Namespace]) -> Vec<&str> {
        namespaces
            .iter()
            .map(|namespace| namespace.name.as_str())
            .collect()
    }

    #[test]
    fn no_search_path_orders_main_then_the_other_databases() {
        let namespaces = namespaces_with_app_and_other();
        assert_eq!(
            names_of(&namespaces.search_order(None)),
            vec!["main", "app", "other"]
        );
    }

    #[test]
    fn search_path_orders_the_names_that_exist_once_each() {
        let namespaces = namespaces_with_app_and_other();
        let search_path = names(&["missing", "other", "main", "app", "other", "temp"]);
        assert_eq!(
            names_of(&namespaces.search_order(Some(&search_path))),
            vec!["other", "main", "app"]
        );
    }

    #[test]
    fn search_path_names_are_matched_as_written() {
        let namespaces = namespaces_with_app_and_other();
        let search_path = names(&["MAIN", "App"]);
        assert_eq!(
            names_of(&namespaces.search_order(Some(&search_path))),
            Vec::<&str>::new()
        );
    }

    #[test]
    fn search_path_with_no_existing_name_is_empty() {
        let namespaces = namespaces_with_app_and_other();
        let search_path = names(&["missing", "temp"]);
        assert!(namespaces.search_order(Some(&search_path)).is_empty());
        assert!(namespaces.search_order(Some(&[])).is_empty());
    }

    #[test]
    fn a_second_name_for_main_is_not_an_other_database() {
        let namespaces = namespaces_with_app_and_other();
        namespaces.add(Namespace::database("public", MAIN_DB_ID));
        assert_eq!(namespaces.database_id("public"), Some(MAIN_DB_ID));
        assert_eq!(namespaces.of_database(MAIN_DB_ID).unwrap().name, "main");
        assert_eq!(names_of(&namespaces.others()), vec!["app", "other"]);
        let search_path = names(&["public", "main", "app"]);
        assert_eq!(
            names_of(&namespaces.search_order(Some(&search_path))),
            vec!["public", "app"]
        );
    }

    #[test]
    fn a_stored_namespace_stores_its_objects_under_its_id() {
        let namespaces = namespaces_with_app_and_other();
        namespaces.add(Namespace::stored("app_in_main", 17));
        let app = namespaces.get("app_in_main").unwrap();
        assert_eq!(app.database_id, MAIN_DB_ID);
        assert_eq!(app.stored_name("t"), "__turso_ns17_t");
        assert_eq!(namespaces.get("app").unwrap().stored_name("t"), "t");
        assert_eq!(namespaces.get("main").unwrap().stored_name("t"), "t");
        assert_eq!(namespaces.get("other").unwrap().stored_name("t"), "t");
        assert_eq!(namespaces.of_database(MAIN_DB_ID).unwrap().name, "main");
    }

    #[test]
    fn a_stored_namespace_is_its_own_entry_of_the_search_order() {
        let namespaces = namespaces_with_app_and_other();
        namespaces.add(Namespace::database("public", MAIN_DB_ID));
        namespaces.add(Namespace::stored("app_in_main", 17));
        let search_path = names(&["app_in_main", "public", "main", "app_in_main"]);
        let order = namespaces.search_order(Some(&search_path));
        assert_eq!(names_of(&order), vec!["app_in_main", "public"]);
        assert_eq!(order[0].stored_name("t"), "__turso_ns17_t");
        assert_eq!(order[1].stored_name("t"), "t");
    }

    #[test]
    fn main_and_temp_always_exist() {
        let namespaces = namespaces_with_app_and_other();
        assert_eq!(namespaces.database_id("main"), Some(MAIN_DB_ID));
        assert_eq!(namespaces.database_id("temp"), Some(TEMP_DB_ID));
        assert_eq!(namespaces.of_database(TEMP_DB_ID).unwrap().name, "temp");
        assert_eq!(namespaces.remove("other"), Some(3));
        assert_eq!(namespaces.of_database(3), None);
        assert_eq!(names_of(&namespaces.others()), vec!["app"]);
    }
}
