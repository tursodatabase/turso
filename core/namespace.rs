//! The namespaces of a connection: the names that qualify object names in
//! SQL, like `aux` in `aux.t`.
//!
//! Every namespace refers to a database, the storage that holds its objects.
//! `main` and `temp` always exist. ATTACH adds a namespace for the attached
//! database, and a frontend can add more names for a database, like `public`
//! for main. A name without a namespace is looked up through the connection's
//! search path, an ordered list of namespace names.
//!
//! A namespace can also be stored in the main database, like a PostgreSQL
//! schema. A stored namespace has an id, and its objects are stored under
//! names that start with `__turso_ns<id>_`, so the names of the objects of
//! different namespaces never collide, whatever the namespaces are called.
//! A stored namespace exists while its marker table
//! `__turso_internal_namespace_<id>_<name>` exists, so creating and dropping
//! one is DDL of the main database: it is part of the transaction, other
//! connections see it when the schema changes, and the schema holds the
//! namespaces whenever it is loaded.

use crate::sync::RwLock;
use crate::{turso_assert, MAIN_DB_ID, TEMP_DB_ID};
use rustc_hash::FxHashMap as HashMap;

/// What the names of the objects of a stored namespace start with, before
/// the namespace's id and an underscore.
pub const STORED_NAMESPACE_PREFIX: &str = "__turso_ns";

/// What the name of the table that marks that a stored namespace exists
/// starts with, before the namespace's id, an underscore and its name.
pub const NAMESPACE_MARKER_PREFIX: &str = "__turso_internal_namespace_";

/// A namespace: its name, the database that stores its objects, and the id
/// it is stored under when it is a namespace stored in the database rather
/// than a whole database.
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

    /// The stored namespace whose marker table is called `table_name`, if
    /// the name is a marker table's.
    pub fn of_marker_table(table_name: &str) -> Option<Self> {
        let rest = table_name.strip_prefix(NAMESPACE_MARKER_PREFIX)?;
        let (id, name) = rest.split_once('_')?;
        let id = id.parse().ok()?;
        Some(Self::stored(name, id))
    }

    /// The name of the table that marks that this stored namespace exists.
    pub fn marker_table(&self) -> Option<String> {
        self.id
            .map(|id| format!("{NAMESPACE_MARKER_PREFIX}{id}_{}", self.name))
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

    /// The name to write into the SQL text of an object named `written`
    /// in this namespace: the name as written when the namespace is a whole
    /// database, else the stored name.
    pub fn name_in_sql(&self, written: &turso_parser::ast::Name) -> turso_parser::ast::Name {
        if self.id.is_none() {
            return written.clone();
        }
        turso_parser::ast::Name::exact(self.stored_name(written.as_str()))
    }

    /// Like [`Namespace::name_in_sql`] for a qualified name: the stored
    /// name has no qualifier, since the prefix says where the object is.
    pub fn qualified_name_in_sql(
        &self,
        written: &turso_parser::ast::QualifiedName,
    ) -> turso_parser::ast::QualifiedName {
        if self.id.is_none() {
            return written.clone();
        }
        turso_parser::ast::QualifiedName::single(self.name_in_sql(&written.name))
    }

    /// Whether `self` and `other` hold the same objects: two names for the
    /// same database and stored namespace, like `main` and `public`.
    pub fn holds_the_same_objects_as(&self, other: &Namespace) -> bool {
        self.database_id == other.database_id && self.id == other.id
    }
}

/// The id of the stored namespace whose object is stored as `stored_name`,
/// or None when the name is not an object of a stored namespace.
pub fn stored_namespace_id(stored_name: &str) -> Option<u32> {
    let rest = stored_name.strip_prefix(STORED_NAMESPACE_PREFIX)?;
    let (id, _) = rest.split_once('_')?;
    id.parse().ok()
}

/// Whether the engine stores an object under `name` for its own use or for
/// a stored namespace, so that a user may not name an object so.
pub fn is_reserved_object_name(name: &str) -> bool {
    crate::schema::RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
        || name.starts_with(STORED_NAMESPACE_PREFIX)
}

#[derive(Default)]
pub struct Namespaces {
    name_to_namespace: RwLock<HashMap<String, Namespace>>,
}

impl Namespaces {
    /// These namespaces together with the stored namespaces `stored`, which
    /// a namespace of the same name added here hides.
    pub fn with_stored<'a>(&self, stored: impl IntoIterator<Item = &'a Namespace>) -> Namespaces {
        let mut name_to_namespace = self.name_to_namespace.read().clone();
        for namespace in stored {
            name_to_namespace
                .entry(namespace.name.clone())
                .or_insert_with(|| namespace.clone());
        }
        Namespaces {
            name_to_namespace: RwLock::new(name_to_namespace),
        }
    }

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

    /// The namespace of database `database_id` that holds the object stored
    /// as `stored_name`: the stored namespace whose id the name starts
    /// with, or the whole database when the name has no namespace's id or
    /// no namespace has that id.
    pub fn of_stored_name(&self, database_id: usize, stored_name: &str) -> Option<Namespace> {
        stored_namespace_id(stored_name)
            .and_then(|id| {
                self.name_to_namespace
                    .read()
                    .values()
                    .find(|namespace| {
                        namespace.database_id == database_id && namespace.id == Some(id)
                    })
                    .cloned()
            })
            .or_else(|| self.of_database(database_id))
    }

    /// The namespaces other than main and temp, ordered by database, then
    /// by name.
    pub fn others(&self) -> Vec<Namespace> {
        let mut namespaces: Vec<_> = self.name_to_namespace.read().values().cloned().collect();
        namespaces.sort_by(|a, b| (a.database_id, &a.name).cmp(&(b.database_id, &b.name)));
        namespaces
    }

    /// The namespaces of the attached databases: the databases other than
    /// main and temp, ordered by database.
    pub fn attached_databases(&self) -> Vec<Namespace> {
        self.others()
            .into_iter()
            .filter(|namespace| {
                namespace.database_id != MAIN_DB_ID && namespace.database_id != TEMP_DB_ID
            })
            .collect()
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
            order.extend(self.attached_databases());
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
    use super::{is_reserved_object_name, stored_namespace_id, Namespace, Namespaces};
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
        assert_eq!(
            names_of(&namespaces.others()),
            vec!["public", "app", "other"]
        );
        assert_eq!(
            names_of(&namespaces.attached_databases()),
            vec!["app", "other"]
        );
        assert_eq!(
            names_of(&namespaces.search_order(None)),
            vec!["main", "app", "other"]
        );
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
        assert_eq!(
            app.marker_table().as_deref(),
            Some("__turso_internal_namespace_17_app_in_main")
        );
        assert_eq!(namespaces.get("app").unwrap().stored_name("t"), "t");
        assert_eq!(namespaces.get("app").unwrap().marker_table(), None);
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
    fn a_stored_name_is_in_the_namespace_of_its_id() {
        let namespaces = namespaces_with_app_and_other();
        namespaces.add(Namespace::stored("app_main", 1));
        namespaces.add(Namespace::stored("app_sub", 12));
        let namespace_of = |stored_name: &str| {
            namespaces
                .of_stored_name(MAIN_DB_ID, stored_name)
                .unwrap()
                .name
        };
        assert_eq!(namespace_of("t"), "main");
        assert_eq!(namespace_of("__turso_ns1_t"), "app_main");
        assert_eq!(namespace_of("__turso_ns12_t"), "app_sub");
        assert_eq!(namespace_of("__turso_ns1_sub.t"), "app_main");
        assert_eq!(namespace_of("__turso_ns7_t"), "main");
        assert_eq!(
            namespaces.of_stored_name(2, "__turso_ns1_t").unwrap().name,
            "app"
        );
    }

    #[test]
    fn a_marker_table_names_its_namespace_and_id() {
        let namespace = Namespace::of_marker_table("__turso_internal_namespace_17_app_v2").unwrap();
        assert_eq!(namespace, Namespace::stored("app_v2", 17));
        assert_eq!(
            Namespace::of_marker_table("__turso_internal_namespace_x_app"),
            None
        );
        assert_eq!(
            Namespace::of_marker_table("__turso_internal_namespace_17"),
            None
        );
        assert_eq!(Namespace::of_marker_table("__turso_internal_types"), None);
        assert_eq!(stored_namespace_id("__turso_ns17_t"), Some(17));
        assert_eq!(stored_namespace_id("__turso_ns17"), None);
        assert_eq!(stored_namespace_id("__turso_nsx_t"), None);
        assert_eq!(stored_namespace_id("t"), None);
    }

    #[test]
    fn the_names_of_stored_namespace_objects_are_reserved() {
        assert!(is_reserved_object_name("__turso_ns17_t"));
        assert!(is_reserved_object_name("__turso_nst"));
        assert!(is_reserved_object_name("__turso_internal_namespace_17_app"));
        assert!(is_reserved_object_name("sqlite_t"));
        assert!(!is_reserved_object_name("__turso_t"));
        assert!(!is_reserved_object_name("t"));
    }

    #[test]
    fn stored_namespaces_are_hidden_by_added_ones_of_the_same_name() {
        let namespaces = namespaces_with_app_and_other();
        let stored = [Namespace::stored("app", 1), Namespace::stored("shop", 2)];
        let all = namespaces.with_stored(&stored);
        assert_eq!(all.get("app").unwrap().database_id, 2);
        assert_eq!(all.get("shop").unwrap(), stored[1]);
        assert_eq!(names_of(&all.others()), vec!["shop", "app", "other"]);
        assert_eq!(names_of(&namespaces.others()), vec!["app", "other"]);
    }

    #[test]
    fn main_and_temp_always_exist() {
        let namespaces = namespaces_with_app_and_other();
        assert_eq!(namespaces.database_id("main"), Some(MAIN_DB_ID));
        assert_eq!(namespaces.database_id("temp"), Some(TEMP_DB_ID));
        assert_eq!(namespaces.of_database(TEMP_DB_ID).unwrap().name, "temp");
        assert_eq!(namespaces.remove("other"), Some(3));
        assert_eq!(namespaces.of_database(3), None);
        assert_eq!(names_of(&namespaces.attached_databases()), vec!["app"]);
    }
}
