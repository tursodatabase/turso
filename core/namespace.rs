//! The namespaces of a connection: the names that qualify object names in
//! SQL, like `aux` in `aux.t`.
//!
//! Every database has a root namespace with the objects of the database
//! itself. `main` and `temp` name the roots of those databases, ATTACH adds a
//! name for the root of the attached database, and a frontend can add more
//! names for a root, like `public` for main. A name without a namespace is
//! looked up through the connection's search path, an ordered list of
//! namespace names.

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

/// What a mangled name starts with, before the namespace's id and an
/// underscore.
pub const MANGLED_NAME_PREFIX: &str = "__turso_ns";

/// The name that object `name` of namespace `namespace` is stored under in
/// the schema rows of its database.
pub fn mangle_name(namespace: NamespaceId, name: &str) -> String {
    if namespace.is_root() {
        return name.to_string();
    }
    format!("{MANGLED_NAME_PREFIX}{}_{name}", namespace.as_u32())
}

/// The namespace and name of the object whose mangled name is `mangled`:
/// the root namespace and the whole name when the name has no namespace
/// id in front.
///
/// `supports_namespaces` says whether the database supports namespaces at all.
/// When it does not, a name that looks like a mangled name is an ordinary
/// name of the root namespace: SQLite allows such names, and a database
/// made by SQLite may hold them.
pub fn demangle_name(supports_namespaces: bool, mangled: &str) -> (NamespaceId, &str) {
    if !supports_namespaces {
        return (NamespaceId::ROOT, mangled);
    }
    let Some(rest) = mangled.strip_prefix(MANGLED_NAME_PREFIX) else {
        return (NamespaceId::ROOT, mangled);
    };
    let Some((id, name)) = rest.split_once('_') else {
        return (NamespaceId::ROOT, mangled);
    };
    match id.parse::<u32>() {
        Ok(id) if id != 0 => (NamespaceId::new(id), name),
        _ => (NamespaceId::ROOT, mangled),
    }
}

/// A name that qualifies object names in SQL and the namespace it refers
/// to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Namespace {
    pub name: String,
    pub target: NamespaceRef,
}

impl Namespace {
    /// The name of the root namespace of database `database_id`: the
    /// objects of the whole database.
    pub fn database(name: &str, database_id: usize) -> Self {
        Self {
            name: name.to_string(),
            target: NamespaceRef::root_of(database_id),
        }
    }

    pub fn database_id(&self) -> usize {
        self.target.database_id
    }

    /// Whether the namespace is one of the namespaces of its database rather
    /// than the whole database.
    pub fn is_stored(&self) -> bool {
        !self.target.id.is_root()
    }
}

/// The names of a connection's namespaces.
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
        self.get(name).map(|namespace| namespace.database_id())
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
                .find(|namespace| namespace.database_id() == database_id && !namespace.is_stored())
                .cloned(),
        }
    }

    /// The name of namespace `target`: the database's own name for a root,
    /// else the first name added for the namespace.
    pub fn name_of(&self, target: NamespaceRef) -> Option<String> {
        if target.id.is_root() {
            return self
                .of_database(target.database_id)
                .map(|namespace| namespace.name);
        }
        let mut names: Vec<&String> = Vec::new();
        let namespaces = self.name_to_namespace.read();
        for (name, namespace) in namespaces.iter() {
            if namespace.target == target {
                names.push(name);
            }
        }
        names.sort();
        names.first().map(|name| name.to_string())
    }

    /// The namespaces of the attached databases: the databases other than
    /// main and temp, ordered by database.
    pub fn attached_databases(&self) -> Vec<Namespace> {
        let mut namespaces: Vec<_> = self
            .name_to_namespace
            .read()
            .values()
            .filter(|namespace| {
                !namespace.is_stored()
                    && namespace.database_id() != MAIN_DB_ID
                    && namespace.database_id() != TEMP_DB_ID
            })
            .cloned()
            .collect();
        namespaces.sort_by(|a, b| (a.database_id(), &a.name).cmp(&(b.database_id(), &b.name)));
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
            .map(|namespace| namespace.database_id())
    }

    /// The namespaces that a name without a namespace is looked up in, in
    /// order. The first one is where CREATE puts an object whose name has
    /// no namespace. Temp is not in the list: it comes before the list when
    /// the connection has a temp database.
    ///
    /// With no search path the order is main, then the attached databases
    /// by database. With a search path it is the namespaces of the names
    /// that exist, each once: a second name for the same namespace, like
    /// `public` for `main`, is skipped. The names are normalized: the
    /// caller has already lowercased the unquoted ones and kept the quoted
    /// ones as written.
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
            if namespace.database_id() == TEMP_DB_ID
                || order.iter().any(|seen| seen.target == namespace.target)
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
    use super::{demangle_name, mangle_name, Namespace, NamespaceId, NamespaceRef, Namespaces};
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
        assert!(namespaces.search_order(Some(&search_path)).is_empty());
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
            names_of(&namespaces.attached_databases()),
            vec!["app", "other"]
        );
        let search_path = names(&["public", "main", "app"]);
        assert_eq!(
            names_of(&namespaces.search_order(Some(&search_path))),
            vec!["public", "app"]
        );
    }

    #[test]
    fn a_namespace_of_main_that_is_not_its_root_is_its_own_entry_of_the_search_order() {
        let namespaces = namespaces_with_app_and_other();
        namespaces.add(Namespace::database("public", MAIN_DB_ID));
        let app_in_main = Namespace {
            name: "app_in_main".to_string(),
            target: NamespaceRef {
                database_id: MAIN_DB_ID,
                id: NamespaceId::new(17),
            },
        };
        namespaces.add(app_in_main.clone());
        let found = namespaces.get("app_in_main").unwrap();
        assert_eq!(found, app_in_main);
        assert_eq!(found.database_id(), MAIN_DB_ID);
        assert!(found.is_stored());
        assert!(!namespaces.get("app").unwrap().is_stored());
        assert_eq!(
            namespaces.name_of(app_in_main.target).as_deref(),
            Some("app_in_main")
        );
        assert_eq!(
            namespaces
                .name_of(NamespaceRef::root_of(MAIN_DB_ID))
                .as_deref(),
            Some("main")
        );
        assert_eq!(
            names_of(&namespaces.attached_databases()),
            vec!["app", "other"]
        );
        let search_path = names(&["app_in_main", "public", "main", "app_in_main"]);
        let order = namespaces.search_order(Some(&search_path));
        assert_eq!(names_of(&order), vec!["app_in_main", "public"]);
        assert_eq!(order[0].target.id, NamespaceId::new(17));
        assert_eq!(order[1].target, NamespaceRef::root_of(MAIN_DB_ID));
    }

    #[test]
    fn a_mangled_name_says_which_namespace_the_object_is_in() {
        assert_eq!(mangle_name(NamespaceId::ROOT, "t"), "t");
        assert_eq!(mangle_name(NamespaceId::new(17), "t"), "__turso_ns17_t");
        assert_eq!(
            mangle_name(NamespaceId::new(1), "sub.t"),
            "__turso_ns1_sub.t"
        );
        assert_eq!(demangle_name(true, "t"), (NamespaceId::ROOT, "t"));
        assert_eq!(
            demangle_name(false, "__turso_ns17_t"),
            (NamespaceId::ROOT, "__turso_ns17_t")
        );
        assert_eq!(
            demangle_name(true, "__turso_ns17_t"),
            (NamespaceId::new(17), "t")
        );
        assert_eq!(
            demangle_name(true, "__turso_ns1_sub.t"),
            (NamespaceId::new(1), "sub.t")
        );
        assert_eq!(
            demangle_name(true, "__turso_ns17"),
            (NamespaceId::ROOT, "__turso_ns17")
        );
        assert_eq!(
            demangle_name(true, "__turso_nsx_t"),
            (NamespaceId::ROOT, "__turso_nsx_t")
        );
        assert_eq!(
            demangle_name(true, "__turso_ns0_t"),
            (NamespaceId::ROOT, "__turso_ns0_t")
        );
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
