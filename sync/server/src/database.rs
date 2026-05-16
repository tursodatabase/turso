use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags};

pub enum DatabaseProvider {
    Single(Arc<Connection>),
    Directory(DatabaseDirectory),
}

impl DatabaseProvider {
    pub fn single(connection: Arc<Connection>) -> Self {
        connection.wal_auto_actions_disable();
        Self::Single(connection)
    }

    pub fn directory(db_dir: PathBuf, open_options: DatabaseOpenOptions) -> Self {
        let mut open_options = open_options;
        open_options.flags.remove(OpenFlags::Create);
        Self::Directory(DatabaseDirectory {
            db_dir,
            open_options,
            connections: Mutex::new(HashMap::new()),
        })
    }

    pub fn connection(&self, db_name: Option<&str>) -> Result<Arc<Connection>> {
        match self {
            Self::Single(connection) => {
                self.validate_route(db_name)?;
                Ok(Arc::clone(connection))
            }
            Self::Directory(directory) => {
                self.validate_route(db_name)?;
                let db_name = db_name.expect("database name must be set after route validation");
                directory.connection(db_name)
            }
        }
    }

    pub fn validate_route(&self, db_name: Option<&str>) -> Result<()> {
        match self {
            Self::Single(_) => {
                if db_name.is_some() {
                    return Err(anyhow!("database name is not supported in single-db mode"));
                }
            }
            Self::Directory(_) => {
                let db_name = db_name.ok_or_else(|| anyhow!("database name is required"))?;
                validate_db_name(db_name)?;
            }
        }

        Ok(())
    }

    pub fn database_exists(&self, db_name: Option<&str>) -> Result<bool> {
        self.validate_route(db_name)?;

        match self {
            Self::Single(_) => Ok(true),
            Self::Directory(directory) => {
                let db_name = db_name.expect("database name must be set after route validation");
                Ok(directory.path(db_name).is_file())
            }
        }
    }
}

pub struct DatabaseDirectory {
    db_dir: PathBuf,
    open_options: DatabaseOpenOptions,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
}

impl DatabaseDirectory {
    fn connection(&self, db_name: &str) -> Result<Arc<Connection>> {
        validate_db_name(db_name)?;

        let mut connections = self
            .connections
            .lock()
            .map_err(|_| anyhow!("database connection cache lock poisoned"))?;

        if let Some(connection) = connections.get(db_name) {
            return Ok(Arc::clone(connection));
        }

        let path = self.path(db_name);
        if !path.is_file() {
            return Err(anyhow!("database file does not exist: {}", path.display()));
        }

        let connection = open_connection(&path, &self.open_options)?;
        connection.wal_auto_actions_disable();
        connections.insert(db_name.to_string(), Arc::clone(&connection));
        Ok(connection)
    }

    fn path(&self, db_name: &str) -> PathBuf {
        self.db_dir.join(db_name)
    }
}

#[derive(Clone)]
pub struct DatabaseOpenOptions {
    pub vfs: Option<String>,
    pub flags: OpenFlags,
    pub db_opts: DatabaseOpts,
}

pub fn open_connection(path: &Path, options: &DatabaseOpenOptions) -> Result<Arc<Connection>> {
    let db_file = normalize_db_path(path.to_string_lossy().to_string());

    if db_file.starts_with("file:") {
        let (_, connection) = Connection::from_uri(&db_file, options.db_opts)?;
        return Ok(connection);
    }

    let (_, database) = Database::open_new(
        &db_file,
        options.vfs.as_deref(),
        options.flags,
        options.db_opts.turso_cli(),
        None,
    )?;

    database.connect().map_err(Into::into)
}

pub fn validate_db_name(db_name: &str) -> Result<()> {
    if db_name.is_empty() {
        return Err(anyhow!("database name must not be empty"));
    }

    if db_name == "." || db_name == ".." {
        return Err(anyhow!("database name must not contain path traversal"));
    }

    if !db_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(anyhow!(
            "database name must contain only ASCII letters, numbers, '.', '_' or '-'"
        ));
    }

    Ok(())
}

/// Normalize `path?key=val` to `file:path?key=val` so query parameters
/// are parsed as URI options (e.g. `?locking=shared_reads`) instead of
/// being treated as part of the filename.
fn normalize_db_path(db_file: String) -> String {
    if db_file.starts_with("file:") {
        return db_file;
    }

    if let Some(pos) = db_file.rfind('?') {
        let query = &db_file[pos + 1..];
        if query.contains('=') {
            let path = &db_file[..pos];
            let encoded_path = path.replace('?', "%3F");
            return format!("file:{encoded_path}?{query}");
        }
    }

    db_file
}

#[cfg(test)]
mod tests {
    use turso_core::{DatabaseOpts, OpenFlags};

    use super::{
        normalize_db_path, open_connection, validate_db_name, DatabaseOpenOptions, DatabaseProvider,
    };

    #[test]
    fn normalize_db_path_adds_file_prefix_for_query_params() {
        assert_eq!(
            normalize_db_path("test.db?locking=shared_reads".into()),
            "file:test.db?locking=shared_reads"
        );
    }

    #[test]
    fn normalize_db_path_preserves_existing_file_prefix() {
        assert_eq!(
            normalize_db_path("file:test.db?locking=shared_reads".into()),
            "file:test.db?locking=shared_reads"
        );
    }

    #[test]
    fn normalize_db_path_keeps_plain_paths_unchanged() {
        assert_eq!(normalize_db_path("test.db".into()), "test.db");
    }

    #[test]
    fn validate_db_name_accepts_simple_names() {
        validate_db_name("alice.db").unwrap();
        validate_db_name("team-123.db").unwrap();
        validate_db_name("tenant_123").unwrap();
    }

    #[test]
    fn validate_db_name_rejects_path_traversal() {
        validate_db_name("").unwrap_err();
        validate_db_name(".").unwrap_err();
        validate_db_name("..").unwrap_err();
        validate_db_name("../secret.db").unwrap_err();
        validate_db_name("secret/tenant.db").unwrap_err();
    }

    #[test]
    fn directory_provider_rejects_missing_database_without_creating_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let provider = DatabaseProvider::directory(
            temp_dir.path().to_path_buf(),
            DatabaseOpenOptions {
                vfs: None,
                flags: OpenFlags::default(),
                db_opts: DatabaseOpts::new(),
            },
        );

        assert!(!provider.database_exists(Some("missing.db")).unwrap());
        assert!(provider.connection(Some("missing.db")).is_err());

        assert!(!temp_dir.path().join("missing.db").exists());
        assert!(!temp_dir.path().join("missing.db-wal").exists());
    }

    #[test]
    fn directory_provider_opens_existing_database() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("existing.db");
        let open_options = DatabaseOpenOptions {
            vfs: None,
            flags: OpenFlags::default(),
            db_opts: DatabaseOpts::new(),
        };

        open_connection(&db_path, &open_options).unwrap();

        let provider = DatabaseProvider::directory(temp_dir.path().to_path_buf(), open_options);
        assert!(provider.database_exists(Some("existing.db")).unwrap());
        provider.connection(Some("existing.db")).unwrap();
    }
}
