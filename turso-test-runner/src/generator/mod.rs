//! Database generator module for creating test databases with fake data.
//!
//! This module provides functionality to generate SQLite databases populated
//! with fake user and product data for testing purposes.

use crate::backends::cli::DefaultDatabaseResolver;
use crate::parser::ast::{DatabaseLocation, TestFile};
use anyhow::{Context, Result};
use fake::Dummy;
use fake::Fake;
use fake::faker::address::en::{CityName, StateAbbr, StreetName, ZipCode};
use fake::faker::internet::en::SafeEmail;
use fake::faker::name::en::{FirstName, LastName};
use fake::faker::phone_number::en::PhoneNumber;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::path::PathBuf;
use tempfile::TempDir;
use turso::{Builder, Connection};

/// Product list for generating product data
const PRODUCT_LIST: &[&str] = &[
    "hat",
    "cap",
    "shirt",
    "sweater",
    "sweatshirt",
    "shorts",
    "jeans",
    "sneakers",
    "boots",
    "coat",
    "accessories",
];

/// A fake user record
#[derive(Debug, Dummy)]
pub struct User {
    #[dummy(faker = "FirstName()")]
    pub first_name: String,
    #[dummy(faker = "LastName()")]
    pub last_name: String,
    #[dummy(faker = "SafeEmail()")]
    pub email: String,
    #[dummy(faker = "PhoneNumber()")]
    pub phone_number: String,
    #[dummy(faker = "StreetName()")]
    pub address: String,
    #[dummy(faker = "CityName()")]
    pub city: String,
    #[dummy(faker = "StateAbbr()")]
    pub state: String,
    #[dummy(faker = "ZipCode()")]
    pub zipcode: String,
    #[dummy(faker = "1..=100")]
    pub age: i64,
}

/// A product record
#[derive(Debug)]
pub struct Product {
    pub name: String,
    pub price: f64,
}

impl Product {
    fn new(name: &str, rng: &mut impl Rng) -> Self {
        Self {
            name: name.to_string(),
            price: rng.random_range(1.0..=100.0),
        }
    }
}

/// Configuration for database generation
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Path to the database file
    pub db_path: String,
    /// Number of users to generate
    pub user_count: usize,
    /// Seed for reproducible random generation
    pub seed: u64,
    /// If true, use INT PRIMARY KEY instead of INTEGER PRIMARY KEY
    /// This prevents the rowid alias optimization in SQLite
    pub no_rowid_alias: bool,
    /// Enable MVCC mode (experimental journal mode)
    pub mvcc: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            db_path: "database.db".to_string(),
            user_count: 10000,
            seed: 42,
            no_rowid_alias: false,
            mvcc: false,
        }
    }
}

/// Generate a database with fake user and product data
pub async fn generate_database(config: &GeneratorConfig) -> Result<()> {
    let db = Builder::new_local(&config.db_path)
        .build()
        .await
        .with_context(|| format!("failed to create database at '{}'", config.db_path))?;

    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to database '{}'", config.db_path))?;

    // Enable MVCC mode if requested (must be done before any transactions)
    if config.mvcc {
        // Use query instead of execute since PRAGMA returns a result row
        let mut rows = conn
            .query("PRAGMA journal_mode = 'experimental_mvcc'", ())
            .await
            .context("failed to enable MVCC mode")?;
        // Consume the result row
        while let Some(_) = rows.next().await? {}
    }

    let mut rng = ChaCha8Rng::seed_from_u64(config.seed);

    conn.execute("BEGIN", ())
        .await
        .context("failed to execute BEGIN transaction")?;

    create_tables(&conn, config.no_rowid_alias)
        .await
        .context("failed to create tables")?;

    insert_users(&conn, config.user_count, config.no_rowid_alias, &mut rng)
        .await
        .context("failed to insert users")?;

    insert_products(&conn, config.no_rowid_alias, &mut rng)
        .await
        .context("failed to insert products")?;

    conn.execute("COMMIT", ())
        .await
        .context("failed to execute COMMIT transaction")?;

    // Checkpoint to ensure data is written to the main database file.
    // This is required for SQLite to read the database with immutable=1 mode,
    // which doesn't read WAL files.
    let mut rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .context("failed to checkpoint database")?;
    // Consume the result
    while let Some(_) = rows.next().await? {}

    // Explicitly close connection and database to release locks
    drop(conn);
    drop(db);

    Ok(())
}

async fn create_tables(conn: &Connection, no_rowid_alias: bool) -> Result<()> {
    let pk_type = if no_rowid_alias {
        "INT PRIMARY KEY"
    } else {
        "INTEGER PRIMARY KEY"
    };

    let users_sql = format!(
        r#"
        CREATE TABLE users (
        id {pk_type},
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        age INTEGER
    );
        "#
    );

    conn.execute(&users_sql, ())
        .await
        .with_context(|| format!("failed to create users table: {}", users_sql.trim()))?;

    if !no_rowid_alias {
        let index_sql = format!("CREATE INDEX age_idx ON users (age);");

        conn.execute(&index_sql, ())
            .await
            .with_context(|| format!("failed to create user index table: {}", index_sql.trim()))?;
    }

    let products_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS products (
            id {pk_type},
            name TEXT,
            price REAL
        )
        "#
    );

    conn.execute(&products_sql, ())
        .await
        .with_context(|| format!("failed to create products table: {}", products_sql.trim()))?;

    Ok(())
}

async fn insert_users(
    conn: &Connection,
    count: usize,
    no_rowid_alias: bool,
    rng: &mut ChaCha8Rng,
) -> Result<()> {
    for i in 0..count {
        let user: User = fake::Faker.fake_with_rng(rng);

        if no_rowid_alias {
            // For INT PRIMARY KEY, we need to explicitly provide the id
            conn.execute(
                r#"
                INSERT INTO users (id, first_name, last_name, email, phone_number, address, city, state, zipcode, age)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                "#,
                [
                    (i + 1).to_string(),
                    user.first_name,
                    user.last_name,
                    user.email,
                    user.phone_number,
                    user.address,
                    user.city,
                    user.state,
                    user.zipcode,
                    user.age.to_string(),
                ],
            )
            .await
            .with_context(|| format!("failed to insert user {} of {}", i + 1, count))?;
        } else {
            conn.execute(
                r#"
                INSERT INTO users (first_name, last_name, email, phone_number, address, city, state, zipcode, age)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                [
                    user.first_name,
                    user.last_name,
                    user.email,
                    user.phone_number,
                    user.address,
                    user.city,
                    user.state,
                    user.zipcode,
                    user.age.to_string(),
                ],
            )
            .await
            .with_context(|| format!("failed to insert user {} of {}", i + 1, count))?;
        }
    }

    Ok(())
}

async fn insert_products(
    conn: &Connection,
    no_rowid_alias: bool,
    rng: &mut ChaCha8Rng,
) -> Result<()> {
    for (idx, product_name) in PRODUCT_LIST.iter().enumerate() {
        let product = Product::new(product_name, rng);

        if no_rowid_alias {
            // For INT PRIMARY KEY, we need to explicitly provide the id
            conn.execute(
                r#"
                INSERT INTO products (id, name, price)
                VALUES (?1, ?2, ?3)
                "#,
                [
                    (idx + 1).to_string(),
                    product.name.clone(),
                    product.price.to_string(),
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to insert product '{}' (id={}, price={})",
                    product.name,
                    idx + 1,
                    product.price
                )
            })?;
        } else {
            conn.execute(
                r#"
                INSERT INTO products (name, price)
                VALUES (?1, ?2)
                "#,
                [product.name.clone(), product.price.to_string()],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to insert product '{}' (price={})",
                    product.name, product.price
                )
            })?;
        }
    }

    Ok(())
}

/// Which default databases are needed for a test run
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultDatabaseNeeds {
    /// Need the default database with INTEGER PRIMARY KEY (rowid alias)
    pub default: bool,
    /// Need the default database with INT PRIMARY KEY (no rowid alias)
    pub no_rowid_alias: bool,
}

impl DefaultDatabaseNeeds {
    /// Check if any default databases are needed
    pub fn any(&self) -> bool {
        self.default || self.no_rowid_alias
    }
}

/// Holds paths to generated default databases
///
/// The temp directory is kept alive as long as this struct exists.
/// When dropped, the temp directory and all generated databases are cleaned up.
pub struct DefaultDatabases {
    /// Temp directory holding the generated databases
    _temp_dir: TempDir,
    /// Path to the default database (INTEGER PRIMARY KEY)
    pub default_path: Option<PathBuf>,
    /// Path to the no-rowid-alias database (INT PRIMARY KEY)
    pub no_rowid_alias_path: Option<PathBuf>,
}

impl DefaultDatabases {
    /// Scan test files to determine which default databases are needed
    pub fn scan_needs<'a>(
        test_files: impl IntoIterator<Item = &'a TestFile>,
    ) -> DefaultDatabaseNeeds {
        let mut needs = DefaultDatabaseNeeds::default();

        for file in test_files {
            for db_config in &file.databases {
                match db_config.location {
                    DatabaseLocation::Default => needs.default = true,
                    DatabaseLocation::DefaultNoRowidAlias => needs.no_rowid_alias = true,
                    _ => {}
                }
            }
        }

        needs
    }

    /// Generate the needed default databases
    ///
    /// Returns None if no default databases are needed.
    pub async fn generate(
        needs: DefaultDatabaseNeeds,
        seed: u64,
        user_count: usize,
        mvcc: bool,
    ) -> Result<Option<Self>> {
        if !needs.any() {
            return Ok(None);
        }

        let temp_dir = TempDir::new().context("failed to create temp directory for databases")?;

        let mut default_path = None;
        let mut no_rowid_alias_path = None;

        if needs.default {
            let path = temp_dir.path().join("database.db");
            let config = GeneratorConfig {
                db_path: path.to_string_lossy().to_string(),
                user_count,
                seed,
                no_rowid_alias: false,
                mvcc,
            };
            generate_database(&config)
                .await
                .context("failed to generate default database (INTEGER PRIMARY KEY)")?;
            default_path = Some(path);
        }

        if needs.no_rowid_alias {
            let path = temp_dir.path().join("database-no-rowidalias.db");
            let config = GeneratorConfig {
                db_path: path.to_string_lossy().to_string(),
                user_count,
                seed,
                no_rowid_alias: true,
                mvcc,
            };
            generate_database(&config)
                .await
                .context("failed to generate no-rowid-alias database (INT PRIMARY KEY)")?;
            no_rowid_alias_path = Some(path);
        }

        Ok(Some(Self {
            _temp_dir: temp_dir,
            default_path,
            no_rowid_alias_path,
        }))
    }
}

impl DefaultDatabaseResolver for DefaultDatabases {
    fn resolve(&self, location: &DatabaseLocation) -> Option<PathBuf> {
        match location {
            DatabaseLocation::Default => self.default_path.clone(),
            DatabaseLocation::DefaultNoRowidAlias => self.no_rowid_alias_path.clone(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn new_config(db_path: &str) -> GeneratorConfig {
        GeneratorConfig {
            db_path: db_path.to_string(),
            user_count: 10,
            seed: 42,
            no_rowid_alias: false,
            mvcc: false,
        }
    }

    async fn generate_db(config: GeneratorConfig) {
        generate_database(&config).await.unwrap();

        // Verify the data was inserted
        let db = Builder::new_local(&config.db_path).build().await.unwrap();
        let conn = db.connect().unwrap();

        // Check user count
        let mut rows = conn.query("SELECT COUNT(*) FROM users", ()).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let count = row.get::<i64>(0).unwrap();
        assert_eq!(count, 10);

        // Check product count
        let mut rows = conn
            .query("SELECT COUNT(*) FROM products", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let count = row.get::<i64>(0).unwrap();
        assert_eq!(count, PRODUCT_LIST.len() as i64);
    }

    #[tokio::test]
    async fn test_generate_database() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let config = new_config(db_path);

        generate_db(config).await;

        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let mut config = new_config(db_path);
        config.no_rowid_alias = true;

        generate_db(config).await;
    }
}
