//! Database generator module for creating test databases with fake data.
//!
//! This module provides functionality to generate SQLite databases populated
//! with fake user and product data for testing purposes.

use crate::backends::cli::DefaultDatabaseResolver;
use crate::parser::ast::{DatabaseLocation, TestFile};
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
use turso::{Builder, Connection, Result as TursoResult};

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
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            db_path: "database.db".to_string(),
            user_count: 10000,
            seed: 42,
            no_rowid_alias: false,
        }
    }
}

/// Generate a database with fake user and product data
pub async fn generate_database(config: &GeneratorConfig) -> TursoResult<()> {
    let db = Builder::new_local(&config.db_path).build().await?;
    let conn = db.connect()?;
    let mut rng = ChaCha8Rng::seed_from_u64(config.seed);

    create_tables(&conn, config.no_rowid_alias).await?;
    insert_users(&conn, config.user_count, &mut rng).await?;
    insert_products(&conn, config.no_rowid_alias, &mut rng).await?;

    Ok(())
}

async fn create_tables(conn: &Connection, no_rowid_alias: bool) -> TursoResult<()> {
    let pk_type = if no_rowid_alias {
        "INT PRIMARY KEY"
    } else {
        "INTEGER PRIMARY KEY"
    };

    conn.execute(
        &format!(
            r#"
            CREATE TABLE IF NOT EXISTS users (
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
            )
            "#
        ),
        (),
    )
    .await?;

    conn.execute(
        &format!(
            r#"
            CREATE TABLE IF NOT EXISTS products (
                id {pk_type},
                name TEXT,
                price REAL
            )
            "#
        ),
        (),
    )
    .await?;

    Ok(())
}

async fn insert_users(conn: &Connection, count: usize, rng: &mut ChaCha8Rng) -> TursoResult<()> {
    for _ in 0..count {
        let user: User = fake::Faker.fake_with_rng(rng);

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
        .await?;
    }

    Ok(())
}

async fn insert_products(
    conn: &Connection,
    no_rowid_alias: bool,
    rng: &mut ChaCha8Rng,
) -> TursoResult<()> {
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
                    product.name,
                    product.price.to_string(),
                ],
            )
            .await?;
        } else {
            conn.execute(
                r#"
                INSERT INTO products (name, price)
                VALUES (?1, ?2)
                "#,
                [product.name, product.price.to_string()],
            )
            .await?;
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
    ) -> Result<Option<Self>, GeneratorError> {
        if !needs.any() {
            return Ok(None);
        }

        let temp_dir = TempDir::new().map_err(|e| GeneratorError::TempDir(e.to_string()))?;

        let mut default_path = None;
        let mut no_rowid_alias_path = None;

        if needs.default {
            let path = temp_dir.path().join("database.db");
            let config = GeneratorConfig {
                db_path: path.to_string_lossy().to_string(),
                user_count,
                seed,
                no_rowid_alias: false,
            };
            generate_database(&config)
                .await
                .map_err(|e| GeneratorError::Generation(e.to_string()))?;
            default_path = Some(path);
        }

        if needs.no_rowid_alias {
            let path = temp_dir.path().join("database-no-rowidalias.db");
            let config = GeneratorConfig {
                db_path: path.to_string_lossy().to_string(),
                user_count,
                seed,
                no_rowid_alias: true,
            };
            generate_database(&config)
                .await
                .map_err(|e| GeneratorError::Generation(e.to_string()))?;
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

/// Errors that can occur during database generation
#[derive(Debug, Clone, thiserror::Error)]
pub enum GeneratorError {
    #[error("failed to create temp directory: {0}")]
    TempDir(String),

    #[error("failed to generate database: {0}")]
    Generation(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_generate_database() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let config = GeneratorConfig {
            db_path: db_path.to_string(),
            user_count: 10,
            seed: 42,
            no_rowid_alias: false,
        };

        generate_database(&config).await.unwrap();

        // Verify the data was inserted
        let db = Builder::new_local(db_path).build().await.unwrap();
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
}
