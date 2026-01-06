//! Database generator module for creating test databases with fake data.
//!
//! This module provides functionality to generate SQLite databases populated
//! with fake user and product data for testing purposes.

use fake::Dummy;
use fake::Fake;
use fake::faker::address::en::{CityName, StateAbbr, StreetName, ZipCode};
use fake::faker::internet::en::SafeEmail;
use fake::faker::name::en::{FirstName, LastName};
use fake::faker::phone_number::en::PhoneNumber;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
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
                [(idx + 1).to_string(), product.name, product.price.to_string()],
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
