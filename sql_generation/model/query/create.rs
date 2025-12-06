use std::fmt::Display;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::model::table::Table;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Create {
    pub table: Table,
}

impl Display for Create {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table.name)?;

        let cols = self
            .table
            .columns
            .iter()
            .map(|column| column.to_string())
            .join(", ");

        write!(f, "{cols}")?;

        // Output foreign key constraints
        for fk in &self.table.foreign_keys {
            write!(
                f,
                ", FOREIGN KEY ({}) REFERENCES {} ({})",
                fk.child_columns.join(", "),
                fk.parent_table,
                fk.parent_columns.join(", ")
            )?;
        }

        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::table::{Column, ColumnType, ForeignKey};

    #[test]
    fn test_create_table_with_foreign_key() {
        let table = Table {
            name: "orders".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
                Column {
                    name: "customer_id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
            ],
            rows: vec![],
            indexes: vec![],
            foreign_keys: vec![ForeignKey {
                child_columns: vec!["customer_id".to_string()],
                parent_table: "customers".to_string(),
                parent_columns: vec!["id".to_string()],
            }],
        };

        let create = Create { table };
        let sql = create.to_string();

        assert_eq!(
            sql,
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, FOREIGN KEY (customer_id) REFERENCES customers (id))"
        );
    }

    #[test]
    fn test_create_table_with_multiple_foreign_keys() {
        let table = Table {
            name: "order_items".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
                Column {
                    name: "order_id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
                Column {
                    name: "product_id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
            ],
            rows: vec![],
            indexes: vec![],
            foreign_keys: vec![
                ForeignKey {
                    child_columns: vec!["order_id".to_string()],
                    parent_table: "orders".to_string(),
                    parent_columns: vec!["id".to_string()],
                },
                ForeignKey {
                    child_columns: vec!["product_id".to_string()],
                    parent_table: "products".to_string(),
                    parent_columns: vec!["id".to_string()],
                },
            ],
        };

        let create = Create { table };
        let sql = create.to_string();

        assert_eq!(
            sql,
            "CREATE TABLE order_items (id INTEGER, order_id INTEGER, product_id INTEGER, FOREIGN KEY (order_id) REFERENCES orders (id), FOREIGN KEY (product_id) REFERENCES products (id))"
        );
    }

    #[test]
    fn test_create_table_with_composite_foreign_key() {
        let table = Table {
            name: "enrollments".to_string(),
            columns: vec![
                Column {
                    name: "student_id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
                Column {
                    name: "course_id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
            ],
            rows: vec![],
            indexes: vec![],
            foreign_keys: vec![ForeignKey {
                child_columns: vec!["student_id".to_string(), "course_id".to_string()],
                parent_table: "student_courses".to_string(),
                parent_columns: vec!["s_id".to_string(), "c_id".to_string()],
            }],
        };

        let create = Create { table };
        let sql = create.to_string();

        assert_eq!(
            sql,
            "CREATE TABLE enrollments (student_id INTEGER, course_id INTEGER, FOREIGN KEY (student_id, course_id) REFERENCES student_courses (s_id, c_id))"
        );
    }

    #[test]
    fn test_create_table_without_foreign_keys() {
        let table = Table {
            name: "simple".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                column_type: ColumnType::Integer,
                constraints: vec![],
            }],
            rows: vec![],
            indexes: vec![],
            foreign_keys: vec![],
        };

        let create = Create { table };
        let sql = create.to_string();

        assert_eq!(sql, "CREATE TABLE simple (id INTEGER)");
    }
}
