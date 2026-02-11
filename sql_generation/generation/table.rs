use std::sync::atomic::{AtomicU64, Ordering};

use indexmap::IndexSet;
use rand::Rng;

use crate::generation::{pick, readable_name_custom, Arbitrary, GenerationContext};
use crate::model::table::{Column, ColumnType, Name, Table};
use turso_parser::ast::ColumnConstraint;

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl Arbitrary for Name {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _c: &C) -> Self {
        let base = readable_name_custom("_", rng).replace("-", "_");
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        Name(format!("{base}_{id}"))
    }
}

impl Table {
    /// Generate a table with some predefined columns
    pub fn arbitrary_with_columns<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        name: String,
        predefined_columns: Vec<Column>,
    ) -> Self {
        let opts = context.opts().table.clone();
        let large_table =
            opts.large_table.enable && rng.random_bool(opts.large_table.large_table_prob);
        let target_column_size = if large_table {
            rng.random_range(opts.large_table.column_range)
        } else {
            rng.random_range(opts.column_range)
        } as usize;

        // Start with predefined columns
        let mut column_set = IndexSet::with_capacity(target_column_size);
        for col in predefined_columns {
            column_set.insert(col);
        }

        // Generate additional columns to reach target size
        while column_set.len() < target_column_size {
            column_set.insert(Column::arbitrary(rng, context));
        }

        Table {
            rows: Vec::new(),
            name,
            columns: Vec::from_iter(column_set),
            indexes: vec![],
        }
    }
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let name = Name::arbitrary(rng, context).0;

        let rowid_alias = rng.random_bool(context.opts().table.rowid_alias_prob);
        if rowid_alias {
            let pk_name = Name::arbitrary(rng, context).0;
            let payload_name = Name::arbitrary(rng, context).0;
            let unique_name = Name::arbitrary(rng, context).0;

            let extra = rng.random_range(0..=2);
            let mut columns = Vec::with_capacity(3 + extra as usize);
            columns.push(Column {
                name: pk_name,
                column_type: ColumnType::Integer,
                constraints: vec![ColumnConstraint::PrimaryKey {
                    order: None,
                    conflict_clause: None,
                    auto_increment: false,
                }],
            });
            columns.push(Column {
                name: payload_name,
                column_type: ColumnType::Text,
                constraints: vec![],
            });
            columns.push(Column {
                name: unique_name,
                column_type: ColumnType::Text,
                constraints: vec![ColumnConstraint::Unique(None)],
            });

            for _ in 0..extra {
                columns.push(Column::arbitrary(rng, context));
            }

            Table {
                rows: Vec::new(),
                name,
                columns,
                indexes: vec![],
            }
        } else {
            Table::arbitrary_with_columns(rng, context, name, vec![])
        }
    }
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let name = Name::arbitrary(rng, context).0;
        let column_type = ColumnType::arbitrary(rng, context);

        let constraints = if rng.random_bool(0.1) {
            vec![turso_parser::ast::ColumnConstraint::Unique(None)]
        } else {
            vec![]
        };

        Self {
            name,
            column_type,
            constraints,
        }
    }
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        pick(&[Self::Integer, Self::Float, Self::Text, Self::Blob], rng).to_owned()
    }
}
