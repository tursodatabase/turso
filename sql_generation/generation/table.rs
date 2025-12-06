use std::sync::atomic::{AtomicU64, Ordering};

use indexmap::IndexSet;
use rand::Rng;

use crate::generation::{pick, pick_n_unique, readable_name_custom, Arbitrary, GenerationContext};
use crate::model::table::{Column, ColumnType, ForeignKey, Name, Table};

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
        for col in std::iter::repeat_with(|| Column::arbitrary(rng, context)) {
            column_set.insert(col);
            if column_set.len() >= target_column_size {
                break;
            }
        }

        let columns: Vec<Column> = Vec::from_iter(column_set);

        // Generate foreign keys if enabled and there are other tables to reference
        let foreign_keys = if opts.foreign_keys.enabled
            && !context.tables().is_empty()
            && rng.random_bool(opts.foreign_keys.probability)
        {
            let num_fks = rng.random_range(1..=opts.foreign_keys.max_per_table);
            (0..num_fks)
                .filter_map(|_| ForeignKey::arbitrary_for_table(rng, context, &columns))
                .collect()
        } else {
            vec![]
        };

        Table {
            rows: Vec::new(),
            name,
            columns,
            indexes: vec![],
            foreign_keys,
        }
    }
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let name = Name::arbitrary(rng, context).0;
        Table::arbitrary_with_columns(rng, context, name, vec![])
    }
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let name = Name::arbitrary(rng, context).0;
        let column_type = ColumnType::arbitrary(rng, context);
        Self {
            name,
            column_type,
            constraints: vec![], // TODO: later implement arbitrary here for ColumnConstraint
        }
    }
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        pick(&[Self::Integer, Self::Float, Self::Text, Self::Blob], rng).to_owned()
    }
}

impl ForeignKey {
    /// Generate a foreign key constraint for a table being created.
    /// Returns None if no suitable parent table can be found.
    pub fn arbitrary_for_table<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        child_columns: &[Column],
    ) -> Option<Self> {
        let tables = context.tables();
        if tables.is_empty() || child_columns.is_empty() {
            return None;
        }

        // Pick a random parent table
        let parent_table = pick(tables, rng);

        // Pick how many columns to include in the FK (1 to min of child/parent column counts)
        let max_fk_cols = child_columns.len().min(parent_table.columns.len());
        if max_fk_cols == 0 {
            return None;
        }
        let num_fk_cols = rng.random_range(1..=max_fk_cols);

        // Pick random columns from the child table
        let child_col_indices: Vec<usize> =
            pick_n_unique(0..child_columns.len(), num_fk_cols, rng).collect();
        let child_col_names: Vec<String> = child_col_indices
            .iter()
            .map(|&i| child_columns[i].name.clone())
            .collect();

        // Pick random columns from the parent table
        let parent_col_indices: Vec<usize> =
            pick_n_unique(0..parent_table.columns.len(), num_fk_cols, rng).collect();
        let parent_col_names: Vec<String> = parent_col_indices
            .iter()
            .map(|&i| parent_table.columns[i].name.clone())
            .collect();

        Some(ForeignKey {
            child_columns: child_col_names,
            parent_table: parent_table.name.clone(),
            parent_columns: parent_col_names,
        })
    }
}
