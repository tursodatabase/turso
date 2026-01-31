use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use indexmap::IndexSet;
use rand::Rng;
use turso_parser::ast::{ColumnConstraint, Name as AstName};

use crate::generation::generated_expr::generate_column_expr_with_refs;
use crate::generation::{pick, readable_name_custom, Arbitrary, GenerationContext};
use crate::model::table::{Column, ColumnType, Name, Table};

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

        let mut columns: Vec<Column> = Vec::from_iter(column_set);

        // Pass 2: Add generated column constraints if enabled
        if opts.generated_columns.enable && columns.len() >= 2 {
            let gen_opts = &opts.generated_columns;

            // Track dependencies between columns: col_idx -> set of column indices it references
            let mut dependencies: HashMap<usize, HashSet<usize>> = HashMap::new();

            for i in 0..columns.len() {
                // Count non-generated columns so far
                let non_generated_count = columns.iter().filter(|c| !c.is_generated()).count();

                // Must keep at least one non-generated column
                if non_generated_count <= 1 {
                    continue;
                }

                // Randomly decide if this column should be generated
                if !rng.random_bool(gen_opts.generated_column_prob) {
                    continue;
                }

                // Generate expression referencing other columns
                let (expr, refs) = generate_column_expr_with_refs(
                    rng,
                    &columns,
                    i,
                    &columns[i].column_type,
                    gen_opts.max_expr_depth,
                );

                // Check if adding this would create a cycle
                if would_create_cycle(&dependencies, i, &refs) {
                    continue;
                }

                // Record dependencies
                dependencies.insert(i, refs);

                // Decide if STORED or VIRTUAL
                let typ = if rng.random_bool(gen_opts.stored_prob) {
                    Some(AstName::from_string("STORED"))
                } else {
                    None
                };

                // Add the generated constraint
                columns[i].constraints.push(ColumnConstraint::Generated {
                    expr: Box::new(expr),
                    typ,
                });
            }
        }

        Table {
            rows: Vec::new(),
            name,
            columns,
            indexes: vec![],
        }
    }
}

/// Check if adding a new dependency would create a cycle.
fn would_create_cycle(
    deps: &HashMap<usize, HashSet<usize>>,
    new_col: usize,
    new_refs: &HashSet<usize>,
) -> bool {
    // DFS from each referenced column to see if we can reach new_col
    for &ref_col in new_refs {
        if can_reach(deps, ref_col, new_col) {
            return true;
        }
    }
    false
}

/// Check if we can reach 'to' from 'from' via dependencies.
fn can_reach(deps: &HashMap<usize, HashSet<usize>>, from: usize, to: usize) -> bool {
    if from == to {
        return true;
    }
    if let Some(refs) = deps.get(&from) {
        for &r in refs {
            if can_reach(deps, r, to) {
                return true;
            }
        }
    }
    false
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
