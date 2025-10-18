use sql_generation::generation::GenerationContext;

use crate::{model::interactions::InteractionStats, profiles::query::QueryProfile};

#[derive(Debug)]
pub struct Remaining {
    pub select: u32,
    pub insert: u32,
    pub create: u32,
    pub create_index: u32,
    pub delete: u32,
    pub update: u32,
    pub drop: u32,
    pub alter_table: u32,
    pub drop_index: u32,
}

impl Remaining {
    pub fn new(
        max_interactions: u32,
        opts: &QueryProfile,
        stats: &InteractionStats,
        mvcc: bool,
        context: &impl GenerationContext,
    ) -> Remaining {
        let total_weight = opts.total_weight();

        let total_select = (max_interactions * opts.select_weight) / total_weight;
        let total_insert = (max_interactions * opts.insert_weight) / total_weight;
        let total_create = (max_interactions * opts.create_table_weight) / total_weight;
        let total_create_index = (max_interactions * opts.create_index_weight) / total_weight;
        let total_delete = (max_interactions * opts.delete_weight) / total_weight;
        let total_update = (max_interactions * opts.update_weight) / total_weight;
        let total_drop = (max_interactions * opts.drop_table_weight) / total_weight;
        let total_alter_table = (max_interactions * opts.alter_table_weight) / total_weight;
        let total_drop_index = (max_interactions * opts.drop_index) / total_weight;

        let remaining_select = total_select
            .checked_sub(stats.select_count)
            .unwrap_or_default();
        let remaining_insert = total_insert
            .checked_sub(stats.insert_count)
            .unwrap_or_default();
        let remaining_create = total_create
            .checked_sub(stats.create_count)
            .unwrap_or_default();
        let mut remaining_create_index = total_create_index
            .checked_sub(stats.create_index_count)
            .unwrap_or_default();
        let remaining_delete = total_delete
            .checked_sub(stats.delete_count)
            .unwrap_or_default();
        let remaining_update = total_update
            .checked_sub(stats.update_count)
            .unwrap_or_default();
        let remaining_drop = total_drop.checked_sub(stats.drop_count).unwrap_or_default();

        let remaining_alter_table = total_alter_table
            .checked_sub(stats.alter_table_count)
            .unwrap_or_default();

        let mut remaining_drop_index = total_drop_index
            .checked_sub(stats.alter_table_count)
            .unwrap_or_default();

        if mvcc {
            // TODO: index not supported yet for mvcc
            remaining_create_index = 0;
            remaining_drop_index = 0;
        }

        // if there are no indexes do not allow creation of drop_index
        if !context
            .tables()
            .iter()
            .any(|table| !table.indexes.is_empty())
        {
            remaining_drop_index = 0;
        }

        Remaining {
            select: remaining_select,
            insert: remaining_insert,
            create: remaining_create,
            create_index: remaining_create_index,
            delete: remaining_delete,
            drop: remaining_drop,
            update: remaining_update,
            alter_table: remaining_alter_table,
            drop_index: remaining_drop_index,
        }
    }
}
