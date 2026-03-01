use std::fmt::Display;

use sql_generation::generation::GenerationContext;

use crate::{
    model::{
        interactions::{Interaction, InteractionType},
    },
    profiles::query::QueryProfile,
};

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
    pub pragma_count: u32,
    pub create_matview: u32,
    pub drop_matview: u32,
    pub enable_cdc: u32,
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
        let total_pragma = (max_interactions * opts.pragma_weight) / total_weight;
        let total_create_matview = (max_interactions * opts.create_matview_weight) / total_weight;
        let total_drop_matview = (max_interactions * opts.drop_matview_weight) / total_weight;
        let total_enable_cdc = (max_interactions * opts.enable_cdc_weight) / total_weight;

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
        let remaining_pragma = total_pragma
            .checked_sub(stats.pragma_count)
            .unwrap_or_default();

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

        let remaining_create_matview = total_create_matview
            .checked_sub(stats.create_matview_count)
            .unwrap_or_default();
        let remaining_enable_cdc = total_enable_cdc
            .checked_sub(stats.enable_cdc_count)
            .unwrap_or_default();

        // Calculate remaining drop matview from profile weight
        // We can only drop matviews if more have been created than dropped in this plan
        let net_matviews_created = stats
            .create_matview_count
            .saturating_sub(stats.drop_matview_count);
        let remaining_drop_matview = if net_matviews_created > 0 {
            total_drop_matview
                .checked_sub(stats.drop_matview_count)
                .unwrap_or_default()
        } else {
            0
        };

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
            pragma_count: remaining_pragma,
            create_matview: remaining_create_matview,
            drop_matview: remaining_drop_matview,
            enable_cdc: remaining_enable_cdc,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct InteractionStats {
    pub select_count: u32,
    pub insert_count: u32,
    pub delete_count: u32,
    pub update_count: u32,
    pub create_count: u32,
    pub create_index_count: u32,
    pub drop_count: u32,
    pub begin_count: u32,
    pub commit_count: u32,
    pub rollback_count: u32,
    pub alter_table_count: u32,
    pub drop_index_count: u32,
    pub pragma_count: u32,
    pub create_matview_count: u32,
    pub drop_matview_count: u32,
    pub enable_cdc_count: u32,
}

impl InteractionStats {
    pub fn update(&mut self, interaction: &Interaction) {
        match &interaction.interaction {
            InteractionType::Query(query)
            | InteractionType::FsyncQuery(query)
            | InteractionType::FaultyQuery(query) => self.query_stat(query),
            _ => {}
        }
    }

    fn query_stat(&mut self, q: &Query) {
        match q {
            Query::Select(_) => self.select_count += 1,
            Query::Insert(_) => self.insert_count += 1,
            Query::Delete(_) => self.delete_count += 1,
            Query::Create(_) => self.create_count += 1,
            Query::Drop(_) => self.drop_count += 1,
            Query::Update(_) => self.update_count += 1,
            Query::CreateIndex(_) => self.create_index_count += 1,
            Query::Begin(_) => self.begin_count += 1,
            Query::Commit(_) => self.commit_count += 1,
            Query::Rollback(_) => self.rollback_count += 1,
            Query::AlterTable(_) => self.alter_table_count += 1,
            Query::DropIndex(_) => self.drop_index_count += 1,
            Query::Placeholder => {}
            Query::Pragma(_) => self.pragma_count += 1,
            Query::CreateView(_) => {}
            Query::CreateMaterializedView(_) => self.create_matview_count += 1,
            Query::DropView(_) => {}
            Query::DropMaterializedView(_) => self.drop_matview_count += 1,
        }
    }
}

impl Display for InteractionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read: {}, Insert: {}, Delete: {}, Update: {}, Create: {}, CreateIndex: {}, Drop: {}, Begin: {}, Commit: {}, Rollback: {}, Alter Table: {}, Drop Index: {}, CreateMatview: {}, DropMatview: {}, EnableCDC: {}",
            self.select_count,
            self.insert_count,
            self.delete_count,
            self.update_count,
            self.create_count,
            self.create_index_count,
            self.drop_count,
            self.begin_count,
            self.commit_count,
            self.rollback_count,
            self.alter_table_count,
            self.drop_index_count,
            self.create_matview_count,
            self.drop_matview_count,
            self.enable_cdc_count,
        )
    }
}
