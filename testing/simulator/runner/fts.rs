use std::collections::BTreeSet;

use anyhow::bail;
use rand::RngCore;
use sql_gen as sg;
use sql_gen::visit::{AstVisitor as SqlgenAstVisitor, ExprContext as SqlgenExprContext};

use crate::{
    model::{
        fts::{
            FtsFeatureTag, FtsIndexSnapshot, FtsLimitPrefixOracle, FtsOracleCheck,
            FtsSchemaSnapshot, FtsSql, FtsTableRename, FtsTableSnapshot, fts_feature_tag_list,
        },
        interactions::{Interactions, InteractionsType},
    },
    profiles::Profile,
    run_simulator,
    runner::{
        bugbase::BugBase,
        cli::{FtsCommand, SimulatorCLI},
    },
    setup_simulation,
};

pub mod oracle;

pub(crate) fn run_fts_campaign(
    cli_opts: &mut SimulatorCLI,
    command: FtsCommand,
    profile: &Profile,
) -> anyhow::Result<()> {
    validate_fts_command(cli_opts, &command, profile)?;

    let base_seed = cli_opts.seed.unwrap_or_else(|| rand::rng().next_u64());

    for seed_offset in 0..command.seeds {
        let seed = base_seed.wrapping_add(seed_offset as u64);
        cli_opts.seed = Some(seed);

        let mut bugbase = if cli_opts.disable_bugbase {
            None
        } else {
            Some(BugBase::load()?)
        };
        let (seed, mut env, plan) = setup_simulation(bugbase.as_mut(), cli_opts, profile);
        env.opts.enable_fts = true;
        env.opts.disable_integrity_check = true;
        env.fts_state = Some(FtsPlanState::new(
            seed,
            command.program_steps,
            command.verify_interval,
        ));

        tracing::info!(
            "Generating native FTS sql_gen interactions: seed={}, program_steps={}, verify_interval={}",
            seed,
            command.program_steps,
            command.verify_interval
        );

        tracing::info!("Executing native FTS sql_gen plan...");
        let paths = env.paths.clone();
        let result = run_simulator(bugbase.as_mut(), cli_opts, env, plan);
        println!("seed: {seed}");
        println!("path: {}", paths.base.display());

        if !cli_opts.keep_files && result.is_ok() {
            paths.delete_all_files();
        }

        result?;
    }

    Ok(())
}

fn validate_fts_command(
    cli_opts: &SimulatorCLI,
    command: &FtsCommand,
    profile: &Profile,
) -> anyhow::Result<()> {
    if cli_opts.differential {
        bail!("FTS simulator workload is Limbo-only; do not use --differential");
    }
    if cli_opts.doublecheck {
        bail!("FTS simulator workload has stateful semantic oracles; do not use --doublecheck");
    }
    if cli_opts.mvcc.unwrap_or(profile.mvcc) {
        bail!("FTS simulator workload uses custom index modules, which MVCC does not support");
    }
    if command.seeds == 0 {
        bail!("--seeds must be greater than zero");
    }
    if command.program_steps == 0 {
        bail!("--program-steps must be greater than zero");
    }
    if command.verify_interval == 0 {
        bail!("--verify-interval must be greater than zero");
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct FtsPlanState {
    seed: u64,
    program_steps: usize,
    verify_interval: usize,
    ctx: sg::Context,
    schema: sg::Schema,
    tx_state: sg::SchemaTransactionState,
    tags: BTreeSet<FtsFeatureTag>,
    phase: FtsPlanPhase,
    pending_sql: Option<FtsPendingSql>,
}

impl FtsPlanState {
    fn new(seed: u64, program_steps: usize, verify_interval: usize) -> Self {
        Self {
            seed,
            program_steps,
            verify_interval,
            ctx: sg::Context::new_with_seed(seed),
            schema: sg::Schema::default(),
            tx_state: sg::SchemaTransactionState::default(),
            tags: BTreeSet::new(),
            phase: FtsPlanPhase::CreateTable,
            pending_sql: None,
        }
    }

    pub(crate) fn finish_sql_result(&mut self, success: bool) {
        let Some(pending) = self.pending_sql.take() else {
            // Static replay plans already carry materialized FTS interactions.
            // They do not need live generator state to advance.
            return;
        };

        if success {
            self.tx_state
                .apply_successful_statement(&mut self.schema, &pending.stmt);
        } else {
            self.tags.insert(FtsFeatureTag::StatementError);
        }
        self.phase = pending.next_phase;
    }

    fn next_interactions(&mut self) -> Option<Interactions> {
        assert!(
            self.pending_sql.is_none(),
            "FTS generator cannot advance before the pending SQL reports its outcome"
        );

        loop {
            match self.phase {
                FtsPlanPhase::CreateTable => {
                    let stmt = generate_sqlgen_main_table(&self.schema, &mut self.ctx);
                    return Some(self.emit_sql(stmt, false, FtsPlanPhase::CreateIndex));
                }
                FtsPlanPhase::CreateIndex => {
                    let stmt = generate_sqlgen_fts_index(&self.schema, &mut self.ctx)
                        .expect("failed to generate initial FTS index");
                    return Some(self.emit_sql(
                        stmt,
                        false,
                        FtsPlanPhase::BootstrapInsert { row_idx: 0 },
                    ));
                }
                FtsPlanPhase::BootstrapInsert { row_idx } => {
                    if row_idx >= 8 {
                        self.phase = FtsPlanPhase::Program {
                            step_idx: 0,
                            oracle_due: false,
                        };
                        continue;
                    }
                    let next_phase = FtsPlanPhase::BootstrapInsert {
                        row_idx: row_idx + 1,
                    };
                    let Some(stmt) =
                        generate_sqlgen_bootstrap_insert(&self.schema, &mut self.ctx, row_idx)
                    else {
                        self.phase = next_phase;
                        continue;
                    };
                    return Some(self.emit_sql(stmt, false, next_phase));
                }
                FtsPlanPhase::Program {
                    step_idx,
                    oracle_due,
                } => {
                    if oracle_due {
                        self.phase = FtsPlanPhase::Program {
                            step_idx,
                            oracle_due: false,
                        };
                        if sqlgen_schema_has_fts(&self.schema)
                            && let Some(check) = generate_sqlgen_fts_oracle(
                                self.seed,
                                step_idx,
                                &self.schema,
                                &self.tx_state,
                                &mut self.ctx,
                                &self.tags,
                            )
                        {
                            return Some(self.emit_oracle(check));
                        }
                    }

                    if step_idx >= self.program_steps {
                        self.phase = FtsPlanPhase::Done;
                        return None;
                    }

                    let next_phase = FtsPlanPhase::Program {
                        step_idx: step_idx + 1,
                        oracle_due: step_idx % self.verify_interval == 0,
                    };
                    let Some(stmt) = generate_sqlgen_program_stmt(
                        &self.schema,
                        &mut self.ctx,
                        &self.tx_state,
                        false,
                    ) else {
                        self.phase = next_phase;
                        continue;
                    };
                    return Some(self.emit_sql(stmt, true, next_phase));
                }
                FtsPlanPhase::Done => return None,
            }
        }
    }

    fn emit_sql(
        &mut self,
        stmt: sg::Stmt,
        ignore_error: bool,
        next_phase: FtsPlanPhase,
    ) -> Interactions {
        record_sqlgen_stmt_tags(&mut self.tags, &stmt, &self.schema);
        let tables = sqlgen_stmt_tables(&self.schema, &stmt);
        let read_only = matches!(stmt, sg::Stmt::Select(_));
        let table_rename = sqlgen_stmt_table_rename(&stmt);
        let sql = FtsSql {
            sql: stmt.to_string(),
            tables,
            ignore_error,
            transaction: matches!(
                stmt,
                sg::Stmt::Begin | sg::Stmt::Commit | sg::Stmt::Rollback
            ),
            read_only,
            table_rename,
        };
        if ignore_error {
            tracing::debug!("FTS generated expected-error-tolerant SQL: {}", sql.sql);
        }
        self.pending_sql = Some(FtsPendingSql { stmt, next_phase });
        Interactions::new(0, InteractionsType::FtsSql(sql))
    }

    fn emit_oracle(&self, check: FtsOracleCheck) -> Interactions {
        tracing::debug!(
            "FTS oracle step={} tags={}",
            check.step,
            fts_feature_tag_list(&check.tags)
        );
        Interactions::new(0, InteractionsType::FtsOracle(check))
    }
}

pub(crate) fn next_fts_interactions(state: &mut FtsPlanState) -> Option<Interactions> {
    state.next_interactions()
}

#[derive(Clone, Copy)]
enum FtsPlanPhase {
    CreateTable,
    CreateIndex,
    BootstrapInsert { row_idx: usize },
    Program { step_idx: usize, oracle_due: bool },
    Done,
}

#[derive(Clone)]
struct FtsPendingSql {
    stmt: sg::Stmt,
    next_phase: FtsPlanPhase,
}

fn sqlgen_stmt_tables(schema: &sg::Schema, stmt: &sg::Stmt) -> Vec<String> {
    let mut collector = SqlgenTableCollector::default();
    sg::visit::walk_stmt(stmt, &mut collector);
    let mut tables = collector.tables;

    match stmt {
        sg::Stmt::DropIndex(drop_index) => {
            if let Some(table) = sqlgen_index_table_name(schema, &drop_index.name) {
                push_sqlgen_table(&mut tables, &table);
            }
        }
        sg::Stmt::OptimizeIndex(optimize_index) => {
            if let Some(index_name) = &optimize_index.name {
                if let Some(table) = sqlgen_index_table_name(schema, index_name) {
                    push_sqlgen_table(&mut tables, &table);
                }
            } else {
                tables.extend(schema.tables.iter().map(sg::Table::qualified_name));
            }
        }
        _ => {}
    }

    tables.sort();
    tables.dedup();
    tables
}

#[derive(Default)]
struct SqlgenTableCollector {
    tables: Vec<String>,
}

impl SqlgenAstVisitor for SqlgenTableCollector {
    fn table(&mut self, table: &str) {
        push_sqlgen_table(&mut self.tables, table);
    }
}

fn push_sqlgen_table(tables: &mut Vec<String>, table: &str) {
    tables.push(table.to_string());
}

fn sqlgen_index_table_name(schema: &sg::Schema, index_name: &str) -> Option<String> {
    let (database, index_name) = sg::split_qualified_name(index_name);
    let index = schema.indexes.iter().find(|index| {
        index.name == index_name && database.is_none_or(|db| index.database.as_deref() == Some(db))
    })?;
    Some(match &index.database {
        Some(database) if !index.table_name.contains('.') => {
            format!("{database}.{}", index.table_name)
        }
        _ => index.table_name.clone(),
    })
}

fn sqlgen_stmt_table_rename(stmt: &sg::Stmt) -> Option<FtsTableRename> {
    let sg::Stmt::AlterTable(alter_table) = stmt else {
        return None;
    };
    let sg::ast::AlterTableAction::RenameTo(new_name) = &alter_table.action else {
        return None;
    };
    let (database, _) = sg::split_qualified_name(&alter_table.table);
    let new_name = if new_name.contains('.') {
        new_name.clone()
    } else if let Some(database) = database {
        format!("{database}.{new_name}")
    } else {
        new_name.clone()
    };
    Some(FtsTableRename {
        old_name: alter_table.table.clone(),
        new_name,
    })
}

fn generate_sqlgen_main_table(schema: &sg::Schema, ctx: &mut sg::Context) -> sg::Stmt {
    let existing_names = schema.table_names_in_database(None);
    let table_name = ctx.gen_unique_name("tbl", &existing_names);
    let mut col_names = std::collections::HashSet::new();
    let mut columns = Vec::new();

    let pk_name = ctx.gen_unique_name("col", &col_names);
    col_names.insert(pk_name.clone());
    columns.push(sqlgen_bootstrap_column(
        pk_name,
        sg::DataType::Integer,
        true,
        true,
    ));

    for _ in 0..2 {
        let name = ctx.gen_unique_name("col", &col_names);
        col_names.insert(name.clone());
        columns.push(sqlgen_bootstrap_column(
            name,
            sg::DataType::Text,
            false,
            ctx.gen_bool_with_prob(0.35),
        ));
    }

    let extra_count = ctx.gen_range_inclusive(1, 4);
    let extra_types = [
        sg::DataType::Integer,
        sg::DataType::Real,
        sg::DataType::Text,
        sg::DataType::Blob,
    ];
    for _ in 0..extra_count {
        let name = ctx.gen_unique_name("col", &col_names);
        col_names.insert(name.clone());
        let data_type = *ctx.choose(&extra_types).unwrap_or(&sg::DataType::Text);
        columns.push(sqlgen_bootstrap_column(
            name,
            data_type,
            false,
            ctx.gen_bool_with_prob(0.25),
        ));
    }

    sg::Stmt::CreateTable(sg::ast::CreateTableStmt {
        table: table_name,
        columns,
        if_not_exists: false,
        strict: false,
        temporary: None,
    })
}

fn sqlgen_bootstrap_column(
    name: String,
    data_type: sg::DataType,
    primary_key: bool,
    not_null: bool,
) -> sg::ast::ColumnDefStmt {
    sg::ast::ColumnDefStmt {
        name,
        data_type,
        primary_key,
        not_null,
        unique: false,
        default: None,
        check: None,
    }
}

fn generate_sqlgen_fts_index(schema: &sg::Schema, ctx: &mut sg::Context) -> Option<sg::Stmt> {
    let table = ctx.choose(&schema.tables)?;
    let mut columns = table
        .columns
        .iter()
        .filter(|column| column.data_type == sg::DataType::Text && !column.unique)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return None;
    }
    let max_columns = columns.len().min(4);
    let column_count = 1 + ctx.gen_range(max_columns);
    columns = ctx.subsequence(&columns, column_count..=column_count);

    let mut spec = sg::FtsIndexSpec::new();
    if ctx.gen_bool_with_prob(0.35) {
        let tokenizers = [
            sg::FtsTokenizer::Default,
            sg::FtsTokenizer::Raw,
            sg::FtsTokenizer::Simple,
            sg::FtsTokenizer::Whitespace,
            sg::FtsTokenizer::Ngram,
        ];
        spec = spec.with_tokenizer(*ctx.choose(&tokenizers)?);
    }
    if ctx.gen_bool_with_prob(0.5) {
        for column in ctx.subsequence(&columns, 1..=columns.len()) {
            let weight = match ctx.gen_range(5) {
                0 => 0.5,
                1 => 1.0,
                2 => 2.0,
                3 => 4.0,
                _ => 10.0,
            };
            spec = spec.with_weight(column, weight);
        }
    }

    Some(sg::Stmt::CreateIndex(sg::ast::CreateIndexStmt {
        name: format!("idx_fts_{}", schema.indexes.len()),
        table: table.qualified_name(),
        columns,
        kind: sg::CreateIndexKind::Fts(spec),
        if_not_exists: false,
    }))
}

fn generate_sqlgen_program_stmt(
    schema: &sg::Schema,
    ctx: &mut sg::Context,
    tx_state: &sg::SchemaTransactionState,
    prefer_insert: bool,
) -> Option<sg::Stmt> {
    let mut policy = sg::Policy::fts();
    policy.max_limit = 16;
    policy.max_expr_depth = 4;
    policy.max_subquery_depth = 2;
    policy.insert_config.max_rows = 3;
    policy.update_config.where_probability = 0.85;
    policy.delete_config.where_probability = 0.85;
    policy.alter_table_config.action_weights.rename_table = 1;
    policy.alter_table_config.action_weights.add_column = 2;
    policy.alter_table_config.action_weights.drop_column = 1;
    policy.alter_table_config.action_weights.rename_column = 2;

    policy.stmt_weights = if prefer_insert {
        sg::StmtWeights {
            insert: 100,
            ..sg::StmtWeights::all_zero()
        }
    } else {
        let (begin, commit, rollback) = if tx_state.in_transaction() {
            (0, 2, 1)
        } else {
            (1, 0, 0)
        };
        sg::StmtWeights {
            select: 28,
            insert: 18,
            update: 10,
            delete: 8,
            alter_table: if tx_state.in_transaction() { 0 } else { 3 },
            create_index: if tx_state.in_transaction() { 0 } else { 4 },
            drop_index: if tx_state.in_transaction() { 0 } else { 1 },
            optimize_index: 5,
            begin,
            commit,
            rollback,
            ..sg::StmtWeights::all_zero()
        }
    };

    let generator: sg::SqlGen<sg::Full> = sg::SqlGen::new(schema.clone(), policy);
    generator.statement(ctx).ok()
}

fn generate_sqlgen_bootstrap_insert(
    schema: &sg::Schema,
    ctx: &mut sg::Context,
    row_idx: usize,
) -> Option<sg::Stmt> {
    let table = schema.tables.first()?;
    let columns = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let values = table
        .columns
        .iter()
        .map(|column| bootstrap_value_expr(column, ctx, row_idx))
        .collect::<Vec<_>>();

    Some(sg::Stmt::Insert(sg::ast::InsertStmt {
        with_clause: None,
        table: table.qualified_name(),
        columns,
        values: vec![values],
        conflict: None,
    }))
}

fn bootstrap_value_expr(column: &sg::ColumnDef, ctx: &mut sg::Context, row_idx: usize) -> sg::Expr {
    if column.nullable && !column.primary_key && ctx.gen_bool_with_prob(0.12) {
        return sg::Expr::Literal(sg::Literal::Null);
    }

    let literal = match column.data_type {
        sg::DataType::Integer => sg::Literal::Integer(if column.primary_key {
            row_idx as i64 + 1
        } else {
            ctx.gen_range(2_000_001) as i64 - 1_000_000
        }),
        sg::DataType::Real => {
            let whole = ctx.gen_range(2_000_001) as f64 - 1_000_000.0;
            let frac = ctx.gen_range(10_000) as f64 / 10_000.0;
            sg::Literal::Real(whole + frac)
        }
        sg::DataType::Text => sg::Literal::Text(sg::generate::fts::generate_fts_document_text(ctx)),
        sg::DataType::Blob => {
            let len = 1 + ctx.gen_range(16);
            let bytes = (0..len).map(|_| ctx.gen_range(256) as u8).collect();
            sg::Literal::Blob(bytes)
        }
        sg::DataType::Null => sg::Literal::Null,
        sg::DataType::IntegerArray | sg::DataType::RealArray | sg::DataType::TextArray => {
            sg::Literal::Text(sg::generate::fts::generate_fts_document_text(ctx))
        }
    };
    sg::Expr::Literal(literal)
}

fn generate_sqlgen_fts_oracle(
    seed: u64,
    step: usize,
    schema: &sg::Schema,
    tx_state: &sg::SchemaTransactionState,
    ctx: &mut sg::Context,
    program_tags: &BTreeSet<FtsFeatureTag>,
) -> Option<FtsOracleCheck> {
    let query = generate_sqlgen_verification_select(schema, ctx)?;
    let mut tags = program_tags.clone();
    let mut query_tags = BTreeSet::new();
    record_sqlgen_stmt_tags(&mut query_tags, &query, schema);
    tags.extend(query_tags.iter().copied());

    let deterministic = sqlgen_tags_are_deterministic(&query_tags);
    let stable_order = sqlgen_query_has_stable_row_order(schema, &query);
    let external_oracles =
        tx_state.can_run_external_snapshot_oracles() && deterministic && stable_order;

    let limit_prefix = deterministic
        .then(|| sqlgen_limit_prefix_oracle(schema, &query))
        .flatten();
    let rebuild = external_oracles;
    let scalar = external_oracles && !sqlgen_tags_depend_on_fts_index(&query_tags);
    let reopen = external_oracles;

    if limit_prefix.is_none() && !rebuild && !scalar && !reopen {
        return None;
    }

    if limit_prefix.is_some() {
        tags.insert(FtsFeatureTag::LimitPrefixOracle);
    }
    if rebuild {
        tags.insert(FtsFeatureTag::RebuildOracle);
    }
    if scalar {
        tags.insert(FtsFeatureTag::ScalarOracle);
    }
    if reopen {
        tags.insert(FtsFeatureTag::ReopenOracle);
    }

    Some(FtsOracleCheck {
        seed,
        step,
        verification_sql: query.to_string(),
        tags,
        schema: sqlgen_schema_snapshot(schema),
        limit_prefix,
        rebuild,
        scalar,
        reopen,
    })
}

fn generate_sqlgen_verification_select(
    schema: &sg::Schema,
    ctx: &mut sg::Context,
) -> Option<sg::Stmt> {
    let mut policy = sg::Policy::fts();
    policy.max_limit = 16;
    policy.function_config = sg::FunctionConfig::deterministic();
    policy.stmt_weights = sg::StmtWeights::select_only();
    policy.select_config.limit_probability = 1.0;
    policy.select_config.order_by_probability = 1.0;
    policy.select_config.where_probability = 1.0;
    policy.expr_weights.fts_match = 80;
    let generator: sg::SqlGen<sg::Full> = sg::SqlGen::new(schema.clone(), policy);
    generator.statement(ctx).ok()
}

fn sqlgen_limit_prefix_oracle(
    schema: &sg::Schema,
    query: &sg::Stmt,
) -> Option<FtsLimitPrefixOracle> {
    let sg::Stmt::Select(select) = query else {
        return None;
    };
    let limit = select.limit?;
    if select.order_by.is_empty() || select.non_unique_order_by_reason(schema).is_some() {
        return None;
    }

    let mut full_select = select.clone();
    full_select.limit = None;
    full_select.offset = None;
    Some(FtsLimitPrefixOracle {
        full_sql: full_select.to_string(),
        limit: limit as usize,
        offset: select.offset.unwrap_or(0) as usize,
    })
}

fn sqlgen_query_has_stable_row_order(schema: &sg::Schema, query: &sg::Stmt) -> bool {
    query.unordered_limit_reason().is_none() && query.non_unique_order_by_reason(schema).is_none()
}

fn record_sqlgen_stmt_tags(
    tags: &mut BTreeSet<FtsFeatureTag>,
    stmt: &sg::Stmt,
    schema: &sg::Schema,
) {
    match stmt {
        sg::Stmt::Insert(_) => {
            tags.insert(FtsFeatureTag::Insert);
        }
        sg::Stmt::Update(update) => {
            tags.insert(FtsFeatureTag::Update);
            if update.sets.iter().any(|(column, _)| {
                sqlgen_table_has_fts_indexed_column(schema, &update.table, column)
            }) {
                tags.insert(FtsFeatureTag::UpdateIndexedColumn);
            }
        }
        sg::Stmt::Delete(_) => {
            tags.insert(FtsFeatureTag::Delete);
        }
        sg::Stmt::CreateIndex(create_index) => {
            if let sg::CreateIndexKind::Fts(spec) = &create_index.kind {
                tags.insert(FtsFeatureTag::FtsIndex);
                record_sqlgen_fts_index_spec_tags(tags, spec);
                if let Some(table) = schema.get_table_by_qualified_name(&create_index.table) {
                    for column_name in &create_index.columns {
                        if let Some(column) = table
                            .columns
                            .iter()
                            .find(|column| column.name == *column_name)
                        {
                            if column.data_type != sg::DataType::Text {
                                tags.insert(FtsFeatureTag::NonTextIndexedColumn);
                            }
                            if column.nullable {
                                tags.insert(FtsFeatureTag::NullableIndexedColumn);
                            }
                        }
                    }
                }
            }
        }
        sg::Stmt::OptimizeIndex(_) => {
            tags.insert(FtsFeatureTag::OptimizeIndex);
        }
        sg::Stmt::Begin => {
            tags.insert(FtsFeatureTag::Begin);
        }
        sg::Stmt::Commit => {
            tags.insert(FtsFeatureTag::Commit);
        }
        sg::Stmt::Rollback => {
            tags.insert(FtsFeatureTag::Rollback);
        }
        sg::Stmt::AlterTable(alter_table) => match &alter_table.action {
            sg::ast::AlterTableAction::RenameTo(_) => {
                tags.insert(FtsFeatureTag::RenameTable);
            }
            sg::ast::AlterTableAction::RenameColumn { .. } => {
                tags.insert(FtsFeatureTag::RenameColumn);
            }
            sg::ast::AlterTableAction::AddColumn(_) => {}
            sg::ast::AlterTableAction::DropColumn(_) => {}
        },
        _ => {}
    }

    let mut recorder = SqlgenTagRecorder { tags };
    sg::visit::walk_stmt(stmt, &mut recorder);
}

fn record_sqlgen_fts_index_spec_tags(tags: &mut BTreeSet<FtsFeatureTag>, spec: &sg::FtsIndexSpec) {
    match spec.tokenizer {
        sg::FtsTokenizer::Default => {}
        sg::FtsTokenizer::Raw => {
            tags.insert(FtsFeatureTag::TokenizerRaw);
        }
        sg::FtsTokenizer::Simple => {
            tags.insert(FtsFeatureTag::TokenizerSimple);
        }
        sg::FtsTokenizer::Whitespace => {
            tags.insert(FtsFeatureTag::TokenizerWhitespace);
        }
        sg::FtsTokenizer::Ngram => {
            tags.insert(FtsFeatureTag::TokenizerNgram);
        }
    }
    if !spec.weights.is_empty() {
        tags.insert(FtsFeatureTag::WeightedFields);
    }
}

struct SqlgenTagRecorder<'a> {
    tags: &'a mut BTreeSet<FtsFeatureTag>,
}

impl SqlgenAstVisitor for SqlgenTagRecorder<'_> {
    fn with_clause(&mut self, _with: &sg::ast::WithClause) {
        self.tags.insert(FtsFeatureTag::Cte);
    }

    fn select(&mut self, select: &sg::ast::SelectStmt) {
        if select.distinct {
            self.tags.insert(FtsFeatureTag::Distinct);
        }
        if select.limit.is_some() {
            self.tags.insert(FtsFeatureTag::Limit);
        }
        if select.offset.is_some() {
            self.tags.insert(FtsFeatureTag::Offset);
        }
    }

    fn compound_arm(&mut self, arm: &sg::ast::CompoundSelectArm) {
        if arm.distinct {
            self.tags.insert(FtsFeatureTag::Distinct);
        }
    }

    fn join(&mut self, join: &sg::ast::JoinClause) {
        match join.join_type {
            sg::ast::JoinType::Left => {
                self.tags.insert(FtsFeatureTag::LeftJoin);
            }
            sg::ast::JoinType::Inner | sg::ast::JoinType::Cross | sg::ast::JoinType::Natural => {
                self.tags.insert(FtsFeatureTag::Join);
            }
        }
    }

    fn expr(&mut self, expr: &sg::Expr, context: SqlgenExprContext) {
        record_sqlgen_expr_node_tags(self.tags, expr, context.in_order_by);
    }
}

fn record_sqlgen_expr_node_tags(
    tags: &mut BTreeSet<FtsFeatureTag>,
    expr: &sg::Expr,
    in_order_by: bool,
) {
    match expr {
        sg::Expr::ColumnRef(_) | sg::Expr::Literal(_) => {}
        sg::Expr::FunctionCall(function) => {
            if !sqlgen_function_is_deterministic(&function.name) {
                tags.insert(FtsFeatureTag::NonDeterministicFunction);
            }
            if function.name.eq_ignore_ascii_case("fts_score") {
                tags.insert(if in_order_by {
                    FtsFeatureTag::FtsScoreOrderBy
                } else {
                    FtsFeatureTag::FtsScoreProjection
                });
            }
        }
        sg::Expr::FtsMatch(fts) => {
            match fts.syntax {
                sg::FtsMatchSyntax::Function => {
                    tags.insert(FtsFeatureTag::MatchFunction);
                }
                sg::FtsMatchSyntax::Match => {
                    tags.insert(FtsFeatureTag::TupleMatch);
                }
            }
            record_sqlgen_fts_query_arg_tags(tags, &fts.query);
        }
        sg::Expr::BinaryOp(_)
        | sg::Expr::UnaryOp(_)
        | sg::Expr::Subquery(_)
        | sg::Expr::Case(_)
        | sg::Expr::Cast(_)
        | sg::Expr::Between(_)
        | sg::Expr::InList(_)
        | sg::Expr::InSubquery(_)
        | sg::Expr::IsNull(_)
        | sg::Expr::Exists(_)
        | sg::Expr::Parenthesized(_)
        | sg::Expr::ArrayLiteral(_)
        | sg::Expr::ArraySubscript(_)
        | sg::Expr::WindowFunction(_)
        | sg::Expr::Collate(_)
        | sg::Expr::Raise(_) => {}
    }
}

fn record_sqlgen_fts_query_arg_tags(tags: &mut BTreeSet<FtsFeatureTag>, query: &sg::Expr) {
    match query {
        sg::Expr::Literal(sg::Literal::Text(query_text)) => {
            if query_text.contains(':') {
                tags.insert(FtsFeatureTag::FieldFilter);
            }
        }
        sg::Expr::Literal(
            sg::Literal::Null
            | sg::Literal::Integer(_)
            | sg::Literal::Real(_)
            | sg::Literal::Blob(_),
        ) => {
            tags.insert(FtsFeatureTag::WeirdQueryArgument);
        }
        _ => {
            tags.insert(FtsFeatureTag::WeirdQueryArgument);
        }
    }
}

fn sqlgen_tags_depend_on_fts_index(tags: &BTreeSet<FtsFeatureTag>) -> bool {
    tags.iter().any(|tag| {
        matches!(
            tag,
            FtsFeatureTag::MatchFunction
                | FtsFeatureTag::TupleMatch
                | FtsFeatureTag::FtsScoreProjection
                | FtsFeatureTag::FtsScoreOrderBy
        )
    })
}

fn sqlgen_tags_are_deterministic(tags: &BTreeSet<FtsFeatureTag>) -> bool {
    !tags.contains(&FtsFeatureTag::NonDeterministicFunction)
}

fn sqlgen_function_is_deterministic(name: &str) -> bool {
    sg::SCALAR_FUNCTIONS
        .iter()
        .find(|function| function.name.eq_ignore_ascii_case(name))
        .map(|function| function.is_deterministic)
        .unwrap_or(true)
}

fn sqlgen_table_has_fts_indexed_column(
    schema: &sg::Schema,
    table_name: &str,
    column_name: &str,
) -> bool {
    let (_, table_name) = sg::split_qualified_name(table_name);
    schema.indexes.iter().any(|index| {
        index.is_fts()
            && index.table_name == table_name
            && index.columns.iter().any(|col| col == column_name)
    })
}

fn sqlgen_schema_has_fts(schema: &sg::Schema) -> bool {
    schema.indexes.iter().any(sg::Index::is_fts)
}

fn sqlgen_schema_snapshot(schema: &sg::Schema) -> FtsSchemaSnapshot {
    FtsSchemaSnapshot {
        tables: schema
            .tables
            .iter()
            .map(|table| FtsTableSnapshot {
                qualified_name: table.qualified_name(),
                columns: table
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                create_sql: sqlgen_create_table_sql(table),
            })
            .collect(),
        indexes: schema
            .indexes
            .iter()
            .map(|index| FtsIndexSnapshot {
                create_sql: sqlgen_create_index_sql(index),
                is_fts: index.is_fts(),
            })
            .collect(),
    }
}

fn sqlgen_create_table_sql(table: &sg::Table) -> String {
    format!(
        "CREATE TABLE {} ({}){}",
        table.qualified_name(),
        table
            .columns
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        if table.strict { " STRICT" } else { "" }
    )
}

fn sqlgen_create_index_sql(index: &sg::Index) -> String {
    let kind = match &index.kind {
        sg::IndexKind::BTree => sg::CreateIndexKind::BTree {
            unique: index.unique,
        },
        sg::IndexKind::Fts(spec) => sg::CreateIndexKind::Fts(spec.clone()),
    };
    sg::ast::CreateIndexStmt {
        name: index.qualified_name(),
        table: index.table_name.clone(),
        columns: index.columns.clone(),
        kind,
        if_not_exists: false,
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn generated_create_table_stmt(seed: u64) -> sg::Stmt {
        let schema = sg::Schema::default();
        let mut ctx = sg::Context::new_with_seed(seed);
        generate_sqlgen_main_table(&schema, &mut ctx)
    }

    fn minimal_command() -> FtsCommand {
        FtsCommand {
            seeds: 1,
            program_steps: 1,
            verify_interval: 1,
        }
    }

    #[test]
    fn failed_pending_schema_statement_does_not_advance_schema() {
        let stmt = generated_create_table_stmt(42);
        let mut state = FtsPlanState::new(1, 1, 1);
        state.pending_sql = Some(FtsPendingSql {
            stmt,
            next_phase: FtsPlanPhase::Done,
        });

        state.finish_sql_result(false);

        assert!(state.schema.tables.is_empty());
        assert!(state.tags.contains(&FtsFeatureTag::StatementError));
    }

    #[test]
    fn successful_pending_schema_statement_advances_schema() {
        let stmt = generated_create_table_stmt(42);
        let mut state = FtsPlanState::new(1, 1, 1);
        state.pending_sql = Some(FtsPendingSql {
            stmt,
            next_phase: FtsPlanPhase::Done,
        });

        state.finish_sql_result(true);

        assert_eq!(state.schema.tables.len(), 1);
        assert!(!state.tags.contains(&FtsFeatureTag::StatementError));
    }

    #[test]
    fn generated_main_table_is_total_for_previously_failing_seed() {
        let stmt = generated_create_table_stmt(3);
        let sg::Stmt::CreateTable(create_table) = stmt else {
            panic!("expected CREATE TABLE");
        };

        assert!(create_table.temporary.is_none());
        assert!(
            create_table
                .columns
                .iter()
                .filter(|column| column.data_type == sg::DataType::Text)
                .count()
                >= 2
        );
    }

    #[test]
    fn fts_command_rejects_cli_mvcc() {
        let cli_opts = SimulatorCLI::parse_from(["limbo_sim", "--mvcc", "true"]);

        let err = validate_fts_command(&cli_opts, &minimal_command(), &Profile::default())
            .expect_err("FTS workload must reject MVCC");

        assert!(err.to_string().contains("MVCC"));
    }

    #[test]
    fn fts_command_rejects_profile_mvcc() {
        let cli_opts = SimulatorCLI::parse_from(["limbo_sim"]);

        let err = validate_fts_command(&cli_opts, &minimal_command(), &Profile::simple_mvcc())
            .expect_err("FTS workload must reject MVCC profiles");

        assert!(err.to_string().contains("MVCC"));
    }
}
