use crate::opts::{Opts, TxMode};
use crate::{log_sql, SqlLog, ThreadRng};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{de, Deserialize, Deserializer};
use serde_yaml::Value as YamlValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use turso_stress::sync::atomic::{AtomicU64, Ordering};

type BoxError = Box<dyn Error + Send + Sync>;
type Result<T> = std::result::Result<T, BoxError>;

const INSERT_CHUNK_SIZE: usize = 200;
const BASE_TIMESTAMP_MS: i64 = 1_704_067_200_000;
const DEFAULT_NOW_TIMESTAMP_MS: i64 = BASE_TIMESTAMP_MS + 63_072_000_000;

#[derive(Debug, Default)]
pub(crate) struct PendingPoolUpdates {
    consumed: Vec<(String, SqlValue)>,
    produced: Vec<(String, SqlValue)>,
}

#[derive(Debug)]
pub(crate) struct WorkloadRuntime {
    profile: WorkloadProfile,
    compiled_operations: Vec<CompiledOperation>,
    pools: HashMap<String, Vec<SqlValue>>,
    sequence_counters: HashMap<String, u64>,
    pool_targets: HashSet<String>,
    total_weight: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadProfile {
    name: String,
    description: Option<String>,
    #[serde(default)]
    schema: WorkloadSchema,
    #[serde(default)]
    data: Vec<DataSpec>,
    #[serde(default)]
    operations: Vec<OperationSpec>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadSchema {
    #[serde(default)]
    tables: Vec<TableSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSpec {
    name: String,
    columns: Vec<ColumnSpec>,
    #[serde(default)]
    primary_key: Vec<String>,
    #[serde(default)]
    indexes: Vec<IndexSpec>,
    #[serde(default)]
    triggers: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnSpec {
    name: String,
    #[serde(rename = "type")]
    data_type: Option<String>,
    sql_type: Option<String>,
    #[serde(default)]
    primary_key: bool,
    nullable: Option<bool>,
    default: Option<YamlValue>,
    #[serde(default)]
    unique: bool,
    references: Option<String>,
    generated: Option<String>,
    #[serde(rename = "virtual", default)]
    virtual_: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexSpec {
    name: String,
    columns: Vec<String>,
    #[serde(default)]
    unique: bool,
    #[serde(rename = "where")]
    where_clause: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataSpec {
    table: String,
    rows: RowsSpec,
    #[serde(default)]
    columns: BTreeMap<String, GeneratorSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RowsSpec {
    Count(usize),
    Literal(Vec<BTreeMap<String, YamlValue>>),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct GeneratorSpec {
    pattern: Option<String>,
    sequence: Option<SequenceSpec>,
    int: Option<IntSpec>,
    real: Option<RealSpec>,
    string: Option<StringSpec>,
    timestamp: Option<TimestampSpec>,
    bool: Option<BoolSpec>,
    #[serde(rename = "enum")]
    enum_: Option<EnumSpec>,
    #[serde(rename = "const")]
    const_: Option<YamlValue>,
    #[serde(rename = "ref")]
    ref_: Option<RefSpec>,
    #[serde(default)]
    consume: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RefSpec {
    Target(String),
    Options(RefOptions),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RefOptions {
    #[serde(rename = "ref")]
    target: String,
    #[serde(default)]
    consume: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SequenceSpec {
    #[serde(default = "default_sequence_start")]
    start: u64,
    #[serde(default = "default_sequence_step")]
    step: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntSpec {
    #[serde(default)]
    min: i64,
    #[serde(default = "default_int_max")]
    max: i64,
    dist: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RealSpec {
    #[serde(default)]
    min: f64,
    #[serde(default = "default_real_max")]
    max: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StringSpec {
    #[serde(default = "default_string_length")]
    length: usize,
    charset: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum TimestampSpec {
    Now(String),
    Range {
        from: Option<YamlValue>,
        to: Option<YamlValue>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoolSpec {
    #[serde(default = "default_p_true")]
    p_true: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnumSpec {
    values: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationSpec {
    name: String,
    weight: u64,
    sql: Option<String>,
    transaction: Option<Vec<StatementSpec>>,
    batch: Option<Vec<StatementSpec>>,
    #[serde(default)]
    concurrent: bool,
    #[serde(default)]
    bind: BTreeMap<String, GeneratorSpec>,
    produces: Option<String>,
    requires: Option<RequiresSpec>,
    #[serde(default)]
    expect: WorkloadExpect,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatementSpec {
    sql: String,
    #[serde(default)]
    bind: BTreeMap<String, GeneratorSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequiresSpec {
    pool: String,
    min: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WorkloadExpect {
    rows: Option<ExpectedRows>,
    errors: Vec<String>,
}

#[derive(Debug, Clone)]
struct ExpectedRows {
    min: usize,
    max: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Sql,
    Transaction,
    Batch,
}

#[derive(Debug, Clone)]
struct CompiledStatement {
    sql: String,
    params: Vec<String>,
    bind: BTreeMap<String, GeneratorSpec>,
}

#[derive(Debug, Clone)]
struct ProducedBind {
    target: String,
    bind_name: String,
}

#[derive(Debug, Clone)]
struct CompiledOperation {
    name: String,
    weight: u64,
    kind: OperationKind,
    concurrent: bool,
    bind: BTreeMap<String, GeneratorSpec>,
    statements: Vec<CompiledStatement>,
    produces: Option<ProducedBind>,
    requires: Option<RequiresSpec>,
    expect: WorkloadExpect,
    ref_requirements: HashMap<String, RefRequirement>,
}

#[derive(Debug)]
struct GeneratedOperation {
    operation_idx: usize,
    name: String,
    kind: OperationKind,
    concurrent: bool,
    statements: Vec<GeneratedStatement>,
    expect: WorkloadExpect,
    pending: PendingPoolUpdates,
}

#[derive(Debug)]
struct GeneratedStatement {
    sql: String,
    params: Vec<String>,
    binds: BTreeMap<String, SqlValue>,
}

pub(crate) struct RunArgs {
    pub(crate) db: Arc<Mutex<turso::Database>>,
    pub(crate) sql_log: SqlLog,
    pub(crate) global_seed: u64,
    pub(crate) multi_progress: MultiProgress,
    pub(crate) progress_style: ProgressStyle,
}

#[derive(Default)]
struct OperationCounters {
    ok: AtomicU64,
    skip: AtomicU64,
    busy: AtomicU64,
    error: AtomicU64,
}

struct WorkloadStats {
    operations: Vec<OperationCounters>,
    error_samples: std::sync::Mutex<Vec<String>>,
}

impl WorkloadStats {
    fn new(operation_count: usize) -> Self {
        Self {
            operations: (0..operation_count)
                .map(|_| OperationCounters::default())
                .collect(),
            error_samples: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn record(&self, operation_idx: usize, outcome: &OperationOutcome) {
        let counters = &self.operations[operation_idx];
        match outcome {
            OperationOutcome::Ok | OperationOutcome::ExpectedError => {
                counters.ok.fetch_add(1, Ordering::Relaxed);
            }
            OperationOutcome::Skip => {
                counters.skip.fetch_add(1, Ordering::Relaxed);
            }
            OperationOutcome::Busy => {
                counters.busy.fetch_add(1, Ordering::Relaxed);
            }
            OperationOutcome::Error(message) => {
                counters.error.fetch_add(1, Ordering::Relaxed);
                let mut samples = self.error_samples.lock().unwrap();
                if samples.len() < ERROR_SAMPLES {
                    samples.push(message.clone());
                }
            }
        }
    }
}

#[derive(Debug)]
enum OperationOutcome {
    Ok,
    ExpectedError,
    Skip,
    Busy,
    Error(String),
}

const ERROR_SAMPLES: usize = 10;

#[derive(Debug, Clone, Default)]
struct RefRequirement {
    consumed: usize,
    sampled: bool,
}

impl RefRequirement {
    fn add(&mut self, consumes: bool) {
        if consumes {
            self.consumed += 1;
        } else {
            self.sampled = true;
        }
    }

    fn required_len(&self) -> usize {
        self.consumed + usize::from(self.sampled)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl SqlValue {
    fn null() -> Self {
        Self::Null
    }

    fn integer(value: i64) -> Self {
        Self::Integer(value)
    }

    fn real(value: f64) -> Self {
        Self::Real(value)
    }

    fn text(value: &str) -> Self {
        Self::Text(value.to_string())
    }

    fn bool(value: bool) -> Self {
        Self::Integer(i64::from(value))
    }

    fn sql(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Real(value) => value.to_string(),
            Self::Text(value) => quote_sql_text(value),
        }
    }

    fn turso_value(&self) -> turso::Value {
        match self {
            Self::Null => turso::Value::Null,
            Self::Integer(value) => turso::Value::Integer(*value),
            Self::Real(value) => turso::Value::Real(*value),
            Self::Text(value) => turso::Value::Text(value.clone()),
        }
    }
}

fn default_sequence_start() -> u64 {
    1
}

fn default_sequence_step() -> u64 {
    1
}

fn default_int_max() -> i64 {
    2_i64.pow(31)
}

fn default_real_max() -> f64 {
    1.0
}

fn default_string_length() -> usize {
    16
}

fn default_p_true() -> f64 {
    0.5
}

fn err(message: impl Into<String>) -> BoxError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

fn quote_sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn target_column(target: &str) -> Option<&str> {
    target.rsplit_once('.').map(|(_, column)| column)
}

fn pattern_sequence_key(pattern: &str) -> String {
    format!("pattern:{pattern}")
}

fn compiled_sequence_key(
    op: &CompiledOperation,
    bind_name: &str,
    generator: &GeneratorSpec,
) -> Option<String> {
    if let Some(pattern) = &generator.pattern {
        return Some(pattern_sequence_key(pattern));
    }
    generator.sequence.as_ref()?;
    if let Some(produces) = &op.produces {
        if produces.bind_name == bind_name {
            return Some(format!("sequence:{}", produces.target));
        }
    }
    Some(format!("operation.{}.{}", op.name, bind_name))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawExpectSpec {
    rows: Option<YamlValue>,
    #[serde(default)]
    errors: Vec<YamlValue>,
}

impl<'de> Deserialize<'de> for WorkloadExpect {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawExpectSpec::deserialize(deserializer)?;
        Self::from_raw(raw).map_err(de::Error::custom)
    }
}

impl WorkloadExpect {
    fn from_raw(raw: RawExpectSpec) -> Result<Self> {
        let rows = raw.rows.as_ref().map(parse_expected_rows).transpose()?;
        let mut errors = Vec::with_capacity(raw.errors.len());
        for error in raw.errors {
            errors.push(expect_error_to_string(&error)?);
        }
        Ok(Self { rows, errors })
    }

    pub(crate) fn check_rows(
        &self,
        actual: usize,
        operation: &str,
    ) -> std::result::Result<(), String> {
        let Some(expected) = &self.rows else {
            return Ok(());
        };
        if actual < expected.min || actual > expected.max {
            return Err(format!(
                "{operation}: expected {}..{} rows, got {actual}",
                expected.min, expected.max
            ));
        }
        Ok(())
    }

    pub(crate) fn error_is_expected(&self, error: &str) -> bool {
        self.errors.iter().any(|expected| error.contains(expected))
    }
}

fn parse_expected_rows(value: &YamlValue) -> Result<ExpectedRows> {
    match value {
        YamlValue::Number(value) => {
            let rows = value
                .as_u64()
                .ok_or_else(|| err("expect.rows must be a non-negative integer"))?
                as usize;
            Ok(ExpectedRows {
                min: rows,
                max: rows,
            })
        }
        YamlValue::String(value) => {
            let (min, max) = value.split_once("..").ok_or_else(|| {
                err(format!(
                    "invalid expect.rows '{value}' (use a number or n..m)"
                ))
            })?;
            let min = min.trim().parse::<usize>()?;
            let max = max.trim().parse::<usize>()?;
            if max < min {
                return Err(err(format!(
                    "invalid expect.rows '{value}': max must be >= min"
                )));
            }
            Ok(ExpectedRows { min, max })
        }
        _ => Err(err("expect.rows must be a number or n..m string")),
    }
}

fn expect_error_to_string(value: &YamlValue) -> Result<String> {
    match value {
        YamlValue::String(value) => Ok(value.clone()),
        YamlValue::Number(value) => Ok(value.to_string()),
        _ => Err(err("expect.errors entries must be strings or numbers")),
    }
}

fn validate_profile(profile: &WorkloadProfile) -> Result<()> {
    if profile.name.trim().is_empty() {
        return Err(err("missing top-level workload name"));
    }
    if profile.operations.is_empty() {
        return Err(err("workload must define at least one operation"));
    }
    for table in &profile.schema.tables {
        if table.name.trim().is_empty() {
            return Err(err("workload table without a name"));
        }
        if table.columns.is_empty() {
            return Err(err(format!("workload table {} has no columns", table.name)));
        }
    }
    for data in &profile.data {
        if let RowsSpec::Count(count) = data.rows {
            if count > 0 && data.columns.is_empty() {
                return Err(err(format!(
                    "data block for table {} must define columns when rows is a count",
                    data.table
                )));
            }
        }
        for (column, generator) in &data.columns {
            validate_generator(generator, &format!("data {}.{column}", data.table))?;
        }
    }
    for op in &profile.operations {
        validate_operation(op)?;
    }
    Ok(())
}

fn validate_operation(op: &OperationSpec) -> Result<()> {
    if op.name.trim().is_empty() {
        return Err(err("workload operation without a name"));
    }
    if op.weight == 0 {
        return Err(err(format!(
            "workload operation {} needs a positive weight",
            op.name
        )));
    }
    let bodies = usize::from(op.sql.is_some())
        + usize::from(op.transaction.is_some())
        + usize::from(op.batch.is_some());
    if bodies != 1 {
        return Err(err(format!(
            "workload operation {} must have exactly one of sql, transaction, or batch",
            op.name
        )));
    }
    for (bind, generator) in &op.bind {
        validate_generator(generator, &format!("operation {} bind {bind}", op.name))?;
    }
    for statement in op
        .transaction
        .iter()
        .flatten()
        .chain(op.batch.iter().flatten())
    {
        for (bind, generator) in &statement.bind {
            validate_generator(
                generator,
                &format!("operation {} statement bind {bind}", op.name),
            )?;
        }
    }
    if let Some(target) = &op.produces {
        validate_pool_target(target, &format!("operation {} produces", op.name))?;
        let Some(column) = target_column(target) else {
            return Err(err(format!(
                "operation {} produces target must be table.column",
                op.name
            )));
        };
        let statement_has_bind = op
            .transaction
            .iter()
            .flatten()
            .chain(op.batch.iter().flatten())
            .any(|statement| statement.bind.contains_key(column));
        if !op.bind.contains_key(column) && !statement_has_bind {
            return Err(err(format!(
                "operation {} produces {target:?} but no bind named {column:?} exists",
                op.name
            )));
        }
    }
    if let Some(requires) = &op.requires {
        if requires.pool.contains('.') {
            validate_pool_target(&requires.pool, &format!("operation {} requires", op.name))?;
        }
    }
    Ok(())
}

fn validate_generator(generator: &GeneratorSpec, context: &str) -> Result<()> {
    let kinds = [
        generator.pattern.is_some(),
        generator.sequence.is_some(),
        generator.int.is_some(),
        generator.real.is_some(),
        generator.string.is_some(),
        generator.timestamp.is_some(),
        generator.bool.is_some(),
        generator.enum_.is_some(),
        generator.const_.is_some(),
        generator.ref_.is_some(),
    ];
    let count = kinds.iter().filter(|kind| **kind).count();
    if count != 1 {
        return Err(err(format!(
            "{context}: generator must have exactly one kind, found {count}"
        )));
    }
    if generator.consume && generator.ref_.is_none() {
        return Err(err(format!(
            "{context}: consume is only valid on a ref generator"
        )));
    }
    if let Some(ref_spec) = &generator.ref_ {
        validate_pool_target(ref_spec.target(), context)?;
    }
    if let Some(sequence) = &generator.sequence {
        if sequence.step == 0 {
            return Err(err(format!("{context}: sequence step must be non-zero")));
        }
    }
    if let Some(int) = &generator.int {
        if int.max < int.min {
            return Err(err(format!("{context}: int generator max must be >= min")));
        }
        if !matches!(
            int.dist.as_deref(),
            None | Some("uniform") | Some("zipfian")
        ) {
            return Err(err(format!("{context}: unsupported int distribution")));
        }
    }
    if let Some(real) = &generator.real {
        if real.max < real.min {
            return Err(err(format!("{context}: real generator max must be >= min")));
        }
    }
    if let Some(string) = &generator.string {
        if !matches!(
            string.charset.as_deref(),
            None | Some("hex") | Some("alpha") | Some("ascii")
        ) {
            return Err(err(format!("{context}: unsupported string charset")));
        }
    }
    if let Some(timestamp) = &generator.timestamp {
        validate_timestamp(timestamp, context)?;
    }
    if let Some(bool_) = &generator.bool {
        if !(0.0..=1.0).contains(&bool_.p_true) {
            return Err(err(format!(
                "{context}: bool p_true must be between 0 and 1"
            )));
        }
    }
    if let Some(enum_) = &generator.enum_ {
        if enum_.values.is_empty() {
            return Err(err(format!(
                "{context}: enum generator must contain values"
            )));
        }
        if enum_.values.values().any(|weight| *weight < 0.0)
            || enum_.values.values().sum::<f64>() <= 0.0
        {
            return Err(err(format!(
                "{context}: enum weights must be non-negative with a positive sum"
            )));
        }
    }
    Ok(())
}

fn validate_timestamp(timestamp: &TimestampSpec, context: &str) -> Result<()> {
    match timestamp {
        TimestampSpec::Now(value) => {
            if !value.eq_ignore_ascii_case("now") {
                return Err(err(format!("{context}: timestamp word must be now")));
            }
        }
        TimestampSpec::Range { from, to } => {
            timestamp_value_to_millis(from.as_ref(), TimestampDefault::Epoch)?;
            timestamp_value_to_millis(to.as_ref(), TimestampDefault::Now)?;
        }
    }
    Ok(())
}

fn validate_pool_target(target: &str, context: &str) -> Result<()> {
    if target.split('.').count() != 2 {
        return Err(err(format!(
            "{context}: target {target:?} must be table.column"
        )));
    }
    Ok(())
}

fn compile_operations(profile: &WorkloadProfile) -> Result<Vec<CompiledOperation>> {
    let mut compiled = Vec::with_capacity(profile.operations.len());
    for op in &profile.operations {
        let (kind, statements) = compile_statement_sources(op);
        let statements = statements
            .into_iter()
            .map(|statement| {
                let params = extract_params(&statement.sql);
                for param in &params {
                    if !op.bind.contains_key(param) && !statement.bind.contains_key(param) {
                        return Err(err(format!(
                            "operation {} statement uses :{param} but no bind defines it: {}",
                            op.name, statement.sql
                        )));
                    }
                }
                Ok(CompiledStatement {
                    sql: statement.sql,
                    params,
                    bind: statement.bind,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let produces = op.produces.as_ref().map(|target| ProducedBind {
            target: target.clone(),
            bind_name: target_column(target)
                .expect("validate_operation checked produces target")
                .to_string(),
        });
        let mut ref_requirements = HashMap::new();
        collect_ref_requirements(&op.bind, &mut ref_requirements);
        for statement in &statements {
            collect_ref_requirements(&statement.bind, &mut ref_requirements);
        }
        compiled.push(CompiledOperation {
            name: op.name.clone(),
            weight: op.weight,
            kind,
            concurrent: op.concurrent,
            bind: op.bind.clone(),
            statements,
            produces,
            requires: op.requires.clone(),
            expect: op.expect.clone(),
            ref_requirements,
        });
    }
    Ok(compiled)
}

fn compile_statement_sources(op: &OperationSpec) -> (OperationKind, Vec<StatementSpec>) {
    if let Some(sql) = &op.sql {
        return (
            OperationKind::Sql,
            vec![StatementSpec {
                sql: sql.clone(),
                bind: BTreeMap::new(),
            }],
        );
    }
    if let Some(statements) = &op.transaction {
        return (OperationKind::Transaction, statements.clone());
    }
    (
        OperationKind::Batch,
        op.batch.clone().expect("validate_operation checked body"),
    )
}

impl WorkloadRuntime {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_yaml_str(&content)
    }

    fn from_yaml_str(content: &str) -> Result<Self> {
        let profile: WorkloadProfile = serde_yaml::from_str(content)?;
        Self::from_profile(profile)
    }

    fn from_profile(profile: WorkloadProfile) -> Result<Self> {
        validate_profile(&profile)?;
        let compiled_operations = compile_operations(&profile)?;
        let total_weight = profile.operations.iter().map(|op| op.weight).sum();
        if total_weight == 0 {
            return Err(err("workload operation weights must sum to more than zero"));
        }

        let pool_targets = collect_pool_targets(&profile);

        Ok(Self {
            profile,
            compiled_operations,
            pools: HashMap::new(),
            sequence_counters: HashMap::new(),
            pool_targets,
            total_weight,
        })
    }

    pub(crate) fn setup_sql(&mut self, rng: &mut ThreadRng) -> Result<Vec<String>> {
        let mut statements = Vec::new();

        for table in &self.profile.schema.tables {
            statements.push(table.create_table_sql()?);
            for index in &table.indexes {
                statements.push(index.create_index_sql(&table.name));
            }
            statements.extend(table.triggers.iter().cloned());
        }

        for data in self.profile.data.clone() {
            self.append_seed_sql(rng, &mut statements, &data)?;
        }

        Ok(statements)
    }

    fn operation_count(&self) -> usize {
        self.compiled_operations.len()
    }

    fn operation_name(&self, idx: usize) -> &str {
        &self.compiled_operations[idx].name
    }

    fn generate_operation(
        &mut self,
        rng: &mut ThreadRng,
    ) -> Result<std::result::Result<GeneratedOperation, usize>> {
        let idx = self.choose_operation_index(rng);
        let op = self.compiled_operations[idx].clone();
        if !self.compiled_operation_can_run(&op) {
            return Ok(Err(idx));
        }
        self.build_compiled_operation(rng, idx, &op).map(Ok)
    }

    fn build_compiled_operation(
        &mut self,
        rng: &mut ThreadRng,
        operation_idx: usize,
        op: &CompiledOperation,
    ) -> Result<GeneratedOperation> {
        let mut pending = PendingPoolUpdates::default();
        let mut op_binds = BTreeMap::new();

        for (name, generator) in &op.bind {
            let sequence_key = compiled_sequence_key(op, name, generator);
            let value = self.eval_generator(
                rng,
                generator,
                sequence_key.as_deref(),
                op.produces
                    .as_ref()
                    .map(|produces| produces.target.as_str()),
                Some(&mut pending),
            )?;
            if let Some(produces) = &op.produces {
                record_produced_bind(Some(&produces.target), name, &value, &mut pending);
            }
            op_binds.insert(name.clone(), value);
        }

        let mut statements = Vec::with_capacity(op.statements.len());
        for statement in &op.statements {
            let mut binds = op_binds.clone();
            for (name, generator) in &statement.bind {
                let sequence_key = compiled_sequence_key(op, name, generator);
                let value = self.eval_generator(
                    rng,
                    generator,
                    sequence_key.as_deref(),
                    op.produces
                        .as_ref()
                        .map(|produces| produces.target.as_str()),
                    Some(&mut pending),
                )?;
                if let Some(produces) = &op.produces {
                    record_produced_bind(Some(&produces.target), name, &value, &mut pending);
                }
                binds.insert(name.clone(), value);
            }
            statements.push(GeneratedStatement {
                sql: statement.sql.clone(),
                params: statement.params.clone(),
                binds,
            });
        }

        Ok(GeneratedOperation {
            operation_idx,
            name: op.name.clone(),
            kind: op.kind,
            concurrent: op.concurrent,
            statements,
            expect: op.expect.clone(),
            pending,
        })
    }

    fn compiled_operation_can_run(&self, op: &CompiledOperation) -> bool {
        if let Some(requires) = &op.requires {
            if self.pool_len(&requires.pool) < requires.min.unwrap_or(1) {
                return false;
            }
        }
        for (target, requirement) in &op.ref_requirements {
            if self.pool_len(target) < requirement.required_len() {
                return false;
            }
        }
        true
    }

    pub(crate) fn apply_success(&mut self, pending: PendingPoolUpdates) {
        for (target, value) in pending.produced {
            self.pools.entry(target).or_default().push(value);
        }
    }

    pub(crate) fn apply_failure(&mut self, pending: PendingPoolUpdates) {
        for (target, value) in pending.consumed {
            self.pools.entry(target).or_default().push(value);
        }
    }

    pub(crate) fn uses_concurrent_transactions(&self) -> bool {
        self.profile
            .operations
            .iter()
            .any(|op| op.concurrent && op.transaction.is_some())
    }

    fn append_seed_sql(
        &mut self,
        rng: &mut ThreadRng,
        statements: &mut Vec<String>,
        data: &DataSpec,
    ) -> Result<()> {
        match &data.rows {
            RowsSpec::Count(count) => {
                let columns = data.columns.keys().cloned().collect::<Vec<_>>();
                if columns.is_empty() && *count > 0 {
                    return Err(err(format!(
                        "data block for table {} must define columns when rows is a count",
                        data.table
                    )));
                }
                for start in (0..*count).step_by(INSERT_CHUNK_SIZE) {
                    let end = (start + INSERT_CHUNK_SIZE).min(*count);
                    let mut rows = Vec::with_capacity(end - start);

                    for _ in start..end {
                        let mut values = Vec::with_capacity(columns.len());
                        for column in &columns {
                            let target = format!("{}.{}", data.table, column);
                            let generator = data
                                .columns
                                .get(column)
                                .ok_or_else(|| err(format!("missing generator for {target}")))?;
                            let value =
                                self.eval_generator(rng, generator, Some(&target), None, None)?;
                            if self.pool_targets.contains(&target) {
                                self.pools.entry(target).or_default().push(value.clone());
                            }
                            values.push(value.sql());
                        }
                        rows.push(format!("({})", values.join(", ")));
                    }

                    if !rows.is_empty() {
                        statements.push(format!(
                            "INSERT INTO {} ({}) VALUES {};",
                            data.table,
                            columns.join(", "),
                            rows.join(", ")
                        ));
                    }
                }
            }
            RowsSpec::Literal(rows) => {
                for row in rows {
                    if row.is_empty() {
                        return Err(err(format!(
                            "literal data row for table {} must define at least one column",
                            data.table
                        )));
                    }
                    let columns = row.keys().cloned().collect::<Vec<_>>();
                    let mut values = Vec::with_capacity(columns.len());
                    for column in &columns {
                        let target = format!("{}.{}", data.table, column);
                        let value =
                            yaml_value_to_sql(row.get(column).ok_or_else(|| {
                                err(format!("missing literal value for {target}"))
                            })?)?;
                        if self.pool_targets.contains(&target) {
                            self.pools.entry(target).or_default().push(value.clone());
                        }
                        values.push(value.sql());
                    }
                    statements.push(format!(
                        "INSERT INTO {} ({}) VALUES ({});",
                        data.table,
                        columns.join(", "),
                        values.join(", ")
                    ));
                }
            }
        }

        Ok(())
    }

    fn choose_operation_index(&self, rng: &mut ThreadRng) -> usize {
        let mut remaining = rng.get_random() % self.total_weight;
        for (idx, op) in self.compiled_operations.iter().enumerate() {
            if remaining < op.weight {
                return idx;
            }
            remaining -= op.weight;
        }
        self.compiled_operations.len() - 1
    }

    fn pool_len(&self, target: &str) -> usize {
        if target.contains('.') {
            return self.pools.get(target).map_or(0, Vec::len);
        }

        self.pools
            .iter()
            .filter(|(pool, _)| pool.starts_with(&format!("{target}.")))
            .map(|(_, values)| values.len())
            .sum()
    }

    fn eval_generator(
        &mut self,
        rng: &mut ThreadRng,
        generator: &GeneratorSpec,
        sequence_key: Option<&str>,
        produced_target: Option<&str>,
        pending: Option<&mut PendingPoolUpdates>,
    ) -> Result<SqlValue> {
        if let Some(pattern) = &generator.pattern {
            let key = pattern_sequence_key(pattern);
            let seq = self.next_sequence(&key, 1, 1);
            return Ok(SqlValue::text(&format_pattern(pattern, seq, rng)?));
        }

        if let Some(sequence) = &generator.sequence {
            let key = sequence_key
                .map(ToString::to_string)
                .or_else(|| produced_target.map(|target| format!("sequence:{target}")))
                .unwrap_or_else(|| "sequence".to_string());
            let value = self.next_sequence(&key, sequence.start, sequence.step);
            return Ok(SqlValue::integer(value as i64));
        }

        if let Some(spec) = &generator.int {
            if spec.max < spec.min {
                return Err(err("int generator max must be >= min"));
            }
            let width = u64::try_from(spec.max as i128 - spec.min as i128 + 1)
                .map_err(|_| err("int generator range is too large"))?;
            let offset = if spec.dist.as_deref() == Some("zipfian") {
                let unit = rng.get_random() as f64 / u64::MAX as f64;
                let n = width as f64;
                let offset = ((unit * n.ln()).exp().floor() as u64).saturating_sub(1);
                offset.min(width - 1)
            } else {
                rng.get_random() % width
            };
            let value = i64::try_from(spec.min as i128 + offset as i128)
                .map_err(|_| err("int generator produced a value outside i64 range"))?;
            return Ok(SqlValue::integer(value));
        }

        if let Some(spec) = &generator.real {
            if spec.max < spec.min {
                return Err(err("real generator max must be >= min"));
            }
            let unit = rng.get_random() as f64 / u64::MAX as f64;
            return Ok(SqlValue::real(spec.min + (spec.max - spec.min) * unit));
        }

        if let Some(spec) = &generator.string {
            return Ok(SqlValue::text(&random_string(rng, spec)?));
        }

        if let Some(spec) = &generator.timestamp {
            return Ok(SqlValue::integer(generate_timestamp(rng, spec)?));
        }

        if let Some(spec) = &generator.bool {
            let unit = rng.get_random() as f64 / u64::MAX as f64;
            return Ok(SqlValue::bool(unit < spec.p_true));
        }

        if let Some(spec) = &generator.enum_ {
            return Ok(SqlValue::text(&choose_enum_value(rng, spec)?));
        }

        if let Some(value) = &generator.const_ {
            return yaml_value_to_sql(value);
        }

        if let Some(ref_spec) = &generator.ref_ {
            let target = ref_spec.target();
            let values = self
                .pools
                .get_mut(target)
                .ok_or_else(|| err(format!("ref pool '{target}' is empty")))?;
            if values.is_empty() {
                return Err(err(format!("ref pool '{target}' is empty")));
            }
            let idx = rng.get_random() as usize % values.len();
            let value = if generator.consume || ref_spec.consume() {
                let value = values.swap_remove(idx);
                if let Some(pending) = pending {
                    pending.consumed.push((target.to_string(), value.clone()));
                }
                value
            } else {
                values[idx].clone()
            };
            return Ok(value);
        }

        Err(err(
            "generator must have exactly one supported generator key",
        ))
    }

    fn next_sequence(&mut self, key: &str, start: u64, step: u64) -> u64 {
        let current = self
            .sequence_counters
            .entry(key.to_string())
            .or_insert(start);
        let value = *current;
        *current = current.saturating_add(step);
        value
    }
}

pub(crate) async fn run(opts: &Opts, args: RunArgs) -> Result<bool> {
    let workload_path = opts
        .workload
        .as_ref()
        .expect("caller checked workload path exists");
    let mut runtime = WorkloadRuntime::load(workload_path)?;
    match &runtime.profile.description {
        Some(description) => println!("workload={} - {description}", runtime.profile.name),
        None => println!("workload={}", runtime.profile.name),
    }

    let mvcc_enabled = opts.tx_mode == TxMode::Concurrent || runtime.uses_concurrent_transactions();
    let setup_conn = args.db.lock().await.connect()?;
    apply_session_pragmas(&setup_conn, opts, mvcc_enabled).await?;

    let mut setup_rng = ThreadRng::new(args.global_seed);
    seed_workload(&setup_conn, &mut runtime, &mut setup_rng, &args.sql_log).await?;
    prepare_workload_statements(&setup_conn, &runtime).await?;

    let stats = Arc::new(WorkloadStats::new(runtime.operation_count()));
    let runtime = Arc::new(std::sync::Mutex::new(runtime));
    let started = std::time::Instant::now();
    let mut handles = Vec::with_capacity(opts.nr_threads);

    for thread in 0..opts.nr_threads {
        let conn = args.db.lock().await.connect()?;
        apply_session_pragmas(&conn, opts, mvcc_enabled).await?;
        let progress_bar = args
            .multi_progress
            .add(ProgressBar::new(opts.nr_iterations as u64));
        progress_bar.set_style(args.progress_style.clone());
        progress_bar.set_prefix(format!("Thread {thread}"));
        progress_bar.set_message("executing operations...");

        let worker = WorkloadWorker {
            thread,
            conn,
            runtime: runtime.clone(),
            stats: stats.clone(),
            sql_log: args.sql_log.clone(),
            tx_mode: opts.tx_mode,
            nr_iterations: opts.nr_iterations,
            rng_seed: args.global_seed.wrapping_add(thread as u64 + 1),
            progress_bar,
        };
        handles.push(turso_stress::future::spawn(run_worker(worker)));
    }

    for handle in handles {
        handle.await??;
    }

    let runtime = runtime.lock().unwrap();
    print_summary(
        &runtime,
        &stats,
        started.elapsed(),
        opts.nr_threads,
        opts.nr_iterations,
    );
    Ok(mvcc_enabled)
}

async fn apply_session_pragmas(
    conn: &turso::Connection,
    opts: &Opts,
    mvcc_enabled: bool,
) -> Result<()> {
    if mvcc_enabled {
        conn.pragma_update("journal_mode", "mvcc").await?;
    } else {
        conn.pragma_update("journal_mode", "WAL").await?;
        conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;
    }
    conn.execute("PRAGMA data_sync_retry = 1", ()).await?;
    Ok(())
}

async fn seed_workload(
    conn: &turso::Connection,
    runtime: &mut WorkloadRuntime,
    rng: &mut ThreadRng,
    sql_log: &SqlLog,
) -> Result<()> {
    for statement in runtime.setup_sql(rng)? {
        match conn.execute(&statement, ()).await {
            Ok(_) => log_sql(sql_log, 0, &statement, "OK"),
            Err(error) => {
                log_sql(sql_log, 0, &statement, &format!("ERROR(fatal): {error}"));
                return Err(err(format!(
                    "workload setup failed while executing {statement}: {error}"
                )));
            }
        }
    }
    Ok(())
}

async fn prepare_workload_statements(
    conn: &turso::Connection,
    runtime: &WorkloadRuntime,
) -> Result<()> {
    for op in &runtime.compiled_operations {
        for statement in &op.statements {
            conn.prepare_cached(&statement.sql).await.map_err(|error| {
                err(format!(
                    "operation {} cannot prepare statement: {error}\n  {}",
                    op.name, statement.sql
                ))
            })?;
        }
    }
    Ok(())
}

struct WorkloadWorker {
    thread: usize,
    conn: turso::Connection,
    runtime: Arc<std::sync::Mutex<WorkloadRuntime>>,
    stats: Arc<WorkloadStats>,
    sql_log: SqlLog,
    tx_mode: TxMode,
    nr_iterations: usize,
    rng_seed: u64,
    progress_bar: ProgressBar,
}

async fn run_worker(worker: WorkloadWorker) -> Result<()> {
    let mut rng = ThreadRng::new(worker.rng_seed);

    for i in 0..worker.nr_iterations {
        let generated = {
            let mut runtime = worker.runtime.lock().unwrap();
            runtime.generate_operation(&mut rng)?
        };

        let generated = match generated {
            Ok(generated) => generated,
            Err(operation_idx) => {
                worker.stats.record(operation_idx, &OperationOutcome::Skip);
                worker.progress_bar.set_position(i as u64 + 1);
                continue;
            }
        };

        let outcome = execute_operation(
            &worker.conn,
            &generated,
            worker.tx_mode,
            &worker.sql_log,
            worker.thread,
        )
        .await?;

        {
            let mut runtime = worker.runtime.lock().unwrap();
            if matches!(outcome, OperationOutcome::Ok) {
                runtime.apply_success(generated.pending);
            } else {
                runtime.apply_failure(generated.pending);
            }
        }

        worker.stats.record(generated.operation_idx, &outcome);
        if let OperationOutcome::Error(message) = outcome {
            turso_macros::turso_assert_unreachable!("unexpected workload SQL error", { "thread": worker.thread, "operation": generated.name, "error": message });
        }
        worker.progress_bar.set_position(i as u64 + 1);
    }

    worker.progress_bar.finish_with_message("done");
    Ok(())
}

async fn execute_operation(
    conn: &turso::Connection,
    operation: &GeneratedOperation,
    tx_mode: TxMode,
    sql_log: &SqlLog,
    thread: usize,
) -> Result<OperationOutcome> {
    match execute_operation_body(conn, operation, tx_mode, sql_log, thread).await {
        Ok(rows) => {
            if let Err(error) = operation.expect.check_rows(rows, &operation.name) {
                return Ok(OperationOutcome::Error(error));
            }
            Ok(OperationOutcome::Ok)
        }
        Err(error) => Ok(classify_error(error, &operation.expect, thread)),
    }
}

async fn execute_operation_body(
    conn: &turso::Connection,
    operation: &GeneratedOperation,
    tx_mode: TxMode,
    sql_log: &SqlLog,
    thread: usize,
) -> std::result::Result<usize, turso::Error> {
    let mut rows = 0;
    match operation.kind {
        OperationKind::Sql => {
            for statement in &operation.statements {
                rows = execute_statement(conn, statement, sql_log, thread).await?;
            }
        }
        OperationKind::Transaction | OperationKind::Batch => {
            let begin = transaction_begin_sql(
                operation.kind == OperationKind::Transaction && operation.concurrent,
                tx_mode,
            );
            execute_logged(conn, begin, sql_log, thread).await?;
            for statement in &operation.statements {
                match execute_statement(conn, statement, sql_log, thread).await {
                    Ok(row_count) => rows = row_count,
                    Err(error) => {
                        let _ = execute_logged(conn, "ROLLBACK;", sql_log, thread).await;
                        return Err(error);
                    }
                }
            }
            if let Err(error) = execute_logged(conn, "COMMIT;", sql_log, thread).await {
                let _ = execute_logged(conn, "ROLLBACK;", sql_log, thread).await;
                return Err(error);
            }
        }
    }
    Ok(rows)
}

fn transaction_begin_sql(concurrent: bool, tx_mode: TxMode) -> &'static str {
    if concurrent || tx_mode == TxMode::Concurrent {
        "BEGIN CONCURRENT;"
    } else {
        "BEGIN;"
    }
}

async fn execute_logged(
    conn: &turso::Connection,
    sql: &str,
    sql_log: &SqlLog,
    thread: usize,
) -> std::result::Result<(), turso::Error> {
    match conn.execute(sql, ()).await {
        Ok(_) => {
            log_sql(sql_log, thread, sql, "OK");
            Ok(())
        }
        Err(error) => {
            log_sql(sql_log, thread, sql, &format!("ERROR: {error}"));
            Err(error)
        }
    }
}

async fn execute_statement(
    conn: &turso::Connection,
    statement: &GeneratedStatement,
    sql_log: &SqlLog,
    thread: usize,
) -> std::result::Result<usize, turso::Error> {
    let rendered = substitute_binds(&statement.sql, &statement.binds)
        .expect("compile checked all SQL params have binds");
    let result = execute_statement_inner(conn, statement).await;
    match &result {
        Ok(_) => log_sql(sql_log, thread, &rendered, "OK"),
        Err(error) => log_sql(sql_log, thread, &rendered, &format!("ERROR: {error}")),
    }
    result
}

async fn execute_statement_inner(
    conn: &turso::Connection,
    statement: &GeneratedStatement,
) -> std::result::Result<usize, turso::Error> {
    let params = statement
        .params
        .iter()
        .map(|param| {
            let value = statement
                .binds
                .get(param)
                .expect("compile checked all SQL params have binds");
            (format!(":{param}"), value.turso_value())
        })
        .collect::<Vec<_>>();
    let mut prepared = conn.prepare_cached(&statement.sql).await?;
    let mut rows = prepared.query(params).await?;
    let mut count = 0;
    while rows.next().await?.is_some() {
        count += 1;
    }
    Ok(count)
}

fn classify_error(error: turso::Error, expect: &WorkloadExpect, thread: usize) -> OperationOutcome {
    let text = error.to_string();
    if expect.error_is_expected(&text) {
        return OperationOutcome::ExpectedError;
    }
    match error {
        turso::Error::Corrupt(error) => {
            turso_macros::turso_assert_unreachable!("corrupt error executing workload operation", { "thread": thread, "error": error });
        }
        turso::Error::Busy(_) | turso::Error::BusySnapshot(_) => OperationOutcome::Busy,
        turso::Error::Error(error) if error == "Write-write conflict" => OperationOutcome::Busy,
        _ => OperationOutcome::Error(text),
    }
}

fn print_summary(
    runtime: &WorkloadRuntime,
    stats: &WorkloadStats,
    elapsed: std::time::Duration,
    nr_threads: usize,
    nr_iterations: usize,
) {
    println!();
    println!(
        "{:<28} {:>10} {:>8} {:>8} {:>8}",
        "operation", "ok", "skip", "busy", "error"
    );
    let mut totals = [0_u64; 4];
    for idx in 0..runtime.operation_count() {
        let counters = &stats.operations[idx];
        let row = [
            counters.ok.load(Ordering::Relaxed),
            counters.skip.load(Ordering::Relaxed),
            counters.busy.load(Ordering::Relaxed),
            counters.error.load(Ordering::Relaxed),
        ];
        for (total, value) in totals.iter_mut().zip(row) {
            *total += value;
        }
        println!(
            "{:<28} {:>10} {:>8} {:>8} {:>8}",
            runtime.operation_name(idx),
            row[0],
            row[1],
            row[2],
            row[3]
        );
    }
    println!(
        "{:<28} {:>10} {:>8} {:>8} {:>8}",
        "total", totals[0], totals[1], totals[2], totals[3]
    );
    let seconds = elapsed.as_secs_f64().max(1e-6);
    println!(
        "elapsed: {seconds:.2}s, {:.0} ops/sec ({nr_threads} threads x {nr_iterations} iterations)",
        (nr_threads * nr_iterations) as f64 / seconds
    );
    let samples = stats.error_samples.lock().unwrap();
    if !samples.is_empty() {
        println!("first {} error(s):", samples.len());
        for sample in samples.iter() {
            println!("  - {sample}");
        }
    }
}

impl RefSpec {
    fn target(&self) -> &str {
        match self {
            Self::Target(target) => target,
            Self::Options(options) => &options.target,
        }
    }

    fn consume(&self) -> bool {
        match self {
            Self::Target(_) => false,
            Self::Options(options) => options.consume,
        }
    }
}

impl TableSpec {
    fn create_table_sql(&self) -> Result<String> {
        let mut definitions = Vec::with_capacity(self.columns.len() + 1);
        for column in &self.columns {
            definitions.push(column.column_sql()?);
        }
        if !self.primary_key.is_empty() {
            definitions.push(format!("PRIMARY KEY ({})", self.primary_key.join(", ")));
        }
        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({});",
            self.name,
            definitions.join(", ")
        ))
    }
}

impl ColumnSpec {
    fn column_sql(&self) -> Result<String> {
        let sql_type = if let Some(sql_type) = &self.sql_type {
            sql_type.clone()
        } else {
            map_canonical_type(self.data_type.as_deref().unwrap_or("text"))?.to_string()
        };

        let mut parts = vec![self.name.clone(), sql_type];
        if let Some(expr) = &self.generated {
            parts.push(format!(
                "AS ({expr}) {}",
                if self.virtual_ { "VIRTUAL" } else { "STORED" }
            ));
        }
        if self.primary_key {
            parts.push("PRIMARY KEY".to_string());
        }
        if self.nullable == Some(false) {
            parts.push("NOT NULL".to_string());
        }
        if let Some(default) = &self.default {
            parts.push(format!("DEFAULT {}", default_sql(default)?));
        }
        if self.unique {
            parts.push("UNIQUE".to_string());
        }
        if let Some(reference) = &self.references {
            if let Some((table, column)) = reference.split_once('.') {
                parts.push(format!("REFERENCES {table}({column})"));
            } else {
                parts.push(format!("REFERENCES {reference}"));
            }
        }

        Ok(parts.join(" "))
    }
}

impl IndexSpec {
    fn create_index_sql(&self, table_name: &str) -> String {
        let unique = if self.unique { "UNIQUE " } else { "" };
        let where_clause = self
            .where_clause
            .as_ref()
            .map(|clause| format!(" WHERE {clause}"))
            .unwrap_or_default();
        format!(
            "CREATE {unique}INDEX IF NOT EXISTS {} ON {} ({}){};",
            self.name,
            table_name,
            self.columns.join(", "),
            where_clause
        )
    }
}

fn collect_pool_targets(profile: &WorkloadProfile) -> HashSet<String> {
    let mut targets = HashSet::new();

    for table in &profile.schema.tables {
        for column in &table.columns {
            if column.primary_key {
                targets.insert(format!("{}.{}", table.name, column.name));
            }
        }
        for column in &table.primary_key {
            targets.insert(format!("{}.{}", table.name, column));
        }
    }

    for operation in &profile.operations {
        if let Some(target) = &operation.produces {
            targets.insert(target.clone());
        }
        if let Some(requires) = &operation.requires {
            if requires.pool.contains('.') {
                targets.insert(requires.pool.clone());
            }
        }
        collect_generator_refs(&operation.bind, &mut targets);
        if let Some(statements) = &operation.transaction {
            collect_statement_refs(statements, &mut targets);
        }
        if let Some(statements) = &operation.batch {
            collect_statement_refs(statements, &mut targets);
        }
    }

    targets
}

fn collect_statement_refs(statements: &[StatementSpec], targets: &mut HashSet<String>) {
    for statement in statements {
        collect_generator_refs(&statement.bind, targets);
    }
}

fn collect_generator_refs(
    generators: &BTreeMap<String, GeneratorSpec>,
    targets: &mut HashSet<String>,
) {
    for generator in generators.values() {
        if let Some(ref_spec) = &generator.ref_ {
            targets.insert(ref_spec.target().to_string());
        }
    }
}

fn collect_ref_requirements(
    generators: &BTreeMap<String, GeneratorSpec>,
    requirements: &mut HashMap<String, RefRequirement>,
) {
    for generator in generators.values() {
        if let Some(ref_spec) = &generator.ref_ {
            requirements
                .entry(ref_spec.target().to_string())
                .or_default()
                .add(generator.consume || ref_spec.consume());
        }
    }
}

fn record_produced_bind(
    produced_target: Option<&str>,
    bind_name: &str,
    value: &SqlValue,
    pending: &mut PendingPoolUpdates,
) {
    let Some(target) = produced_target else {
        return;
    };
    if target_column(target) != Some(bind_name) {
        return;
    }
    if pending
        .produced
        .iter()
        .any(|(existing_target, existing_value)| {
            existing_target == target && existing_value == value
        })
    {
        return;
    }
    pending.produced.push((target.to_string(), value.clone()));
}

fn map_canonical_type(data_type: &str) -> Result<&'static str> {
    let sql_type = match data_type.to_ascii_lowercase().as_str() {
        "text" => "TEXT",
        "integer" => "INTEGER",
        "real" => "REAL",
        "blob" => "BLOB",
        "boolean" | "timestamp" => "INTEGER",
        "json" => "TEXT",
        _ => return Err(err(format!("unknown column type '{data_type}'"))),
    };
    Ok(sql_type)
}

fn default_sql(value: &YamlValue) -> Result<String> {
    match value {
        YamlValue::String(value) => Ok(quote_sql_text(value)),
        _ => Ok(yaml_value_to_sql(value)?.sql()),
    }
}

fn yaml_value_to_sql(value: &YamlValue) -> Result<SqlValue> {
    match value {
        YamlValue::Null => Ok(SqlValue::null()),
        YamlValue::Bool(value) => Ok(SqlValue::bool(*value)),
        YamlValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(SqlValue::integer(value))
            } else if let Some(value) = value.as_f64() {
                Ok(SqlValue::real(value))
            } else {
                Err(err("unsupported YAML number"))
            }
        }
        YamlValue::String(value) => Ok(SqlValue::text(value)),
        YamlValue::Sequence(_) | YamlValue::Mapping(_) | YamlValue::Tagged(_) => {
            let rendered = serde_yaml::to_string(value)?;
            Ok(SqlValue::text(rendered.trim()))
        }
    }
}

fn generate_timestamp(rng: &mut ThreadRng, spec: &TimestampSpec) -> Result<i64> {
    match spec {
        TimestampSpec::Now(value) => parse_timestamp_literal(value),
        TimestampSpec::Range { from, to } => {
            let start = timestamp_value_to_millis(from.as_ref(), TimestampDefault::Epoch)?;
            let end = timestamp_value_to_millis(to.as_ref(), TimestampDefault::Now)?;
            let lo = start.min(end);
            let hi = start.max(end);
            let width = (hi - lo)
                .checked_add(1)
                .ok_or_else(|| err("timestamp generator range is too large"))?
                as u64;
            Ok(lo + (rng.get_random() % width) as i64)
        }
    }
}

enum TimestampDefault {
    Epoch,
    Now,
}

fn timestamp_value_to_millis(value: Option<&YamlValue>, default: TimestampDefault) -> Result<i64> {
    let Some(value) = value else {
        return Ok(match default {
            TimestampDefault::Epoch => 0,
            TimestampDefault::Now => DEFAULT_NOW_TIMESTAMP_MS,
        });
    };
    match value {
        YamlValue::Number(value) => value
            .as_i64()
            .ok_or_else(|| err("timestamp numeric bounds must be signed integers")),
        YamlValue::String(value) => parse_timestamp_literal(value),
        YamlValue::Tagged(value) => timestamp_value_to_millis(Some(&value.value), default),
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Sequence(_) | YamlValue::Mapping(_) => {
            Err(err(
                "timestamp bounds must be 'now', Unix milliseconds, YYYY-MM-DD, or RFC3339",
            ))
        }
    }
}

fn parse_timestamp_literal(value: &str) -> Result<i64> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("now") {
        return Ok(DEFAULT_NOW_TIMESTAMP_MS);
    }
    if let Ok(milliseconds) = value.parse::<i64>() {
        return Ok(milliseconds);
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.timestamp_millis());
    }
    if let Ok(date_time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(date_time.and_utc().timestamp_millis());
    }
    if let Ok(date_time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(date_time.and_utc().timestamp_millis());
    }
    parse_yyyy_mm_dd_to_unix_ms(value)
}

fn parse_yyyy_mm_dd_to_unix_ms(value: &str) -> Result<i64> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| err(format!("invalid timestamp literal '{value}'")))?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| err(format!("invalid timestamp literal '{value}'")))
        .map(|date_time| date_time.and_utc().timestamp_millis())
}

fn extract_params(sql: &str) -> Vec<String> {
    let mut params = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' | '"' | '`' => skip_quoted_sql(&mut chars, ch),
            '[' => skip_bracketed_identifier(&mut chars),
            '-' if chars.peek() == Some(&'-') => skip_line_comment(&mut chars),
            '/' if chars.peek() == Some(&'*') => skip_block_comment(&mut chars),
            ':' => collect_param(&mut chars, &mut params),
            _ => {}
        }
    }

    params
}

fn skip_quoted_sql(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, quote: char) {
    while let Some(ch) = chars.next() {
        if ch == quote {
            if chars.peek() == Some(&quote) {
                chars.next();
            } else {
                break;
            }
        }
    }
}

fn skip_bracketed_identifier(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    for ch in chars.by_ref() {
        if ch == ']' {
            break;
        }
    }
}

fn skip_line_comment(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    chars.next();
    for ch in chars.by_ref() {
        if ch == '\n' {
            break;
        }
    }
}

fn skip_block_comment(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    chars.next();
    let mut previous = '\0';
    for ch in chars.by_ref() {
        if previous == '*' && ch == '/' {
            break;
        }
        previous = ch;
    }
}

fn collect_param(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, params: &mut Vec<String>) {
    let mut name = String::new();
    while let Some(next) = chars.peek().copied() {
        if next == '_' || next.is_ascii_alphanumeric() {
            name.push(next);
            chars.next();
        } else {
            break;
        }
    }
    if !name.is_empty() && !params.contains(&name) {
        params.push(name);
    }
}

fn substitute_binds(sql: &str, binds: &BTreeMap<String, SqlValue>) -> Result<String> {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => copy_quoted_sql(&mut chars, &mut out, '\''),
            '"' => copy_quoted_sql(&mut chars, &mut out, '"'),
            '`' => copy_quoted_sql(&mut chars, &mut out, '`'),
            '[' => copy_bracketed_identifier(&mut chars, &mut out),
            '-' if chars.peek() == Some(&'-') => copy_line_comment(&mut chars, &mut out),
            '/' if chars.peek() == Some(&'*') => copy_block_comment(&mut chars, &mut out),
            ':' => substitute_bind(&mut chars, &mut out, binds)?,
            _ => out.push(ch),
        }
    }

    Ok(out)
}

fn copy_quoted_sql(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    out: &mut String,
    quote: char,
) {
    out.push(quote);
    while let Some(ch) = chars.next() {
        out.push(ch);
        if ch == quote {
            if chars.peek() == Some(&quote) {
                out.push(chars.next().expect("peeked quote must exist"));
            } else {
                break;
            }
        }
    }
}

fn copy_bracketed_identifier(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    out: &mut String,
) {
    out.push('[');
    for ch in chars.by_ref() {
        out.push(ch);
        if ch == ']' {
            break;
        }
    }
}

fn copy_line_comment(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, out: &mut String) {
    out.push('-');
    out.push(chars.next().expect("peeked line comment marker must exist"));
    for ch in chars.by_ref() {
        out.push(ch);
        if ch == '\n' {
            break;
        }
    }
}

fn copy_block_comment(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, out: &mut String) {
    out.push('/');
    out.push(
        chars
            .next()
            .expect("peeked block comment marker must exist"),
    );
    let mut previous = '\0';
    for ch in chars.by_ref() {
        out.push(ch);
        if previous == '*' && ch == '/' {
            break;
        }
        previous = ch;
    }
}

fn substitute_bind(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    out: &mut String,
    binds: &BTreeMap<String, SqlValue>,
) -> Result<()> {
    let mut name = String::new();
    while let Some(next) = chars.peek().copied() {
        if next == '_' || next.is_ascii_alphanumeric() {
            name.push(next);
            chars.next();
        } else {
            break;
        }
    }

    if name.is_empty() {
        out.push(':');
    } else if let Some(value) = binds.get(&name) {
        out.push_str(&value.sql());
    } else {
        return Err(err(format!("missing bind value :{name}")));
    }
    Ok(())
}

fn format_pattern(pattern: &str, seq: u64, rng: &mut ThreadRng) -> Result<String> {
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut rest = pattern;

    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 1..];
        let close = after_open
            .find('}')
            .ok_or_else(|| err(format!("unclosed pattern placeholder in '{pattern}'")))?;
        let placeholder = &after_open[..close];

        if placeholder == "seq" {
            out.push_str(&seq.to_string());
        } else if let Some(width) = placeholder.strip_prefix("seq:0") {
            let width = width.parse::<usize>()?;
            out.push_str(&format!("{seq:0width$}"));
        } else if let Some(length) = placeholder.strip_prefix("rand:") {
            let length = length.parse::<usize>()?;
            out.push_str(&random_ascii(rng, length));
        } else if placeholder == "uuid" {
            out.push_str(&random_uuid(rng));
        } else {
            return Err(err(format!(
                "unsupported pattern placeholder '{placeholder}'"
            )));
        }

        rest = &after_open[close + 1..];
    }

    out.push_str(rest);
    Ok(out)
}

fn random_string(rng: &mut ThreadRng, spec: &StringSpec) -> Result<String> {
    match spec.charset.as_deref() {
        Some("hex") => Ok(random_from_alphabet(rng, b"0123456789abcdef", spec.length)),
        Some("alpha") => Ok(random_from_alphabet(
            rng,
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
            spec.length,
        )),
        Some("ascii") | None => Ok(random_ascii(rng, spec.length)),
        Some(charset) => Err(err(format!("unknown charset '{charset}'"))),
    }
}

fn random_ascii(rng: &mut ThreadRng, length: usize) -> String {
    random_from_alphabet(
        rng,
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.",
        length,
    )
}

fn random_from_alphabet(rng: &mut ThreadRng, alphabet: &[u8], length: usize) -> String {
    let mut out = String::with_capacity(length);
    for _ in 0..length {
        out.push(alphabet[rng.get_random() as usize % alphabet.len()] as char);
    }
    out
}

fn random_uuid(rng: &mut ThreadRng) -> String {
    let a = rng.get_random();
    let b = rng.get_random();
    let c = rng.get_random();
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (a >> 32) as u32,
        (a >> 16) as u16,
        (a & 0x0fff) as u16,
        (((b >> 48) as u16) & 0x3fff) | 0x8000,
        c & 0x0000_ffff_ffff_ffff
    )
}

fn choose_enum_value(rng: &mut ThreadRng, spec: &EnumSpec) -> Result<String> {
    let total: f64 = spec.values.values().sum();
    if total <= 0.0 {
        return Err(err("enum generator weights must sum to more than zero"));
    }

    let mut remaining = (rng.get_random() as f64 / u64::MAX as f64) * total;
    for (value, weight) in &spec.values {
        if remaining <= *weight {
            return Ok(value.clone());
        }
        remaining -= *weight;
    }

    spec.values
        .keys()
        .next_back()
        .cloned()
        .ok_or_else(|| err("enum generator must contain at least one value"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_are_extracted_outside_literals_and_comments() {
        assert_eq!(
            extract_params("SELECT * FROM t WHERE id = :id AND ts > :after OR id = :id"),
            ["id", "after"]
        );
        assert_eq!(
            extract_params(
                r#"INSERT INTO t VALUES (json('{"type":"none"}'), :v, 'it''s :not', "q:no")"#
            ),
            ["v"]
        );
        assert_eq!(
            extract_params("SELECT ':x', [bracket:y] -- :z\n WHERE a = :a"),
            ["a"]
        );
    }

    #[test]
    fn bind_substitution_skips_literals_and_comments() {
        let mut binds = BTreeMap::new();
        binds.insert("id".to_string(), SqlValue::text("actual"));

        let sql = "SELECT '12:34', ':id', \"quoted:id\", [bracket:id] \
                   -- comment :id\n\
                   /* block :id */ WHERE id = :id";
        let substituted = substitute_binds(sql, &binds).unwrap();

        assert!(substituted.contains("'12:34'"));
        assert!(substituted.contains("':id'"));
        assert!(substituted.contains("\"quoted:id\""));
        assert!(substituted.contains("[bracket:id]"));
        assert!(substituted.contains("-- comment :id"));
        assert!(substituted.contains("/* block :id */"));
        assert!(substituted.contains("WHERE id = 'actual'"));
    }

    #[test]
    fn zero_weight_operations_are_rejected() {
        let err = WorkloadRuntime::from_yaml_str(
            r#"
name: zero-weight-fallback
schema:
  tables:
    - name: users
      columns:
        - { name: id, type: text, primary_key: true }
operations:
  - name: blocked
    weight: 1
    sql: "SELECT :id"
    bind:
      id: { ref: users.id }
  - name: disabled
    weight: 0
    sql: "SELECT 1"
"#,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("operation disabled needs a positive weight"));
    }

    #[test]
    fn timestamp_range_honors_profile_bounds() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: timestamp-range
schema:
  tables:
    - name: events
      columns:
        - { name: id, type: text, primary_key: true }
operations:
  - name: select_timestamp
    weight: 1
    sql: "SELECT :ts"
    bind:
      ts: { timestamp: { from: 2024-01-01, to: 2024-01-01 } }
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);

        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();

        assert_eq!(generated.statements[0].sql, "SELECT :ts");
        assert_eq!(
            generated.statements[0].binds.get("ts").unwrap().sql(),
            BASE_TIMESTAMP_MS.to_string()
        );
    }

    #[test]
    fn timestamp_now_is_deterministic() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: timestamp-now
schema:
  tables:
    - name: events
      columns:
        - { name: id, type: text, primary_key: true }
operations:
  - name: select_timestamp
    weight: 1
    sql: "SELECT :ts"
    bind:
      ts: { timestamp: now }
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);

        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();

        assert_eq!(
            generated.statements[0].binds.get("ts").unwrap().sql(),
            DEFAULT_NOW_TIMESTAMP_MS.to_string()
        );
    }

    #[test]
    fn generator_defaults_match_typescript_workload_runner() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: generator-defaults
schema:
  tables: []
operations:
  - name: defaults
    weight: 1
    sql: "SELECT :i, :r, length(:s), :b"
    bind:
      i: { int: {} }
      r: { real: {} }
      s: { string: {} }
      b: { bool: {} }
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);

        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();
        let rendered =
            substitute_binds(&generated.statements[0].sql, &generated.statements[0].binds).unwrap();

        assert!(rendered.starts_with("SELECT "));
        assert!(rendered.contains("length('"));
    }

    #[test]
    fn expect_rows_and_errors_are_parsed() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: expectations
schema:
  tables: []
operations:
  - name: expected
    weight: 1
    sql: "SELECT 1"
    expect:
      rows: 1..2
      errors: ["SQLITE_BUSY", 2067]
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);

        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();

        assert!(generated.expect.check_rows(2, "expected").is_ok());
        assert!(generated
            .expect
            .error_is_expected("error code 2067 constraint"));
        assert!(generated.expect.error_is_expected("SQLITE_BUSY"));
    }

    #[test]
    fn ddl_type_mapping_matches_typescript_workload_runner() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: ddl-types
schema:
  tables:
    - name: typed
      columns:
        - { name: id, type: text, primary_key: true }
        - { name: payload, type: json }
        - { name: at, type: timestamp }
        - { name: enabled, type: boolean }
operations:
  - name: noop
    weight: 1
    sql: "SELECT 1"
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);
        let ddl = runtime.setup_sql(&mut rng).unwrap();

        assert_eq!(
            ddl[0],
            "CREATE TABLE IF NOT EXISTS typed (id TEXT PRIMARY KEY, payload TEXT, at INTEGER, enabled INTEGER);"
        );
    }

    #[test]
    fn generated_seed_rows_require_columns() {
        let err = WorkloadRuntime::from_yaml_str(
            r#"
name: empty-seed-columns
schema:
  tables:
    - name: defaults
      columns:
        - { name: id, type: integer, primary_key: true }
data:
  - table: defaults
    rows: 1
operations:
  - name: noop
    weight: 1
    sql: "SELECT 1"
"#,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("must define columns when rows is a count"));
    }

    #[test]
    fn statement_level_refs_are_checked_before_operation_is_selected() {
        let runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: statement-local-ref
schema:
  tables:
    - name: users
      columns:
        - { name: id, type: text, primary_key: true }
    - name: logs
      columns:
        - { name: id, type: text, primary_key: true }
        - { name: user_id, type: text }
operations:
  - name: uses_missing_statement_ref
    weight: 1
    transaction:
      - sql: "INSERT INTO logs (id, user_id) VALUES (:id, :user_id)"
        bind:
          id: { pattern: "log-{seq}" }
          user_id: { ref: users.id }
  - name: fallback
    weight: 1
    sql: "SELECT 1"
"#,
        )
        .unwrap();

        assert!(!runtime.compiled_operation_can_run(&runtime.compiled_operations[0]));
        assert!(runtime.compiled_operation_can_run(&runtime.compiled_operations[1]));
    }

    #[test]
    fn statement_level_produced_values_are_recorded() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: statement-local-produces
schema:
  tables:
    - name: items
      columns:
        - { name: id, type: text, primary_key: true }
operations:
  - name: insert_item
    weight: 1
    produces: items.id
    transaction:
      - sql: "INSERT INTO items (id) VALUES (:id)"
        bind:
          id: { pattern: "item-{seq}" }
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);

        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();

        assert_eq!(generated.pending.produced.len(), 1);
        assert_eq!(generated.pending.produced[0].0, "items.id");
        assert_eq!(generated.pending.produced[0].1.sql(), "'item-1'");
    }

    #[test]
    fn compile_rejects_unbound_params() {
        let err = WorkloadRuntime::from_yaml_str(
            r#"
name: unbound-param
schema:
  tables: []
operations:
  - name: bad
    weight: 1
    sql: "SELECT :missing"
"#,
        )
        .unwrap_err();

        assert!(err.to_string().contains(":missing"));
    }

    #[test]
    fn validation_rejects_bad_workloads() {
        let cases = [
            (
                "name: t\noperations:\n  - name: x\n    weight: 1\n    sql: SELECT 1\n    bind:\n      a: { int: {}, real: {} }",
                "exactly one kind",
            ),
            (
                "name: t\noperations:\n  - name: x\n    weight: 1\n    sql: SELECT 1\n    bind:\n      a: { const: 1, consume: true }",
                "consume is only valid",
            ),
            (
                "name: t\noperations:\n  - name: x\n    weight: 1\n    sql: SELECT 1\n    produces: core",
                "table.column",
            ),
            (
                "name: t\noperations:\n  - name: x\n    weight: 1\n    sql: SELECT 1\n    unknown: true",
                "unknown field",
            ),
        ];

        for (yaml, expected) in cases {
            let err = WorkloadRuntime::from_yaml_str(yaml).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "expected {expected:?}, got {err}"
            );
        }
    }

    #[test]
    fn pattern_sequence_is_shared_between_seed_and_runtime() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: pattern-sharing
schema:
  tables:
    - name: items
      columns:
        - { name: id, type: text, primary_key: true }
data:
  - table: items
    rows: 2
    columns:
      id: { pattern: "item-{seq}" }
operations:
  - name: insert_item
    weight: 1
    produces: items.id
    bind:
      id: { pattern: "item-{seq}" }
    sql: "INSERT INTO items (id) VALUES (:id)"
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);

        runtime.setup_sql(&mut rng).unwrap();
        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();

        assert_eq!(
            generated.statements[0].binds.get("id").unwrap().sql(),
            "'item-3'"
        );
    }

    fn test_sql_log() -> SqlLog {
        Arc::new(std::sync::Mutex::new(std::io::BufWriter::new(
            tempfile::tempfile().unwrap(),
        )))
    }

    async fn memory_connection() -> turso::Connection {
        let db = turso::Builder::new_local(":memory:").build().await.unwrap();
        db.connect().unwrap()
    }

    #[tokio::test]
    async fn workload_execution_uses_real_parameters() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: real-params
schema:
  tables: []
operations:
  - name: insert_note
    weight: 1
    bind:
      id: { const: 1 }
    sql: "INSERT INTO t (id, note) VALUES (:id, ':id')"
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);
        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();
        let conn = memory_connection().await;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, note TEXT)", ())
            .await
            .unwrap();

        let outcome = execute_operation(&conn, &generated, TxMode::SQLite, &test_sql_log(), 0)
            .await
            .unwrap();

        assert!(matches!(outcome, OperationOutcome::Ok));
        let mut rows = conn
            .query("SELECT note FROM t WHERE id = 1", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), ":id");
    }

    #[tokio::test]
    async fn workload_batch_rolls_back_after_partial_failure() {
        let mut runtime = WorkloadRuntime::from_yaml_str(
            r#"
name: batch-rollback
schema:
  tables: []
operations:
  - name: duplicate_batch
    weight: 1
    batch:
      - sql: "INSERT INTO t(id) VALUES (1)"
      - sql: "INSERT INTO t(id) VALUES (1)"
      - sql: "INSERT INTO t(id) VALUES (2)"
"#,
        )
        .unwrap();
        let mut rng = ThreadRng::new(1);
        let generated = runtime.generate_operation(&mut rng).unwrap().unwrap();
        let conn = memory_connection().await;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)", ())
            .await
            .unwrap();

        let outcome = execute_operation(&conn, &generated, TxMode::SQLite, &test_sql_log(), 0)
            .await
            .unwrap();

        assert!(matches!(outcome, OperationOutcome::Error(_)));
        let mut rows = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 0);
    }

    #[test]
    fn tx_mode_concurrent_overrides_operation_flag() {
        assert_eq!(
            transaction_begin_sql(false, TxMode::Concurrent),
            "BEGIN CONCURRENT;"
        );
        assert_eq!(
            transaction_begin_sql(true, TxMode::SQLite),
            "BEGIN CONCURRENT;"
        );
    }
}
