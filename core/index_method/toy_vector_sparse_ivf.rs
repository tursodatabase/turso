use std::collections::{BTreeSet, HashSet, VecDeque};

use crate::coro::{with_handle, BoxedResumable, Co, Runner, StepContext, YieldSlot};
use crate::types::{IOCompletions, IOResultOr};
use turso_parser::ast::{self, SortOrder};

use crate::numeric::Numeric;
use crate::{
    index_method::{
        parse_patterns, BackingIndex, BackingSchema, BackingStoreOp, IndexMethod,
        IndexMethodAttachment, IndexMethodConfiguration, IndexMethodContext, IndexMethodCursor,
        IndexMethodDefinition, TOY_VECTOR_SPARSE_IVF_INDEX_METHOD_NAME,
    },
    return_if_io,
    storage::btree::{BTreeKey, CursorTrait},
    sync::Arc,
    translate::collate::CollationSeq,
    types::{IOResult, ImmutableRecord, KeyInfo, SeekKey, SeekOp, SeekResult},
    vdbe::Register,
    vector::{
        operations,
        vector_types::{Vector, VectorType},
    },
    LimboError, Result, Value, ValueRef,
};

/// Simple inverted index for sparse vectors
/// > CREATE INDEX t_idx ON t USING toy_vector_sparse_ivf (embedding)
///
/// It accept single column which must contain vector encoded in sparse format (e.g. vector32_sparse(...))
/// It can handle jaccard similarity scoring queries like the following:
/// > SELECT vector_distance_jaccard(embedding, ?) as d FROM t ORDER BY d LIMIT ?
#[derive(Debug)]
pub struct VectorSparseInvertedIndexMethod;

#[derive(Debug)]
pub struct VectorSparseInvertedIndexMethodAttachment {
    configuration: IndexMethodConfiguration,
    patterns: Vec<ast::Select>,
}

/// Names [`VectorCtx`] as the context type of the async operations of the
/// sparse vector index cursor.
struct VectorStep;

impl StepContext for VectorStep {
    type Error = Box<LimboError>;
    type Ctx<'a> = VectorCtx<'a>;
}

/// The context of one step of an async operation of the cursor. The async
/// function gets it back on every step, so it never keeps the cursor or the
/// registers across a yield.
struct VectorCtx<'a> {
    cursor: &'a mut VectorSparseInvertedIndexMethodCursor,
    /// The registers the VDBE passes on every step of `insert`, `delete`,
    /// and `query_start`.
    values: &'a [Register],
    io: Option<IOCompletions>,
    err: Option<Box<LimboError>>,
}

impl YieldSlot<Box<LimboError>> for VectorCtx<'_> {
    fn park_io(&mut self, io: IOCompletions) {
        self.io = Some(io);
    }

    fn take_io(&mut self) -> Option<IOCompletions> {
        self.io.take()
    }

    fn park_err(&mut self, err: Box<LimboError>) {
        self.err = Some(err);
    }

    fn take_err(&mut self) -> Option<Box<LimboError>> {
        self.err.take()
    }
}

type VectorOp<Out> = BoxedResumable<VectorStep, (), Out>;

/// The runners of the cursor operations. Each one is boxed on first use
/// and reused for the next operation.
#[derive(Default)]
struct VectorOps {
    insert: Option<VectorOp<()>>,
    delete: Option<VectorOp<()>>,
    search: Option<VectorOp<bool>>,
}

#[derive(Debug, PartialEq)]
struct FloatOrd(f64);

impl Eq for FloatOrd {}
impl PartialOrd for FloatOrd {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FloatOrd {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug)]
struct ComponentStat {
    position: u32,
    cnt: i64,
    min: f64,
    max: f64,
}

fn parse_stat_row(record: Option<&ImmutableRecord>) -> Result<ComponentStat> {
    let Some(record) = record else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected row".to_string(),
        ));
    };
    let ValueRef::Numeric(Numeric::Integer(position)) = record.get_value(0)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected integer".to_string(),
        ));
    };
    let ValueRef::Numeric(Numeric::Integer(cnt)) = record.get_value(1)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected integer".to_string(),
        ));
    };
    let ValueRef::Numeric(Numeric::Float(min)) = record.get_value(2)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected float".to_string(),
        ));
    };
    let ValueRef::Numeric(Numeric::Float(max)) = record.get_value(3)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected float".to_string(),
        ));
    };
    Ok(ComponentStat {
        position: position as u32,
        cnt,
        min: f64::from(min),
        max: f64::from(max),
    })
}
#[derive(Debug)]
struct ComponentRow {
    position: u32,
    sum: f64,
    rowid: i64,
}

fn parse_inverted_index_row(record: Option<&ImmutableRecord>) -> Result<ComponentRow> {
    let Some(record) = record else {
        return Err(LimboError::Corrupt(
            "inverted index corrupted: expected row".to_string(),
        ));
    };
    let ValueRef::Numeric(Numeric::Integer(position)) = record.get_value(0)? else {
        return Err(LimboError::Corrupt(
            "inverted index corrupted: expected integer".to_string(),
        ));
    };
    let ValueRef::Numeric(Numeric::Float(sum)) = record.get_value(1)? else {
        return Err(LimboError::Corrupt(
            "inverted index corrupted: expected float".to_string(),
        ));
    };
    let ValueRef::Numeric(Numeric::Integer(rowid)) = record.get_value(2)? else {
        return Err(LimboError::Corrupt(
            "inverted index corrupted: expected integer".to_string(),
        ));
    };
    Ok(ComponentRow {
        position: position as u32,
        sum: f64::from(sum),
        rowid,
    })
}

#[derive(Debug, PartialEq)]
pub enum ScanOrder {
    DatasetFrequencyAsc,
    QueryWeightDesc,
}

pub struct VectorSparseInvertedIndexMethodCursor {
    configuration: IndexMethodConfiguration,
    delta: f64,
    scan_portion: f64,
    scan_order: ScanOrder,
    schema: BackingSchema,
    inverted_index_cursor: Option<Box<dyn CursorTrait>>,
    stats_cursor: Option<Box<dyn CursorTrait>>,
    main_btree: Option<Box<dyn CursorTrait>>,
    pending_store_ops: VecDeque<BackingStoreOp>,
    ops: VectorOps,
    search_result: VecDeque<(i64, f64)>,
}

impl IndexMethod for VectorSparseInvertedIndexMethod {
    fn attach(
        &self,
        configuration: &IndexMethodConfiguration,
    ) -> Result<Arc<dyn IndexMethodAttachment>> {
        let query_pattern1 = format!(
            "SELECT vector_distance_jaccard({}, ?) as distance FROM {} ORDER BY distance LIMIT ?",
            configuration.columns[0].name, configuration.table_name
        );
        let query_pattern2 = format!(
            "SELECT vector_distance_jaccard(?, {}) as distance FROM {} ORDER BY distance LIMIT ?",
            configuration.columns[0].name, configuration.table_name
        );
        Ok(Arc::new(VectorSparseInvertedIndexMethodAttachment {
            configuration: configuration.clone(),
            patterns: parse_patterns(&[&query_pattern1, &query_pattern2])?,
        }))
    }
}

impl IndexMethodAttachment for VectorSparseInvertedIndexMethodAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: TOY_VECTOR_SPARSE_IVF_INDEX_METHOD_NAME,
            table_name: &self.configuration.table_name,
            index_name: &self.configuration.index_name,
            patterns: self.patterns.as_slice(),
            backing_btree: false,
            results_materialized: true,
            mvcc_support: super::IndexMethodMvccSupport::TransactionalBackingStore,
        }
    }
    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        Ok(Box::new(VectorSparseInvertedIndexMethodCursor::new(
            self.configuration.clone(),
        )))
    }
}

impl VectorSparseInvertedIndexMethodCursor {
    pub fn new(configuration: IndexMethodConfiguration) -> Self {
        let columns = configuration
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let schema = BackingSchema::new(
            Vec::new(),
            vec![
                BackingIndex::on_table(
                    &configuration.table_name,
                    format!("{}_inverted_index", configuration.index_name),
                    columns.clone(),
                    // component, length, rowid
                    vec![key_info(), key_info(), key_info()],
                ),
                BackingIndex::on_table(
                    &configuration.table_name,
                    format!("{}_stats", configuration.index_name),
                    columns,
                    // component
                    vec![key_info()],
                ),
            ],
        );
        let delta = match configuration.parameters.get("delta") {
            Some(&Value::Numeric(Numeric::Float(delta))) => f64::from(delta),
            _ => 0.0,
        };
        let scan_portion = match configuration.parameters.get("scan_portion") {
            Some(&Value::Numeric(Numeric::Float(scan_portion))) => f64::from(scan_portion),
            _ => 1.0,
        };
        let scan_order = match configuration.parameters.get("scan_order") {
            Some(Value::Text(scan_order)) if scan_order.as_str() == "dataset_frequency_asc" => {
                ScanOrder::DatasetFrequencyAsc
            }
            Some(Value::Text(scan_order)) if scan_order.as_str() == "query_weight_desc" => {
                ScanOrder::QueryWeightDesc
            }
            _ => ScanOrder::QueryWeightDesc,
        };
        Self {
            configuration,
            delta,
            scan_portion,
            scan_order,
            schema,
            inverted_index_cursor: None,
            stats_cursor: None,
            main_btree: None,
            pending_store_ops: VecDeque::new(),
            search_result: VecDeque::new(),
            ops: VectorOps::default(),
        }
    }

    fn drive_pending_store_ops(&mut self) -> IOResultOr<()> {
        while let Some(op) = self.pending_store_ops.front_mut() {
            return_if_io!(op.step());
            self.pending_store_ops.pop_front();
        }
        Ok(IOResult::Done(()))
    }

    fn open_store_cursors(&mut self, context: &IndexMethodContext) -> Result<()> {
        self.inverted_index_cursor = Some(open_store_cursor(context, &self.schema.indexes[0])?);
        self.stats_cursor = Some(open_store_cursor(context, &self.schema.indexes[1])?);
        Ok(())
    }

    fn inverted_index(&mut self) -> Result<&mut dyn CursorTrait, Box<LimboError>> {
        self.inverted_index_cursor
            .as_deref_mut()
            .ok_or_else(|| LimboError::InternalError("cursor must be opened".to_string()).into())
    }

    fn stats(&mut self) -> Result<&mut dyn CursorTrait, Box<LimboError>> {
        self.stats_cursor
            .as_deref_mut()
            .ok_or_else(|| LimboError::InternalError("cursor must be opened".to_string()).into())
    }

    fn main_table(&mut self) -> Result<&mut dyn CursorTrait, Box<LimboError>> {
        self.main_btree
            .as_deref_mut()
            .ok_or_else(|| LimboError::InternalError("cursor must be opened".to_string()).into())
    }

    /// Orders the components of a query by the scan order and keeps the
    /// scan portion of them.
    fn order_components(&self, mut components: Vec<(ComponentStat, f32)>) -> Vec<ComponentStat> {
        match self.scan_order {
            ScanOrder::DatasetFrequencyAsc => {
                // order by cnt ASC in order to check low-cardinality components first
                components.sort_by_key(|(c, _)| c.cnt);
            }
            ScanOrder::QueryWeightDesc => {
                // order by weight DESC in order to check high-impact components first
                components.sort_by_key(|(_, w)| std::cmp::Reverse(FloatOrd(*w as f64)));
            }
        }
        let take = (components.len() as f64 * self.scan_portion).ceil() as usize;
        components
            .into_iter()
            .take(take)
            .map(|(c, _)| c)
            .collect::<Vec<_>>()
    }

    /// The largest component sum a row can have and still beat the worst
    /// of the `limit` best distances found so far. `None` when the search
    /// has no bound yet.
    fn sum_threshold(
        &self,
        components: &VecDeque<ComponentStat>,
        distances: &BTreeSet<(FloatOrd, i64)>,
        limit: i64,
        sum: f64,
    ) -> Option<f64> {
        // we estimate jaccard distance with the following approach:
        // J = min(L, M1 + M2 + ... + Mr) / (Q + N - min(L, M1 + M2 + ... + Mr))
        // so we want J > best + delta; define M1 + M2 + ... + Mr = M
        // J = min(L, M) / (Q + L - min(L, M)) > best + delta
        // we need to consider two cases:
        // 1. L < M: J = L / (Q + L - L) > best + delta => L > (best + delta) * Q
        // 2. L > M: J = M / (Q + L - M) > best + delta => M > (best + delta) * (Q + L - M) => L < M / (best + delta) - (Q - M)
        // so we have two intervals: [(best + delta) * Q .. M] and [M .. M / (best + delta) - (Q - M)]
        // to simplify code for now we will pick upper bound from second range if it is not degenerate, otherwise check first range
        let m = components.iter().map(|c| c.max).sum::<f64>().min(sum);
        if distances.len() < limit as usize {
            return None;
        }
        let (max_threshold, _) = distances.last()?;
        let best = 1.0 - max_threshold.0;
        if best <= 0.0 {
            return None;
        }
        let delta = self.delta;
        let q = sum;
        let first_range_l = (best + delta) * q;
        let second_range_r = m / (best + delta) - (q - m);
        let sum_threshold = if m <= second_range_r {
            second_range_r
        } else if first_range_l <= m {
            m
        } else {
            -1.0
        };
        tracing::debug!(
            "sum_threshold={:?}, max_threshold={}, remained_sum={}, sum={}, components={:?}",
            Some(sum_threshold),
            best,
            m,
            sum,
            components
        );
        Some(sum_threshold)
    }
}

/// The sparse vector and the rowid of an insert or a delete.
fn vector_and_rowid(values: &[Register]) -> Result<(Vector<'static>, i64)> {
    let Some(vector) = values[0].get_value().to_blob() else {
        return Err(LimboError::InternalError(
            "first value must be sparse vector".to_string(),
        ));
    };
    let vector = Vector::from_slice_owned(vector)?;
    if !matches!(vector.vector_type, VectorType::Float32Sparse) {
        return Err(LimboError::InternalError(
            "first value must be sparse vector".to_string(),
        ));
    }
    let Some(rowid) = values[1].get_value().as_int() else {
        return Err(LimboError::InternalError(
            "second value must be i64 rowid".to_string(),
        ));
    };
    Ok((vector, rowid))
}

/// The sparse vector of a query, from the register that holds it.
fn query_vector(values: &[Register]) -> Result<Vector<'static>> {
    let Some(vector) = values[1].get_value().to_blob() else {
        return Err(LimboError::InternalError(
            "first value must be sparse vector".to_string(),
        ));
    };
    let vector = Vector::from_slice_owned(vector)?;
    if !matches!(vector.vector_type, VectorType::Float32Sparse) {
        return Err(LimboError::InternalError(
            "first value must be sparse vector".to_string(),
        ));
    }
    Ok(vector)
}

/// The sparse vector and the limit of a query.
fn query_vector_and_limit(values: &[Register]) -> Result<(Vector<'static>, i64)> {
    let vector = query_vector(values)?;
    let Some(limit) = values[2].get_value().as_int() else {
        return Err(LimboError::InternalError(
            "second value must be i64 limit parameter".to_string(),
        ));
    };
    Ok((vector, limit))
}

fn inverted_key(position: u32, sum: f64, rowid: i64) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[
            Value::from_i64(position as i64),
            Value::from_f64(sum),
            Value::from_i64(rowid),
        ],
        3,
    )
}

fn stats_key(position: u32) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(&[Value::from_i64(position as i64)], 1)
}

fn stats_row(position: u32, cnt: i64, min: f64, max: f64) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[
            Value::from_i64(position as i64),
            Value::from_i64(cnt),
            Value::from_f64(min),
            Value::from_f64(max),
        ],
        4,
    )
}

fn seek_eq(cursor: &mut dyn CursorTrait, key: &ImmutableRecord) -> IOResultOr<SeekResult> {
    cursor.seek(
        SeekKey::IndexKey(key.as_record_ref()),
        SeekOp::GE { eq_only: true },
    )
}

fn insert_key(cursor: &mut dyn CursorTrait, key: &ImmutableRecord) -> IOResultOr<()> {
    cursor.insert(&BTreeKey::IndexKey(key.as_record_ref()))
}

fn read_stat_row(cursor: &mut dyn CursorTrait) -> IOResultOr<ComponentStat> {
    let record = match cursor.record()? {
        IOResult::Done(record) => record,
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    };
    Ok(IOResult::Done(parse_stat_row(record)?))
}

fn read_inverted_row(cursor: &mut dyn CursorTrait) -> IOResultOr<ComponentRow> {
    let record = match cursor.record()? {
        IOResult::Done(record) => record,
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    };
    Ok(IOResult::Done(parse_inverted_index_row(record)?))
}

/// Adds every component of the vector to the inverted index and updates
/// the stats row of the component.
async fn run_insert(co: &mut Co<VectorStep>, _: ()) -> Result<(), Box<LimboError>> {
    let (vector, rowid) = co.with(|ctx| vector_and_rowid(ctx.values))?;
    let sparse = vector.as_f32_sparse();
    let sum = sparse.values.iter().sum::<f32>() as f64;
    for (idx, &position) in sparse.idx.iter().enumerate() {
        let key = inverted_key(position, sum, rowid)?;
        tracing::debug!(
            "insert_state: seek: component={}, sum={}, rowid={}",
            position,
            sum,
            rowid,
        );
        let result = co
            .io(|ctx| seek_eq(ctx.cursor.inverted_index()?, &key))
            .await;
        tracing::debug!("insert_state: seek: result={:?}", result);
        co.io(|ctx| insert_key(ctx.cursor.inverted_index()?, &key))
            .await;

        let value = sparse.values[idx] as f64;
        let key = stats_key(position)?;
        let result = co.io(|ctx| seek_eq(ctx.cursor.stats()?, &key)).await;
        let (cnt, min, max) = match result {
            SeekResult::Found => {
                let component = co.io(|ctx| read_stat_row(ctx.cursor.stats()?)).await;
                (
                    component.cnt + 1,
                    value.min(component.min),
                    value.max(component.max),
                )
            }
            SeekResult::NotFound | SeekResult::TryAdvance => (1, value, value),
        };
        tracing::debug!(
            "update stats(insert): {} (cnt={}, min={}, max={})",
            position,
            cnt,
            min,
            max,
        );
        let row = stats_row(position, cnt, min, max)?;
        co.io(|ctx| insert_key(ctx.cursor.stats()?, &row)).await;
    }
    Ok(())
}

/// Removes every component of the vector from the inverted index and
/// updates the stats row of the component.
async fn run_delete(co: &mut Co<VectorStep>, _: ()) -> Result<(), Box<LimboError>> {
    let (vector, rowid) = co.with(|ctx| vector_and_rowid(ctx.values))?;
    let sparse = vector.as_f32_sparse();
    let sum = sparse.values.iter().sum::<f32>() as f64;
    for &position in sparse.idx {
        let key = inverted_key(position, sum, rowid)?;
        tracing::debug!(
            "delete_state: seek: component={}, sum={}, rowid={}",
            position,
            sum,
            rowid,
        );
        let result = co
            .io(|ctx| seek_eq(ctx.cursor.inverted_index()?, &key))
            .await;
        match result {
            SeekResult::Found => {}
            SeekResult::TryAdvance => {
                co.io(|ctx| ctx.cursor.inverted_index()?.next()).await;
                if !co.with(|ctx| ctx.cursor.inverted_index().is_ok_and(|c| c.has_record())) {
                    return Err(LimboError::Corrupt("inverted index corrupted".to_string()).into());
                }
            }
            SeekResult::NotFound => {
                return Err(LimboError::Corrupt("inverted index corrupted".to_string()).into());
            }
        }
        co.io(|ctx| ctx.cursor.inverted_index()?.delete()).await;

        let key = stats_key(position)?;
        let result = co.io(|ctx| seek_eq(ctx.cursor.stats()?, &key)).await;
        if !matches!(result, SeekResult::Found) {
            return Err(LimboError::Corrupt(
                "stats index corrupted: can't find component row".to_string(),
            )
            .into());
        }
        let component = co.io(|ctx| read_stat_row(ctx.cursor.stats()?)).await;
        tracing::debug!(
            "update stats(delete): {} (cnt={}, min={}, max={})",
            position,
            component.cnt - 1,
            component.min,
            component.max,
        );
        let row = stats_row(position, component.cnt - 1, component.min, component.max)?;
        co.io(|ctx| insert_key(ctx.cursor.stats()?, &row)).await;
    }
    Ok(())
}

/// Finds the `limit` rows nearest to the query vector by jaccard distance
/// and stores them in the search result. Returns true when it found rows.
async fn run_search(co: &mut Co<VectorStep>, _: ()) -> Result<bool, Box<LimboError>> {
    let (vector, limit) = co.with(|ctx| query_vector_and_limit(ctx.values))?;
    let sparse = vector.as_f32_sparse();
    let sum = sparse.values.iter().sum::<f32>() as f64;

    let mut components = Vec::new();
    for (idx, &position) in sparse.idx.iter().enumerate() {
        let key = stats_key(position)?;
        let result = co.io(|ctx| seek_eq(ctx.cursor.stats()?, &key)).await;
        if matches!(result, SeekResult::Found) {
            let component = co.io(|ctx| read_stat_row(ctx.cursor.stats()?)).await;
            components.push((component, sparse.values[idx]));
        }
    }
    let components = co.with(|ctx| {
        let components = ctx.cursor.order_components(components);
        tracing::debug!(
            "query_start: components: {:?}, delta: {}, scan_portion: {}, scan_order: {:?}",
            components,
            ctx.cursor.delta,
            ctx.cursor.scan_portion,
            ctx.cursor.scan_order,
        );
        components
    });
    let mut components: VecDeque<ComponentStat> = components.into();
    let mut collected: HashSet<i64> = HashSet::default();
    let mut distances: BTreeSet<(FloatOrd, i64)> = BTreeSet::new();

    while !components.is_empty() {
        let sum_threshold = co.with(|ctx| {
            ctx.cursor
                .sum_threshold(&components, &distances, limit, sum)
        });
        let component = components
            .pop_front()
            .expect("components queue is not empty");
        let key = stats_key(component.position)?;
        let result = co
            .io(|ctx| {
                ctx.cursor.inverted_index()?.seek(
                    SeekKey::IndexKey(key.as_record_ref()),
                    SeekOp::GE { eq_only: false },
                )
            })
            .await;
        let mut on_row = matches!(result, SeekResult::Found);
        if !on_row {
            co.io(|ctx| ctx.cursor.inverted_index()?.next()).await;
            on_row = co.with(|ctx| ctx.cursor.inverted_index().is_ok_and(|c| c.has_record()));
        }
        let mut current = Vec::new();
        while on_row {
            let row = co
                .io(|ctx| read_inverted_row(ctx.cursor.inverted_index()?))
                .await;
            if row.position != component.position
                || sum_threshold.is_some_and(|threshold| row.sum > threshold)
            {
                break;
            }
            if collected.insert(row.rowid) {
                current.push(row.rowid);
            }
            co.io(|ctx| ctx.cursor.inverted_index()?.next()).await;
            on_row = co.with(|ctx| ctx.cursor.inverted_index().is_ok_and(|c| c.has_record()));
        }
        current.sort_unstable();

        for rowid in current {
            let result = co
                .io(|ctx| {
                    ctx.cursor
                        .main_table()?
                        .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
                })
                .await;
            if !matches!(result, SeekResult::Found) {
                return Err(LimboError::Corrupt(
                    "vector_sparse_ivf corrupted: unable to find rowid in main table".to_string(),
                )
                .into());
            }
            let distance = co.io(read_distance).await;
            if let Some(distance) = distance {
                distances.insert((FloatOrd(distance), rowid));
                if distances.len() > limit as usize {
                    let _ = distances.pop_last();
                }
            }
        }
    }

    let search_result = distances.iter().map(|(d, i)| (*i, d.0)).collect();
    Ok(co.with(|ctx| {
        ctx.cursor.search_result = search_result;
        !ctx.cursor.search_result.is_empty()
    }))
}

/// The jaccard distance between the row the main table cursor is on and
/// the query vector. `None` when the cursor has no row.
fn read_distance(ctx: &mut VectorCtx<'_>) -> IOResultOr<Option<f64>> {
    let column_idx = ctx.cursor.configuration.columns[0].pos_in_table;
    let values = ctx.values;
    let record = match ctx.cursor.main_table()?.record()? {
        IOResult::Done(record) => record,
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    };
    let Some(record) = record else {
        return Ok(IOResult::Done(None));
    };
    let ValueRef::Blob(data) = record.get_value(column_idx)? else {
        return Err(LimboError::InternalError(
            "table column value must be sparse vector".to_string(),
        )
        .into());
    };
    let data = Vector::from_slice_owned(data)?;
    if !matches!(data.vector_type, VectorType::Float32Sparse) {
        return Err(LimboError::InternalError(
            "table column value must be sparse vector".to_string(),
        )
        .into());
    }
    let arg = query_vector(values)?;
    tracing::debug!(
        "vector: {:?}, query: {:?}",
        data.as_f32_sparse(),
        arg.as_f32_sparse()
    );
    let distance = operations::jaccard::vector_distance_jaccard(&data, &arg)?;
    Ok(IOResult::Done(Some(distance)))
}

fn open_store_cursor(
    context: &IndexMethodContext,
    index: &BackingIndex,
) -> Result<Box<dyn CursorTrait>> {
    context
        .backing_store(index)?
        .ok_or_else(|| {
            LimboError::InternalError(format!("backing store {} not found", index.name))
        })?
        .open_cursor()
}

fn key_info() -> KeyInfo {
    KeyInfo {
        collation: CollationSeq::Binary,
        sort_order: SortOrder::Asc,
        nulls_order: None,
    }
}

impl IndexMethodCursor for VectorSparseInvertedIndexMethodCursor {
    fn create(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending_store_ops.is_empty() {
            self.pending_store_ops
                .push_back(context.create_backing_schema(&self.schema)?);
        }
        self.drive_pending_store_ops()
    }

    fn destroy(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending_store_ops.is_empty() {
            self.pending_store_ops
                .push_back(context.drop_backing_schema(&self.schema)?);
        }
        self.drive_pending_store_ops()
    }

    fn open_read(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.open_store_cursors(context)?;
        self.main_btree = Some(context.open_table_cursor(&self.configuration.table_name)?);
        Ok(IOResult::Done(()))
    }

    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.open_store_cursors(context)?;
        Ok(IOResult::Done(()))
    }

    fn stage_statement_commit(&mut self, _context: &IndexMethodContext) -> IOResultOr<()> {
        Ok(IOResult::Done(()))
    }

    fn abort_statement(&mut self, _context: &IndexMethodContext) {}

    fn on_transaction_committed(&mut self, _context: &IndexMethodContext) {}

    fn on_transaction_rolled_back(&mut self, _context: &IndexMethodContext) {}

    fn on_savepoint_rolled_back(&mut self, _context: &IndexMethodContext) {}

    fn close(&mut self, _context: &IndexMethodContext) {
        self.inverted_index_cursor = None;
        self.stats_cursor = None;
        self.main_btree = None;
    }

    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.inverted_index()?;
        self.stats()?;
        let mut op = self
            .ops
            .insert
            .take()
            .unwrap_or_else(|| Runner::boxed(|co, args| with_handle(co, args, run_insert)));
        let mut ctx = VectorCtx {
            cursor: self,
            values,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, ());
        self.ops.insert = Some(op);
        result
    }

    fn delete(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.inverted_index()?;
        self.stats()?;
        let mut op = self
            .ops
            .delete
            .take()
            .unwrap_or_else(|| Runner::boxed(|co, args| with_handle(co, args, run_delete)));
        let mut ctx = VectorCtx {
            cursor: self,
            values,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, ());
        self.ops.delete = Some(op);
        result
    }

    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool> {
        self.inverted_index()?;
        self.stats()?;
        self.main_table()?;
        let mut op = self
            .ops
            .search
            .take()
            .unwrap_or_else(|| Runner::boxed(|co, args| with_handle(co, args, run_search)));
        let mut ctx = VectorCtx {
            cursor: self,
            values,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, ());
        self.ops.search = Some(op);
        result
    }

    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        let Some(result) = self.search_result.front() else {
            return Err(LimboError::InternalError(
                "search_result must not be empty when query_rowid is called".to_string(),
            )
            .into());
        };
        Ok(IOResult::Done(Some(result.0)))
    }

    fn query_column(&mut self, _: usize) -> IOResultOr<Value> {
        let Some(result) = self.search_result.front() else {
            return Err(LimboError::InternalError(
                "search_result must not be empty when query_column is called".to_string(),
            )
            .into());
        };
        Ok(IOResult::Done(Value::from_f64(result.1)))
    }

    fn query_next(&mut self) -> IOResultOr<bool> {
        let _ = self.search_result.pop_front();
        Ok(IOResult::Done(!self.search_result.is_empty()))
    }
}
