use std::{
    collections::{BTreeSet, HashSet, VecDeque},
    sync::Arc,
};

use turso_parser::ast::{self, SortOrder};

use crate::{
    index_method::{
        open_index_cursor, open_table_cursor, parse_patterns, IndexMethod, IndexMethodAttachment,
        IndexMethodConfiguration, IndexMethodCursor, IndexMethodDefinition,
        BACKING_BTREE_INDEX_METHOD_NAME, TOY_VECTOR_SPARSE_IVF_INDEX_METHOD_NAME,
    },
    return_if_io,
    storage::btree::{BTreeCursor, BTreeKey, CursorTrait},
    translate::collate::CollationSeq,
    types::{IOResult, ImmutableRecord, KeyInfo, SeekKey, SeekOp, SeekResult},
    vdbe::Register,
    vector::{
        operations,
        vector_types::{Vector, VectorType},
    },
    Connection, LimboError, Result, Value, ValueRef,
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

#[derive(Debug)]
pub enum VectorSparseInvertedIndexInsertState {
    Init,
    Prepare {
        vector: Option<Vector<'static>>,
        sum: f64,
        rowid: i64,
        idx: usize,
    },
    SeekInverted {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    InsertInverted {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    SeekStats {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    ReadStats {
        vector: Option<Vector<'static>>,
        sum: f64,
        rowid: i64,
        idx: usize,
    },
    UpdateStats {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
}

#[derive(Debug)]
pub enum VectorSparseInvertedIndexDeleteState {
    Init,
    Prepare {
        vector: Option<Vector<'static>>,
        sum: f64,
        rowid: i64,
        idx: usize,
    },
    SeekInverted {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    NextInverted {
        vector: Option<Vector<'static>>,
        sum: f64,
        rowid: i64,
        idx: usize,
    },
    DeleteInverted {
        vector: Option<Vector<'static>>,
        sum: f64,
        rowid: i64,
        idx: usize,
    },
    SeekStats {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    ReadStats {
        vector: Option<Vector<'static>>,
        sum: f64,
        rowid: i64,
        idx: usize,
    },
    UpdateStats {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
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
    let ValueRef::Integer(position) = record.get_value(0)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected integer".to_string(),
        ));
    };
    let ValueRef::Integer(cnt) = record.get_value(1)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected integer".to_string(),
        ));
    };
    let ValueRef::Float(min) = record.get_value(2)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected float".to_string(),
        ));
    };
    let ValueRef::Float(max) = record.get_value(3)? else {
        return Err(LimboError::Corrupt(
            "stats index corrupted: expected float".to_string(),
        ));
    };
    Ok(ComponentStat {
        position: position as u32,
        cnt,
        min,
        max,
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
    let ValueRef::Integer(position) = record.get_value(0)? else {
        return Err(LimboError::Corrupt(
            "inverted index corrupted: expected integer".to_string(),
        ));
    };
    let ValueRef::Float(sum) = record.get_value(1)? else {
        return Err(LimboError::Corrupt(
            "inverted index corrupted: expected float".to_string(),
        ));
    };
    let ValueRef::Integer(rowid) = record.get_value(2)? else {
        return Err(LimboError::Corrupt(
            "inverted index corrupted: expected integer".to_string(),
        ));
    };
    Ok(ComponentRow {
        position: position as u32,
        sum,
        rowid,
    })
}

#[derive(Debug)]
enum VectorSparseInvertedIndexSearchState {
    Init,
    CollectComponentsSeek {
        sum: f64,
        vector: Option<Vector<'static>>,
        idx: usize,
        components: Option<Vec<(ComponentStat, f32)>>,
        limit: i64,
        key: Option<ImmutableRecord>,
    },
    CollectComponentsRead {
        sum: f64,
        vector: Option<Vector<'static>>,
        idx: usize,
        components: Option<Vec<(ComponentStat, f32)>>,
        limit: i64,
    },
    Seek {
        sum: f64,
        components: Option<VecDeque<ComponentStat>>,
        collected: Option<HashSet<i64>>,
        distances: Option<BTreeSet<(FloatOrd, i64)>>,
        limit: i64,
        key: Option<ImmutableRecord>,
        sum_threshold: Option<f64>,
        component: Option<u32>,
    },
    Read {
        sum: f64,
        components: Option<VecDeque<ComponentStat>>,
        collected: Option<HashSet<i64>>,
        distances: Option<BTreeSet<(FloatOrd, i64)>>,
        limit: i64,
        sum_threshold: Option<f64>,
        component: u32,
        current: Option<Vec<i64>>,
    },
    Next {
        sum: f64,
        components: Option<VecDeque<ComponentStat>>,
        collected: Option<HashSet<i64>>,
        distances: Option<BTreeSet<(FloatOrd, i64)>>,
        limit: i64,
        sum_threshold: Option<f64>,
        component: u32,
        current: Option<Vec<i64>>,
    },
    EvaluateSeek {
        sum: f64,
        components: Option<VecDeque<ComponentStat>>,
        collected: Option<HashSet<i64>>,
        distances: Option<BTreeSet<(FloatOrd, i64)>>,
        limit: i64,
        current: Option<VecDeque<i64>>,
        rowid: Option<i64>,
    },
    EvaluateRead {
        sum: f64,
        components: Option<VecDeque<ComponentStat>>,
        collected: Option<HashSet<i64>>,
        distances: Option<BTreeSet<(FloatOrd, i64)>>,
        limit: i64,
        current: Option<VecDeque<i64>>,
        rowid: i64,
    },
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
    inverted_index_btree: String,
    inverted_index_cursor: Option<BTreeCursor>,
    stats_btree: String,
    stats_cursor: Option<BTreeCursor>,
    main_btree: Option<BTreeCursor>,
    insert_state: VectorSparseInvertedIndexInsertState,
    delete_state: VectorSparseInvertedIndexDeleteState,
    search_state: VectorSparseInvertedIndexSearchState,
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
            index_name: &self.configuration.index_name,
            patterns: self.patterns.as_slice(),
            backing_btree: false,
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
        let inverted_index_btree = format!("{}_inverted_index", configuration.index_name);
        let stats_btree = format!("{}_stats", configuration.index_name);
        let delta = match configuration.parameters.get("delta") {
            Some(&Value::Float(delta)) => delta,
            _ => 0.0,
        };
        let scan_portion = match configuration.parameters.get("scan_portion") {
            Some(&Value::Float(scan_portion)) => scan_portion,
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
            inverted_index_btree,
            inverted_index_cursor: None,
            stats_btree,
            stats_cursor: None,
            main_btree: None,
            search_result: VecDeque::new(),
            insert_state: VectorSparseInvertedIndexInsertState::Init,
            delete_state: VectorSparseInvertedIndexDeleteState::Init,
            search_state: VectorSparseInvertedIndexSearchState::Init,
        }
    }
}

fn key_info() -> KeyInfo {
    KeyInfo {
        collation: CollationSeq::Binary,
        sort_order: SortOrder::Asc,
    }
}

impl IndexMethodCursor for VectorSparseInvertedIndexMethodCursor {
    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        // we need to properly track subprograms and propagate result to the root program to make this execution async

        let columns = &self.configuration.columns;
        let columns = columns.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
        let inverted_index_create = format!(
            "CREATE INDEX {} ON {} USING {} ({})",
            self.inverted_index_btree,
            self.configuration.table_name,
            BACKING_BTREE_INDEX_METHOD_NAME,
            columns.join(", ")
        );
        let stats_index_create = format!(
            "CREATE INDEX {} ON {} USING {} ({})",
            self.stats_btree,
            self.configuration.table_name,
            BACKING_BTREE_INDEX_METHOD_NAME,
            columns.join(", ")
        );
        for sql in [inverted_index_create, stats_index_create] {
            let mut stmt = connection.prepare(&sql)?;
            // by default we set needs_stmt_subtransactions = true to all write transaction
            // this will lead to Busy error here - because Transaction opcode will be unable to acquire ownership to the subjournal as it already owned by parent statement which is still active
            //
            // as we run nested statement - we actually don't need subjournal as it already started before in the parent statement
            // so, this is hacky way to fix the situation for toy index for now, but we need to implement proper helpers in order to avoid similar errors in other code later
            stmt.program.needs_stmt_subtransactions = false;
            connection.start_nested();
            let result = stmt.run_ignore_rows();
            connection.end_nested();
            result?;
        }

        Ok(IOResult::Done(()))
    }

    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        let inverted_index_drop = format!("DROP INDEX {}", self.inverted_index_btree);
        let stats_index_drop = format!("DROP INDEX {}", self.stats_btree);
        for sql in [inverted_index_drop, stats_index_drop] {
            let mut stmt = connection.prepare(&sql)?;
            connection.start_nested();
            let result = stmt.run_ignore_rows();
            connection.end_nested();
            result?;
        }

        Ok(IOResult::Done(()))
    }

    fn open_read(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        self.inverted_index_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.inverted_index_btree,
            // component, length, rowid
            vec![key_info(), key_info(), key_info()],
        )?);
        self.stats_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.stats_btree,
            // component
            vec![key_info()],
        )?);
        self.main_btree = Some(open_table_cursor(
            connection,
            &self.configuration.table_name,
        )?);
        Ok(IOResult::Done(()))
    }

    fn open_write(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        self.inverted_index_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.inverted_index_btree,
            // component, length, rowid
            vec![key_info(), key_info(), key_info()],
        )?);
        self.stats_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.stats_btree,
            // component
            vec![key_info()],
        )?);
        Ok(IOResult::Done(()))
    }

    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        let Some(inverted_cursor) = &mut self.inverted_index_cursor else {
            return Err(LimboError::InternalError(
                "inverted cursor must be opened".to_string(),
            ));
        };
        let Some(stats_cursor) = &mut self.stats_cursor else {
            return Err(LimboError::InternalError(
                "stats cursor must be opened".to_string(),
            ));
        };
        loop {
            tracing::debug!("insert_state: {:?}", self.insert_state);
            match &mut self.insert_state {
                VectorSparseInvertedIndexInsertState::Init => {
                    let Some(vector) = values[0].get_value().to_blob() else {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    };
                    let vector = Vector::from_vec(vector.to_vec())?;
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
                    let sum = vector.as_f32_sparse().values.iter().sum::<f32>() as f64;
                    self.insert_state = VectorSparseInvertedIndexInsertState::Prepare {
                        vector: Some(vector),
                        sum,
                        rowid,
                        idx: 0,
                    }
                }
                VectorSparseInvertedIndexInsertState::Prepare {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in Prepare state".to_string(),
                        ));
                    };
                    if *idx == v.as_f32_sparse().idx.len() {
                        self.insert_state = VectorSparseInvertedIndexInsertState::Init;
                        return Ok(IOResult::Done(()));
                    }
                    let position = v.as_f32_sparse().idx[*idx];
                    let key = ImmutableRecord::from_values(
                        &[
                            Value::Integer(position as i64),
                            Value::Float(*sum),
                            Value::Integer(*rowid),
                        ],
                        3,
                    );
                    tracing::debug!(
                        "insert_state: seek: component={}, sum={}, rowid={}",
                        position,
                        *sum,
                        *rowid,
                    );
                    self.insert_state = VectorSparseInvertedIndexInsertState::SeekInverted {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexInsertState::SeekInverted {
                    vector,
                    sum,
                    rowid,
                    idx,
                    key,
                } => {
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in SeekInverted state".to_string(),
                        ));
                    };
                    let result =
                        return_if_io!(inverted_cursor
                            .seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: true }));
                    tracing::debug!("insert_state: seek: result={:?}", result);
                    self.insert_state = VectorSparseInvertedIndexInsertState::InsertInverted {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: key.take(),
                    };
                }
                VectorSparseInvertedIndexInsertState::InsertInverted {
                    vector,
                    sum,
                    rowid,
                    idx,
                    key,
                } => {
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in InsertInverted state".to_string(),
                        ));
                    };
                    return_if_io!(inverted_cursor.insert(&BTreeKey::IndexKey(k)));

                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in InsertInverted state".to_string(),
                        ));
                    };
                    let position = v.as_f32_sparse().idx[*idx];
                    let key = ImmutableRecord::from_values(&[Value::Integer(position as i64)], 1);
                    self.insert_state = VectorSparseInvertedIndexInsertState::SeekStats {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexInsertState::SeekStats {
                    vector,
                    sum,
                    key,
                    rowid,
                    idx,
                } => {
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in SeekStats state".to_string(),
                        ));
                    };
                    let result = return_if_io!(
                        stats_cursor.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: true })
                    );
                    match result {
                        SeekResult::Found => {
                            self.insert_state = VectorSparseInvertedIndexInsertState::ReadStats {
                                vector: vector.take(),
                                sum: *sum,
                                idx: *idx,
                                rowid: *rowid,
                            };
                        }
                        SeekResult::NotFound | SeekResult::TryAdvance => {
                            let Some(v) = vector.as_ref() else {
                                return Err(LimboError::InternalError(
                                    "vector must be present in SeekStats state".to_string(),
                                ));
                            };
                            let position = v.as_f32_sparse().idx[*idx];
                            let value = v.as_f32_sparse().values[*idx] as f64;
                            tracing::debug!(
                                "update stats(insert): {} (cnt={}, min={}, max={})",
                                position,
                                1,
                                value,
                                value,
                            );
                            let key = ImmutableRecord::from_values(
                                &[
                                    Value::Integer(position as i64),
                                    Value::Integer(1),
                                    Value::Float(value),
                                    Value::Float(value),
                                ],
                                4,
                            );
                            self.insert_state = VectorSparseInvertedIndexInsertState::UpdateStats {
                                vector: vector.take(),
                                sum: *sum,
                                idx: *idx,
                                rowid: *rowid,
                                key: Some(key),
                            };
                        }
                    }
                }
                VectorSparseInvertedIndexInsertState::ReadStats {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    let record = return_if_io!(stats_cursor.record());
                    let component = parse_stat_row(record.as_deref())?;
                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in ReadStats state".to_string(),
                        ));
                    };
                    let position = v.as_f32_sparse().idx[*idx];
                    let value = v.as_f32_sparse().values[*idx] as f64;
                    tracing::debug!(
                        "update stats(insert): {} (cnt={}, min={}, max={})",
                        position,
                        component.cnt + 1,
                        value.min(component.min),
                        value.max(component.max),
                    );
                    let key = ImmutableRecord::from_values(
                        &[
                            Value::Integer(position as i64),
                            Value::Integer(component.cnt + 1),
                            Value::Float(value.min(component.min)),
                            Value::Float(value.max(component.max)),
                        ],
                        4,
                    );
                    self.insert_state = VectorSparseInvertedIndexInsertState::UpdateStats {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexInsertState::UpdateStats {
                    vector,
                    sum,
                    key,
                    rowid,
                    idx,
                } => {
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in UpdateStats state".to_string(),
                        ));
                    };
                    return_if_io!(stats_cursor.insert(&BTreeKey::IndexKey(k)));

                    self.insert_state = VectorSparseInvertedIndexInsertState::Prepare {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx + 1,
                        rowid: *rowid,
                    };
                }
            }
        }
    }

    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        let Some(cursor) = &mut self.inverted_index_cursor else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        let Some(stats_cursor) = &mut self.stats_cursor else {
            return Err(LimboError::InternalError(
                "stats cursor must be opened".to_string(),
            ));
        };
        loop {
            tracing::debug!("delete_state: {:?}", self.delete_state);
            match &mut self.delete_state {
                VectorSparseInvertedIndexDeleteState::Init => {
                    let Some(vector) = values[0].get_value().to_blob() else {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    };
                    let vector = Vector::from_vec(vector.to_vec())?;
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
                    let sum = vector.as_f32_sparse().values.iter().sum::<f32>() as f64;
                    self.delete_state = VectorSparseInvertedIndexDeleteState::Prepare {
                        vector: Some(vector),
                        sum,
                        rowid,
                        idx: 0,
                    }
                }
                VectorSparseInvertedIndexDeleteState::Prepare {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in Prepare state".to_string(),
                        ));
                    };
                    if *idx == v.as_f32_sparse().idx.len() {
                        self.delete_state = VectorSparseInvertedIndexDeleteState::Init;
                        return Ok(IOResult::Done(()));
                    }
                    let position = v.as_f32_sparse().idx[*idx];
                    let key = ImmutableRecord::from_values(
                        &[
                            Value::Integer(position as i64),
                            Value::Float(*sum),
                            Value::Integer(*rowid),
                        ],
                        3,
                    );
                    self.delete_state = VectorSparseInvertedIndexDeleteState::SeekInverted {
                        vector: vector.take(),
                        idx: *idx,
                        sum: *sum,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexDeleteState::SeekInverted {
                    vector,
                    sum,
                    rowid,
                    idx,
                    key,
                } => {
                    let component_idx = vector
                        .as_ref()
                        .and_then(|v| v.as_f32_sparse().idx.get(*idx).copied())
                        .ok_or_else(|| {
                            LimboError::InternalError(
                                "vector must be present in SeekInverted state".to_string(),
                            )
                        })?;
                    tracing::debug!(
                        "delete_state: seek: component={}, sum={}, rowid={}",
                        component_idx,
                        *sum,
                        *rowid,
                    );
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in SeekInverted state".to_string(),
                        ));
                    };
                    let result = return_if_io!(
                        cursor.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: true })
                    );
                    match result {
                        SeekResult::Found => {
                            self.delete_state =
                                VectorSparseInvertedIndexDeleteState::DeleteInverted {
                                    vector: vector.take(),
                                    sum: *sum,
                                    idx: *idx,
                                    rowid: *rowid,
                                };
                        }
                        SeekResult::TryAdvance => {
                            self.delete_state =
                                VectorSparseInvertedIndexDeleteState::NextInverted {
                                    vector: vector.take(),
                                    sum: *sum,
                                    idx: *idx,
                                    rowid: *rowid,
                                };
                        }
                        SeekResult::NotFound => {
                            return Err(LimboError::Corrupt("inverted index corrupted".to_string()))
                        }
                    }
                }
                VectorSparseInvertedIndexDeleteState::NextInverted {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    if !return_if_io!(cursor.next()) {
                        return Err(LimboError::Corrupt("inverted index corrupted".to_string()));
                    }
                    self.delete_state = VectorSparseInvertedIndexDeleteState::DeleteInverted {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                    };
                }
                VectorSparseInvertedIndexDeleteState::DeleteInverted {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    return_if_io!(cursor.delete());
                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in DeleteInverted state".to_string(),
                        ));
                    };
                    let position = v.as_f32_sparse().idx[*idx];
                    let key = ImmutableRecord::from_values(&[Value::Integer(position as i64)], 1);
                    self.delete_state = VectorSparseInvertedIndexDeleteState::SeekStats {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexDeleteState::SeekStats {
                    vector,
                    sum,
                    key,
                    rowid,
                    idx,
                } => {
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in SeekStats state".to_string(),
                        ));
                    };
                    let result = return_if_io!(
                        stats_cursor.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: true })
                    );
                    match result {
                        SeekResult::Found => {
                            self.delete_state = VectorSparseInvertedIndexDeleteState::ReadStats {
                                vector: vector.take(),
                                sum: *sum,
                                idx: *idx,
                                rowid: *rowid,
                            };
                        }
                        SeekResult::NotFound | SeekResult::TryAdvance => {
                            return Err(LimboError::Corrupt(
                                "stats index corrupted: can't find component row".to_string(),
                            ))
                        }
                    }
                }
                VectorSparseInvertedIndexDeleteState::ReadStats {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    let record = return_if_io!(stats_cursor.record());
                    let component = parse_stat_row(record.as_deref())?;
                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in ReadStats state".to_string(),
                        ));
                    };
                    let position = v.as_f32_sparse().idx[*idx];
                    tracing::debug!(
                        "update stats(delete): {} (cnt={}, min={}, max={})",
                        position,
                        component.cnt - 1,
                        component.min,
                        component.max,
                    );
                    let key = ImmutableRecord::from_values(
                        &[
                            Value::Integer(position as i64),
                            Value::Integer(component.cnt - 1),
                            Value::Float(component.min),
                            Value::Float(component.max),
                        ],
                        4,
                    );
                    self.delete_state = VectorSparseInvertedIndexDeleteState::UpdateStats {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexDeleteState::UpdateStats {
                    vector,
                    sum,
                    key,
                    rowid,
                    idx,
                } => {
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in UpdateStats state".to_string(),
                        ));
                    };
                    return_if_io!(stats_cursor.insert(&BTreeKey::IndexKey(k)));

                    self.delete_state = VectorSparseInvertedIndexDeleteState::Prepare {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx + 1,
                        rowid: *rowid,
                    };
                }
            }
        }
    }

    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<bool>> {
        let Some(inverted) = &mut self.inverted_index_cursor else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        let Some(stats) = &mut self.stats_cursor else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        let Some(main) = &mut self.main_btree else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        loop {
            tracing::debug!("query_state: {:?}", self.search_state);
            match &mut self.search_state {
                VectorSparseInvertedIndexSearchState::Init => {
                    let Some(vector) = values[1].get_value().to_blob() else {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    };
                    let Some(limit) = values[2].get_value().as_int() else {
                        return Err(LimboError::InternalError(
                            "second value must be i64 limit parameter".to_string(),
                        ));
                    };
                    let vector = Vector::from_vec(vector.to_vec())?;
                    if !matches!(vector.vector_type, VectorType::Float32Sparse) {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    }
                    let sparse = vector.as_f32_sparse();
                    let sum = sparse.values.iter().sum::<f32>() as f64;
                    self.search_state =
                        VectorSparseInvertedIndexSearchState::CollectComponentsSeek {
                            sum,
                            vector: Some(vector),
                            idx: 0,
                            components: Some(Vec::new()),
                            key: None,
                            limit,
                        };
                }
                VectorSparseInvertedIndexSearchState::CollectComponentsSeek {
                    sum,
                    vector,
                    idx,
                    components,
                    limit,
                    key,
                } => {
                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in CollectComponentsSeek state".to_string(),
                        ));
                    };
                    let p = &v.as_f32_sparse().idx[*idx..];
                    if p.is_empty() && key.is_none() {
                        let Some(mut components) = components.take() else {
                            return Err(LimboError::InternalError(
                                "components must be present in CollectComponentsSeek state"
                                    .to_string(),
                            ));
                        };
                        match self.scan_order {
                            ScanOrder::DatasetFrequencyAsc => {
                                // order by cnt ASC in order to check low-cardinality components first
                                components.sort_by_key(|(c, _)| c.cnt);
                            }
                            ScanOrder::QueryWeightDesc => {
                                // order by weight DESC in order to check high-impact components first
                                components
                                    .sort_by_key(|(_, w)| std::cmp::Reverse(FloatOrd(*w as f64)));
                            }
                        }
                        let take = (components.len() as f64 * self.scan_portion).ceil() as usize;
                        let components = components
                            .into_iter()
                            .take(take)
                            .map(|(c, _)| c)
                            .collect::<Vec<_>>();

                        tracing::debug!(
                            "query_start: components: {:?}, delta: {}, scan_portion: {}, scan_order: {:?}",
                            components,
                            self.delta,
                            self.scan_portion,
                            self.scan_order,
                        );
                        self.search_state = VectorSparseInvertedIndexSearchState::Seek {
                            sum: *sum,
                            components: Some(components.into()),
                            collected: Some(HashSet::new()),
                            distances: Some(BTreeSet::new()),
                            limit: *limit,
                            key: None,
                            component: None,
                            sum_threshold: None,
                        };
                        continue;
                    }
                    if key.is_none() {
                        let Some(v) = vector.as_ref() else {
                            return Err(LimboError::InternalError(
                                "vector must be present in CollectComponentsSeek state".to_string(),
                            ));
                        };
                        let position = v.as_f32_sparse().idx[*idx];
                        *key = Some(ImmutableRecord::from_values(
                            &[Value::Integer(position as i64)],
                            1,
                        ));
                    }
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in CollectComponentsSeek state".to_string(),
                        ));
                    };
                    let result = return_if_io!(
                        stats.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: true })
                    );
                    match result {
                        SeekResult::Found => {
                            self.search_state =
                                VectorSparseInvertedIndexSearchState::CollectComponentsRead {
                                    sum: *sum,
                                    vector: vector.take(),
                                    idx: *idx,
                                    components: components.take(),
                                    limit: *limit,
                                };
                        }
                        SeekResult::NotFound | SeekResult::TryAdvance => {
                            self.search_state =
                                VectorSparseInvertedIndexSearchState::CollectComponentsSeek {
                                    sum: *sum,
                                    components: components.take(),
                                    vector: vector.take(),
                                    idx: *idx + 1,
                                    limit: *limit,
                                    key: None,
                                };
                        }
                    }
                }
                VectorSparseInvertedIndexSearchState::CollectComponentsRead {
                    sum,
                    vector,
                    idx,
                    components,
                    limit,
                } => {
                    let record = return_if_io!(stats.record());
                    let Some(v) = vector.as_ref() else {
                        return Err(LimboError::InternalError(
                            "vector must be present in CollectComponentsRead state".to_string(),
                        ));
                    };
                    let value = v.as_f32_sparse().values[*idx];
                    let component = parse_stat_row(record.as_deref())?;
                    let Some(comps) = components.as_mut() else {
                        return Err(LimboError::InternalError(
                            "components must be present in CollectComponentsRead state".to_string(),
                        ));
                    };
                    comps.push((component, value));
                    self.search_state =
                        VectorSparseInvertedIndexSearchState::CollectComponentsSeek {
                            sum: *sum,
                            components: components.take(),
                            vector: vector.take(),
                            idx: *idx + 1,
                            limit: *limit,
                            key: None,
                        };
                }
                VectorSparseInvertedIndexSearchState::Seek {
                    sum,
                    components,
                    collected,
                    distances,
                    limit,
                    key,
                    component,
                    sum_threshold,
                } => {
                    let Some(c) = components.as_ref() else {
                        return Err(LimboError::InternalError(
                            "components must be present in Seek state".to_string(),
                        ));
                    };
                    if c.is_empty() && key.is_none() {
                        let Some(distances) = distances.take() else {
                            return Err(LimboError::InternalError(
                                "distances must be present in Seek state".to_string(),
                            ));
                        };
                        self.search_result = distances.iter().map(|(d, i)| (*i, d.0)).collect();
                        return Ok(IOResult::Done(!self.search_result.is_empty()));
                    }
                    if key.is_none() {
                        // we estimate jaccard distance with the following approach:
                        // J = min(L, M1 + M2 + ... + Mr) / (Q + N - min(L, M1 + M2 + ... + Mr))
                        // so we want J > best + delta; define M1 + M2 + ... + Mr = M
                        // J = min(L, M) / (Q + L - min(L, M)) > best + delta
                        // we need to consider two cases:
                        // 1. L < M: J = L / (Q + L - L) > best + delta => L > (best + delta) * Q
                        // 2. L > M: J = M / (Q + L - M) > best + delta => M > (best + delta) * (Q + L - M) => L < M / (best + delta) - (Q - M)
                        // so we have two intervals: [(best + delta) * Q .. M] and [M .. M / (best + delta) - (Q - M)]
                        // to simplify code for now we will pick upper bound from second range if it is not degenerate, otherwise check first range
                        let m = c.iter().map(|c| c.max).sum::<f64>().min(*sum);
                        let Some(dists) = distances.as_ref() else {
                            return Err(LimboError::InternalError(
                                "distances must be present in Seek state".to_string(),
                            ));
                        };
                        if dists.len() >= *limit as usize {
                            if let Some((max_threshold, _)) = dists.last() {
                                let best = 1.0 - max_threshold.0;
                                let delta = self.delta;
                                let q = *sum;

                                if best > 0.0 {
                                    let first_range_l = (best + delta) * q;
                                    let second_range_r = m / (best + delta) - (q - m);
                                    if m <= second_range_r {
                                        *sum_threshold = Some(second_range_r);
                                    } else if first_range_l <= m {
                                        *sum_threshold = Some(m);
                                    } else {
                                        *sum_threshold = Some(-1.0);
                                    }
                                    tracing::debug!(
                                        "sum_threshold={:?}, max_threshold={}, remained_sum={}, sum={}, components={:?}",
                                        sum_threshold,
                                        best,
                                        m,
                                        sum,
                                        c
                                    );
                                }
                            }
                        }
                        let Some(comps) = components.as_mut() else {
                            return Err(LimboError::InternalError(
                                "components must be present in Seek state".to_string(),
                            ));
                        };
                        let Some(c) = comps.pop_front() else {
                            return Err(LimboError::InternalError(
                                "components queue must not be empty in Seek state".to_string(),
                            ));
                        };
                        *key = Some(ImmutableRecord::from_values(
                            &[Value::Integer(c.position as i64)],
                            1,
                        ));
                        *component = Some(c.position);
                    }
                    let Some(k) = key.as_ref() else {
                        return Err(LimboError::InternalError(
                            "key must be present in Seek state".to_string(),
                        ));
                    };
                    let result = return_if_io!(
                        inverted.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: false })
                    );
                    match result {
                        SeekResult::Found => {
                            let Some(comp) = component.take() else {
                                return Err(LimboError::InternalError(
                                    "component must be present in Seek state".to_string(),
                                ));
                            };
                            self.search_state = VectorSparseInvertedIndexSearchState::Read {
                                sum: *sum,
                                components: components.take(),
                                collected: collected.take(),
                                distances: distances.take(),
                                current: Some(Vec::new()),
                                limit: *limit,
                                sum_threshold: sum_threshold.take(),
                                component: comp,
                            };
                        }
                        SeekResult::TryAdvance | SeekResult::NotFound => {
                            let Some(comp) = component.take() else {
                                return Err(LimboError::InternalError(
                                    "component must be present in Seek state".to_string(),
                                ));
                            };
                            self.search_state = VectorSparseInvertedIndexSearchState::Next {
                                sum: *sum,
                                components: components.take(),
                                collected: collected.take(),
                                distances: distances.take(),
                                current: Some(Vec::new()),
                                limit: *limit,
                                sum_threshold: sum_threshold.take(),
                                component: comp,
                            };
                        }
                    }
                }
                VectorSparseInvertedIndexSearchState::Read {
                    sum,
                    components,
                    collected,
                    distances,
                    limit,
                    sum_threshold,
                    component,
                    current,
                } => {
                    let record = return_if_io!(inverted.record());
                    let row = parse_inverted_index_row(record.as_deref())?;
                    if row.position != *component
                        || (sum_threshold.is_some()
                            && row.sum
                                > sum_threshold.ok_or_else(|| {
                                    LimboError::InternalError(
                                        "sum_threshold must be present when checked".to_string(),
                                    )
                                })?)
                    {
                        let Some(mut current) = current.take() else {
                            return Err(LimboError::InternalError(
                                "current must be present in Read state".to_string(),
                            ));
                        };
                        current.sort();

                        self.search_state = VectorSparseInvertedIndexSearchState::EvaluateSeek {
                            sum: *sum,
                            components: components.take(),
                            collected: collected.take(),
                            distances: distances.take(),
                            limit: *limit,
                            current: Some(current.into()),
                            rowid: None,
                        };
                        continue;
                    }
                    let Some(coll) = collected.as_mut() else {
                        return Err(LimboError::InternalError(
                            "collected must be present in Read state".to_string(),
                        ));
                    };
                    if coll.insert(row.rowid) {
                        let Some(curr) = current.as_mut() else {
                            return Err(LimboError::InternalError(
                                "current must be present in Read state".to_string(),
                            ));
                        };
                        curr.push(row.rowid);
                    }

                    self.search_state = VectorSparseInvertedIndexSearchState::Next {
                        sum: *sum,
                        components: components.take(),
                        collected: collected.take(),
                        distances: distances.take(),
                        limit: *limit,
                        sum_threshold: *sum_threshold,
                        component: *component,
                        current: current.take(),
                    };
                }
                VectorSparseInvertedIndexSearchState::Next {
                    sum,
                    components,
                    collected,
                    distances,
                    limit,
                    sum_threshold,
                    component,
                    current,
                } => {
                    let result = return_if_io!(inverted.next());
                    if !result {
                        let Some(mut current) = current.take() else {
                            return Err(LimboError::InternalError(
                                "current must be present in Next state".to_string(),
                            ));
                        };
                        current.sort();

                        self.search_state = VectorSparseInvertedIndexSearchState::EvaluateSeek {
                            sum: *sum,
                            components: components.take(),
                            collected: collected.take(),
                            distances: distances.take(),
                            limit: *limit,
                            current: Some(current.into()),
                            rowid: None,
                        };
                    } else {
                        self.search_state = VectorSparseInvertedIndexSearchState::Read {
                            sum: *sum,
                            components: components.take(),
                            collected: collected.take(),
                            distances: distances.take(),
                            limit: *limit,
                            sum_threshold: *sum_threshold,
                            component: *component,
                            current: current.take(),
                        };
                    }
                }
                VectorSparseInvertedIndexSearchState::EvaluateSeek {
                    sum,
                    components,
                    collected,
                    distances,
                    limit,
                    current,
                    rowid,
                } => {
                    let Some(c) = current.as_ref() else {
                        return Err(LimboError::InternalError(
                            "current must be present in EvaluateSeek state".to_string(),
                        ));
                    };
                    if c.is_empty() && rowid.is_none() {
                        self.search_state = VectorSparseInvertedIndexSearchState::Seek {
                            sum: *sum,
                            components: components.take(),
                            collected: collected.take(),
                            distances: distances.take(),
                            limit: *limit,
                            component: None,
                            key: None,
                            sum_threshold: None,
                        };
                        continue;
                    }
                    if rowid.is_none() {
                        let Some(curr) = current.as_mut() else {
                            return Err(LimboError::InternalError(
                                "current must be present in EvaluateSeek state".to_string(),
                            ));
                        };
                        *rowid = Some(curr.pop_front().ok_or_else(|| {
                            LimboError::InternalError(
                                "current queue must not be empty in EvaluateSeek state".to_string(),
                            )
                        })?);
                    }

                    let Some(rid) = rowid.as_ref() else {
                        return Err(LimboError::InternalError(
                            "rowid must be present in EvaluateSeek state".to_string(),
                        ));
                    };
                    let rowid = *rid;
                    let k = SeekKey::TableRowId(rowid);
                    let result = return_if_io!(main.seek(k, SeekOp::GE { eq_only: true }));
                    if !matches!(result, SeekResult::Found) {
                        return Err(LimboError::Corrupt(
                            "vector_sparse_ivf corrupted: unable to find rowid in main table"
                                .to_string(),
                        ));
                    };
                    self.search_state = VectorSparseInvertedIndexSearchState::EvaluateRead {
                        sum: *sum,
                        components: components.take(),
                        collected: collected.take(),
                        distances: distances.take(),
                        limit: *limit,
                        current: current.take(),
                        rowid,
                    };
                }
                VectorSparseInvertedIndexSearchState::EvaluateRead {
                    sum,
                    components,
                    collected,
                    distances,
                    limit,
                    current,
                    rowid,
                } => {
                    let record = return_if_io!(main.record());
                    if let Some(record) = record {
                        let column_idx = self.configuration.columns[0].pos_in_table;
                        let ValueRef::Blob(data) = record.get_value(column_idx)? else {
                            return Err(LimboError::InternalError(
                                "table column value must be sparse vector".to_string(),
                            ));
                        };
                        let data = Vector::from_vec(data.to_vec())?;
                        if !matches!(data.vector_type, VectorType::Float32Sparse) {
                            return Err(LimboError::InternalError(
                                "table column value must be sparse vector".to_string(),
                            ));
                        }
                        let Some(arg) = values[1].get_value().to_blob() else {
                            return Err(LimboError::InternalError(
                                "first value must be sparse vector".to_string(),
                            ));
                        };
                        let arg = Vector::from_vec(arg.to_vec())?;
                        if !matches!(arg.vector_type, VectorType::Float32Sparse) {
                            return Err(LimboError::InternalError(
                                "first value must be sparse vector".to_string(),
                            ));
                        }
                        tracing::debug!(
                            "vector: {:?}, query: {:?}",
                            data.as_f32_sparse(),
                            arg.as_f32_sparse()
                        );
                        let distance = operations::jaccard::vector_distance_jaccard(&data, &arg)?;
                        let Some(dists) = distances.as_mut() else {
                            return Err(LimboError::InternalError(
                                "distances must be present in EvaluateRead state".to_string(),
                            ));
                        };
                        dists.insert((FloatOrd(distance), *rowid));
                        if dists.len() > *limit as usize {
                            let _ = dists.pop_last();
                        }
                    }

                    self.search_state = VectorSparseInvertedIndexSearchState::EvaluateSeek {
                        sum: *sum,
                        components: components.take(),
                        collected: collected.take(),
                        distances: distances.take(),
                        limit: *limit,
                        current: current.take(),
                        rowid: None,
                    };
                }
            }
        }
    }

    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        let Some(result) = self.search_result.front() else {
            return Err(LimboError::InternalError(
                "search_result must not be empty when query_rowid is called".to_string(),
            ));
        };
        Ok(IOResult::Done(Some(result.0)))
    }

    fn query_column(&mut self, _: usize) -> Result<IOResult<Value>> {
        let Some(result) = self.search_result.front() else {
            return Err(LimboError::InternalError(
                "search_result must not be empty when query_column is called".to_string(),
            ));
        };
        Ok(IOResult::Done(Value::Float(result.1)))
    }

    fn query_next(&mut self) -> Result<IOResult<bool>> {
        let _ = self.search_result.pop_front();
        Ok(IOResult::Done(!self.search_result.is_empty()))
    }
}
