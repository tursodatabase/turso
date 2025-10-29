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
    SeekScratch {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    InsertScratch {
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
    SeekScratch {
        vector: Option<Vector<'static>>,
        sum: f64,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    DeleteScratch {
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
enum VectorSparseInvertedIndexSearchState {
    Init,
    Prepare {
        collected: Option<HashSet<i64>>,
        positions: Option<Vec<u32>>,
        idx: usize,
        limit: i64,
    },
    Seek {
        collected: Option<HashSet<i64>>,
        positions: Option<Vec<u32>>,
        key: Option<ImmutableRecord>,
        idx: usize,
        limit: i64,
    },
    Read {
        collected: Option<HashSet<i64>>,
        positions: Option<Vec<u32>>,
        key: Option<ImmutableRecord>,
        idx: usize,
        limit: i64,
    },
    Next {
        collected: Option<HashSet<i64>>,
        positions: Option<Vec<u32>>,
        key: Option<ImmutableRecord>,
        idx: usize,
        limit: i64,
    },
    EvaluateSeek {
        rowids: Option<Vec<i64>>,
        distances: Option<BTreeSet<(FloatOrd, i64)>>,
        limit: i64,
    },
    EvaluateRead {
        rowids: Option<Vec<i64>>,
        distances: Option<BTreeSet<(FloatOrd, i64)>>,
        limit: i64,
    },
}

pub struct VectorSparseInvertedIndexMethodCursor {
    configuration: IndexMethodConfiguration,
    delta: f64,
    scratch_btree: String,
    scratch_cursor: Option<BTreeCursor>,
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
        let scratch_btree = format!("{}_scratch", configuration.index_name);
        let stats_btree = format!("{}_stats", configuration.index_name);
        let delta = match configuration.parameters.get("delta") {
            Some(&Value::Float(delta)) => delta,
            None => 0.0,
        };
        Self {
            configuration,
            delta,
            scratch_btree,
            scratch_cursor: None,
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
        let scratch_index_create = format!(
            "CREATE INDEX {} ON {} USING {} ({})",
            self.scratch_btree,
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
        for sql in [scratch_index_create, stats_index_create] {
            let mut stmt = connection.prepare(&sql)?;
            connection.start_nested();
            let result = stmt.run_ignore_rows();
            connection.end_nested();
            result?;
        }

        Ok(IOResult::Done(()))
    }

    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        let scratch_index_drop = format!("DROP INDEX {}", self.scratch_btree);
        let stats_index_drop = format!("DROP INDEX {}", self.stats_btree);
        for sql in [scratch_index_drop, stats_index_drop] {
            let mut stmt = connection.prepare(&sql)?;
            connection.start_nested();
            let result = stmt.run_ignore_rows();
            connection.end_nested();
            result?;
        }

        Ok(IOResult::Done(()))
    }

    fn open_read(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        self.scratch_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.scratch_btree,
            // component, length, rowid
            vec![key_info(), key_info(), key_info()],
        )?);
        self.stats_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.stats_btree,
            // component, count, non-zero-min, non-zero-max
            vec![key_info(), key_info(), key_info(), key_info()],
        )?);
        self.main_btree = Some(open_table_cursor(
            connection,
            &self.configuration.table_name,
        )?);
        Ok(IOResult::Done(()))
    }

    fn open_write(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        self.scratch_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.scratch_btree,
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
        let Some(scratch_cursor) = &mut self.scratch_cursor else {
            return Err(LimboError::InternalError(
                "scratch cursor must be opened".to_string(),
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
                    let v = vector.as_ref().unwrap();
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
                    self.insert_state = VectorSparseInvertedIndexInsertState::SeekScratch {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexInsertState::SeekScratch {
                    vector,
                    sum,
                    rowid,
                    idx,
                    key,
                } => {
                    let k = key.as_ref().unwrap();
                    let _ =
                        return_if_io!(scratch_cursor
                            .seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: false }));
                    self.insert_state = VectorSparseInvertedIndexInsertState::InsertScratch {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                        key: key.take(),
                    };
                }
                VectorSparseInvertedIndexInsertState::InsertScratch {
                    vector,
                    sum,
                    rowid,
                    idx,
                    key,
                } => {
                    let k = key.as_ref().unwrap();
                    return_if_io!(scratch_cursor.insert(&BTreeKey::IndexKey(k)));

                    let v = vector.as_ref().unwrap();
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
                    let k = key.as_ref().unwrap();
                    let result = return_if_io!(
                        stats_cursor.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: false })
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
                            let v = vector.as_ref().unwrap();
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
                                    Value::Integer(1 as i64),
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
                    let record = return_if_io!(stats_cursor.record()).unwrap();
                    let ValueRef::Integer(cnt) = record.get_value(1)? else {
                        return Err(LimboError::Corrupt(format!(
                            "stats index corrupted: expected integer"
                        )));
                    };
                    let ValueRef::Float(min) = record.get_value(2)? else {
                        return Err(LimboError::Corrupt(format!(
                            "stats index corrupted: expected float"
                        )));
                    };
                    let ValueRef::Float(max) = record.get_value(3)? else {
                        return Err(LimboError::Corrupt(format!(
                            "stats index corrupted: expected float"
                        )));
                    };
                    let v = vector.as_ref().unwrap();
                    let position = v.as_f32_sparse().idx[*idx];
                    let value = v.as_f32_sparse().values[*idx] as f64;
                    tracing::debug!(
                        "update stats(insert): {} (cnt={}, min={}, max={})",
                        position,
                        cnt + 1,
                        value.min(min),
                        value.max(max),
                    );
                    let key = ImmutableRecord::from_values(
                        &[
                            Value::Integer(position as i64),
                            Value::Integer(cnt + 1),
                            Value::Float(value.min(min)),
                            Value::Float(value.max(max)),
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
                    let k = key.as_ref().unwrap();
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
        let Some(cursor) = &mut self.scratch_cursor else {
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
                    let v = vector.as_ref().unwrap();
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
                    self.delete_state = VectorSparseInvertedIndexDeleteState::SeekScratch {
                        vector: vector.take(),
                        idx: *idx,
                        sum: *sum,
                        rowid: *rowid,
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexDeleteState::SeekScratch {
                    vector,
                    sum,
                    rowid,
                    idx,
                    key,
                } => {
                    let k = key.as_ref().unwrap();
                    let result = return_if_io!(
                        cursor.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: true })
                    );
                    if !matches!(result, SeekResult::Found) {
                        return Err(LimboError::Corrupt("inverted index corrupted".to_string()));
                    }
                    self.delete_state = VectorSparseInvertedIndexDeleteState::DeleteScratch {
                        vector: vector.take(),
                        sum: *sum,
                        idx: *idx,
                        rowid: *rowid,
                    };
                }
                VectorSparseInvertedIndexDeleteState::DeleteScratch {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    return_if_io!(cursor.delete());
                    let v = vector.as_ref().unwrap();
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
                    let k = key.as_ref().unwrap();
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
                            return Err(LimboError::Corrupt(format!(
                                "stats index corrupted: can't find component row"
                            )))
                        }
                    }
                }
                VectorSparseInvertedIndexDeleteState::ReadStats {
                    vector,
                    sum,
                    rowid,
                    idx,
                } => {
                    let record = return_if_io!(stats_cursor.record()).unwrap();
                    let ValueRef::Integer(cnt) = record.get_value(1)? else {
                        return Err(LimboError::Corrupt(format!(
                            "stats index corrupted: expected integer"
                        )));
                    };
                    let ValueRef::Float(min) = record.get_value(2)? else {
                        return Err(LimboError::Corrupt(format!(
                            "stats index corrupted: expected float"
                        )));
                    };
                    let ValueRef::Float(max) = record.get_value(3)? else {
                        return Err(LimboError::Corrupt(format!(
                            "stats index corrupted: expected float"
                        )));
                    };
                    let v = vector.as_ref().unwrap();
                    let position = v.as_f32_sparse().idx[*idx];
                    tracing::debug!(
                        "update stats(delete): {} (cnt={}, min={}, max={})",
                        position,
                        cnt - 1,
                        min,
                        max,
                    );
                    let key = ImmutableRecord::from_values(
                        &[
                            Value::Integer(position as i64),
                            Value::Integer(cnt - 1),
                            Value::Float(min),
                            Value::Float(max),
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
                    let k = key.as_ref().unwrap();
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
        let Some(scratch) = &mut self.scratch_cursor else {
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
                    let vector = Vector::from_slice(vector)?;
                    if !matches!(vector.vector_type, VectorType::Float32Sparse) {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    }
                    let sparse = vector.as_f32_sparse();
                    self.search_state = VectorSparseInvertedIndexSearchState::Prepare {
                        collected: Some(HashSet::new()),
                        positions: Some(sparse.idx.to_vec()),
                        idx: 0,
                        limit,
                    };
                }
                VectorSparseInvertedIndexSearchState::Prepare {
                    collected,
                    positions,
                    idx,
                    limit,
                } => {
                    let p = positions.as_ref().unwrap();
                    if *idx == p.len() {
                        let mut rowids = collected
                            .take()
                            .unwrap()
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();
                        rowids.sort();
                        self.search_state = VectorSparseInvertedIndexSearchState::EvaluateSeek {
                            rowids: Some(rowids),
                            distances: Some(BTreeSet::new()),
                            limit: *limit,
                        };
                        continue;
                    }
                    let position = p[*idx];
                    let key = ImmutableRecord::from_values(&[Value::Integer(position as i64)], 1);
                    self.search_state = VectorSparseInvertedIndexSearchState::Seek {
                        collected: collected.take(),
                        positions: positions.take(),
                        key: Some(key),
                        idx: *idx,
                        limit: *limit,
                    };
                }
                VectorSparseInvertedIndexSearchState::Seek {
                    collected,
                    positions,
                    key,
                    idx,
                    limit,
                } => {
                    let k = key.as_ref().unwrap();
                    let result = return_if_io!(
                        scratch.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: false })
                    );
                    match result {
                        SeekResult::Found => {
                            self.search_state = VectorSparseInvertedIndexSearchState::Read {
                                collected: collected.take(),
                                positions: positions.take(),
                                key: key.take(),
                                idx: *idx,
                                limit: *limit,
                            };
                        }
                        SeekResult::TryAdvance | SeekResult::NotFound => {
                            self.search_state = VectorSparseInvertedIndexSearchState::Next {
                                collected: collected.take(),
                                positions: positions.take(),
                                key: key.take(),
                                idx: *idx,
                                limit: *limit,
                            };
                        }
                    }
                }
                VectorSparseInvertedIndexSearchState::Read {
                    collected,
                    positions,
                    key,
                    idx,
                    limit,
                } => {
                    let record = return_if_io!(scratch.record());
                    if let Some(record) = record {
                        let ValueRef::Integer(position) = record.get_value(0)? else {
                            return Err(LimboError::InternalError(
                                "first value of index record must be int".to_string(),
                            ));
                        };
                        let ValueRef::Integer(rowid) = record.get_value(1)? else {
                            return Err(LimboError::InternalError(
                                "second value of index record must be int".to_string(),
                            ));
                        };
                        tracing::debug!("position/rowid: {}/{}", position, rowid);
                        if position == positions.as_ref().unwrap()[*idx] as i64 {
                            collected.as_mut().unwrap().insert(rowid);
                            self.search_state = VectorSparseInvertedIndexSearchState::Next {
                                collected: collected.take(),
                                positions: positions.take(),
                                key: key.take(),
                                idx: *idx,
                                limit: *limit,
                            };
                            continue;
                        }
                    }
                    self.search_state = VectorSparseInvertedIndexSearchState::Prepare {
                        collected: collected.take(),
                        positions: positions.take(),
                        idx: *idx + 1,
                        limit: *limit,
                    };
                }
                VectorSparseInvertedIndexSearchState::Next {
                    collected,
                    positions,
                    key,
                    idx,
                    limit,
                } => {
                    let result = return_if_io!(scratch.next());
                    if !result {
                        self.search_state = VectorSparseInvertedIndexSearchState::Prepare {
                            collected: collected.take(),
                            positions: positions.take(),
                            idx: *idx + 1,
                            limit: *limit,
                        };
                    } else {
                        self.search_state = VectorSparseInvertedIndexSearchState::Read {
                            collected: collected.take(),
                            positions: positions.take(),
                            key: key.take(),
                            idx: *idx,
                            limit: *limit,
                        };
                    }
                }
                VectorSparseInvertedIndexSearchState::EvaluateSeek {
                    rowids,
                    distances,
                    limit,
                } => {
                    let Some(rowid) = rowids.as_ref().unwrap().last() else {
                        let distances = distances.take().unwrap();
                        self.search_result = distances.iter().map(|(d, i)| (*i, d.0)).collect();
                        return Ok(IOResult::Done(!self.search_result.is_empty()));
                    };
                    let result = return_if_io!(
                        main.seek(SeekKey::TableRowId(*rowid), SeekOp::GE { eq_only: true })
                    );
                    if !matches!(result, SeekResult::Found) {
                        return Err(LimboError::Corrupt(
                            "vector_sparse_ivf corrupted: unable to find rowid in main table"
                                .to_string(),
                        ));
                    };
                    self.search_state = VectorSparseInvertedIndexSearchState::EvaluateRead {
                        rowids: rowids.take(),
                        distances: distances.take(),
                        limit: *limit,
                    };
                }
                VectorSparseInvertedIndexSearchState::EvaluateRead {
                    rowids,
                    distances,
                    limit,
                } => {
                    let record = return_if_io!(main.record());
                    let rowid = rowids.as_mut().unwrap().pop().unwrap();
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
                        let distances = distances.as_mut().unwrap();
                        distances.insert((FloatOrd(distance), rowid));
                        if distances.len() > *limit as usize {
                            let _ = distances.pop_last();
                        }
                    }

                    self.search_state = VectorSparseInvertedIndexSearchState::EvaluateSeek {
                        rowids: rowids.take(),
                        distances: distances.take(),
                        limit: *limit,
                    };
                }
            }
        }
    }

    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        let result = self.search_result.front().unwrap();
        Ok(IOResult::Done(Some(result.0)))
    }

    fn query_column(&mut self, _: usize) -> Result<IOResult<Value>> {
        let result = self.search_result.front().unwrap();
        Ok(IOResult::Done(Value::Float(result.1)))
    }

    fn query_next(&mut self) -> Result<IOResult<bool>> {
        let _ = self.search_result.pop_front();
        Ok(IOResult::Done(!self.search_result.is_empty()))
    }
}
