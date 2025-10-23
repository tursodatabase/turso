use std::{collections::HashMap, sync::Arc};

use turso_parser::ast::SortOrder;

use crate::{
    return_if_io,
    storage::btree::{BTreeCursor, BTreeKey, CursorTrait},
    translate::collate::CollationSeq,
    types::{IOResult, ImmutableRecord, IndexInfo, KeyInfo, SeekKey, SeekOp, SeekResult},
    vdbe::Register,
    vector::vector_types::{Vector, VectorType},
    Connection, LimboError, MvStore, Result, Statement, Value,
};

#[derive(Debug, Clone)]
pub struct IndexConfiguration {
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub settings: HashMap<String, Value>,
}

pub trait IndexModule: std::fmt::Debug + Send + Sync {
    fn init(&self, configuration: &IndexConfiguration) -> Result<Box<dyn IndexCursor>>;
}

pub const HIDDEN_BTREE_MODULE_NAME: &str = "btree";

#[derive(Debug)]
pub struct VectorSparseInvertedIndex;

pub enum VectorSparseInvertedIndexCreateState {
    Init,
    Run { stmt: Statement },
}

pub enum VectorSparseInvertedIndexInsertState {
    Init,
    Prepare {
        positions: Option<Vec<u32>>,
        rowid: i64,
        idx: usize,
    },
    Seek {
        positions: Option<Vec<u32>>,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    Insert {
        positions: Option<Vec<u32>>,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
}

pub enum VectorSparseInvertedIndexDeleteState {
    Init,
    Prepare {
        positions: Option<Vec<u32>>,
        rowid: i64,
        idx: usize,
    },
    Seek {
        positions: Option<Vec<u32>>,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    Insert {
        positions: Option<Vec<u32>>,
        rowid: i64,
        idx: usize,
    },
}

pub struct VectorSparseInvertedIndexCursor {
    configuration: IndexConfiguration,
    scratch_btree: String,
    cursor: Option<BTreeCursor>,
    definition: IndexDefinition,
    create_state: VectorSparseInvertedIndexCreateState,
    insert_state: VectorSparseInvertedIndexInsertState,
    delete_state: VectorSparseInvertedIndexDeleteState,
}

impl IndexModule for VectorSparseInvertedIndex {
    fn init(&self, configuration: &IndexConfiguration) -> Result<Box<dyn IndexCursor>> {
        Ok(Box::new(VectorSparseInvertedIndexCursor::new(
            configuration.clone(),
        )))
    }
}

impl VectorSparseInvertedIndexCursor {
    pub fn new(configuration: IndexConfiguration) -> Self {
        let scratch_btree = format!("{}_scratch", configuration.index_name);
        let query_pattern1 = format!("SELECT rowid, vector_distance_jaccard({}, :v) as distance FROM {} ORDER BY distance LIMIT :k", configuration.columns[0], configuration.table_name);
        let query_pattern2 = format!("SELECT rowid, vector_distance_jaccard(:v, {}) as distance FROM {} ORDER BY distance LIMIT :k", configuration.columns[0], configuration.table_name);
        let definition = IndexDefinition {
            patterns: vec![query_pattern1, query_pattern2],
        };
        Self {
            configuration,
            scratch_btree,
            definition,
            cursor: None,
            create_state: VectorSparseInvertedIndexCreateState::Init,
            insert_state: VectorSparseInvertedIndexInsertState::Init,
            delete_state: VectorSparseInvertedIndexDeleteState::Init,
        }
    }
}

impl IndexCursor for VectorSparseInvertedIndexCursor {
    fn definition(&self) -> IndexDefinition {
        todo!()
    }

    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        loop {
            match &mut self.create_state {
                VectorSparseInvertedIndexCreateState::Init => {
                    let sql = format!(
                        "CREATE INDEX {} ON {} USING {} ({})",
                        self.scratch_btree,
                        self.configuration.table_name,
                        HIDDEN_BTREE_MODULE_NAME,
                        self.configuration.columns.join(", ")
                    );
                    let stmt = connection.prepare(&sql)?;
                    connection.start_nested();
                    self.create_state = VectorSparseInvertedIndexCreateState::Run { stmt };
                }
                VectorSparseInvertedIndexCreateState::Run { stmt } => {
                    // we need to properly track subprograms and propagate result to the root program to make this execution async
                    let result = stmt.run_ignore_rows();
                    connection.end_nested();
                    result?;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        todo!()
    }

    fn open_read(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        let key_info = KeyInfo {
            collation: CollationSeq::Binary,
            sort_order: SortOrder::Asc,
        };
        self.cursor = Some(open_btree_cursor(
            connection,
            &self.configuration.table_name,
            &self.scratch_btree,
            vec![key_info.clone(), key_info],
        )?);
        Ok(IOResult::Done(()))
    }

    fn open_write(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        let key_info = KeyInfo {
            collation: CollationSeq::Binary,
            sort_order: SortOrder::Asc,
        };
        self.cursor = Some(open_btree_cursor(
            connection,
            &self.configuration.table_name,
            &self.scratch_btree,
            vec![key_info.clone(), key_info],
        )?);
        Ok(IOResult::Done(()))
    }

    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        let Some(cursor) = &mut self.cursor else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        tracing::info!("insert: {:?}", values);
        loop {
            match &mut self.insert_state {
                VectorSparseInvertedIndexInsertState::Init => {
                    let Some(vector) = values[0].get_value().to_blob() else {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    };
                    let Some(rowid) = values[1].get_value().as_int() else {
                        return Err(LimboError::InternalError(
                            "second value must be i64 rowid".to_string(),
                        ));
                    };
                    let vector = Vector::from_slice(vector)?;
                    if !matches!(vector.vector_type, VectorType::Float32Sparse) {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    }
                    let sparse = vector.as_f32_sparse();
                    self.insert_state = VectorSparseInvertedIndexInsertState::Prepare {
                        positions: Some(sparse.idx.to_vec()),
                        rowid,
                        idx: 0,
                    }
                }
                VectorSparseInvertedIndexInsertState::Prepare {
                    positions,
                    rowid,
                    idx,
                } => {
                    let p = positions.as_ref().unwrap();
                    if *idx == p.len() {
                        self.insert_state = VectorSparseInvertedIndexInsertState::Init;
                        return Ok(IOResult::Done(()));
                    }
                    let position = p[*idx];
                    tracing::info!("position: {}", position);
                    let key = ImmutableRecord::from_values(
                        &[Value::Integer(position as i64), Value::Integer(*rowid)],
                        2,
                    );
                    self.insert_state = VectorSparseInvertedIndexInsertState::Seek {
                        idx: *idx,
                        rowid: *rowid,
                        positions: positions.take(),
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexInsertState::Seek {
                    positions,
                    rowid,
                    idx,
                    key,
                } => {
                    let k = key.as_ref().unwrap();
                    let _ = return_if_io!(
                        cursor.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: false })
                    );
                    self.insert_state = VectorSparseInvertedIndexInsertState::Insert {
                        idx: *idx,
                        rowid: *rowid,
                        positions: positions.take(),
                        key: key.take(),
                    };
                }
                VectorSparseInvertedIndexInsertState::Insert {
                    positions,
                    rowid,
                    idx,
                    key,
                } => {
                    let k = key.as_ref().unwrap();
                    tracing::info!("insert_key: {:?}", k);
                    let _ = return_if_io!(cursor.insert(&BTreeKey::IndexKey(k)));
                    self.insert_state = VectorSparseInvertedIndexInsertState::Prepare {
                        idx: *idx + 1,
                        rowid: *rowid,
                        positions: positions.take(),
                    };
                }
            }
        }
    }

    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        let Some(cursor) = &mut self.cursor else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        tracing::info!("delete: {:?}", values);
        loop {
            match &mut self.delete_state {
                VectorSparseInvertedIndexDeleteState::Init => {
                    let Some(vector) = values[0].get_value().to_blob() else {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    };
                    let Some(rowid) = values[1].get_value().as_int() else {
                        return Err(LimboError::InternalError(
                            "second value must be i64 rowid".to_string(),
                        ));
                    };
                    let vector = Vector::from_slice(vector)?;
                    if !matches!(vector.vector_type, VectorType::Float32Sparse) {
                        return Err(LimboError::InternalError(
                            "first value must be sparse vector".to_string(),
                        ));
                    }
                    let sparse = vector.as_f32_sparse();
                    self.delete_state = VectorSparseInvertedIndexDeleteState::Prepare {
                        positions: Some(sparse.idx.to_vec()),
                        rowid,
                        idx: 0,
                    }
                }
                VectorSparseInvertedIndexDeleteState::Prepare {
                    positions,
                    rowid,
                    idx,
                } => {
                    let p = positions.as_ref().unwrap();
                    if *idx == p.len() {
                        self.delete_state = VectorSparseInvertedIndexDeleteState::Init;
                        return Ok(IOResult::Done(()));
                    }
                    let position = p[*idx];
                    let key = ImmutableRecord::from_values(
                        &[Value::Integer(position as i64), Value::Integer(*rowid)],
                        2,
                    );
                    self.delete_state = VectorSparseInvertedIndexDeleteState::Seek {
                        idx: *idx,
                        rowid: *rowid,
                        positions: positions.take(),
                        key: Some(key),
                    };
                }
                VectorSparseInvertedIndexDeleteState::Seek {
                    positions,
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
                    self.delete_state = VectorSparseInvertedIndexDeleteState::Insert {
                        idx: *idx,
                        rowid: *rowid,
                        positions: positions.take(),
                    };
                }
                VectorSparseInvertedIndexDeleteState::Insert {
                    positions,
                    rowid,
                    idx,
                } => {
                    let _ = return_if_io!(cursor.delete());
                    self.delete_state = VectorSparseInvertedIndexDeleteState::Prepare {
                        idx: *idx + 1,
                        rowid: *rowid,
                        positions: positions.take(),
                    };
                }
            }
        }
    }

    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        todo!()
    }

    fn query_next(&mut self) -> Result<IOResult<bool>> {
        todo!()
    }

    fn query_column(&mut self, position: usize) -> Result<IOResult<&Value>> {
        todo!()
    }

    fn commit(&mut self) -> Result<()> {
        // no explicit commit/rollback for vector_sparse_ivf as it fully backed by btree layer
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        // no explicit commit/rollback for vector_sparse_ivf as it fully backed by btree layer
        Ok(())
    }
}

fn open_btree_cursor(
    connection: &Connection,
    table: &str,
    index: &str,
    keys: Vec<KeyInfo>,
) -> Result<BTreeCursor> {
    let pager = connection.pager.read().clone();
    let schema = connection.schema.read();
    let Some(scratch) = schema.get_index(table, index) else {
        return Err(LimboError::InternalError(format!(
            "index {} for table {} not found",
            index, table
        )));
    };
    let mut cursor = BTreeCursor::new(pager, scratch.root_page, keys.len());
    cursor.index_info = Some(IndexInfo {
        has_rowid: false,
        num_cols: keys.len(),
        key_info: keys,
    });
    Ok(cursor)
}

pub struct IndexDefinition {
    pub patterns: Vec<String>,
}

pub trait IndexCursor {
    fn definition(&self) -> IndexDefinition;

    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;
    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;

    fn open_read(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;
    fn open_write(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;

    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>>;
    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>>;
    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<()>>;
    fn query_next(&mut self) -> Result<IOResult<bool>>;
    fn query_column(&mut self, position: usize) -> Result<IOResult<&Value>>;

    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
}
