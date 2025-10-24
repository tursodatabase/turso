use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use turso_parser::ast::{self, SortOrder};

use crate::{
    index::{
        open_index_cursor, open_table_cursor, parse_patterns, IndexConfiguration, IndexCursor,
        IndexDefinition, IndexDescriptor, IndexModule, HIDDEN_BTREE_MODULE_NAME,
        TOY_TEXT_IVF_MODULE_NAME,
    },
    return_if_io,
    storage::btree::{BTreeCursor, BTreeKey, CursorTrait},
    translate::collate::CollationSeq,
    types::{IOResult, ImmutableRecord, KeyInfo, SeekKey, SeekOp, SeekResult, Text},
    vdbe::{RegexCache, Register},
    Connection, LimboError, Result, Statement, Value, ValueRef,
};

#[derive(Debug)]
pub struct TextInvertedIndex;

#[derive(Debug)]
pub struct TextInvertedIndexDescriptor {
    configuration: IndexConfiguration,
    patterns: Vec<ast::Select>,
}

pub enum TextInvertedIndexCreateState {
    Init,
    Run { stmt: Statement },
}

pub enum TextInvertedIndexInsertState {
    Init,
    Prepare {
        tokens: Option<Vec<String>>,
        rowid: i64,
        idx: usize,
    },
    Seek {
        tokens: Option<Vec<String>>,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    Insert {
        tokens: Option<Vec<String>>,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
}

pub enum TextInvertedIndexDeleteState {
    Init,
    Prepare {
        tokens: Option<Vec<String>>,
        rowid: i64,
        idx: usize,
    },
    Seek {
        tokens: Option<Vec<String>>,
        key: Option<ImmutableRecord>,
        rowid: i64,
        idx: usize,
    },
    Insert {
        tokens: Option<Vec<String>>,
        rowid: i64,
        idx: usize,
    },
}

#[derive(Debug)]
pub enum TextInvertedIndexSearchState {
    Init,
    Prepare {
        collected: Option<HashSet<i64>>,
        tokens: Option<Vec<String>>,
        idx: usize,
    },
    Seek {
        collected: Option<HashSet<i64>>,
        tokens: Option<Vec<String>>,
        key: Option<ImmutableRecord>,
        idx: usize,
    },
    Read {
        collected: Option<HashSet<i64>>,
        tokens: Option<Vec<String>>,
        key: Option<ImmutableRecord>,
        idx: usize,
    },
    Next {
        collected: Option<HashSet<i64>>,
        tokens: Option<Vec<String>>,
        key: Option<ImmutableRecord>,
        idx: usize,
    },
}

pub struct TextInvertedIndexCursor {
    regex_cache: RegexCache,
    configuration: IndexConfiguration,
    scratch_btree: String,
    scratch_cursor: Option<BTreeCursor>,
    main_btree: Option<BTreeCursor>,
    create_state: TextInvertedIndexCreateState,
    insert_state: TextInvertedIndexInsertState,
    delete_state: TextInvertedIndexDeleteState,
    search_state: TextInvertedIndexSearchState,
    search_result: VecDeque<i64>,
}

impl IndexModule for TextInvertedIndex {
    fn descriptor(&self, configuration: &IndexConfiguration) -> Result<Arc<dyn IndexDescriptor>> {
        let query_pattern = format!(
            "SELECT rowid FROM {} WHERE fts_has_any_token({}, ?)",
            configuration.table_name, configuration.columns[0].name
        );
        Ok(Arc::new(TextInvertedIndexDescriptor {
            configuration: configuration.clone(),
            patterns: parse_patterns(&[&query_pattern])?,
        }))
    }
}

impl IndexDescriptor for TextInvertedIndexDescriptor {
    fn definition<'a>(&'a self) -> IndexDefinition<'a> {
        IndexDefinition {
            module_name: TOY_TEXT_IVF_MODULE_NAME,
            index_name: &self.configuration.index_name,
            patterns: self.patterns.as_slice(),
            hidden: false,
        }
    }
    fn init(&self) -> Result<Box<dyn IndexCursor>> {
        Ok(Box::new(TextInvertedIndexCursor::new(
            self.configuration.clone(),
        )))
    }
}

impl TextInvertedIndexCursor {
    pub fn new(configuration: IndexConfiguration) -> Self {
        let scratch_btree = format!("{}_scratch", configuration.index_name);
        let regex_cache = RegexCache::new();
        Self {
            regex_cache,
            configuration,
            scratch_btree,
            scratch_cursor: None,
            main_btree: None,
            search_result: VecDeque::new(),
            create_state: TextInvertedIndexCreateState::Init,
            insert_state: TextInvertedIndexInsertState::Init,
            delete_state: TextInvertedIndexDeleteState::Init,
            search_state: TextInvertedIndexSearchState::Init,
        }
    }
}

impl TextInvertedIndexCursor {
    pub fn tokenize<'a>(&mut self, text: &'a str) -> impl Iterator<Item = &'a str> + use<'a, '_> {
        let regex = self.regex_cache.token_regex();
        regex.find_iter(text).map(|m| m.as_str())
    }
}

impl IndexCursor for TextInvertedIndexCursor {
    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        loop {
            match &mut self.create_state {
                TextInvertedIndexCreateState::Init => {
                    let columns = &self.configuration.columns;
                    let columns = columns.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
                    let sql = format!(
                        "CREATE INDEX {} ON {} USING {} ({})",
                        self.scratch_btree,
                        self.configuration.table_name,
                        HIDDEN_BTREE_MODULE_NAME,
                        columns.join(", ")
                    );
                    let stmt = connection.prepare(&sql)?;
                    connection.start_nested();
                    self.create_state = TextInvertedIndexCreateState::Run { stmt };
                }
                TextInvertedIndexCreateState::Run { stmt } => {
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
        let columns = &self.configuration.columns;
        let sql = format!("DROP INDEX {}", self.scratch_btree);
        let mut stmt = connection.prepare(&sql)?;
        connection.start_nested();
        let result = stmt.run_ignore_rows();
        connection.end_nested();
        result?;
        return Ok(IOResult::Done(()));
    }

    fn open_read(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        let key_info = KeyInfo {
            collation: CollationSeq::Binary,
            sort_order: SortOrder::Asc,
        };
        self.scratch_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.scratch_btree,
            vec![key_info.clone(), key_info],
        )?);
        self.main_btree = Some(open_table_cursor(
            connection,
            &self.configuration.table_name,
        )?);
        Ok(IOResult::Done(()))
    }

    fn open_write(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        let key_info = KeyInfo {
            collation: CollationSeq::Binary,
            sort_order: SortOrder::Asc,
        };
        self.scratch_cursor = Some(open_index_cursor(
            connection,
            &self.configuration.table_name,
            &self.scratch_btree,
            vec![key_info.clone(), key_info],
        )?);
        Ok(IOResult::Done(()))
    }

    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        let mut tokens = if matches!(self.insert_state, TextInvertedIndexInsertState::Init) {
            let Some(text) = values[0].get_value().to_text() else {
                return Err(LimboError::InternalError(
                    "first value must be text".to_string(),
                ));
            };
            Some(self.tokenize(text).map(|x| x.to_string()).collect())
        } else {
            None
        };

        let Some(cursor) = &mut self.scratch_cursor else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        loop {
            match &mut self.insert_state {
                TextInvertedIndexInsertState::Init => {
                    let Some(rowid) = values[1].get_value().as_int() else {
                        return Err(LimboError::InternalError(
                            "second value must be i64 rowid".to_string(),
                        ));
                    };
                    self.insert_state = TextInvertedIndexInsertState::Prepare {
                        tokens: tokens.take(),
                        rowid,
                        idx: 0,
                    }
                }
                TextInvertedIndexInsertState::Prepare { tokens, rowid, idx } => {
                    let t = tokens.as_ref().unwrap();
                    if *idx == t.len() {
                        self.insert_state = TextInvertedIndexInsertState::Init;
                        return Ok(IOResult::Done(()));
                    }
                    let token = &t[*idx];
                    let key = ImmutableRecord::from_values(
                        &[Value::Text(Text::new(token)), Value::Integer(*rowid)],
                        2,
                    );
                    self.insert_state = TextInvertedIndexInsertState::Seek {
                        idx: *idx,
                        rowid: *rowid,
                        tokens: tokens.take(),
                        key: Some(key),
                    };
                }
                TextInvertedIndexInsertState::Seek {
                    tokens,
                    rowid,
                    idx,
                    key,
                } => {
                    let k = key.as_ref().unwrap();
                    let _ = return_if_io!(
                        cursor.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: false })
                    );
                    self.insert_state = TextInvertedIndexInsertState::Insert {
                        idx: *idx,
                        rowid: *rowid,
                        tokens: tokens.take(),
                        key: key.take(),
                    };
                }
                TextInvertedIndexInsertState::Insert {
                    tokens,
                    rowid,
                    idx,
                    key,
                } => {
                    let k = key.as_ref().unwrap();
                    let _ = return_if_io!(cursor.insert(&BTreeKey::IndexKey(k)));
                    self.insert_state = TextInvertedIndexInsertState::Prepare {
                        idx: *idx + 1,
                        rowid: *rowid,
                        tokens: tokens.take(),
                    };
                }
            }
        }
    }

    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        let mut tokens = if matches!(self.delete_state, TextInvertedIndexDeleteState::Init) {
            let Some(text) = values[0].get_value().to_text() else {
                return Err(LimboError::InternalError(
                    "first value must be text".to_string(),
                ));
            };
            Some(self.tokenize(text).map(|x| x.to_string()).collect())
        } else {
            None
        };

        let Some(cursor) = &mut self.scratch_cursor else {
            return Err(LimboError::InternalError(
                "cursor must be opened".to_string(),
            ));
        };
        loop {
            match &mut self.delete_state {
                TextInvertedIndexDeleteState::Init => {
                    let Some(rowid) = values[1].get_value().as_int() else {
                        return Err(LimboError::InternalError(
                            "second value must be i64 rowid".to_string(),
                        ));
                    };
                    self.delete_state = TextInvertedIndexDeleteState::Prepare {
                        tokens: tokens.take(),
                        rowid,
                        idx: 0,
                    }
                }
                TextInvertedIndexDeleteState::Prepare { tokens, rowid, idx } => {
                    let t = tokens.as_ref().unwrap();
                    if *idx == t.len() {
                        self.delete_state = TextInvertedIndexDeleteState::Init;
                        return Ok(IOResult::Done(()));
                    }
                    let token = &t[*idx];
                    let key = ImmutableRecord::from_values(
                        &[Value::Text(Text::new(token)), Value::Integer(*rowid)],
                        2,
                    );
                    self.delete_state = TextInvertedIndexDeleteState::Seek {
                        idx: *idx,
                        rowid: *rowid,
                        tokens: tokens.take(),
                        key: Some(key),
                    };
                }
                TextInvertedIndexDeleteState::Seek {
                    tokens,
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
                    self.delete_state = TextInvertedIndexDeleteState::Insert {
                        idx: *idx,
                        rowid: *rowid,
                        tokens: tokens.take(),
                    };
                }
                TextInvertedIndexDeleteState::Insert { tokens, rowid, idx } => {
                    let _ = return_if_io!(cursor.delete());
                    self.delete_state = TextInvertedIndexDeleteState::Prepare {
                        idx: *idx + 1,
                        rowid: *rowid,
                        tokens: tokens.take(),
                    };
                }
            }
        }
    }

    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<bool>> {
        let mut tokens = if matches!(self.search_state, TextInvertedIndexSearchState::Init) {
            let Some(text) = values[1].get_value().to_text() else {
                return Err(LimboError::InternalError(
                    "first value must be text".to_string(),
                ));
            };
            Some(self.tokenize(text).map(|x| x.to_string()).collect())
        } else {
            None
        };

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
            tracing::debug!("state: {:?}", self.search_state);
            match &mut self.search_state {
                TextInvertedIndexSearchState::Init => {
                    self.search_state = TextInvertedIndexSearchState::Prepare {
                        collected: Some(HashSet::new()),
                        tokens: tokens.take(),
                        idx: 0,
                    };
                }
                TextInvertedIndexSearchState::Prepare {
                    collected,
                    tokens,
                    idx,
                } => {
                    let t = tokens.as_ref().unwrap();
                    if *idx == t.len() {
                        let mut rowids = collected
                            .take()
                            .unwrap()
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();
                        rowids.sort();
                        self.search_result = rowids.into();
                        return Ok(IOResult::Done(!self.search_result.is_empty()));
                    }
                    let position = &t[*idx];
                    let key = ImmutableRecord::from_values(&[Value::Text(Text::new(position))], 2);
                    self.search_state = TextInvertedIndexSearchState::Seek {
                        collected: collected.take(),
                        tokens: tokens.take(),
                        key: Some(key),
                        idx: *idx,
                    };
                }
                TextInvertedIndexSearchState::Seek {
                    collected,
                    tokens,
                    key,
                    idx,
                } => {
                    let k = key.as_ref().unwrap();
                    let result = return_if_io!(
                        scratch.seek(SeekKey::IndexKey(k), SeekOp::GE { eq_only: false })
                    );
                    match result {
                        SeekResult::Found => {
                            self.search_state = TextInvertedIndexSearchState::Read {
                                collected: collected.take(),
                                tokens: tokens.take(),
                                key: key.take(),
                                idx: *idx,
                            };
                        }
                        SeekResult::TryAdvance => {
                            self.search_state = TextInvertedIndexSearchState::Next {
                                collected: collected.take(),
                                tokens: tokens.take(),
                                key: key.take(),
                                idx: *idx,
                            };
                        }
                        SeekResult::NotFound => {
                            return Err(LimboError::Corrupt("inverted index corrupted".to_string()))
                        }
                    }
                }
                TextInvertedIndexSearchState::Read {
                    collected,
                    tokens,
                    key,
                    idx,
                } => {
                    let record = return_if_io!(scratch.record());
                    if let Some(record) = record {
                        let ValueRef::Text(token, _) = record.get_value(0)? else {
                            return Err(LimboError::InternalError(format!(
                                "first value of index record must be text"
                            )));
                        };
                        let ValueRef::Integer(rowid) = record.get_value(1)? else {
                            return Err(LimboError::InternalError(format!(
                                "second value of index record must be int"
                            )));
                        };
                        tracing::debug!("position/rowid: {:?}/{}", token, rowid);
                        if token.eq_ignore_ascii_case(tokens.as_ref().unwrap()[*idx].as_bytes()) {
                            collected.as_mut().unwrap().insert(rowid);
                            self.search_state = TextInvertedIndexSearchState::Next {
                                collected: collected.take(),
                                tokens: tokens.take(),
                                key: key.take(),
                                idx: *idx,
                            };
                            continue;
                        }
                    }
                    self.search_state = TextInvertedIndexSearchState::Prepare {
                        collected: collected.take(),
                        tokens: tokens.take(),
                        idx: *idx + 1,
                    };
                }
                TextInvertedIndexSearchState::Next {
                    collected,
                    tokens,
                    key,
                    idx,
                } => {
                    let result = return_if_io!(scratch.next());
                    if !result {
                        self.search_state = TextInvertedIndexSearchState::Prepare {
                            collected: collected.take(),
                            tokens: tokens.take(),
                            idx: *idx + 1,
                        };
                    } else {
                        self.search_state = TextInvertedIndexSearchState::Read {
                            collected: collected.take(),
                            tokens: tokens.take(),
                            key: key.take(),
                            idx: *idx,
                        };
                    }
                }
            }
        }
    }

    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        let result = self.search_result.front().unwrap();
        Ok(IOResult::Done(Some(*result)))
    }

    fn query_column(&mut self, _: usize) -> Result<IOResult<Value>> {
        let result = self.search_result.front().unwrap();
        Ok(IOResult::Done(Value::Integer(*result)))
    }

    fn query_next(&mut self) -> Result<IOResult<bool>> {
        let _ = self.search_result.pop_front();
        Ok(IOResult::Done(!self.search_result.is_empty()))
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
