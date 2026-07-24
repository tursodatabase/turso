use super::*;

#[derive(Debug)]
struct PendingDeltaRead {
    completion: Completion,
    buffer: Arc<Buffer>,
}

/// Read-only cursor over a transaction's captured changes for one base table,
/// in capture order. Spill segments are loaded asynchronously one bounded
/// batch at a time, followed by the in-memory tail. Exposes base columns at
/// their table positions and the weight as one extra trailing column.
pub struct DeltaCursor {
    file: Option<Arc<dyn File>>,
    spill_segments: Vec<SpillSegment>,
    table_id: Option<TableChangeId>,
    next_segment: usize,
    pending_read: Option<PendingDeltaRead>,
    tail: Arc<Vec<ChangeEvent>>,
    tail_positions: Vec<usize>,
    tail_loaded: bool,
    reading_tail: bool,
    /// Current spill batch, or the in-memory tail.
    rows: Vec<(i64, Vec<Value>, isize)>,
    /// Number of base-table columns; the weight is column `num_columns`.
    num_columns: usize,
    /// Position of the current row; `rows.len()` means exhausted.
    pos: usize,
    /// False until `rewind` has been called.
    positioned: bool,
}

impl DeltaCursor {
    pub(crate) fn new(delta: CapturedDelta, num_columns: usize) -> Self {
        let tail_positions = delta
            .tail
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                (Some(&event.table_id) == delta.table_id.as_ref()).then_some(index)
            })
            .collect();
        Self {
            file: delta.file,
            spill_segments: delta.spilled,
            table_id: delta.table_id,
            next_segment: 0,
            pending_read: None,
            tail: delta.tail,
            tail_positions,
            tail_loaded: false,
            reading_tail: false,
            rows: Vec::new(),
            num_columns,
            pos: 0,
            positioned: false,
        }
    }

    pub fn empty(num_columns: usize) -> Self {
        Self::new(CapturedDelta::empty(), num_columns)
    }

    /// Position at the first row. Returns false when the delta is empty.
    pub fn rewind(&mut self) -> Result<IOResult<bool>> {
        if !self.positioned || self.pending_read.is_none() {
            self.next_segment = 0;
            self.pending_read = None;
            self.tail_loaded = false;
            self.reading_tail = false;
            self.rows.clear();
        }
        self.pos = 0;
        self.positioned = true;
        self.load_next_batch()
    }

    /// Advance to the next row. Returns false when exhausted.
    pub fn next(&mut self) -> Result<IOResult<bool>> {
        turso_assert!(self.positioned, "DeltaCursor::next before rewind");
        let batch_len = if self.reading_tail {
            self.tail_positions.len()
        } else {
            self.rows.len()
        };
        if self.pos < batch_len {
            self.pos += 1;
        }
        if self.pos < batch_len {
            return Ok(IOResult::Done(true));
        }
        self.load_next_batch()
    }

    fn load_next_batch(&mut self) -> Result<IOResult<bool>> {
        loop {
            if let Some(pending) = self.pending_read.take() {
                if !pending.completion.finished() {
                    let completion = pending.completion.clone();
                    self.pending_read = Some(pending);
                    return Ok(IOResult::IO(IOCompletions::Single(completion)));
                }
                if let Some(error) = pending.completion.get_error() {
                    return Err(LimboError::CompletionError(error));
                }
                let segment = &self.spill_segments[self.next_segment];
                self.rows = parse_spilled_delta(
                    pending.buffer.as_slice(),
                    segment,
                    self.table_id.as_ref(),
                )?;
                self.reading_tail = false;
                self.next_segment += 1;
                self.pos = 0;
                if !self.rows.is_empty() {
                    return Ok(IOResult::Done(true));
                }
                continue;
            }

            if self.next_segment < self.spill_segments.len() {
                let segment = &self.spill_segments[self.next_segment];
                if segment.rows == 0
                    || self
                        .table_id
                        .as_ref()
                        .is_none_or(|table_id| !segment.table_ids.contains(table_id))
                {
                    self.next_segment += 1;
                    continue;
                }
                let file = self.file.as_ref().ok_or_else(|| {
                    LimboError::InternalError("spilled view delta has no backing file".to_string())
                })?;
                let buffer = Arc::new(Buffer::new_temporary(segment.len));
                let expected = segment.len;
                let completion = Completion::new_read(buffer.clone(), move |result| {
                    let Ok((_buffer, bytes_read)) = result else {
                        return None;
                    };
                    if bytes_read as usize != expected {
                        return Some(CompletionError::ShortRead {
                            page_idx: 0,
                            expected,
                            actual: bytes_read as usize,
                        });
                    }
                    None
                });
                let completion = file.pread(segment.offset, completion)?;
                self.pending_read = Some(PendingDeltaRead {
                    completion: completion.clone(),
                    buffer,
                });
                if !completion.finished() {
                    return Ok(IOResult::IO(IOCompletions::Single(completion)));
                }
                continue;
            }

            if !self.tail_loaded {
                self.tail_loaded = true;
                self.reading_tail = true;
                self.pos = 0;
                if !self.tail_positions.is_empty() {
                    return Ok(IOResult::Done(true));
                }
            }
            self.reading_tail = false;
            self.rows.clear();
            self.pos = 0;
            return Ok(IOResult::Done(false));
        }
    }

    fn assert_current(&self) {
        turso_assert!(self.positioned, "DeltaCursor read before rewind");
        let len = if self.reading_tail {
            self.tail_positions.len()
        } else {
            self.rows.len()
        };
        turso_assert!(self.pos < len, "DeltaCursor read past end");
    }

    pub fn rowid(&self) -> i64 {
        self.assert_current();
        if self.reading_tail {
            self.tail[self.tail_positions[self.pos]].rowid
        } else {
            self.rows[self.pos].0
        }
    }

    pub fn column(&self, idx: usize) -> Value {
        self.assert_current();
        let (values, weight) = if self.reading_tail {
            let event = &self.tail[self.tail_positions[self.pos]];
            (&event.values, event.weight)
        } else {
            let (_, values, weight) = &self.rows[self.pos];
            (values, *weight)
        };
        if idx == self.num_columns {
            return Value::from_i64(weight as i64);
        }
        let num_columns = self.num_columns;
        turso_assert!(
            idx < num_columns,
            "DeltaCursor column {idx} out of range ({num_columns} columns + weight)"
        );
        turso_assert!(
            values.len() == num_columns,
            "captured row image width mismatch",
            { "actual": values.len(), "expected": num_columns }
        );
        values[idx].clone()
    }
}

fn parse_spilled_delta(
    bytes: &[u8],
    segment: &SpillSegment,
    table_id: Option<&TableChangeId>,
) -> Result<Vec<(i64, Vec<Value>, isize)>> {
    let mut rows = Vec::new();
    let mut offset = 0;
    for _ in 0..segment.rows {
        let size_bytes = bytes.get(offset..offset + 8).ok_or_else(|| {
            LimboError::Corrupt("truncated materialized-view delta frame".to_string())
        })?;
        let payload_len = u64::from_le_bytes(size_bytes.try_into().expect("eight-byte slice"));
        let payload_len = usize::try_from(payload_len).map_err(|_| {
            LimboError::Corrupt("oversized materialized-view delta frame".to_string())
        })?;
        offset += 8;
        let payload = bytes.get(offset..offset + payload_len).ok_or_else(|| {
            LimboError::Corrupt("truncated materialized-view delta record".to_string())
        })?;
        offset += payload_len;
        let mut record = ImmutableRecord::new(payload_len)?;
        record.start_serialization(payload)?;
        let mut values = record.get_values_owned()?;
        if values.len() < 3 {
            return Err(LimboError::Corrupt(
                "materialized-view delta record lacks table identity, rowid, or weight".to_string(),
            ));
        }
        let event_table_id = match values.remove(0) {
            Value::Numeric(crate::numeric::Numeric::Integer(root_page)) => {
                TableChangeId::RootPage(root_page)
            }
            #[cfg(test)]
            Value::Text(name) => TableChangeId::Name(name.as_str().to_string()),
            _ => {
                return Err(LimboError::Corrupt(
                    "materialized-view delta table identity has an invalid type".to_string(),
                ));
            }
        };
        let rowid = match values.remove(0) {
            Value::Numeric(crate::numeric::Numeric::Integer(value)) => value,
            _ => {
                return Err(LimboError::Corrupt(
                    "materialized-view delta rowid is not an integer".to_string(),
                ));
            }
        };
        let weight = match values.remove(0) {
            Value::Numeric(crate::numeric::Numeric::Integer(value)) => value as isize,
            _ => {
                return Err(LimboError::Corrupt(
                    "materialized-view delta weight is not an integer".to_string(),
                ));
            }
        };
        if Some(&event_table_id) == table_id {
            rows.push((rowid, values, weight));
        }
    }
    Ok(rows)
}
