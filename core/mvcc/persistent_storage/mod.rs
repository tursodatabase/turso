use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::mvcc::database::LogRecord;
use crate::{LimboError, Result};

const LOG_MAGIC: &[u8] = b"MVCCLOG1";
const LOG_RECORD_HEADER_SIZE: usize = 16; // 8 bytes for length + 8 bytes for checksum

#[derive(Debug)]
pub enum Storage {
    Noop,
    LogicalWal {
        writer: Arc<parking_lot::Mutex<BufWriter<File>>>,
        path: String,
        current_lsn: Arc<AtomicU64>,
    },
}

impl Storage {
    pub fn new_noop() -> Self {
        Self::Noop
    }

    pub fn new_logical_wal(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|e| LimboError::InternalError(format!("Failed to open log file: {}", e)))?;

        // Check if this is a new file and write magic header
        let metadata = file.metadata().map_err(|e| {
            LimboError::InternalError(format!("Failed to get file metadata: {}", e))
        })?;

        let mut writer = BufWriter::new(file);
        let current_lsn = if metadata.len() == 0 {
            // New file, write magic header
            writer.write_all(LOG_MAGIC).map_err(|e| {
                LimboError::InternalError(format!("Failed to write magic header: {}", e))
            })?;
            writer
                .flush()
                .map_err(|e| LimboError::InternalError(format!("Failed to flush: {}", e)))?;
            // Sync all data to disk after magic header
            writer.get_ref().sync_all()
                .map_err(|e| LimboError::InternalError(format!("Failed to sync: {}", e)))?;
            LOG_MAGIC.len() as u64
        } else {
            // Existing file, validate magic and find current LSN
            let mut reader =
                BufReader::new(OpenOptions::new().read(true).open(path).map_err(|e| {
                    LimboError::InternalError(format!("Failed to open for reading: {}", e))
                })?);

            let mut magic = [0u8; 8];
            reader
                .read_exact(&mut magic)
                .map_err(|e| LimboError::InternalError(format!("Failed to read magic: {}", e)))?;

            if &magic != LOG_MAGIC {
                return Err(LimboError::InternalError(
                    "Invalid log file magic header".to_string(),
                ));
            }

            metadata.len()
        };

        Ok(Self::LogicalWal {
            writer: Arc::new(parking_lot::Mutex::new(writer)),
            path: path.to_string(),
            current_lsn: Arc::new(AtomicU64::new(current_lsn)),
        })
    }
}

impl Storage {
    pub fn log_tx(&self, record: LogRecord) -> Result<()> {
        match self {
            Self::Noop => Ok(()),
            Self::LogicalWal {
                writer,
                current_lsn,
                ..
            } => {
                // Serialize the LogRecord
                let serialized = self.serialize_log_record(&record)?;

                // Calculate checksum
                let checksum = self.calculate_checksum(&serialized);

                // Write record header (length + checksum)
                let length = serialized.len() as u64;
                let mut header = Vec::with_capacity(LOG_RECORD_HEADER_SIZE);
                header.extend_from_slice(&length.to_le_bytes());
                header.extend_from_slice(&checksum.to_le_bytes());

                // Acquire writer lock and write
                let mut w = writer.lock();
                w.write_all(&header).map_err(|e| {
                    LimboError::InternalError(format!("Failed to write header: {}", e))
                })?;
                w.write_all(&serialized).map_err(|e| {
                    LimboError::InternalError(format!("Failed to write record: {}", e))
                })?;
                w.flush()
                    .map_err(|e| LimboError::InternalError(format!("Failed to flush: {}", e)))?;

                // Sync all data to disk
                w.get_ref().sync_all()
                    .map_err(|e| LimboError::InternalError(format!("Failed to sync: {}", e)))?;

                // Update LSN
                let bytes_written = LOG_RECORD_HEADER_SIZE + serialized.len();
                current_lsn.fetch_add(bytes_written as u64, Ordering::SeqCst);

                Ok(())
            }
        }
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        match self {
            Self::Noop => Err(LimboError::InternalError(
                "cannot read from Noop storage".to_string(),
            )),
            Self::LogicalWal { path, .. } => {
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(path)
                    .map_err(|e| LimboError::InternalError(format!("Failed to open log: {}", e)))?;

                // Skip magic header
                file.seek(SeekFrom::Start(LOG_MAGIC.len() as u64))
                    .map_err(|e| LimboError::InternalError(format!("Failed to seek: {}", e)))?;

                let mut records = Vec::new();
                let mut reader = BufReader::new(file);

                loop {
                    // Read header
                    let mut header = [0u8; LOG_RECORD_HEADER_SIZE];
                    match reader.read_exact(&mut header) {
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => {
                            return Err(LimboError::InternalError(format!(
                                "Failed to read header: {}",
                                e
                            )))
                        }
                    }

                    let length = u64::from_le_bytes(header[0..8].try_into().unwrap());
                    let checksum = u64::from_le_bytes(header[8..16].try_into().unwrap());

                    // Read record data
                    let mut data = vec![0u8; length as usize];
                    reader.read_exact(&mut data).map_err(|e| {
                        LimboError::InternalError(format!("Failed to read data: {}", e))
                    })?;

                    // Verify checksum
                    let calculated_checksum = self.calculate_checksum(&data);
                    if calculated_checksum != checksum {
                        return Err(LimboError::InternalError(
                            "Checksum mismatch in log record".to_string(),
                        ));
                    }

                    // Deserialize record
                    let record = self.deserialize_log_record(&data)?;
                    records.push(record);
                }

                Ok(records)
            }
        }
    }

    fn serialize_log_record(&self, record: &LogRecord) -> Result<Vec<u8>> {
        // Simple serialization: tx_timestamp (8 bytes) + row_versions count (8 bytes) + row data
        // For now, we'll just store the tx_timestamp as a placeholder
        // Full serialization of row_versions would require access to RowVersion internals
        let mut data = Vec::new();
        data.extend_from_slice(&record.tx_timestamp.to_le_bytes());
        // TODO: Serialize row_versions when we have access to them
        data.extend_from_slice(&0u64.to_le_bytes()); // row_versions count = 0 for now
        Ok(data)
    }

    fn deserialize_log_record(&self, data: &[u8]) -> Result<LogRecord> {
        if data.len() < 16 {
            return Err(LimboError::InternalError(
                "Invalid log record data".to_string(),
            ));
        }

        let tx_timestamp = u64::from_le_bytes(data[0..8].try_into().unwrap());
        // TODO: Deserialize row_versions when we have the full format

        Ok(LogRecord {
            tx_timestamp,
            row_versions: Vec::new(),
        })
    }

    fn calculate_checksum(&self, data: &[u8]) -> u64 {
        // Simple CRC-like checksum for now
        let mut checksum = 0u64;
        for chunk in data.chunks(8) {
            let mut bytes = [0u8; 8];
            for (i, &b) in chunk.iter().enumerate() {
                bytes[i] = b;
            }
            checksum = checksum.wrapping_add(u64::from_le_bytes(bytes));
            checksum = checksum.rotate_left(1);
        }
        checksum
    }

    pub fn get_current_lsn(&self) -> Option<u64> {
        match self {
            Self::Noop => None,
            Self::LogicalWal { current_lsn, .. } => Some(current_lsn.load(Ordering::SeqCst)),
        }
    }
}
