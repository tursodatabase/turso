use std::sync::{Arc, Mutex};

use turso_core::{io::FileSyncType, Buffer, Completion, EncryptionContext, File};

use crate::{
    errors::Error,
    sync_cipher::{
        ChangeCounts, ChangeIdentity, ChangeKind, CHUNK_SIZE, FILE_HEADER_SIZE, RECORD_HEADER_SIZE,
    },
    types::{Coro, SyncEngineIoResult},
    Result,
};

const READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SealedChanges {
    identity: ChangeIdentity,
    kind: ChangeKind,
    counts: ChangeCounts,
    size: u64,
}

impl SealedChanges {
    pub(crate) fn kind(&self) -> ChangeKind {
        self.kind
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.counts.records == 0
    }
}

enum State {
    Invalid,
    Writing {
        identity: ChangeIdentity,
        kind: ChangeKind,
        counts: ChangeCounts,
        offset: u64,
    },
    Sealed(SealedChanges),
}

pub(crate) struct ChangesFile {
    file: Arc<dyn File>,
    crypto: Option<EncryptionContext>,
    state: State,
    pending: Option<Completion>,
}

impl ChangesFile {
    pub(crate) fn new(file: Arc<dyn File>, crypto: Option<EncryptionContext>) -> Self {
        Self {
            file,
            crypto,
            state: State::Invalid,
            pending: None,
        }
    }

    pub(crate) fn raw_file(&self) -> &Arc<dyn File> {
        &self.file
    }

    pub(crate) fn sealed(&self) -> Result<SealedChanges> {
        match &self.state {
            State::Sealed(sealed) => Ok(sealed.clone()),
            _ => Err(invalid("change download is not sealed")),
        }
    }

    pub(crate) async fn validate_boundary<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<bool> {
        let sealed = self.sealed()?;
        let mut reader = self.open_reader(coro, &sealed).await?;
        if sealed.is_empty() {
            assert!(reader.next_record(coro).await?.is_none());
        }
        Ok(sealed.is_empty())
    }

    pub(crate) async fn prepare_legacy<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<()> {
        if self.crypto.is_some() {
            return Err(invalid("encrypted staging does not support legacy sync"));
        }
        self.state = State::Invalid;
        self.wait_pending(coro).await
    }

    #[cfg(test)]
    pub(crate) fn from_plaintext_test_file(
        file: Arc<dyn File>,
        kind: ChangeKind,
        records: usize,
    ) -> Self {
        let size = file.size().expect("test file size");
        let sealed = SealedChanges {
            identity: ChangeIdentity::new(kind).expect("test identity"),
            kind,
            counts: ChangeCounts {
                records: records as u64,
                bytes: size,
            },
            size,
        };
        Self {
            file,
            crypto: None,
            state: State::Sealed(sealed),
            pending: None,
        }
    }

    pub(crate) async fn reset<Ctx>(&mut self, coro: &Coro<Ctx>, kind: ChangeKind) -> Result<()> {
        self.state = State::Invalid;
        self.wait_pending(coro).await?;
        let identity = ChangeIdentity::new(kind)?;
        self.pending = Some(self.file.truncate(0, Completion::new_trunc(|_| {}))?);
        self.wait_pending(coro).await?;
        let offset = if self.crypto.is_some() {
            self.write(coro, 0, identity.header().to_vec()).await?;
            FILE_HEADER_SIZE as u64
        } else {
            0
        };
        self.state = State::Writing {
            identity,
            kind,
            counts: ChangeCounts::default(),
            offset,
        };
        Ok(())
    }

    pub(crate) async fn append_record<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        bytes: &[u8],
    ) -> Result<()> {
        let State::Writing {
            identity,
            kind,
            mut counts,
            mut offset,
        } = std::mem::replace(&mut self.state, State::Invalid)
        else {
            return Err(invalid("change download is not writable"));
        };
        let record_header = identity.record_header(bytes.len() as u64)?;
        let record_index = counts.records;
        counts.append(bytes.len() as u64)?;
        if let Some(crypto) = self.crypto.clone() {
            self.write(coro, offset, record_header.to_vec()).await?;
            offset = checked_add(offset, RECORD_HEADER_SIZE)?;
            for (index, chunk) in bytes.chunks(CHUNK_SIZE).enumerate() {
                let stored = identity.seal_chunk(
                    &crypto,
                    record_index,
                    &record_header,
                    index as u64,
                    chunk,
                )?;
                let length = stored.len();
                self.write(coro, offset, stored).await?;
                offset = checked_add(offset, length)?;
            }
        } else {
            self.write(coro, offset, bytes.to_vec()).await?;
            offset = checked_add(offset, bytes.len())?;
        }
        self.state = State::Writing {
            identity,
            kind,
            counts,
            offset,
        };
        Ok(())
    }

    pub(crate) async fn finish<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<SealedChanges> {
        let State::Writing {
            identity,
            kind,
            counts,
            mut offset,
        } = std::mem::replace(&mut self.state, State::Invalid)
        else {
            return Err(invalid("change download is not writable"));
        };
        if let Some(crypto) = &self.crypto {
            let end = identity.seal_end(crypto, counts)?;
            let length = end.len();
            self.write(coro, offset, end).await?;
            offset = checked_add(offset, length)?;
        }
        self.pending = Some(
            self.file
                .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?,
        );
        self.wait_pending(coro).await?;
        let sealed = SealedChanges {
            identity,
            kind,
            counts,
            size: offset,
        };
        self.state = State::Sealed(sealed.clone());
        Ok(sealed)
    }

    pub(crate) async fn open_reader<'a, Ctx>(
        &'a mut self,
        coro: &Coro<Ctx>,
        sealed: &SealedChanges,
    ) -> Result<ChangesReader<'a>> {
        if !matches!(&self.state, State::Sealed(current) if current == sealed) {
            return Err(invalid(
                "change download is not sealed or has been replaced",
            ));
        }
        self.state = State::Invalid;
        if self.file.size()? != sealed.size {
            return Err(invalid("change file length has changed"));
        }
        let (offset, end_offset) = if let Some(crypto) = self.crypto.clone() {
            let header = self.read_exact(coro, 0, FILE_HEADER_SIZE).await?;
            sealed.identity.validate_header(&header)?;
            let end_size = ChangeIdentity::end_size(&crypto);
            let end_offset = sealed
                .size
                .checked_sub(end_size as u64)
                .filter(|&offset| offset >= FILE_HEADER_SIZE as u64)
                .ok_or_else(|| invalid("truncated change file"))?;
            let end = self.read_exact(coro, end_offset, end_size).await?;
            sealed.identity.validate_end(&crypto, &end, sealed.counts)?;
            (FILE_HEADER_SIZE as u64, end_offset)
        } else {
            (0, sealed.size)
        };
        self.state = State::Sealed(sealed.clone());
        Ok(ChangesReader {
            file: self,
            sealed: sealed.clone(),
            offset,
            end_offset,
            counts: ChangeCounts::default(),
            finished: false,
            plaintext_buffer: Vec::new(),
            plaintext_buffer_pos: 0,
        })
    }

    async fn write<Ctx>(&mut self, coro: &Coro<Ctx>, offset: u64, bytes: Vec<u8>) -> Result<()> {
        self.wait_pending(coro).await?;
        checked_add(offset, bytes.len())?;
        let buffer = Arc::new(Buffer::new(bytes));
        let expected = buffer.len();
        let written = Arc::new(Mutex::new(None));
        let result = written.clone();
        // The completion owns a buffer reference even if the coroutine is dropped.
        let retained = buffer.clone();
        self.pending = Some(self.file.pwrite(
            offset,
            buffer,
            Completion::new_write(move |rc| {
                let _ = &retained;
                *result.lock().expect("write result") = Some(rc);
            }),
        )?);
        self.wait_pending(coro).await?;
        let written = written.lock().expect("write result").take();
        match written {
            Some(Ok(n)) if n >= 0 && n as usize == expected => Ok(()),
            _ => Err(invalid("short write to change file")),
        }
    }

    async fn read_exact<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        mut offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        self.wait_pending(coro).await?;
        let mut bytes = Vec::new();
        while bytes.len() < length {
            let wanted = (length - bytes.len()).min(READ_BUFFER_SIZE);
            let buffer = Arc::new(Buffer::new_temporary(wanted));
            let read = Arc::new(Mutex::new(None));
            let result = read.clone();
            self.pending = Some(self.file.pread(
                offset,
                Completion::new_read(buffer.clone(), move |rc| {
                    *result.lock().expect("read result") = Some(rc.map(|(_, n)| n));
                    None
                }),
            )?);
            self.wait_pending(coro).await?;
            let n = match read.lock().expect("read result").take() {
                Some(Ok(n)) if n > 0 && n as usize <= wanted => n as usize,
                _ => return Err(invalid("unexpected EOF in change file")),
            };
            bytes.extend_from_slice(&buffer.as_slice()[..n]);
            offset = checked_add(offset, n)?;
        }
        Ok(bytes)
    }

    async fn wait_pending<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<()> {
        let Some(completion) = self.pending.as_ref() else {
            return Ok(());
        };
        while !completion.finished() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        let error = completion.get_error();
        self.pending = None;
        if let Some(error) = error {
            return Err(Error::IoError(error.to_string()));
        }
        Ok(())
    }
}

pub(crate) struct ChangesReader<'a> {
    file: &'a mut ChangesFile,
    sealed: SealedChanges,
    offset: u64,
    end_offset: u64,
    counts: ChangeCounts,
    finished: bool,
    plaintext_buffer: Vec<u8>,
    plaintext_buffer_pos: usize,
}

impl ChangesReader<'_> {
    pub(crate) async fn next_record<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<Option<Vec<u8>>> {
        if self.finished {
            return Ok(None);
        }
        if !matches!(self.file.state, State::Sealed(_)) {
            return Err(invalid(
                "change reader failed; reset the download before reuse",
            ));
        }
        self.file.state = State::Invalid;
        let result = self.read_record(coro).await;
        if result.is_ok() {
            self.file.state = State::Sealed(self.sealed.clone());
        }
        result
    }

    async fn read_record<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<Option<Vec<u8>>> {
        if self.counts.records == self.sealed.counts.records {
            if self.offset != self.end_offset
                || self.counts != self.sealed.counts
                || self.file.file.size()? != self.sealed.size
            {
                return Err(invalid("change file end does not match downloaded records"));
            }
            if let Some(crypto) = self.file.crypto.clone() {
                let bytes = self
                    .file
                    .read_exact(coro, self.offset, ChangeIdentity::end_size(&crypto))
                    .await?;
                self.sealed
                    .identity
                    .validate_end(&crypto, &bytes, self.counts)?;
            }
            self.finished = true;
            return Ok(None);
        }
        let bytes = if let Some(crypto) = self.file.crypto.clone() {
            let header_end = checked_add(self.offset, RECORD_HEADER_SIZE)?;
            if header_end > self.end_offset {
                return Err(invalid("truncated change record header"));
            }
            let header = self
                .file
                .read_exact(coro, self.offset, RECORD_HEADER_SIZE)
                .await?;
            let header: [u8; RECORD_HEADER_SIZE] =
                header.try_into().expect("read exact record header");
            let length = self.sealed.identity.record_length(&header)?;
            let chunks = length.div_ceil(CHUNK_SIZE as u64);
            let stored_length = chunks
                .checked_mul((crypto.tag_size() + crypto.nonce_size()) as u64)
                .and_then(|overhead| overhead.checked_add(length))
                .and_then(|size| header_end.checked_add(size))
                .filter(|&end| end <= self.end_offset)
                .ok_or_else(|| invalid("change record exceeds file boundary"))?;
            let mut bytes = Vec::new();
            self.offset = header_end;
            for index in 0..chunks {
                let size = self.sealed.identity.chunk_plaintext_size(&header, index)?
                    + crypto.tag_size()
                    + crypto.nonce_size();
                let stored = self.file.read_exact(coro, self.offset, size).await?;
                let plaintext = self.sealed.identity.open_chunk(
                    &crypto,
                    self.counts.records,
                    &header,
                    index,
                    &stored,
                )?;
                bytes.extend_from_slice(&plaintext);
                self.offset = checked_add(self.offset, size)?;
            }
            assert_eq!(self.offset, stored_length);
            bytes
        } else {
            let mut prefix = Vec::new();
            let length = match self.sealed.kind {
                ChangeKind::Pages => 4120,
                ChangeKind::Logical => {
                    let mut length = 0u64;
                    for shift in (0..70).step_by(7) {
                        if self.offset >= self.end_offset {
                            return Err(invalid("truncated change protobuf length"));
                        }
                        self.read_plaintext(coro, 1, &mut prefix).await?;
                        let byte = *prefix.last().expect("read delimiter byte");
                        if shift == 63 && byte > 1 {
                            return Err(invalid("change protobuf length overflow"));
                        }
                        length |= u64::from(byte & 0x7f) << shift;
                        if byte & 0x80 == 0 {
                            break;
                        }
                    }
                    length
                }
            };
            if self
                .offset
                .checked_add(length)
                .filter(|&end| end <= self.end_offset)
                .is_none()
            {
                return Err(invalid("change record exceeds file boundary"));
            }
            let length =
                usize::try_from(length).map_err(|_| invalid("change record length overflow"))?;
            self.read_plaintext(coro, length, &mut prefix).await?;
            prefix
        };
        self.counts.append(bytes.len() as u64)?;
        if self.counts.bytes > self.sealed.counts.bytes {
            return Err(invalid("change records exceed downloaded byte count"));
        }
        Ok(Some(bytes))
    }

    async fn read_plaintext<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        mut length: usize,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        while length > 0 {
            if self.plaintext_buffer_pos == self.plaintext_buffer.len() {
                let wanted = (self.end_offset - self.offset).min(READ_BUFFER_SIZE as u64) as usize;
                if wanted == 0 {
                    return Err(invalid("unexpected EOF in change file"));
                }
                self.plaintext_buffer = self.file.read_exact(coro, self.offset, wanted).await?;
                self.plaintext_buffer_pos = 0;
            }
            let count = length.min(self.plaintext_buffer.len() - self.plaintext_buffer_pos);
            let end = self.plaintext_buffer_pos + count;
            output.extend_from_slice(&self.plaintext_buffer[self.plaintext_buffer_pos..end]);
            self.plaintext_buffer_pos = end;
            self.offset = checked_add(self.offset, count)?;
            length -= count;
        }
        Ok(())
    }
}

fn checked_add(offset: u64, length: usize) -> Result<u64> {
    offset
        .checked_add(length as u64)
        .ok_or_else(|| invalid("change file offset overflow"))
}

fn invalid(message: &str) -> Error {
    Error::DatabaseSyncEngineError(message.into())
}
