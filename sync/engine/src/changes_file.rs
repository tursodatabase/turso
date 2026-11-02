use std::sync::{Arc, Mutex};

use turso_core::{Buffer, Completion, EncryptionContext, File};

use crate::{
    errors::Error,
    sync_cipher::{ChangeCounts, ChangeIdentity, ChangeKind, CHUNK_SIZE},
    types::{Coro, SyncEngineIoResult},
    Result,
};

const READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SealedChanges {
    identity: ChangeIdentity,
    counts: ChangeCounts,
    size: u64,
}

impl SealedChanges {
    pub(crate) fn kind(&self) -> ChangeKind {
        self.identity.kind()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.counts.records == 0
    }
}

enum State {
    Invalid,
    Writing {
        identity: ChangeIdentity,
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
    write_result: Arc<Mutex<Option<i32>>>,
}

impl ChangesFile {
    pub(crate) fn new(file: Arc<dyn File>, crypto: Option<EncryptionContext>) -> Self {
        Self {
            file,
            crypto,
            state: State::Invalid,
            pending: None,
            write_result: Arc::new(Mutex::new(None)),
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
        let mut reader = self.open_reader(coro, None).await?;
        let empty = reader.is_empty();
        if empty {
            assert!(reader.next_record(coro).await?.is_none());
        }
        Ok(empty)
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
        plaintext_bytes: usize,
    ) -> Self {
        let size = file.size().expect("test file size");
        let sealed = SealedChanges {
            identity: ChangeIdentity::new(kind).expect("test identity"),
            counts: ChangeCounts {
                records: records as u64,
                bytes: plaintext_bytes as u64,
            },
            size,
        };
        Self {
            file,
            crypto: None,
            state: State::Sealed(sealed),
            pending: None,
            write_result: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn reset<Ctx>(&mut self, coro: &Coro<Ctx>, kind: ChangeKind) -> Result<()> {
        self.state = State::Invalid;
        self.wait_pending(coro).await?;
        let identity = ChangeIdentity::new(kind)?;
        self.pending = Some(self.file.truncate(0, Completion::new_trunc(|_| {}))?);
        self.wait_pending(coro).await?;
        let offset = 0;
        self.state = State::Writing {
            identity,
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
            mut counts,
            mut offset,
        } = std::mem::replace(&mut self.state, State::Invalid)
        else {
            return Err(invalid("change download is not writable"));
        };
        let length = bytes.len() as u64;
        identity.validate_record_length(length)?;
        let record_index = counts.records;
        counts.append(bytes.len() as u64)?;
        let prefix_len = if identity.kind() == ChangeKind::Logical {
            prost::length_delimiter_len(bytes.len())
        } else {
            0
        };
        let capacity = if self.crypto.is_none() {
            bytes
                .len()
                .checked_add(prefix_len)
                .ok_or_else(|| invalid("change record length overflow"))?
        } else {
            prefix_len
        };
        let mut buffer = Vec::with_capacity(capacity);
        if identity.kind() == ChangeKind::Logical {
            prost::encode_length_delimiter(bytes.len(), &mut buffer)
                .map_err(|e| invalid(&format!("cannot encode change length: {e}")))?;
        }
        if self.crypto.is_some() {
            if prefix_len > 0 {
                self.write(coro, offset, buffer).await?;
                offset = checked_add(offset, prefix_len)?;
            }
            for (index, chunk) in bytes.chunks(CHUNK_SIZE).enumerate() {
                let stored = identity.seal_chunk(
                    self.crypto.as_ref().expect("encrypted change file"),
                    record_index,
                    length,
                    index as u64,
                    chunk,
                )?;
                let length = stored.len();
                self.write(coro, offset, stored).await?;
                offset = checked_add(offset, length)?;
            }
        } else {
            buffer.extend_from_slice(bytes);
            let length = buffer.len();
            self.write(coro, offset, buffer).await?;
            offset = checked_add(offset, length)?;
        }
        self.state = State::Writing {
            identity,
            counts,
            offset,
        };
        Ok(())
    }

    pub(crate) async fn append_page<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        buffer: Arc<Buffer>,
    ) -> Result<()> {
        if !matches!(
            &self.state,
            State::Writing { identity, .. } if identity.kind() == ChangeKind::Pages
        ) {
            self.state = State::Invalid;
            return Err(invalid("change download is not writable for pages"));
        }
        if self.crypto.is_some() {
            return self.append_record(coro, buffer.as_slice()).await;
        }
        let State::Writing {
            identity,
            mut counts,
            offset,
        } = std::mem::replace(&mut self.state, State::Invalid)
        else {
            unreachable!("page download must be writable");
        };
        identity.validate_record_length(buffer.len() as u64)?;
        counts.append(buffer.len() as u64)?;
        let end = checked_add(offset, buffer.len())?;
        self.write_buffer(coro, offset, buffer).await?;
        self.state = State::Writing {
            identity,
            counts,
            offset: end,
        };
        Ok(())
    }

    /// Seal only in memory: this scratch batch is abandoned after restart, so
    /// completed writes must be visible to readers but need no durability flush.
    pub(crate) async fn finish<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<SealedChanges> {
        let State::Writing {
            identity,
            counts,
            offset,
        } = std::mem::replace(&mut self.state, State::Invalid)
        else {
            return Err(invalid("change download is not writable"));
        };
        self.wait_pending(coro).await?;
        let sealed = SealedChanges {
            identity,
            counts,
            size: offset,
        };
        self.state = State::Sealed(sealed.clone());
        Ok(sealed)
    }

    pub(crate) async fn open_reader<'a, Ctx>(
        &'a mut self,
        coro: &Coro<Ctx>,
        expected_kind: Option<ChangeKind>,
    ) -> Result<ChangesReader<'a>> {
        let sealed = self.sealed()?;
        if expected_kind.is_some_and(|kind| kind != sealed.kind()) {
            return Err(invalid("unexpected change record kind"));
        }
        self.state = State::Invalid;
        if self.file.size()? != sealed.size {
            return Err(invalid("change file length has changed"));
        }
        self.wait_pending(coro).await?;
        self.state = State::Sealed(sealed.clone());
        Ok(ChangesReader {
            file: self,
            sealed,
            offset: 0,
            counts: ChangeCounts::default(),
            finished: false,
            read_buffer: Vec::new(),
            read_buffer_pos: 0,
        })
    }

    async fn write<Ctx>(&mut self, coro: &Coro<Ctx>, offset: u64, bytes: Vec<u8>) -> Result<()> {
        self.write_buffer(coro, offset, Arc::new(Buffer::new(bytes)))
            .await
    }

    async fn write_buffer<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        offset: u64,
        buffer: Arc<Buffer>,
    ) -> Result<()> {
        self.wait_pending(coro).await?;
        checked_add(offset, buffer.len())?;
        let expected = buffer.len();
        *self.write_result.lock().expect("write result") = None;
        let result = self.write_result.clone();
        // The completion owns a buffer reference even if the coroutine is dropped.
        let retained = buffer.clone();
        self.pending = Some(self.file.pwrite(
            offset,
            buffer,
            Completion::new_write(move |rc| {
                let _ = &retained;
                *result.lock().expect("write result") = rc.ok();
            }),
        )?);
        self.wait_pending(coro).await?;
        let written = self.write_result.lock().expect("write result").take();
        match written {
            Some(n) if n >= 0 && n as usize == expected => Ok(()),
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
    counts: ChangeCounts,
    finished: bool,
    read_buffer: Vec<u8>,
    read_buffer_pos: usize,
}

impl ChangesReader<'_> {
    pub(crate) fn is_empty(&self) -> bool {
        self.sealed.is_empty()
    }

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
            if self.offset != self.sealed.size
                || self.counts != self.sealed.counts
                || self.file.file.size()? != self.sealed.size
            {
                return Err(invalid("change file end does not match downloaded records"));
            }
            self.finished = true;
            return Ok(None);
        }
        let length = match self.sealed.kind() {
            ChangeKind::Pages => 4120,
            ChangeKind::Logical => self.read_length(coro).await?,
        };
        self.sealed.identity.validate_record_length(length)?;
        if length
            > self
                .sealed
                .counts
                .bytes
                .checked_sub(self.counts.bytes)
                .ok_or_else(|| invalid("change byte count overflow"))?
        {
            return Err(invalid("change records exceed downloaded byte count"));
        }
        let chunks = length.div_ceil(CHUNK_SIZE as u64);
        let overhead = self
            .file
            .crypto
            .as_ref()
            .map_or(0, |c| c.tag_size() + c.nonce_size());
        let stored_length = chunks
            .checked_mul(overhead as u64)
            .and_then(|n| n.checked_add(length))
            .ok_or_else(|| invalid("change record length overflow"))?;
        if self
            .offset
            .checked_add(stored_length)
            .filter(|&end| end <= self.sealed.size)
            .is_none()
        {
            return Err(invalid("change record exceeds file boundary"));
        }
        let length_usize =
            usize::try_from(length).map_err(|_| invalid("change record length overflow"))?;
        let mut bytes = Vec::new();
        if self.file.crypto.is_some() {
            for index in 0..chunks {
                let size = self.sealed.identity.chunk_plaintext_size(length, index)? + overhead;
                let mut stored = Vec::new();
                self.read_buffered(coro, size, &mut stored).await?;
                bytes.extend(self.sealed.identity.open_chunk(
                    self.file.crypto.as_ref().expect("encrypted change file"),
                    self.counts.records,
                    length,
                    index,
                    &stored,
                )?);
            }
        } else {
            self.read_buffered(coro, length_usize, &mut bytes).await?;
        }
        self.counts.append(bytes.len() as u64)?;
        if self.counts.bytes > self.sealed.counts.bytes {
            return Err(invalid("change records exceed downloaded byte count"));
        }
        Ok(Some(bytes))
    }

    async fn read_length<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<u64> {
        let mut length = 0u64;
        for index in 0..10 {
            let byte = self.read_byte(coro).await?;
            if index == 9 && byte > 1 {
                return Err(invalid("change protobuf length overflow"));
            }
            length |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                if length == 0 || (index > 0 && byte == 0) {
                    return Err(invalid("noncanonical or zero change protobuf length"));
                }
                return Ok(length);
            }
        }
        Err(invalid("unterminated change protobuf length"))
    }

    async fn read_byte<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<u8> {
        self.refill(coro).await?;
        let byte = self.read_buffer[self.read_buffer_pos];
        self.read_buffer_pos += 1;
        self.offset = checked_add(self.offset, 1)?;
        Ok(byte)
    }

    async fn read_buffered<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        mut length: usize,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        while length > 0 {
            self.refill(coro).await?;
            let count = length.min(self.read_buffer.len() - self.read_buffer_pos);
            let end = self.read_buffer_pos + count;
            output.extend_from_slice(&self.read_buffer[self.read_buffer_pos..end]);
            self.read_buffer_pos = end;
            self.offset = checked_add(self.offset, count)?;
            length -= count;
        }
        Ok(())
    }

    async fn refill<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<()> {
        if self.read_buffer_pos == self.read_buffer.len() {
            let wanted = (self.sealed.size - self.offset).min(READ_BUFFER_SIZE as u64) as usize;
            if wanted == 0 {
                return Err(invalid("unexpected EOF in change file"));
            }
            self.read_buffer = self.file.read_exact(coro, self.offset, wanted).await?;
            self.read_buffer_pos = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use genawaiter::{sync::Gen, GeneratorState};
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use turso_core::{io::FileSyncType, CipherMode, EncryptionKey, MemoryIO, OpenFlags, IO};

    fn crypto() -> EncryptionContext {
        EncryptionContext::new(
            CipherMode::Aes256Gcm,
            &EncryptionKey::new_256([3; 32]),
            4096,
        )
        .expect("crypto")
    }

    fn run<T, F: Future<Output = Result<T>>>(f: impl FnOnce(Coro<()>) -> F) -> Result<T> {
        let mut gen = Gen::new(|co| f(co.into()));
        match gen.resume_with(Ok(())) {
            GeneratorState::Complete(result) => result,
            GeneratorState::Yielded(_) => panic!("MemoryIO should complete inline"),
        }
    }

    fn memory_file() -> Arc<dyn File> {
        MemoryIO::new()
            .open_file("test-changes", OpenFlags::Create, false)
            .expect("file")
    }

    #[test]
    fn roundtrip_records_empty_downloads_and_slot_reuse() {
        for encrypted in [false, true] {
            let file = memory_file();
            let mut changes = ChangesFile::new(file.clone(), encrypted.then(crypto));
            run(|coro| async move {
                changes.reset(&coro, ChangeKind::Logical).await?;
                let empty = changes.finish(&coro).await?;
                assert!(empty.is_empty());
                assert_eq!(file.size()?, 0);
                assert!(changes
                    .open_reader(&coro, Some(empty.kind()))
                    .await?
                    .next_record(&coro)
                    .await?
                    .is_none());
                assert!(changes.finish(&coro).await.is_err());
                changes.reset(&coro, ChangeKind::Logical).await?;
                let mut records = Vec::new();
                for size in [1, 2, 32766, 32768, 100_000] {
                    let record = vec![42; size];
                    changes.append_record(&coro, &record).await?;
                    records.push(record);
                }
                let batch = changes.finish(&coro).await?;
                assert!(!batch.is_empty());
                assert!(changes
                    .open_reader(&coro, Some(ChangeKind::Pages))
                    .await
                    .is_err());
                let mut reader = changes.open_reader(&coro, Some(batch.kind())).await?;
                for expected in records {
                    assert_eq!(reader.next_record(&coro).await?, Some(expected));
                }
                assert!(reader.next_record(&coro).await?.is_none());
                assert!(reader.next_record(&coro).await?.is_none());
                drop(reader);
                changes.reset(&coro, ChangeKind::Pages).await?;
                changes.append_record(&coro, &[7; 4120]).await?;
                let pages = changes.finish(&coro).await?;
                let mut reader = changes.open_reader(&coro, Some(pages.kind())).await?;
                assert_eq!(reader.next_record(&coro).await?, Some(vec![7; 4120]));
                assert!(reader.next_record(&coro).await?.is_none());
                Ok(())
            })
            .expect("roundtrip");
        }
    }

    #[test]
    fn logical_disk_format_has_one_delimiter_and_message_only_counts() {
        for encrypted in [false, true] {
            let file = memory_file();
            let mut changes = ChangesFile::new(file.clone(), encrypted.then(crypto));
            run(|coro| async move {
                changes.reset(&coro, ChangeKind::Logical).await?;
                let message = vec![42; CHUNK_SIZE + 1];
                changes.append_record(&coro, &message).await?;
                let sealed = changes.finish(&coro).await?;
                let prefix_len = prost::length_delimiter_len(message.len());
                assert_eq!(
                    sealed.counts,
                    ChangeCounts {
                        records: 1,
                        bytes: message.len() as u64
                    }
                );
                assert_eq!(
                    sealed.size as usize,
                    prefix_len + message.len() + if encrypted { 2 * 28 } else { 0 }
                );
                let stored = changes.read_exact(&coro, 0, sealed.size as usize).await?;
                let mut input = stored.as_slice();
                assert_eq!(
                    prost::decode_length_delimiter(&mut input).unwrap(),
                    message.len()
                );
                if encrypted {
                    // Decryption returns message bytes, with no nested delimiter.
                    let first = sealed.identity.open_chunk(
                        &crypto(),
                        0,
                        message.len() as u64,
                        0,
                        &input[..CHUNK_SIZE + 28],
                    )?;
                    assert_eq!(first, message[..CHUNK_SIZE]);
                } else {
                    assert_eq!(input, message);
                }
                let mut reopened = ChangesFile::new(file, encrypted.then(crypto));
                assert!(reopened.sealed().is_err());
                assert!(reopened
                    .open_reader(&coro, Some(sealed.kind()))
                    .await
                    .is_err());
                reopened.reset(&coro, ChangeKind::Logical).await?;
                assert!(reopened.finish(&coro).await?.is_empty());
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn encrypted_records_reject_reordering_and_old_batch_substitution() {
        for kind in [ChangeKind::Logical, ChangeKind::Pages] {
            for old_batch in [false, true] {
                let mut changes = ChangesFile::new(memory_file(), Some(crypto()));
                run(|coro| async move {
                    let length = if kind == ChangeKind::Pages { 4120 } else { 100 };
                    changes.reset(&coro, kind).await?;
                    changes.append_record(&coro, &vec![1; length]).await?;
                    changes.append_record(&coro, &vec![2; length]).await?;
                    let first = changes.finish(&coro).await?;
                    let stored = changes.read_exact(&coro, 0, first.size as usize).await?;
                    let sealed = if old_batch {
                        changes.reset(&coro, kind).await?;
                        changes.append_record(&coro, &vec![1; length]).await?;
                        changes.append_record(&coro, &vec![2; length]).await?;
                        let sealed = changes.finish(&coro).await?;
                        changes.write(&coro, 0, stored).await?;
                        sealed
                    } else {
                        let half = stored.len() / 2;
                        changes
                            .write(&coro, 0, [&stored[half..], &stored[..half]].concat())
                            .await?;
                        first
                    };
                    let mut reader = changes.open_reader(&coro, Some(sealed.kind())).await?;
                    assert!(matches!(
                        reader.next_record(&coro).await,
                        Err(Error::AuthenticationFailed)
                    ));
                    Ok(())
                })
                .unwrap();
            }
        }
    }

    #[test]
    fn corrupt_payload_truncation_and_trailing_bytes_fail() {
        for damage in [0, 1, 2] {
            let file = memory_file();
            let mut changes = ChangesFile::new(file.clone(), Some(crypto()));
            run(|coro| async move {
                changes.reset(&coro, ChangeKind::Logical).await?;
                changes.append_record(&coro, &[9; 100]).await?;
                let sealed = changes.finish(&coro).await?;
                match damage {
                    0 => {
                        let mut reader = changes.open_reader(&coro, Some(sealed.kind())).await?;
                        let pos = 1;
                        let original = reader.file.read_exact(&coro, pos, 1).await?[0];
                        reader.file.write(&coro, pos, vec![original ^ 1]).await?;
                        assert!(matches!(
                            reader.next_record(&coro).await,
                            Err(Error::AuthenticationFailed)
                        ));
                        assert!(reader.next_record(&coro).await.is_err());
                    }
                    1 => {
                        let c = file.truncate(sealed.size - 1, Completion::new_trunc(|_| {}))?;
                        assert!(c.succeeded());
                        assert!(changes
                            .open_reader(&coro, Some(sealed.kind()))
                            .await
                            .is_err());
                    }
                    2 => {
                        changes.write(&coro, sealed.size, vec![0]).await?;
                        assert!(changes
                            .open_reader(&coro, Some(sealed.kind()))
                            .await
                            .is_err());
                    }
                    _ => unreachable!(),
                }
                Ok(())
            })
            .expect("tamper test");
        }
    }

}
