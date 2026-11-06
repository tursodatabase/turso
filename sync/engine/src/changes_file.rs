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
        plaintext_bytes: usize,
    ) -> Self {
        let size = file.size().expect("test file size");
        let sealed = SealedChanges {
            identity: ChangeIdentity::new(kind).expect("test identity"),
            kind,
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
        let length = bytes.len() as u64;
        identity.validate_record_length(length)?;
        let record_index = counts.records;
        counts.append(bytes.len() as u64)?;
        let prefix_len = if kind == ChangeKind::Logical {
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
        if kind == ChangeKind::Logical {
            prost::encode_length_delimiter(bytes.len(), &mut buffer)
                .map_err(|e| invalid(&format!("cannot encode change length: {e}")))?;
        }
        if let Some(crypto) = self.crypto.clone() {
            if prefix_len > 0 {
                self.write(coro, offset, buffer).await?;
                offset = checked_add(offset, prefix_len)?;
            }
            for (index, chunk) in bytes.chunks(CHUNK_SIZE).enumerate() {
                let stored =
                    identity.seal_chunk(&crypto, record_index, length, index as u64, chunk)?;
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
            kind,
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
            self.state,
            State::Writing {
                kind: ChangeKind::Pages,
                ..
            }
        ) {
            self.state = State::Invalid;
            return Err(invalid("change download is not writable for pages"));
        }
        if self.crypto.is_some() {
            return self.append_record(coro, buffer.as_slice()).await;
        }
        let State::Writing {
            identity,
            kind,
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
            kind,
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
            kind,
            counts,
            offset,
        } = std::mem::replace(&mut self.state, State::Invalid)
        else {
            return Err(invalid("change download is not writable"));
        };
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
        self.wait_pending(coro).await?;
        let (offset, end_offset) = (0, sealed.size);
        self.state = State::Sealed(sealed.clone());
        Ok(ChangesReader {
            file: self,
            sealed: sealed.clone(),
            offset,
            end_offset,
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
    end_offset: u64,
    counts: ChangeCounts,
    finished: bool,
    read_buffer: Vec<u8>,
    read_buffer_pos: usize,
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
            self.finished = true;
            return Ok(None);
        }
        let length = match self.sealed.kind {
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
            .filter(|&end| end <= self.end_offset)
            .is_none()
        {
            return Err(invalid("change record exceeds file boundary"));
        }
        let length_usize =
            usize::try_from(length).map_err(|_| invalid("change record length overflow"))?;
        let mut bytes = Vec::new();
        if let Some(crypto) = self.file.crypto.clone() {
            for index in 0..chunks {
                let size = self.sealed.identity.chunk_plaintext_size(length, index)? + overhead;
                let mut stored = Vec::new();
                self.read_buffered(coro, size, &mut stored).await?;
                bytes.extend(self.sealed.identity.open_chunk(
                    &crypto,
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
        let mut byte = Vec::with_capacity(1);
        for index in 0..10 {
            byte.clear();
            self.read_buffered(coro, 1, &mut byte).await?;
            let byte = byte[0];
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

    async fn read_buffered<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        mut length: usize,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        while length > 0 {
            if self.read_buffer_pos == self.read_buffer.len() {
                let wanted = (self.end_offset - self.offset).min(READ_BUFFER_SIZE as u64) as usize;
                if wanted == 0 {
                    return Err(invalid("unexpected EOF in change file"));
                }
                self.read_buffer = self.file.read_exact(coro, self.offset, wanted).await?;
                self.read_buffer_pos = 0;
            }
            let count = length.min(self.read_buffer.len() - self.read_buffer_pos);
            let end = self.read_buffer_pos + count;
            output.extend_from_slice(&self.read_buffer[self.read_buffer_pos..end]);
            self.read_buffer_pos = end;
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
                    .open_reader(&coro, &empty)
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
                assert!(changes.open_reader(&coro, &empty).await.is_err());
                let mut reader = changes.open_reader(&coro, &batch).await?;
                for expected in records {
                    assert_eq!(reader.next_record(&coro).await?, Some(expected));
                }
                assert!(reader.next_record(&coro).await?.is_none());
                assert!(reader.next_record(&coro).await?.is_none());
                drop(reader);
                changes.reset(&coro, ChangeKind::Pages).await?;
                changes.append_record(&coro, &[7; 4120]).await?;
                let pages = changes.finish(&coro).await?;
                let mut reader = changes.open_reader(&coro, &pages).await?;
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
                assert!(reopened.open_reader(&coro, &sealed).await.is_err());
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
                    let mut reader = changes.open_reader(&coro, &sealed).await?;
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
                        let mut reader = changes.open_reader(&coro, &sealed).await?;
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
                        assert!(changes.open_reader(&coro, &sealed).await.is_err());
                    }
                    2 => {
                        changes.write(&coro, sealed.size, vec![0]).await?;
                        assert!(changes.open_reader(&coro, &sealed).await.is_err());
                    }
                    _ => unreachable!(),
                }
                Ok(())
            })
            .expect("tamper test");
        }
    }

    struct ControlledFile {
        inner: Arc<dyn File>,
        delay: AtomicBool,
        fail_write: AtomicBool,
        fail_sync: AtomicBool,
        read_limit: AtomicUsize,
        reads: AtomicUsize,
        truncates: AtomicUsize,
        pending: Mutex<Vec<(u64, std::sync::Weak<Buffer>, Completion)>>,
        writes: Mutex<Vec<Vec<u8>>>,
        write_buffers: Mutex<Vec<usize>>,
        short_write: AtomicBool,
    }

    impl ControlledFile {
        fn new() -> Self {
            Self {
                inner: memory_file(),
                delay: AtomicBool::new(false),
                fail_write: AtomicBool::new(false),
                fail_sync: AtomicBool::new(false),
                read_limit: AtomicUsize::new(usize::MAX),
                reads: AtomicUsize::new(0),
                truncates: AtomicUsize::new(0),
                pending: Mutex::new(Vec::new()),
                writes: Mutex::new(Vec::new()),
                write_buffers: Mutex::new(Vec::new()),
                short_write: AtomicBool::new(false),
            }
        }

        fn flush(&self) {
            for (pos, buffer, c) in self.pending.lock().expect("pending").drain(..) {
                let buffer = buffer
                    .upgrade()
                    .expect("completion must retain the write buffer");
                assert!(self
                    .inner
                    .pwrite(pos, buffer, c)
                    .expect("delayed write")
                    .succeeded());
            }
        }
    }

    impl File for ControlledFile {
        fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> turso_core::Result<()> {
            self.inner.unlock_file()
        }
        fn size(&self) -> turso_core::Result<u64> {
            self.inner.size()
        }
        fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let length = c
                .as_read()
                .buf()
                .len()
                .min(self.read_limit.load(Ordering::SeqCst));
            let output = c.clone();
            let buffer = Arc::new(Buffer::new_temporary(length));
            let completion = Completion::new_read(buffer, move |result| {
                match result {
                    Ok((buffer, n)) => {
                        output.as_read().buf().as_mut_slice()[..n as usize]
                            .copy_from_slice(&buffer.as_slice()[..n as usize]);
                        output.complete(n);
                    }
                    Err(error) => output.error(error),
                }
                None
            });
            assert!(self.inner.pread(pos, completion)?.finished());
            Ok(c)
        }
        fn pwrite(
            &self,
            pos: u64,
            buffer: Arc<Buffer>,
            c: Completion,
        ) -> turso_core::Result<Completion> {
            self.writes
                .lock()
                .expect("writes")
                .push(buffer.as_slice().to_vec());
            self.write_buffers
                .lock()
                .expect("write buffers")
                .push(Arc::as_ptr(&buffer) as usize);
            if self.fail_write.load(Ordering::SeqCst) {
                c.error(turso_core::CompletionError::ShortWrite);
                Ok(c)
            } else if self.short_write.load(Ordering::SeqCst) {
                c.complete(buffer.len() as i32 - 1);
                Ok(c)
            } else if self.delay.load(Ordering::SeqCst) {
                self.pending.lock().expect("pending").push((
                    pos,
                    Arc::downgrade(&buffer),
                    c.clone(),
                ));
                Ok(c)
            } else {
                self.inner.pwrite(pos, buffer, c)
            }
        }
        fn sync(&self, c: Completion, kind: FileSyncType) -> turso_core::Result<Completion> {
            if self.fail_sync.load(Ordering::SeqCst) {
                c.error(turso_core::CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "injected sync failure",
                ));
                Ok(c)
            } else {
                self.inner.sync(c, kind)
            }
        }
        fn truncate(&self, size: u64, c: Completion) -> turso_core::Result<Completion> {
            self.truncates.fetch_add(1, Ordering::SeqCst);
            self.inner.truncate(size, c)
        }
    }

    #[test]
    fn plaintext_pages_reuse_the_input_buffer() {
        let file = Arc::new(ControlledFile::new());
        let mut changes = ChangesFile::new(file.clone(), None);
        let buffer = Arc::new(Buffer::new_temporary(4120));
        run(|coro| async move {
            changes.reset(&coro, ChangeKind::Pages).await?;
            for value in 0..16 {
                buffer.as_mut_slice().fill(value);
                changes.append_page(&coro, buffer.clone()).await?;
            }
            let sealed = changes.finish(&coro).await?;
            assert_eq!(
                *file.write_buffers.lock().unwrap(),
                vec![Arc::as_ptr(&buffer) as usize; 16],
                "plaintext page writes must reuse the caller's buffer"
            );
            let mut reader = changes.open_reader(&coro, &sealed).await?;
            for value in 0..16 {
                assert_eq!(reader.next_record(&coro).await?, Some(vec![value; 4120]));
            }
            assert!(reader.next_record(&coro).await?.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn page_write_failures_require_reset_before_reuse() {
        for encrypted in [false, true] {
            for short in [false, true] {
                let file = Arc::new(ControlledFile::new());
                let mut changes = ChangesFile::new(file.clone(), encrypted.then(crypto));
                let buffer = Arc::new(Buffer::new(vec![42; 4120]));
                run(|coro| async move {
                    changes.reset(&coro, ChangeKind::Pages).await?;
                    file.fail_write.store(!short, Ordering::SeqCst);
                    file.short_write.store(short, Ordering::SeqCst);
                    assert!(changes.append_page(&coro, buffer.clone()).await.is_err());
                    assert!(changes.finish(&coro).await.is_err());
                    file.fail_write.store(false, Ordering::SeqCst);
                    file.short_write.store(false, Ordering::SeqCst);
                    changes.reset(&coro, ChangeKind::Pages).await?;
                    changes.append_page(&coro, buffer.clone()).await?;
                    let sealed = changes.finish(&coro).await?;
                    let mut reader = changes.open_reader(&coro, &sealed).await?;
                    assert_eq!(reader.next_record(&coro).await?, Some(vec![42; 4120]));
                    assert!(reader.next_record(&coro).await?.is_none());
                    Ok(())
                })
                .unwrap();
            }
        }
    }

    #[test]
    fn plaintext_records_use_single_writes_and_buffered_reads() {
        for kind in [ChangeKind::Logical, ChangeKind::Pages] {
            let file = Arc::new(ControlledFile::new());
            let mut changes = ChangesFile::new(file.clone(), None);
            let records = match kind {
                ChangeKind::Logical => {
                    // The second delimiter straddles the 64 KiB buffer boundary.
                    [65532, 128, 200_000, 1]
                        .into_iter()
                        .chain(std::iter::repeat_n(100, 10_000))
                        .map(|size| vec![42; size])
                        .collect::<Vec<_>>()
                }
                ChangeKind::Pages => vec![vec![23; 4120]; 100],
            };
            let size: usize = records
                .iter()
                .map(|r| {
                    r.len()
                        + if kind == ChangeKind::Logical {
                            prost::length_delimiter_len(r.len())
                        } else {
                            0
                        }
                })
                .sum();
            run(|coro| async move {
                changes.reset(&coro, kind).await?;
                for record in &records {
                    changes.append_record(&coro, record).await?;
                }
                let sealed = changes.finish(&coro).await?;
                assert_eq!(file.writes.lock().unwrap().len(), records.len());
                let mut reader = changes.open_reader(&coro, &sealed).await?;
                for record in records {
                    assert_eq!(reader.next_record(&coro).await?, Some(record));
                }
                assert!(reader.next_record(&coro).await?.is_none());
                assert_eq!(file.reads.load(Ordering::SeqCst), size.div_ceil(64 * 1024));
                Ok(())
            })
            .expect("buffered plaintext replay");
        }
    }

    #[test]
    fn buffered_short_reads_and_invalid_lengths() {
        for encrypted in [false, true] {
            for prefix in [
                None,
                Some(vec![0x80]),
                Some(vec![0xff; 10]),
                Some(vec![0x81, 0]),
                Some(vec![0]),
                Some(vec![0xff, 0xff, 0x7f]),
            ] {
                let file = Arc::new(ControlledFile::new());
                file.read_limit.store(997, Ordering::SeqCst);
                let mut changes = ChangesFile::new(file.clone(), encrypted.then(crypto));
                run(|coro| async move {
                    changes.reset(&coro, ChangeKind::Logical).await?;
                    let record = vec![42; 70_000];
                    changes.append_record(&coro, &record).await?;
                    let mut sealed = changes.finish(&coro).await?;
                    if let Some(prefix) = &prefix {
                        assert!(file.truncate(0, Completion::new_trunc(|_| {}))?.succeeded());
                        changes.write(&coro, 0, prefix.clone()).await?;
                        sealed.size = prefix.len() as u64;
                        changes.state = State::Sealed(sealed.clone());
                    }
                    let mut reader = changes.open_reader(&coro, &sealed).await?;
                    if prefix.is_none() {
                        assert_eq!(reader.next_record(&coro).await?, Some(record));
                        assert!(reader.next_record(&coro).await?.is_none());
                    } else {
                        assert!(reader.next_record(&coro).await.is_err());
                        assert!(reader.next_record(&coro).await.is_err());
                    }
                    Ok(())
                })
                .expect("read validation");
            }
        }
    }

}
