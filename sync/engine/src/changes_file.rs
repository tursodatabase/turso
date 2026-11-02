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

fn checked_add(offset: u64, length: usize) -> Result<u64> {
    offset
        .checked_add(length as u64)
        .ok_or_else(|| invalid("change file offset overflow"))
}

fn invalid(message: &str) -> Error {
    Error::DatabaseSyncEngineError(message.into())
}
