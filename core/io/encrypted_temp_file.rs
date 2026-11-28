use crate::io::{Buffer, Completion, File, FileSyncType};
use crate::storage::encryption::{fill_random, CipherMode, EncryptionContext, EncryptionKey};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, OnceLock};
use crate::{turso_assert, turso_assert_eq, CompletionError, LimboError, Result};
use std::io::ErrorKind;

/// A temporary file on disk that encrypts its content with a key that only this process holds.
///
/// Temporary files hold rows that statements copy from their databases, for example the
/// pages of ephemeral tables, sorted runs and hash table partitions. When a database is
/// encrypted, these rows must not go to disk in plaintext. A temporary file is read only by
/// this process and only while the file exists, so each file gets a new random key, and the
/// key is never written anywhere. The key is made on the first write, because most temporary
/// files are never written.
///
/// The content is divided into blocks of `block_size` bytes. Block `i` is stored at
/// `i * slot_size()` as its ciphertext, followed by its authentication tag and its nonce. The
/// first 8 bytes of the nonce of each encrypted block are the next value of a counter of the
/// file, also when the block replaces an older version of itself, so no nonce is used twice
/// with the key of the file. The other bytes of the nonce are random bytes of the file, made
/// with its key, so that different files do not use the same nonces. The number of the block
/// is the associated data, so a block that is moved to another position does not decrypt.
///
/// A write must start at a block boundary. If a write does not end at a block boundary, the
/// rest of its last block is filled with zeros. A read can start and end anywhere.
///
/// Each write encrypts its content into a new buffer. The content cannot be encrypted in
/// place: the slots are larger than the content, and the caller can still use its buffer,
/// for example a page in the page cache. So a write uses about twice its size in memory until
/// it completes.
pub(crate) struct EncryptedTempFile {
    inner: Arc<dyn File>,
    cipher: CipherMode,
    block_size: usize,
    /// The key and nonce bytes of this file, made on the first write.
    keys: OnceLock<FileKeys>,
    /// The counter whose next value starts the nonce of the next encrypted block.
    next_nonce: AtomicU64,
    /// The end of the content: the largest end of a write, or a smaller length given to
    /// `truncate`. It is zero until the first write.
    size: AtomicU64,
}

/// The secrets of one encrypted temporary file.
struct FileKeys {
    /// The cipher with the random key of the file.
    ctx: Arc<EncryptionContext>,
    /// Random bytes that end the nonce of every block of the file, after the counter.
    nonce_tail: Vec<u8>,
}

/// The number of bytes at the start of a nonce that hold the counter.
const NONCE_COUNTER_SIZE: usize = size_of::<u64>();

impl EncryptedTempFile {
    pub(crate) fn new(inner: Arc<dyn File>, cipher: CipherMode, block_size: usize) -> Self {
        turso_assert!(block_size > 0, "a temporary file block must not be empty");
        turso_assert!(
            cipher.nonce_size() > NONCE_COUNTER_SIZE,
            "a nonce must have room for the counter and random bytes"
        );
        Self {
            inner,
            cipher,
            block_size,
            keys: OnceLock::new(),
            next_nonce: AtomicU64::new(0),
            size: AtomicU64::new(0),
        }
    }
}

impl File for EncryptedTempFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let len = c.as_read().buf().len() as u64;
        // Like a plain file, a read past the end of the content returns fewer bytes.
        let end = pos
            .saturating_add(len)
            .min(self.size.load(Ordering::Acquire));
        if pos >= end {
            c.complete(0);
            return Ok(c);
        }
        let ctx = self
            .keys
            .get()
            .expect("a temporary file with content was written, so it has a key")
            .ctx
            .clone();
        let block_size = self.block_size;
        let first_block = pos / block_size as u64;
        let blocks = ((end - 1) / block_size as u64 - first_block + 1) as usize;
        let slot_size = self.slot_size();
        let encrypted_len = blocks * slot_size;
        let caller = c.clone();
        let read = Completion::new_read(
            Arc::new(Buffer::new_temporary(encrypted_len)),
            move |result| {
                // A cancelled caller no longer expects data in its buffer.
                if caller.finished() {
                    return caller.get_error();
                }
                let decrypted = match result {
                    Ok((buf, read)) if read as usize == encrypted_len => decrypt_range(
                        &ctx,
                        block_size,
                        first_block,
                        &buf.as_slice()[..encrypted_len],
                        pos..end,
                        caller.as_read().buf().as_mut_slice(),
                    ),
                    // The blocks were written completely, so a short read means that the file
                    // changed.
                    Ok(_) => Err(CompletionError::IOError(
                        ErrorKind::UnexpectedEof,
                        "read encrypted temporary file",
                    )),
                    Err(err) => Err(err),
                };
                match decrypted {
                    Ok(()) => caller.complete((end - pos) as i32),
                    Err(err) => caller.error(err),
                }
                caller.get_error()
            },
        );
        // The callback of `read` finishes `c`, so the caller waits for `c`.
        drop(self.inner.pread(first_block * slot_size as u64, read)?);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        let block_size = self.block_size as u64;
        if pos % block_size != 0 {
            return Err(self.unaligned("write", pos));
        }
        let len = buffer.len();
        if len == 0 {
            c.complete(0);
            return Ok(c);
        }
        let first_block = pos / block_size;
        let encrypted = self.encrypt_blocks(first_block, buffer.as_slice())?;
        let encrypted_len = encrypted.len();
        let caller = c.clone();
        let write = Completion::new_write(move |result| match result {
            Ok(written) if written as usize == encrypted_len => caller.complete(len as i32),
            Ok(_) => caller.error(CompletionError::ShortWrite),
            Err(err) => caller.error(err),
        });
        // The callback of `write` finishes `c`, so the caller waits for `c`.
        drop(self.inner.pwrite(
            first_block * self.slot_size() as u64,
            Arc::new(Buffer::new(encrypted)),
            write,
        )?);
        // The size grows when the write is submitted, not when it completes. The sorter, the
        // hash tables, the change log and the ephemeral pager read a range only after the write
        // of that range completed, as they must with a plain file, so a read never decrypts a
        // block that is still being written.
        self.size.fetch_max(pos + len as u64, Ordering::AcqRel);
        Ok(c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> Result<u64> {
        Ok(self.size.load(Ordering::Acquire))
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let block_size = self.block_size as u64;
        if len % block_size != 0 {
            return Err(self.unaligned("truncate", len));
        }
        let c = self
            .inner
            .truncate(len / block_size * self.slot_size() as u64, c)?;
        // Only a write adds content, because blocks that were never written do not decrypt.
        self.size.fetch_min(len, Ordering::AcqRel);
        Ok(c)
    }
}

impl EncryptedTempFile {
    /// Encrypts `plaintext`, which starts at block `first_block`, into the slots of its blocks.
    fn encrypt_blocks(&self, first_block: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let keys = self.keys()?;
        let slot_size = self.slot_size();
        let tag_size = self.cipher.tag_size();
        let blocks = plaintext.len().div_ceil(self.block_size);
        let mut encrypted = vec![0u8; blocks * slot_size];
        for (block, (data, slot)) in (first_block..).zip(
            plaintext
                .chunks(self.block_size)
                .zip(encrypted.chunks_exact_mut(slot_size)),
        ) {
            // The slot is zeroed, so a short last block is padded with zeros.
            let (ciphertext, rest) = slot.split_at_mut(self.block_size);
            ciphertext[..data.len()].copy_from_slice(data);
            let (tag, nonce) = rest.split_at_mut(tag_size);
            let counter = self.next_nonce.fetch_add(1, Ordering::Relaxed);
            turso_assert!(
                counter != u64::MAX,
                "the nonce counter of a temporary file must not wrap"
            );
            let (nonce_counter, nonce_tail) = nonce.split_at_mut(NONCE_COUNTER_SIZE);
            nonce_counter.copy_from_slice(&counter.to_le_bytes());
            nonce_tail.copy_from_slice(&keys.nonce_tail);
            keys.ctx.encrypt_chunk_in_place_with_nonce(
                ciphertext,
                &block.to_le_bytes(),
                tag,
                nonce,
            )?;
        }
        Ok(encrypted)
    }

    /// Returns the key and nonce bytes of this file, which the first call makes.
    fn keys(&self) -> Result<&FileKeys> {
        if let Some(keys) = self.keys.get() {
            return Ok(keys);
        }
        let key = EncryptionKey::random(self.cipher)?;
        let ctx = Arc::new(EncryptionContext::new(self.cipher, &key, self.block_size)?);
        let mut nonce_tail = vec![0u8; self.cipher.nonce_size() - NONCE_COUNTER_SIZE];
        fill_random(&mut nonce_tail)?;
        // When two first writes race, both use the keys that are stored first.
        Ok(self.keys.get_or_init(|| FileKeys { ctx, nonce_tail }))
    }

    /// The number of bytes that one block uses on disk.
    fn slot_size(&self) -> usize {
        self.block_size + self.cipher.tag_size() + self.cipher.nonce_size()
    }

    /// Returns the error for an access that does not start at a block boundary.
    fn unaligned(&self, operation: &str, pos: u64) -> LimboError {
        LimboError::InternalError(format!(
            "{operation} of encrypted temporary file at {pos} is not at a multiple of {}",
            self.block_size
        ))
    }
}

/// Decrypts the slots in `encrypted`, whose first block is `first_block`, and copies the
/// content in `range` to `out`.
fn decrypt_range(
    ctx: &EncryptionContext,
    block_size: usize,
    first_block: u64,
    encrypted: &[u8],
    range: std::ops::Range<u64>,
    out: &mut [u8],
) -> std::result::Result<(), CompletionError> {
    let slot_size = block_size + ctx.tag_size() + ctx.nonce_size();
    turso_assert_eq!(encrypted.len() % slot_size, 0);
    let mut plaintext = Vec::with_capacity(block_size);
    let mut copied = 0;
    for (block, slot) in (first_block..).zip(encrypted.chunks_exact(slot_size)) {
        let (ciphertext_and_tag, nonce) = slot.split_at(block_size + ctx.tag_size());
        ctx.decrypt_chunk_into(
            ciphertext_and_tag,
            nonce,
            &block.to_le_bytes(),
            &mut plaintext,
        )
        .map_err(|_| CompletionError::IOError(ErrorKind::InvalidData, "decrypt temporary file"))?;
        let block_start = block * block_size as u64;
        let from = (range.start.max(block_start) - block_start) as usize;
        let to = (range.end.min(block_start + block_size as u64) - block_start) as usize;
        out[copied..copied + to - from].copy_from_slice(&plaintext[from..to]);
        copied += to - from;
    }
    turso_assert_eq!(copied as u64, range.end - range.start);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{OpenFlags, IO};
    use crate::MemoryIO;

    const BLOCK_SIZE: usize = 64;

    fn new_file(cipher: CipherMode) -> (Arc<MemoryIO>, Arc<dyn File>, EncryptedTempFile) {
        let io = Arc::new(MemoryIO::new());
        let inner = io.open_file("temp", OpenFlags::Create, false).unwrap();
        let file = EncryptedTempFile::new(inner.clone(), cipher, BLOCK_SIZE);
        (io, inner, file)
    }

    fn write(io: &MemoryIO, file: &dyn File, pos: u64, data: &[u8]) -> Result<()> {
        let written = Arc::new(AtomicU64::new(0));
        let written_by_write = written.clone();
        let c = Completion::new_write(move |result| {
            written_by_write.store(result.unwrap() as u64, Ordering::Release);
        });
        let c = file.pwrite(pos, Arc::new(Buffer::new(data.to_vec())), c)?;
        io.wait_for_completion(c)?;
        assert_eq!(written.load(Ordering::Acquire), data.len() as u64);
        Ok(())
    }

    fn read(io: &MemoryIO, file: &dyn File, pos: u64, len: usize) -> Result<Vec<u8>> {
        let out = Arc::new(crate::sync::Mutex::new(Vec::new()));
        let out_of_read = out.clone();
        let c = Completion::new_read(Arc::new(Buffer::new_temporary(len)), move |result| {
            let (buf, read) = result.ok()?;
            *out_of_read.lock() = buf.as_slice()[..read as usize].to_vec();
            None
        });
        io.wait_for_completion(file.pread(pos, c)?)?;
        let bytes = out.lock().clone();
        Ok(bytes)
    }

    fn content(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn reads_any_range_of_what_was_written() {
        for cipher in [CipherMode::Aes256Gcm, CipherMode::Aegis256] {
            let (io, _, file) = new_file(cipher);
            let data = content(5 * BLOCK_SIZE + 17);
            write(&io, &file, 0, &data[..2 * BLOCK_SIZE + 5]).unwrap();
            write(&io, &file, 3 * BLOCK_SIZE as u64, &data[3 * BLOCK_SIZE..]).unwrap();
            assert_eq!(file.size().unwrap(), data.len() as u64);
            for (start, end) in [(0, 1), (0, BLOCK_SIZE), (5, 3 * BLOCK_SIZE + 9), (60, 70)] {
                let bytes = read(&io, &file, start as u64, end - start).unwrap();
                let mut expected = data[start..end].to_vec();
                // The write that ended inside block 2 padded the rest of that block.
                for (i, byte) in expected.iter_mut().enumerate() {
                    if (2 * BLOCK_SIZE + 5..3 * BLOCK_SIZE).contains(&(start + i)) {
                        *byte = 0;
                    }
                }
                assert_eq!(bytes, expected, "cipher {cipher}, range {start}..{end}");
            }
            // A read past the end returns only the bytes that exist.
            let tail = read(&io, &file, data.len() as u64 - 3, 10).unwrap();
            assert_eq!(tail, data[data.len() - 3..]);
            assert!(read(&io, &file, data.len() as u64, 10).unwrap().is_empty());
        }
    }

    #[test]
    fn writes_only_ciphertext_to_disk() {
        let (io, inner, file) = new_file(CipherMode::Aegis256);
        let data = b"secret temporary row".repeat(10);
        write(&io, &file, 0, &data).unwrap();
        let on_disk = read(&io, inner.as_ref(), 0, inner.size().unwrap() as usize).unwrap();
        assert!(!on_disk.windows(6).any(|window| window == b"secret"));
        assert_eq!(
            on_disk.len(),
            data.len().div_ceil(BLOCK_SIZE) * file.slot_size()
        );
    }

    #[test]
    fn rewritten_blocks_read_back_the_new_content() {
        let (io, _, file) = new_file(CipherMode::Aes256Gcm);
        write(&io, &file, 0, &[1; 2 * BLOCK_SIZE]).unwrap();
        write(&io, &file, BLOCK_SIZE as u64, &[2; BLOCK_SIZE]).unwrap();
        let bytes = read(&io, &file, 0, 2 * BLOCK_SIZE).unwrap();
        assert_eq!(bytes[..BLOCK_SIZE], [1; BLOCK_SIZE]);
        assert_eq!(bytes[BLOCK_SIZE..], [2; BLOCK_SIZE]);
    }

    /// The nonce of every encrypted block starts with the next value of the counter, also when
    /// the block replaces an older version of itself, so no nonce is used twice with the key.
    /// The rest of the nonce is random for each file.
    #[test]
    fn each_written_block_gets_the_next_nonce() {
        for cipher in [
            CipherMode::Aes256Gcm,
            CipherMode::Aegis256,
            CipherMode::Aegis128L,
        ] {
            let (io, inner, file) = new_file(cipher);
            write(&io, &file, 0, &content(2 * BLOCK_SIZE)).unwrap();
            write(&io, &file, 0, &content(BLOCK_SIZE)).unwrap();
            let slot_size = file.slot_size();
            let on_disk = read(&io, inner.as_ref(), 0, 2 * slot_size).unwrap();
            let nonce_tail = &file.keys.get().unwrap().nonce_tail;
            assert_eq!(nonce_tail.len(), cipher.nonce_size() - NONCE_COUNTER_SIZE);
            for (slot, counter) in on_disk.chunks_exact(slot_size).zip([2u64, 1]) {
                let nonce = &slot[slot_size - cipher.nonce_size()..];
                let (value, tail) = nonce.split_at(NONCE_COUNTER_SIZE);
                assert_eq!(value, counter.to_le_bytes(), "cipher {cipher}");
                assert_eq!(tail, nonce_tail.as_slice(), "cipher {cipher}");
            }
            assert_eq!(
                read(&io, &file, 0, BLOCK_SIZE).unwrap(),
                content(BLOCK_SIZE)
            );

            let (other_io, _, other_file) = new_file(cipher);
            write(&other_io, &other_file, 0, b"other").unwrap();
            assert_ne!(
                &other_file.keys.get().unwrap().nonce_tail,
                nonce_tail,
                "cipher {cipher}"
            );
        }
    }

    /// Most temporary files are never written, so a file makes its key on the first write.
    #[test]
    fn the_key_is_made_on_the_first_write() {
        let (io, inner, file) = new_file(CipherMode::Aegis256);
        assert!(file.keys.get().is_none());
        assert!(read(&io, &file, 0, BLOCK_SIZE).unwrap().is_empty());
        assert_eq!(file.size().unwrap(), 0);
        assert!(file.keys.get().is_none());
        assert_eq!(inner.size().unwrap(), 0);
        write(&io, &file, 0, b"first").unwrap();
        assert!(file.keys.get().is_some());
        assert_eq!(read(&io, &file, 0, BLOCK_SIZE).unwrap(), b"first");
    }

    #[test]
    fn changed_or_moved_blocks_fail_to_read() {
        let (io, inner, file) = new_file(CipherMode::Aegis256);
        write(&io, &file, 0, &content(2 * BLOCK_SIZE)).unwrap();
        let slot_size = file.slot_size();
        let on_disk = read(&io, inner.as_ref(), 0, 2 * slot_size).unwrap();

        let mut flipped = on_disk.clone();
        flipped[3] ^= 1;
        write(&io, inner.as_ref(), 0, &flipped).unwrap();
        assert!(read(&io, &file, 0, BLOCK_SIZE).is_err());

        let mut swapped = on_disk[slot_size..].to_vec();
        swapped.extend_from_slice(&on_disk[..slot_size]);
        write(&io, inner.as_ref(), 0, &swapped).unwrap();
        assert!(read(&io, &file, 0, BLOCK_SIZE).is_err());
    }

    #[test]
    fn unaligned_writes_are_rejected() {
        let (io, _, file) = new_file(CipherMode::Aes256Gcm);
        assert!(write(&io, &file, 1, b"x").is_err());
        assert!(write(&io, &file, BLOCK_SIZE as u64 + 3, b"x").is_err());
    }
}
