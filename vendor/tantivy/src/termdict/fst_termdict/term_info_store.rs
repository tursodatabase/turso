use std::cmp;
use std::io::{self, Read, Write};

use byteorder::{ByteOrder, LittleEndian};
use common::{BinarySerializable, FixedSize};
use tantivy_bitpacker::{compute_num_bits, BitPacker};

use crate::directory::{FileSlice, OwnedBytes};
use crate::postings::TermInfo;
use crate::termdict::TermOrdinal;

const BLOCK_LEN: usize = 256;

#[derive(Debug, Eq, PartialEq, Default)]
struct TermInfoBlockMeta {
    offset: u64,
    ref_term_info: TermInfo,
    doc_freq_nbits: u8,
    postings_offset_nbits: u8,
    positions_offset_nbits: u8,
}

impl BinarySerializable for TermInfoBlockMeta {
    fn serialize<W: Write + ?Sized>(&self, write: &mut W) -> io::Result<()> {
        self.offset.serialize(write)?;
        self.ref_term_info.serialize(write)?;
        write.write_all(&[
            self.doc_freq_nbits,
            self.postings_offset_nbits,
            self.positions_offset_nbits,
        ])?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let offset = u64::deserialize(reader)?;
        let ref_term_info = TermInfo::deserialize(reader)?;
        let mut buffer = [0u8; 3];
        reader.read_exact(&mut buffer)?;
        Ok(TermInfoBlockMeta {
            offset,
            ref_term_info,
            doc_freq_nbits: buffer[0],
            postings_offset_nbits: buffer[1],
            positions_offset_nbits: buffer[2],
        })
    }
}

impl FixedSize for TermInfoBlockMeta {
    const SIZE_IN_BYTES: usize = u64::SIZE_IN_BYTES + TermInfo::SIZE_IN_BYTES + 3;
}

impl TermInfoBlockMeta {
    fn num_bits(&self) -> u8 {
        self.doc_freq_nbits + self.postings_offset_nbits + self.positions_offset_nbits
    }

    // Here inner_offset is the offset within the block, WITHOUT the first term_info.
    // In other word, term_info #1,#2,#3 gets inner_offset 0,1,2... While term_info #0
    // is encoded without bitpacking.
    fn deserialize_term_info(&self, data: &[u8], inner_offset: usize) -> TermInfo {
        assert!(inner_offset < BLOCK_LEN - 1);
        let num_bits = self.num_bits() as usize;

        let posting_start_addr = num_bits * inner_offset;
        // the posting_start is the posting_start of the next term info.
        let posting_end_addr = posting_start_addr + num_bits;
        let positions_start_addr = posting_start_addr + self.postings_offset_nbits as usize;
        // the position_end is the positions_start of the next term info.
        let positions_end_addr = positions_start_addr + num_bits;

        let doc_freq_addr = positions_start_addr + self.positions_offset_nbits as usize;

        let postings_start_offset = self.ref_term_info.postings_range.start
            + extract_bits(data, posting_start_addr, self.postings_offset_nbits) as usize;
        let postings_end_offset = self.ref_term_info.postings_range.start
            + extract_bits(data, posting_end_addr, self.postings_offset_nbits) as usize;

        let positions_start_offset = self.ref_term_info.positions_range.start
            + extract_bits(data, positions_start_addr, self.positions_offset_nbits) as usize;
        let positions_end_offset = self.ref_term_info.positions_range.start
            + extract_bits(data, positions_end_addr, self.positions_offset_nbits) as usize;

        let doc_freq = extract_bits(data, doc_freq_addr, self.doc_freq_nbits) as u32;

        TermInfo {
            doc_freq,
            postings_range: postings_start_offset..postings_end_offset,
            positions_range: positions_start_offset..positions_end_offset,
        }
    }
}

#[derive(Clone)]
pub struct TermInfoStore {
    num_terms: usize,
    block_meta_bytes: OwnedBytes,
    term_info_bytes: OwnedBytes,
}

fn extract_bits(data: &[u8], addr_bits: usize, num_bits: u8) -> u64 {
    assert!(num_bits <= 56);
    let addr_byte = addr_bits / 8;
    let bit_shift = (addr_bits % 8) as u64;
    let val_unshifted_unmasked: u64 = if data.len() >= addr_byte + 8 {
        LittleEndian::read_u64(&data[addr_byte..][..8])
    } else {
        // the buffer is not large enough.
        // Let's copy the few remaining bytes to a 8 byte buffer
        // padded with 0s.
        let mut buf = [0u8; 8];
        let data_to_copy = &data[addr_byte..];
        let nbytes = data_to_copy.len();
        buf[..nbytes].copy_from_slice(data_to_copy);
        LittleEndian::read_u64(&buf)
    };
    let val_shifted_unmasked = val_unshifted_unmasked >> bit_shift;
    let mask = (1u64 << u64::from(num_bits)) - 1;
    val_shifted_unmasked & mask
}

impl TermInfoStore {
    pub fn open(term_info_store_file: FileSlice) -> io::Result<TermInfoStore> {
        let (len_slice, main_slice) = term_info_store_file.split(16);
        let mut bytes = len_slice.read_bytes()?;
        let len = u64::deserialize(&mut bytes)? as usize;
        let num_terms = u64::deserialize(&mut bytes)? as usize;
        let (block_meta_file, term_info_file) = main_slice.split(len);
        let term_info_bytes = term_info_file.read_bytes()?;
        Ok(TermInfoStore {
            num_terms,
            block_meta_bytes: block_meta_file.read_bytes()?,
            term_info_bytes,
        })
    }

    pub fn get(&self, term_ord: TermOrdinal) -> TermInfo {
        let block_id = (term_ord as usize) / BLOCK_LEN;
        let buffer = self.block_meta_bytes.as_slice();
        let mut block_data: &[u8] = &buffer[block_id * TermInfoBlockMeta::SIZE_IN_BYTES..];
        let term_info_block_data = TermInfoBlockMeta::deserialize(&mut block_data)
            .expect("Failed to deserialize terminfoblockmeta");
        let inner_offset = (term_ord as usize) % BLOCK_LEN;
        if inner_offset == 0 {
            return term_info_block_data.ref_term_info;
        }
        let term_info_data = self.term_info_bytes.as_slice();
        term_info_block_data.deserialize_term_info(
            &term_info_data[term_info_block_data.offset as usize..],
            inner_offset - 1,
        )
    }

    pub fn num_terms(&self) -> usize {
        self.num_terms
    }
}

pub struct TermInfoStoreWriter {
    buffer_block_metas: Vec<u8>,
    buffer_term_infos: Vec<u8>,
    term_infos: Vec<TermInfo>,
    num_terms: u64,
}

fn bitpack_serialize<W: Write>(
    write: &mut W,
    bit_packer: &mut BitPacker,
    term_info_block_meta: &TermInfoBlockMeta,
    term_info: &TermInfo,
) -> io::Result<()> {
    bit_packer.write(
        term_info.postings_range.start as u64,
        term_info_block_meta.postings_offset_nbits,
        write,
    )?;
    bit_packer.write(
        term_info.positions_range.start as u64,
        term_info_block_meta.positions_offset_nbits,
        write,
    )?;
    bit_packer.write(
        u64::from(term_info.doc_freq),
        term_info_block_meta.doc_freq_nbits,
        write,
    )?;
    Ok(())
}

impl TermInfoStoreWriter {
    pub fn new() -> TermInfoStoreWriter {
        TermInfoStoreWriter {
            buffer_block_metas: Vec::new(),
            buffer_term_infos: Vec::new(),
            term_infos: Vec::with_capacity(BLOCK_LEN),
            num_terms: 0u64,
        }
    }

    fn flush_block(&mut self) -> io::Result<()> {
        let mut bit_packer = BitPacker::new();
        let ref_term_info = self.term_infos[0].clone();

        let last_term_info = if let Some(last_term_info) = self.term_infos.last().cloned() {
            last_term_info
        } else {
            return Ok(());
        };
        let postings_end_offset =
            last_term_info.postings_range.end - ref_term_info.postings_range.start;
        let positions_end_offset =
            last_term_info.positions_range.end - ref_term_info.positions_range.start;
        for term_info in &mut self.term_infos[1..] {
            term_info.postings_range.start -= ref_term_info.postings_range.start;
            term_info.positions_range.start -= ref_term_info.positions_range.start;
        }

        let mut max_doc_freq: u32 = 0u32;

        for term_info in &self.term_infos[1..] {
            max_doc_freq = cmp::max(max_doc_freq, term_info.doc_freq);
        }

        let max_doc_freq_nbits: u8 = compute_num_bits(u64::from(max_doc_freq));
        let max_postings_offset_nbits = compute_num_bits(postings_end_offset as u64);
        let max_positions_offset_nbits = compute_num_bits(positions_end_offset as u64);

        let term_info_block_meta = TermInfoBlockMeta {
            offset: self.buffer_term_infos.len() as u64,
            ref_term_info,
            doc_freq_nbits: max_doc_freq_nbits,
            postings_offset_nbits: max_postings_offset_nbits,
            positions_offset_nbits: max_positions_offset_nbits,
        };

        term_info_block_meta.serialize(&mut self.buffer_block_metas)?;
        for term_info in &self.term_infos[1..] {
            bitpack_serialize(
                &mut self.buffer_term_infos,
                &mut bit_packer,
                &term_info_block_meta,
                term_info,
            )?;
        }

        // We still need to serialize the end offset for postings & positions.
        bit_packer.write(
            postings_end_offset as u64,
            term_info_block_meta.postings_offset_nbits,
            &mut self.buffer_term_infos,
        )?;
        bit_packer.write(
            positions_end_offset as u64,
            term_info_block_meta.positions_offset_nbits,
            &mut self.buffer_term_infos,
        )?;

        // Block need end up at the end of a byte.
        bit_packer.flush(&mut self.buffer_term_infos)?;
        self.term_infos.clear();

        Ok(())
    }

    pub fn write_term_info(&mut self, term_info: &TermInfo) -> io::Result<()> {
        self.num_terms += 1u64;
        self.term_infos.push(term_info.clone());
        if self.term_infos.len() >= BLOCK_LEN {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn serialize<W: io::Write + ?Sized>(&mut self, write: &mut W) -> io::Result<()> {
        if !self.term_infos.is_empty() {
            self.flush_block()?;
        }
        let len = self.buffer_block_metas.len() as u64;
        len.serialize(write)?;
        self.num_terms.serialize(write)?;
        write.write_all(&self.buffer_block_metas)?;
        write.write_all(&self.buffer_term_infos)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use common::BinarySerializable;
    use tantivy_bitpacker::{compute_num_bits, BitPacker};

    use super::{extract_bits, TermInfoBlockMeta, TermInfoStore, TermInfoStoreWriter};
    use crate::directory::FileSlice;
    use crate::postings::TermInfo;

    #[test]
    fn test_term_info_block() {
        crate::tests::fixed_size_test::<TermInfoBlockMeta>();
    }

    #[test]
    fn test_bitpacked() {
        let mut buffer = Vec::new();
        let mut bitpack = BitPacker::new();
        bitpack.write(321u64, 9, &mut buffer).unwrap();
        assert_eq!(compute_num_bits(321u64), 9);
        bitpack.write(2u64, 2, &mut buffer).unwrap();
        assert_eq!(compute_num_bits(2u64), 2);
        bitpack.write(51, 6, &mut buffer).unwrap();
        assert_eq!(compute_num_bits(51), 6);
        bitpack.close(&mut buffer).unwrap();
        assert_eq!(buffer.len(), 3);
        assert_eq!(extract_bits(&buffer[..], 0, 9), 321u64);
        assert_eq!(extract_bits(&buffer[..], 9, 2), 2u64);
        assert_eq!(extract_bits(&buffer[..], 11, 6), 51u64);
    }

    #[test]
    fn test_term_info_block_meta_serialization() {
        let term_info_block_meta = TermInfoBlockMeta {
            offset: 2009u64,
            ref_term_info: TermInfo {
                doc_freq: 512,
                postings_range: 51..57,
                positions_range: 110..134,
            },
            doc_freq_nbits: 10,
            postings_offset_nbits: 5,
            positions_offset_nbits: 8,
        };
        let mut buffer: Vec<u8> = Vec::new();
        term_info_block_meta.serialize(&mut buffer).unwrap();
        let mut cursor: &[u8] = &buffer[..];
        let term_info_block_meta_serde = TermInfoBlockMeta::deserialize(&mut cursor).unwrap();
        assert_eq!(term_info_block_meta_serde, term_info_block_meta);
    }

    #[test]
    fn test_pack() -> crate::Result<()> {
        let mut store_writer = TermInfoStoreWriter::new();
        let mut term_infos = vec![];
        let offset = |i| i * 13 + i * i;
        for i in 0usize..1000usize {
            let term_info = TermInfo {
                doc_freq: i as u32,
                postings_range: offset(i)..offset(i + 1),
                positions_range: offset(i) * 3..offset(i + 1) * 3,
            };
            store_writer.write_term_info(&term_info)?;
            term_infos.push(term_info);
        }
        let mut buffer = Vec::new();
        store_writer.serialize(&mut buffer)?;
        let term_info_store = TermInfoStore::open(FileSlice::from(buffer))?;
        for i in 0..1000 {
            assert_eq!(
                term_info_store.get(i as u64),
                term_infos[i],
                "term info {i}"
            );
        }
        Ok(())
    }
}
