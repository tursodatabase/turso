use std::io;

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

/// pack_uint packs the given integer in the smallest number of bytes possible,
/// and writes it to the given writer. The number of bytes written is returned
/// on success.
pub fn pack_uint<W: io::Write>(wtr: W, n: u64) -> io::Result<u8> {
    let nbytes = pack_size(n);
    pack_uint_in(wtr, n, nbytes).map(|_| nbytes)
}

/// pack_uint_in is like pack_uint, but always uses the number of bytes given
/// to pack the number given.
///
/// `nbytes` must be >= pack_size(n) and <= 8, where `pack_size(n)` is the
/// smallest number of bytes that can store the integer given.
pub fn pack_uint_in<W: io::Write>(mut wtr: W, n: u64, nbytes: u8) -> io::Result<()> {
    wtr.write_uint::<LittleEndian>(n, nbytes as usize)
        .map_err(From::from)
}

/// unpack_uint is the dual of pack_uint. It unpacks the integer at the current
/// position in `slice` after reading `nbytes` bytes.
///
/// `nbytes` must be >= 1 and <= 8.
#[inline(always)]
pub fn unpack_uint(slice: &[u8], nbytes: u8) -> u64 {
    LittleEndian::read_uint(slice, nbytes as usize)
}

/// pack_size returns the smallest number of bytes that can encode `n`.
pub fn pack_size(n: u64) -> u8 {
    if n < 1 << 8 {
        1
    } else if n < 1 << 16 {
        2
    } else if n < 1 << 24 {
        3
    } else if n < 1 << 32 {
        4
    } else if n < 1 << 40 {
        5
    } else if n < 1 << 48 {
        6
    } else if n < 1 << 56 {
        7
    } else {
        8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{QuickCheck, StdGen};
    use std::io;

    #[test]
    fn prop_pack_in_out() {
        fn p(num: u64) -> bool {
            let mut buf = io::Cursor::new(vec![]);
            let size = pack_uint(&mut buf, num).unwrap();
            buf.set_position(0);
            num == unpack_uint(buf.get_ref(), size)
        }
        QuickCheck::new()
            .gen(StdGen::new(::rand::thread_rng(), 257)) // pick byte boundary
            .quickcheck(p as fn(u64) -> bool);
    }
}
