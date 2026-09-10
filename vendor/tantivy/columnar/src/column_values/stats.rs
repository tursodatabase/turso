use std::io;
use std::io::Write;
use std::num::NonZeroU64;

use common::{BinarySerializable, VInt};

use crate::RowId;

/// Column statistics.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnStats {
    /// GCD of the elements `el - min(column)`.
    pub gcd: NonZeroU64,
    /// Minimum value of the column.
    pub min_value: u64,
    /// Maximum value of the column.
    pub max_value: u64,
    /// Number of rows in the column.
    pub num_rows: RowId,
}

impl ColumnStats {
    /// Amplitude of value.
    /// Difference between the maximum and the minimum value.
    pub fn amplitude(&self) -> u64 {
        self.max_value - self.min_value
    }
}

impl BinarySerializable for ColumnStats {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        VInt(self.min_value).serialize(writer)?;
        VInt(self.gcd.get()).serialize(writer)?;
        VInt(self.amplitude() / self.gcd).serialize(writer)?;
        VInt(self.num_rows as u64).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let min_value = VInt::deserialize(reader)?.0;
        let gcd = VInt::deserialize(reader)?.0;
        let gcd = NonZeroU64::new(gcd)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "GCD of 0 is forbidden"))?;
        let amplitude = VInt::deserialize(reader)?.0 * gcd.get();
        let max_value = min_value + amplitude;
        let num_rows = VInt::deserialize(reader)?.0 as RowId;
        Ok(ColumnStats {
            min_value,
            max_value,
            num_rows,
            gcd,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use common::BinarySerializable;

    use crate::column_values::ColumnStats;

    #[track_caller]
    fn test_stats_ser_deser_aux(stats: &ColumnStats, num_bytes: usize) {
        let mut buffer: Vec<u8> = Vec::new();
        stats.serialize(&mut buffer).unwrap();
        assert_eq!(buffer.len(), num_bytes);
        let deser_stats = ColumnStats::deserialize(&mut &buffer[..]).unwrap();
        assert_eq!(stats, &deser_stats);
    }

    #[test]
    fn test_stats_serialization() {
        test_stats_ser_deser_aux(
            &(ColumnStats {
                gcd: NonZeroU64::new(3).unwrap(),
                min_value: 1,
                max_value: 3001,
                num_rows: 10,
            }),
            5,
        );
        test_stats_ser_deser_aux(
            &(ColumnStats {
                gcd: NonZeroU64::new(1_000).unwrap(),
                min_value: 1,
                max_value: 3001,
                num_rows: 10,
            }),
            5,
        );
        test_stats_ser_deser_aux(
            &(ColumnStats {
                gcd: NonZeroU64::new(1).unwrap(),
                min_value: 0,
                max_value: 0,
                num_rows: 0,
            }),
            4,
        );
    }
}
