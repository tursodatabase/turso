extern crate tantivy_columnar;

use core::fmt;
use std::fmt::{Display, Formatter};

use tantivy_columnar::{ColumnarReader, ColumnarWriter};

pub enum Card {
    MultiSparse,
    Multi,
    Sparse,
    Dense,
    Full,
}
impl Display for Card {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Card::MultiSparse => write!(f, "multi sparse 1/13"),
            Card::Multi => write!(f, "multi 2x"),
            Card::Sparse => write!(f, "sparse 1/13"),
            Card::Dense => write!(f, "dense 1/12"),
            Card::Full => write!(f, "full"),
        }
    }
}
pub fn generate_columnar_with_name(card: Card, num_docs: u32, column_name: &str) -> ColumnarReader {
    let mut columnar_writer = ColumnarWriter::default();

    if let Card::MultiSparse = card {
        columnar_writer.record_numerical(0, column_name, 10u64);
        columnar_writer.record_numerical(0, column_name, 10u64);
    }

    for i in 0..num_docs {
        match card {
            Card::MultiSparse | Card::Sparse => {
                if i % 13 == 0 {
                    columnar_writer.record_numerical(i, column_name, i as u64);
                }
            }
            Card::Dense => {
                if i % 12 == 0 {
                    columnar_writer.record_numerical(i, column_name, i as u64);
                }
            }
            Card::Full => {
                columnar_writer.record_numerical(i, column_name, i as u64);
            }
            Card::Multi => {
                columnar_writer.record_numerical(i, column_name, i as u64);
                columnar_writer.record_numerical(i, column_name, i as u64);
            }
        }
    }

    let mut wrt: Vec<u8> = Vec::new();
    columnar_writer.serialize(num_docs, &mut wrt).unwrap();
    ColumnarReader::open(wrt).unwrap()
}
