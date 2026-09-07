use columnar::ColumnarReader;
use common::file_slice::{FileSlice, WrapFile};
use std::io;
use std::path::Path;
use tantivy::directory::footer::Footer;

fn main() -> io::Result<()> {
    println!("Opens a columnar file written by tantivy and validates it.");
    let path = std::env::args().nth(1).unwrap();

    let path = Path::new(&path);
    println!("Reading {:?}", path);
    let _reader = open_and_validate_columnar(path.to_str().unwrap())?;

    Ok(())
}

pub fn validate_columnar_reader(reader: &ColumnarReader) {
    let num_rows = reader.num_rows();
    println!("num_rows: {}", num_rows);
    let columns = reader.list_columns().unwrap();
    println!("num columns: {:?}", columns.len());
    for (col_name, dynamic_column_handle) in columns {
        let col = dynamic_column_handle.open().unwrap();
        match col {
            columnar::DynamicColumn::Bool(_)
            | columnar::DynamicColumn::I64(_)
            | columnar::DynamicColumn::U64(_)
            | columnar::DynamicColumn::F64(_)
            | columnar::DynamicColumn::IpAddr(_)
            | columnar::DynamicColumn::DateTime(_)
            | columnar::DynamicColumn::Bytes(_) => {}
            columnar::DynamicColumn::Str(str_column) => {
                let num_vals = str_column.ords().values.num_vals();
                let num_terms_dict = str_column.num_terms() as u64;
                let max_ord = str_column.ords().values.iter().max().unwrap_or_default();
                println!("{col_name:35}  num_vals {num_vals:10} \t num_terms_dict {num_terms_dict:8} max_ord: {max_ord:8}",);
                for ord in str_column.ords().values.iter() {
                    assert!(ord < num_terms_dict);
                }
            }
        }
    }
}

/// Opens a columnar file that was written by tantivy and validates it.
pub fn open_and_validate_columnar(path: &str) -> io::Result<ColumnarReader> {
    let wrap_file = WrapFile::new(std::fs::File::open(path)?)?;
    let slice = FileSlice::new(std::sync::Arc::new(wrap_file));
    let (_footer, slice) = Footer::extract_footer(slice.clone()).unwrap();
    let reader = ColumnarReader::open(slice).unwrap();
    validate_columnar_reader(&reader);
    Ok(reader)
}
