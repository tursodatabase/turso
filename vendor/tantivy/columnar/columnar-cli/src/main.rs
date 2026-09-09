use columnar::ColumnarWriter;
use columnar::NumericalValue;
use serde_json_borrow;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::time::Instant;

#[derive(Default)]
struct JsonStack {
    path: String,
    stack: Vec<usize>,
}

impl JsonStack {
    fn push(&mut self, seg: &str) {
        let len = self.path.len();
        self.stack.push(len);
        self.path.push('.');
        self.path.push_str(seg);
    }

    fn pop(&mut self) {
        if let Some(len) = self.stack.pop() {
            self.path.truncate(len);
        }
    }

    fn path(&self) -> &str {
        &self.path[1..]
    }
}

fn append_json_to_columnar(
    doc: u32,
    json_value: &serde_json_borrow::Value,
    columnar: &mut ColumnarWriter,
    stack: &mut JsonStack,
) -> usize {
    let mut count = 0;
    match json_value {
        serde_json_borrow::Value::Null => {}
        serde_json_borrow::Value::Bool(val) => {
            columnar.record_numerical(
                doc,
                stack.path(),
                NumericalValue::from(if *val { 1u64 } else { 0u64 }),
            );
            count += 1;
        }
        serde_json_borrow::Value::Number(num) => {
            let numerical_value: NumericalValue = if let Some(num_i64) = num.as_i64() {
                num_i64.into()
            } else if let Some(num_u64) = num.as_u64() {
                num_u64.into()
            } else if let Some(num_f64) = num.as_f64() {
                num_f64.into()
            } else {
                panic!();
            };
            count += 1;
            columnar.record_numerical(
                doc,
                stack.path(),
                numerical_value,
            );
        }
        serde_json_borrow::Value::Str(msg) => {
            columnar.record_str(
                doc,
                stack.path(),
                msg,
            );
            count += 1;
        },
        serde_json_borrow::Value::Array(vals) => {
            for val in vals {
                count += append_json_to_columnar(doc, val, columnar, stack);
            }
        },
        serde_json_borrow::Value::Object(json_map) => {
            for (child_key, child_val) in json_map {
                stack.push(child_key);
                count += append_json_to_columnar(doc, child_val, columnar, stack);
                stack.pop();
            }
        },
    }
    count
}

fn main() -> io::Result<()> {
    let file = File::open("gh_small.json")?;
    let mut reader = BufReader::new(file);
    let mut line = String::with_capacity(100);
    let mut columnar = columnar::ColumnarWriter::default();
    let mut doc = 0;
    let start = Instant::now();
    let mut stack = JsonStack::default();
    let mut total_count = 0;

    let start_build = Instant::now();
    loop {
        line.clear();
        let len = reader.read_line(&mut line)?;
        if len == 0 {
            break;
        }
        let Ok(json_value) = serde_json::from_str::<serde_json_borrow::Value>(&line) else { continue; };
        total_count += append_json_to_columnar(doc, &json_value, &mut columnar, &mut stack);
        doc += 1;
    }
    println!("Build in {:?}", start_build.elapsed());

    println!("value count {total_count}");

    let mut buffer = Vec::new();
    let start_serialize = Instant::now();
    columnar.serialize(doc, None, &mut buffer)?;
    println!("Serialized in {:?}", start_serialize.elapsed());
    println!("num docs: {doc}, {:?}", start.elapsed());
    println!("buffer len {} MB", buffer.len() / 1_000_000);
    let columnar = columnar::ColumnarReader::open(buffer)?;
    for (column_name, dynamic_column) in columnar.list_columns()? {
        let num_bytes = dynamic_column.num_bytes();
        let typ = dynamic_column.column_type();
        if num_bytes > 1_000_000 {
            println!("{column_name} {typ:?}  {} KB", num_bytes / 1_000);
        }
    }
    println!("{} columns", columnar.num_columns());
    Ok(())
}
