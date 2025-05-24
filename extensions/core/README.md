# Limbo extension API

The `limbo_ext` crate simplifies the creation and registration of libraries meant to extend the functionality of `Limbo`, that can be loaded
like traditional `sqlite3` extensions, but are able to be written in much more ergonomic Rust.
 
---

## Currently supported features

 - [ x ] **Scalar Functions**: Create scalar functions using the `scalar` macro.
 - [ x ] **Aggregate Functions**: Define aggregate functions with `AggregateDerive` macro and `AggFunc` trait.
 - [ x ]  **Virtual tables**: Create a module for a virtual table with the `VTabModuleDerive` macro and `VTabCursor` trait.
 - [ x ] **VFS Modules**: Extend Limbo's OS interface by implementing `VfsExtension` and `VfsFile` traits.
---

## Installation

On the root of the workspace run: 
```sh
cargo new --lib extensions/your_crate_name
```

Add the crate to your `extensions/your_crate_name/Cargo.toml`:

This modification in `Cargo.toml` is only needed if you are creating an extension as a separate Crate. If you are 
creating an extension inside `core`, this step should be skipped.

```toml

[features]
static = ["limbo_ext/static"]

[dependencies]
limbo_ext = { path = "path/to/limbo/extensions/core", features = ["static", "vfs"] } # temporary until crate is published

# mimalloc is required if you intend on linking dynamically. It is imported for you by the register_extension
# macro, so no configuration is needed. But it must be added to your Cargo.toml
[target.'cfg(not(target_family = "wasm"))'.dependencies]
mimalloc = { version = "0.1", default-features = false }


# NOTE: Crate must be of type `cdylib` if you wish to link dynamically
[lib]
crate-type = ["cdylib", "lib"]
```

`cargo build` will output a shared library that can be loaded by the following options:

#### **CLI:**
    `.load target/debug/libyour_crate_name`

#### **SQL:**
   `SELECT load_extension('target/debug/libyour_crate_name')`


Extensions can be registered with the `register_extension!` macro:

```rust

register_extension!{
    scalars: { double }, // name of your function, if different from attribute name
    aggregates: { Percentile },
    vtabs: { CsvVTable },
    vfs: { ExampleFS },
}
```

**NOTE**: Currently, any Derive macro used from this crate is required to be in the same
file as the `register_extension` macro.


### Scalar Example:
```rust
use limbo_ext::{register_extension, Value, scalar};

/// Annotate each with the scalar macro, specifying the name you would like to call it with
/// and optionally, an alias.. e.g. SELECT double(4); or SELECT twice(4);
#[scalar(name = "double", alias = "twice")]
fn double(&self, args: &[Value]) -> Value {
    if let Some(arg) = args.first() {
        match arg.value_type() {
            ValueType::Float => {
                let val = arg.to_float().unwrap();
                Value::from_float(val * 2.0)
            }
            ValueType::Integer => {
                let val = arg.to_integer().unwrap();
                Value::from_integer(val * 2)
            }
        }
    } else {
        Value::null()
    }
}
```

### Aggregates Example:

```rust

use limbo_ext::{register_extension, AggregateDerive, AggFunc, Value};
/// annotate your struct with the AggregateDerive macro, and it must implement the below AggFunc trait
#[derive(AggregateDerive)]
struct Percentile;

impl AggFunc for Percentile {
    /// The state to track during the steps
    type State = (Vec<f64>, Option<f64>, Option<String>); // Tracks the values, Percentile, and errors

    /// Define your error type, must impl Display
    type Error = String;

    /// Define the name you wish to call your function by. 
    /// e.g. SELECT percentile(value, 40);
     const NAME: &str = "percentile";

    /// Define the number of expected arguments for your function.
     const ARGS: i32 = 2;

    /// Define a function called on each row/value in a relevant group/column
    fn step(state: &mut Self::State, args: &[Value]) {
        let (values, p_value, error) = state;

        if let (Some(y), Some(p)) = (
            args.first().and_then(Value::to_float),
            args.get(1).and_then(Value::to_float),
        ) {
            if !(0.0..=100.0).contains(&p) {
                *error = Some("Percentile P must be between 0 and 100.".to_string());
                return;
            }

            if let Some(existing_p) = *p_value {
                if (existing_p - p).abs() >= 0.001 {
                    *error = Some("P values must remain consistent.".to_string());
                    return;
                }
            } else {
                *p_value = Some(p);
            }

            values.push(y);
        }
    }
    /// A function to finalize the state into a value to be returned as a result
    /// or an error (if you chose to track an error state as well)
    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut values, p_value, error) = state;

        if let Some(error) = error {
            return Err(error);
        }

        if values.is_empty() {
            return Ok(Value::null());
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = values.len() as f64;
        let p = p_value.unwrap();
        let index = (p * (n - 1.0) / 100.0).floor() as usize;

        Ok(Value::from_float(values[index]))
    }
}
```

### Virtual Table Example:

```rust

/// Example: A virtual table that operates on a CSV file as a database table.
/// This example assumes that the CSV file is located at "data.csv" in the current directory.
#[derive(Debug, VTabModuleDerive)]
struct CsvVTableModule;

impl VTabModule for CsvVTableModule {
    type Table = CsvTable;
    /// Declare the name for your virtual table
    const NAME: &'static str = "csv_data";
    /// Declare the type of vtable (TableValuedFunction or VirtualTable)
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;

    /// Declare your virtual table and its schema
    fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let schema = "CREATE TABLE csv_data(
            name TEXT,
            age TEXT,
            city TEXT
        )".into();
        Ok((schema, CsvTable {}))
    }
}

struct CsvTable {}

impl VTable for CsvTable {
    type Cursor = CsvCursor;
    /// Define your error type. Must impl Display and match Cursor::Error
    type Error = &'static str;

    /// Open to return a new cursor: In this simple example, the CSV file is read completely into memory on connect.
    fn open(&self) -> Result<Self::Cursor, Self::Error> {
        // Read CSV file contents from "data.csv"
        let csv_content = fs::read_to_string("data.csv").unwrap_or_default();
        // For simplicity, we'll ignore the header row.
        let rows: Vec<Vec<String>> = csv_content
            .lines()
            .skip(1)
            .map(|line| {
                line.split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            })
            .collect();
        Ok(CsvCursor { rows, index: 0 })
    }

    /// *Optional* methods for non-readonly tables

    /// Update the value at rowid
    fn update(&mut self, _rowid: i64, _args: &[Value]) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Insert the value(s)
    fn insert(&mut self, _args: &[Value]) -> Result<i64, Self::Error> {
        Ok(0)
    }
    /// Delete the value at rowid
    fn delete(&mut self, _rowid: i64) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The cursor for iterating over CSV rows.
#[derive(Debug)]
struct CsvCursor {
    rows: Vec<Vec<String>>,
    index: usize,
}

/// Implement the VTabCursor trait for your cursor type
impl VTabCursor for CsvCursor {
    type Error = &'static str;

    /// Filter through result columns. (not used in this simple example)
    fn filter(&mut self, _args: &[Value], _idx_info: Option<(&str, i32)>) -> ResultCode {
        ResultCode::OK
    }

    /// Next advances the cursor to the next row.
    fn next(&mut self) -> ResultCode {
        if self.index < self.rows.len() - 1 {
            self.index += 1;
            ResultCode::OK
        } else {
            ResultCode::EOF
        }
    }

    /// Return true if the cursor is at the end.
    fn eof(&self) -> bool {
        self.index >= self.rows.len()
    }

    /// Return the value for the column at the given index in the current row.
    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        let row = &self.rows[self.index];
        if (idx as usize) < row.len() {
            Ok(Value::from_text(&row[idx as usize]))
        } else {
            Ok(Value::null())
        }
    }

    fn rowid(&self) -> i64 {
        self.index as i64
    }
}
```

### VFS Example

**NOTE**: Requires 'vfs' feature enabled.

```rust
use limbo_ext::{ExtResult as Result, VfsDerive, VfsExtension, VfsFile};

/// Your struct must also impl Default
#[derive(VfsDerive, Default)]
struct ExampleFS;


struct ExampleFile {
    file: std::fs::File,
}

impl VfsExtension for ExampleFS {
    /// The name of your vfs module
    const NAME: &'static str = "example";

    type File = ExampleFile;

    fn open(&self, path: &str, flags: i32, _direct: bool) -> Result<Self::File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(flags & 1 != 0)
            .open(path)
            .map_err(|_| ResultCode::Error)?;
        Ok(TestFile { file })
    }

    fn run_once(&self) -> Result<()> {
    // (optional) method to cycle/advance IO, if your extension is asynchronous
        Ok(())
    }

    fn close(&self, file: Self::File) -> Result<()> {
    // (optional) method to close or drop the file
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
    // (optional) method to generate random number. Used for testing
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

   fn get_current_time(&self) -> String {
    // (optional) method to generate random number. Used for testing
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

impl VfsFile for ExampleFile {
    fn read(
        &mut self,
        buf: &mut [u8],
        count: usize,
        offset: i64,
    ) -> Result<i32> {
        if file.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        file.file
            .read(&mut buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> Result<i32> {
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .write(&buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_all().map_err(|_| ResultCode::Error)
    }

    fn lock(&self, _exclusive: bool) -> Result<()> {
        // (optional) method to lock the file
        Ok(())
    }

    fn unlock(&self) -> Result<()> {
       // (optional) method to lock the file
        Ok(())
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
```

## Cargo.toml Config

Edit the workspace `Cargo.toml` to include your extension as a workspace dependency, e.g:

```diff
[workspace.dependencies]
limbo_core = { path = "core", version = "0.0.17" }
limbo_crypto = { path = "extensions/crypto", version = "0.0.17" }
limbo_ext = { path = "extensions/core", version = "0.0.17" }
limbo_macros = { path = "macros", version = "0.0.17" }
limbo_uuid = { path = "extensions/uuid", version = "0.0.17" }
...
+limbo_csv = { path = "extensions/csv", version = "0.0.17" }

```

And add your extension as a feature in `core/Cargo.toml`:

```diff
[features]
default = ["fs", "json", "uuid", "time"]
fs = []
json = ["dep:jsonb", "dep:pest", "dep:pest_derive", "dep:serde", "dep:indexmap"]
uuid = ["limbo_uuid/static"]
...
+csv = ["limbo_csv/static"]
```

## Register Extension in Core

Lastly, you have to register your extension statically in the core crate:

```diff
pub fn register_builtins(&self) -> Result<(), String> {
        #[allow(unused_variables)]
        let ext_api = self.build_limbo_ext();
        #[cfg(feature = "uuid")]
        if unsafe { !limbo_uuid::register_extension_static(&ext_api).is_ok() } {
            return Err("Failed to register uuid extension".to_string());
        }
+        #[cfg(feature = "csv")]
+        if unsafe { !limbo_csv::register_extension_static(&ext_api).is_ok() } {
+            return Err("Failed to register csv extension".to_string());
+        }
        Ok(())
    }
```
