mod ext;
extern crate proc_macro;
use proc_macro::{token_stream::IntoIter, Group, TokenStream, TokenTree};
use std::collections::HashMap;

/// A procedural macro that derives a `Description` trait for enums.
/// This macro extracts documentation comments (specified with `/// Description...`) for enum variants
/// and generates an implementation for `get_description`, which returns the associated description.
#[proc_macro_derive(Description, attributes(desc))]
pub fn derive_description_from_doc(item: TokenStream) -> TokenStream {
    // Convert the TokenStream into an iterator of TokenTree
    let mut tokens = item.into_iter();

    let mut enum_name = String::new();

    // Vector to store enum variants and their associated payloads (if any)
    let mut enum_variants: Vec<(String, Option<String>)> = Vec::<(String, Option<String>)>::new();

    // HashMap to store descriptions associated with each enum variant
    let mut variant_description_map: HashMap<String, String> = HashMap::new();

    // Parses the token stream to extract the enum name and its variants
    while let Some(token) = tokens.next() {
        match token {
            TokenTree::Ident(ident) if ident.to_string() == "enum" => {
                // Get the enum name
                if let Some(TokenTree::Ident(name)) = tokens.next() {
                    enum_name = name.to_string();
                }
            }
            TokenTree::Group(group) => {
                let mut group_tokens_iter: IntoIter = group.stream().into_iter();

                let mut last_seen_desc: Option<String> = None;
                while let Some(token) = group_tokens_iter.next() {
                    match token {
                        TokenTree::Punct(punct) => {
                            if punct.to_string() == "#" {
                                last_seen_desc = process_description(&mut group_tokens_iter);
                            }
                        }
                        TokenTree::Ident(ident) => {
                            // Capture the enum variant name and associate it with its description
                            let ident_str = ident.to_string();
                            if let Some(desc) = &last_seen_desc {
                                variant_description_map.insert(ident_str.clone(), desc.clone());
                            }
                            enum_variants.push((ident_str, None));
                            last_seen_desc = None;
                        }
                        TokenTree::Group(group) => {
                            // Capture payload information for the current enum variant
                            if let Some(last_variant) = enum_variants.last_mut() {
                                last_variant.1 = Some(process_payload(group));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    generate_get_description(enum_name, &variant_description_map, enum_variants)
}

/// Processes a Rust docs to extract the description string.
fn process_description(token_iter: &mut IntoIter) -> Option<String> {
    if let Some(TokenTree::Group(doc_group)) = token_iter.next() {
        let mut doc_group_iter = doc_group.stream().into_iter();
        // Skip the `desc` and `(` tokens to reach the actual description
        doc_group_iter.next();
        doc_group_iter.next();
        if let Some(TokenTree::Literal(description)) = doc_group_iter.next() {
            return Some(description.to_string());
        }
    }
    None
}

/// Processes the payload of an enum variant to extract variable names (ignoring types).
fn process_payload(payload_group: Group) -> String {
    let payload_group_iter = payload_group.stream().into_iter();
    let mut variable_name_list = String::from("");
    let mut is_variable_name = true;
    for token in payload_group_iter {
        match token {
            TokenTree::Ident(ident) => {
                if is_variable_name {
                    variable_name_list.push_str(&format!("{ident},"));
                }
                is_variable_name = false;
            }
            TokenTree::Punct(punct) => {
                if punct.to_string() == "," {
                    is_variable_name = true;
                }
            }
            _ => {}
        }
    }
    format!("{{ {variable_name_list} }}").to_string()
}
/// Generates the `get_description` implementation for the processed enum.
fn generate_get_description(
    enum_name: String,
    variant_description_map: &HashMap<String, String>,
    enum_variants: Vec<(String, Option<String>)>,
) -> TokenStream {
    let mut all_enum_arms = String::from("");
    for (variant, payload) in enum_variants {
        let payload = payload.unwrap_or("".to_string());
        let desc;
        if let Some(description) = variant_description_map.get(&variant) {
            desc = format!("Some({description})");
        } else {
            desc = "None".to_string();
        }
        all_enum_arms.push_str(&format!("{enum_name}::{variant} {payload} => {desc},\n"));
    }

    let enum_impl = format!(
        "impl {enum_name}  {{ 
     pub fn get_description(&self) -> Option<&str> {{
     match self {{
     {all_enum_arms}
     }}
     }}
     }}"
    );
    enum_impl.parse().unwrap()
}

/// Register your extension with 'core' by providing the relevant functions
///```ignore
///use turso_ext::{register_extension, scalar, Value, AggregateDerive, AggFunc};
///
/// register_extension!{ scalars: { return_one }, aggregates: { SumPlusOne } }
///
///#[scalar(name = "one")]
///fn return_one(args: &[Value]) -> Value {
///  return Value::from_integer(1);
///}
///
///#[derive(AggregateDerive)]
///struct SumPlusOne;
///
///impl AggFunc for SumPlusOne {
///   type State = i64;
///   const NAME: &'static str = "sum_plus_one";
///   const ARGS: i32 = 1;
///
///   fn step(state: &mut Self::State, args: &[Value]) {
///      let Some(val) = args[0].to_integer() else {
///        return;
///      };
///      *state += val;
///     }
///
///     fn finalize(state: Self::State) -> Value {
///        Value::from_integer(state + 1)
///     }
///}
///
/// ```
#[proc_macro]
pub fn register_extension(input: TokenStream) -> TokenStream {
    ext::register_extension(input)
}

/// Declare a scalar function for your extension. This requires the name:
/// #[scalar(name = "example")] of what you wish to call your function with.
/// ```ignore
/// use turso_ext::{scalar, Value};
/// #[scalar(name = "double", alias = "twice")] // you can provide an <optional> alias
/// fn double(args: &[Value]) -> Value {
///       let arg = args.get(0).unwrap();
///       match arg.value_type() {
///           ValueType::Float => {
///               let val = arg.to_float().unwrap();
///               Value::from_float(val * 2.0)
///           }
///           ValueType::Integer => {
///               let val = arg.to_integer().unwrap();
///               Value::from_integer(val * 2)
///           }
///       }
///   } else {
///       Value::null()
///   }
/// }
/// ```
#[proc_macro_attribute]
pub fn scalar(attr: TokenStream, input: TokenStream) -> TokenStream {
    ext::scalar(attr, input)
}

/// Define an aggregate function for your extension by deriving
/// AggregateDerive on a struct that implements the AggFunc trait.
/// ```ignore
/// use turso_ext::{register_extension, Value, AggregateDerive, AggFunc};
///
///#[derive(AggregateDerive)]
///struct SumPlusOne;
///
///impl AggFunc for SumPlusOne {
///   type State = i64;
///   type Error = &'static str;
///   const NAME: &'static str = "sum_plus_one";
///   const ARGS: i32 = 1;
///   fn step(state: &mut Self::State, args: &[Value]) {
///      let Some(val) = args[0].to_integer() else {
///        return;
///     };
///     *state += val;
///     }
///     fn finalize(state: Self::State) -> Result<Value, Self::Error> {
///        Ok(Value::from_integer(state + 1))
///     }
///}
/// ```
#[proc_macro_derive(AggregateDerive)]
pub fn derive_agg_func(input: TokenStream) -> TokenStream {
    ext::derive_agg_func(input)
}

/// Macro to derive a VTabModule for your extension. This macro will generate
/// the necessary functions to register your module with core. You must implement
/// the VTabModule, VTable, and VTabCursor traits.
/// ```ignore
/// #[derive(Debug, VTabModuleDerive)]
/// struct CsvVTabModule;
///
/// impl VTabModule for CsvVTabModule {
///  type Table = CsvTable;
///  const NAME: &'static str = "csv_data";
///  const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
///
///   /// Declare your virtual table and its schema
///  fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
///     let schema = "CREATE TABLE csv_data(
///             name TEXT,
///             age TEXT,
///             city TEXT
///         )".into();
///     Ok((schema, CsvTable {}))
///  }
/// }
///
/// struct CsvTable {}
///
/// // Implement the VTable trait for your virtual table
/// impl VTable for CsvTable {
///  type Cursor = CsvCursor;
///  type Error = &'static str;
///
///  /// Open the virtual table and return a cursor
///  fn open(&self) -> Result<Self::Cursor, Self::Error> {
///     let csv_content = fs::read_to_string("data.csv").unwrap_or_default();
///     let rows: Vec<Vec<String>> = csv_content
///         .lines()
///         .skip(1)
///         .map(|line| {
///             line.split(',')
///                 .map(|s| s.trim().to_string())
///                 .collect()
///         })
///         .collect();
///     Ok(CsvCursor { rows, index: 0 })
///  }
///
/// /// **Optional** methods for non-readonly tables:
///
///  /// Update the row with the provided values, return the new rowid
///  fn update(&mut self, rowid: i64, args: &[Value]) -> Result<Option<i64>, Self::Error> {
///      Ok(None)// return Ok(None) for read-only
///  }
///
///  /// Insert a new row with the provided values, return the new rowid
///  fn insert(&mut self, args: &[Value]) -> Result<(), Self::Error> {
///      Ok(()) //
///  }
///
///  /// Delete the row with the provided rowid
///  fn delete(&mut self, rowid: i64) -> Result<(), Self::Error> {
///    Ok(())
///  }
///
///  /// Destroy the virtual table. Any cleanup logic for when the table is deleted comes heres
///  fn destroy(&mut self) -> Result<(), Self::Error> {
///     Ok(())
///  }
/// }
///
///  #[derive(Debug)]
/// struct CsvCursor {
///   rows: Vec<Vec<String>>,
///   index: usize,
/// }
///
/// impl CsvCursor {
///   /// Returns the value for a given column index.
///   fn column(&self, idx: u32) -> Result<Value, Self::Error> {
///       let row = &self.rows[self.index];
///       if (idx as usize) < row.len() {
///           Value::from_text(&row[idx as usize])
///       } else {
///           Value::null()
///       }
///   }
/// }
///
/// // Implement the VTabCursor trait for your virtual cursor
/// impl VTabCursor for CsvCursor {
///  type Error = &'static str;
///
///  /// Filter the virtual table based on arguments (omitted here for simplicity)
///  fn filter(&mut self, _args: &[Value], _idx_info: Option<(&str, i32)>) -> ResultCode {
///      ResultCode::OK
///  }
///
///  /// Move the cursor to the next row
///  fn next(&mut self) -> ResultCode {
///     if self.index < self.rows.len() - 1 {
///         self.index += 1;
///         ResultCode::OK
///     } else {
///         ResultCode::EOF
///     }
///  }
///
///  fn eof(&self) -> bool {
///      self.index >= self.rows.len()
///  }
///
///  /// Return the value for a given column index
///  fn column(&self, idx: u32) -> Result<Value, Self::Error> {
///      self.column(idx)
///  }
///
///  fn rowid(&self) -> i64 {
///      self.index as i64
///  }
/// }
///
#[proc_macro_derive(VTabModuleDerive)]
pub fn derive_vtab_module(input: TokenStream) -> TokenStream {
    ext::derive_vtab_module(input)
}

/// ```ignore
/// use turso_ext::{ExtResult as Result, VfsDerive, VfsExtension, VfsFile};
///
/// // Your struct must also impl Default
/// #[derive(VfsDerive, Default)]
/// struct ExampleFS;
///
///
/// struct ExampleFile {
///    file: std::fs::File,
///
///
/// impl VfsExtension for ExampleFS {
///    /// The name of your vfs module
///    const NAME: &'static str = "example";
///
///    type File = ExampleFile;
///
///    fn open(&self, path: &str, flags: i32, _direct: bool) -> Result<Self::File> {
///        let file = OpenOptions::new()
///            .read(true)
///            .write(true)
///            .create(flags & 1 != 0)
///            .open(path)
///            .map_err(|_| ResultCode::Error)?;
///        Ok(TestFile { file })
///    }
///
///    fn run_once(&self) -> Result<()> {
///    // (optional) method to cycle/advance IO, if your extension is asynchronous
///        Ok(())
///    }
///
///    fn close(&self, file: Self::File) -> Result<()> {
///    // (optional) method to close or drop the file
///        Ok(())
///    }
///
///    fn generate_random_number(&self) -> i64 {
///    // (optional) method to generate random number. Used for testing
///        let mut buf = [0u8; 8];
///        getrandom::fill(&mut buf).unwrap();
///        i64::from_ne_bytes(buf)
///    }
///
///   fn get_current_time(&self) -> String {
///    // (optional) method to generate random number. Used for testing
///        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
///    }
///
///
/// impl VfsFile for ExampleFile {
///    fn read(
///        &mut self,
///        buf: &mut [u8],
///        count: usize,
///        offset: i64,
///    ) -> Result<i32> {
///        if file.file.seek(SeekFrom::Start(offset as u64)).is_err() {
///            return Err(ResultCode::Error);
///        }
///        file.file
///            .read(&mut buf[..count])
///            .map_err(|_| ResultCode::Error)
///            .map(|n| n as i32)
///    }
///
///    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> Result<i32> {
///        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
///            return Err(ResultCode::Error);
///        }
///        self.file
///            .write(&buf[..count])
///            .map_err(|_| ResultCode::Error)
///            .map(|n| n as i32)
///    }
///
///    fn sync(&self) -> Result<()> {
///        self.file.sync_all().map_err(|_| ResultCode::Error)
///    }
///
///    fn size(&self) -> i64 {
///      self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
///   }
///}
///
///```
#[proc_macro_derive(VfsDerive)]
pub fn derive_vfs_module(input: TokenStream) -> TokenStream {
    ext::derive_vfs_module(input)
}
