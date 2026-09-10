use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

use columnar::{
    BytesColumn, Column, ColumnType, ColumnValues, ColumnarReader, DynamicColumn,
    DynamicColumnHandle, HasAssociatedColumnType, StrColumn,
};
use common::ByteCount;

use crate::core::json_utils::{encode_column_name, json_path_sep_to_dot};
use crate::directory::FileSlice;
use crate::schema::{Field, FieldEntry, FieldType, Schema};
use crate::space_usage::{FieldUsage, PerFieldSpaceUsage};
use crate::TantivyError;

/// Provides access to all of the BitpackedFastFieldReader.
///
/// Internally, `FastFieldReaders` have preloaded fast field readers,
/// and just wraps several `HashMap`.
#[derive(Clone)]
pub struct FastFieldReaders {
    columnar: Arc<ColumnarReader>,
    schema: Schema,
}

impl FastFieldReaders {
    pub(crate) fn open(fast_field_file: FileSlice, schema: Schema) -> io::Result<FastFieldReaders> {
        let columnar = Arc::new(ColumnarReader::open(fast_field_file)?);
        Ok(FastFieldReaders { columnar, schema })
    }

    fn resolve_field(&self, column_name: &str) -> crate::Result<Option<String>> {
        let default_field_opt: Option<Field> = if cfg!(feature = "quickwit") {
            self.schema.get_field("_dynamic").ok()
        } else {
            None
        };
        self.resolve_column_name_given_default_field(column_name, default_field_opt)
    }

    pub(crate) fn space_usage(&self) -> io::Result<PerFieldSpaceUsage> {
        let mut per_field_usages: Vec<FieldUsage> = Default::default();
        for (mut field_name, column_handle) in self.columnar.iter_columns()? {
            json_path_sep_to_dot(&mut field_name);
            let space_usage = column_handle.space_usage()?;
            let mut field_usage = FieldUsage::empty(field_name);
            field_usage.set_column_usage(space_usage);
            per_field_usages.push(field_usage);
        }
        Ok(PerFieldSpaceUsage::new(per_field_usages))
    }

    pub(crate) fn columnar(&self) -> &ColumnarReader {
        self.columnar.as_ref()
    }

    /// Transforms a user-supplied fast field name into a column name.
    ///
    /// A user-supplied fast field name is not necessarily a schema field name
    /// because we handle fast fields.
    ///
    /// For instance, if the documents look like `{.., "attributes": {"color": "red"}}` and
    /// `attributes` is a json fast field,  a user could want to run a term aggregation over
    /// colors, by referring to the field as `attributes.color`.
    ///
    /// This function transforms `attributes.color` into a column key to be used in the `columnar`.
    ///
    /// The logic works as follows, first we identify which field is targeted by calling
    /// `schema.find_field(..)`. This method will attempt to split the user splied fast field
    /// name by non-escaped dots, and find the longest matching schema field name.
    /// In our case, it would return the (attribute_field, "color").
    ///
    /// If no field is found, but a dynamic field is supplied, then we
    /// will simply assume the user is targeting the dynamic field. (This feature is used in
    /// Quickwit.)
    ///
    /// We then encode the `(field, path)` into the right `columnar_key`.
    fn resolve_column_name_given_default_field<'a>(
        &'a self,
        field_name: &'a str,
        default_field_opt: Option<Field>,
    ) -> crate::Result<Option<String>> {
        let Some((field, path)): Option<(Field, &str)> = self
            .schema
            .find_field_with_default(field_name, default_field_opt)
        else {
            return Ok(None);
        };
        let field_entry: &FieldEntry = self.schema.get_field_entry(field);
        if !field_entry.is_fast() {
            return Err(TantivyError::InvalidArgument(format!(
                "Field {field_name:?} is not configured as fast field"
            )));
        }
        Ok(match (field_entry.field_type(), path) {
            (FieldType::JsonObject(json_options), path) if !path.is_empty() => {
                Some(encode_column_name(
                    field_entry.name(),
                    path,
                    json_options.is_expand_dots_enabled(),
                ))
            }
            (_, "") => Some(field_entry.name().to_string()),
            _ => None,
        })
    }

    /// Returns a typed column associated to a given field name.
    ///
    /// If no column associated with that field_name exists,
    /// or existing columns do not have the required type,
    /// returns `None`.
    pub fn column_opt<T>(&self, field_name: &str) -> crate::Result<Option<Column<T>>>
    where
        T: HasAssociatedColumnType,
        DynamicColumn: Into<Option<Column<T>>>,
    {
        let Some(dynamic_column_handle) =
            self.dynamic_column_handle(field_name, T::column_type())?
        else {
            return Ok(None);
        };
        let dynamic_column = dynamic_column_handle.open()?;
        Ok(dynamic_column.into())
    }

    /// Returns the number of `bytes` associated with a column.
    ///
    /// Returns 0 if the column does not exist.
    pub fn column_num_bytes(&self, field: &str) -> crate::Result<ByteCount> {
        let Some(resolved_field_name) = self.resolve_field(field)? else {
            return Ok(0u64.into());
        };
        Ok(self
            .columnar
            .read_columns(&resolved_field_name)?
            .into_iter()
            .map(|column_handle| column_handle.num_bytes())
            .sum())
    }

    /// Returns a typed column value object.
    ///
    /// In that column value:
    /// - Rows with no value are associated with the default value.
    /// - Rows with several values are associated with the first value.
    pub fn column_first_or_default<T>(&self, field: &str) -> crate::Result<Arc<dyn ColumnValues<T>>>
    where
        T: PartialOrd + Copy + HasAssociatedColumnType + Send + Sync + 'static,
        DynamicColumn: Into<Option<Column<T>>>,
    {
        let col: Column<T> = self.column(field)?;
        Ok(col.first_or_default_col(T::default_value()))
    }

    /// Returns a typed column associated to a given field name.
    ///
    /// Returns an error if no column associated with that field_name exists.
    fn column<T>(&self, field: &str) -> crate::Result<Column<T>>
    where
        T: PartialOrd + Copy + HasAssociatedColumnType + Send + Sync + 'static,
        DynamicColumn: Into<Option<Column<T>>>,
    {
        let col_opt: Option<Column<T>> = self.column_opt(field)?;
        col_opt.ok_or_else(|| {
            crate::TantivyError::SchemaError(format!(
                "Field `{field}` is missing or is not configured as a fast field."
            ))
        })
    }

    /// Returns the `u64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a u64 fast field, this method returns an Error.
    pub fn u64(&self, field: &str) -> crate::Result<Column<u64>> {
        self.column(field)
    }

    /// Returns the `date` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a date fast field, this method returns an Error.
    pub fn date(&self, field: &str) -> crate::Result<Column<common::DateTime>> {
        self.column(field)
    }

    /// Returns the `ip` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 fast field, this method returns an Error.
    pub fn ip_addr(&self, field: &str) -> crate::Result<Column<Ipv6Addr>> {
        self.column(field)
    }

    /// Returns a `str` column.
    pub fn str(&self, field_name: &str) -> crate::Result<Option<StrColumn>> {
        let Some(dynamic_column_handle) =
            self.dynamic_column_handle(field_name, ColumnType::Str)?
        else {
            return Ok(None);
        };
        let dynamic_column = dynamic_column_handle.open()?;
        Ok(dynamic_column.into())
    }

    /// Returns a `bytes` column.
    pub fn bytes(&self, field_name: &str) -> crate::Result<Option<BytesColumn>> {
        let Some(dynamic_column_handle) =
            self.dynamic_column_handle(field_name, ColumnType::Bytes)?
        else {
            return Ok(None);
        };
        let dynamic_column = dynamic_column_handle.open()?;
        Ok(dynamic_column.into())
    }

    /// Returns a `dynamic_column_handle`.
    pub fn dynamic_column_handle(
        &self,
        field_name: &str,
        column_type: ColumnType,
    ) -> crate::Result<Option<DynamicColumnHandle>> {
        let Some(resolved_field_name) = self.resolve_field(field_name)? else {
            return Ok(None);
        };
        let dynamic_column_handle_opt = self
            .columnar
            .read_columns(&resolved_field_name)?
            .into_iter()
            .find(|column| column.column_type() == column_type);
        Ok(dynamic_column_handle_opt)
    }

    /// Returns all `dynamic_column_handle` that match the given field name.
    pub fn dynamic_column_handles(
        &self,
        field_name: &str,
    ) -> crate::Result<Vec<DynamicColumnHandle>> {
        let Some(resolved_field_name) = self.resolve_field(field_name)? else {
            return Ok(Vec::new());
        };
        let dynamic_column_handles = self
            .columnar
            .read_columns(&resolved_field_name)?
            .into_iter()
            .collect();
        Ok(dynamic_column_handles)
    }

    /// Returns all `dynamic_column_handle` that are inner fields of the provided JSON path.
    pub fn dynamic_subpath_column_handles(
        &self,
        root_path: &str,
    ) -> crate::Result<Vec<DynamicColumnHandle>> {
        let Some(resolved_field_name) = self.resolve_field(root_path)? else {
            return Ok(Vec::new());
        };
        let dynamic_column_handles = self
            .columnar
            .read_subpath_columns(&resolved_field_name)?
            .into_iter()
            .collect();
        Ok(dynamic_column_handles)
    }

    #[doc(hidden)]
    pub async fn list_dynamic_column_handles(
        &self,
        field_name: &str,
    ) -> crate::Result<Vec<DynamicColumnHandle>> {
        let Some(resolved_field_name) = self.resolve_field(field_name)? else {
            return Ok(Vec::new());
        };
        let columns = self
            .columnar
            .read_columns_async(&resolved_field_name)
            .await?;
        Ok(columns)
    }

    #[doc(hidden)]
    pub async fn list_subpath_dynamic_column_handles(
        &self,
        root_path: &str,
    ) -> crate::Result<Vec<DynamicColumnHandle>> {
        let Some(resolved_field_name) = self.resolve_field(root_path)? else {
            return Ok(Vec::new());
        };
        let columns = self
            .columnar
            .read_subpath_columns_async(&resolved_field_name)
            .await?;
        Ok(columns)
    }

    /// Returns the `u64` column used to represent any `u64`-mapped typed (String/Bytes term ids,
    /// i64, u64, f64, DateTime).
    ///
    /// Returns Ok(None) for empty columns
    #[doc(hidden)]
    pub fn u64_lenient_for_type(
        &self,
        type_white_list_opt: Option<&[ColumnType]>,
        field_name: &str,
    ) -> crate::Result<Option<(Column<u64>, ColumnType)>> {
        let Some(resolved_field_name) = self.resolve_field(field_name)? else {
            return Ok(None);
        };
        for col in self.columnar.read_columns(&resolved_field_name)? {
            if let Some(type_white_list) = type_white_list_opt {
                if !type_white_list.contains(&col.column_type()) {
                    continue;
                }
            }
            if let Some(col_u64) = col.open_u64_lenient()? {
                return Ok(Some((col_u64, col.column_type())));
            }
        }
        Ok(None)
    }

    /// Returns the all `u64` column used to represent any `u64`-mapped typed (String/Bytes term
    /// ids, i64, u64, f64, bool, DateTime).
    ///
    /// In case of JSON, there may be two columns. One for term and one for numerical types. (This
    /// may change later to 3 types if JSON handles DateTime)
    #[doc(hidden)]
    pub fn u64_lenient_for_type_all(
        &self,
        type_white_list_opt: Option<&[ColumnType]>,
        field_name: &str,
    ) -> crate::Result<Vec<(Column<u64>, ColumnType)>> {
        let mut columns_and_types = Vec::new();
        let Some(resolved_field_name) = self.resolve_field(field_name)? else {
            return Ok(columns_and_types);
        };
        for col in self.columnar.read_columns(&resolved_field_name)? {
            if let Some(type_white_list) = type_white_list_opt {
                if !type_white_list.contains(&col.column_type()) {
                    continue;
                }
            }
            if let Some(col_u64) = col.open_u64_lenient()? {
                columns_and_types.push((col_u64, col.column_type()));
            }
        }
        Ok(columns_and_types)
    }

    /// Returns the `u64` column used to represent any `u64`-mapped typed (i64, u64, f64, DateTime).
    ///
    /// Returns Ok(None) for empty columns
    #[doc(hidden)]
    pub fn u64_lenient(
        &self,
        field_name: &str,
    ) -> crate::Result<Option<(Column<u64>, ColumnType)>> {
        self.u64_lenient_for_type(None, field_name)
    }

    /// Returns the `i64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a i64 fast field, this method returns an Error.
    pub fn i64(&self, field_name: &str) -> crate::Result<Column<i64>> {
        self.column(field_name)
    }

    /// Returns the `f64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a f64 fast field, this method returns an Error.
    pub fn f64(&self, field_name: &str) -> crate::Result<Column<f64>> {
        self.column(field_name)
    }

    /// Returns the `bool` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a bool fast field, this method returns an Error.
    pub fn bool(&self, field_name: &str) -> crate::Result<Column<bool>> {
        self.column(field_name)
    }
}

#[cfg(test)]
mod tests {
    use columnar::ColumnType;

    use crate::schema::{JsonObjectOptions, Schema, FAST};
    use crate::{Index, IndexWriter, TantivyDocument};

    #[test]
    fn test_fast_field_reader_resolve_with_dynamic_internal() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("age", FAST);
        schema_builder.add_json_field("json_expand_dots_disabled", FAST);
        schema_builder.add_json_field(
            "json_expand_dots_enabled",
            JsonObjectOptions::default()
                .set_fast(None)
                .set_expand_dots_enabled(),
        );
        let dynamic_field = schema_builder.add_json_field("_dyna", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(TantivyDocument::default())
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let reader = searcher.segment_reader(0u32);
        let fast_field_readers = reader.fast_fields();
        assert_eq!(
            fast_field_readers
                .resolve_column_name_given_default_field("age", None)
                .unwrap(),
            Some("age".to_string())
        );
        assert_eq!(
            fast_field_readers
                .resolve_column_name_given_default_field("age", Some(dynamic_field))
                .unwrap(),
            Some("age".to_string())
        );
        assert_eq!(
            fast_field_readers
                .resolve_column_name_given_default_field(
                    "json_expand_dots_disabled.attr.color",
                    None
                )
                .unwrap(),
            Some("json_expand_dots_disabled\u{1}attr\u{1}color".to_string())
        );
        assert_eq!(
            fast_field_readers
                .resolve_column_name_given_default_field(
                    "json_expand_dots_disabled.attr\\.color",
                    Some(dynamic_field)
                )
                .unwrap(),
            Some("json_expand_dots_disabled\u{1}attr.color".to_string())
        );
        assert_eq!(
            fast_field_readers
                .resolve_column_name_given_default_field(
                    "json_expand_dots_enabled.attr\\.color",
                    Some(dynamic_field)
                )
                .unwrap(),
            Some("json_expand_dots_enabled\u{1}attr\u{1}color".to_string())
        );
        assert_eq!(
            fast_field_readers
                .resolve_column_name_given_default_field("notinschema.attr.color", None)
                .unwrap(),
            None
        );
        assert_eq!(
            fast_field_readers
                .resolve_column_name_given_default_field(
                    "notinschema.attr.color",
                    Some(dynamic_field)
                )
                .unwrap(),
            Some("_dyna\u{1}notinschema\u{1}attr\u{1}color".to_string())
        );
    }

    #[test]
    fn test_fast_field_reader_dynamic_column_handles() {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", FAST);
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(id=> 1u64, json => json!({"foo": 42})))
            .unwrap();
        index_writer
            .add_document(doc!(id=> 2u64, json => json!({"foo": true})))
            .unwrap();
        index_writer
            .add_document(doc!(id=> 3u64, json => json!({"foo": "bar"})))
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let reader = searcher.segment_reader(0u32);
        let fast_fields = reader.fast_fields();
        let id_columns = fast_fields.dynamic_column_handles("id").unwrap();
        assert_eq!(id_columns.len(), 1);
        assert_eq!(id_columns.first().unwrap().column_type(), ColumnType::U64);

        let foo_columns = fast_fields.dynamic_column_handles("json.foo").unwrap();
        assert_eq!(foo_columns.len(), 3);
        assert!(foo_columns
            .iter()
            .any(|column| column.column_type() == ColumnType::I64));
        assert!(foo_columns
            .iter()
            .any(|column| column.column_type() == ColumnType::Bool));
        assert!(foo_columns
            .iter()
            .any(|column| column.column_type() == ColumnType::Str));

        let json_columns = fast_fields.dynamic_column_handles("json").unwrap();
        assert_eq!(json_columns.len(), 0);

        let json_subcolumns = fast_fields.dynamic_subpath_column_handles("json").unwrap();
        assert_eq!(json_subcolumns.len(), 3);

        let foo_subcolumns = fast_fields
            .dynamic_subpath_column_handles("json.foo")
            .unwrap();
        assert_eq!(foo_subcolumns.len(), 0);
    }
}
