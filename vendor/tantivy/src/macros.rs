/// `doc!` is a shortcut that helps building `Document`
/// objects.
///
/// Assuming that `field1` and `field2` are `Field` instances.
/// You can create a document with a value of `value1` for `field1`
/// `value2` for `field2`, as follows :
///
/// ```c
/// doc!(
///     field1 => value1,
///     field2 => value2,
/// )
/// ```
///
/// The value can be a `u64`, a `&str`, a `i64`, or a `String`.
///
/// # Warning
///
/// The document hence created, is not yet validated against a schema.
/// Nothing prevents its user from creating an invalid document missing a
/// field, or associating a `String` to a `u64` field for instance.
///
/// # Example
///
/// ```rust
/// use tantivy::schema::{Schema, TEXT, FAST};
/// use tantivy::doc;
///
/// //...
///
/// # fn main() {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let author = schema_builder.add_text_field("text", TEXT);
/// let likes = schema_builder.add_u64_field("num_u64", FAST);
/// let schema = schema_builder.build();
/// let doc = doc!(
///     title => "Life Aquatic",
///     author => "Wes Anderson",
///     likes => 4u64
/// );
/// # }
/// ```
#[macro_export]
macro_rules! doc(
    () => {
        {
            ($crate::TantivyDocument::default())
        }
    }; // avoids a warning due to the useless `mut`.
    ($($field:expr => $value:expr),*) => {
        {
            let mut document = $crate::TantivyDocument::default();
            $(
                document.add_field_value($field, &$value);
            )*
            document
        }
    };
    // if there is a trailing comma retry with the trailing comma stripped.
    ($($field:expr => $value:expr),+ ,) => {
        doc!( $( $field => $value ), *)
    };
);

#[cfg(test)]
mod test {
    use crate::schema::{Schema, FAST, TEXT};

    #[test]
    fn test_doc_basic() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let author = schema_builder.add_text_field("text", TEXT);
        let likes = schema_builder.add_u64_field("num_u64", FAST);
        let _schema = schema_builder.build();
        let _doc = doc!(
        title => "Life Aquatic",
        author => "Wes Anderson",
        likes => 4u64
        );
    }

    #[test]
    fn test_doc_trailing_comma() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let author = schema_builder.add_text_field("text", TEXT);
        let likes = schema_builder.add_u64_field("num_u64", FAST);
        let _schema = schema_builder.build();
        let _doc = doc!(
        title => "Life Aquatic",
        author => "Wes Anderson",
        likes => 4u64,
        );
    }
}
