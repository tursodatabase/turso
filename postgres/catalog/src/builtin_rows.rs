//! The built-in rows of each catalog: the rows of its table as the values of a
//! [`BuiltinCatalog`](crate::BuiltinCatalog).
//!
//! The generated file has, for each catalog, how a row writes its values
//! ([`CatalogRow`]) and a [`CatalogRows`] over its table. The documentation
//! of `turso_pg_catalog_gen` gives the value of each type.

use turso_core::Value;

use crate::builtin_catalog::BuiltinRows;

use crate::text_out::{float4_as_f64, push_array, push_vector, Float4};
use crate::Oid;

/// A row of the table of a catalog.
pub(crate) trait CatalogRow: Sync + 'static {
    /// Writes the values of the row, in the order of the columns of the
    /// `CREATE_SQL` of the catalog, to the end of `out`.
    fn write_values(&self, out: &mut Vec<Value>);
}

/// The built-in rows of a catalog, over its table.
pub(crate) struct CatalogRows<R: 'static> {
    rows: &'static [R],
    /// The oid of a row, when `oid` is the `INTEGER PRIMARY KEY` and the
    /// rows are in the order of their oids.
    oid: Option<fn(&R) -> Oid>,
    version: u32,
}

impl<R> CatalogRows<R> {
    pub(crate) const fn new(rows: &'static [R], oid: Option<fn(&R) -> Oid>, version: u32) -> Self {
        Self { rows, oid, version }
    }
}

impl<R: CatalogRow> BuiltinRows for CatalogRows<R> {
    fn len(&self) -> usize {
        self.rows.len()
    }

    fn row(&self, index: usize, out: &mut Vec<Value>) {
        self.rows[index].write_values(out);
    }

    fn rowid_column(&self) -> Option<usize> {
        // The generator writes `oid` as the first column.
        self.oid.map(|_| 0)
    }

    fn find(&self, rowid: i64) -> Option<usize> {
        match self.oid {
            Some(oid) => {
                let key = Oid::try_from(rowid).ok()?;
                self.rows.binary_search_by_key(&key, oid).ok()
            }
            None => usize::try_from(rowid)
                .ok()?
                .checked_sub(1)
                .filter(|index| *index < self.rows.len()),
        }
    }

    fn version(&self) -> u32 {
        self.version
    }
}

/// The value of a column of a row.
pub(crate) fn value(value: impl ToValue) -> Value {
    value.to_value()
}

/// A field of a row that is a column of the built-in catalog.
pub(crate) trait ToValue {
    fn to_value(self) -> Value;
}

/// An `oidvector` or `int2vector`, whose text separates the elements with
/// spaces.
pub(crate) struct Vector<T: 'static>(pub(crate) &'static [T]);

/// An array, whose text has the elements in braces.
pub(crate) struct Array<T: 'static>(pub(crate) &'static [T]);

impl<T: ToValue> ToValue for Option<T> {
    fn to_value(self) -> Value {
        self.map_or(Value::Null, ToValue::to_value)
    }
}

/// An oid or an xid.
impl ToValue for u32 {
    fn to_value(self) -> Value {
        Value::from_i64(i64::from(self))
    }
}

impl ToValue for i16 {
    fn to_value(self) -> Value {
        Value::from_i64(i64::from(self))
    }
}

impl ToValue for i32 {
    fn to_value(self) -> Value {
        Value::from_i64(i64::from(self))
    }
}

impl ToValue for i64 {
    fn to_value(self) -> Value {
        Value::from_i64(self)
    }
}

impl ToValue for f32 {
    fn to_value(self) -> Value {
        Value::from_f64(float4_as_f64(self))
    }
}

impl ToValue for bool {
    fn to_value(self) -> Value {
        Value::from_i64(i64::from(self))
    }
}

/// A `"char"`, which the generator makes sure is ASCII.
impl ToValue for char {
    fn to_value(self) -> Value {
        assert!(
            self.is_ascii(),
            "a \"char\" of a catalog is ASCII: {self:?}"
        );
        let byte = self as usize;
        Value::from_text(&ASCII[byte..byte + 1])
    }
}

impl ToValue for &'static str {
    fn to_value(self) -> Value {
        Value::from_text(self)
    }
}

impl<T: std::fmt::Display> ToValue for Vector<T> {
    fn to_value(self) -> Value {
        let mut text = String::new();
        push_vector(self.0, &mut text);
        Value::from_text(text)
    }
}

impl ToValue for Array<Oid> {
    fn to_value(self) -> Value {
        array_value(self.0)
    }
}

impl ToValue for Array<i16> {
    fn to_value(self) -> Value {
        array_value(self.0)
    }
}

impl ToValue for Array<char> {
    fn to_value(self) -> Value {
        array_value(self.0)
    }
}

impl ToValue for Array<&'static str> {
    fn to_value(self) -> Value {
        array_value(self.0)
    }
}

/// An array of `float4`, whose elements have the text of `float4out`.
impl ToValue for Array<f32> {
    fn to_value(self) -> Value {
        let elements: Vec<Float4> = self.0.iter().map(|value| Float4(*value)).collect();
        array_value(&elements)
    }
}

fn array_value<T: std::fmt::Display>(elements: &[T]) -> Value {
    let mut text = String::new();
    push_array(elements, &mut text);
    Value::from_text(text)
}

/// Each ASCII character, at its own position, so that the text of a
/// `"char"` is a slice of it and needs no allocation.
static ASCII: &str = match std::str::from_utf8(&ASCII_BYTES) {
    Ok(text) => text,
    Err(_) => panic!("ASCII is UTF-8"),
};

const ASCII_BYTES: [u8; 128] = {
    let mut bytes = [0; 128];
    let mut byte = 0;
    while byte < 128 {
        bytes[byte] = byte as u8;
        byte += 1;
    }
    bytes
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_type_has_its_value() {
        assert_eq!(value(16u32), Value::from_i64(16));
        assert_eq!(value(u32::MAX), Value::from_i64(4_294_967_295));
        assert_eq!(value(-1i16), Value::from_i64(-1));
        assert_eq!(value(-1i32), Value::from_i64(-1));
        assert_eq!(value(i64::MIN), Value::from_i64(i64::MIN));
        assert_eq!(value(0.01f32), Value::from_f64(0.01));
        assert_eq!(value(true), Value::from_i64(1));
        assert_eq!(value(false), Value::from_i64(0));
        assert_eq!(value('b'), Value::from_text("b"));
        assert_eq!(value(','), Value::from_text(","));
        assert_eq!(value("bool"), Value::from_text("bool"));
        assert_eq!(value(None::<&'static str>), Value::Null);
        assert_eq!(value(Some(7u32)), Value::from_i64(7));
        assert_eq!(value(Vector::<Oid>(&[23, 23])), Value::from_text("23 23"));
        assert_eq!(value(Vector::<Oid>(&[])), Value::from_text(""));
        assert_eq!(value(Vector::<i16>(&[1, -2])), Value::from_text("1 -2"));
        assert_eq!(
            value(Array::<Oid>(&[25, 20, 1184])),
            Value::from_text("{25,20,1184}")
        );
        assert_eq!(value(Array::<i16>(&[1, 2])), Value::from_text("{1,2}"));
        assert_eq!(value(Array(&['i', 'o'])), Value::from_text("{i,o}"));
        assert_eq!(
            value(Array::<&str>(&["a b", "NULL", "x"])),
            Value::from_text(r#"{"a b","NULL",x}"#)
        );
        assert_eq!(
            value(Array::<f32>(&[0.5, 1e10])),
            Value::from_text("{0.5,1e+10}")
        );
        assert_eq!(value(Some(Array::<Oid>(&[]))), Value::from_text("{}"));
    }

    #[test]
    fn the_ascii_table_has_each_character_at_its_position() {
        for byte in 0u8..128 {
            assert_eq!(ASCII.as_bytes()[usize::from(byte)], byte);
        }
    }

    struct Row(Oid);

    impl CatalogRow for Row {
        fn write_values(&self, out: &mut Vec<Value>) {
            out.push(value(self.0));
        }
    }

    static ROWS: [Row; 3] = [Row(10), Row(20), Row(30)];

    #[test]
    fn find_searches_the_oid_or_takes_the_position() {
        let by_oid = CatalogRows::new(&ROWS, Some(|row: &Row| row.0), 7);
        assert_eq!(by_oid.len(), 3);
        assert_eq!(by_oid.version(), 7);
        assert_eq!(by_oid.find(20), Some(1));
        assert_eq!(by_oid.find(10), Some(0));
        assert_eq!(by_oid.find(25), None);
        assert_eq!(by_oid.find(-20), None);
        assert_eq!(by_oid.find(i64::from(u32::MAX) + 21), None);

        let by_position = CatalogRows::new(&ROWS, None, 7);
        assert_eq!(by_position.find(1), Some(0));
        assert_eq!(by_position.find(3), Some(2));
        assert_eq!(by_position.find(0), None);
        assert_eq!(by_position.find(4), None);
        assert_eq!(by_position.find(-1), None);
        assert_eq!(by_position.find(i64::MIN), None);

        let mut out = Vec::new();
        by_oid.row(2, &mut out);
        assert_eq!(out, [Value::from_i64(30)]);
    }
}
