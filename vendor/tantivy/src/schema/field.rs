use std::io;
use std::io::{Read, Write};

use common::BinarySerializable;

/// `Field` is represented by an unsigned 32-bit integer type.
/// The schema holds the mapping between field names and `Field` objects.
#[derive(
    Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Field(u32);

impl Field {
    /// Create a new field object for the given FieldId.
    pub const fn from_field_id(field_id: u32) -> Field {
        Field(field_id)
    }

    /// Returns a u32 identifying uniquely a field within a schema.
    pub const fn field_id(self) -> u32 {
        self.0
    }
}

impl BinarySerializable for Field {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.0.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Field> {
        u32::deserialize(reader).map(Field)
    }
}
