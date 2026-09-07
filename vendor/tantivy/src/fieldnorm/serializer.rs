use std::io;
use std::io::Write;

use crate::directory::{CompositeWrite, WritePtr};
use crate::schema::Field;

/// The fieldnorms serializer is in charge of
/// the serialization of field norms for all fields.
pub struct FieldNormsSerializer {
    composite_write: CompositeWrite,
}

impl FieldNormsSerializer {
    /// Constructor
    pub fn from_write(write: WritePtr) -> io::Result<FieldNormsSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(FieldNormsSerializer { composite_write })
    }

    /// Serialize the given field
    pub fn serialize_field(&mut self, field: Field, fieldnorms_data: &[u8]) -> io::Result<()> {
        let write = self.composite_write.for_field(field);
        write.write_all(fieldnorms_data)?;
        write.flush()?;
        Ok(())
    }

    /// Clean up / flush / close
    pub fn close(self) -> io::Result<()> {
        self.composite_write.close()?;
        Ok(())
    }
}
