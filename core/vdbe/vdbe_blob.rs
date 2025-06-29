use std::rc::Rc;

use crate::{storage::btree::BTreeCursor, Connection, Statement};

use super::CursorID;

pub struct BlobHandle {
    n_byte: i32,
    col_index: u16,
    cursor_id: CursorID,
    conn: Rc<Connection>,
    statement: Statement,
    // TODO: Add db_name field when functionality to add multiple databases is implemented
    table_name: String,
}

impl BlobHandle {
    fn new(
        n_byte: i32,
        col_index: u16,
        cursor_id: CursorID,
        conn: Rc<Connection>,
        table_name: String,
    ) -> Self {
        Self {
            n_byte,
            col_index,
            cursor_id,
            conn,
            statement,
            table_name,
        }
    }
}
