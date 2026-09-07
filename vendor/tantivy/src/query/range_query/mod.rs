use crate::schema::Type;

mod fast_field_range_doc_set;
mod range_query;
mod range_query_fastfield;

pub use common::bounds::BoundsRange;

pub use self::range_query::*;
pub use self::range_query_fastfield::*;

// TODO is this correct?
pub(crate) fn is_type_valid_for_fastfield_range_query(typ: Type) -> bool {
    match typ {
        Type::Str
        | Type::U64
        | Type::I64
        | Type::F64
        | Type::Bool
        | Type::Date
        | Type::Json
        | Type::IpAddr
        | Type::Bytes => true,
        Type::Facet => false,
    }
}
