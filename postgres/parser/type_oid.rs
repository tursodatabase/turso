//! The oids of the PostgreSQL types that tursopg knows by name.

/// The oid of `boolean`.
pub const BOOL_OID: u32 = 16;

/// The oid of the PostgreSQL type named `name`, or None for a name tursopg
/// does not know. `name` is a PostgreSQL type name such as `int2` or `jsonb`,
/// or the name tursopg stores a column type under, such as `INTEGER`. A type
/// modifier in parentheses, as in `varchar(100)`, is left out of the lookup.
///
/// The type annotation of a cast, the type of a result column that the server
/// sends, and the type oids of the catalog all come from this function, so
/// that they agree.
pub fn type_oid(name: &str) -> Option<u32> {
    let base = match name.find('(') {
        Some(pos) => &name[..pos],
        None => name,
    };
    let oid = match base.to_uppercase().as_str() {
        "INTEGER" | "INT" | "INT4" => 23,
        "SMALLINT" | "INT2" => 21,
        "BIGINT" | "INT8" => 20,
        "TINYINT" | "MEDIUMINT" => 23,
        "TEXT" => 25,
        "VARCHAR" | "CHAR" | "CLOB" | "NCHAR" | "NVARCHAR" | "CHARACTER VARYING" => 1043,
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" | "FLOAT8" => 701,
        "FLOAT4" => 700,
        "BLOB" | "BYTEA" => 17,
        "NUMERIC" | "DECIMAL" => 1700,
        "BOOLEAN" | "BOOL" => BOOL_OID,
        "UUID" => 2950,
        "JSON" => 114,
        "JSONB" => 3802,
        "DATE" => 1082,
        "TIME" => 1083,
        "TIMESTAMP" => 1114,
        "TIMESTAMPTZ" => 1184,
        "INET" => 869,
        "CIDR" => 650,
        "MACADDR" => 829,
        "OID" => 26,
        _ => return None,
    };
    Some(oid)
}
