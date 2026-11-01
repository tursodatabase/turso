use crate::error::sqlstate_error;
use crate::functions::{pg_scalar_function, Uuid};
use turso_core::native_ext::FunctionArity;
use turso_core::{Connection, OpenOptions, Result};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_UNTRANSLATABLE_CHARACTER,
};

pub(crate) fn register_functions(options: OpenOptions) -> OpenOptions {
    options
        .extension_function(pg_scalar_function(
            "auth.uid",
            FunctionArity::Exact(0),
            false,
            |connection, _| {
                claim(connection, "request.jwt.claim.sub", "sub")?
                    .map(|sub| canonical_uuid(&sub).map(Uuid))
                    .transpose()
            },
        ))
        .extension_function(pg_scalar_function(
            "auth.email",
            FunctionArity::Exact(0),
            false,
            |connection, _| claim(connection, "request.jwt.claim.email", "email"),
        ))
        .extension_function(pg_scalar_function(
            "auth.jwt",
            FunctionArity::Exact(0),
            false,
            |connection, _| jwt(connection),
        ))
}

fn claim(conn: &Connection, setting_name: &str, claim_name: &str) -> Result<Option<String>> {
    if let Some(value) = setting(conn, setting_name) {
        return Ok(Some(value));
    }
    let Some(claims) = setting(conn, "request.jwt.claims") else {
        return Ok(None);
    };
    Ok(match parse_claims(&claims)?.get(claim_name) {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(value)) => Some(value.clone()),
        Some(value) => Some(value.to_string()),
    })
}

fn canonical_uuid(text: &str) -> Result<String> {
    uuid::Uuid::parse_str(text)
        .map(|uuid| uuid.hyphenated().to_string())
        .map_err(|_| {
            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!("invalid input syntax for type uuid: \"{text}\""),
            )
        })
}

fn jwt(conn: &Connection) -> Result<Option<String>> {
    let Some(claims) =
        setting(conn, "request.jwt.claim").or_else(|| setting(conn, "request.jwt.claims"))
    else {
        return Ok(None);
    };
    parse_claims(&claims)?;
    Ok(Some(claims))
}

fn setting(conn: &Connection, name: &str) -> Option<String> {
    crate::session::custom_parameter(conn, name).filter(|value| !value.is_empty())
}

fn parse_claims(claims: &str) -> Result<serde_json::Value> {
    let parsed_claims = serde_json::from_str(claims).map_err(|_| {
        sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            "invalid input syntax for type json",
        )
    })?;
    if has_nul_escape(claims) {
        return Err(sqlstate_error(
            SQLSTATE_UNTRANSLATABLE_CHARACTER,
            "unsupported Unicode escape sequence",
        ));
    }
    Ok(parsed_claims)
}

fn has_nul_escape(json: &str) -> bool {
    let mut bytes = json.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'\\' && bytes.next() == Some(b'u') && bytes.by_ref().take(4).eq(*b"0000") {
            return true;
        }
    }
    false
}
