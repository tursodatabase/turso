use std::sync::Arc;

use turso_core::native_ext::{FunctionArity, ScalarCall, ScalarFunction};
use turso_core::types::IOResultOr;
use turso_core::{
    Connection, ExternalFunc, IOResult, LimboError, OpenOptions, Register, Result, Value,
};

pub(crate) fn register_functions(mut options: OpenOptions) -> OpenOptions {
    for function in [AuthFunction::Uid, AuthFunction::Email, AuthFunction::Jwt] {
        options = options.extension_function(
            ExternalFunc::new_native_scalar(
                function.name().to_string(),
                FunctionArity::Exact(0),
                false,
                function,
            )
            .expect("auth function declarations have a valid argument count"),
        );
    }
    options
}

#[derive(Debug, Clone, Copy)]
enum AuthFunction {
    Uid,
    Email,
    Jwt,
}

impl AuthFunction {
    fn name(self) -> &'static str {
        match self {
            Self::Uid => "auth.uid",
            Self::Email => "auth.email",
            Self::Jwt => "auth.jwt",
        }
    }
}

impl ScalarFunction for AuthFunction {
    type Call = Self;

    fn create_call(&self) -> Result<Self::Call> {
        Ok(*self)
    }
}

impl ScalarCall for AuthFunction {
    fn step(&mut self, connection: &Arc<Connection>, _args: &[Register]) -> IOResultOr<Value> {
        let value = match self {
            Self::Uid => claim(connection, "request.jwt.claim.sub", "sub")?
                .map(|sub| canonical_uuid(&sub))
                .transpose()?,
            Self::Email => claim(connection, "request.jwt.claim.email", "email")?,
            Self::Jwt => jwt(connection)?,
        };
        Ok(IOResult::Done(value.map_or(Value::Null, Value::build_text)))
    }
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
            LimboError::InvalidArgument(format!("invalid input syntax for type uuid: \"{text}\""))
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
    let claims = serde_json::from_str(claims).map_err(|_| {
        LimboError::InvalidArgument("invalid input syntax for type json".to_string())
    })?;
    if has_nul_character(&claims) {
        return Err(LimboError::InvalidArgument(
            "unsupported Unicode escape sequence".to_string(),
        ));
    }
    Ok(claims)
}

fn has_nul_character(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => text.contains('\0'),
        serde_json::Value::Array(items) => items.iter().any(has_nul_character),
        serde_json::Value::Object(members) => members
            .iter()
            .any(|(name, value)| name.contains('\0') || has_nul_character(value)),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            false
        }
    }
}
