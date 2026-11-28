//! The types that are the oid of an object of the catalog: regclass, the oid
//! of a relation, and regtype, the oid of a type.

use super::oid::oid_coerce;
use super::regtype::regtype_oid_of_text;
use super::{FormatName, RegisteredType};
use std::sync::Arc;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
use turso_core::{Connection, LimboError, Result, Value};

/// An oid alias value is the oid of the object, stored as that integer.
#[derive(Debug, Clone, Copy)]
pub(super) enum OidAlias {
    RegClass,
    RegType,
}

impl OidAlias {
    /// The oid of the object that `text` names. The input of an oid alias
    /// looks the object up, so it needs a connection.
    fn read(self, connection: Option<&Arc<Connection>>, text: &str) -> Result<Value> {
        let connection = connection.ok_or_else(|| {
            LimboError::InternalError(format!("the input of {} needs a connection", self.name()))
        })?;
        let oid = match self {
            OidAlias::RegClass => crate::catalog::regclass_oid_of_text(connection, text)?,
            OidAlias::RegType => regtype_oid_of_text(connection, text)?,
        };
        Ok(Value::from_i64(oid))
    }

    fn name(self) -> &'static str {
        match self {
            OidAlias::RegClass => "regclass",
            OidAlias::RegType => "regtype",
        }
    }
}

impl Type for OidAlias {
    fn name(&self) -> &str {
        OidAlias::name(*self)
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("any".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn input(&self) -> Option<TextInput> {
        Some(match self {
            OidAlias::RegClass => |connection, text| OidAlias::RegClass.read(connection, text),
            OidAlias::RegType => |connection, text| OidAlias::RegType.read(connection, text),
        })
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(oid_coerce))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Integer)
    }
}

impl RegisteredType for OidAlias {
    fn format_name(&self) -> FormatName {
        FormatName::Plain(OidAlias::name(*self))
    }
}
