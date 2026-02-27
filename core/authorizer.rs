use crate::{Connection, LimboError, Result};

/// Action codes for the authorizer callback, matching SQLite's integer codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum AuthAction {
    CreateIndex = 1,
    CreateTable = 2,
    CreateTempIndex = 3,
    CreateTempTable = 4,
    CreateTempTrigger = 5,
    CreateTempView = 6,
    CreateTrigger = 7,
    CreateView = 8,
    Delete = 9,
    DropIndex = 10,
    DropTable = 11,
    DropTempIndex = 12,
    DropTempTable = 13,
    DropTempTrigger = 14,
    DropTempView = 15,
    DropTrigger = 16,
    DropView = 17,
    Insert = 18,
    Pragma = 19,
    Read = 20,
    Select = 21,
    Transaction = 22,
    Update = 23,
    Attach = 24,
    Detach = 25,
    AlterTable = 26,
    Reindex = 27,
    Analyze = 28,
    CreateVtable = 29,
    DropVtable = 30,
    Function = 31,
    Savepoint = 32,
    Recursive = 33,
}

/// Result codes from the authorizer callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum AuthResult {
    Ok = 0,
    Deny = 1,
    Ignore = 2,
}

impl AuthResult {
    pub fn from_i32(val: i32) -> Self {
        match val {
            0 => AuthResult::Ok,
            1 => AuthResult::Deny,
            2 => AuthResult::Ignore,
            _ => AuthResult::Deny,
        }
    }
}

/// Type alias for the authorizer callback.
///
/// Parameters:
/// - action: AuthAction code
/// - arg3: First string argument (meaning depends on action code)
/// - arg4: Second string argument (meaning depends on action code)
/// - db_name: Database name ("main", "temp", or attached db alias)
/// - trigger_or_view: Name of innermost trigger/view, or None
///
/// Returns: AuthResult
pub type AuthorizerCallback = Box<
    dyn Fn(AuthAction, Option<&str>, Option<&str>, Option<&str>, Option<&str>) -> AuthResult
        + Send
        + Sync,
>;

/// Represents the authorizer configuration for a connection.
pub enum Authorizer {
    /// No authorizer set (default) - all operations allowed.
    None,
    /// Custom authorizer callback.
    Custom { callback: AuthorizerCallback },
}

impl Default for Authorizer {
    fn default() -> Self {
        Authorizer::None
    }
}

impl std::fmt::Debug for Authorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Authorizer::None => write!(f, "Authorizer::None"),
            Authorizer::Custom { .. } => write!(f, "Authorizer::Custom"),
        }
    }
}

/// Check authorization for an action during translation.
/// Returns Ok(AuthResult::Ok) or Ok(AuthResult::Ignore) on success.
/// Returns Err(LimboError::AuthorizationDenied) on DENY.
pub fn check_auth(
    connection: &Connection,
    action: AuthAction,
    arg3: Option<&str>,
    arg4: Option<&str>,
    db_name: Option<&str>,
) -> Result<AuthResult> {
    let result = connection.authorize(action, arg3, arg4, db_name, None);
    match result {
        AuthResult::Ok => Ok(AuthResult::Ok),
        AuthResult::Ignore => Ok(AuthResult::Ignore),
        AuthResult::Deny => Err(LimboError::AuthorizationDenied(
            "not authorized".to_string(),
        )),
    }
}
