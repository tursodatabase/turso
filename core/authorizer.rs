//! SQLite authorizer support.
//!
//! The authorizer callback is invoked during SQL statement compilation (in `translate()`)
//! to check whether specific actions are allowed. This enables isolation between
//! connections by restricting what operations each connection can perform.
//!
//! See: <https://sqlite.org/c3ref/set_authorizer.html>

/// Authorizer action codes matching SQLite's constants.
/// The 5th argument to the authorizer callback is the database name ("main", "temp", etc.)
/// and the 6th argument is the name of the innermost trigger or view, or NULL.
pub mod action {
    pub const SQLITE_CREATE_INDEX: i32 = 1;
    pub const SQLITE_CREATE_TABLE: i32 = 2;
    pub const SQLITE_CREATE_TEMP_INDEX: i32 = 3;
    pub const SQLITE_CREATE_TEMP_TABLE: i32 = 4;
    pub const SQLITE_CREATE_TEMP_TRIGGER: i32 = 5;
    pub const SQLITE_CREATE_TEMP_VIEW: i32 = 6;
    pub const SQLITE_CREATE_TRIGGER: i32 = 7;
    pub const SQLITE_CREATE_VIEW: i32 = 8;
    pub const SQLITE_DELETE: i32 = 9;
    pub const SQLITE_DROP_INDEX: i32 = 10;
    pub const SQLITE_DROP_TABLE: i32 = 11;
    pub const SQLITE_DROP_TEMP_INDEX: i32 = 12;
    pub const SQLITE_DROP_TEMP_TABLE: i32 = 13;
    pub const SQLITE_DROP_TEMP_TRIGGER: i32 = 14;
    pub const SQLITE_DROP_TEMP_VIEW: i32 = 15;
    pub const SQLITE_DROP_TRIGGER: i32 = 16;
    pub const SQLITE_DROP_VIEW: i32 = 17;
    pub const SQLITE_INSERT: i32 = 18;
    pub const SQLITE_PRAGMA: i32 = 19;
    pub const SQLITE_READ: i32 = 20;
    pub const SQLITE_SELECT: i32 = 21;
    pub const SQLITE_TRANSACTION: i32 = 22;
    pub const SQLITE_UPDATE: i32 = 23;
    pub const SQLITE_ATTACH: i32 = 24;
    pub const SQLITE_DETACH: i32 = 25;
    pub const SQLITE_ALTER_TABLE: i32 = 26;
    pub const SQLITE_REINDEX: i32 = 27;
    pub const SQLITE_ANALYZE: i32 = 28;
    pub const SQLITE_CREATE_VTABLE: i32 = 29;
    pub const SQLITE_DROP_VTABLE: i32 = 30;
    pub const SQLITE_FUNCTION: i32 = 31;
    pub const SQLITE_SAVEPOINT: i32 = 32;
    pub const SQLITE_RECURSIVE: i32 = 33;
}

/// Authorizer return codes.
pub const SQLITE_OK: i32 = 0;
pub const SQLITE_DENY: i32 = 1;
pub const SQLITE_IGNORE: i32 = 2;

/// Callback type for the authorizer.
///
/// Parameters: (action_code, arg3, arg4, db_name, trigger_name)
/// - action_code: One of the SQLITE_* action constants
/// - arg3: Action-specific argument (e.g., table name)
/// - arg4: Action-specific argument (e.g., column name)
/// - db_name: Database name ("main", "temp", etc.) or None
/// - trigger_name: Innermost trigger/view name, or None for top-level SQL
///
/// Returns: SQLITE_OK (0) to allow, SQLITE_DENY (1) to reject with error,
/// SQLITE_IGNORE (2) to silently disallow (e.g., replace column with NULL for READ).
pub type AuthorizerCallback =
    Box<dyn Fn(i32, Option<&str>, Option<&str>, Option<&str>, Option<&str>) -> i32 + Send + Sync>;
