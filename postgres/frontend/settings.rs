use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use turso_core::{Connection, ConnectionHooks, SavepointMemo};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SettingScope {
    Session,
    Transaction,
}

#[derive(Default)]
pub(crate) struct PgSessionState {
    settings: Mutex<ConnectionSettings>,
}

impl PgSessionState {
    pub(crate) fn mutate(&self, conn: &Connection, mutate: impl FnOnce(&mut ConnectionSettings)) {
        let mut settings = self.settings.lock().unwrap();
        settings.save_memo_before_change(conn.has_active_root_statement(), !conn.get_auto_commit());
        mutate(&mut settings);
    }

    pub(crate) fn setting(&self, name: &str) -> Option<String> {
        self.settings.lock().unwrap().get(name).map(str::to_owned)
    }

    pub(crate) fn has_setting(&self, name: &str) -> bool {
        self.settings.lock().unwrap().contains(name)
    }

    pub(crate) fn name_as_defined(&self, name: &str) -> Option<String> {
        self.settings
            .lock()
            .unwrap()
            .names_as_defined
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    fn update_settings(
        &self,
        conn: &Connection,
        update: impl FnOnce(&mut ConnectionSettings) -> bool,
    ) {
        let changed_search_path = {
            let mut settings = self.settings.lock().unwrap();
            update(&mut settings).then(|| settings.search_path().map(<[String]>::to_vec))
        };
        if let Some(search_path) = changed_search_path {
            conn.set_search_path(search_path);
        }
    }

    fn has_changes_to_end(&self) -> bool {
        self.settings.lock().unwrap().has_changes_to_end()
    }
}

impl ConnectionHooks for PgSessionState {
    fn root_statement_ended(&self, conn: &Connection, failed: bool) {
        if !self.has_changes_to_end() {
            return;
        }
        let transaction_ended = conn.get_auto_commit();
        self.update_settings(conn, |settings| {
            settings.end_statement(failed, transaction_ended)
        });
    }

    fn transaction_rolled_back(&self, conn: &Connection) {
        if !self.has_changes_to_end() {
            return;
        }
        self.update_settings(conn, ConnectionSettings::rollback_transaction);
    }

    fn savepoint_created(&self, _conn: &Connection) -> SavepointMemo {
        Arc::new(self.settings.lock().unwrap().current_values().clone())
    }

    fn rolled_back_to_savepoint(&self, conn: &Connection, memo: &SavepointMemo) {
        let saved = memo
            .downcast_ref::<SettingValues>()
            .expect("savepoint_created stores SettingValues")
            .clone();
        self.update_settings(conn, |settings| settings.rollback_to_savepoint(saved));
    }
}

#[derive(Default)]
pub(crate) struct ConnectionSettings {
    current: SettingValues,
    memo_before_transaction: Option<SettingValues>,
    memo_before_statement: Option<SettingValues>,
    /// Each parameter's name as written when it was first set, by
    /// ascii-lowercase name. SHOW names its column this way. A rollback
    /// keeps the parameter defined, so it keeps this name too.
    names_as_defined: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SettingValues {
    search_path: Setting<Option<Vec<String>>>,
    /// Custom parameters, by ascii-lowercase name. Also contains the `search_path` as the user entered it.
    named: HashMap<String, Setting<Option<String>>>,
    /// whether this struct contains any LOCAL values
    has_transaction_values: bool,
}

#[derive(Clone, Debug, Default)]
struct Setting<T> {
    session_value: T,
    transaction_value: Option<T>,
}

impl ConnectionSettings {
    pub(crate) fn search_path(&self) -> Option<&[String]> {
        self.current.search_path.current().as_deref()
    }

    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.current.named.get(name)?.current().as_deref()
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.current.named.contains_key(name)
    }

    pub(crate) fn current_values(&self) -> &SettingValues {
        &self.current
    }

    pub(crate) fn has_changes_to_end(&self) -> bool {
        self.memo_before_statement.is_some()
            || self.memo_before_transaction.is_some()
            || self.current.has_transaction_values
    }

    pub(crate) fn save_memo_before_change(
        &mut self,
        statement_running: bool,
        in_transaction: bool,
    ) {
        if statement_running && self.memo_before_statement.is_none() {
            self.memo_before_statement = Some(self.current.clone());
        }
        if (statement_running || in_transaction) && self.memo_before_transaction.is_none() {
            self.memo_before_transaction = Some(self.current.clone());
        }
    }

    pub(crate) fn set_search_path(
        &mut self,
        search_path: Option<Vec<String>>,
        scope: SettingScope,
    ) {
        self.current.search_path.set(search_path, scope);
        self.current.has_transaction_values |= scope == SettingScope::Transaction;
    }

    pub(crate) fn set(&mut self, name: &str, value: Option<String>, scope: SettingScope) {
        let key = name.to_ascii_lowercase();
        self.names_as_defined
            .entry(key.clone())
            .or_insert_with(|| name.to_owned());
        self.current.named.entry(key).or_default().set(value, scope);
        self.current.has_transaction_values |= scope == SettingScope::Transaction;
    }

    /// Sets every named parameter to its default value for the session.
    pub(crate) fn reset_all(&mut self) {
        for setting in self.current.named.values_mut() {
            setting.set(None, SettingScope::Session);
        }
    }

    pub(crate) fn end_statement(
        &mut self,
        statement_failed: bool,
        transaction_ended: bool,
    ) -> bool {
        let memo_before_statement = self.memo_before_statement.take();
        let memo_to_restore = if transaction_ended {
            self.memo_before_transaction
                .take()
                .or(memo_before_statement)
        } else {
            memo_before_statement
        };
        self.change_current_values(|current| {
            if let Some(saved) = memo_to_restore.filter(|_| statement_failed) {
                current.restore(saved);
            }
            if transaction_ended {
                current.end_transaction();
            }
        })
    }

    pub(crate) fn rollback_transaction(&mut self) -> bool {
        self.memo_before_statement = None;
        let memo_before_transaction = self.memo_before_transaction.take();
        self.change_current_values(|current| {
            if let Some(saved) = memo_before_transaction {
                current.restore(saved);
            }
            current.end_transaction();
        })
    }

    pub(crate) fn rollback_to_savepoint(&mut self, saved: SettingValues) -> bool {
        self.change_current_values(|current| current.restore(saved))
    }

    fn change_current_values(&mut self, change: impl FnOnce(&mut SettingValues)) -> bool {
        let search_path = self.current.search_path.current().clone();
        change(&mut self.current);
        search_path != *self.current.search_path.current()
    }
}

impl SettingValues {
    fn restore(&mut self, mut saved: SettingValues) {
        self.search_path = saved.search_path;
        self.has_transaction_values = saved.has_transaction_values;
        for (name, setting) in &mut self.named {
            *setting = saved.named.remove(name).unwrap_or_default();
        }
    }

    fn end_transaction(&mut self) {
        self.search_path.end_transaction();
        for setting in self.named.values_mut() {
            setting.end_transaction();
        }
        self.has_transaction_values = false;
    }
}

impl<T> Setting<T> {
    fn set(&mut self, value: T, scope: SettingScope) {
        match scope {
            SettingScope::Session => {
                self.session_value = value;
                self.transaction_value = None;
            }
            SettingScope::Transaction => self.transaction_value = Some(value),
        }
    }

    fn current(&self) -> &T {
        self.transaction_value
            .as_ref()
            .unwrap_or(&self.session_value)
    }

    fn end_transaction(&mut self) {
        self.transaction_value = None;
    }
}

pub(crate) enum ConfigParameter {
    SearchPath,
    ApplicationName,
    Fixed(&'static FixedParameter),
}

pub(crate) enum Parameter<'a> {
    BuiltIn(ConfigParameter),
    Custom(&'a str),
}

pub(crate) fn find_parameter(name: &str) -> Option<Parameter<'_>> {
    let parameter = if name.eq_ignore_ascii_case("search_path") {
        ConfigParameter::SearchPath
    } else if name.eq_ignore_ascii_case("application_name") {
        ConfigParameter::ApplicationName
    } else if let Some(parameter) = FIXED_PARAMETERS
        .iter()
        .find(|parameter| parameter.name.eq_ignore_ascii_case(name))
    {
        ConfigParameter::Fixed(parameter)
    } else {
        return name.contains('.').then_some(Parameter::Custom(name));
    };
    Some(Parameter::BuiltIn(parameter))
}

impl ConfigParameter {
    pub(crate) fn set(
        &self,
        conn: &Connection,
        value: Option<&str>,
        scope: SettingScope,
    ) -> turso_core::Result<String> {
        match self {
            Self::SearchPath => crate::session::set_search_path(conn, value, scope),
            Self::ApplicationName => {
                crate::session::session_state(conn)?.mutate(conn, |settings| {
                    settings.set(self.name(), value.map(str::to_owned), scope);
                });
                Ok(value.unwrap_or_default().to_owned())
            }
            Self::Fixed(parameter) => {
                if let Some(value) = value {
                    parameter.validate(value)?;
                }
                Ok(parameter.value.to_owned())
            }
        }
    }

    pub(crate) fn get(&self, conn: &Connection) -> String {
        match self {
            Self::SearchPath => crate::session::search_path_setting(conn)
                .unwrap_or_else(|| crate::session::DEFAULT_SEARCH_PATH.to_owned()),
            Self::ApplicationName => conn
                .context::<PgSessionState>()
                .and_then(|state| state.setting(self.name()))
                .unwrap_or_default(),
            Self::Fixed(parameter) => parameter.value.to_owned(),
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::SearchPath => "search_path",
            Self::ApplicationName => "application_name",
            Self::Fixed(parameter) => parameter.name,
        }
    }

    pub(crate) fn accepts_list(&self) -> bool {
        matches!(
            self,
            Self::Fixed(FixedParameter {
                validation: FixedValidation::DateStyle,
                ..
            })
        )
    }
}

pub(crate) struct FixedParameter {
    name: &'static str,
    value: &'static str,
    validation: FixedValidation,
}

enum FixedValidation {
    Exact,
    Identifier,
    Bool(bool),
    DateStyle,
    Encoding,
    FloatDigits,
}

impl FixedParameter {
    fn validate(&self, value: &str) -> turso_core::Result<()> {
        let value_trimmed = value.trim();
        let valid = match self.validation {
            FixedValidation::Exact => value_trimmed.eq_ignore_ascii_case(self.value),
            FixedValidation::Identifier => value == self.value,
            FixedValidation::Bool(expected) => {
                crate::functions::parse_pg_bool(value_trimmed) == Some(expected)
            }
            FixedValidation::DateStyle => {
                let parts: Vec<_> = value_trimmed.split(',').map(str::trim).collect();
                matches!(parts.as_slice(), [style] if style.eq_ignore_ascii_case("iso") || style.eq_ignore_ascii_case("mdy"))
                    || matches!(parts.as_slice(), [style, order] if
                        (style.eq_ignore_ascii_case("iso") && order.eq_ignore_ascii_case("mdy")) ||
                        (style.eq_ignore_ascii_case("mdy") && order.eq_ignore_ascii_case("iso")))
            }
            FixedValidation::Encoding => ["UTF8", "UTF-8", "UNICODE"]
                .iter()
                .any(|encoding| value_trimmed.eq_ignore_ascii_case(encoding)),
            FixedValidation::FloatDigits => matches!(value_trimmed.parse::<i32>(), Ok(1..=3)),
        };
        if valid {
            Ok(())
        } else {
            Err(crate::error::sqlstate_error(
                turso_pg_parser::sqlstate::SQLSTATE_INVALID_PARAMETER_VALUE,
                format!(
                    "invalid value for parameter \"{}\": \"{value}\"; supported value is \"{}\"",
                    self.name, self.value
                ),
            ))
        }
    }
}

// These fixed settings are mainly compatibility stubs for pg_dump, including
// gateways that validate its SET commands through set_config. They report modes
// already in effect; they do not implement configurable PostgreSQL behavior such
// as timeouts, RLS, or alternate date formats. Unsupported values must error.
const FIXED_PARAMETERS: &[FixedParameter] = &[
    FixedParameter {
        name: "DateStyle",
        value: "ISO, MDY",
        validation: FixedValidation::DateStyle,
    },
    FixedParameter {
        name: "IntervalStyle",
        value: "postgres",
        validation: FixedValidation::Exact,
    },
    FixedParameter {
        name: "client_encoding",
        value: "UTF8",
        validation: FixedValidation::Encoding,
    },
    FixedParameter {
        name: "standard_conforming_strings",
        value: "on",
        validation: FixedValidation::Bool(true),
    },
    FixedParameter {
        name: "extra_float_digits",
        value: "1",
        validation: FixedValidation::FloatDigits,
    },
    FixedParameter {
        name: "statement_timeout",
        value: "0",
        validation: FixedValidation::Exact,
    },
    FixedParameter {
        name: "lock_timeout",
        value: "0",
        validation: FixedValidation::Exact,
    },
    FixedParameter {
        name: "idle_in_transaction_session_timeout",
        value: "0",
        validation: FixedValidation::Exact,
    },
    FixedParameter {
        name: "transaction_timeout",
        value: "0",
        validation: FixedValidation::Exact,
    },
    FixedParameter {
        name: "row_security",
        value: "off",
        validation: FixedValidation::Bool(false),
    },
    FixedParameter {
        name: "quote_all_identifiers",
        value: "off",
        validation: FixedValidation::Bool(false),
    },
    FixedParameter {
        name: "check_function_bodies",
        value: "off",
        validation: FixedValidation::Bool(false),
    },
    FixedParameter {
        name: "synchronize_seqscans",
        value: "off",
        validation: FixedValidation::Bool(false),
    },
    FixedParameter {
        name: "default_tablespace",
        value: "",
        validation: FixedValidation::Identifier,
    },
    FixedParameter {
        name: "default_table_access_method",
        value: "heap",
        validation: FixedValidation::Identifier,
    },
    FixedParameter {
        name: "xmloption",
        value: "content",
        validation: FixedValidation::Exact,
    },
    FixedParameter {
        name: "client_min_messages",
        value: "warning",
        validation: FixedValidation::Exact,
    },
];

#[cfg(test)]
mod tests {
    use super::{find_parameter, ConfigParameter, Parameter, FIXED_PARAMETERS};

    #[test]
    fn config_parameter_parses_sql_names() {
        for name in ["search_path", "SEARCH_PATH", "SeArCh_PaTh"] {
            assert!(
                matches!(
                    find_parameter(name),
                    Some(Parameter::BuiltIn(ConfigParameter::SearchPath))
                ),
                "{name}"
            );
        }
        for parameter in FIXED_PARAMETERS {
            for name in [
                parameter.name.to_ascii_lowercase(),
                parameter.name.to_ascii_uppercase(),
            ] {
                assert!(
                    matches!(
                        find_parameter(&name),
                        Some(Parameter::BuiltIn(ConfigParameter::Fixed(_)))
                    ),
                    "{name}"
                );
            }
        }
        for name in ["", "SearchPath", "search_path "] {
            assert!(find_parameter(name).is_none(), "{name}");
        }
    }
}
