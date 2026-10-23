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
        self.current
            .named
            .entry(name.to_owned())
            .or_default()
            .set(value, scope);
        self.current.has_transaction_values |= scope == SettingScope::Transaction;
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
