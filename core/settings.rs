use rustc_hash::FxHashMap as HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettingScope {
    Session,
    Transaction,
}

#[derive(Default)]
pub(crate) struct ConnectionSettings {
    values: SettingValues,
    values_before_transaction: Option<SettingValues>,
    values_before_statement: Option<SettingValues>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SettingValues {
    search_path: Setting<Option<Vec<String>>>,
    named: HashMap<String, Setting<Option<String>>>,
    has_transaction_values: bool,
}

#[derive(Clone, Debug, Default)]
struct Setting<T> {
    session_value: T,
    transaction_value: Option<T>,
}

impl ConnectionSettings {
    pub(crate) fn search_path(&self) -> Option<&[String]> {
        self.values.search_path.current().as_deref()
    }

    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.values.named.get(name)?.current().as_deref()
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.values.named.contains_key(name)
    }

    pub(crate) fn values(&self) -> &SettingValues {
        &self.values
    }

    pub(crate) fn has_changes_to_end(&self) -> bool {
        self.values_before_statement.is_some()
            || self.values_before_transaction.is_some()
            || self.values.has_transaction_values
    }

    pub(crate) fn save_values_before_change(
        &mut self,
        statement_running: bool,
        in_transaction: bool,
    ) {
        if statement_running && self.values_before_statement.is_none() {
            self.values_before_statement = Some(self.values.clone());
        }
        if (statement_running || in_transaction) && self.values_before_transaction.is_none() {
            self.values_before_transaction = Some(self.values.clone());
        }
    }

    pub(crate) fn set_search_path(
        &mut self,
        search_path: Option<Vec<String>>,
        scope: SettingScope,
    ) {
        self.values.search_path.set(search_path, scope);
        self.values.has_transaction_values |= scope == SettingScope::Transaction;
    }

    pub(crate) fn set(&mut self, name: &str, value: Option<String>, scope: SettingScope) {
        self.values
            .named
            .entry(name.to_owned())
            .or_default()
            .set(value, scope);
        self.values.has_transaction_values |= scope == SettingScope::Transaction;
    }

    pub(crate) fn end_statement(
        &mut self,
        statement_failed: bool,
        transaction_ended: bool,
    ) -> bool {
        let values_before_statement = self.values_before_statement.take();
        let values_to_restore = if transaction_ended {
            self.values_before_transaction
                .take()
                .or(values_before_statement)
        } else {
            values_before_statement
        };
        self.change_values(|values| {
            if let Some(saved) = values_to_restore.filter(|_| statement_failed) {
                values.restore(saved);
            }
            if transaction_ended {
                values.end_transaction();
            }
        })
    }

    pub(crate) fn rollback_transaction(&mut self) -> bool {
        self.values_before_statement = None;
        let values_before_transaction = self.values_before_transaction.take();
        self.change_values(|values| {
            if let Some(saved) = values_before_transaction {
                values.restore(saved);
            }
            values.end_transaction();
        })
    }

    pub(crate) fn rollback_to_savepoint(&mut self, saved: SettingValues) -> bool {
        self.change_values(|values| values.restore(saved))
    }

    fn change_values(&mut self, change: impl FnOnce(&mut SettingValues)) -> bool {
        let search_path = self.values.search_path.current().clone();
        change(&mut self.values);
        search_path != *self.values.search_path.current()
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
