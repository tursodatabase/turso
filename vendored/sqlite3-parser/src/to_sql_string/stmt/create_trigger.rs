use std::fmt::Display;

use crate::{
    ast::{self, fmt::ToTokens},
    to_sql_string::ToSqlString,
};

impl ToSqlString for ast::CreateTrigger {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "CREATE{} TRIGGER {}{}{} {} ON {}{}{} BEGIN {} END;",
            if self.temporary { " TEMP" } else { "" },
            if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.trigger_name.to_sql_string(context),
            self.time.map_or("".to_string(), |time| format!(" {time}")),
            self.event,
            self.tbl_name.to_sql_string(context),
            if self.for_each_row {
                " FOR EACH ROW"
            } else {
                ""
            },
            self.when_clause
                .as_ref()
                .map_or("".to_string(), |expr| format!(
                    " WHEN {}",
                    expr.to_sql_string(context)
                )),
            self.commands
                .iter()
                .map(|command| format!("{};", command.to_sql_string(context)))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl Display for ast::TriggerTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_fmt(f)
    }
}

impl Display for ast::TriggerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Delete => "DELETE".to_string(),
                Self::Insert => "INSERT".to_string(),
                Self::Update => "UPDATE".to_string(),
                Self::UpdateOf(col_names) => format!(
                    "UPDATE OF {}",
                    col_names
                        .iter()
                        .map(|name| name.0.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        )
    }
}

impl ToSqlString for ast::TriggerCmd {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::Delete(delete) => delete.to_sql_string(context),
            Self::Insert(insert) => insert.to_sql_string(context),
            Self::Select(select) => select.to_sql_string(context),
            Self::Update(update) => update.to_sql_string(context),
        }
    }
}

impl ToSqlString for ast::TriggerCmdDelete {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        // https://sqlite.org/lang_createtrigger.html
        // TODO: no CTEs and returning clause present in ast for delete
        // Also for tbl_name it should be a qualified table name with indexed by clauses
        format!(
            "DELETE FROM {}{}",
            self.tbl_name.0,
            self.where_clause
                .as_ref()
                .map_or("".to_string(), |expr| format!(
                    " WHERE {}",
                    expr.to_sql_string(context)
                ))
        )
    }
}

impl ToSqlString for ast::TriggerCmdInsert {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        // https://sqlite.org/lang_createtrigger.html
        // FOR TRIGGER SHOULD JUST USE REGULAR INSERT AST
        // TODO: no ALIAS after table name
        // TODO: no DEFAULT VALUES
        format!(
            "INSERT {}INTO {} {}{}{}{}",
            self.or_conflict
                .map_or("".to_string(), |conflict| format!("OR {conflict} ")),
            self.tbl_name.0,
            self.col_names
                .as_ref()
                .map_or("".to_string(), |col_names| format!(
                    "({}) ",
                    col_names
                        .iter()
                        .map(|name| name.0.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            self.select.to_sql_string(context),
            self.upsert
                .as_ref()
                .map_or("".to_string(), |upsert| format!(
                    " {}",
                    upsert.to_sql_string(context)
                )),
            self.returning
                .as_ref()
                .map_or("".to_string(), |returning| format!(
                    " RETURNING {}",
                    returning
                        .iter()
                        .map(|col| col.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
        )
    }
}

impl ToSqlString for ast::Upsert {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "ON CONFLICT{}{}{}",
            self.index.as_ref().map_or("".to_string(), |index| format!(
                "{} ",
                index.to_sql_string(context)
            )),
            self.do_clause.to_sql_string(context),
            self.next.as_ref().map_or("".to_string(), |next| format!(
                " {}",
                next.to_sql_string(context)
            ))
        )
    }
}

impl ToSqlString for ast::UpsertIndex {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "({}){}",
            self.targets
                .iter()
                .map(|target| target.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(", "),
            self.where_clause
                .as_ref()
                .map_or("".to_string(), |expr| format!(
                    " WHERE {}",
                    expr.to_sql_string(context)
                ))
        )
    }
}

impl ToSqlString for ast::UpsertDo {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::Nothing => "DO NOTHING".to_string(),
            Self::Set { sets, where_clause } => {
                format!(
                    "DO UPDATE SET {}{}",
                    sets.iter()
                        .map(|set| set.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", "),
                    where_clause.as_ref().map_or("".to_string(), |expr| format!(
                        " WHERE {}",
                        expr.to_sql_string(context)
                    ))
                )
            }
        }
    }
}

impl ToSqlString for ast::Set {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        if self.col_names.len() == 1 {
            format!(
                "{} = {}",
                &self.col_names[0],
                self.expr.to_sql_string(context)
            )
        } else {
            format!(
                "({}) = {}",
                self.col_names
                    .iter()
                    .map(|name| name.0.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
                self.expr.to_sql_string(context)
            )
        }
    }
}

impl ToSqlString for ast::TriggerCmdUpdate {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "UPDATE {}{} SET {}{}{}",
            self.or_conflict
                .map_or("".to_string(), |conflict| format!("OR {conflict}")),
            self.tbl_name.0, // TODO: should be a qualified table name,
            self.sets
                .iter()
                .map(|set| set.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(", "),
            self.from.as_ref().map_or("".to_string(), |from| format!(
                " {}",
                from.to_sql_string(context)
            )),
            self.where_clause
                .as_ref()
                .map_or("".to_string(), |expr| format!(
                    " WHERE {}",
                    expr.to_sql_string(context)
                ))
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(
        test_log_employee_insert,
        "CREATE TRIGGER log_employee_insert
         AFTER INSERT ON employees
         FOR EACH ROW
         BEGIN
             INSERT INTO employee_log (action, employee_id, timestamp)
             VALUES ('INSERT', NEW.id, CURRENT_TIMESTAMP);
         END;"
    );

    to_sql_string_test!(
        test_log_salary_update,
        "CREATE TRIGGER log_salary_update
        AFTER UPDATE OF salary ON employees
        FOR EACH ROW
        BEGIN
            INSERT INTO employee_log (action, employee_id, old_value, new_value, timestamp)
            VALUES ('UPDATE', OLD.id, OLD.salary, NEW.salary, CURRENT_TIMESTAMP);
        END;"
    );

    to_sql_string_test!(
        test_log_employee_delete,
        "CREATE TRIGGER log_employee_delete
         AFTER DELETE ON employees
         FOR EACH ROW
         BEGIN
             INSERT INTO employee_log (action, employee_id, timestamp)
             VALUES ('DELETE', OLD.id, CURRENT_TIMESTAMP);
         END;"
    );

    to_sql_string_test!(
        test_check_salary_insert,
        "CREATE TRIGGER check_salary_insert
         BEFORE INSERT ON employees
         FOR EACH ROW
         WHEN NEW.salary < 0
         BEGIN
             SELECT RAISE(FAIL, 'Salary cannot be negative');
         END;"
    );

    to_sql_string_test!(
        test_insert_employee_dept,
        "CREATE TRIGGER insert_employee_dept
         INSTEAD OF INSERT ON employee_dept
         FOR EACH ROW
         BEGIN
             INSERT INTO departments (name) SELECT NEW.department WHERE NOT EXISTS (SELECT 1 FROM departments WHERE name = NEW.department);
             INSERT INTO employees (name, department_id) VALUES (NEW.name, (SELECT id FROM departments WHERE name = NEW.department));
         END;"
    );
}
