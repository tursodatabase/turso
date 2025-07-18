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
         END"
    );

    to_sql_string_test!(
        test_log_salary_update,
        "CREATE TRIGGER log_salary_update
        AFTER UPDATE OF salary ON employees
        FOR EACH ROW
        BEGIN
            INSERT INTO employee_log (action, employee_id, old_value, new_value, timestamp)
            VALUES ('UPDATE', OLD.id, OLD.salary, NEW.salary, CURRENT_TIMESTAMP);
        END"
    );

    to_sql_string_test!(
        test_log_employee_delete,
        "CREATE TRIGGER log_employee_delete
         AFTER DELETE ON employees
         FOR EACH ROW
         BEGIN
             INSERT INTO employee_log (action, employee_id, timestamp)
             VALUES ('DELETE', OLD.id, CURRENT_TIMESTAMP);
         END"
    );

    to_sql_string_test!(
        test_check_salary_insert,
        "CREATE TRIGGER check_salary_insert
         BEFORE INSERT ON employees
         FOR EACH ROW
         WHEN NEW.salary < 0
         BEGIN
             SELECT RAISE (FAIL, 'Salary cannot be negative');
         END"
    );

    to_sql_string_test!(
        test_insert_employee_dept,
        "CREATE TRIGGER insert_employee_dept
         INSTEAD OF INSERT ON employee_dept
         FOR EACH ROW
         BEGIN
             INSERT INTO departments (name) SELECT NEW.department WHERE NOT EXISTS (SELECT 1 FROM departments WHERE name = NEW.department);
             INSERT INTO employees (name, department_id) VALUES (NEW.name, (SELECT id FROM departments WHERE name = NEW.department));
         END"
    );
}
