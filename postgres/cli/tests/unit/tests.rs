use super::*;

#[test]
fn test_successful_exit_commands() {
    for command in [
        "exit", "quit", "EXIT", "QuIt", "exit;", "quit;", "EXIT;", "exit ;  ", "quit ;  ",
    ] {
        assert!(
            is_exit_command(command),
            "expected {command:?} to be an exit command"
        );
    }
}

#[test]
fn test_unsuccessful_exit_commands() {
    for command in [
        "",
        " quit ",
        "exit;;",
        "exit now",
        "quitter",
        "some_exit",
        "SELECT 'quit';",
        "quit;;",
    ] {
        assert!(
            !is_exit_command(command),
            "expected {command:?} not to be an exit command"
        );
    }
}
