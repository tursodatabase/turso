use super::CommandParser;

#[test]
fn cli_assert() {
    use clap::CommandFactory;
    CommandParser::command().debug_assert();
}
