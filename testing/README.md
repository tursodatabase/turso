# Turso Testing

## Testing Extensions
When adding tests for extensions, please follow these guidelines:
* Tests that verify the internal logic or behavior of a particular extension should go into `cli_tests/extensions.py`.
* Tests that verify how extensions interact with the database engine, such as virtual table handling, should be written 
in TCL (see `vtab.test` as an example).

To check which extensions are available in TCL, or to add a new one, refer to the `tester.tcl` file and look at the `extension_map`.
