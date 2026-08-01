//! Build.rs script to generate a binary syntax set for syntect
//! based on the SQL.sublime-syntax file shared with the CLI.

use std::env;
use std::path::Path;

use syntect::dumps::dump_to_uncompressed_file;
use syntect::parsing::SyntaxDefinition;
use syntect::parsing::SyntaxSet;

fn main() {
    println!("cargo::rerun-if-changed=../../cli/SQL.sublime-syntax");
    println!("cargo::rerun-if-changed=build.rs");

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let syntax =
        SyntaxDefinition::load_from_str(include_str!("../../cli/SQL.sublime-syntax"), false, None)
            .unwrap();
    let mut ps = SyntaxSet::new().into_builder();
    ps.add(syntax);
    let ps = ps.build();
    dump_to_uncompressed_file(
        &ps,
        Path::new(&out_dir).join("SQL_syntax_set_dump.packdump"),
    )
    .unwrap();
}
