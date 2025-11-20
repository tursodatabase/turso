//! Build.rs script to generate a binary syntax set for syntect
//! based on the SQL.sublime-syntax file.

use std::env;
use std::path::Path;

use syntect::dumps::dump_to_uncompressed_file;
use syntect::parsing::SyntaxDefinition;
use syntect::parsing::SyntaxSet;

fn main() {
    println!("cargo::rerun-if-changed=SQL.sublime-syntax");
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=manuals");

    let out_dir = env::var_os("OUT_DIR").expect("OUT_DIR not set by cargo");
    let syntax = SyntaxDefinition::load_from_str(include_str!("./SQL.sublime-syntax"), false, None)
        .expect("failed to load SQL syntax definition");
    let mut ps = SyntaxSet::new().into_builder();
    ps.add(syntax);
    let ps = ps.build();
    dump_to_uncompressed_file(
        &ps,
        Path::new(&out_dir).join("SQL_syntax_set_dump.packdump"),
    )
    .expect("failed to dump syntax set");
}
