use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Result, Write};
use std::path::Path;
use std::process::Command;

use cc::Build;

/// generates a trie-like function with nested match expressions for parsing SQL keywords
/// example: input: [["ABORT", "TK_ABORT"], ["ACTION", "TK_ACTION"], ["ADD", "TK_ADD"],]
/// A
/// ├─ B
/// │  ├─ O
/// │  │  ├─ R
/// │  │  │  ├─ T -> TK_ABORT
/// ├─ C
/// │  ├─ T
/// │  │  ├─ I
/// │  │  │  ├─ O
/// │  │  │  │  ├─ N -> TK_ACTION
/// ├─ D
/// │  ├─ D -> TK_ADD
fn build_keyword_map(
    writer: &mut impl Write,
    func_name: &str,
    keywords: &[[&'static str; 2]],
) -> Result<()> {
    assert!(!keywords.is_empty());
    let mut min_len = keywords[0][0].len();
    let mut max_len = keywords[0][0].len();

    struct PathEntry {
        result: Option<&'static str>,
        sub_entries: HashMap<u8, Box<PathEntry>>,
    }

    let mut paths = Box::new(PathEntry {
        result: None,
        sub_entries: HashMap::new(),
    });

    for keyword in keywords {
        let keyword_b = keyword[0].as_bytes();

        if keyword_b.len() < min_len {
            min_len = keyword_b.len();
        }

        if keyword_b.len() > max_len {
            max_len = keyword_b.len();
        }

        let mut current = &mut paths;

        for &b in keyword_b {
            let upper_b = b.to_ascii_uppercase();

            match current.sub_entries.get(&upper_b) {
                Some(_) => {
                    current = current.sub_entries.get_mut(&upper_b).unwrap();
                }
                None => {
                    let new_entry = Box::new(PathEntry {
                        result: None,
                        sub_entries: HashMap::new(),
                    });
                    current.sub_entries.insert(upper_b, new_entry);
                    current = current.sub_entries.get_mut(&upper_b).unwrap();
                }
            }
        }

        assert!(current.result.is_none());
        current.result = Some(keyword[1]);
    }

    fn write_entry(writer: &mut impl Write, entry: &PathEntry) -> Result<()> {
        if let Some(result) = entry.result {
            writeln!(writer, "if idx == buf.len() {{")?;
            writeln!(writer, "return Some(TokenType::{result});")?;
            writeln!(writer, "}}")?;
        }

        if entry.sub_entries.is_empty() {
            writeln!(writer, "None")?;
            return Ok(());
        }

        writeln!(writer, "if idx >= buf.len() {{")?;
        writeln!(writer, "return None;")?;
        writeln!(writer, "}}")?;

        writeln!(writer, "match buf[idx] {{")?;
        for (&b, sub_entry) in &entry.sub_entries {
            if b.is_ascii_alphabetic() {
                writeln!(writer, "{} | {} => {{", b, b.to_ascii_lowercase())?;
            } else {
                writeln!(writer, "{b} => {{")?;
            }
            writeln!(writer, "idx += 1;")?;
            write_entry(writer, sub_entry)?;
            writeln!(writer, "}}")?;
        }

        writeln!(writer, "_ => None")?;
        writeln!(writer, "}}")?;
        Ok(())
    }

    writeln!(
        writer,
        "pub(crate) const MAX_KEYWORD_LEN: usize = {max_len};"
    )?;
    writeln!(
        writer,
        "pub(crate) const MIN_KEYWORD_LEN: usize = {min_len};"
    )?;
    writeln!(writer, "/// Check if `word` is a keyword")?;
    writeln!(
        writer,
        "pub fn {func_name}(buf: &[u8]) -> Option<TokenType> {{"
    )?;
    writeln!(
        writer,
        "if buf.len() < MIN_KEYWORD_LEN || buf.len() > MAX_KEYWORD_LEN {{"
    )?;
    writeln!(writer, "return None;")?;
    writeln!(writer, "}}")?;
    writeln!(writer, "let mut idx = 0;")?;
    write_entry(writer, &paths)?;
    writeln!(writer, "}}")?;
    Ok(())
}

fn main() -> Result<()> {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir);
    let rlemon = out_path.join("rlemon");

    let lemon_src_dir = Path::new("third_party").join("lemon");
    let rlemon_src = lemon_src_dir.join("lemon.c");

    // compile rlemon:
    {
        assert!(Build::new()
            .target(&env::var("HOST").unwrap())
            .get_compiler()
            .to_command()
            .arg("-o")
            .arg(rlemon.clone())
            .arg(rlemon_src)
            .status()?
            .success());
    }

    let sql_parser = "src/parser/parse.y";
    // run rlemon / generate parser:
    {
        assert!(Command::new(rlemon)
            .arg("-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT")
            .arg("-Tthird_party/lemon/lempar.rs")
            .arg(format!("-d{out_dir}"))
            .arg(sql_parser)
            .status()?
            .success());
        // TODO ./rlemon -m -Tthird_party/lemon/lempar.rs examples/simple.y
    }

    let keywords = out_path.join("keywords.rs");
    let mut keywords = BufWriter::new(File::create(keywords)?);
    build_keyword_map(
        &mut keywords,
        "keyword_token",
        &[
            ["ABORT", "TK_ABORT"],
            ["ACTION", "TK_ACTION"],
            ["ADD", "TK_ADD"],
            ["AFTER", "TK_AFTER"],
            ["ALL", "TK_ALL"],
            ["ALTER", "TK_ALTER"],
            ["ALWAYS", "TK_ALWAYS"],
            ["ANALYZE", "TK_ANALYZE"],
            ["AND", "TK_AND"],
            ["AS", "TK_AS"],
            ["ASC", "TK_ASC"],
            ["ATTACH", "TK_ATTACH"],
            ["AUTOINCREMENT", "TK_AUTOINCR"],
            ["BEFORE", "TK_BEFORE"],
            ["BEGIN", "TK_BEGIN"],
            ["BETWEEN", "TK_BETWEEN"],
            ["BY", "TK_BY"],
            ["CASCADE", "TK_CASCADE"],
            ["CASE", "TK_CASE"],
            ["CAST", "TK_CAST"],
            ["CHECK", "TK_CHECK"],
            ["COLLATE", "TK_COLLATE"],
            ["COLUMN", "TK_COLUMNKW"],
            ["COMMIT", "TK_COMMIT"],
            ["CONFLICT", "TK_CONFLICT"],
            ["CONSTRAINT", "TK_CONSTRAINT"],
            ["CREATE", "TK_CREATE"],
            ["CROSS", "TK_JOIN_KW"],
            ["CURRENT", "TK_CURRENT"],
            ["CURRENT_DATE", "TK_CTIME_KW"],
            ["CURRENT_TIME", "TK_CTIME_KW"],
            ["CURRENT_TIMESTAMP", "TK_CTIME_KW"],
            ["DATABASE", "TK_DATABASE"],
            ["DEFAULT", "TK_DEFAULT"],
            ["DEFERRABLE", "TK_DEFERRABLE"],
            ["DEFERRED", "TK_DEFERRED"],
            ["DELETE", "TK_DELETE"],
            ["DESC", "TK_DESC"],
            ["DETACH", "TK_DETACH"],
            ["DISTINCT", "TK_DISTINCT"],
            ["DO", "TK_DO"],
            ["DROP", "TK_DROP"],
            ["EACH", "TK_EACH"],
            ["ELSE", "TK_ELSE"],
            ["END", "TK_END"],
            ["ESCAPE", "TK_ESCAPE"],
            ["EXCEPT", "TK_EXCEPT"],
            ["EXCLUDE", "TK_EXCLUDE"],
            ["EXCLUSIVE", "TK_EXCLUSIVE"],
            ["EXISTS", "TK_EXISTS"],
            ["EXPLAIN", "TK_EXPLAIN"],
            ["FAIL", "TK_FAIL"],
            ["FILTER", "TK_FILTER"],
            ["FIRST", "TK_FIRST"],
            ["FOLLOWING", "TK_FOLLOWING"],
            ["FOR", "TK_FOR"],
            ["FOREIGN", "TK_FOREIGN"],
            ["FROM", "TK_FROM"],
            ["FULL", "TK_JOIN_KW"],
            ["GENERATED", "TK_GENERATED"],
            ["GLOB", "TK_LIKE_KW"],
            ["GROUP", "TK_GROUP"],
            ["GROUPS", "TK_GROUPS"],
            ["HAVING", "TK_HAVING"],
            ["IF", "TK_IF"],
            ["IGNORE", "TK_IGNORE"],
            ["IMMEDIATE", "TK_IMMEDIATE"],
            ["IN", "TK_IN"],
            ["INDEX", "TK_INDEX"],
            ["INDEXED", "TK_INDEXED"],
            ["INITIALLY", "TK_INITIALLY"],
            ["INNER", "TK_JOIN_KW"],
            ["INSERT", "TK_INSERT"],
            ["INSTEAD", "TK_INSTEAD"],
            ["INTERSECT", "TK_INTERSECT"],
            ["INTO", "TK_INTO"],
            ["IS", "TK_IS"],
            ["ISNULL", "TK_ISNULL"],
            ["JOIN", "TK_JOIN"],
            ["KEY", "TK_KEY"],
            ["LAST", "TK_LAST"],
            ["LEFT", "TK_JOIN_KW"],
            ["LIKE", "TK_LIKE_KW"],
            ["LIMIT", "TK_LIMIT"],
            ["MATCH", "TK_MATCH"],
            ["MATERIALIZED", "TK_MATERIALIZED"],
            ["NATURAL", "TK_JOIN_KW"],
            ["NO", "TK_NO"],
            ["NOT", "TK_NOT"],
            ["NOTHING", "TK_NOTHING"],
            ["NOTNULL", "TK_NOTNULL"],
            ["NULL", "TK_NULL"],
            ["NULLS", "TK_NULLS"],
            ["OF", "TK_OF"],
            ["OFFSET", "TK_OFFSET"],
            ["ON", "TK_ON"],
            ["OR", "TK_OR"],
            ["ORDER", "TK_ORDER"],
            ["OTHERS", "TK_OTHERS"],
            ["OUTER", "TK_JOIN_KW"],
            ["OVER", "TK_OVER"],
            ["PARTITION", "TK_PARTITION"],
            ["PLAN", "TK_PLAN"],
            ["PRAGMA", "TK_PRAGMA"],
            ["PRECEDING", "TK_PRECEDING"],
            ["PRIMARY", "TK_PRIMARY"],
            ["QUERY", "TK_QUERY"],
            ["RAISE", "TK_RAISE"],
            ["RANGE", "TK_RANGE"],
            ["RECURSIVE", "TK_RECURSIVE"],
            ["REFERENCES", "TK_REFERENCES"],
            ["REGEXP", "TK_LIKE_KW"],
            ["REINDEX", "TK_REINDEX"],
            ["RELEASE", "TK_RELEASE"],
            ["RENAME", "TK_RENAME"],
            ["REPLACE", "TK_REPLACE"],
            ["RETURNING", "TK_RETURNING"],
            ["RESTRICT", "TK_RESTRICT"],
            ["RIGHT", "TK_JOIN_KW"],
            ["ROLLBACK", "TK_ROLLBACK"],
            ["ROW", "TK_ROW"],
            ["ROWS", "TK_ROWS"],
            ["SAVEPOINT", "TK_SAVEPOINT"],
            ["SELECT", "TK_SELECT"],
            ["SET", "TK_SET"],
            ["TABLE", "TK_TABLE"],
            ["TEMP", "TK_TEMP"],
            ["TEMPORARY", "TK_TEMP"],
            ["THEN", "TK_THEN"],
            ["TIES", "TK_TIES"],
            ["TO", "TK_TO"],
            ["TRANSACTION", "TK_TRANSACTION"],
            ["TRIGGER", "TK_TRIGGER"],
            ["UNBOUNDED", "TK_UNBOUNDED"],
            ["UNION", "TK_UNION"],
            ["UNIQUE", "TK_UNIQUE"],
            ["UPDATE", "TK_UPDATE"],
            ["USING", "TK_USING"],
            ["VACUUM", "TK_VACUUM"],
            ["VALUES", "TK_VALUES"],
            ["VIEW", "TK_VIEW"],
            ["VIRTUAL", "TK_VIRTUAL"],
            ["WHEN", "TK_WHEN"],
            ["WHERE", "TK_WHERE"],
            ["WINDOW", "TK_WINDOW"],
            ["WITH", "TK_WITH"],
            ["WITHOUT", "TK_WITHOUT"],
        ],
    )?;

    println!("cargo:rerun-if-changed=third_party/lemon/lemon.c");
    println!("cargo:rerun-if-changed=third_party/lemon/lempar.rs");
    println!("cargo:rerun-if-changed=src/parser/parse.y");
    // TODO examples/simple.y if test
    Ok(())
}
