use rusqlite::Connection;
use sqlsmith_rs_common::rand_by_seed::LcgRng;

enum PragmaKind {
    NoArg(&'static str),
    BoolArg(&'static str),
    IntArg(&'static str, i64, i64),
    StringArg(&'static str),
}

pub fn get_pragma_stmt_by_seed(rng: &mut LcgRng) -> Option<String> {
    use PragmaKind::*;
    const PRAGMAS: &[PragmaKind] = &[
        // No-argument pragmas
        NoArg("integrity_check"),
        NoArg("quick_check"),
        NoArg("foreign_key_check"),
        NoArg("database_list"),
        NoArg("collation_list"),
        NoArg("table_info"),
        NoArg("index_list"),
        NoArg("index_info"),
        NoArg("stats"),
        NoArg("page_count"),
        NoArg("schema_version"),
        NoArg("user_version"),
        NoArg("encoding"),
        NoArg("application_id"),
        NoArg("auto_vacuum"),
        NoArg("cache_size"),
        NoArg("page_size"),
        NoArg("wal_checkpoint"),
        NoArg("journal_mode"),
        NoArg("locking_mode"),
        NoArg("synchronous"),
        NoArg("temp_store"),
        NoArg("secure_delete"),
        NoArg("data_version"),
        NoArg("freelist_count"),
        NoArg("max_page_count"),
        NoArg("read_uncommitted"),
        NoArg("recursive_triggers"),
        NoArg("reverse_unordered_selects"),
        NoArg("shrink_memory"),
        NoArg("soft_heap_limit"),
        NoArg("threads"),
        NoArg("trusted_schema"),
        NoArg("writable_schema"),
        // Boolean pragmas
        BoolArg("foreign_keys"),
        BoolArg("case_sensitive_like"),
        BoolArg("automatic_index"),
        BoolArg("cache_spill"),
        BoolArg("recursive_triggers"),
        BoolArg("journal_size_limit"),
        BoolArg("legacy_file_format"),
        BoolArg("writable_schema"),
        BoolArg("secure_delete"),
        BoolArg("read_uncommitted"),
        BoolArg("reverse_unordered_selects"),
        BoolArg("trusted_schema"),
        // Integer pragmas
        IntArg("cache_size", -10000, 10000),
        IntArg("page_size", 512, 65536),
        IntArg("mmap_size", 0, 104857600),
        IntArg("wal_autocheckpoint", 1, 10000),
        IntArg("max_page_count", 1, 1000000),
        IntArg("soft_heap_limit", 0, 104857600),
        IntArg("threads", 0, 8),
        // String pragmas
        StringArg("journal_mode"),
        StringArg("locking_mode"),
        StringArg("synchronous"),
        StringArg("temp_store"),
        StringArg("encoding"),
    ];
    let idx = (rng.rand().unsigned_abs() as usize) % PRAGMAS.len();
    let pragma = &PRAGMAS[idx];
    let sql = match pragma {
        NoArg(name) => format!("PRAGMA {};", name),
        BoolArg(name) => {
            let val = if rng.rand().abs() % 2 == 0 {
                "ON"
            } else {
                "OFF"
            };
            format!("PRAGMA {} = {};", name, val)
        }
        IntArg(name, min, max) => {
            let val = min + (rng.rand().unsigned_abs() as i64) % (max - min + 1);
            format!("PRAGMA {} = {};", name, val)
        }
        StringArg(name) => {
            let val = match *name {
                "journal_mode" => {
                    const MODES: &[&str] =
                        &["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"];
                    MODES[(rng.rand().unsigned_abs() as usize) % MODES.len()]
                }
                "locking_mode" => {
                    const MODES: &[&str] = &["NORMAL", "EXCLUSIVE"];
                    MODES[(rng.rand().unsigned_abs() as usize) % MODES.len()]
                }
                "synchronous" => {
                    const MODES: &[&str] = &["OFF", "NORMAL", "FULL", "EXTRA"];
                    MODES[(rng.rand().unsigned_abs() as usize) % MODES.len()]
                }
                "temp_store" => {
                    const MODES: &[&str] = &["DEFAULT", "FILE", "MEMORY"];
                    MODES[(rng.rand().unsigned_abs() as usize) % MODES.len()]
                }
                "encoding" => {
                    const MODES: &[&str] =
                        &["\"UTF-8\"", "\"UTF-16\"", "\"UTF-16le\"", "\"UTF-16be\""];
                    MODES[(rng.rand().unsigned_abs() as usize) % MODES.len()]
                }
                _ => "ON",
            };
            format!("PRAGMA {} = {};", name, val)
        }
    };
    Some(sql)
}
