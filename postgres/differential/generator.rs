//! Random cases of statements with INSERT, UPDATE and DELETE inside WITH.
//!
//! Every table has the shape `(id int PRIMARY KEY, a int, b text, u int
//! UNIQUE)` with an index on `a`, and every CTE returns rows of the shape
//! `(id, a, b)`, so any part can read any table or earlier CTE.
//!
//! PostgreSQL runs the CTEs that nothing reads after the main statement, in a
//! fixed order. It runs a CTE that another part reads row by row while the
//! reader runs, so the order of the two depends on its query plan; the engine
//! runs such a CTE to completion first. So the generator keeps one rule: an
//! INSERT, UPDATE or DELETE whose rows another part reads is the only part
//! that writes its table. Then the order cannot change the result, and real
//! PostgreSQL is an exact oracle.
//!
//! The generator also avoids differences that have nothing to do with
//! INSERT, UPDATE and DELETE inside WITH:
//! - A plain UPDATE or DELETE of the PostgreSQL frontend reads its own
//!   partial writes in subqueries of SET and RETURNING, so only statements
//!   with INSERT, UPDATE or DELETE inside WITH read their target there.
//! - A plain INSERT ... ON CONFLICT DO UPDATE that meets its own row twice is
//!   an error in PostgreSQL but not in the engine, so the rows of one INSERT
//!   have distinct `id` and `u` values.
//! - Unique checks run row by row in both engines, in an order neither
//!   defines, so UPDATE never shifts `id` or `u` into the range of other rows:
//!   it moves `id` past every existing id, or sets `u` to a constant.

use rand::seq::IndexedRandom;
use rand::Rng;
use rand_chacha::ChaCha8Rng;

/// One fuzz case: tables, their rows, and the statements to compare.
pub struct Case {
    pub tables: Vec<String>,
    pub setup: Vec<String>,
    pub statements: Vec<String>,
}

pub struct Generator {
    rng: ChaCha8Rng,
    /// Make `a` of every table after the first a foreign key to `t0.id`.
    foreign_keys: bool,
    /// UPDATEs of the case so far that move rows to new ids.
    id_moves: u32,
}

const TEXTS: &[&str] = &["x", "y", "z", "xy"];

/// Largest id of the rows a case starts with.
const MAX_SETUP_ID: i64 = 40;

impl Generator {
    pub fn new(rng: ChaCha8Rng, foreign_keys: bool) -> Self {
        Self {
            rng,
            foreign_keys,
            id_moves: 0,
        }
    }

    pub fn case(&mut self, statement_count: usize) -> Case {
        let table_count = self.rng.random_range(2..=3);
        let tables: Vec<String> = (0..table_count).map(|i| format!("t{i}")).collect();
        let mut setup = Vec::new();
        let mut parent_ids = Vec::new();
        for (table_index, table) in tables.iter().enumerate() {
            let is_child = self.foreign_keys && table_index > 0;
            let a_column = if is_child {
                let on_delete = ["CASCADE", "SET NULL", "NO ACTION"]
                    .choose(&mut self.rng)
                    .unwrap();
                format!("a int REFERENCES t0 (id) ON DELETE {on_delete}")
            } else {
                "a int".to_string()
            };
            setup.push(format!(
                "CREATE TABLE {table} (id int PRIMARY KEY, {a_column}, b text, u int UNIQUE)"
            ));
            setup.push(format!("CREATE INDEX {table}_a ON {table} (a)"));
            let row_count = self.rng.random_range(0..=20);
            let mut ids: Vec<i64> = (1..=MAX_SETUP_ID).collect();
            let mut unique_values: Vec<i64> = (1..=MAX_SETUP_ID).collect();
            let mut rows = Vec::new();
            for _ in 0..row_count {
                let index = self.rng.random_range(0..ids.len());
                let id = ids.swap_remove(index);
                let a = if is_child {
                    match parent_ids.choose(&mut self.rng) {
                        Some(parent_id) if self.rng.random_bool(0.85) => format!("{parent_id}"),
                        _ => "NULL".to_string(),
                    }
                } else {
                    self.int_value()
                };
                if table_index == 0 {
                    parent_ids.push(id);
                }
                let u = if self.rng.random_bool(0.2) {
                    "NULL".to_string()
                } else {
                    let index = self.rng.random_range(0..unique_values.len());
                    unique_values.swap_remove(index).to_string()
                };
                let b = if self.rng.random_bool(0.1) {
                    // Larger than a page, so the row spills to overflow pages.
                    format!("'{}{}'", "x".repeat(5000), id)
                } else {
                    self.text_value()
                };
                rows.push(format!("({id}, {a}, {b}, {u})"));
            }
            if !rows.is_empty() {
                setup.push(format!("INSERT INTO {table} VALUES {}", rows.join(", ")));
            }
        }
        let statements = (0..statement_count)
            .map(|_| self.statement(&tables))
            .collect();
        Case {
            tables,
            setup,
            statements,
        }
    }

    fn statement(&mut self, tables: &[String]) -> String {
        if self.rng.random_bool(0.15) {
            // A plain statement changes the data the next statements see.
            let target = tables.choose(&mut self.rng).unwrap().clone();
            let sources: Vec<Source> = tables.iter().cloned().map(Source::Table).collect();
            let returning = self.rng.random_bool(0.3);
            let kind = *[PartKind::Insert, PartKind::Update, PartKind::Delete]
                .choose(&mut self.rng)
                .unwrap();
            return self.dml(kind, &target, &sources, returning, false);
        }
        // Rejection sampling: plans that break the interleaving rule are dropped.
        loop {
            let plan = self.plan(tables);
            if plan.follows_interleaving_rule() {
                return self.render(&plan, tables);
            }
        }
    }

    fn plan(&mut self, tables: &[String]) -> Plan {
        let cte_count = self.rng.random_range(1..=3);
        let mut parts = Vec::with_capacity(cte_count + 1);
        for index in 0..=cte_count {
            let is_main = index == cte_count;
            let kind = match self.rng.random_range(0..10) {
                0..=2 if !is_main => PartKind::Select,
                0..=3 if is_main => PartKind::Select,
                4..=5 => PartKind::Insert,
                6..=7 => PartKind::Update,
                _ => PartKind::Delete,
            };
            let target = match kind {
                PartKind::Select => None,
                _ => Some(self.rng.random_range(0..tables.len())),
            };
            // A part reads up to two earlier CTEs; the rest of its reads are tables.
            let mut reads = Vec::new();
            for _ in 0..self.rng.random_range(0..=2) {
                if index > 0 && self.rng.random_bool(0.6) {
                    let cte = self.rng.random_range(0..index);
                    if !reads.contains(&cte) {
                        reads.push(cte);
                    }
                }
            }
            let returning = is_main && self.rng.random_bool(0.5)
                || !is_main && kind != PartKind::Select && self.rng.random_bool(0.8);
            parts.push(PlanPart {
                kind,
                target,
                reads,
                returning,
            });
        }
        // A CTE without RETURNING rows cannot be read.
        for index in 0..parts.len() {
            let reads = parts[index].reads.clone();
            let readable: Vec<usize> = reads
                .into_iter()
                .filter(|&read| parts[read].kind == PartKind::Select || parts[read].returning)
                .collect();
            parts[index].reads = readable;
        }
        Plan { parts }
    }

    fn render(&mut self, plan: &Plan, tables: &[String]) -> String {
        let mut ctes = Vec::new();
        let main_index = plan.parts.len() - 1;
        let mut main = String::new();
        // Without INSERT, UPDATE or DELETE inside WITH, the statement is a
        // plain statement with a SELECT CTE.
        let reads_own_target = plan.parts[..main_index]
            .iter()
            .any(|part| part.kind != PartKind::Select);
        for (index, part) in plan.parts.iter().enumerate() {
            let mut sources: Vec<Source> = part
                .reads
                .iter()
                .map(|&read| Source::Cte(format!("c{read}")))
                .collect();
            if sources.is_empty() || self.rng.random_bool(0.5) {
                sources.push(Source::Table(tables.choose(&mut self.rng).unwrap().clone()));
            }
            let body = match part.kind {
                PartKind::Select => {
                    if index == main_index {
                        self.main_select(&sources)
                    } else {
                        self.select_rows(&sources)
                    }
                }
                _ => {
                    let target = &tables[part.target.expect("DML part has a target")];
                    self.dml(
                        part.kind,
                        target,
                        &sources,
                        part.returning,
                        reads_own_target,
                    )
                }
            };
            if index == main_index {
                main = body;
            } else {
                ctes.push(format!("c{index} AS ({body})"));
            }
        }
        format!("WITH {} {main}", ctes.join(", "))
    }

    /// `reads_own_target`: subqueries of SET and RETURNING may read the target.
    fn dml(
        &mut self,
        kind: PartKind,
        target: &str,
        sources: &[Source],
        returning: bool,
        reads_own_target: bool,
    ) -> String {
        let (mut sql, qualify) = match kind {
            PartKind::Insert => (self.insert(target, sources), false),
            PartKind::Update => self.update(target, sources, reads_own_target),
            PartKind::Delete => (self.delete(target, sources), false),
            PartKind::Select => unreachable!("not a DML kind"),
        };
        if returning {
            // Known engine bug: in INSERT ... ON CONFLICT DO UPDATE ... RETURNING,
            // a correlated subquery does not see the updated row (it reads NULL
            // or the previous row's values). Until
            // https://github.com/tursodatabase/turso/issues/5489 is fixed, such
            // RETURNING clauses get no subquery, so runs do not stop on it.
            let correlated_subquery = reads_own_target && !sql.contains("DO UPDATE");
            sql.push_str(&self.returning(target, qualify, correlated_subquery));
        }
        sql
    }

    /// RETURNING with the columns `id, a, b`. With UPDATE ... FROM, columns are
    /// qualified, because the FROM table has columns of the same names.
    fn returning(&mut self, target: &str, qualify: bool, correlated_subquery: bool) -> String {
        let column = |name: &str| {
            if qualify {
                format!("{target}.{name}")
            } else {
                name.to_string()
            }
        };
        let (id, a, b) = (column("id"), column("a"), column("b"));
        match self.rng.random_range(0..4) {
            0 => format!(" RETURNING {id} * 1 AS id, coalesce({a}, -1) + 1 AS a, {b} || '!' AS b"),
            1 if correlated_subquery => format!(
                " RETURNING {id}, (SELECT count(*) FROM {target} x WHERE x.id <= {target}.id) AS a, {b}"
            ),
            _ => format!(" RETURNING {id}, {a}, {b}"),
        }
    }

    fn insert(&mut self, target: &str, sources: &[Source]) -> String {
        let mut sql = if self.rng.random_bool(0.5) {
            let mut ids: Vec<i64> = (1..=MAX_SETUP_ID + 8).collect();
            let mut unique_values: Vec<i64> = (1..=MAX_SETUP_ID).collect();
            let rows: Vec<String> = (0..self.rng.random_range(1..=3))
                .map(|_| {
                    let id = ids.swap_remove(self.rng.random_range(0..ids.len()));
                    let u =
                        unique_values.swap_remove(self.rng.random_range(0..unique_values.len()));
                    format!("({id}, {}, {}, {u})", self.int_value(), self.text_value())
                })
                .collect();
            format!("INSERT INTO {target} VALUES {}", rows.join(", "))
        } else {
            let source = sources.choose(&mut self.rng).unwrap().name().to_string();
            let offset = [0, 20, 40].choose(&mut self.rng).unwrap();
            let predicate = self.predicate(&source, sources);
            format!(
                "INSERT INTO {target} SELECT id + {offset}, a, b, id + {offset} FROM {source} WHERE {predicate}"
            )
        };
        let conflict_target = if self.rng.random_bool(0.6) { "id" } else { "u" };
        match self.rng.random_range(0..10) {
            0..=1 => sql.push_str(&format!(" ON CONFLICT ({conflict_target}) DO NOTHING")),
            2..=3 => sql.push_str(&format!(
                " ON CONFLICT ({conflict_target}) DO UPDATE SET a = coalesce(excluded.a, 0) + 1, b = excluded.b"
            )),
            4 => sql.push_str(&format!(
                " ON CONFLICT ({conflict_target}) DO UPDATE SET a = excluded.a WHERE {target}.a IS NOT NULL"
            )),
            _ => {}
        }
        sql
    }

    /// The UPDATE, and whether it has a FROM clause.
    fn update(
        &mut self,
        target: &str,
        sources: &[Source],
        reads_own_target: bool,
    ) -> (String, bool) {
        if self.rng.random_bool(0.25) {
            let source = sources.choose(&mut self.rng).unwrap().name().to_string();
            return (
                format!(
                    "UPDATE {target} SET a = s.a, b = s.b FROM {source} s WHERE {target}.id = s.id"
                ),
                true,
            );
        }
        let set = match self.rng.random_range(0..10) {
            // Moves rows to new rowids. Each move adds a different power of two
            // times 1000, and ids stay below 1000 apart, so two rows never get
            // the same id whatever order the rows are updated in.
            0 if self.id_moves < 20 => {
                let offset = 1000_i64 << self.id_moves;
                self.id_moves += 1;
                format!("id = id + {offset}")
            }
            1 => format!("u = {}", self.rng.random_range(1..=MAX_SETUP_ID)),
            2 if reads_own_target => format!("a = (SELECT max(x.a) FROM {target} x)"),
            3 if reads_own_target => format!(
                "a = (SELECT count(*) FROM {target} x WHERE x.id < {target}.id AND x.a IS NOT NULL)"
            ),
            _ => {
                let value = self.int_expr("a");
                if self.rng.random_bool(0.4) {
                    format!("a = {value}, b = {}", self.text_value())
                } else {
                    format!("a = {value}")
                }
            }
        };
        let predicate = self.predicate(target, sources);
        (
            format!("UPDATE {target} SET {set} WHERE {predicate}"),
            false,
        )
    }

    fn delete(&mut self, target: &str, sources: &[Source]) -> String {
        if self.rng.random_bool(0.1) {
            return format!("DELETE FROM {target}");
        }
        let predicate = self.predicate(target, sources);
        format!("DELETE FROM {target} WHERE {predicate}")
    }

    fn select_rows(&mut self, sources: &[Source]) -> String {
        let source = sources.choose(&mut self.rng).unwrap().name().to_string();
        let predicate = self.predicate(&source, sources);
        format!("SELECT id, a, b FROM {source} WHERE {predicate}")
    }

    fn main_select(&mut self, sources: &[Source]) -> String {
        let first = sources.choose(&mut self.rng).unwrap().name().to_string();
        match self.rng.random_range(0..5) {
            0 => format!("SELECT count(*), sum(a), min(b), max(id) FROM {first}"),
            1 if sources.len() > 1 => {
                let second = sources
                    .iter()
                    .map(Source::name)
                    .find(|name| *name != first)
                    .unwrap()
                    .to_string();
                format!("SELECT x.id, x.a, y.a, y.b FROM {first} x JOIN {second} y ON x.id = y.id")
            }
            2 if sources.len() > 1 => {
                let parts: Vec<String> = sources
                    .iter()
                    .map(|source| format!("SELECT id, a, b FROM {}", source.name()))
                    .collect();
                parts.join(" UNION ALL ")
            }
            3 => {
                let limit = self.rng.random_range(0..=3);
                format!("SELECT id, a, b FROM {first} ORDER BY id, a, b LIMIT {limit}")
            }
            _ => {
                let predicate = self.predicate(&first, sources);
                format!("SELECT id, a, b FROM {first} WHERE {predicate}")
            }
        }
    }

    /// A condition on the rows of `table`, sometimes with a subquery that reads
    /// one of the sources.
    fn predicate(&mut self, table: &str, sources: &[Source]) -> String {
        let k = self.rng.random_range(0..=MAX_SETUP_ID);
        match self.rng.random_range(0..11) {
            0 => "true".to_string(),
            1 => format!("{table}.id % 2 = {}", k % 2),
            2 => format!("{table}.id > {k}"),
            3 => format!("{table}.a > {}", k % 6),
            4 => format!("{table}.a IS NULL"),
            5 => format!("{table}.b = {}", self.text_value()),
            6 => format!("{table}.id IN ({}, {})", k, (k * 7) % (MAX_SETUP_ID + 1)),
            7 | 8 => {
                let source = sources.choose(&mut self.rng).unwrap().name().to_string();
                format!("{table}.id IN (SELECT id FROM {source})")
            }
            9 => {
                let source = sources.choose(&mut self.rng).unwrap().name().to_string();
                format!("EXISTS (SELECT 1 FROM {source} s WHERE s.a = {table}.a)")
            }
            _ => {
                let source = sources.choose(&mut self.rng).unwrap().name().to_string();
                format!("{table}.a >= (SELECT min(a) FROM {source})")
            }
        }
    }

    fn int_expr(&mut self, column: &str) -> String {
        match self.rng.random_range(0..4) {
            0 => self.int_value(),
            1 => format!("{column} + {}", self.rng.random_range(1..=3)),
            2 => format!("coalesce({column}, 0) * 2"),
            _ => format!("-{column}"),
        }
    }

    fn int_value(&mut self) -> String {
        if self.rng.random_bool(0.15) {
            "NULL".to_string()
        } else {
            self.rng.random_range(0..=6).to_string()
        }
    }

    fn text_value(&mut self) -> String {
        if self.rng.random_bool(0.1) {
            "NULL".to_string()
        } else {
            format!("'{}'", TEXTS.choose(&mut self.rng).unwrap())
        }
    }
}

enum Source {
    Table(String),
    Cte(String),
}

impl Source {
    fn name(&self) -> &str {
        match self {
            Source::Table(name) | Source::Cte(name) => name,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PartKind {
    Select,
    Insert,
    Update,
    Delete,
}

struct PlanPart {
    kind: PartKind,
    /// Index of the table an INSERT, UPDATE or DELETE writes.
    target: Option<usize>,
    /// Earlier CTEs this part reads.
    reads: Vec<usize>,
    returning: bool,
}

/// The CTEs of a statement, followed by its main statement.
struct Plan {
    parts: Vec<PlanPart>,
}

impl Plan {
    /// An INSERT, UPDATE or DELETE whose rows another part reads, directly or
    /// through SELECT CTEs, must be the only part that writes its table.
    fn follows_interleaving_rule(&self) -> bool {
        for (index, part) in self.parts.iter().enumerate() {
            let Some(target) = part.target else {
                continue;
            };
            if !self.is_read(index) {
                continue;
            }
            let shared = self
                .parts
                .iter()
                .enumerate()
                .any(|(other, other_part)| other != index && other_part.target == Some(target));
            if shared {
                return false;
            }
        }
        true
    }

    fn is_read(&self, index: usize) -> bool {
        self.parts.iter().any(|part| part.reads.contains(&index))
    }
}
