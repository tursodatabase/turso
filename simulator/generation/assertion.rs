use anyhow::{Result as AnyResult, anyhow};
use sql_generation::model::table::SimValue;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use tracing::instrument;
use turso_core::{LimboError, Value};

use crate::runner::env::SimulatorEnv;

// ---------- Values & schema ----------

#[derive(Clone, Debug)]
pub struct Relation {
    pub schema: Vec<String>,      // column names
    pub rows: Vec<Vec<SimValue>>, // bag/multiset of rows
}

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Schema: {:?}", self.schema)?;
        writeln!(f, "Rows:")?;
        for r in &self.rows {
            for (i, v) in r.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", v)?;
            }
            writeln!(f)?; // new line after each row
        }
        Ok(())
    }
}

impl Relation {
    pub fn new(schema: Vec<String>) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        Self {
            schema: Vec::new(),
            rows: Vec::new(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.schema.is_empty() && self.rows.is_empty()
    }

    pub fn from_rows(rows: Vec<Vec<SimValue>>) -> Self {
        let width = rows.first().map(|r| r.len()).unwrap_or(0);
        let schema = (0..width).map(|i| format!("c{}", i + 1)).collect();
        Self { schema, rows }
    }
    pub fn project_by_names(&self, cols: &[String]) -> AnyResult<Relation> {
        let indices = cols
            .iter()
            .map(|c| {
                self.schema
                    .iter()
                    .position(|s| s.eq_ignore_ascii_case(c))
                    .ok_or_else(|| {
                        anyhow!("project: column '{}' not found in {:?}", c, self.schema)
                    })
            })
            .collect::<AnyResult<Vec<_>>>()?;
        Ok(self.project_by_indices(&indices))
    }
    pub fn project_by_indices(&self, idx: &[usize]) -> Relation {
        let schema = idx.iter().map(|&i| self.schema[i].clone()).collect();
        let rows = self
            .rows
            .iter()
            .map(|r| idx.iter().map(|&i| r[i].clone()).collect::<Vec<_>>())
            .collect();
        Relation { schema, rows }
    }
    pub fn distinct(&self) -> Relation {
        let mut seen: HashSet<RowKey> = HashSet::new();
        let mut rows = Vec::new();
        for r in &self.rows {
            let k = RowKey(r.clone());
            if seen.insert(k) {
                rows.push(r.clone());
            }
        }
        Relation {
            schema: self.schema.clone(),
            rows,
        }
    }
}

// RowKey uses SimValue’s Eq/Hash so NULL==NULL holds (SQL “IS NOT DISTINCT FROM”).
#[derive(Clone, Debug, Eq)]
pub struct RowKey(Vec<SimValue>);
impl PartialEq for RowKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Hash for RowKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.0 {
            match v {
                SimValue(Value::Null) => {
                    // all NULLs hash to the same value
                    0u8.hash(state);
                }
                SimValue(v) => {
                    // other values use their string representation for hashing
                    v.to_string().hash(state);
                }
            }
        }
    }
}

// ---------- Environment & bindings ----------

pub type BoundValue = Result<Relation, LimboError>;

pub type Bindings = HashMap<String, BoundValue>;

// ---------- AST (as previously discussed) ----------

pub type RelVar = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Col(pub String);

#[derive(Debug, Clone)]
pub enum RelExpr {
    Var(RelVar),
    Literal {
        schema: Vec<String>,
        rows: Vec<Vec<SimValue>>,
    },
    EnvTable {
        conn_index: usize,
        table: String,
    },
    Project {
        input: Box<RelExpr>,
        cols: Vec<Col>,
    },
    Distinct(Box<RelExpr>),
    Union {
        left: Box<RelExpr>,
        right: Box<RelExpr>,
    }, // set union
    UnionAll {
        left: Box<RelExpr>,
        right: Box<RelExpr>,
    }, // bag union
    Intersect {
        left: Box<RelExpr>,
        right: Box<RelExpr>,
    }, // set intersect
    Except {
        left: Box<RelExpr>,
        right: Box<RelExpr>,
    }, // set difference
}

impl RelExpr {
    pub fn rows(rows: Vec<Vec<SimValue>>) -> Self {
        let schema = if let Some(first) = rows.first() {
            (0..first.len()).map(|i| format!("c{}", i + 1)).collect()
        } else {
            Vec::new()
        };
        RelExpr::Literal { schema, rows }
    }
}

#[derive(Debug, Clone)]
pub enum CountExpr {
    Count(RelExpr),
    Int(i64),
    Add(Box<CountExpr>, Box<CountExpr>),
    Sub(Box<CountExpr>, Box<CountExpr>),
}

#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
pub enum Assertion {
    EqBag {
        left: RelExpr,
        right: RelExpr,
    },
    EqSet {
        left: RelExpr,
        right: RelExpr,
    },
    Subset {
        left: RelExpr,
        right: RelExpr,
    }, // set ⊆
    BagSubset {
        left: RelExpr,
        right: RelExpr,
    }, // multiset ⊆
    Disjoint {
        left: RelExpr,
        right: RelExpr,
    }, // set disjointness
    Contains {
        sup: RelExpr,
        sub: RelExpr,
    }, // sub must be 1-row
    SchemaEq {
        left: RelExpr,
        right: RelExpr,
    },

    CountCmp {
        left: CountExpr,
        op: CmpOp,
        right: CountExpr,
    },

    IsError {
        var: RelVar,
    },
    IsErrorLike {
        var: RelVar,
        needle: String,
        reason: String,
    },

    SchemaHasTable {
        conn_index: usize,
        table: String,
    },
    SchemaNoTable {
        conn_index: usize,
        table: String,
    },

    Not(Box<Assertion>),
    And(Box<Assertion>, Box<Assertion>),
    Or(Box<Assertion>, Box<Assertion>),
    Forall {
        bind: RelVar,
        iter: RelExpr,
        assert: Box<Assertion>,
    },
}

impl Assertion {
    pub fn table_should_not_exist(table_name: &str, connection_index: usize) -> Self {
        Assertion::SchemaNoTable {
            conn_index: connection_index,
            table: table_name.to_string(),
        }
    }

    pub fn table_exists(table_name: &str) -> Self {
        // keep conn 0 semantics from your original
        Assertion::SchemaHasTable {
            conn_index: 0,
            table: table_name.to_string(),
        }
    }

    pub fn expect_error(var: String, err_substr: String, reason: String) -> Self {
        Assertion::IsErrorLike {
            var,
            needle: err_substr,
            reason: reason,
        }
    }

    /// Require naming the result you want to compare (better than implicit “top of stack”).
    pub fn compare_result_length(var: String, other: usize, op: &str, _reason: &str) -> Self {
        let op = match op {
            "=" => CmpOp::Eq,
            "!=" => CmpOp::Ne,
            "<" => CmpOp::Lt,
            "<=" => CmpOp::Le,
            ">" => CmpOp::Gt,
            ">=" => CmpOp::Ge,
            _ => panic!("unknown comparison op {}", op),
        };
        Assertion::CountCmp {
            left: CountExpr::Count(RelExpr::Var(var)),
            op,
            right: CountExpr::Int(other as i64),
        }
        // If you want the ‘reason’ string in results, store it alongside or in a wrapper struct.
    }

    /// Compare current result against an environment table snapshot (bag equality).
    pub fn shadow_equivalence(var: String, table: String, conn_index: usize) -> Self {
        Assertion::EqBag {
            left: RelExpr::Var(var),
            right: RelExpr::EnvTable { conn_index, table },
        }
    }

    /// Assert that all expected rows (with multiplicity) are included in var’s result.
    pub fn expected_in_result_set(var: String, expected: Vec<Vec<SimValue>>) -> Self {
        Assertion::BagSubset {
            left: RelExpr::rows(expected),
            right: RelExpr::Var(var),
        }
    }

    /// Assert exact bag equality with provided expected rows (order-insensitive).
    pub fn expected_result(var: String, expected: Vec<Vec<SimValue>>) -> Self {
        Assertion::EqBag {
            left: RelExpr::Var(var),
            right: RelExpr::rows(expected),
        }
    }
}

// ---------- Pretty helpers ----------

impl Display for Col {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Display for CmpOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        })
    }
}

impl Display for RelExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use RelExpr::*;
        match self {
            Var(v) => f.write_str(v),

            Literal { schema, rows } => {
                // VALUES-like display: VALUES (..), (..)
                f.write_str("columns[")?;
                for (i, col) in schema.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(col)?;
                }
                f.write_str("]")?;
                f.write_str("rows[")?;
                for (i, r) in rows.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str("(")?;
                    for (j, v) in r.iter().enumerate() {
                        if j > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{}", v)?;
                    }
                    f.write_str(")")?;
                }
                f.write_str("]")
            }
            EnvTable { conn_index, table } => write!(f, "env({}, {})", conn_index, table),
            Project { input, cols } => {
                f.write_str("project(")?;
                write!(f, "{input}")?;
                if !cols.is_empty() {
                    f.write_str(", ")?;
                    for (i, c) in cols.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{c}")?;
                    }
                }
                f.write_str(")")
            }
            Distinct(inner) => write!(f, "distinct({inner})"),
            Union { left, right } => write!(f, "({left}) ∪ ({right})"),
            UnionAll { left, right } => write!(f, "({left}) ∪ ALL ({right})"),
            Intersect { left, right } => write!(f, "({left}) ∩ ({right})"),
            Except { left, right } => write!(f, "({left}) \\ ({right})"),
        }
    }
}

impl Display for CountExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use CountExpr::*;
        match self {
            Count(r) => write!(f, "count({r})"),
            Int(i) => write!(f, "{i}"),
            Add(a, b) => write!(f, "({a}) + ({b})"),
            Sub(a, b) => write!(f, "({a}) - ({b})"),
        }
    }
}

impl Display for Assertion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use Assertion::*;
        match self {
            EqBag { left, right } => write!(f, "{left} = {right}"),
            EqSet { left, right } => write!(f, "{left} ≡ {right}"),
            Subset { left, right } => write!(f, "{left} ⊆ {right}"),
            BagSubset { left, right } => write!(f, "{left} ⊆ₘ {right}"),
            Disjoint { left, right } => write!(f, "disjoint({left}, {right})"),
            Contains { sup, sub } => write!(f, "contains({sup}, {sub})"),
            SchemaEq { left, right } => write!(f, "schema_eq({left}, {right})"),
            CountCmp { left, op, right } => write!(f, "{left} {op} {right}"),
            IsError { var } => write!(f, "error({var})"),
            IsErrorLike {
                var,
                needle,
                reason,
            } => {
                write!(f, "error_like({var}, {:?}, {:?})", needle, reason)
            }
            SchemaHasTable { conn_index, table } => {
                write!(f, "has_table(conn={}, {})", conn_index, table)
            }
            SchemaNoTable { conn_index, table } => {
                write!(f, "no_table(conn={}, {})", conn_index, table)
            }
            Not(a) => write!(f, "not({a})"),
            And(a, b) => write!(f, "({a}) and ({b})"),
            Or(a, b) => write!(f, "({a}) or ({b})"),
            Forall { bind, iter, assert } => {
                write!(f, "for {bind} in {iter}:")?;
                write!(f, "\t{assert}")
            }
        }
    }
}

/// ---------- Result types ----------

#[derive(Debug, Clone)]
pub enum CompareResult {
    Ok,
    Err(CompareReason),
}

#[derive(Debug, Clone)]
pub enum CompareReason {
    // Schema issues
    SchemaLenMismatch {
        left_len: usize,
        right_len: usize,
    },
    SchemaMismatch {
        left: Vec<String>,
        right: Vec<String>,
    },

    // Bag equality: what differs between multisets
    /// Rows that are missing in the right side (need > have) and extra in the right (have > need)
    BagDiff {
        /// For each row: (row, need_in_left, have_in_right)
        missing_in_right: Vec<(RowKey, usize, usize)>,
        /// For each row: (row, have_in_right, need_in_left)
        extra_in_right: Vec<(RowKey, usize, usize)>,
    },

    // Bag subset: which rows have insufficient multiplicity in the superset
    /// For each row: (row, have_in_sup, need_in_sub)
    BagShortfall(Vec<(RowKey, usize, usize)>),

    // Set subset: which rows are missing in the superset (presence only)
    SetMissing(Vec<RowKey>),

    // Set disjointness: which rows violate disjointness
    SetIntersection(Vec<RowKey>),
}

impl Display for CompareReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use CompareReason::*;
        match self {
            SchemaLenMismatch {
                left_len,
                right_len,
            } => write!(
                f,
                "schema length mismatch: left={}, right={}",
                left_len, right_len
            ),
            SchemaMismatch { left, right } => {
                write!(f, "schema mismatch: left={:?}, right={:?}", left, right)
            }
            BagDiff {
                missing_in_right,
                extra_in_right,
            } => {
                if !missing_in_right.is_empty() {
                    writeln!(f, "missing in right (need vs have):")?;
                    for (rk, need, have) in missing_in_right {
                        writeln!(f, "  {} : need {}, have {}", rk, need, have)?;
                    }
                }
                if !extra_in_right.is_empty() {
                    writeln!(f, "extra in right (have vs need):")?;
                    for (rk, have, need) in extra_in_right {
                        writeln!(f, "  {} : have {}, need {}", rk, have, need)?;
                    }
                }
                Ok(())
            }
            BagShortfall(items) => {
                writeln!(f, "bag shortfall in superset (have vs need):")?;
                for (rk, have, need) in items {
                    writeln!(f, "  {} : have {}, need {}", rk, have, need)?;
                }
                Ok(())
            }
            SetMissing(rows) => {
                writeln!(f, "set missing in superset:")?;
                for rk in rows {
                    writeln!(f, "  {}", rk)?;
                }
                Ok(())
            }
            SetIntersection(rows) => {
                writeln!(f, "sets are not disjoint; common rows:")?;
                for rk in rows {
                    writeln!(f, "  {}", rk)?;
                }
                Ok(())
            }
        }
    }
}

/// Optional: limit how many rows we include in reasons to keep messages manageable.
const DIFF_CAP: usize = 50;

// Pretty-print RowKey concisely.
impl Display for RowKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{v}")?;
        }
        f.write_str("]")
    }
}

/// Convert rows to a multiset (bag) keyed by RowKey.
fn to_multiset(rows: &[Vec<SimValue>]) -> HashMap<RowKey, usize> {
    let mut m = HashMap::new();
    for r in rows {
        *m.entry(RowKey(r.clone())).or_insert(0) += 1;
    }
    m
}

/// Truncate a Vec to at most `cap` items (non-panicking).
fn cap<T>(mut v: Vec<T>, cap_n: usize) -> Vec<T> {
    if v.len() > cap_n {
        v.truncate(cap_n);
    }
    v
}

/// ---------- Detailed comparisons (bag) ----------

pub fn bag_eq(a: &Relation, b: &Relation) -> CompareResult {
    if a.rows.is_empty() && b.rows.is_empty() {
        return CompareResult::Ok;
    }

    if a.schema.len() != b.schema.len() {
        return CompareResult::Err(CompareReason::SchemaLenMismatch {
            left_len: a.schema.len(),
            right_len: b.schema.len(),
        });
    }
    // If you require exact name/type equality, swap the len check for full schema compare:
    // if a.schema != b.schema { return Err(CompareReason::SchemaMismatch { ... }) }

    let ma = to_multiset(&a.rows);
    let mb = to_multiset(&b.rows);

    if ma == mb {
        return CompareResult::Ok;
    }

    // Build diffs
    let mut missing_in_right = Vec::new();
    let mut extra_in_right = Vec::new();

    // Keys present anywhere (union)
    let mut all_keys: HashSet<RowKey> = ma.keys().cloned().collect();
    all_keys.extend(mb.keys().cloned());

    for k in all_keys {
        let need = ma.get(&k).copied().unwrap_or(0);
        let have = mb.get(&k).copied().unwrap_or(0);
        if need > have {
            missing_in_right.push((k.clone(), need, have));
        } else if have > need {
            extra_in_right.push((k.clone(), have, need));
        }
    }

    CompareResult::Err(CompareReason::BagDiff {
        missing_in_right: cap(missing_in_right, DIFF_CAP),
        extra_in_right: cap(extra_in_right, DIFF_CAP),
    })
}

pub fn bag_subset(sub: &Relation, sup: &Relation) -> CompareResult {
    if sub.schema.len() != sup.schema.len() {
        return CompareResult::Err(CompareReason::SchemaLenMismatch {
            left_len: sub.schema.len(),
            right_len: sup.schema.len(),
        });
    }

    let need = to_multiset(&sub.rows);
    let have = to_multiset(&sup.rows);

    let mut short = Vec::new();
    for (k, n) in need {
        let h = have.get(&k).copied().unwrap_or(0);
        if h < n {
            short.push((k, h, n));
        }
    }

    if short.is_empty() {
        CompareResult::Ok
    } else {
        CompareResult::Err(CompareReason::BagShortfall(cap(short, DIFF_CAP)))
    }
}

/// ---------- Detailed comparisons (set) ----------

pub fn set_eq(a: &Relation, b: &Relation) -> CompareResult {
    if a.schema != b.schema {
        return CompareResult::Err(CompareReason::SchemaMismatch {
            left: a.schema.clone(),
            right: b.schema.clone(),
        });
    }
    let da = a.distinct();
    let db = b.distinct();
    bag_eq(&da, &db) // defers to bag_eq, which will return BagDiff with multiplicity 1
}

pub fn set_subset(sub: &Relation, sup: &Relation) -> CompareResult {
    if sub.schema != sup.schema {
        return CompareResult::Err(CompareReason::SchemaMismatch {
            left: sub.schema.clone(),
            right: sup.schema.clone(),
        });
    }
    let da = sub.distinct();
    let ds = sup.distinct();

    let have: HashSet<RowKey> = ds.rows.into_iter().map(RowKey).collect();
    let want: HashSet<RowKey> = da.rows.into_iter().map(RowKey).collect();

    let missing: Vec<RowKey> = want.difference(&have).cloned().collect();

    if missing.is_empty() {
        CompareResult::Ok
    } else {
        CompareResult::Err(CompareReason::SetMissing(cap(missing, DIFF_CAP)))
    }
}

pub fn set_disjoint(a: &Relation, b: &Relation) -> CompareResult {
    if a.schema != b.schema {
        // Prior code returned true when schemas differ; keep that behavior but
        // you could change to SchemaMismatch if you prefer strictness.
        return CompareResult::Ok;
    }
    let da = a.distinct();
    let db = b.distinct();

    let sa: HashSet<RowKey> = da.rows.into_iter().map(RowKey).collect();
    let sb: HashSet<RowKey> = db.rows.into_iter().map(RowKey).collect();

    let inter: Vec<RowKey> = sa.intersection(&sb).cloned().collect();

    if inter.is_empty() {
        CompareResult::Ok
    } else {
        CompareResult::Err(CompareReason::SetIntersection(cap(inter, DIFF_CAP)))
    }
}

// ---------- Evaluators ----------

#[instrument(%ret, skip(gamma, env, expr), fields(assertion=%expr))]
fn eval_rel(expr: &RelExpr, gamma: &Bindings, env: &SimulatorEnv) -> anyhow::Result<Relation> {
    use RelExpr::*;

    match expr {
        Var(v) => match gamma.get(v) {
            Some(BoundValue::Ok(rel)) => Ok(rel.clone()),
            Some(BoundValue::Err(e)) => Err(anyhow!("binding '{}' is error: {}", v, e)),
            None => Err(anyhow!("unknown binding '{}'", v)),
        },
        Literal { schema, rows } => Ok(Relation {
            schema: schema.clone(),
            rows: rows.clone(),
        }),
        EnvTable { conn_index, table } => env
            .get_conn_tables(*conn_index)
            .commited_tables
            .iter()
            .find(|t| t.name == *table)
            .map(|t| Relation {
                schema: t.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
                rows: t.rows.clone(),
            })
            .ok_or_else(|| anyhow!("table '{}' does not exist", table)),
        Project { input, cols } => {
            let r = eval_rel(input, gamma, env)?;
            let cols: Vec<String> = cols.iter().map(|c| c.0.clone()).collect();
            r.project_by_names(&cols)
        }
        Distinct(input) => Ok(eval_rel(input, gamma, env)?.distinct()),
        Union { left, right } => {
            // set union = distinct(bag union)
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            if l.schema != r.schema {
                return Err(anyhow!(
                    "union: schema mismatch {:?} vs {:?}",
                    l.schema,
                    r.schema
                ));
            }
            let mut rows = l.rows.clone();
            rows.extend(r.rows.clone());
            Ok(Relation {
                schema: l.schema,
                rows,
            }
            .distinct())
        }
        UnionAll { left, right } => {
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            if l.schema != r.schema {
                return Err(anyhow!(
                    "union all: schema mismatch {:?} vs {:?}",
                    l.schema,
                    r.schema
                ));
            }
            let mut rows = l.rows.clone();
            rows.extend(r.rows);
            Ok(Relation {
                schema: l.schema,
                rows,
            })
        }
        Intersect { left, right } => {
            // set intersect
            let l = eval_rel(left, gamma, env)?.distinct();
            let r = eval_rel(right, gamma, env)?.distinct();
            if l.schema != r.schema {
                return Err(anyhow!("intersect: schema mismatch"));
            }
            let sl: HashSet<RowKey> = l.rows.iter().cloned().map(RowKey).collect();
            let sr: HashSet<RowKey> = r.rows.iter().cloned().map(RowKey).collect();
            let rows: Vec<Vec<SimValue>> = sl.intersection(&sr).map(|k| k.0.clone()).collect();
            Ok(Relation {
                schema: l.schema,
                rows,
            })
        }
        Except { left, right } => {
            // set difference: left \ right
            let l = eval_rel(left, gamma, env)?.distinct();
            let r = eval_rel(right, gamma, env)?.distinct();
            if l.schema != r.schema {
                return Err(anyhow!("except: schema mismatch"));
            }
            let sl: HashSet<RowKey> = l.rows.iter().cloned().map(RowKey).collect();
            let sr: HashSet<RowKey> = r.rows.iter().cloned().map(RowKey).collect();
            let rows: Vec<Vec<SimValue>> = sl.difference(&sr).map(|k| k.0.clone()).collect();
            Ok(Relation {
                schema: l.schema,
                rows,
            })
        }
    }
}

fn eval_count(e: &CountExpr, gamma: &Bindings, env: &SimulatorEnv) -> AnyResult<i64> {
    use CountExpr::*;
    match e {
        Count(expr) => Ok(eval_rel(expr, gamma, env)?.rows.len() as i64),
        Int(i) => Ok(*i),
        Add(a, b) => Ok(eval_count(a, gamma, env)? + eval_count(b, gamma, env)?),
        Sub(a, b) => Ok(eval_count(a, gamma, env)? - eval_count(b, gamma, env)?),
    }
}

fn cmp_usize(op: CmpOp, l: i64, r: i64) -> bool {
    match op {
        CmpOp::Eq => l == r,
        CmpOp::Ne => l != r,
        CmpOp::Lt => l < r,
        CmpOp::Le => l <= r,
        CmpOp::Gt => l > r,
        CmpOp::Ge => l >= r,
    }
}

#[instrument(ret, skip(a, gamma, env), fields(assertion=%a))]
pub fn eval_assertion(a: &Assertion, gamma: &Bindings, env: &SimulatorEnv) -> anyhow::Result<()> {
    use Assertion::*;
    let fail = |msg: String| {
        tracing::error!("Assertion {a} failed: {}", msg);
        Err(anyhow::anyhow!(msg))
    };

    match a {
        EqBag { left, right } => {
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            match bag_eq(&l, &r) {
                CompareResult::Ok => Ok(()),
                CompareResult::Err(reason) => fail(format!("bag equality failed:\n{}", reason)),
            }
        }
        EqSet { left, right } => {
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            match set_eq(&l, &r) {
                CompareResult::Ok => Ok(()),
                CompareResult::Err(reason) => fail(format!("set equality failed:\n{}", reason)),
            }
        }
        Subset { left, right } => {
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            match set_subset(&l, &r) {
                CompareResult::Ok => Ok(()),
                CompareResult::Err(reason) => {
                    fail(format!("set inclusion failed (left ⊆ right): {}", reason))
                }
            }
        }
        BagSubset { left, right } => {
            println!("Evaluating BagSubset assertion...");
            println!("Left expression: {:?}", left);
            println!("Right expression: {:?}", right);
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            match bag_subset(&l, &r) {
                CompareResult::Ok => Ok(()),
                CompareResult::Err(reason) => {
                    tracing::info!("left relation: {}", l);
                    tracing::info!("right relation: {}", r);
                    fail(format!("bag inclusion failed (left ⊆ₘ right): {}", reason))
                }
            }
        }
        Disjoint { left, right } => {
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            match set_disjoint(&l, &r) {
                CompareResult::Ok => Ok(()),
                CompareResult::Err(reason) => fail(format!("disjointness failed: {}", reason)),
            }
        }
        Contains { sup, sub } => {
            let s = eval_rel(sup, gamma, env)?;
            let t = eval_rel(sub, gamma, env)?;
            if t.rows.len() != 1 || t.schema != s.schema {
                return fail(format!(
                    "contains(): sub must be 1 row and schemas equal; got sub_rows={}, sub_schema={:?}, sup_schema={:?}",
                    t.rows.len(),
                    t.schema,
                    s.schema
                ));
            }
            let ms = to_multiset(&s.rows);
            let key = RowKey(t.rows[0].clone());
            if ms.get(&key).copied().unwrap_or(0) >= 1 {
                Ok(())
            } else {
                fail("contains failed".into())
            }
        }
        SchemaEq { left, right } => {
            let l = eval_rel(left, gamma, env)?;
            let r = eval_rel(right, gamma, env)?;
            if l.schema == r.schema {
                Ok(())
            } else {
                fail(format!("schema mismatch: {:?} vs {:?}", l.schema, r.schema))
            }
        }
        CountCmp { left, op, right } => {
            let l = eval_count(left, gamma, env)?;
            let r = eval_count(right, gamma, env)?;
            if cmp_usize(*op, l, r) {
                Ok(())
            } else {
                fail(format!(
                    "count compare failed: {l} {op} {r} (lhs={l}, rhs={r})",
                ))
            }
        }
        IsError { var } => match gamma.get(var) {
            Some(BoundValue::Err(_)) => Ok(()),
            Some(BoundValue::Ok(rel)) => {
                fail(format!("expected error, got {} row(s)", rel.rows.len()))
            }
            None => fail(format!("unknown binding '{}'", var)),
        },
        IsErrorLike {
            var,
            needle,
            reason,
        } => match gamma.get(var) {
            Some(BoundValue::Err(e)) => {
                let msg = e.to_string().to_lowercase();
                if msg.contains(&needle.to_lowercase()) {
                    Ok(())
                } else {
                    fail(format!(
                        "expected error containing '{}' due to {}, but got '{}'",
                        needle, reason, msg
                    ))
                }
            }
            Some(BoundValue::Ok(rel)) => fail(format!(
                "expected error containing '{}' due to {}, but got {} row(s)",
                needle,
                reason,
                rel.rows.len()
            )),
            None => fail(format!("unknown binding '{}'", var)),
        },
        SchemaHasTable { conn_index, table } => {
            if env
                .get_conn_tables(*conn_index)
                .commited_tables
                .iter()
                .find(|t| t.name == *table)
                .is_some()
            {
                Ok(())
            } else {
                fail(format!("table '{}' does not exist", table))
            }
        }
        SchemaNoTable { conn_index, table } => {
            if env
                .get_conn_tables(*conn_index)
                .commited_tables
                .iter()
                .find(|t| t.name == *table)
                .is_some()
            {
                fail(format!("table '{}' already exists", table))
            } else {
                Ok(())
            }
        }
        Not(b) => match eval_assertion(b, gamma, env) {
            anyhow::Result::Ok(_) => fail("not: inner assertion succeeded".into()),
            anyhow::Result::Err(_) => Ok(()),
        },
        And(l, r) => {
            match eval_assertion(l, gamma, env) {
                anyhow::Result::Ok(_) => eval_assertion(r, gamma, env),
                anyhow::Result::Err(e) => Err(e), // short-circuit
            }
        }
        Or(l, r) => {
            match eval_assertion(l, gamma, env) {
                anyhow::Result::Ok(_) => Ok(()), // short-circuit
                anyhow::Result::Err(_) => eval_assertion(r, gamma, env),
            }
        }
        Forall { bind, iter, assert } => {
            let rel = eval_rel(iter, gamma, env)?;
            for row in rel.rows {
                let mut gamma_ext = gamma.clone();
                let row_relation = Relation {
                    schema: rel.schema.clone(),
                    rows: vec![row.clone()],
                };
                gamma_ext.insert(bind.clone(), Ok(row_relation));
                match eval_assertion(assert, &gamma_ext, env) {
                    anyhow::Result::Ok(_) => {} // continue
                    anyhow::Result::Err(e) => {
                        return fail(format!(
                            "forall: assertion failed for binding '{}': {}",
                            bind, e
                        ));
                    }
                }
            }
            Ok(())
        }
    }
}
