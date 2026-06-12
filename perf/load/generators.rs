use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng as _, SeedableRng};

/// A generated scalar value, convertible to a SQL literal or a bind parameter.
#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
}

impl Val {
    pub fn sql_literal(&self) -> String {
        match self {
            Val::Null => "NULL".to_string(),
            Val::Int(n) => n.to_string(),
            Val::Real(r) => r.to_string(),
            Val::Text(s) => format!("'{}'", s.replace('\'', "''")),
        }
    }
}

impl From<&Val> for turso::Value {
    fn from(v: &Val) -> Self {
        match v {
            Val::Null => turso::Value::Null,
            Val::Int(n) => turso::Value::Integer(*n),
            Val::Real(r) => turso::Value::Real(*r),
            Val::Text(s) => turso::Value::Text(s.clone()),
        }
    }
}

pub fn yaml_to_val(v: &serde_yaml::Value) -> Result<Val> {
    Ok(match v {
        serde_yaml::Value::Null => Val::Null,
        serde_yaml::Value::Bool(b) => Val::Int(*b as i64),
        serde_yaml::Value::Number(n) => match n.as_i64() {
            Some(i) => Val::Int(i),
            None => Val::Real(n.as_f64().unwrap()),
        },
        serde_yaml::Value::String(s) => Val::Text(s.clone()),
        other => bail!("unsupported literal value: {other:?}"),
    })
}

/// Deterministic RNG so --seed gives reproducible parameters.
pub struct Rng(StdRng);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(StdRng::seed_from_u64(seed))
    }

    pub fn f64(&mut self) -> f64 {
        self.0.random()
    }

    pub fn int(&mut self, min: i64, max: i64) -> i64 {
        self.0.random_range(min..=max)
    }

    /// Skewed pick in [min, max]: low values are much more likely (log-uniform).
    pub fn zipfian(&mut self, min: i64, max: i64) -> i64 {
        let n = (max - min + 1) as f64;
        let v = ((self.f64() * n.ln()).exp() - 1.0).floor();
        min + v.min(n - 1.0) as i64
    }
}

/// Monotonic counters behind `{seq}` / `sequence` generators. Counters are
/// keyed by generator identity so the seed phase and the bench phase share
/// them, letting runtime inserts continue past the seeded id range.
#[derive(Default)]
pub struct SequenceCounters {
    counters: HashMap<String, i64>,
    /// Applied (in steps) the first time a key is seen. Set before a bench
    /// that did not seed in-process, so runtime ids land far past any rows
    /// inserted by earlier runs.
    pub offset: i64,
}

impl SequenceCounters {
    pub fn next(&mut self, key: &str, start: i64, step: i64) -> i64 {
        let entry = self
            .counters
            .entry(key.to_string())
            .or_insert(start + self.offset * step);
        let current = *entry;
        *entry += step;
        current
    }
}

/// In-memory pools of live key values, one per `table.column` ref target.
/// Seeded from the data section (or hydrated from the database) and kept
/// current as operations insert (`produces`) and delete (`consume`) rows.
pub struct Pools {
    pools: HashMap<String, Vec<Val>>,
}

impl Pools {
    pub fn new(keys: impl IntoIterator<Item = String>) -> Self {
        Self {
            pools: keys.into_iter().map(|k| (k, Vec::new())).collect(),
        }
    }

    pub fn has(&self, key: &str) -> bool {
        self.pools.contains_key(key)
    }

    pub fn add(&mut self, key: &str, value: Val) {
        if let Some(pool) = self.pools.get_mut(key) {
            pool.push(value);
        }
    }

    pub fn size(&self, key: &str) -> usize {
        if let Some(pool) = self.pools.get(key) {
            return pool.len();
        }
        // A bare table name matches any pool on that table, e.g. `core` -> `core.id`.
        self.pools
            .iter()
            .find(|(k, _)| k.starts_with(&format!("{key}.")))
            .map(|(_, v)| v.len())
            .unwrap_or(0)
    }

    pub fn sample(&mut self, rng: &mut Rng, key: &str, consume: bool) -> Option<Val> {
        let pool = self.pools.get_mut(key)?;
        if pool.is_empty() {
            return None;
        }
        let idx = rng.int(0, pool.len() as i64 - 1) as usize;
        if consume {
            Some(pool.swap_remove(idx))
        } else {
            Some(pool[idx].clone())
        }
    }
}

const HEX: &[u8] = b"0123456789abcdef";
const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
const ASCII: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.";

fn random_string(rng: &mut Rng, length: usize, charset: &[u8]) -> String {
    (0..length)
        .map(|_| charset[rng.int(0, charset.len() as i64 - 1) as usize] as char)
        .collect()
}

fn uuid(rng: &mut Rng) -> String {
    let mut hex: Vec<u8> = (0..32).map(|_| HEX[rng.int(0, 15) as usize]).collect();
    hex[12] = b'4';
    hex[16] = b"89ab"[rng.int(0, 3) as usize];
    let s = String::from_utf8(hex).unwrap();
    format!(
        "{}-{}-{}-{}-{}",
        &s[0..8],
        &s[8..12],
        &s[12..16],
        &s[16..20],
        &s[20..]
    )
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Parse a timestamp bound: "now", a number (ms since epoch), or a date /
/// datetime string.
fn parse_time(v: &serde_yaml::Value) -> Result<i64> {
    match v {
        serde_yaml::Value::String(s) if s == "now" => Ok(now_ms()),
        serde_yaml::Value::Number(n) => Ok(n.as_i64().unwrap_or(n.as_f64().unwrap_or(0.0) as i64)),
        serde_yaml::Value::String(s) => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return Ok(dt.timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                return Ok(dt.and_utc().timestamp_millis());
            }
            if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                return Ok(d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis());
            }
            bail!("invalid timestamp bound: {s}")
        }
        other => bail!("invalid timestamp bound: {other:?}"),
    }
}

#[derive(Debug, Clone)]
pub enum PatternPart {
    Literal(String),
    Seq { width: usize },
    Rand { length: usize },
    Uuid,
}

#[derive(Debug, Clone)]
pub enum Generator {
    Pattern {
        key: String,
        parts: Vec<PatternPart>,
    },
    Sequence {
        start: i64,
        step: i64,
    },
    Int {
        min: i64,
        max: i64,
        zipfian: bool,
    },
    Real {
        min: f64,
        max: f64,
    },
    Str {
        length: usize,
        charset: &'static [u8],
    },
    TimestampNow,
    TimestampRange {
        lo: i64,
        hi: i64,
    },
    Bool {
        p_true: f64,
    },
    Enum {
        values: Vec<(String, f64)>,
        total: f64,
    },
    Const(Val),
    Ref {
        key: String,
        consume: bool,
    },
}

fn get_i64(map: &serde_yaml::Value, key: &str, default: i64) -> i64 {
    map.get(key).and_then(|v| v.as_i64()).unwrap_or(default)
}

fn get_f64(map: &serde_yaml::Value, key: &str, default: f64) -> f64 {
    map.get(key).and_then(|v| v.as_f64()).unwrap_or(default)
}

/// Expand a pattern like "i-{seq:010}" / "{rand:8}" / "{uuid}" into parts.
fn parse_pattern(pattern: &str) -> Vec<PatternPart> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut rest = pattern;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open..].find('}') else {
            break;
        };
        let inner = &rest[open + 1..open + close];
        let (kind, width) = match inner.split_once(':') {
            Some((kind, w)) => (
                kind,
                w.trim_start_matches('0')
                    .parse::<usize>()
                    .ok()
                    .or_else(|| w.parse().ok()),
            ),
            None => (inner, None),
        };
        let part = match kind {
            "seq" => Some(PatternPart::Seq {
                width: width.unwrap_or(0),
            }),
            "rand" => Some(PatternPart::Rand {
                length: width.unwrap_or(8),
            }),
            "uuid" => Some(PatternPart::Uuid),
            _ => None,
        };
        match part {
            Some(part) => {
                literal.push_str(&rest[..open]);
                if !literal.is_empty() {
                    parts.push(PatternPart::Literal(std::mem::take(&mut literal)));
                }
                parts.push(part);
            }
            None => {
                // Not a recognized placeholder; keep it verbatim.
                literal.push_str(&rest[..open + close + 1]);
            }
        }
        rest = &rest[open + close + 1..];
    }
    literal.push_str(rest);
    if !literal.is_empty() {
        parts.push(PatternPart::Literal(literal));
    }
    parts
}

impl Generator {
    /// Parse a generator spec: a single-key map such as `{ int: { min: 1, max: 50 } }`,
    /// plus modifiers like `consume`.
    pub fn parse(spec: &serde_yaml::Value) -> Result<Generator> {
        if let Some(pattern) = spec.get("pattern") {
            let pattern = pattern
                .as_str()
                .context("pattern generator must be a string")?;
            return Ok(Generator::Pattern {
                key: format!("pattern:{pattern}"),
                parts: parse_pattern(pattern),
            });
        }
        if let Some(sequence) = spec.get("sequence") {
            return Ok(Generator::Sequence {
                start: get_i64(sequence, "start", 1),
                step: get_i64(sequence, "step", 1),
            });
        }
        if let Some(int) = spec.get("int") {
            let dist = int
                .get("dist")
                .and_then(|v| v.as_str())
                .unwrap_or("uniform");
            return Ok(Generator::Int {
                min: get_i64(int, "min", 0),
                max: get_i64(int, "max", 1 << 31),
                zipfian: dist == "zipfian",
            });
        }
        if let Some(real) = spec.get("real") {
            return Ok(Generator::Real {
                min: get_f64(real, "min", 0.0),
                max: get_f64(real, "max", 1.0),
            });
        }
        if let Some(string) = spec.get("string") {
            let charset = match string.get("charset").and_then(|v| v.as_str()) {
                None | Some("ascii") => ASCII,
                Some("hex") => HEX,
                Some("alpha") => ALPHA,
                Some(other) => bail!("unknown charset \"{other}\""),
            };
            return Ok(Generator::Str {
                length: get_i64(string, "length", 16) as usize,
                charset,
            });
        }
        if let Some(timestamp) = spec.get("timestamp") {
            if timestamp.as_str() == Some("now") {
                return Ok(Generator::TimestampNow);
            }
            let lo = match timestamp.get("from") {
                Some(from) => parse_time(from)?,
                None => 0,
            };
            let hi = match timestamp.get("to") {
                Some(to) => parse_time(to)?,
                None => now_ms(),
            };
            return Ok(Generator::TimestampRange {
                lo: lo.min(hi),
                hi: lo.max(hi),
            });
        }
        if let Some(b) = spec.get("bool") {
            return Ok(Generator::Bool {
                p_true: get_f64(b, "p_true", 0.5),
            });
        }
        if let Some(e) = spec.get("enum") {
            let values = e
                .get("values")
                .and_then(|v| v.as_mapping())
                .context("enum generator needs a values map")?;
            let mut entries = Vec::new();
            for (k, w) in values {
                let key = k.as_str().map(str::to_string).unwrap_or_else(|| {
                    serde_yaml::to_string(k)
                        .unwrap_or_default()
                        .trim()
                        .to_string()
                });
                entries.push((key, w.as_f64().context("enum weight must be a number")?));
            }
            let total = entries.iter().map(|(_, w)| w).sum();
            return Ok(Generator::Enum {
                values: entries,
                total,
            });
        }
        if let Some(c) = spec.get("const") {
            return Ok(Generator::Const(yaml_to_val(c)?));
        }
        if let Some(r) = spec.get("ref") {
            return Ok(Generator::Ref {
                key: r
                    .as_str()
                    .context("ref target must be a string")?
                    .to_string(),
                consume: spec.get("consume").and_then(|v| v.as_bool()) == Some(true),
            });
        }
        bail!("unknown generator: {spec:?}")
    }
}

/// All per-run generation state: RNG, sequence counters, and entity pools.
/// Shared by the seed phase and every bench worker.
pub struct GenState {
    pub rng: Rng,
    pub seqs: SequenceCounters,
    pub pools: Pools,
}

impl GenState {
    pub fn new(seed: u64, pool_keys: impl IntoIterator<Item = String>) -> Self {
        Self {
            rng: Rng::new(seed),
            seqs: SequenceCounters::default(),
            pools: Pools::new(pool_keys),
        }
    }

    /// Evaluate a generator to a value. Returns None when the generator cannot
    /// produce one right now (a `ref` against an empty pool), which the caller
    /// treats as a skipped iteration. `seq_key` namespaces `sequence` counters.
    pub fn eval(&mut self, gen: &Generator, seq_key: &str) -> Option<Val> {
        Some(match gen {
            Generator::Pattern { key, parts } => {
                let mut out = String::new();
                for part in parts {
                    match part {
                        PatternPart::Literal(s) => out.push_str(s),
                        PatternPart::Seq { width } => {
                            let n = self.seqs.next(key, 1, 1);
                            out.push_str(&format!("{n:0width$}"));
                        }
                        PatternPart::Rand { length } => {
                            out.push_str(&random_string(&mut self.rng, *length, ASCII))
                        }
                        PatternPart::Uuid => out.push_str(&uuid(&mut self.rng)),
                    }
                }
                Val::Text(out)
            }
            Generator::Sequence { start, step } => Val::Int(self.seqs.next(
                &format!("sequence:{seq_key}"),
                *start,
                *step,
            )),
            Generator::Int { min, max, zipfian } => Val::Int(if *zipfian {
                self.rng.zipfian(*min, *max)
            } else {
                self.rng.int(*min, *max)
            }),
            Generator::Real { min, max } => Val::Real(min + self.rng.f64() * (max - min)),
            Generator::Str { length, charset } => {
                Val::Text(random_string(&mut self.rng, *length, charset))
            }
            Generator::TimestampNow => Val::Int(now_ms()),
            Generator::TimestampRange { lo, hi } => Val::Int(self.rng.int(*lo, *hi)),
            Generator::Bool { p_true } => Val::Int((self.rng.f64() < *p_true) as i64),
            Generator::Enum { values, total } => {
                let mut r = self.rng.f64() * total;
                let mut picked = values.last().map(|(v, _)| v.clone()).unwrap_or_default();
                for (value, weight) in values {
                    r -= weight;
                    if r <= 0.0 {
                        picked = value.clone();
                        break;
                    }
                }
                Val::Text(picked)
            }
            Generator::Const(v) => v.clone(),
            Generator::Ref { key, consume } => {
                return self.pools.sample(&mut self.rng, key, *consume)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen(yaml: &str) -> Generator {
        Generator::parse(&serde_yaml::from_str(yaml).unwrap()).unwrap()
    }

    #[test]
    fn pattern_with_padded_seq() {
        let mut state = GenState::new(42, []);
        let g = gen(r#"{ pattern: "i-{seq:010}" }"#);
        assert_eq!(state.eval(&g, "k"), Some(Val::Text("i-0000000001".into())));
        assert_eq!(state.eval(&g, "k"), Some(Val::Text("i-0000000002".into())));
    }

    #[test]
    fn uuid_pattern_shape() {
        let mut state = GenState::new(42, []);
        let g = gen(r#"{ pattern: "{uuid}" }"#);
        let Some(Val::Text(s)) = state.eval(&g, "k") else {
            panic!("expected text");
        };
        assert_eq!(s.len(), 36);
        assert_eq!(&s[14..15], "4");
    }

    #[test]
    fn int_ranges() {
        let mut state = GenState::new(7, []);
        let g = gen(r#"{ int: { min: 1, max: 10 } }"#);
        let z = gen(r#"{ int: { min: 1, max: 1000, dist: zipfian } }"#);
        for _ in 0..1000 {
            let Some(Val::Int(n)) = state.eval(&g, "k") else {
                panic!()
            };
            assert!((1..=10).contains(&n));
            let Some(Val::Int(n)) = state.eval(&z, "k") else {
                panic!()
            };
            assert!((1..=1000).contains(&n));
        }
    }

    #[test]
    fn ref_pool_consume() {
        let mut state = GenState::new(1, ["t.id".to_string()]);
        let g = gen(r#"{ ref: t.id, consume: true }"#);
        assert_eq!(state.eval(&g, "k"), None);
        state.pools.add("t.id", Val::Int(7));
        assert_eq!(state.eval(&g, "k"), Some(Val::Int(7)));
        assert_eq!(state.eval(&g, "k"), None);
    }

    #[test]
    fn sequences_are_keyed() {
        let mut state = GenState::new(1, []);
        let g = gen(r#"{ sequence: { start: 5, step: 2 } }"#);
        assert_eq!(state.eval(&g, "a"), Some(Val::Int(5)));
        assert_eq!(state.eval(&g, "a"), Some(Val::Int(7)));
        assert_eq!(state.eval(&g, "b"), Some(Val::Int(5)));
    }

    #[test]
    fn timestamp_range_parses_dates() {
        let g = gen(r#"{ timestamp: { from: 2024-01-01, to: now } }"#);
        let Generator::TimestampRange { lo, hi } = g else {
            panic!("expected range");
        };
        assert_eq!(lo, 1704067200000);
        assert!(hi >= lo);
    }
}
