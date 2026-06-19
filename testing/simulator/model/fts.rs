use std::{
    collections::BTreeSet,
    fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, strum::IntoStaticStr,
)]
#[strum(serialize_all = "kebab-case")]
pub(crate) enum FtsFeatureTag {
    FtsIndex,
    TokenizerRaw,
    TokenizerSimple,
    TokenizerWhitespace,
    TokenizerNgram,
    WeightedFields,
    NonTextIndexedColumn,
    NullableIndexedColumn,
    MatchFunction,
    TupleMatch,
    FtsScoreProjection,
    FtsScoreOrderBy,
    FieldFilter,
    WeirdQueryArgument,
    NonDeterministicFunction,
    Limit,
    Offset,
    Distinct,
    Join,
    LeftJoin,
    Cte,
    Insert,
    Update,
    UpdateIndexedColumn,
    Delete,
    OptimizeIndex,
    Begin,
    Commit,
    Rollback,
    RenameTable,
    RenameColumn,
    StatementError,
    RebuildOracle,
    ReopenOracle,
    ScalarOracle,
    LimitPrefixOracle,
}

impl FtsFeatureTag {
    pub(crate) fn name(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FtsTableRename {
    pub(crate) old_name: String,
    pub(crate) new_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FtsSql {
    pub(crate) sql: String,
    pub(crate) tables: Vec<String>,
    pub(crate) ignore_error: bool,
    pub(crate) transaction: bool,
    pub(crate) read_only: bool,
    pub(crate) table_rename: Option<FtsTableRename>,
}

impl FtsSql {
    pub(crate) fn uses(&self) -> Vec<String> {
        self.tables.clone()
    }

    pub(crate) fn is_transaction(&self) -> bool {
        self.transaction
    }

    pub(crate) fn starts_transaction(&self) -> bool {
        self.transaction && self.sql.eq_ignore_ascii_case("BEGIN")
    }

    pub(crate) fn ends_transaction(&self) -> bool {
        self.transaction
            && (self.sql.eq_ignore_ascii_case("COMMIT")
                || self.sql.eq_ignore_ascii_case("ROLLBACK"))
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub(crate) fn table_rename(&self) -> Option<&FtsTableRename> {
        self.table_rename.as_ref()
    }
}

impl Display for FtsSql {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql)
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, strum::IntoStaticStr,
)]
#[strum(serialize_all = "kebab-case")]
pub(crate) enum FtsOracleKind {
    LimitPrefix,
    Rebuild,
    Scalar,
    Reopen,
}

impl FtsOracleKind {
    pub(crate) fn name(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FtsLimitPrefixOracle {
    pub(crate) full_sql: String,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FtsTableSnapshot {
    pub(crate) qualified_name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) create_sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FtsIndexSnapshot {
    pub(crate) create_sql: String,
    pub(crate) is_fts: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct FtsSchemaSnapshot {
    pub(crate) tables: Vec<FtsTableSnapshot>,
    pub(crate) indexes: Vec<FtsIndexSnapshot>,
}

impl FtsSchemaSnapshot {
    pub(crate) fn tables(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.qualified_name.clone())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FtsOracleCheck {
    pub(crate) seed: u64,
    pub(crate) step: usize,
    pub(crate) verification_sql: String,
    pub(crate) tags: BTreeSet<FtsFeatureTag>,
    pub(crate) schema: FtsSchemaSnapshot,
    pub(crate) limit_prefix: Option<FtsLimitPrefixOracle>,
    pub(crate) rebuild: bool,
    pub(crate) scalar: bool,
    pub(crate) reopen: bool,
}

impl FtsOracleCheck {
    pub(crate) fn uses(&self) -> Vec<String> {
        self.schema.tables()
    }

    pub(crate) fn oracle_kinds(&self) -> Vec<FtsOracleKind> {
        let mut kinds = Vec::new();
        if self.limit_prefix.is_some() {
            kinds.push(FtsOracleKind::LimitPrefix);
        }
        if self.rebuild {
            kinds.push(FtsOracleKind::Rebuild);
        }
        if self.scalar {
            kinds.push(FtsOracleKind::Scalar);
        }
        if self.reopen {
            kinds.push(FtsOracleKind::Reopen);
        }
        kinds
    }
}

impl Display for FtsOracleCheck {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kinds = self
            .oracle_kinds()
            .into_iter()
            .map(FtsOracleKind::name)
            .collect::<Vec<_>>()
            .join(",");
        let tags = fts_feature_tag_list(&self.tags);
        writeln!(
            f,
            "-- FTS_ORACLE seed={} step={} oracles={} tags={}",
            self.seed, self.step, kinds, tags
        )?;
        write!(f, "{}", self.verification_sql)
    }
}

pub(crate) fn fts_feature_tag_list(tags: &BTreeSet<FtsFeatureTag>) -> String {
    if tags.is_empty() {
        return "none".to_string();
    }
    tags.iter()
        .copied()
        .map(FtsFeatureTag::name)
        .collect::<Vec<_>>()
        .join(",")
}
