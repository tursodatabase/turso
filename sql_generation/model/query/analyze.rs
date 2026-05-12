use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Analyze {
    pub target: AnalyzeTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AnalyzeTarget {
    All,
    Table {
        table_name: String,
    },
    Index {
        table_name: String,
        index_name: String,
    },
}

impl std::fmt::Display for Analyze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.target {
            AnalyzeTarget::All => write!(f, "ANALYZE"),
            AnalyzeTarget::Table { table_name } => write!(f, "ANALYZE {table_name}"),
            AnalyzeTarget::Index {
                table_name,
                index_name,
            } => {
                // In SQLite, ANALYZE index syntax uses: ANALYZE [db.]index_name.
                // The database prefix goes on the index name.
                let db_prefix = table_name
                    .split_once('.')
                    .map(|(db, _)| format!("{db}."))
                    .unwrap_or_default();
                write!(f, "ANALYZE {db_prefix}{index_name}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_formats_supported_analyze_targets() {
        assert_eq!(
            Analyze {
                target: AnalyzeTarget::All
            }
            .to_string(),
            "ANALYZE"
        );
        assert_eq!(
            Analyze {
                target: AnalyzeTarget::Table {
                    table_name: "t".to_string()
                }
            }
            .to_string(),
            "ANALYZE t"
        );
        assert_eq!(
            Analyze {
                target: AnalyzeTarget::Table {
                    table_name: "aux.t".to_string()
                }
            }
            .to_string(),
            "ANALYZE aux.t"
        );
        assert_eq!(
            Analyze {
                target: AnalyzeTarget::Index {
                    table_name: "t".to_string(),
                    index_name: "idx_t".to_string()
                }
            }
            .to_string(),
            "ANALYZE idx_t"
        );
        assert_eq!(
            Analyze {
                target: AnalyzeTarget::Index {
                    table_name: "aux.t".to_string(),
                    index_name: "idx_t".to_string()
                }
            }
            .to_string(),
            "ANALYZE aux.idx_t"
        );
    }
}
