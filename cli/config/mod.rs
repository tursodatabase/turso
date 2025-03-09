mod palette;

use crate::utils::read_file;
use nu_ansi_term::Color;
use palette::LimboColor;
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, Clone, Default, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub table: TableConfig,
    pub syntax_highlight: SyntaxHighlightConfig,
}

impl Config {
    pub fn from_config_file(path: PathBuf) -> Self {
        if let Some(config) = Self::read_config_str(path) {
            Self::from_config_str(&config)
        } else {
            Self::default()
        }
    }

    pub fn from_config_str(config: &str) -> Self {
        toml::from_str(config).unwrap_or_default()
    }

    fn read_config_str(path: PathBuf) -> Option<String> {
        if path.exists() {
            read_file(path).ok()
        } else {
            None
        }
    }
}

#[derive(Deserialize, Clone, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TableConfig {
    pub header_color: LimboColor,
    pub column_colors: Vec<LimboColor>,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            header_color: LimboColor(Color::Green),
            column_colors: vec![
                LimboColor(Color::Fixed(1)),
                LimboColor(Color::Fixed(2)),
                LimboColor(Color::Fixed(3)),
            ],
        }
    }
}

#[derive(Deserialize, Clone, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SyntaxHighlightConfig {
    pub theme_name: String,
}

impl Default for SyntaxHighlightConfig {
    fn default() -> Self {
        Self {
            theme_name: "base16-ocean.dark".to_string(),
        }
    }
}
