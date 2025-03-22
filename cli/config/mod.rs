mod palette;

use crate::utils::read_file;
use crate::HOME_DIR;
use nu_ansi_term::Color;
use once_cell::sync::Lazy;
use palette::LimboColor;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer};
use std::fmt::Debug;
use std::path::PathBuf;
use validator::Validate;

pub static CONFIG_DIR: Lazy<PathBuf> = Lazy::new(|| HOME_DIR.join(".config/limbo"));

fn ok_or_default<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + Default + Validate + Debug,
    D: Deserializer<'de>,
{
    let v: toml::Value = Deserialize::deserialize(deserializer)?;
    let x = T::deserialize(v)
        .map(|v| {
            let validate = v.validate();
            if validate.is_err() {
                tracing::error!(
                    "Invalid value for {}.\nOriginal config value: {:?}",
                    validate.unwrap_err(),
                    v
                );
                T::default()
            } else {
                v
            }
        })
        .unwrap_or_default();
    Ok(x)
}

#[derive(Debug, Deserialize, Clone, Default, JsonSchema, Validate)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    #[serde(deserialize_with = "ok_or_default")]
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
        toml::from_str(config)
            .inspect_err(|err| tracing::error!("{}", err))
            .unwrap_or_default()
    }

    fn read_config_str(path: PathBuf) -> Option<String> {
        if path.exists() {
            read_file(path).ok()
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, Clone, JsonSchema, Validate)]
#[serde(default, deny_unknown_fields)]
pub struct TableConfig {
    #[serde(default = "TableConfig::default_header_color")]
    pub header_color: LimboColor,
    #[serde(default = "TableConfig::default_column_colors")]
    #[validate(length(min = 1))]
    pub column_colors: Vec<LimboColor>,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            header_color: TableConfig::default_header_color(),
            column_colors: TableConfig::default_column_colors(),
        }
    }
}

impl TableConfig {
    // Default colors for Pekka's tastes
    fn default_header_color() -> LimboColor {
        LimboColor(Color::White)
    }

    fn default_column_colors() -> Vec<LimboColor> {
        vec![
            LimboColor(Color::Green),
            LimboColor(Color::Black),
            // Comfy Table Color::Grey
            LimboColor(Color::Fixed(7)),
        ]
    }
}

#[derive(Debug, Deserialize, Clone, JsonSchema)]
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
