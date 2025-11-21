mod palette;
mod terminal;

use crate::{input::OutputMode, HOME_DIR};
use nu_ansi_term::Color;
use palette::LimboColor;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer};
use std::{fmt::Debug, fs::read_to_string, path::PathBuf, sync::LazyLock};
use terminal::{TerminalDetector, TerminalTheme};
use validator::Validate;

pub static CONFIG_DIR: LazyLock<PathBuf> = LazyLock::new(|| HOME_DIR.join(".config/limbo"));

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
                    "Invalid value for {}.\n Original config value: {:?}",
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

#[derive(Debug, Deserialize, Clone, Default, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    #[serde(deserialize_with = "ok_or_default")]
    pub table: TableConfig,
    pub highlight: HighlightConfig,
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
            tracing::trace!("Trying to read from {:?}", path);

            let result = read_to_string(path);

            if result.is_err() {
                tracing::debug!("Error reading file: {:?}", result);
            } else {
                tracing::trace!("File read successfully");
            };

            result.ok()
        } else {
            None
        }
    }

    pub fn for_output_mode(mode: OutputMode) -> Self {
        let table = if mode == OutputMode::Pretty {
            TableConfig::adaptive_colors()
        } else {
            TableConfig::no_colors()
        };
        Self {
            table,
            highlight: HighlightConfig::default(),
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
        // Always use adaptive colors based on terminal theme
        Self::adaptive_colors()
    }
}

impl TableConfig {
    // These methods are needed for serde default attributes
    fn default_header_color() -> LimboColor {
        // Use adaptive colors for serde defaults too
        Self::adaptive_colors().header_color
    }

    fn default_column_colors() -> Vec<LimboColor> {
        // Use adaptive colors for serde defaults too
        Self::adaptive_colors().column_colors
    }

    /// Get adaptive colors based on detected terminal theme
    pub fn adaptive_colors() -> Self {
        let theme = TerminalDetector::detect_theme();
        match theme {
            TerminalTheme::Light => Self::light_theme_colors(),
            TerminalTheme::Dark => Self::dark_theme_colors(),
            TerminalTheme::Unknown => Self::no_colors(), // No colors for unsupported platforms
        }
    }

    /// No colors configuration - for Windows or when detection fails
    fn no_colors() -> Self {
        Self {
            header_color: LimboColor(Color::Default),
            column_colors: vec![LimboColor(Color::Default)],
        }
    }

    /// Colors optimized for light terminal backgrounds
    fn light_theme_colors() -> Self {
        Self {
            header_color: LimboColor(Color::Black),
            column_colors: vec![
                LimboColor(Color::Fixed(22)), // Dark green
                LimboColor(Color::Fixed(17)), // Dark blue
                LimboColor(Color::Fixed(88)), // Dark red
                LimboColor(Color::Fixed(94)), // Orange
                LimboColor(Color::Fixed(55)), // Purple
            ],
        }
    }

    /// Colors optimized for dark terminal backgrounds
    fn dark_theme_colors() -> Self {
        Self {
            header_color: LimboColor(Color::LightGray),
            column_colors: vec![
                LimboColor(Color::LightGreen),
                LimboColor(Color::LightBlue),
                LimboColor(Color::LightCyan),
                LimboColor(Color::LightYellow),
                LimboColor(Color::LightMagenta),
            ],
        }
    }
}

#[derive(Debug, Deserialize, Clone, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct HighlightConfig {
    pub enable: bool,
    pub theme: String,
    pub prompt: LimboColor,
    pub hint: LimboColor,
    pub candidate: LimboColor,
}

impl Default for HighlightConfig {
    fn default() -> Self {
        Self {
            enable: true,
            theme: "base16-ocean.dark".to_string(),
            prompt: LimboColor(Color::Rgb(34u8, 197u8, 94u8)),
            hint: LimboColor(Color::Rgb(107u8, 114u8, 128u8)),
            candidate: LimboColor(Color::Green),
        }
    }
}
