use core::fmt;
use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

use nu_ansi_term::Color;
use schemars::JsonSchema;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize,
};
use tracing::trace;
use validator::Validate;

#[derive(Debug, Clone, Serialize)]
pub struct LimboColor(pub Color);

impl TryFrom<&str> for LimboColor {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Parse RGB hex values
        trace!("Parsing color_string: {}", value);

        let color = match value.chars().collect::<Vec<_>>()[..] {
            // #rrggbb hex color
            ['#', r1, r2, g1, g2, b1, b2] => {
                let r = u8::from_str_radix(&format!("{r1}{r2}"), 16).map_err(|e| e.to_string())?;
                let g = u8::from_str_radix(&format!("{g1}{g2}"), 16).map_err(|e| e.to_string())?;
                let b = u8::from_str_radix(&format!("{b1}{b2}"), 16).map_err(|e| e.to_string())?;
                Some(Color::Rgb(r, g, b))
            }
            // #rgb shorthand hex color
            ['#', r, g, b] => {
                let r = u8::from_str_radix(&format!("{r}{r}"), 16).map_err(|e| e.to_string())?;
                let g = u8::from_str_radix(&format!("{g}{g}"), 16).map_err(|e| e.to_string())?;
                let b = u8::from_str_radix(&format!("{b}{b}"), 16).map_err(|e| e.to_string())?;
                Some(Color::Rgb(r, g, b))
            }
            // 0-255 color code
            [c1, c2, c3] => {
                if let Ok(ansi_color_num) = str::parse::<u8>(&format!("{c1}{c2}{c3}")) {
                    Some(Color::Fixed(ansi_color_num))
                } else {
                    None
                }
            }
            [c1, c2] => {
                if let Ok(ansi_color_num) = str::parse::<u8>(&format!("{c1}{c2}")) {
                    Some(Color::Fixed(ansi_color_num))
                } else {
                    None
                }
            }
            [c1] => {
                if let Ok(ansi_color_num) = str::parse::<u8>(&format!("{c1}")) {
                    Some(Color::Fixed(ansi_color_num))
                } else {
                    None
                }
            }
            // unknown format
            _ => None,
        };

        if let Some(color) = color {
            return Ok(LimboColor(color));
        }

        // Check for any predefined color strings
        // There are no predefined enums for bright colors, so we use Color::Fixed
        let predefined_color = match value.to_lowercase().as_str() {
            "black" => Color::Black,
            "red" => Color::Red,
            "green" => Color::Green,
            "yellow" => Color::Yellow,
            "blue" => Color::Blue,
            "purple" => Color::Purple,
            "cyan" => Color::Cyan,
            "magenta" => Color::Magenta,
            "white" => Color::White,
            "bright-black" => Color::DarkGray, // "bright-black" is dark grey
            "bright-red" => Color::LightRed,
            "bright-green" => Color::LightGreen,
            "bright-yellow" => Color::LightYellow,
            "bright-blue" => Color::LightBlue,
            "bright-cyan" => Color::LightCyan,
            "birght-magenta" => Color::LightMagenta,
            "bright-white" => Color::LightGray,
            "dark-red" => Color::Fixed(1),
            "dark-green" => Color::Fixed(2),
            "dark-yellow" => Color::Fixed(3),
            "dark-blue" => Color::Fixed(4),
            "dark-magenta" => Color::Fixed(5),
            "dark-cyan" => Color::Fixed(6),
            "grey" => Color::Fixed(7),
            "dark-grey" => Color::Fixed(8),
            _ => return Err(format!("Could not parse color in string: {}", value)),
        };

        trace!("Read predefined color: {}", value);
        Ok(LimboColor(predefined_color))
    }
}

impl Display for LimboColor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self.0 {
            Color::Black => "black".to_string(),
            Color::Red => "red".to_string(),
            Color::Green => "green".to_string(),
            Color::Yellow => "yellow".to_string(),
            Color::Blue => "blue".to_string(),
            Color::Purple => "purple".to_string(),
            Color::Cyan => "cyan".to_string(),
            Color::Magenta => "magenta".to_string(),
            Color::White => "white".to_string(),
            Color::DarkGray => "bright-black".to_string(), // "bright-black" is dark grey
            Color::LightRed => "bright-red".to_string(),
            Color::LightGreen => "bright-green".to_string(),
            Color::LightYellow => "bright-yellow".to_string(),
            Color::LightBlue => "bright-blue".to_string(),
            Color::LightCyan => "bright-cyan".to_string(),
            Color::LightMagenta | Color::LightPurple => "bright-magenta".to_string(),
            Color::LightGray => "bright-white".to_string(),
            Color::Fixed(1) => "dark-red".to_string(),
            Color::Fixed(2) => "dark-green".to_string(),
            Color::Fixed(3) => "dark-yellow".to_string(),
            Color::Fixed(4) => "dark-blue".to_string(),
            Color::Fixed(5) => "dark-magenta".to_string(),
            Color::Fixed(6) => "dark-cyan".to_string(),
            Color::Fixed(7) => "grey".to_string(),
            Color::Fixed(8) => "dark-grey".to_string(),
            Color::Rgb(r, g, b) => format!("#{r:x}{g:x}{b:X}"),
            Color::Fixed(ansi_color_num) => format!("{ansi_color_num}"),
            Color::Default => unreachable!(),
        };
        write!(f, "{val}")
    }
}

impl From<comfy_table::Color> for LimboColor {
    fn from(value: comfy_table::Color) -> Self {
        let color = match value {
            comfy_table::Color::Rgb { r, g, b } => Color::Rgb(r, g, b),
            comfy_table::Color::AnsiValue(ansi_color_num) => Color::Fixed(ansi_color_num),
            comfy_table::Color::Black => Color::Black,
            comfy_table::Color::Red => Color::Red,
            comfy_table::Color::Green => Color::Green,
            comfy_table::Color::Yellow => Color::Yellow,
            comfy_table::Color::Blue => Color::Blue,
            comfy_table::Color::Cyan => Color::Cyan,
            comfy_table::Color::Magenta => Color::Magenta,
            comfy_table::Color::White => Color::White,
            comfy_table::Color::DarkRed => Color::Fixed(1),
            comfy_table::Color::DarkGreen => Color::Fixed(2),
            comfy_table::Color::DarkYellow => Color::Fixed(3),
            comfy_table::Color::DarkBlue => Color::Fixed(4),
            comfy_table::Color::DarkMagenta => Color::Fixed(5),
            comfy_table::Color::DarkCyan => Color::Fixed(6),
            comfy_table::Color::Grey => Color::Fixed(7),
            comfy_table::Color::DarkGrey => Color::Fixed(8),
            comfy_table::Color::Reset => unreachable!(), // Should never have Reset Color here
        };
        LimboColor(color)
    }
}

impl<'de> Deserialize<'de> for LimboColor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LimboColorVisitor;

        impl<'de> Visitor<'de> for LimboColorVisitor {
            type Value = LimboColor;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct LimboColor")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                LimboColor::try_from(v).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(LimboColorVisitor)
    }
}

impl JsonSchema for LimboColor {
    fn schema_name() -> String {
        "LimboColor".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        // Include the module, in case a type with the same name is in another module/crate
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::LimboColor"))
    }

    fn json_schema(generator: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        generator.subschema_for::<LimboColor>()
    }
}

impl Deref for LimboColor {
    type Target = Color;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LimboColor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Validate for LimboColor {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}

impl LimboColor {
    pub fn into_comfy_table_color(&self) -> comfy_table::Color {
        match self.0 {
            Color::Black => comfy_table::Color::Black,
            Color::Red => comfy_table::Color::Red,
            Color::Green => comfy_table::Color::Green,
            Color::Yellow => comfy_table::Color::Yellow,
            Color::Blue => comfy_table::Color::Blue,
            Color::Magenta | Color::Purple => comfy_table::Color::Magenta,
            Color::Cyan => comfy_table::Color::Cyan,
            Color::White | Color::Default => comfy_table::Color::White,
            Color::Fixed(1) => comfy_table::Color::DarkRed,
            Color::Fixed(2) => comfy_table::Color::DarkGreen,
            Color::Fixed(3) => comfy_table::Color::DarkYellow,
            Color::Fixed(4) => comfy_table::Color::DarkBlue,
            Color::Fixed(5) => comfy_table::Color::DarkMagenta,
            Color::Fixed(6) => comfy_table::Color::DarkCyan,
            Color::Fixed(7) => comfy_table::Color::Grey,
            Color::Fixed(8) => comfy_table::Color::DarkGrey,
            Color::DarkGray => comfy_table::Color::AnsiValue(241),
            Color::LightRed => comfy_table::Color::AnsiValue(09),
            Color::LightGreen => comfy_table::Color::AnsiValue(10),
            Color::LightYellow => comfy_table::Color::AnsiValue(11),
            Color::LightBlue => comfy_table::Color::AnsiValue(12),
            Color::LightMagenta | Color::LightPurple => comfy_table::Color::AnsiValue(13),
            Color::LightCyan => comfy_table::Color::AnsiValue(14),
            Color::LightGray => comfy_table::Color::AnsiValue(15),
            Color::Rgb(r, g, b) => comfy_table::Color::Rgb { r, g, b },
            Color::Fixed(ansi_color_num) => comfy_table::Color::AnsiValue(ansi_color_num),
        }
    }
}
