use core::fmt;
use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Deref, DerefMut},
};

use indexmap::IndexMap;
use nu_ansi_term::Color;
use schemars::JsonSchema;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer,
};
use tracing::{debug, trace, warn};

pub type Palette = IndexMap<String, String>;

#[derive(Debug, Clone)]
pub struct LimboColor(pub Color);

impl TryFrom<&str> for LimboColor {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Parse RGB hex values
        trace!("Parsing color_string: {}", value);
        if value.starts_with('#') {
            trace!("Attempting to read hexadecimal color string: {}", value);
            if value.len() != 7 {
                return Err(format!("Could not parse hexadecimal string: {}", value));
            }
            let r: u8 = u8::from_str_radix(&value[1..3], 16).map_err(|e| e.to_string())?;
            let g: u8 = u8::from_str_radix(&value[3..5], 16).map_err(|e| e.to_string())?;
            let b: u8 = u8::from_str_radix(&value[5..7], 16).map_err(|e| e.to_string())?;
            trace!("Read RGB color string: {},{},{}", r, g, b);
            return Ok(LimboColor(Color::Rgb(r, g, b)));
        }

        // Parse a u8 (ansi color)
        if let Result::Ok(ansi_color_num) = value.parse::<u8>() {
            trace!("Read ANSI color string: {}", ansi_color_num);
            return Ok(LimboColor(Color::Fixed(ansi_color_num)));
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

    // Function copied from Starship rs project
    /** Parse a string that represents a color setting, returning None if this fails
     There are three valid color formats:
      - #RRGGBB      (a hash followed by an RGB hex)
      - u8           (a number from 0-255, representing an ANSI color)
      - colstring    (one of the 16 predefined color strings or a custom user-defined color)
    */
    fn parse_color_string(color_string: &str, palette: Option<&Palette>) -> Option<LimboColor> {
        // Check palette for a matching user-defined color
        if let Some(palette_color) = palette.as_ref().and_then(|x| x.get(color_string)) {
            trace!(
                "Read user-defined color string: {} defined as {}",
                color_string,
                palette_color
            );
            return Self::parse_color_string(palette_color, None);
        } else {
            match color_string.try_into() {
                Ok(color) => Some(color),
                Err(err) => {
                    debug!(err);
                    None
                }
            }
        }
    }
}

fn get_palette<'a>(
    palettes: &'a HashMap<String, Palette>,
    palette_name: Option<&str>,
) -> Option<&'a Palette> {
    if let Some(palette_name) = palette_name {
        let palette = palettes.get(palette_name);
        if palette.is_some() {
            trace!("Found color palette: {}", palette_name);
        } else {
            warn!("Could not find color palette: {}", palette_name);
        }
        palette
    } else {
        trace!("No color palette specified, using defaults");
        None
    }
}
