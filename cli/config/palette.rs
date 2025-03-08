use std::collections::HashMap;

use indexmap::IndexMap;
use nu_ansi_term::Color;
use tracing::{debug, trace, warn};

pub type Palette = IndexMap<String, String>;

// Function copied from Starship rs project
/** Parse a string that represents a color setting, returning None if this fails
 There are three valid color formats:
  - #RRGGBB      (a hash followed by an RGB hex)
  - u8           (a number from 0-255, representing an ANSI color)
  - colstring    (one of the 16 predefined color strings or a custom user-defined color)
*/
fn parse_color_string(color_string: &str, palette: Option<&Palette>) -> Option<Color> {
    // Parse RGB hex values
    trace!("Parsing color_string: {}", color_string);
    if color_string.starts_with('#') {
        trace!(
            "Attempting to read hexadecimal color string: {}",
            color_string
        );
        if color_string.len() != 7 {
            debug!("Could not parse hexadecimal string: {}", color_string);
            return None;
        }
        let r: u8 = u8::from_str_radix(&color_string[1..3], 16).ok()?;
        let g: u8 = u8::from_str_radix(&color_string[3..5], 16).ok()?;
        let b: u8 = u8::from_str_radix(&color_string[5..7], 16).ok()?;
        trace!("Read RGB color string: {},{},{}", r, g, b);
        return Some(Color::Rgb(r, g, b));
    }

    // Parse a u8 (ansi color)
    if let Result::Ok(ansi_color_num) = color_string.parse::<u8>() {
        trace!("Read ANSI color string: {}", ansi_color_num);
        return Some(Color::Fixed(ansi_color_num));
    }

    // Check palette for a matching user-defined color
    if let Some(palette_color) = palette.as_ref().and_then(|x| x.get(color_string)) {
        trace!(
            "Read user-defined color string: {} defined as {}",
            color_string,
            palette_color
        );
        return parse_color_string(palette_color, None);
    }

    // Check for any predefined color strings
    // There are no predefined enums for bright colors, so we use Color::Fixed
    let predefined_color = match color_string.to_lowercase().as_str() {
        "black" => Some(Color::Black),
        "red" => Some(Color::Red),
        "green" => Some(Color::Green),
        "yellow" => Some(Color::Yellow),
        "blue" => Some(Color::Blue),
        "purple" => Some(Color::Purple),
        "cyan" => Some(Color::Cyan),
        "white" => Some(Color::White),
        "bright-black" => Some(Color::DarkGray), // "bright-black" is dark grey
        "bright-red" => Some(Color::LightRed),
        "bright-green" => Some(Color::LightGreen),
        "bright-yellow" => Some(Color::LightYellow),
        "bright-blue" => Some(Color::LightBlue),
        "bright-purple" => Some(Color::LightPurple),
        "bright-cyan" => Some(Color::LightCyan),
        "bright-white" => Some(Color::LightGray),
        _ => None,
    };

    if predefined_color.is_some() {
        trace!("Read predefined color: {}", color_string);
    } else {
        debug!("Could not parse color in string: {}", color_string);
    }
    predefined_color
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
