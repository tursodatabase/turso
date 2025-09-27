use include_dir::{include_dir, Dir};
use rand::seq::SliceRandom;
use std::io::{IsTerminal, Write};
use termimad::MadSkin;

static MANUAL_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/manuals");

/// Get a random feature to highlight from available manuals
pub fn get_random_feature_hint() -> Option<String> {
    let features: Vec<(&str, String)> = MANUAL_DIR
        .files()
        .filter_map(|file| {
            let path = file.path();
            let name = path.file_stem()?.to_str()?;

            if name == "index" {
                return None;
            }

            let content = file.contents_utf8()?;
            let display_name = extract_display_name(content).unwrap_or_else(|| name.to_string());
            Some((name, display_name))
        })
        .collect();

    if features.is_empty() {
        return None;
    }

    features
        .choose(&mut rand::thread_rng())
        .map(|(feature, display_name)| {
            format!("Did you know that Turso supports {display_name}? Type .manual {feature} to learn more.")
        })
}

fn extract_display_name(content: &str) -> Option<String> {
    if !content.starts_with("---") {
        return None;
    }

    let lines: Vec<&str> = content.lines().collect();
    let end_idx = lines[1..].iter().position(|&line| line == "---")? + 1;

    for line in &lines[1..end_idx] {
        if let Some(display_name) = line.strip_prefix("display_name: ") {
            return Some(display_name.trim_matches('"').to_string());
        }
    }

    None
}

fn strip_frontmatter(content: &str) -> &str {
    if !content.starts_with("---") {
        return content;
    }

    if let Some(end_pos) = content[3..].find("\n---\n") {
        &content[end_pos + 7..]
    } else {
        content
    }
}

pub fn display_manual(page: Option<&str>, writer: &mut dyn Write) -> anyhow::Result<()> {
    let page_name = page.unwrap_or("index");
    let file_name = format!("{page_name}.md");

    // Try to find the manual page
    let content = if let Some(file) = MANUAL_DIR.get_file(&file_name) {
        file.contents_utf8()
            .ok_or_else(|| anyhow::anyhow!("Failed to read manual page: {}", page_name))?
    } else if page.is_none() {
        // If no page specified, list available pages
        return list_available_manuals(writer);
    } else {
        return Err(anyhow::anyhow!("Manual page not found: {}", page_name));
    };

    // Strip frontmatter before displaying
    let content = strip_frontmatter(content);

    // Check if we're in a terminal or piped output
    if IsTerminal::is_terminal(&std::io::stdout()) {
        // Use termimad for nice terminal rendering
        render_in_terminal(content)?;
    } else {
        // Plain output for pipes/redirects
        writeln!(writer, "{content}")?;
    }

    Ok(())
}

fn render_in_terminal(content: &str) -> anyhow::Result<()> {
    // Create a skin with nice styling
    let mut skin = MadSkin::default();

    // Customize the skin for better appearance
    skin.set_headers_fg(termimad::crossterm::style::Color::Cyan);
    skin.bold.set_fg(termimad::crossterm::style::Color::Yellow);
    skin.italic
        .set_fg(termimad::crossterm::style::Color::Magenta);
    skin.inline_code
        .set_fg(termimad::crossterm::style::Color::Green);
    skin.code_block
        .set_fg(termimad::crossterm::style::Color::Green);

    // Just print the formatted content
    skin.print_text(content);

    Ok(())
}

fn list_available_manuals(writer: &mut dyn Write) -> anyhow::Result<()> {
    writeln!(writer, "Available manual pages:")?;
    writeln!(writer)?;

    let mut pages: Vec<String> = Vec::new();

    for file in MANUAL_DIR.files() {
        if let Some(name) = file.path().file_stem() {
            if let Some(name_str) = name.to_str() {
                pages.push(name_str.to_string());
            }
        }
    }

    pages.sort();

    for page in pages {
        writeln!(writer, "  .manual {page}  # or .man {page}")?;
    }

    if MANUAL_DIR.files().count() == 0 {
        writeln!(writer, "  (No manual pages found)")?;
    }

    writeln!(writer)?;
    writeln!(writer, "Usage: .manual <page>  or  .man <page>")?;

    Ok(())
}
