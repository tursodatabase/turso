use include_dir::{include_dir, Dir};
use rand::seq::SliceRandom;
use std::io::{stdout, IsTerminal, Write};

use termimad::{
    crossterm::{
        event::{read, Event, KeyCode},
        queue,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    },
    Area, MadSkin, MadView,
};

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

// not ideal but enough for our usecase , probably overkill maybe.
fn levenshtein(a: &str, b: &str) -> usize {
    let a_chars: Vec<_> = a.chars().collect();
    let b_chars: Vec<_> = b.chars().collect();
    let (a_len, b_len) = (a_chars.len(), b_chars.len());
    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }
    let mut prev_row: Vec<usize> = (0..=b_len).collect();
    let mut current_row = vec![0; b_len + 1];
    for i in 1..=a_len {
        current_row[0] = i;
        for j in 1..=b_len {
            let substitution_cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            current_row[j] = (prev_row[j] + 1)
                .min(current_row[j - 1] + 1)
                .min(prev_row[j - 1] + substitution_cost);
        }
        prev_row.clone_from_slice(&current_row);
    }
    prev_row[b_len]
}

fn find_closest_manual_page<'a>(
    page_name: &str,
    available_pages: impl Iterator<Item = &'a str>,
) -> Option<&'a str> {
    const RELATIVE_SIMILARITY_THRESHOLD: f64 = 0.4;

    available_pages
        .filter_map(|candidate| {
            let distance = levenshtein(page_name, candidate);
            let longer_len = std::cmp::max(page_name.chars().count(), candidate.chars().count());
            if longer_len == 0 {
                return None;
            }
            let relative_distance = distance as f64 / longer_len as f64;

            if relative_distance < RELATIVE_SIMILARITY_THRESHOLD {
                Some((candidate, distance))
            } else {
                None
            }
        })
        .min_by_key(|&(_, score)| score)
        .map(|(name, _)| name)
}

pub fn display_manual(page: Option<&str>, writer: &mut dyn Write) -> anyhow::Result<()> {
    let page_name = page.unwrap_or("index");
    let file_name = format!("{page_name}.md");

    if let Some(file) = MANUAL_DIR.get_file(&file_name) {
        let content = file
            .contents_utf8()
            .ok_or_else(|| anyhow::anyhow!("Failed to read manual page: {}", page_name))?;
        let content = strip_frontmatter(content);
        if IsTerminal::is_terminal(&std::io::stdout()) {
            render_in_terminal(content)?;
        } else {
            writeln!(writer, "{content}")?;
        }
        Ok(())
    } else if page.is_none() {
        // If no page specified, list available pages
        return list_available_manuals(writer);
    } else {
        let available_pages = MANUAL_DIR
            .files()
            .filter_map(|file| file.path().file_stem().and_then(|stem| stem.to_str()));
        let mut error_message = format!("Manual page not found: {page_name}");
        if let Some(suggestion) = find_closest_manual_page(page_name, available_pages) {
            error_message.push_str(&format!("\n\nDid you mean '.manual {suggestion}'?"));
        }
        Err(anyhow::anyhow!(error_message))
    }
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

    let mut w = stdout();
    queue!(w, EnterAlternateScreen)?;
    enable_raw_mode()?;

    let area = Area::full_screen();
    let mut view = MadView::from(content.to_string(), area, skin);

    loop {
        view.write_on(&mut w)?;
        w.flush()?;

        match read()? {
            Event::Key(key) => match key.code {
                KeyCode::Up | KeyCode::Char('k') => view.try_scroll_lines(-1),
                KeyCode::Down | KeyCode::Char('j') => view.try_scroll_lines(1),
                KeyCode::PageUp => view.try_scroll_pages(-1),
                KeyCode::PageDown => view.try_scroll_pages(1),
                KeyCode::Char('g') => view.scroll = 0,
                KeyCode::Char('G') => view.try_scroll_lines(i32::MAX),

                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => break,

                _ => {}
            },
            Event::Resize(width, height) => {
                let new_area = Area::new(0, 0, width, height);
                view.resize(&new_area);
            }
            _ => {}
        }
    }

    disable_raw_mode()?;
    queue!(w, LeaveAlternateScreen)?;
    w.flush()?;

    Ok(())
}

fn list_available_manuals(writer: &mut dyn Write) -> anyhow::Result<()> {
    writeln!(writer, "Available manual pages:")?;
    writeln!(writer)?;
    let mut pages: Vec<String> = MANUAL_DIR
        .files()
        .filter_map(|file| file.path().file_stem()?.to_str().map(String::from))
        .collect();
    pages.sort();

    for page in &pages {
        writeln!(writer, "  .manual {page}  # or .man {page}")?;
    }

    if pages.is_empty() {
        writeln!(writer, "  (No manual pages found)")?;
    }
    writeln!(writer)?;
    writeln!(writer, "Usage: .manual <page>  or  .man <page>")?;

    Ok(())
}
