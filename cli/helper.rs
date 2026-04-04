use clap::Parser;
use crossterm::style::Color as CrosstermColor;
use nu_ansi_term::{Color, Style};
use reedline::{
    Completer, Highlighter, Prompt, PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus,
    Span, StyledText, Suggestion, ValidationResult, Validator,
};
use shlex::Shlex;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{ffi::OsString, path::PathBuf, str::FromStr as _};
use syntect::dumps::from_uncompressed_data;
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::{Scope, SyntaxSet};
use syntect::util::LinesWithEndings;
use turso_core::Connection;

use crate::config::{HighlightConfig, CONFIG_DIR};

macro_rules! try_result {
    ($expr:expr, $err:expr) => {
        match $expr {
            Ok(val) => val,
            Err(_) => return $err,
        }
    };
}

// --- Completer ---

pub struct TursoCompleter<C: Parser + Send + Sync + 'static> {
    conn: Arc<Connection>,
    cmd: clap::Command,
    _cmd_phantom: PhantomData<C>,
}

impl<C: Parser + Send + Sync + 'static> TursoCompleter<C> {
    pub fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            cmd: C::command(),
            _cmd_phantom: PhantomData,
        }
    }

    fn dot_completion(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let inner_line = &line[1..];
        let inner_pos = pos.saturating_sub(1);

        let args = Shlex::new(inner_line);
        let mut args = std::iter::once("".to_owned())
            .chain(args)
            .map(OsString::from)
            .collect::<Vec<_>>();
        if inner_line.ends_with(' ') {
            args.push(OsString::new());
        }
        let arg_index = args.len() - 1;

        let prefix_pos = find_word_start(inner_line, inner_pos);

        match clap_complete::engine::complete(
            &mut self.cmd,
            args,
            arg_index,
            PathBuf::from_str(".").ok().as_deref(),
        ) {
            Ok(candidates) => candidates
                .iter()
                .map(|candidate| {
                    let value = candidate.get_value().to_string_lossy().into_owned();
                    Suggestion {
                        value,
                        description: None,
                        style: None,
                        extra: None,
                        span: Span::new(prefix_pos + 1, pos),
                        append_whitespace: false,
                        display_override: None,
                        match_indices: None,
                    }
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn sql_completion(&self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix_pos = find_word_start(line, pos);
        let prefix = &line[prefix_pos..pos];

        let query = try_result!(
            self.conn.query(format!(
                "SELECT candidate FROM completion('{prefix}', '{line}') ORDER BY 1;"
            )),
            Vec::new()
        );

        let mut candidates = Vec::new();
        if let Some(mut rows) = query {
            let _ = rows.run_with_row_callback(|row| {
                let completion: &str = row.get::<&str>(0)?;
                candidates.push(Suggestion {
                    value: completion.to_string(),
                    description: None,
                    style: None,
                    extra: None,
                    span: Span::new(prefix_pos, pos),
                    append_whitespace: false,
                    display_override: None,
                    match_indices: None,
                });
                Ok(())
            });
        }

        candidates
    }
}

fn find_word_start(line: &str, pos: usize) -> usize {
    let bytes = line.as_bytes();
    let mut i = pos;
    while i > 0 {
        let c = bytes[i - 1] as char;
        if is_break_char(c) {
            break;
        }
        i -= 1;
    }
    i
}

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        fn is_break_char(c: char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\\' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' |
            '{' | '(' | '\0')
        }
    } else if #[cfg(windows)] {
        fn is_break_char(c: char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' | '{' |
            '(' | '\0')
        }
    } else if #[cfg(target_arch = "wasm32")] {
        fn is_break_char(_c: char) -> bool { false }
    }
}

impl<C: Parser + Send + Sync + 'static> Completer for TursoCompleter<C> {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        if line.starts_with('.') {
            self.dot_completion(line, pos)
        } else {
            self.sql_completion(line, pos)
        }
    }
}

// --- Nucleo History Completer ---

pub struct NucleoHistoryCompleter {
    nucleo: nucleo::Nucleo<String>,
    last_line: String,
    matcher: nucleo::Matcher,
    indices_buf: Vec<u32>,
    utf32_buf: Vec<char>,
}

impl NucleoHistoryCompleter {
    pub fn new(history_path: PathBuf) -> Self {
        let nucleo = nucleo::Nucleo::new(
            nucleo::Config::DEFAULT,
            Arc::new(|| {}),
            Some(1), // single worker thread is enough for history
            1,       // single column
        );

        let injector = nucleo.injector();

        // Load and inject history entries (deduped, most recent first)
        if let Ok(content) = std::fs::read_to_string(&history_path) {
            let mut seen = std::collections::HashSet::new();
            for line in content.lines().rev() {
                if !line.is_empty() && seen.insert(line.to_string()) {
                    injector.push(line.to_string(), |s, cols| {
                        cols[0] = s.as_str().into();
                    });
                }
            }
        }

        Self {
            nucleo,
            last_line: String::new(),
            matcher: nucleo::Matcher::new(nucleo::Config::DEFAULT),
            indices_buf: Vec::new(),
            utf32_buf: Vec::new(),
        }
    }
}

impl Completer for NucleoHistoryCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        use nucleo::pattern::{CaseMatching, Normalization};

        // Update pattern only when input changed
        let append = line.starts_with(self.last_line.as_str()) && !self.last_line.is_empty();
        self.nucleo
            .pattern
            .reparse(0, line, CaseMatching::Smart, Normalization::Smart, append);
        self.last_line = line.to_string();

        // Let the matcher process
        self.nucleo.tick(10);

        let snapshot = self.nucleo.snapshot();

        if line.is_empty() {
            // No query: return recent history as-is
            let count = snapshot.matched_item_count().min(50);
            return snapshot
                .matched_items(..count)
                .map(|item| Suggestion {
                    value: item.data.clone(),
                    description: None,
                    style: None,
                    extra: None,
                    span: Span::new(pos - line.len(), pos),
                    append_whitespace: false,
                    display_override: None,
                    match_indices: None,
                })
                .collect();
        }

        // Compute match indices for highlighting, reusing buffers
        let pattern = snapshot.pattern();
        let count = snapshot.matched_item_count().min(50);
        let items: Vec<_> = snapshot.matched_items(..count).collect();

        items
            .into_iter()
            .map(|item| {
                self.indices_buf.clear();
                self.utf32_buf.clear();
                let haystack = nucleo::Utf32Str::new(item.data, &mut self.utf32_buf);
                pattern.column_pattern(0).indices(
                    haystack,
                    &mut self.matcher,
                    &mut self.indices_buf,
                );
                self.indices_buf.sort_unstable();
                self.indices_buf.dedup();

                Suggestion {
                    value: item.data.clone(),
                    description: None,
                    style: None,
                    extra: None,
                    span: Span::new(pos - line.len(), pos),
                    append_whitespace: false,
                    display_override: None,
                    match_indices: Some(self.indices_buf.iter().map(|&i| i as usize).collect()),
                }
            })
            .collect()
    }
}

// --- Highlighter ---

pub struct TursoHighlighter {
    syntax_set: SyntaxSet,
    theme_set: ThemeSet,
    syntax_config: HighlightConfig,
}

impl TursoHighlighter {
    pub fn new(syntax_config: Option<HighlightConfig>) -> Self {
        let ps = from_uncompressed_data(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/SQL_syntax_set_dump.packdump"
        )))
        .unwrap();
        let mut ts = ThemeSet::load_defaults();
        let theme_dir = CONFIG_DIR.join("themes");
        if theme_dir.exists() {
            if let Err(err) = ts.add_from_folder(theme_dir) {
                tracing::error!("{err}");
            }
        }
        TursoHighlighter {
            syntax_set: ps,
            theme_set: ts,
            syntax_config: syntax_config.unwrap_or_default(),
        }
    }
}

impl Highlighter for TursoHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled = StyledText::new();

        if self.syntax_config.enable {
            let syntax = self
                .syntax_set
                .find_syntax_by_scope(Scope::new("source.sql").unwrap())
                .unwrap();
            let theme = self
                .theme_set
                .themes
                .get(&self.syntax_config.theme)
                .unwrap_or(&self.theme_set.themes["base16-ocean.dark"]);
            let mut h = HighlightLines::new(syntax, theme);

            for new_line in LinesWithEndings::from(line) {
                let ranges = h.highlight_line(new_line, &self.syntax_set).unwrap();
                for (style, text) in ranges {
                    let fg = style.foreground;
                    let nu_style = Style::new().fg(Color::Rgb(fg.r, fg.g, fg.b));
                    styled.push((nu_style, text.to_string()));
                }
            }
        } else {
            let style = Style::new().fg(Color::White);
            styled.push((style, line.to_string()));
        }

        styled
    }
}

// --- Validator ---

pub struct TursoValidator;

impl Validator for TursoValidator {
    fn validate(&self, _line: &str) -> ValidationResult {
        ValidationResult::Complete
    }
}

// --- Prompt ---

pub struct TursoPrompt {
    prompt: String,
    prompt_color: nu_ansi_term::Color,
}

impl TursoPrompt {
    pub fn new(prompt: String, prompt_color: nu_ansi_term::Color) -> Self {
        Self {
            prompt,
            prompt_color,
        }
    }

    pub fn set_prompt(&mut self, prompt: String) {
        self.prompt = prompt;
    }

    fn nu_color_to_crossterm(c: nu_ansi_term::Color) -> CrosstermColor {
        match c {
            nu_ansi_term::Color::Rgb(r, g, b) => CrosstermColor::Rgb { r, g, b },
            nu_ansi_term::Color::Black => CrosstermColor::Black,
            nu_ansi_term::Color::Red => CrosstermColor::DarkRed,
            nu_ansi_term::Color::Green => CrosstermColor::DarkGreen,
            nu_ansi_term::Color::Yellow => CrosstermColor::DarkYellow,
            nu_ansi_term::Color::Blue => CrosstermColor::DarkBlue,
            nu_ansi_term::Color::Purple => CrosstermColor::DarkMagenta,
            nu_ansi_term::Color::Cyan => CrosstermColor::DarkCyan,
            nu_ansi_term::Color::White => CrosstermColor::White,
            nu_ansi_term::Color::DarkGray => CrosstermColor::DarkGrey,
            nu_ansi_term::Color::LightRed => CrosstermColor::Red,
            nu_ansi_term::Color::LightGreen => CrosstermColor::Green,
            nu_ansi_term::Color::LightYellow => CrosstermColor::Yellow,
            nu_ansi_term::Color::LightBlue => CrosstermColor::Blue,
            nu_ansi_term::Color::LightPurple => CrosstermColor::Magenta,
            nu_ansi_term::Color::LightCyan => CrosstermColor::Cyan,
            nu_ansi_term::Color::LightGray => CrosstermColor::Grey,
            nu_ansi_term::Color::Fixed(n) => CrosstermColor::AnsiValue(n),
            _ => CrosstermColor::Reset,
        }
    }
}

impl Prompt for TursoPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.prompt)
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _edit_mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("   ...> ")
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        let prefix = match history_search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        Cow::Owned(format!(
            "({}reverse-i-search)`{}': ",
            prefix, history_search.term
        ))
    }

    fn get_prompt_color(&self) -> CrosstermColor {
        Self::nu_color_to_crossterm(self.prompt_color)
    }

    fn get_prompt_multiline_color(&self) -> nu_ansi_term::Color {
        self.prompt_color
    }

    fn get_indicator_color(&self) -> CrosstermColor {
        CrosstermColor::Reset
    }

    fn get_prompt_right_color(&self) -> CrosstermColor {
        CrosstermColor::Reset
    }
}
