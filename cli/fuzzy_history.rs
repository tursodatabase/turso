use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use nucleo::{Config, Matcher, Nucleo, Utf32Str};
use ratatui::{
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, List, ListItem, ListState, Paragraph},
    DefaultTerminal,
};
use std::path::PathBuf;
use std::sync::Arc;
use syntect::dumps::from_uncompressed_data;
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::{Scope, SyntaxSet};
use syntect::util::LinesWithEndings;
use tui_input::backend::crossterm::EventHandler;
use tui_input::Input;

use crate::config::{HighlightConfig, CONFIG_DIR};

type HighlightSpans = Vec<(Style, String)>;

struct SqlHighlighter {
    syntax_set: SyntaxSet,
    theme_set: ThemeSet,
    theme_name: String,
}

impl SqlHighlighter {
    fn new(config: Option<&HighlightConfig>) -> Self {
        let ps = from_uncompressed_data(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/SQL_syntax_set_dump.packdump"
        )))
        .unwrap();
        let mut ts = ThemeSet::load_defaults();
        let theme_dir = CONFIG_DIR.join("themes");
        if theme_dir.exists() {
            let _ = ts.add_from_folder(theme_dir);
        }
        let theme_name = config
            .map(|c| c.theme.clone())
            .unwrap_or_else(|| "base16-ocean.dark".to_string());
        Self {
            syntax_set: ps,
            theme_set: ts,
            theme_name,
        }
    }

    fn highlight(&self, text: &str) -> HighlightSpans {
        let syntax = self
            .syntax_set
            .find_syntax_by_scope(Scope::new("source.sql").unwrap())
            .unwrap();
        let theme = self
            .theme_set
            .themes
            .get(&self.theme_name)
            .unwrap_or(&self.theme_set.themes["base16-ocean.dark"]);
        let mut h = HighlightLines::new(syntax, theme);

        let mut result = Vec::new();
        for line in LinesWithEndings::from(text) {
            let ranges = h.highlight_line(line, &self.syntax_set).unwrap();
            for (style, s) in ranges {
                let fg = style.foreground;
                let ratatui_style = Style::default().fg(Color::Rgb(fg.r, fg.g, fg.b));
                result.push((ratatui_style, s.to_string()));
            }
        }
        result
    }
}

/// Caches the rendered list items, only rebuilding when the query or matches change.
struct ItemsCache {
    items: Vec<ListItem<'static>>,
    dirty: bool,
    matcher: Matcher,
    indices_buf: Vec<u32>,
    utf32_buf: Vec<char>,
}

impl ItemsCache {
    fn new() -> Self {
        Self {
            items: Vec::new(),
            dirty: true,
            matcher: Matcher::new(Config::DEFAULT),
            indices_buf: Vec::new(),
            utf32_buf: Vec::new(),
        }
    }

    fn invalidate(&mut self) {
        self.dirty = true;
    }

    fn get(
        &mut self,
        nucleo: &Nucleo<String>,
        query: &str,
        highlighter: &SqlHighlighter,
        highlight_cache: &mut std::collections::HashMap<String, HighlightSpans>,
    ) -> &[ListItem<'static>] {
        if !self.dirty {
            return &self.items;
        }
        self.dirty = false;

        let snapshot = nucleo.snapshot();
        let matched = snapshot.matched_item_count();
        let pattern = snapshot.pattern();
        let count = matched.min(500) as usize;

        // Collect entries + indices first to release snapshot borrow
        let mut entries: Vec<(String, Vec<u32>)> = Vec::with_capacity(count);
        for i in 0..count {
            if let Some(item) = snapshot.get_matched_item(i as u32) {
                self.indices_buf.clear();
                self.utf32_buf.clear();
                if !query.is_empty() {
                    let haystack = Utf32Str::new(item.data, &mut self.utf32_buf);
                    pattern.column_pattern(0).indices(
                        haystack,
                        &mut self.matcher,
                        &mut self.indices_buf,
                    );
                    self.indices_buf.sort_unstable();
                    self.indices_buf.dedup();
                }
                entries.push((item.data.clone(), self.indices_buf.clone()));
            }
        }

        let match_modifier = Modifier::BOLD | Modifier::UNDERLINED;
        self.items.clear();
        for (text, indices) in &entries {
            let syntax_spans = highlight_cache
                .entry(text.clone())
                .or_insert_with(|| highlighter.highlight(text))
                .clone();

            let mut result_spans = Vec::new();
            let mut char_offset = 0;
            for (base_style, fragment) in &syntax_spans {
                let mut current_text = String::new();
                let mut current_style = *base_style;

                for ch in fragment.chars() {
                    let is_match = indices.contains(&(char_offset as u32));
                    let style = if is_match {
                        base_style.add_modifier(match_modifier)
                    } else {
                        *base_style
                    };
                    if style != current_style && !current_text.is_empty() {
                        result_spans.push(Span::styled(
                            std::mem::take(&mut current_text),
                            current_style,
                        ));
                    }
                    current_style = style;
                    current_text.push(ch);
                    char_offset += 1;
                }
                if !current_text.is_empty() {
                    result_spans.push(Span::styled(current_text, current_style));
                }
            }
            self.items.push(ListItem::new(Line::from(result_spans)));
        }

        &self.items
    }
}

struct FzfState {
    nucleo: Nucleo<String>,
    input: Input,
    selected: usize,
    highlighter: SqlHighlighter,
    highlight_cache: std::collections::HashMap<String, HighlightSpans>,
    items_cache: ItemsCache,
}

impl FzfState {
    fn new(
        history_path: &PathBuf,
        highlight_config: Option<&HighlightConfig>,
    ) -> Option<(Self, std::sync::mpsc::Receiver<(String, HighlightSpans)>)> {
        let entries = read_history(history_path);
        if entries.is_empty() {
            return None;
        }

        let mut nucleo: Nucleo<String> = Nucleo::new(Config::DEFAULT, Arc::new(|| {}), Some(1), 1);
        let injector = nucleo.injector();
        for entry in &entries {
            injector.push(entry.clone(), |s, cols| {
                cols[0] = s.as_str().into();
            });
        }

        nucleo
            .pattern
            .reparse(0, "", Default::default(), Default::default(), false);
        nucleo.tick(50);

        let highlighter = SqlHighlighter::new(highlight_config);

        // Populate highlight cache in background thread
        let (hl_tx, hl_rx) = std::sync::mpsc::channel();
        let entries_clone = entries.clone();
        let hl_config_clone = highlight_config.cloned();
        std::thread::spawn(move || {
            let hl = SqlHighlighter::new(hl_config_clone.as_ref());
            for entry in entries_clone {
                let spans = hl.highlight(&entry);
                if hl_tx.send((entry, spans)).is_err() {
                    break;
                }
            }
        });

        Some((
            Self {
                nucleo,
                input: Input::default(),
                selected: 0,
                highlighter,
                highlight_cache: std::collections::HashMap::new(),
                items_cache: ItemsCache::new(),
            },
            hl_rx,
        ))
    }

    fn query(&self) -> &str {
        self.input.value()
    }

    fn update_pattern(&mut self, append: bool) {
        self.nucleo.pattern.reparse(
            0,
            self.input.value(),
            Default::default(),
            Default::default(),
            append,
        );
        self.nucleo.tick(10);
        self.items_cache.invalidate();
    }

    fn matched_count(&self) -> u32 {
        self.nucleo.snapshot().matched_item_count()
    }

    fn total_count(&self) -> u32 {
        self.nucleo.snapshot().item_count()
    }

    fn get_selected(&self) -> Option<String> {
        let snapshot = self.nucleo.snapshot();
        if snapshot.matched_item_count() == 0 {
            return None;
        }
        let idx = self
            .selected
            .min(snapshot.matched_item_count() as usize - 1);
        snapshot
            .get_matched_item(idx as u32)
            .map(|item| item.data.clone())
    }
}

pub fn run(
    history_path: &PathBuf,
    highlight_config: Option<&HighlightConfig>,
    initial_query: String,
) -> Option<String> {
    let (mut state, hl_rx) = FzfState::new(history_path, highlight_config)?;

    if !initial_query.is_empty() {
        state.input = Input::new(initial_query);
        state.update_pattern(false);
    }

    let mut terminal = ratatui::try_init().ok()?;

    // Dedicated thread for terminal events — never blocks rendering
    let (term_tx, term_rx) = std::sync::mpsc::channel::<Event>();
    std::thread::spawn(move || loop {
        if event::poll(std::time::Duration::from_millis(50)).unwrap_or(false) {
            if let Ok(ev) = event::read() {
                if term_tx.send(ev).is_err() {
                    break;
                }
            }
        }
    });

    let result = event_loop(&mut terminal, &mut state, &term_rx, &hl_rx);

    ratatui::try_restore().ok()?;

    result
}

fn draw(frame: &mut ratatui::Frame, state: &mut FzfState) {
    let area = frame.area();

    let [list_area, info_area, input_area] = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(area);

    let query = state.query().to_string();
    let matched = state.matched_count();
    let total = state.total_count();
    let selected = state.selected;

    let items = state.items_cache.get(
        &state.nucleo,
        &query,
        &state.highlighter,
        &mut state.highlight_cache,
    );

    let mut list_state = ListState::default();
    if matched > 0 {
        list_state.select(Some(selected.min(matched as usize - 1)));
    }

    let list = List::new(items.to_vec())
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ")
        .block(Block::default());

    frame.render_stateful_widget(list, list_area, &mut list_state);

    // Info line
    let info = Paragraph::new(Line::from(vec![Span::styled(
        format!("  {matched}/{total}"),
        Style::default().fg(Color::Yellow),
    )]));
    frame.render_widget(info, info_area);

    // Input line with tui-input
    let scroll = state.input.visual_scroll(input_area.width as usize - 2);
    let input_widget = Paragraph::new(Line::from(vec![
        Span::styled("> ", Style::default().fg(Color::Cyan)),
        Span::raw(state.input.value()),
    ]));
    frame.render_widget(input_widget, input_area);

    frame.set_cursor_position((
        input_area.x + 2 + (state.input.visual_cursor() - scroll) as u16,
        input_area.y,
    ));
}

fn event_loop(
    terminal: &mut DefaultTerminal,
    state: &mut FzfState,
    term_rx: &std::sync::mpsc::Receiver<Event>,
    hl_rx: &std::sync::mpsc::Receiver<(String, HighlightSpans)>,
) -> Option<String> {
    loop {
        // Drain any ready highlight results into the cache
        while let Ok((text, spans)) = hl_rx.try_recv() {
            state.highlight_cache.insert(text, spans);
        }

        terminal.draw(|frame| draw(frame, state)).ok()?;

        // Process all queued events before next render, drain the backlog
        let first = term_rx.recv().ok()?;
        for ev in std::iter::once(first).chain(std::iter::from_fn(|| term_rx.try_recv().ok())) {
            if let Some(result) = handle_event(state, &ev) {
                return result;
            }
        }
    }
}

/// Returns `Some(result)` if the loop should exit, `None` to continue.
fn handle_event(state: &mut FzfState, event: &Event) -> Option<Option<String>> {
    let Event::Key(key) = event else {
        return None;
    };
    match key {
        KeyEvent {
            code: KeyCode::Esc, ..
        }
        | KeyEvent {
            code: KeyCode::Char('c'),
            modifiers: KeyModifiers::CONTROL,
            ..
        }
        | KeyEvent {
            code: KeyCode::Char('g'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => Some(None),
        KeyEvent {
            code: KeyCode::Enter,
            ..
        } => Some(state.get_selected()),
        KeyEvent {
            code: KeyCode::Up, ..
        }
        | KeyEvent {
            code: KeyCode::Char('p'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => {
            state.selected = state.selected.saturating_sub(1);
            None
        }
        KeyEvent {
            code: KeyCode::Down,
            ..
        }
        | KeyEvent {
            code: KeyCode::Char('n'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => {
            let max = state.matched_count().saturating_sub(1) as usize;
            state.selected = (state.selected + 1).min(max);
            None
        }
        _ => {
            let old_value = state.query().to_string();
            state.input.handle_event(event);
            let new_value = state.query().to_string();
            if new_value != old_value {
                let append = new_value.starts_with(&old_value) && !old_value.is_empty();
                state.selected = 0;
                state.update_pattern(append);
            }
            None
        }
    }
}

fn read_history(path: &PathBuf) -> Vec<String> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut seen = std::collections::HashSet::new();
    content
        .lines()
        .rev()
        .filter(|line| !line.is_empty() && seen.insert(line.to_string()))
        .map(|s| s.to_string())
        .collect()
}
