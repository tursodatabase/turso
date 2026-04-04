use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use nucleo::{Config, Matcher, Nucleo, Utf32Str};
use ratatui::{
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, List, ListItem, ListState, Paragraph},
    DefaultTerminal,
};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use syntect::dumps::from_uncompressed_data;
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::{Scope, SyntaxSet};
use syntect::util::LinesWithEndings;
use tui_input::backend::crossterm::EventHandler;
use tui_input::Input;

use crate::config::{HighlightConfig, CONFIG_DIR};

type HighlightSpans = Vec<(Style, String)>;

struct HistoryEntry {
    text: String,
    alive: Arc<AtomicBool>,
}

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

#[derive(Clone)]
struct VisibleRow {
    text: String,
    indices: Vec<u32>,
}

/// Caches the visible matched rows, only rebuilding when the query or matches change.
struct ItemsCache {
    rows: Vec<VisibleRow>,
    dirty: bool,
    matcher: Matcher,
    indices_buf: Vec<u32>,
    utf32_buf: Vec<char>,
}

impl ItemsCache {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            dirty: true,
            matcher: Matcher::new(Config::DEFAULT),
            indices_buf: Vec::new(),
            utf32_buf: Vec::new(),
        }
    }

    fn invalidate(&mut self) {
        self.dirty = true;
    }

    fn get(&mut self, nucleo: &Nucleo<HistoryEntry>, query: &str) -> &[VisibleRow] {
        if !self.dirty {
            return &self.rows;
        }
        self.dirty = false;

        let snapshot = nucleo.snapshot();
        let matched = snapshot.matched_item_count();
        let pattern = snapshot.pattern();

        self.rows.clear();
        for i in 0..matched {
            if self.rows.len() == 500 {
                break;
            }
            if let Some(item) = snapshot.get_matched_item(i as u32) {
                if !item.data.alive.load(Ordering::Acquire) {
                    continue;
                }
                self.indices_buf.clear();
                self.utf32_buf.clear();
                if !query.is_empty() {
                    let haystack = Utf32Str::new(&item.data.text, &mut self.utf32_buf);
                    pattern.column_pattern(0).indices(
                        haystack,
                        &mut self.matcher,
                        &mut self.indices_buf,
                    );
                    self.indices_buf.sort_unstable();
                    self.indices_buf.dedup();
                }
                self.rows.push(VisibleRow {
                    text: item.data.text.clone(),
                    indices: self.indices_buf.clone(),
                });
            }
        }

        if query.is_empty() {
            self.rows.reverse();
        }

        &self.rows
    }
}

struct FzfState {
    ui: UiState,
    matches: MatchState,
    render: RenderState,
}

struct UiState {
    input: Input,
    selected: usize,
    history_loaded: Arc<AtomicBool>,
}

struct MatchState {
    nucleo: Nucleo<HistoryEntry>,
    items_cache: ItemsCache,
    live_count: Arc<std::sync::atomic::AtomicUsize>,
}

struct RenderState {
    highlighter: SqlHighlighter,
    highlight_cache: std::collections::HashMap<String, HighlightSpans>,
}

impl MatchState {
    fn update_pattern(&mut self, query: &str, append: bool) {
        self.nucleo
            .pattern
            .reparse(0, query, Default::default(), Default::default(), append);
    }

    fn refresh(&mut self, query: &str, selected: &mut usize) {
        let status = self.nucleo.tick(10);
        if !status.changed {
            return;
        }

        self.items_cache.invalidate();
        let visible = self.visible_count(query);
        if visible == 0 {
            *selected = 0;
        } else {
            *selected = (*selected).min(visible - 1);
        }
    }

    fn visible_rows(&mut self, query: &str) -> &[VisibleRow] {
        self.items_cache.get(&self.nucleo, query)
    }

    fn visible_count(&mut self, query: &str) -> usize {
        self.visible_rows(query).len()
    }

    fn total_count(&self) -> u32 {
        self.live_count.load(Ordering::Acquire) as u32
    }
}

impl RenderState {
    fn render_row(&mut self, row: &VisibleRow) -> ListItem<'static> {
        let syntax_spans = self
            .highlight_cache
            .entry(row.text.clone())
            .or_insert_with(|| self.highlighter.highlight(&row.text))
            .clone();

        let match_modifier = Modifier::BOLD | Modifier::UNDERLINED;
        let mut result_spans = Vec::new();
        let mut char_offset = 0;
        for (base_style, fragment) in syntax_spans {
            let mut current_text = String::new();
            let mut current_style = base_style;

            for ch in fragment.chars() {
                let is_match = row.indices.contains(&(char_offset as u32));
                let style = if is_match {
                    base_style.add_modifier(match_modifier)
                } else {
                    base_style
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

        ListItem::new(Line::from(result_spans))
    }
}

enum AppEvent {
    Terminal(Event),
    DataChanged,
}

enum EventOutcome {
    Continue { tick: bool },
    Exit(Option<String>),
}

impl FzfState {
    fn new(
        history_path: &PathBuf,
        highlight_config: Option<&HighlightConfig>,
        app_tx: std::sync::mpsc::Sender<AppEvent>,
    ) -> Option<Self> {
        if !history_path.exists() {
            return None;
        }

        let notify_tx = app_tx.clone();
        let nucleo_notify = Arc::new(move || {
            let _ = notify_tx.send(AppEvent::DataChanged);
        });
        let nucleo: Nucleo<HistoryEntry> = Nucleo::new(Config::DEFAULT, nucleo_notify, Some(1), 1);
        let injector = nucleo.injector();

        let history_loaded = Arc::new(AtomicBool::new(false));
        let live_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let history_path = history_path.clone();
        let history_loaded_clone = history_loaded.clone();
        let live_count_clone = live_count.clone();
        let loader_wake_tx = app_tx.clone();
        std::thread::spawn(move || {
            let mut latest = std::collections::HashMap::<String, Arc<AtomicBool>>::new();
            let Ok(file) = std::fs::File::open(history_path) else {
                history_loaded_clone.store(true, Ordering::Release);
                let _ = loader_wake_tx.send(AppEvent::DataChanged);
                return;
            };
            for line in BufReader::new(file).lines() {
                let Ok(text) = line else {
                    continue;
                };
                if text.is_empty() {
                    continue;
                }

                let alive = Arc::new(AtomicBool::new(true));
                if let Some(previous) = latest.insert(text.clone(), alive.clone()) {
                    previous.store(false, Ordering::Release);
                } else {
                    live_count_clone.fetch_add(1, Ordering::Relaxed);
                }

                injector.push(HistoryEntry { text, alive }, |entry, cols| {
                    cols[0] = entry.text.as_str().into();
                });
            }
            history_loaded_clone.store(true, Ordering::Release);
            let _ = loader_wake_tx.send(AppEvent::DataChanged);
        });

        Some(Self {
            ui: UiState {
                input: Input::default(),
                selected: 0,
                history_loaded,
            },
            matches: MatchState {
                nucleo,
                items_cache: ItemsCache::new(),
                live_count,
            },
            render: RenderState {
                highlighter: SqlHighlighter::new(highlight_config),
                highlight_cache: std::collections::HashMap::new(),
            },
        })
    }

    fn query(&self) -> &str {
        self.ui.input.value()
    }

    fn update_pattern(&mut self, append: bool) {
        let query = self.ui.input.value();
        self.matches.update_pattern(query, append);
    }

    fn refresh_matches(&mut self) {
        let query = self.ui.input.value();
        self.matches.refresh(query, &mut self.ui.selected);
    }

    fn visible_rows(&mut self) -> &[VisibleRow] {
        let query = self.ui.input.value();
        self.matches.visible_rows(query)
    }

    fn visible_count(&mut self) -> usize {
        let query = self.ui.input.value();
        self.matches.visible_count(query)
    }

    fn total_count(&self) -> u32 {
        self.matches.total_count()
    }

    fn get_selected(&mut self) -> Option<String> {
        let idx = self.ui.selected.min(self.visible_count().saturating_sub(1));
        self.visible_rows().get(idx).map(|row| row.text.clone())
    }
}

pub fn run(
    history_path: &PathBuf,
    highlight_config: Option<&HighlightConfig>,
    initial_query: String,
) -> Option<String> {
    let (app_tx, app_rx) = std::sync::mpsc::channel::<AppEvent>();
    let mut state = FzfState::new(history_path, highlight_config, app_tx.clone())?;

    if !initial_query.is_empty() {
        state.ui.input = Input::new(initial_query);
        state.update_pattern(false);
        let _ = app_tx.send(AppEvent::DataChanged);
    }

    let mut terminal = ratatui::try_init().ok()?;

    // Dedicated thread for terminal events — never blocks rendering
    std::thread::spawn(move || loop {
        if event::poll(Duration::from_millis(50)).unwrap_or(false) {
            if let Ok(ev) = event::read() {
                if app_tx.send(AppEvent::Terminal(ev)).is_err() {
                    break;
                }
            }
        }
    });

    let result = event_loop(&mut terminal, &mut state, &app_rx);

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

    let rows = state.visible_rows().to_vec();
    let matched = rows.len();
    let total = state.total_count();
    let selected = state.ui.selected;
    let items = rows
        .iter()
        .map(|row| state.render.render_row(row))
        .collect::<Vec<_>>();

    let mut list_state = ListState::default();
    if matched > 0 {
        list_state.select(Some(selected.min(matched - 1)));
    }

    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ")
        .block(Block::default());

    frame.render_stateful_widget(list, list_area, &mut list_state);

    // Info line
    let mut info_spans = vec![Span::styled(
        format!("  {matched}/{total}"),
        Style::default().fg(Color::Yellow),
    )];
    if !state.ui.history_loaded.load(Ordering::Acquire) {
        info_spans.push(Span::raw(" "));
        info_spans.push(Span::styled(
            "loading history...",
            Style::default().fg(Color::DarkGray),
        ));
    }
    let info = Paragraph::new(Line::from(info_spans));
    frame.render_widget(info, info_area);

    // Input line with tui-input
    let scroll = state.ui.input.visual_scroll(input_area.width as usize - 2);
    let input_widget = Paragraph::new(Line::from(vec![
        Span::styled("> ", Style::default().fg(Color::Cyan)),
        Span::raw(state.ui.input.value()),
    ]));
    frame.render_widget(input_widget, input_area);

    frame.set_cursor_position((
        input_area.x + 2 + (state.ui.input.visual_cursor() - scroll) as u16,
        input_area.y,
    ));
}

fn event_loop(
    terminal: &mut DefaultTerminal,
    state: &mut FzfState,
    app_rx: &std::sync::mpsc::Receiver<AppEvent>,
) -> Option<String> {
    loop {
        terminal.draw(|frame| draw(frame, state)).ok()?;

        let first = app_rx.recv().ok()?;
        for app_event in std::iter::once(first).chain(std::iter::from_fn(|| app_rx.try_recv().ok()))
        {
            match app_event {
                AppEvent::Terminal(ev) => match handle_event(state, &ev) {
                    EventOutcome::Continue { tick } => {
                        if tick {
                            state.refresh_matches();
                        }
                    }
                    EventOutcome::Exit(result) => return result,
                },
                AppEvent::DataChanged => {
                    state.refresh_matches();
                }
            }
        }
    }
}

fn handle_event(state: &mut FzfState, event: &Event) -> EventOutcome {
    let Event::Key(key) = event else {
        return EventOutcome::Continue { tick: false };
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
        } => EventOutcome::Exit(None),
        KeyEvent {
            code: KeyCode::Enter,
            ..
        } => EventOutcome::Exit(state.get_selected()),
        KeyEvent {
            code: KeyCode::Up, ..
        }
        | KeyEvent {
            code: KeyCode::Char('p'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => {
            state.ui.selected = state.ui.selected.saturating_sub(1);
            EventOutcome::Continue { tick: false }
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
            let max = state.visible_count().saturating_sub(1);
            state.ui.selected = (state.ui.selected + 1).min(max);
            EventOutcome::Continue { tick: false }
        }
        _ => {
            let old_value = state.query().to_string();
            state.ui.input.handle_event(event);
            let new_value = state.query();
            if new_value != old_value {
                let append = new_value.starts_with(&old_value) && !old_value.is_empty();
                state.ui.selected = 0;
                state.update_pattern(append);
                state.matches.items_cache.invalidate();
                EventOutcome::Continue { tick: true }
            } else {
                EventOutcome::Continue { tick: false }
            }
        }
    }
}
