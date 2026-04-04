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

struct FzfState {
    nucleo: Nucleo<String>,
    query: String,
    selected: usize,
    matcher: Matcher,
    indices_buf: Vec<u32>,
    utf32_buf: Vec<char>,
}

impl FzfState {
    fn new(history_path: &PathBuf) -> Option<Self> {
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

        Some(Self {
            nucleo,
            query: String::new(),
            selected: 0,
            matcher: Matcher::new(Config::DEFAULT),
            indices_buf: Vec::new(),
            utf32_buf: Vec::new(),
        })
    }

    fn update_pattern(&mut self, append: bool) {
        self.nucleo.pattern.reparse(
            0,
            &self.query,
            Default::default(),
            Default::default(),
            append,
        );
        self.nucleo.tick(10);
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

    fn build_items(&mut self) -> Vec<ListItem<'static>> {
        let snapshot = self.nucleo.snapshot();
        let matched = snapshot.matched_item_count();
        let pattern = snapshot.pattern();
        let count = matched.min(200) as usize;

        let mut items = Vec::with_capacity(count);
        for i in 0..count {
            if let Some(item) = snapshot.get_matched_item(i as u32) {
                // Compute match indices
                self.indices_buf.clear();
                self.utf32_buf.clear();
                if !self.query.is_empty() {
                    let haystack = Utf32Str::new(item.data, &mut self.utf32_buf);
                    pattern.column_pattern(0).indices(
                        haystack,
                        &mut self.matcher,
                        &mut self.indices_buf,
                    );
                    self.indices_buf.sort_unstable();
                    self.indices_buf.dedup();
                }

                let line = build_highlighted_line(item.data, &self.indices_buf);
                items.push(ListItem::new(line));
            }
        }
        items
    }
}

fn build_highlighted_line(text: &str, indices: &[u32]) -> Line<'static> {
    let match_style = Style::default()
        .fg(Color::Green)
        .add_modifier(Modifier::BOLD);
    let normal_style = Style::default();

    let mut spans = Vec::new();
    let mut current_text = String::new();
    let mut current_is_match = false;

    for (ci, ch) in text.chars().enumerate() {
        let is_match = indices.contains(&(ci as u32));
        if is_match != current_is_match && !current_text.is_empty() {
            let style = if current_is_match {
                match_style
            } else {
                normal_style
            };
            spans.push(Span::styled(std::mem::take(&mut current_text), style));
        }
        current_is_match = is_match;
        current_text.push(ch);
    }
    if !current_text.is_empty() {
        let style = if current_is_match {
            match_style
        } else {
            normal_style
        };
        spans.push(Span::styled(current_text, style));
    }

    Line::from(spans)
}

pub fn run(history_path: &PathBuf) -> Option<String> {
    let mut state = FzfState::new(history_path)?;

    let mut terminal = ratatui::try_init().ok()?;

    let result = event_loop(&mut terminal, &mut state);

    ratatui::try_restore().ok()?;

    result
}

fn event_loop(terminal: &mut DefaultTerminal, state: &mut FzfState) -> Option<String> {
    loop {
        terminal
            .draw(|frame| {
                let area = frame.area();

                let [list_area, info_area, input_area] = Layout::vertical([
                    Constraint::Min(1),
                    Constraint::Length(1),
                    Constraint::Length(1),
                ])
                .areas(area);

                // Build list items
                let items = state.build_items();
                let matched = state.matched_count();

                // List (reversed: most recent/best match at bottom)
                let mut list_state = ListState::default();
                if matched > 0 {
                    list_state.select(Some(state.selected.min(matched as usize - 1)));
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
                let total = state.total_count();
                let info = Paragraph::new(Line::from(vec![Span::styled(
                    format!("  {matched}/{total}"),
                    Style::default().fg(Color::Yellow),
                )]));
                frame.render_widget(info, info_area);

                // Input line
                let input = Paragraph::new(Line::from(vec![
                    Span::styled("> ", Style::default().fg(Color::Cyan)),
                    Span::raw(&state.query),
                ]));
                frame.render_widget(input, input_area);

                // Place cursor after query text
                frame.set_cursor_position(((2 + state.query.len()) as u16, input_area.y));
            })
            .ok()?;

        if let Ok(Event::Key(key)) = event::read() {
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
                } => return None,
                KeyEvent {
                    code: KeyCode::Enter,
                    ..
                } => return state.get_selected(),
                KeyEvent {
                    code: KeyCode::Up, ..
                }
                | KeyEvent {
                    code: KeyCode::Char('p'),
                    modifiers: KeyModifiers::CONTROL,
                    ..
                } => {
                    let max = state.matched_count().saturating_sub(1) as usize;
                    state.selected = (state.selected + 1).min(max);
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
                    state.selected = state.selected.saturating_sub(1);
                }
                KeyEvent {
                    code: KeyCode::Backspace,
                    ..
                } => {
                    state.query.pop();
                    state.selected = 0;
                    state.update_pattern(false);
                }
                KeyEvent {
                    code: KeyCode::Char(c),
                    modifiers,
                    ..
                } if !modifiers.contains(KeyModifiers::CONTROL)
                    && !modifiers.contains(KeyModifiers::ALT) =>
                {
                    state.query.push(c);
                    state.selected = 0;
                    state.update_pattern(true);
                }
                _ => {}
            }
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
