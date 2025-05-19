use clap::Parser;
use limbo_core::{Connection, StepResult};
use nu_ansi_term::{Color, Style};
use rustyline::completion::{extract_word, Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::HistoryHinter;
use rustyline::{Completer, Helper, Hinter, Validator};
use shlex::Shlex;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::{ffi::OsString, path::PathBuf, str::FromStr as _};
use syntect::dumps::from_uncompressed_data;
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::{Scope, SyntaxSet};
use syntect::util::{as_24_bit_terminal_escaped, LinesWithEndings};

use crate::commands::CommandParser;
use crate::config::{HighlightConfig, CONFIG_DIR};

macro_rules! try_result {
    ($expr:expr, $err:expr) => {
        match $expr {
            Ok(val) => val,
            Err(_) => return Ok($err),
        }
    };
}

#[derive(Helper, Completer, Hinter, Validator)]
pub struct LimboHelper {
    #[rustyline(Completer)]
    completer: SqlCompleter<CommandParser>,
    syntax_set: SyntaxSet,
    theme_set: ThemeSet,
    syntax_config: HighlightConfig,
    #[rustyline(Hinter)]
    hinter: HistoryHinter,
}

impl LimboHelper {
    pub fn new(
        conn: Rc<Connection>,
        io: Arc<dyn limbo_core::IO>,
        syntax_config: Option<HighlightConfig>,
    ) -> Self {
        // Load only predefined syntax
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
        LimboHelper {
            completer: SqlCompleter::new(conn, io),
            syntax_set: ps,
            theme_set: ts,
            syntax_config: syntax_config.unwrap_or_default(),
            hinter: HistoryHinter::new(),
        }
    }
}

impl Highlighter for LimboHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> std::borrow::Cow<'l, str> {
        let _ = pos;
        if self.syntax_config.enable {
            // TODO use lifetimes to store highlight lines
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
            let ranges = {
                let mut ret_ranges = Vec::new();
                for new_line in LinesWithEndings::from(line) {
                    let ranges: Vec<(syntect::highlighting::Style, &str)> =
                        h.highlight_line(new_line, &self.syntax_set).unwrap();
                    ret_ranges.extend(ranges);
                }
                ret_ranges
            };
            let mut ret_line = as_24_bit_terminal_escaped(&ranges[..], false);
            // Push this escape sequence to reset terminal color modes at the end of the string
            ret_line.push_str("\x1b[0m");
            std::borrow::Cow::Owned(ret_line)
        } else {
            // Appease Pekka in syntax highlighting
            let style = Style::new().fg(Color::White); // Standard shell text color
            let styled_str = style.paint(line);
            std::borrow::Cow::Owned(styled_str.to_string())
        }
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> std::borrow::Cow<'b, str> {
        let _ = default;
        // Dark emerald green for prompt
        let style = Style::new().bold().fg(self.syntax_config.prompt.0);
        let styled_str = style.paint(prompt);
        std::borrow::Cow::Owned(styled_str.to_string())
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        let style = Style::new().bold().fg(self.syntax_config.hint.0); // Brighter dark grey for hints
        let styled_str = style.paint(hint);
        std::borrow::Cow::Owned(styled_str.to_string())
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        completion: rustyline::CompletionType,
    ) -> std::borrow::Cow<'c, str> {
        let _ = completion;
        let style = Style::new().fg(self.syntax_config.candidate.0);
        let styled_str = style.paint(candidate);
        std::borrow::Cow::Owned(styled_str.to_string())
    }

    fn highlight_char(&self, line: &str, pos: usize, kind: rustyline::highlight::CmdKind) -> bool {
        let _ = (line, pos);
        !matches!(kind, rustyline::highlight::CmdKind::MoveCursor)
    }
}

pub struct SqlCompleter<C: Parser + Send + Sync + 'static> {
    conn: Rc<Connection>,
    io: Arc<dyn limbo_core::IO>,
    // Has to be a ref cell as Rustyline takes immutable reference to self
    // This problem would be solved with Reedline as it uses &mut self for completions
    cmd: RefCell<clap::Command>,
    _cmd_phantom: PhantomData<C>,
}

impl<C: Parser + Send + Sync + 'static> SqlCompleter<C> {
    pub fn new(conn: Rc<Connection>, io: Arc<dyn limbo_core::IO>) -> Self {
        Self {
            conn,
            io,
            cmd: C::command().into(),
            _cmd_phantom: PhantomData::default(),
        }
    }

    fn dot_completion(
        &self,
        mut line: &str,
        mut pos: usize,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // TODO maybe check to see if the line is empty and then just output the command names
        line = &line[1..];
        pos = pos - 1;

        let (prefix_pos, _) = extract_word(line, pos, ESCAPE_CHAR, default_break_chars);

        let args = Shlex::new(line);
        let mut args = std::iter::once("".to_owned())
            .chain(args)
            .map(OsString::from)
            .collect::<Vec<_>>();
        if line.ends_with(' ') {
            args.push(OsString::new());
        }
        let arg_index = args.len() - 1;
        // dbg!(&pos, line, &args, arg_index);

        let mut cmd = self.cmd.borrow_mut();
        match clap_complete::engine::complete(
            &mut cmd,
            args,
            arg_index,
            PathBuf::from_str(".").ok().as_deref(),
        ) {
            Ok(candidates) => {
                let candidates = candidates
                    .iter()
                    .map(|candidate| Pair {
                        display: candidate.get_value().to_string_lossy().into_owned(),
                        replacement: candidate.get_value().to_string_lossy().into_owned(),
                    })
                    .collect::<Vec<Pair>>();

                Ok((prefix_pos + 1, candidates))
            }
            Err(_) => Ok((prefix_pos + 1, Vec::new())),
        }
    }

    fn sql_completion(&self, line: &str, pos: usize) -> rustyline::Result<(usize, Vec<Pair>)> {
        // TODO: have to differentiate words if they are enclosed in single of double quotes
        let (prefix_pos, prefix) = extract_word(line, pos, ESCAPE_CHAR, default_break_chars);
        let mut candidates = Vec::new();

        let query = try_result!(
            self.conn.query(format!(
                "SELECT candidate FROM completion('{prefix}', '{line}') ORDER BY 1;"
            )),
            (prefix_pos, candidates)
        );

        if let Some(mut rows) = query {
            loop {
                match try_result!(rows.step(), (prefix_pos, candidates)) {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let completion: &str =
                            try_result!(row.get::<&str>(0), (prefix_pos, candidates));
                        let pair = Pair {
                            display: completion.to_string(),
                            replacement: completion.to_string(),
                        };
                        candidates.push(pair);
                    }
                    StepResult::IO => {
                        try_result!(self.io.run_once(), (prefix_pos, candidates));
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        break;
                    }
                }
            }
        }

        Ok((prefix_pos, candidates))
    }
}

// Got this from the FilenameCompleter.
// TODO have to see what chars break words in Sqlite
cfg_if::cfg_if! {
    if #[cfg(unix)] {
        // rl_basic_word_break_characters, rl_completer_word_break_characters
        const fn default_break_chars(c : char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\\' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' |
            '{' | '(' | '\0')
        }
        const ESCAPE_CHAR: Option<char> = Some('\\');
        // In double quotes, not all break_chars need to be escaped
        // https://www.gnu.org/software/bash/manual/html_node/Double-Quotes.html
        #[allow(dead_code)]
        const fn double_quotes_special_chars(c: char) -> bool { matches!(c, '"' | '$' | '\\' | '`') }
    } else if #[cfg(windows)] {
        // Remove \ to make file completion works on windows
        const fn default_break_chars(c: char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' | '{' |
            '(' | '\0')
        }
        const ESCAPE_CHAR: Option<char> = None;
        #[allow(dead_code)]
        const fn double_quotes_special_chars(c: char) -> bool { c == '"' } // TODO Validate: only '"' ?
    } else if #[cfg(target_arch = "wasm32")] {
        const fn default_break_chars(c: char) -> bool { false }
        const ESCAPE_CHAR: Option<char> = None;
        #[allow(dead_code)]
        const fn double_quotes_special_chars(c: char) -> bool { false }
    }
}

impl<C: Parser + Send + Sync + 'static> Completer for SqlCompleter<C> {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        if line.starts_with(".") {
            self.dot_completion(line, pos)
        } else {
            self.sql_completion(line, pos)
        }
    }
}
