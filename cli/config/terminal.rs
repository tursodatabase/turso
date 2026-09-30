#[cfg(unix)]
use std::io::Read;
#[cfg(any(unix, target_os = "windows"))]
use std::io::{self, IsTerminal, Write};
#[cfg(any(unix, target_os = "windows"))]
use std::time::Duration;

/// OSC 11 query for the terminal background color: ESC ] 11 ; ? ESC \
#[cfg(any(unix, target_os = "windows"))]
const BACKGROUND_COLOR_QUERY: &str = "\x1b]11;?\x1b\\";
/// Color responses are short OSC strings; cap reads so an unresponsive or
/// chatty terminal cannot grow the buffer unboundedly.
#[cfg(any(unix, target_os = "windows"))]
const MAX_RESPONSE_BYTES: usize = 256;
#[cfg(any(unix, target_os = "windows"))]
const RESPONSE_POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(any(unix, target_os = "windows"))]
const RESPONSE_TIMEOUT: Duration = Duration::from_millis(500);
/// Console input queues deliver key events faster than pipes relay bytes, so
/// the console path can give up sooner.
#[cfg(target_os = "windows")]
const CONSOLE_RESPONSE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TerminalTheme {
    Light,
    Dark,
    Unknown, // No colors - can't detect or unsupported platform
}

pub struct TerminalDetector;

impl TerminalDetector {
    /// Parse ANSI color response to determine if background is light or dark
    fn parse_ansi_color_response(response: &str) -> Option<TerminalTheme> {
        // Look for patterns like: ]11;rgb:RRRR/GGGG/BBBB or ]11;#RRGGBB

        // Try hex format first: ]11;#RRGGBB
        if let Some(start) = response.find("]11;#") {
            let color_part = &response[start + 5..];
            if let Some(hex_end) = color_part.find(|ch: char| !ch.is_ascii_hexdigit()) {
                let hex = &color_part[..hex_end];
                if hex.len() >= 6 {
                    return Self::parse_hex_color(&hex[..6]);
                }
            }
        }

        // Try rgb: format: ]11;rgb:RRRR/GGGG/BBBB
        if let Some(start) = response.find("rgb:") {
            let color_part = &response[start + 4..];
            let mut parts = color_part.split('/');

            if let (Some(r), Some(g), Some(b)) = (
                parts.next().and_then(Self::parse_rgb_component),
                parts.next().and_then(Self::parse_rgb_component),
                parts.next().and_then(Self::parse_rgb_component),
            ) {
                return Some(Self::classify_color_brightness(r, g, b));
            }
        }

        None
    }

    /// Parse hex color format (#RRGGBB)
    fn parse_hex_color(hex: &str) -> Option<TerminalTheme> {
        if hex.len() != 6 || !hex.as_bytes().iter().all(u8::is_ascii_hexdigit) {
            return None;
        }

        let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
        let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
        let b = u8::from_str_radix(&hex[4..6], 16).ok()?;

        Some(Self::classify_color_brightness(r, g, b))
    }

    fn parse_rgb_component(component: &str) -> Option<u8> {
        let hex_len = component
            .as_bytes()
            .iter()
            .take_while(|byte| byte.is_ascii_hexdigit())
            .take(4)
            .count();
        if hex_len == 0 {
            return None;
        }

        let value = u16::from_str_radix(&component[..hex_len], 16).ok()?;
        Some((value >> 8) as u8)
    }

    /// Classify color brightness using perceived luminance
    fn classify_color_brightness(r: u8, g: u8, b: u8) -> TerminalTheme {
        // Use ITU-R BT.709 luma coefficients for perceived brightness
        let luminance = 0.2126 * r as f32 + 0.7152 * g as f32 + 0.0722 * b as f32;

        // Threshold around 128 (middle gray)
        if luminance > 128.0 {
            TerminalTheme::Light
        } else {
            TerminalTheme::Dark
        }
    }
}

#[cfg(any(unix, target_os = "windows"))]
impl TerminalDetector {
    /// Detect the terminal background theme once per process; each detection
    /// round-trips a query through the terminal with a timeout, so repeated
    /// calls would visibly stall the CLI.
    pub fn detect_theme() -> TerminalTheme {
        static THEME: std::sync::OnceLock<TerminalTheme> = std::sync::OnceLock::new();
        *THEME.get_or_init(Self::detect_theme_uncached)
    }

    fn detect_theme_uncached() -> TerminalTheme {
        // Only interactive terminals can answer the query.
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            return TerminalTheme::Unknown;
        }

        Self::detect_via_ansi_query().unwrap_or(TerminalTheme::Unknown)
    }
}

#[cfg(target_os = "windows")]
impl TerminalDetector {
    // GetConsoleScreenBufferInfoEx can return the default palette, so only use OSC 11.
    // https://github.com/microsoft/terminal/issues/10639
    fn detect_via_ansi_query() -> Option<TerminalTheme> {
        let reader = WindowsTerminalResponseReader::new()?;

        print!("{BACKGROUND_COLOR_QUERY}");
        io::stdout().flush().ok()?;

        let response = reader.read()?;
        Self::parse_ansi_color_response(&response)
    }
}

#[cfg(target_os = "windows")]
enum WindowsTerminalResponseReader {
    Console(WindowsConsoleModeGuard),
    Pipe(windows_sys::Win32::Foundation::HANDLE),
}

#[cfg(target_os = "windows")]
impl WindowsTerminalResponseReader {
    fn new() -> Option<Self> {
        if let Some(guard) = WindowsConsoleModeGuard::set_for_terminal_query() {
            return Some(Self::Console(guard));
        }

        use windows_sys::Win32::Storage::FileSystem::{GetFileType, FILE_TYPE_PIPE};
        use windows_sys::Win32::System::Console::STD_INPUT_HANDLE;

        let input = windows_std_handle(STD_INPUT_HANDLE)?;
        // SAFETY: `input` is a valid standard handle and remains owned by the process.
        (unsafe { GetFileType(input) } == FILE_TYPE_PIPE).then_some(Self::Pipe(input))
    }

    fn read(&self) -> Option<String> {
        match self {
            Self::Console(guard) => {
                read_windows_console_response(guard.input, CONSOLE_RESPONSE_TIMEOUT)
            }
            Self::Pipe(input) => read_windows_pipe_response(*input, RESPONSE_TIMEOUT),
        }
    }
}

#[cfg(target_os = "windows")]
struct WindowsConsoleModeGuard {
    input: windows_sys::Win32::Foundation::HANDLE,
    output: windows_sys::Win32::Foundation::HANDLE,
    original_input_mode: u32,
    original_output_mode: u32,
}

#[cfg(target_os = "windows")]
impl WindowsConsoleModeGuard {
    fn set_for_terminal_query() -> Option<Self> {
        use windows_sys::Win32::System::Console::{
            GetConsoleMode, SetConsoleMode, ENABLE_ECHO_INPUT, ENABLE_LINE_INPUT,
            ENABLE_PROCESSED_OUTPUT, ENABLE_VIRTUAL_TERMINAL_INPUT,
            ENABLE_VIRTUAL_TERMINAL_PROCESSING, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };

        let input = windows_std_handle(STD_INPUT_HANDLE)?;
        let output = windows_std_handle(STD_OUTPUT_HANDLE)?;

        let mut original_input_mode = 0;
        let mut original_output_mode = 0;
        // SAFETY: both handles were validated and the mode pointers are writable.
        unsafe {
            if GetConsoleMode(input, &mut original_input_mode) == 0
                || GetConsoleMode(output, &mut original_output_mode) == 0
            {
                return None;
            }
        }

        let query_input_mode = (original_input_mode | ENABLE_VIRTUAL_TERMINAL_INPUT)
            & !(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT);
        let query_output_mode =
            original_output_mode | ENABLE_PROCESSED_OUTPUT | ENABLE_VIRTUAL_TERMINAL_PROCESSING;

        // SAFETY: both handles remain valid, and the requested flags are valid
        // console modes. The input mode is restored if configuring output fails.
        unsafe {
            if SetConsoleMode(input, query_input_mode) == 0 {
                return None;
            }
            if SetConsoleMode(output, query_output_mode) == 0 {
                let _ = SetConsoleMode(input, original_input_mode);
                return None;
            }
        }

        Some(Self {
            input,
            output,
            original_input_mode,
            original_output_mode,
        })
    }
}

#[cfg(target_os = "windows")]
impl Drop for WindowsConsoleModeGuard {
    fn drop(&mut self) {
        use windows_sys::Win32::System::Console::SetConsoleMode;

        // SAFETY: the guard retains the valid handles and modes captured when it
        // was created. Restoration errors cannot be recovered during drop.
        unsafe {
            let _ = SetConsoleMode(self.input, self.original_input_mode);
            let _ = SetConsoleMode(self.output, self.original_output_mode);
        }
    }
}

#[cfg(target_os = "windows")]
fn windows_std_handle(
    handle_id: windows_sys::Win32::System::Console::STD_HANDLE,
) -> Option<windows_sys::Win32::Foundation::HANDLE> {
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::System::Console::GetStdHandle;

    // SAFETY: `handle_id` is one of the standard-handle constants supplied by callers.
    let handle = unsafe { GetStdHandle(handle_id) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        None
    } else {
        Some(handle)
    }
}

/// A response is complete once the terminal sends BEL or the ESC \ terminator.
#[cfg(any(unix, target_os = "windows"))]
fn completed_terminal_response(bytes: &[u8]) -> Option<String> {
    let response = String::from_utf8_lossy(bytes);
    (response.contains('\x07') || response.contains("\x1b\\")).then(|| response.into_owned())
}

#[cfg(target_os = "windows")]
fn read_windows_console_response(
    input: windows_sys::Win32::Foundation::HANDLE,
    timeout: Duration,
) -> Option<String> {
    use std::time::Instant;
    use windows_sys::Win32::System::Console::{
        GetNumberOfConsoleInputEvents, ReadConsoleInputW, INPUT_RECORD, KEY_EVENT,
    };

    let deadline = Instant::now() + timeout;
    let mut bytes = Vec::with_capacity(MAX_RESPONSE_BYTES);

    while Instant::now() < deadline && bytes.len() < MAX_RESPONSE_BYTES {
        let mut available = 0;
        // SAFETY: `input` is a valid console input handle and `available` is writable.
        if unsafe { GetNumberOfConsoleInputEvents(input, &mut available) } == 0 {
            return None;
        }
        if available == 0 {
            std::thread::sleep(RESPONSE_POLL_INTERVAL);
            continue;
        }

        let mut records = [INPUT_RECORD::default(); 16];
        let mut read = 0u32;
        // SAFETY: `input` is a valid console input handle, `records` is writable
        // for the requested count, and `read` receives the initialized count.
        if unsafe {
            ReadConsoleInputW(
                input,
                records.as_mut_ptr(),
                available.min(records.len() as u32),
                &mut read,
            )
        } == 0
        {
            return None;
        }

        for record in &records[..read as usize] {
            if record.EventType != KEY_EVENT as u16 {
                continue;
            }
            // SAFETY: `EventType` identifies the active union field as `KeyEvent`.
            let event = unsafe { record.Event.KeyEvent };
            if event.bKeyDown == 0 {
                continue;
            }
            // SAFETY: `uChar` belongs to the active key-event record.
            let code_unit = unsafe { event.uChar.UnicodeChar };
            if code_unit == 0 {
                continue;
            }
            let Ok(byte) = u8::try_from(code_unit) else {
                continue;
            };
            let repeats = usize::from(event.wRepeatCount.max(1))
                .min(MAX_RESPONSE_BYTES.saturating_sub(bytes.len()));
            bytes.extend(std::iter::repeat_n(byte, repeats));
        }

        if let Some(response) = completed_terminal_response(&bytes) {
            return Some(response);
        }
    }

    None
}

#[cfg(target_os = "windows")]
fn read_windows_pipe_response(
    input: windows_sys::Win32::Foundation::HANDLE,
    timeout: Duration,
) -> Option<String> {
    use std::ptr::null_mut;
    use std::time::Instant;
    use windows_sys::Win32::Storage::FileSystem::ReadFile;
    use windows_sys::Win32::System::Pipes::PeekNamedPipe;

    let deadline = Instant::now() + timeout;
    let mut bytes = Vec::with_capacity(MAX_RESPONSE_BYTES);

    while Instant::now() < deadline && bytes.len() < MAX_RESPONSE_BYTES {
        let mut available = 0;
        // SAFETY: `input` is a valid pipe handle and `available` is writable.
        if unsafe { PeekNamedPipe(input, null_mut(), 0, null_mut(), &mut available, null_mut()) }
            == 0
        {
            return None;
        }
        if available == 0 {
            std::thread::sleep(RESPONSE_POLL_INTERVAL);
            continue;
        }

        let mut buffer = [0u8; 64];
        let requested = available
            .min(buffer.len() as u32)
            .min((MAX_RESPONSE_BYTES - bytes.len()) as u32);
        let mut read = 0;
        // SAFETY: `input` is a valid pipe handle, `buffer` is writable for
        // `requested` bytes, and peeking proved that many bytes are available.
        if unsafe {
            ReadFile(
                input,
                buffer.as_mut_ptr().cast(),
                requested,
                &mut read,
                null_mut(),
            )
        } == 0
        {
            return None;
        }
        bytes.extend_from_slice(&buffer[..read as usize]);

        if let Some(response) = completed_terminal_response(&bytes) {
            return Some(response);
        }
    }

    None
}

#[cfg(unix)]
impl TerminalDetector {
    /// Query terminal background color using ANSI escape sequence OSC 11
    fn detect_via_ansi_query() -> Option<TerminalTheme> {
        // Save current terminal settings
        let original_termios = Self::save_terminal_settings()?;

        // Set terminal to raw mode
        Self::set_raw_mode()?;

        // Send query and read response
        let theme = Self::query_background_color();

        // Restore terminal settings
        Self::restore_terminal_settings(&original_termios);

        theme
    }

    /// Save current terminal settings
    fn save_terminal_settings() -> Option<libc::termios> {
        use std::os::unix::io::AsRawFd;

        let stdin_fd = io::stdin().as_raw_fd();
        let mut termios = unsafe { std::mem::zeroed::<libc::termios>() };

        unsafe {
            if libc::tcgetattr(stdin_fd, &mut termios) == 0 {
                Some(termios)
            } else {
                None
            }
        }
    }

    /// Set terminal to raw mode
    fn set_raw_mode() -> Option<()> {
        use std::os::unix::io::AsRawFd;

        let stdin_fd = io::stdin().as_raw_fd();
        let mut termios = unsafe { std::mem::zeroed::<libc::termios>() };

        unsafe {
            if libc::tcgetattr(stdin_fd, &mut termios) != 0 {
                return None;
            }

            // Set raw mode: disable canonical mode, echo, and signals
            termios.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG);
            termios.c_iflag &= !(libc::IXON | libc::ICRNL);
            termios.c_oflag &= !libc::OPOST;

            // Set minimum characters to read and timeout
            termios.c_cc[libc::VMIN] = 0;
            termios.c_cc[libc::VTIME] = 1; // 0.1 second timeout

            if libc::tcsetattr(stdin_fd, libc::TCSANOW, &termios) == 0 {
                Some(())
            } else {
                None
            }
        }
    }

    /// Restore terminal settings
    fn restore_terminal_settings(original: &libc::termios) {
        use std::os::unix::io::AsRawFd;

        let stdin_fd = io::stdin().as_raw_fd();
        unsafe {
            libc::tcsetattr(stdin_fd, libc::TCSANOW, original);
        }
    }

    /// Send background color query and read response
    fn query_background_color() -> Option<TerminalTheme> {
        print!("{BACKGROUND_COLOR_QUERY}");
        io::stdout().flush().ok()?;

        let mut buffer = [0u8; MAX_RESPONSE_BYTES];
        let mut total_read = 0;

        let deadline = std::time::Instant::now() + RESPONSE_TIMEOUT;
        while std::time::Instant::now() < deadline && total_read < buffer.len() {
            match io::stdin().read(&mut buffer[total_read..]) {
                Ok(0) => std::thread::sleep(RESPONSE_POLL_INTERVAL),
                Ok(bytes_read) => {
                    total_read += bytes_read;
                    if let Some(response) = completed_terminal_response(&buffer[..total_read]) {
                        return Self::parse_ansi_color_response(&response);
                    }
                }
                Err(_) => std::thread::sleep(RESPONSE_POLL_INTERVAL),
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_color_parsing() {
        assert_eq!(
            TerminalDetector::parse_hex_color("ffffff"),
            Some(TerminalTheme::Light)
        );
        assert_eq!(
            TerminalDetector::parse_hex_color("f0f0f0"),
            Some(TerminalTheme::Light)
        );
        assert_eq!(
            TerminalDetector::parse_hex_color("000000"),
            Some(TerminalTheme::Dark)
        );
        assert_eq!(
            TerminalDetector::parse_hex_color("202020"),
            Some(TerminalTheme::Dark)
        );
        assert_eq!(TerminalDetector::parse_hex_color("invalid"), None);
        assert_eq!(TerminalDetector::parse_hex_color("12345"), None);
        assert_eq!(TerminalDetector::parse_hex_color("\u{20ac}\u{20ac}"), None);
    }

    #[test]
    fn test_brightness_classification() {
        assert_eq!(
            TerminalDetector::classify_color_brightness(255, 255, 255),
            TerminalTheme::Light
        );
        assert_eq!(
            TerminalDetector::classify_color_brightness(0, 0, 0),
            TerminalTheme::Dark
        );
        assert_eq!(
            TerminalDetector::classify_color_brightness(128, 128, 128),
            TerminalTheme::Dark
        );
        assert_eq!(
            TerminalDetector::classify_color_brightness(200, 200, 200),
            TerminalTheme::Light
        );
    }

    #[test]
    fn test_ansi_response_parsing() {
        let hex_response = "\x1b]11;#ffffff\x1b\\";
        assert_eq!(
            TerminalDetector::parse_ansi_color_response(hex_response),
            Some(TerminalTheme::Light)
        );

        let unterminated_hex_response = "\x1b]11;#000000";
        assert_eq!(
            TerminalDetector::parse_ansi_color_response(unterminated_hex_response),
            None
        );

        let rgb_response = "\x1b]11;rgb:0000/0000/0000\x1b\\";
        assert_eq!(
            TerminalDetector::parse_ansi_color_response(rgb_response),
            Some(TerminalTheme::Dark)
        );

        let invalid_response = "invalid response";
        assert_eq!(
            TerminalDetector::parse_ansi_color_response(invalid_response),
            None
        );
    }
}
