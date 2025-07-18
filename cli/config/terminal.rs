#[cfg(unix)]
use std::io::{self, IsTerminal, Read, Write};
#[cfg(unix)]
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TerminalTheme {
    Light,
    Dark,
    Unknown, // No colors - can't detect or unsupported platform
}

pub struct TerminalDetector;

#[cfg(target_os = "windows")]
impl TerminalDetector {
    /// Windows: Always return Unknown (no colors)
    /// Terminal detection is unreliable on Windows, so we disable colors entirely
    pub fn detect_theme() -> TerminalTheme {
        TerminalTheme::Unknown
    }
}

#[cfg(unix)]
impl TerminalDetector {
    /// Detects terminal background using ANSI escape sequences on Unix systems
    pub fn detect_theme() -> TerminalTheme {
        // Only works on interactive terminals where both stdin and stdout are terminals
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            return TerminalTheme::Unknown; // No colors for non-interactive
        }

        // Try ANSI escape sequence method
        if let Some(theme) = Self::detect_via_ansi_query() {
            return theme;
        }

        // Fallback - return Unknown (no colors) if detection fails
        TerminalTheme::Unknown
    }

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
        // Send OSC 11 query: ESC ] 11 ; ? ESC \
        print!("\x1b]11;?\x1b\\");
        io::stdout().flush().ok()?;

        // Read response with timeout
        let mut buffer = [0u8; 256];
        let mut total_read = 0;

        // Try to read response for up to 500ms
        let start_time = std::time::Instant::now();
        while start_time.elapsed() < Duration::from_millis(500) {
            match io::stdin().read(&mut buffer[total_read..]) {
                Ok(0) => {
                    // No data available, sleep briefly and continue
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Ok(bytes_read) => {
                    total_read += bytes_read;

                    let response = String::from_utf8_lossy(&buffer[..total_read]);

                    // Look for end of response (ESC \ or BEL)
                    if response.contains('\x07') || response.contains("\x1b\\") {
                        return Self::parse_ansi_color_response(&response);
                    }

                    // Prevent buffer overflow
                    if total_read >= buffer.len() - 1 {
                        break;
                    }
                }
                Err(_) => {
                    // Error reading, sleep briefly and continue
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        None
    }

    /// Parse ANSI color response to determine if background is light or dark
    fn parse_ansi_color_response(response: &str) -> Option<TerminalTheme> {
        // Look for patterns like: ]11;rgb:RRRR/GGGG/BBBB or ]11;#RRGGBB

        // Try hex format first: ]11;#RRGGBB
        if let Some(start) = response.find("]11;#") {
            let color_part = &response[start + 5..];
            if let Some(hex_end) = color_part.find(|c: char| !c.is_ascii_hexdigit()) {
                let hex_color = &color_part[..hex_end];
                if hex_color.len() >= 6 {
                    return Self::parse_hex_color(&hex_color[..6]);
                }
            }
        }

        // Try rgb: format: ]11;rgb:RRRR/GGGG/BBBB
        if let Some(start) = response.find("rgb:") {
            let color_part = &response[start + 4..];

            // Parse RGB values (format: RRRR/GGGG/BBBB)
            let parts: Vec<&str> = color_part.split('/').take(3).collect();
            if parts.len() == 3 {
                if let (Ok(r), Ok(g), Ok(b)) = (
                    u16::from_str_radix(&parts[0][..parts[0].len().min(4)], 16),
                    u16::from_str_radix(&parts[1][..parts[1].len().min(4)], 16),
                    u16::from_str_radix(&parts[2][..parts[2].len().min(4)], 16),
                ) {
                    // Convert to 0-255 range
                    let r = (r >> 8) as u8;
                    let g = (g >> 8) as u8;
                    let b = (b >> 8) as u8;

                    return Some(Self::classify_color_brightness(r, g, b));
                }
            }
        }

        None
    }

    /// Parse hex color format (#RRGGBB)
    fn parse_hex_color(hex: &str) -> Option<TerminalTheme> {
        if hex.len() != 6 {
            return None;
        }

        let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
        let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
        let b = u8::from_str_radix(&hex[4..6], 16).ok()?;

        Some(Self::classify_color_brightness(r, g, b))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    mod unix_tests {
        use super::*;

        #[test]
        fn test_hex_color_parsing() {
            // Test light colors
            assert_eq!(
                TerminalDetector::parse_hex_color("ffffff"),
                Some(TerminalTheme::Light)
            );
            assert_eq!(
                TerminalDetector::parse_hex_color("f0f0f0"),
                Some(TerminalTheme::Light)
            );

            // Test dark colors
            assert_eq!(
                TerminalDetector::parse_hex_color("000000"),
                Some(TerminalTheme::Dark)
            );
            assert_eq!(
                TerminalDetector::parse_hex_color("202020"),
                Some(TerminalTheme::Dark)
            );

            // Test invalid input
            assert_eq!(TerminalDetector::parse_hex_color("invalid"), None);
            assert_eq!(TerminalDetector::parse_hex_color("12345"), None);
        }

        #[test]
        fn test_brightness_classification() {
            // Pure white
            assert_eq!(
                TerminalDetector::classify_color_brightness(255, 255, 255),
                TerminalTheme::Light
            );

            // Pure black
            assert_eq!(
                TerminalDetector::classify_color_brightness(0, 0, 0),
                TerminalTheme::Dark
            );

            // Medium gray (should be close to threshold)
            assert_eq!(
                TerminalDetector::classify_color_brightness(128, 128, 128),
                TerminalTheme::Dark // Slightly below threshold
            );

            // Light gray
            assert_eq!(
                TerminalDetector::classify_color_brightness(200, 200, 200),
                TerminalTheme::Light
            );
        }

        #[test]
        fn test_ansi_response_parsing() {
            // Test hex format response
            let hex_response = "\x1b]11;#ffffff\x1b\\";
            assert_eq!(
                TerminalDetector::parse_ansi_color_response(hex_response),
                Some(TerminalTheme::Light)
            );

            // Test rgb format response
            let rgb_response = "\x1b]11;rgb:0000/0000/0000\x1b\\";
            assert_eq!(
                TerminalDetector::parse_ansi_color_response(rgb_response),
                Some(TerminalTheme::Dark)
            );

            // Test invalid response
            let invalid_response = "invalid response";
            assert_eq!(
                TerminalDetector::parse_ansi_color_response(invalid_response),
                None
            );
        }
    }
}
