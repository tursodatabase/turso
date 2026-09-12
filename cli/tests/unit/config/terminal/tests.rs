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
