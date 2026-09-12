use super::*;

#[test]
fn test_utf8_read() {
    let input = "Café".as_bytes();
    let (c, size) = utf8_read(&input[0..]);
    assert_eq!(c, b'C' as u32);
    assert_eq!(size, 1);
    let (c, size) = utf8_read(&input[3..]);
    assert_eq!(c, 0x00E9); // é
    assert_eq!(size, 2);
}

#[test]
fn test_transliterate_basic() {
    let result = transliterate_str("Café");
    assert_eq!(result, "Cafe");
    let result = transliterate_str("Naïve");
    assert_eq!(result, "Naive");
}

#[test]
fn test_transliterate_german() {
    let result = transliterate_str("Müller");
    assert_eq!(result, "Mueller");
    let result = transliterate_str("Größe");
    assert_eq!(result, "Groesse");
}

#[test]
fn test_script_code() {
    assert_eq!(script_code("Hello".as_bytes()), 215);
    assert_eq!(script_code("123".as_bytes()), 215);
    assert_eq!(script_code("привет".as_bytes()), 220);
    assert_eq!(script_code("γειά".as_bytes()), 200);
    assert_eq!(script_code("helloпривет".as_bytes()), 998);
}
