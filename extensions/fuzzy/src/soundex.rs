/// Computes and returns the soundex representation of a given string.
/// https://en.wikipedia.org/wiki/Soundex
pub fn soundex(input: Option<&str>) -> Option<String> {
    if let Some(input_str) = input {
        if input_str.is_empty() {
            return Some("".to_string());
        }

        let str_bytes = input_str.as_bytes();
        let str_len = str_bytes.len();

        let mut code = String::with_capacity(4);
        code.push(str_bytes[0].to_ascii_uppercase() as char);

        let mut buf: Vec<char> = Vec::with_capacity(str_len);
        for &byte in str_bytes {
            buf.push(soundex_encode(byte as char));
        }

        let mut d = 1; // digit counter
        let mut i = 1; // index counter

        while i < str_len && d < 4 {
            let current = buf[i];
            let previous = buf[i - 1];

            if current != previous && current != '0' {
                if i > 1 {
                    let two_back = buf[i - 2];
                    let separator = str_bytes[i - 1].to_ascii_lowercase() as char;
                    if current == two_back && (separator == 'h' || separator == 'w') {
                        i += 1;
                        continue;
                    }
                }

                code.push(current);
                d += 1;
            }
            i += 1;
        }

        while d < 4 {
            code.push('0');
            d += 1;
        }

        Some(code)
    } else {
        None
    }
}

/// Helper function
fn soundex_encode(c: char) -> char {
    match c.to_ascii_lowercase() {
        'b' | 'f' | 'p' | 'v' => '1',
        'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => '2',
        'd' | 't' => '3',
        'l' => '4',
        'm' | 'n' => '5',
        'r' => '6',
        _ => '0',
    }
}
