/// Computes and returns the soundex representation of a given non NULL string.
/// More information about the algorithm can be found here:
///     http://ntz-develop.blogspot.com/2011/03/phonetic-algorithms.html
pub fn rsoundex(input: Option<&str>) -> Option<String> {
    if let Some(s) = input {
        if s.is_empty() {
            return Some("".to_string());
        }

        let str_bytes = s.as_bytes();
        let str_len = str_bytes.len();

        let mut code = String::with_capacity(str_len + 1);
        code.push(str_bytes[0].to_ascii_uppercase() as char);

        let mut buf: Vec<char> = Vec::with_capacity(str_len);
        for &b in str_bytes {
            buf.push(refined_soundex_encode(b as char));
        }

        let mut prev: Option<char> = None;
        for c in buf {
            if Some(c) != prev {
                code.push(c);
                prev = Some(c);
            }
        }

        Some(code)
    } else {
        None
    }
}

//helper
fn refined_soundex_encode(c: char) -> char {
    match c.to_ascii_lowercase() {
        'b' | 'p' => '1',
        'f' | 'v' => '2',
        'c' | 'k' | 's' => '3',
        'g' | 'j' => '4',
        'q' | 'x' | 'z' => '5',
        'd' | 't' => '6',
        'l' => '7',
        'm' | 'n' => '8',
        'r' => '9',
        _ => '0',
    }
}
