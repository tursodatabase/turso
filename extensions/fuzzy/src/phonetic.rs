use crate::common::*;

/// Generate a "phonetic hash" from a string of ASCII characters.
///
/// The algorithm:
///  Maps characters by character class as defined above
///  Omits double-letters
///  Omits vowels beside R and L
///  Omits T when followed by CH
///  Omits W when followed by R
///  Omits D when followed by J or G
///  Omits K in KN or G in GN at the beginning of a word
///
/// Returns a Vec<u8> containing the phonetic hash, or None if input is invalid.
pub fn phonetic_hash(z_in: &[u8]) -> Option<Vec<u8>> {
    if z_in.is_empty() {
        return Some(Vec::new());
    }

    let mut z_out = Vec::with_capacity(z_in.len() + 1);
    let mut c_prev = 0x77u8;
    let mut c_prev_x = 0x77u8;
    let mut a_class = &INIT_CLASS;

    let mut input = z_in;
    if z_in.len() > 2 {
        match z_in[0] {
            b'g' | b'k' => {
                if z_in[1] == b'n' {
                    input = &z_in[1..];
                }
            }
            _ => {}
        }
    }

    let mut i = 0;
    while i < input.len() {
        let mut c = input[i];

        if i + 1 < input.len() {
            if c == b'w' && input[i + 1] == b'r' {
                i += 1;
                continue;
            }
            if c == b'd' && (input[i + 1] == b'j' || input[i + 1] == b'g') {
                i += 1;
                continue;
            }
            if i + 2 < input.len() && c == b't' && input[i + 1] == b'c' && input[i + 2] == b'h' {
                i += 1;
                continue;
            }
        }

        c = a_class[(c & 0x7f) as usize];

        if c == CCLASS_SPACE {
            i += 1;
            continue;
        }

        if c == CCLASS_OTHER && c_prev != CCLASS_DIGIT {
            i += 1;
            continue;
        }

        a_class = &MID_CLASS;

        if c == CCLASS_VOWEL && (c_prev_x == CCLASS_R || c_prev_x == CCLASS_L) {
            i += 1;
            continue;
        }

        if (c == CCLASS_R || c == CCLASS_L) && c_prev_x == CCLASS_VOWEL && !z_out.is_empty() {
            z_out.pop();
        }

        c_prev = c;

        if c == CCLASS_SILENT {
            i += 1;
            continue;
        }

        c_prev_x = c;
        if (c as usize) < CLASS_NAME.len() {
            c = CLASS_NAME[c as usize];
        } else {
            c = b'?';
        }

        if z_out.is_empty() || c != *z_out.last().unwrap() {
            z_out.push(c);
        }

        i += 1;
    }

    Some(z_out)
}

pub fn phonetic_hash_str(input: Option<&str>) -> Option<String> {
    match input {
        None => None,
        Some(s) => {
            phonetic_hash(s.as_bytes()).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        }
    }
}
