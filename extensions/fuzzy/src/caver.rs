// remove_non_letters deletes everything from the source string,
// except lowercased letters a-z
fn remove_non_letters(src: &str) -> String {
    src.chars()
        .filter(|x: &char| x.is_ascii_lowercase())
        .collect()
}

// replace_start replaces the `old` substring with the `new` one
// if it matches at the beginning of the `src` string
fn replace_start(src: &str, old: &str, new: &str) -> String {
    if let Some(suffix) = src.strip_prefix(old) {
        let mut result = String::with_capacity(src.len() - old.len() + new.len());
        result.push_str(new);
        result.push_str(suffix);
        result
    } else {
        src.to_string()
    }
}

// replace_end replaces the `old` substring with the `new` one
// if it matches at the end of the `src` string
fn replace_end(src: &str, old: &str, new: &str) -> String {
    if let Some(prefix) = src.strip_suffix(old) {
        let mut result = String::with_capacity(src.len() - old.len() + new.len());
        result.push_str(prefix);
        result.push_str(new);
        result
    } else {
        src.to_string()
    }
}

// replace replaces all `old` substrings with `new` ones
// in the the `src` string
fn replace(src: &str, old: &str, new: &str) -> String {
    if old.is_empty() || src.is_empty() {
        return src.to_string();
    }

    let mut result = String::with_capacity(src.len());
    let mut idx = 0;

    while idx < src.len() {
        if idx + old.len() <= src.len() && &src[idx..idx + old.len()] == old {
            result.push_str(new);
            idx += old.len();
        } else {
            let ch = src[idx..].chars().next().unwrap();
            result.push(ch);
            idx += ch.len_utf8();
        }
    }

    result
}

// replace_seq replaces all sequences of the `old` character
// with the `new` substring in the the `src` string
fn replace_seq(src: &str, old: char, new: &str) -> String {
    let mut result = String::with_capacity(src.len());
    let mut match_len = 0;

    for ch in src.chars() {
        if ch == old {
            match_len += 1;
        } else {
            if match_len > 0 {
                result.push_str(new);
                match_len = 0;
            }
            result.push(ch);
        }
    }

    if match_len > 0 {
        result.push_str(new);
    }

    result
}

// pad pads `src` string with trailing 1s
// up to the length of 10 characters
fn pad(src: &str) -> String {
    let max_len = 10;
    let mut result = String::with_capacity(max_len);
    for ch in src.chars().take(max_len) {
        result.push(ch);
    }
    while result.chars().count() < max_len {
        result.push('1');
    }

    result
}

// caverphone implements the Caverphone phonetic hashing algorithm
// https://en.wikipedia.org/wiki/Caverphone
fn caverphone(src: &str) -> String {
    if src.is_empty() {
        return String::new();
    }

    let mut res = remove_non_letters(src);
    res = replace_end(&res, "e", "");
    res = replace_start(&res, "cough", "cou2f");
    res = replace_start(&res, "rough", "rou2f");
    res = replace_start(&res, "tough", "tou2f");
    res = replace_start(&res, "enough", "enou2f");
    res = replace_start(&res, "trough", "trou2f");

    res = replace_start(&res, "gn", "2n");
    res = replace_end(&res, "mb", "m2");

    res = replace(&res, "cq", "2q");
    res = replace(&res, "ci", "si");
    res = replace(&res, "ce", "se");
    res = replace(&res, "cy", "sy");
    res = replace(&res, "tch", "2ch");
    res = replace(&res, "c", "k");
    res = replace(&res, "q", "k");
    res = replace(&res, "x", "k");
    res = replace(&res, "v", "f");
    res = replace(&res, "dg", "2g");
    res = replace(&res, "tio", "sio");
    res = replace(&res, "tia", "sia");
    res = replace(&res, "d", "t");
    res = replace(&res, "ph", "fh");
    res = replace(&res, "b", "p");
    res = replace(&res, "sh", "s2");
    res = replace(&res, "z", "s");

    res = replace_start(&res, "a", "A");
    res = replace_start(&res, "e", "A");
    res = replace_start(&res, "i", "A");
    res = replace_start(&res, "o", "A");
    res = replace_start(&res, "u", "A");

    res = replace(&res, "a", "3");
    res = replace(&res, "e", "3");
    res = replace(&res, "i", "3");
    res = replace(&res, "o", "3");
    res = replace(&res, "u", "3");

    res = replace(&res, "j", "y");
    res = replace_start(&res, "y3", "Y3");
    res = replace_start(&res, "y", "A");
    res = replace(&res, "y", "3");

    res = replace(&res, "3gh3", "3kh3");
    res = replace(&res, "gh", "22");
    res = replace(&res, "g", "k");

    res = replace_seq(&res, 's', "S");
    res = replace_seq(&res, 't', "T");
    res = replace_seq(&res, 'p', "P");
    res = replace_seq(&res, 'k', "K");
    res = replace_seq(&res, 'f', "F");
    res = replace_seq(&res, 'm', "M");
    res = replace_seq(&res, 'n', "N");

    res = replace(&res, "w3", "W3");
    res = replace(&res, "wh3", "Wh3");
    res = replace_end(&res, "w", "3");
    res = replace(&res, "w", "2");

    res = replace_start(&res, "h", "A");
    res = replace(&res, "h", "2");

    res = replace(&res, "r3", "R3");
    res = replace_end(&res, "r", "3");
    res = replace(&res, "r", "2");

    res = replace(&res, "l3", "L3");
    res = replace_end(&res, "l", "3");
    res = replace(&res, "l", "2");

    res = replace(&res, "2", "");
    res = replace_end(&res, "3", "A");
    res = replace(&res, "3", "");

    res = pad(&res);

    res
}

pub fn caver_str(input: Option<&str>) -> Option<String> {
    input.map(caverphone)
}
